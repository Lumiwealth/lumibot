"""Ultra-optimized DataBento backtesting using pure polars with zero pandas conversions.

This implementation:
1. Uses polars columnar storage directly
2. Lazy evaluation for maximum performance
3. Efficient caching with parquet files
4. Vectorized operations only
5. Inherits from DataSourceBacktesting (proper architecture)
"""

import os
import traceback
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, Optional, Union

import numpy as np
import polars as pl

from lumibot.data_sources import DataSourceBacktesting
from lumibot.data_sources.polars_mixin import PolarsMixin
from lumibot.entities import Asset, Bars
from lumibot.tools import databento_helper_polars
from lumibot.tools.lumibot_logger import get_logger

logger = get_logger(__name__)
START_BUFFER = timedelta(days=5)


class DataBentoDataPolarsBacktesting(PolarsMixin, DataSourceBacktesting):
    """Ultra-optimized DataBento backtesting data source with pure polars."""

    SOURCE = "DATABENTO"
    MIN_TIMESTEP = "minute"
    TIMESTEP_MAPPING = [
        {"timestep": "minute", "representations": ["1m", "minute", "1 minute"]},
        {"timestep": "hour", "representations": ["1h", "hour", "1 hour"]},
        {"timestep": "day", "representations": ["1d", "day", "1 day"]},
    ]

    def __init__(
        self,
        datetime_start,
        datetime_end,
        api_key=None,
        max_memory=None,
        timeout=30,
        max_retries=3,
        **kwargs,
    ):
        super().__init__(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            api_key=api_key,
            **kwargs
        )

        self.name = "databento"
        self._api_key = api_key or os.environ.get("DATABENTO_API_KEY")
        self._timeout = timeout
        self._max_retries = max_retries
        self.MAX_STORAGE_BYTES = max_memory

        # Initialize polars storage from mixin
        self._init_polars_storage()

        # DataBento-specific caches
        self._eager_cache: Dict[Asset, pl.DataFrame] = {}

        # Prefetch tracking - CRITICAL for performance
        self._prefetch_cache: Dict[tuple, bool] = {}
        self._prefetched_assets = set()  # Track which assets have been fully loaded

        logger.info(f"DataBento backtesting initialized for period: {datetime_start} to {datetime_end}")

    def _enforce_storage_limit(self, data_store: Dict[Asset, pl.LazyFrame]):
        """Enforce storage limit by removing least recently used data."""
        # Use mixin's enforce method
        self._enforce_storage_limit_polars(self.MAX_STORAGE_BYTES)

        # Clean up DataBento-specific caches
        if self.MAX_STORAGE_BYTES and len(self._eager_cache) > 0:
            # Remove from eager cache too
            assets_to_remove = [a for a in self._eager_cache.keys() if a not in data_store]
            for asset in assets_to_remove:
                del self._eager_cache[asset]

    def _store_data(self, asset: Asset, data: pl.DataFrame) -> pl.LazyFrame:
        """Store data efficiently using lazy frames.

        Returns lazy frame for efficient subsequent operations.
        """
        # Use mixin's store method first
        lazy_data = self._store_data_polars(asset, data)

        if lazy_data is None:
            return None

        # Update the stored data
        self._data_store[asset] = lazy_data

        # Enforce storage limit
        if self.MAX_STORAGE_BYTES:
            self._enforce_storage_limit(self._data_store)

        return lazy_data

    def get_start_datetime_and_ts_unit(self, length, timestep, start_dt=None, start_buffer=timedelta(days=5)):
        """
        Get the start datetime for the data.

        Parameters
        ----------
        length : int
            The number of data points to get.
        timestep : str
            The timestep to use. For example, "minute" or "hour" or "day".
        start_dt : datetime
            The start datetime to use. If None, the current self.datetime_start will be used.
        start_buffer : timedelta
            The buffer to add to the start datetime.

        Returns
        -------
        datetime
            The start datetime.
        str
            The timestep unit.
        """
        # Convert timestep string to timedelta and get start datetime
        td, ts_unit = self.convert_timestep_str_to_timedelta(timestep)
        if ts_unit == "day":
            weeks_requested = length // 5  # Full trading week is 5 days
            extra_padding_days = weeks_requested * 3  # to account for 3day weekends
            td = timedelta(days=length + extra_padding_days)
        else:
            td *= length
        if start_dt is not None:
            start_datetime = start_dt - td
        else:
            start_datetime = self.datetime_start - td
        start_datetime = start_datetime - start_buffer
        return start_datetime, ts_unit

    def is_data_cached(self, asset: Asset, start_dt, end_dt, timestep: str) -> bool:
        """
        Check if data is already cached for the given parameters.

        Parameters
        ----------
        asset : Asset
            The asset to check
        start_dt : datetime
            Start datetime
        end_dt : datetime
            End datetime
        timestep : str
            Time granularity

        Returns
        -------
        bool
            True if data is cached, False otherwise
        """
        search_asset = asset
        if isinstance(asset, tuple):
            search_asset = asset

        # Check if in data store
        if search_asset not in self._data_store:
            return False

        # Check if in filtered cache for daily data
        if timestep == "day":
            cache_key = (search_asset, start_dt.date(), timestep)
            if cache_key in self._filtered_data_cache:
                return True

        # Check prefetch cache
        cache_key = (search_asset, start_dt.date(), end_dt.date(), timestep)
        return cache_key in self._prefetch_cache

    def _update_data(self, asset: Asset, quote: Asset, length: int, timestep: str, start_dt=None):
        """
        Get asset data and update the self._data_store dictionary.

        Parameters
        ----------
        asset : Asset
            The asset to get data for.
        quote : Asset
            The quote asset to use. For example, if asset is "SPY" and quote is "USD", the data will be for "SPY/USD".
        length : int
            The number of data points to get.
        timestep : str
            The timestep to use. For example, "minute" or "hour" or "day".
        start_dt : datetime
            The start datetime to use. If None, the current self.start_datetime will be used.
        """
        search_asset = asset
        asset_separated = asset
        quote_asset = quote if quote is not None else Asset("USD", "forex")

        if isinstance(search_asset, tuple):
            asset_separated, quote_asset = search_asset
        else:
            search_asset = (search_asset, quote_asset)

        # CRITICAL: If asset was prefetched, don't fetch again!
        if search_asset in self._prefetched_assets:
            return

        # Check if we already have data in the store
        if search_asset in self._data_store:
            # Data already loaded, mark as prefetched and return
            self._prefetched_assets.add(search_asset)
            return

        # Get the start datetime and timestep unit
        start_datetime, ts_unit = self.get_start_datetime_and_ts_unit(
            length, timestep, start_dt, start_buffer=START_BUFFER
        )

        # Fetch data for ENTIRE backtest period (like pandas does)
        start_datetime = self.datetime_start - START_BUFFER
        end_datetime = self.datetime_end + timedelta(days=1)

        logger.info(f"Prefetching {asset_separated.symbol} data from {start_datetime.date()} to {end_datetime.date()}")

        # Check if we have data for this asset
        if search_asset in self._data_store:
            # For daily timestep, use optimized caching strategy
            if ts_unit == "day":
                # Check if we need to clear cache for new date
                current_date = self._datetime.date()

                # Try to get from filtered cache first
                cache_key = (search_asset, current_date, ts_unit)
                if cache_key in self._filtered_data_cache:
                    result = self._filtered_data_cache[cache_key]
                    if len(result) >= length:
                        # Cache hit!
                        return

        # Download data from DataBento using polars helper
        try:
            df = databento_helper_polars.get_price_data_from_databento_polars(
                api_key=self._api_key,
                asset=asset_separated,
                start=start_datetime,
                end=end_datetime,
                timestep=timestep,
                venue=None,
                force_cache_update=False
            )
        except Exception as e:
            # Handle all exceptions
            logger.error(f"Error getting data from DataBento: {e}")
            logger.error(traceback.format_exc())
            # Mark as prefetched even on error to avoid retry loops
            self._prefetched_assets.add(search_asset)
            raise Exception("Error getting data from DataBento") from e

        if (df is None) or len(df) == 0:
            logger.warning(
                f"DataBento returned no data: asset={getattr(asset_separated, 'symbol', asset_separated)} "
                f"quote={getattr(quote_asset, 'symbol', quote_asset)} "
                f"timestep={timestep} start={start_datetime.strftime('%Y-%m-%d %H:%M:%S')} "
                f"end={end_datetime.strftime('%Y-%m-%d %H:%M:%S')} len=0"
            )
            # Mark as prefetched to avoid retry
            self._prefetched_assets.add(search_asset)
            return

        # Store data
        self._store_data(search_asset, df)
        logger.info(f"Cached {len(df)} rows for {asset_separated.symbol}")

        # Mark as prefetched
        self._prefetched_assets.add(search_asset)

    def _pull_source_symbol_bars(
        self,
        asset: Asset,
        length: int,
        timestep: str = "day",
        timeshift: int = None,
        quote: Asset = None,
        exchange: str = None,
        include_after_hours: bool = True,
    ) -> Optional[pl.DataFrame]:
        """Pull bars with maximum efficiency using pre-filtered cache."""

        # Build search key
        search_asset = asset if not isinstance(asset, tuple) else asset
        if quote:
            search_asset = (asset, quote)

        # For daily timestep, use optimized caching strategy
        if timestep == "day":
            current_date = self._datetime.date()
            cache_key = (search_asset, current_date, timestep)

            # Try cache first
            if cache_key in self._filtered_data_cache:
                result = self._filtered_data_cache[cache_key]
                if len(result) >= length:
                    return result.tail(length)

        # Get the current datetime and calculate the start datetime
        current_dt = self.get_datetime()
        # Get data from DataBento
        self._update_data(asset, quote, length, timestep, current_dt)

        # Get lazy data
        search_asset = asset if not isinstance(asset, tuple) else asset
        if quote:
            search_asset = (asset, quote)

        lazy_data = self._get_data_lazy(search_asset)

        if lazy_data is None:
            return None

        # Use lazy evaluation and collect only when needed
        # Check if we have cached filtered data first
        if timestep == "day":
            current_date = self._datetime.date()
            cache_key = (search_asset, current_date, timestep)
            if cache_key in self._filtered_data_cache:
                data = self._filtered_data_cache[cache_key]
            else:
                # Collect with filtering for efficiency
                data = lazy_data.collect()
        else:
            # For minute data, collect on demand
            data = lazy_data.collect()

        # OPTIMIZATION: Direct filtering on eager DataFrame
        current_dt = self.to_default_timezone(self._datetime)

        # Determine end filter
        if timestep == "day":
            dt = self._datetime.replace(hour=23, minute=59, second=59, microsecond=999999)
            end_filter = dt - timedelta(days=1)
        else:
            end_filter = current_dt

        if timeshift:
            if isinstance(timeshift, int):
                timeshift = timedelta(days=timeshift)
            end_filter = end_filter - timeshift

        logger.debug(f"Filtering {asset.symbol} data: current_dt={current_dt}, end_filter={end_filter}, timestep={timestep}, timeshift={timeshift}")

        # Convert to lazy frame for filtering
        lazy_data = data.lazy() if not hasattr(data, 'collect') else data

        # Use mixin's filter method
        result = self._filter_data_polars(search_asset, lazy_data, end_filter, length, timestep)

        if result is None:
            return None

        if len(result) < length:
            logger.debug(
                f"Requested {length} bars but only {len(result)} available "
                f"for {asset.symbol} before {end_filter}"
            )

        logger.debug(f"Returning {len(result)} bars for {asset.symbol}")

        return result

    def _parse_source_symbol_bars(
        self,
        response: pl.DataFrame,
        asset: Asset,
        quote: Optional[Asset] = None,
        length: Optional[int] = None,
        return_polars: bool = False,
    ) -> Bars:
        """Parse bars from polars DataFrame."""
        if quote is not None:
            logger.warning(f"quote is not implemented for DataBentoData, but {quote} was passed as the quote")

        # Use mixin's parse method
        return self._parse_source_symbol_bars_polars(
            response, asset, self.SOURCE, quote, length, return_polars=return_polars
        )

    def get_last_price(
        self,
        asset: Asset,
        timestep: str = "minute",
        quote: Optional[Asset] = None,
        exchange: Optional[str] = None,
        **kwargs
    ) -> Union[float, Decimal, None]:
        """Get last price with aggressive caching."""

        if timestep is None:
            timestep = self.get_timestep()

        # Use mixin's cache check
        current_datetime = self._datetime
        cached_price = self._get_cached_last_price_polars(asset, current_datetime, timestep)
        if cached_price is not None:
            return cached_price

        try:
            dt = self.get_datetime()
            self._update_data(asset, quote, 1, timestep, dt)
        except Exception as e:
            logger.error(f"Error get_last_price from DataBento: {e}")
            logger.error(f"Error get_last_price from DataBento: {asset=} {quote=} {timestep=} {dt=} {e}")
            self._cache_last_price_polars(asset, None, current_datetime, timestep)
            return None

        # Get price efficiently
        # For daily data, don't apply additional timeshift since _pull_source_symbol_bars
        # already handles getting the previous day's data
        # Only request 1 bar for efficiency (matching pandas implementation)
        timeshift = None if timestep == "day" else timedelta(days=-1)
        length = 1

        bars_data = self._pull_source_symbol_bars(
            asset, length, timestep=timestep, timeshift=timeshift, quote=quote
        )

        if bars_data is None or len(bars_data) == 0:
            logger.warning(f"No bars data for {asset.symbol} at {current_datetime}")
            self._cache_last_price_polars(asset, None, current_datetime, timestep)
            return None

        # Direct column access - since we only request 1 bar, take the first (and only) element
        open_price = bars_data["open"][0]

        # Convert if needed
        if isinstance(open_price, (np.int64, np.integer)):
            open_price = Decimal(int(open_price))
        elif isinstance(open_price, (np.float64, np.floating)):
            open_price = float(open_price)

        # Use mixin's cache method
        self._cache_last_price_polars(asset, open_price, current_datetime, timestep)
        return open_price

    def get_historical_prices(
        self,
        asset: Asset,
        length: int,
        timestep: str = None,
        timeshift: Optional[timedelta] = None,
        quote: Optional[Asset] = None,
        exchange: Optional[str] = None,
        include_after_hours: bool = False,
        return_polars: bool = False,
    ) -> Optional[Bars]:
        """Get historical prices using polars."""
        logger.debug(f"get_historical_prices called for {asset.symbol}")
        if timestep is None:
            timestep = self.get_timestep()

        # Get bars data
        bars_data = self._pull_source_symbol_bars(
            asset,
            length,
            timestep=timestep,
            timeshift=timeshift,
            quote=quote,
            include_after_hours=include_after_hours
        )

        if bars_data is None:
            return None

        # Create and return Bars object
        return self._parse_source_symbol_bars(
            bars_data, asset, quote=quote, length=length, return_polars=return_polars
        )

    def get_chains(self, asset: Asset, quote: Asset = None, exchange: str = None):
        """Get option chains - not implemented for DataBento."""
        logger.warning("get_chains is not implemented for DataBentoData")
        return None

    def get_quote(self, asset: Asset) -> None:
        """Get quote - not implemented for DataBento backtesting."""
        return None
