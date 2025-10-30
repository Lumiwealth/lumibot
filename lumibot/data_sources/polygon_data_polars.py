"""Ultra-optimized Polygon data source using pure polars with zero pandas conversions.

This implementation:
1. Eliminates datalines - uses polars columnar storage directly
2. Zero pandas conversions - pure polars throughout
3. Lazy evaluation for maximum performance
4. Efficient caching with parquet files
5. Vectorized operations only
"""
# NOTE: This module is intentionally disabled. The DataBento Polars migration only
# supports Polars for DataBento; other data sources must use the pandas implementations.
raise RuntimeError('Yahoo/Polygon Polars backends are not production-ready; use the pandas data sources instead.')



import traceback
from datetime import timedelta
from decimal import Decimal
from typing import Dict, List, Optional, Union

import numpy as np
import polars as pl
from polygon.exceptions import BadResponse
from termcolor import colored

from lumibot.data_sources import DataSourceBacktesting
from lumibot.data_sources.polars_mixin import PolarsMixin
from lumibot.entities import Asset, Bars
from lumibot.tools import polygon_helper_polars_optimized
from lumibot.tools.lumibot_logger import get_logger

try:
    from lumibot.tools import polygon_helper_async
    ASYNC_AVAILABLE = True
except ImportError:
    ASYNC_AVAILABLE = False
# PolygonClient imported at runtime to avoid circular imports

logger = get_logger(__name__)
START_BUFFER = timedelta(days=5)


class PolygonDataPolars(PolarsMixin, DataSourceBacktesting):
    """Ultra-optimized Polygon data source with pure polars."""

    SOURCE = "POLYGON"
    MIN_TIMESTEP = "minute"
    TIMESTEP_MAPPING = [
        {"timestep": "day", "representations": ["1d", "day"]},
        {"timestep": "minute", "representations": ["1m", "1 minute", "minute"]},
        {"timestep": "hour", "representations": ["1h", "1 hour", "hour"]},
    ]
    option_quote_fallback_allowed = True

    def __init__(
        self,
        datetime_start,
        datetime_end,
        api_key=None,
        max_memory=None,
        use_async=True,
        enable_prefetch_cache=True,
        **kwargs,
    ):
        super().__init__(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            api_key=api_key,
            **kwargs
        )

        # Memory limit, off by default
        self.MAX_STORAGE_BYTES = max_memory

        # RESTClient API for Polygon.io polygon-api-client
        from lumibot.tools.polygon_helper import PolygonClient
        self.polygon_client = PolygonClient.create(api_key=api_key)

        # Initialize polars storage from mixin
        self._init_polars_storage()

        # Additional Polygon-specific caches
        self._eager_cache: Dict[Asset, pl.DataFrame] = {}

        # Batch download queue for optimized downloads
        self._download_queue = set()
        self._download_lock = False

        # Prefetch cache for aggressive prefetching
        self.enable_prefetch_cache = enable_prefetch_cache
        self._prefetch_cache: Dict[tuple, bool] = {}  # Track what's been prefetched

        # Use async downloads if available and requested
        self.use_async = use_async and ASYNC_AVAILABLE
        if self.use_async:
            logger.info("Using async Polygon downloads for maximum performance")

    def _enforce_storage_limit(self, data_store: Dict[Asset, pl.LazyFrame]):
        """Enforce storage limit by removing least recently used data."""
        # Use mixin's enforce method
        self._enforce_storage_limit_polars(self.MAX_STORAGE_BYTES)

        # Clean up Polygon-specific caches
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

        # Calculate additional derived columns for Polygon
        lazy_data = lazy_data.with_columns([
            pl.col("close").pct_change().alias("price_change"),
            pl.when(pl.col("dividend").is_not_null())
                .then(pl.col("dividend") / pl.col("close"))
                .otherwise(0.0)
                .alias("dividend_yield"),
            pl.when(pl.col("dividend").is_not_null())
                .then((pl.col("dividend") / pl.col("close")) + pl.col("close").pct_change())
                .otherwise(pl.col("close").pct_change())
                .alias("return")
        ])

        # Add missing dividend column if needed
        if "dividend" not in data.columns:
            lazy_data = lazy_data.with_columns(pl.lit(0.0).alias("dividend"))

        # Update the stored data
        self._data_store[asset] = lazy_data

        # Enforce storage limit
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
            The timestep to use. For example, "1minute" or "1hour" or "1day".
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
            The timestep to use. For example, "1minute" or "1hour" or "1day".
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

        # Get the start datetime and timestep unit
        start_datetime, ts_unit = self.get_start_datetime_and_ts_unit(
            length, timestep, start_dt, start_buffer=START_BUFFER
        )

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

        # Download data from Polygon using async or sync helper
        try:
            if self.use_async:
                # Use async version for better performance
                df = polygon_helper_async.get_price_data_from_polygon_async(
                    self._api_key,
                    asset_separated,
                    start_datetime,
                    self.datetime_end,
                    timespan=ts_unit,
                    quote_asset=quote_asset
                )
            else:
                # Fall back to sync version
                df = polygon_helper_polars_optimized.get_price_data_from_polygon_polars(
                    self._api_key,
                    asset_separated,
                    start_datetime,
                    self.datetime_end,
                    timespan=ts_unit,
                    quote_asset=quote_asset
                )
        except BadResponse as e:
            # Assuming e.message or similar attribute contains the error message
            formatted_start_datetime = start_datetime.strftime("%Y-%m-%d")
            formatted_end_datetime = self.datetime_end.strftime("%Y-%m-%d")
            text = str(e)
            plan_msgs = (
                "Your plan doesn't include this data timeframe",
                "Your plan doesn\u2019t include this data timeframe",
                "not entitled to this data",
                "NOT_AUTHORIZED",
            )
            invalid_key_msgs = ("Unknown API Key", "Invalid API Key")
            if any(m in text for m in plan_msgs) and not any(m in text for m in invalid_key_msgs):
                msg = (
                    "Polygon Access Denied: Your subscription does not allow you to backtest that far back in time. "
                    f"Requested {asset_separated} {ts_unit} bars from {formatted_start_datetime} to {formatted_end_datetime}. "
                    "Consider starting later or upgrading your Polygon subscription (https://polygon.io/?utm_source=affiliate&utm_campaign=lumi10, code 'LUMI10')."
                )
                logger.error(colored(msg, color="red"))
                # Non-fatal: skip this download window and continue
                return
            elif "Unknown API Key" in str(e):
                error_message = colored(
                    "Polygon Access Denied: Your API key is invalid. "
                    "Please check your API key and try again. "
                    "You can get an API key at https://polygon.io/?utm_source=affiliate&utm_campaign=lumi10 "
                    "Please use the full link to give us credit for the sale, it helps support this project. "
                    "You can use the coupon code 'LUMI10' for 10% off. ",
                    color="red")
                raise Exception(error_message) from e
            else:
                # Handle other BadResponse exceptions not related to plan limitations
                logger.error(traceback.format_exc())
                raise
        except Exception as e:
            # Handle all other exceptions
            logger.error(traceback.format_exc())
            raise Exception("Error getting data from Polygon") from e

        if (df is None) or len(df) == 0:
            # Add diagnostic logging to help debug missing data scenarios (e.g., crypto day bars)
            try:
                from lumibot.tools.polygon_helper import get_polygon_symbol as _sym_fn
                dbg_symbol = _sym_fn(asset_separated, self.polygon_client, quote_asset)
            except Exception:
                dbg_symbol = f"<symbol-error {asset_separated.symbol}>"
            logger.warning(
                "Polygon returned no data: asset=%s resolved_symbol=%s quote=%s ts_unit=%s start=%s end=%s (len=%s)",
                getattr(asset_separated, 'symbol', asset_separated),
                dbg_symbol,
                getattr(quote_asset, 'symbol', quote_asset),
                ts_unit,
                start_datetime.strftime('%Y-%m-%d %H:%M:%S'),
                self.datetime_end.strftime('%Y-%m-%d %H:%M:%S'),
                0,
            )
            return

        # Store data
        self._store_data(search_asset, df)

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
        # Get data from Polygon
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
            logger.warning(f"quote is not implemented for PolygonData, but {quote} was passed as the quote")

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
            logger.error(f"Error get_last_price from Polygon: {e}")
            logger.error(f"Error get_last_price from Polygon: {asset=} {quote=} {timestep=} {dt=} {e}")
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

    def get_historical_prices_between_dates(
        self,
        asset,
        timestep="minute",
        quote=None,
        exchange=None,
        include_after_hours=True,
        start_date=None,
        end_date=None,
    ):
        """Get pricing data for an asset for the entire backtesting period."""
        self._update_data(asset, quote, 1, timestep)

        search_asset = asset if not isinstance(asset, tuple) else asset
        if quote:
            search_asset = (asset, quote)

        lazy_data = self._get_data_lazy(search_asset)

        if lazy_data is None:
            return None

        # Filter by date range if provided
        if start_date or end_date:
            filters = []
            if start_date:
                # Use pl.lit() to ensure datetime precision compatibility across Polars versions
                filters.append(pl.col("datetime") >= pl.lit(start_date))
            if end_date:
                # Use pl.lit() to ensure datetime precision compatibility across Polars versions
                filters.append(pl.col("datetime") <= pl.lit(end_date))

            response = lazy_data.filter(pl.all_horizontal(filters)).collect()
        else:
            response = lazy_data.collect()

        if response is None or len(response) == 0:
            return None

        bars = self._parse_source_symbol_bars(response, asset, quote=quote)
        return bars

    def get_chains(self, asset: Asset, quote: Asset = None, exchange: str = None):
        """
        Integrates the Polygon client library into the LumiBot backtest for Options Data
        in the same structure as Interactive Brokers options chain data.

        Parameters
        ----------
        asset : Asset
            The underlying asset symbol. Typically an equity like "SPY" or "NVDA".
        quote : Asset, optional
            The quote asset to use, e.g. Asset("USD"). (Usually unused for equities.)
        exchange : str, optional
            The exchange to which the chain belongs (e.g., "SMART").

        Returns
        -------
        dict
            A dictionary of dictionaries describing the option chain.

            Format:
            - "Multiplier": int
                e.g. 100
            - "Exchange": str
                e.g. "NYSE"
            - "Chains": dict
                Dictionary with "CALL" and "PUT" keys.
                Each key is itself a dictionary mapping expiration dates (YYYY-MM-DD) to a list of strikes.

            Example
            -------
            {
                "Multiplier": 100,
                "Exchange": "NYSE",
                "Chains": {
                    "CALL": {
                        "2023-07-31": [100.0, 101.0, ...],
                        "2023-08-07": [...],
                        ...
                    },
                    "PUT": {
                        "2023-07-31": [100.0, 101.0, ...],
                        ...
                    }
                }
            }

        Notes
        -----
        This function uses optimized parallel fetching when available, falling back
        to the standard cached version if the optimizer is not available.
        """
        logger.debug(f"polygon_backtesting.get_chains called for {asset.symbol}")

        # Use standard option chain fetching
        option_contracts = polygon_helper_polars_optimized.get_chains_cached(
            api_key=self._api_key,
            asset=asset,
            quote=quote,
            exchange=exchange,
            current_date=self.get_datetime().date(),
            polygon_client=self.polygon_client,
        )

        return option_contracts

    def get_quote(self, asset: Asset) -> None:
        """Get quote - not implemented for Polygon."""
        return None

    def batch_prefetch_data(self, prefetch_requests: List[tuple]):
        """
        Batch prefetch multiple data requests for maximum efficiency.
        
        This method downloads multiple asset/time combinations concurrently,
        optimizing for speed by batching requests.
        
        Parameters
        ----------
        prefetch_requests : List[tuple]
            List of (asset, start_dt, end_dt, timestep) tuples to prefetch
        """
        import concurrent.futures

        if not prefetch_requests:
            return

        # Filter out already cached data
        requests_to_fetch = []
        for asset, start_dt, end_dt, timestep in prefetch_requests:
            if not self.is_data_cached(asset, start_dt, end_dt, timestep):
                requests_to_fetch.append((asset, start_dt, end_dt, timestep))

        if not requests_to_fetch:
            logger.debug("All requested data already cached")
            return

        logger.debug(f"Batch prefetching {len(requests_to_fetch)} data requests")

        def fetch_single(request):
            """Fetch a single data request."""
            asset, start_dt, end_dt, timestep = request
            try:
                # Calculate length for the request
                if timestep == "day":
                    length = (end_dt - start_dt).days + 30  # Extra buffer
                else:
                    length = 200  # Default lookback for minute data

                # Use the existing update method
                self._update_data(asset, None, length, timestep, start_dt)

                # Mark as prefetched
                cache_key = (asset, start_dt.date(), end_dt.date(), timestep)
                self._prefetch_cache[cache_key] = True

                return True
            except Exception as e:
                logger.debug(f"Failed to prefetch {asset}: {e}")
                return False

        # Use thread pool for concurrent downloads
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            for request in requests_to_fetch:
                future = executor.submit(fetch_single, request)
                futures.append(future)

            # Wait for all to complete with timeout
            concurrent.futures.wait(futures, timeout=30)

        logger.debug(f"Batch prefetch completed for {len(requests_to_fetch)} requests")
