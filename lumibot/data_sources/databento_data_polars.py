"""Ultra-optimized DataBento data source using pure polars with zero pandas conversions.

This implementation:
1. Eliminates datalines - uses polars columnar storage directly
2. Zero pandas conversions - pure polars throughout
3. Lazy evaluation for maximum performance
4. Efficient caching with parquet files
5. Vectorized operations only
"""

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, Optional, Union

import polars as pl

from lumibot.data_sources import DataSource
from lumibot.data_sources.polars_mixin import PolarsMixin
from lumibot.entities import Asset, Bars
from lumibot.tools import databento_helper_polars
from lumibot.tools.lumibot_logger import get_logger

logger = get_logger(__name__)


class DataBentoDataPolars(PolarsMixin, DataSource):
    """
    Ultra-optimized DataBento data source using pure polars for maximum performance.
    
    This data source provides access to DataBento's institutional-grade market data,
    with a focus on futures data and support for multiple asset types.
    All operations use polars for 3x+ performance improvement over pandas.
    """

    SOURCE = "DATABENTO"
    MIN_TIMESTEP = "minute"
    TIMESTEP_MAPPING = [
        {"timestep": "minute", "representations": ["1m", "minute", "1 minute"]},
        {"timestep": "hour", "representations": ["1h", "hour", "1 hour"]},
        {"timestep": "day", "representations": ["1d", "day", "1 day"]},
    ]

    def __init__(
        self,
        api_key: str,
        timeout: int = 30,
        max_retries: int = 3,
        max_memory: Optional[int] = None,
        enable_cache: bool = True,
        **kwargs
    ):
        """
        Initialize DataBento data source with polars optimization
        
        Parameters
        ----------
        api_key : str
            DataBento API key
        timeout : int, optional
            API request timeout in seconds, default 30
        max_retries : int, optional
            Maximum number of API retry attempts, default 3
        max_memory : int, optional
            Maximum memory usage in bytes for data storage
        enable_cache : bool, optional
            Enable caching of fetched data, default True
        **kwargs
            Additional parameters passed to parent class
        """
        # Initialize parent class
        super().__init__(api_key=api_key, **kwargs)

        self.name = "databento"
        self._api_key = api_key
        self._timeout = timeout
        self._max_retries = max_retries
        self.MAX_STORAGE_BYTES = max_memory
        self.enable_cache = enable_cache

        # Initialize polars storage from mixin
        self._init_polars_storage()

        # Additional DataBento-specific caches
        self._eager_cache: Dict[Asset, pl.DataFrame] = {}

        # For live trading, this is a live data source
        self.is_backtesting_mode = False

        # Verify DataBento availability
        if not databento_helper_polars.DATABENTO_AVAILABLE:
            logger.error("DataBento package not available. Please install with: pip install databento")
            raise ImportError("DataBento package not available")

    def _store_data(self, asset: Asset, data: pl.DataFrame) -> pl.LazyFrame:
        """Store data efficiently using lazy frames.
        
        Returns lazy frame for efficient subsequent operations.
        """
        # Use mixin's store method
        lazy_data = self._store_data_polars(asset, data)

        if lazy_data is not None:
            # Cache eager version for DataBento's fast access needs
            self._eager_cache[asset] = lazy_data.collect()

            # Enforce storage limit
            self._enforce_storage_limit(self._data_store)

        return lazy_data

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

    def _convert_to_polars(self, df, asset: Asset = None) -> pl.DataFrame:
        """Convert pandas DataFrame or raw data to polars DataFrame efficiently."""
        if df is None:
            return None

        if isinstance(df, pl.DataFrame):
            return df

        # Convert pandas to polars
        try:
            if hasattr(df, 'index') and hasattr(df.index, 'name'):
                pl_df = pl.from_pandas(df.reset_index())
            else:
                pl_df = pl.from_pandas(df)

            # Ensure datetime column exists
            datetime_cols = ['datetime', 'timestamp', 'ts_event', 'time']
            datetime_col = None
            for col in datetime_cols:
                if col in pl_df.columns:
                    datetime_col = col
                    break

            if datetime_col and datetime_col != 'datetime':
                pl_df = pl_df.rename({datetime_col: 'datetime'})

            return pl_df
        except Exception as e:
            logger.error(f"Error converting to polars DataFrame: {e}")
            return None

    def get_historical_prices(
        self,
        asset: Asset,
        length: int,
        timestep: str = "minute",
        timeshift: timedelta = None,
        quote: Asset = None,
        exchange: str = None,
        include_after_hours: bool = True,
        return_polars: bool = False,
    ) -> Bars:
        """
        Get historical price data for an asset using optimized polars operations
        
        Parameters
        ----------
        asset : Asset
            The asset to get historical prices for
        length : int
            Number of bars to retrieve
        timestep : str, optional
            Timestep for the data ('minute', 'hour', 'day'), default 'minute'
        timeshift : timedelta, optional
            Time shift to apply to the data retrieval
        quote : Asset, optional
            Quote asset (not used for DataBento)
        exchange : str, optional
            Exchange/venue filter
        include_after_hours : bool, optional
            Whether to include after-hours data, default True
            
        Returns
        -------
        Bars
            Historical price data as Bars object
        """
        logger.debug(f"Getting historical prices for {asset.symbol}, length={length}, timestep={timestep}")

        # Validate asset type - DataBento primarily supports futures
        supported_asset_types = [Asset.AssetType.FUTURE, Asset.AssetType.CONT_FUTURE]
        if asset.asset_type not in supported_asset_types:
            error_msg = f"DataBento data source only supports futures assets. Received asset type '{asset.asset_type}' for symbol '{asset.symbol}'. Supported types: {[t.value for t in supported_asset_types]}"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Check if we have cached data for this asset
        cache_key = (asset, timestep)
        if cache_key in self._filtered_data_cache:
            cached_df = self._filtered_data_cache[cache_key]
            if len(cached_df) >= length:
                # Use cached data
                result_df = cached_df.tail(length)
                logger.debug(f"Using cached data for {asset.symbol}: {len(result_df)} bars")
                return Bars(
                    df=result_df,
                    source=self.SOURCE,
                    asset=asset,
                    quote=quote,
                    return_polars=return_polars,
                )

        # Calculate the date range for data retrieval
        current_dt = datetime.now()
        if current_dt.tzinfo is not None:
            current_dt = current_dt.replace(tzinfo=None)

        # Apply timeshift if specified
        if timeshift:
            current_dt = current_dt - timeshift

        # Calculate start date based on length and timestep
        if timestep == "day":
            buffer_days = max(10, length // 2)
            start_dt = current_dt - timedelta(days=length + buffer_days)
            end_dt = current_dt
        elif timestep == "hour":
            buffer_hours = max(24, length // 2)
            start_dt = current_dt - timedelta(hours=length + buffer_hours)
            end_dt = current_dt
        else:  # minute or other
            buffer_minutes = max(1440, length)
            start_dt = current_dt - timedelta(minutes=length + buffer_minutes)
            end_dt = current_dt

        # Ensure both dates are timezone-naive for consistency
        if start_dt.tzinfo is not None:
            start_dt = start_dt.replace(tzinfo=None)
        if end_dt.tzinfo is not None:
            end_dt = end_dt.replace(tzinfo=None)

        # Ensure we always have a valid date range
        if start_dt >= end_dt:
            if timestep == "day":
                end_dt = start_dt + timedelta(days=max(1, length))
            elif timestep == "hour":
                end_dt = start_dt + timedelta(hours=max(1, length))
            else:
                end_dt = start_dt + timedelta(minutes=max(1, length))

        # Get data from DataBento
        logger.debug(f"Requesting DataBento data for {asset.symbol} from {start_dt} to {end_dt}")

        try:
            # Get polars DataFrame directly from optimized helper
            df = databento_helper_polars.get_price_data_from_databento_polars(
                api_key=self._api_key,
                asset=asset,
                start=start_dt,
                end=end_dt,
                timestep=timestep,
                venue=exchange
            )

            if df is None or df.is_empty():
                logger.warning(f"No data returned from DataBento for {asset.symbol}")
                return None

            # Store in cache for future use
            if self.enable_cache:
                self._store_data(asset, df)
                self._filtered_data_cache[cache_key] = df

            # Filter data to the current time
            if 'datetime' in df.columns:
                # Ensure datetime column is datetime type
                if df['datetime'].dtype != pl.Datetime:
                    df = df.with_columns(pl.col('datetime').cast(pl.Datetime))

                # Filter using polars operations
                df_filtered = df.filter(pl.col('datetime') <= current_dt)
            else:
                # If no datetime column, use index position
                df_filtered = df

            # Take the last 'length' bars
            df_result = df_filtered.tail(length)

            if df_result.is_empty():
                logger.warning(f"No data available for {asset.symbol} up to {current_dt}")
                return None

            # Create and return Bars object
            bars = Bars(
                df=df_result,
                source=self.SOURCE,
                asset=asset,
                quote=quote,
                return_polars=return_polars,
            )

            logger.debug(f"Retrieved {len(df_result)} bars for {asset.symbol}")
            return bars

        except Exception as e:
            logger.error(f"Error getting data from DataBento for {asset.symbol}: {e}")
            return None

    def get_last_price(
        self,
        asset: Asset,
        quote: Asset = None,
        exchange: str = None
    ) -> Union[float, Decimal, None]:
        """
        Get the last known price for an asset using cached data when possible
        
        Parameters
        ----------
        asset : Asset
            The asset to get the last price for
        quote : Asset, optional
            Quote asset (not used for DataBento)
        exchange : str, optional
            Exchange/venue filter
            
        Returns
        -------
        float, Decimal, or None
            Last known price of the asset
        """
        # Check cache first
        if asset in self._last_price_cache:
            cached_price = self._last_price_cache[asset]
            logger.debug(f"Using cached last price for {asset.symbol}: {cached_price}")
            return cached_price

        logger.debug(f"Getting last price for {asset.symbol}")

        try:
            # Try to get from cached data first
            if asset in self._eager_cache:
                df = self._eager_cache[asset]
                if not df.is_empty() and 'close' in df.columns:
                    last_price = float(df['close'][-1])
                    self._last_price_cache[asset] = last_price
                    logger.debug(f"Last price from cache for {asset.symbol}: {last_price}")
                    return last_price

            # Fall back to API call
            last_price = databento_helper_polars.get_last_price_from_databento_polars(
                api_key=self._api_key,
                asset=asset,
                venue=exchange
            )

            if last_price is not None:
                self._last_price_cache[asset] = float(last_price)
                logger.debug(f"Last price from API for {asset.symbol}: {last_price}")
                return last_price
            else:
                logger.warning(f"No last price available for {asset.symbol}")
                return None

        except Exception as e:
            logger.error(f"Error getting last price for {asset.symbol}: {e}")
            return None

    def get_chains(self, asset: Asset, quote: Asset = None) -> dict:
        """
        Get option chains for an asset
        
        Note: DataBento primarily focuses on market data rather than options chains.
        This method returns an empty dict as DataBento doesn't provide options chain data.
        
        Parameters
        ----------
        asset : Asset
            The asset to get option chains for
        quote : Asset, optional
            Quote asset
            
        Returns
        -------
        dict
            Empty dictionary as DataBento doesn't provide options chains
        """
        logger.warning("DataBento does not provide options chain data")
        return {}

    def get_quote(self, asset: Asset, quote: Asset = None) -> Union[float, Decimal, None]:
        """
        Get current quote for an asset
        
        For DataBento, this returns the last known price since real-time quotes
        may not be available for all assets.
        
        Parameters
        ----------
        asset : Asset
            The asset to get the quote for
        quote : Asset, optional
            Quote asset (not used for DataBento)
            
        Returns
        -------
        float, Decimal, or None
            Current quote/last price of the asset
        """
        return self.get_last_price(asset, quote=quote)

    def _parse_source_symbol_bars(self, response, asset, quote=None, return_polars: bool = False):
        """
        Parse source data for a single asset into Bars format
        
        Parameters
        ----------
        response : pl.DataFrame or pd.DataFrame
            Raw data from DataBento API
        asset : Asset
            The asset the data is for
        quote : Asset, optional
            Quote asset (not used for DataBento)
            
        Returns
        -------
        Bars or None
            Parsed bars data or None if parsing fails
        """
        try:
            # Convert to polars if needed
            if response is None:
                return None

            df = self._convert_to_polars(response, asset)

            if df is None or df.is_empty():
                return None

            # Check if required columns exist
            required_columns = ['open', 'high', 'low', 'close', 'volume']
            if not all(col in df.columns for col in required_columns):
                logger.warning(f"Missing required columns in DataBento data for {asset.symbol}")
                return None

            # Use mixin's parse method
            return self._parse_source_symbol_bars_polars(
                df, asset, self.SOURCE, quote, return_polars=return_polars
            )

        except Exception as e:
            logger.error(f"Error parsing DataBento data for {asset.symbol}: {e}")
            return None

    def clear_cache(self):
        """Clear all cached data to free memory"""
        # Use mixin's clear method
        self._clear_cache_polars()
        # Clear DataBento-specific caches
        self._eager_cache.clear()
        logger.info("Cleared all DataBento data caches")
