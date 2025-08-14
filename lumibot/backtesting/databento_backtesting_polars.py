"""Ultra-optimized DataBento backtesting using pure polars"""

from datetime import timedelta

import polars as pl

from lumibot.data_sources import DataSourceBacktesting
from lumibot.entities import Asset, Bars
from lumibot.tools import databento_helper_polars
from lumibot.tools.lumibot_logger import get_logger

logger = get_logger(__name__)

START_BUFFER = timedelta(days=5)


class DataBentoDataBacktestingPolars(DataSourceBacktesting):
    """
    Ultra-optimized backtesting implementation of DataBento data source using polars
    
    This class provides DataBento-specific backtesting functionality with
    3x+ performance improvement through polars operations and efficient caching.
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
        datetime_start,
        datetime_end,
        pandas_data=None,
        api_key=None,
        timeout=30,
        max_retries=3,
        max_memory=None,
        enable_cache=True,
        **kwargs,
    ):
        """
        Initialize DataBento backtesting data source with polars optimization
        
        Parameters
        ----------
        datetime_start : datetime
            Start datetime for backtesting period
        datetime_end : datetime
            End datetime for backtesting period
        pandas_data : dict, optional
            Pre-loaded pandas data (will be converted to polars)
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
        # Initialize parent
        super().__init__(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            api_key=api_key,
            **kwargs
        )

        self.name = "databento"
        # Load API key from environment if not provided
        import os
        self._api_key = api_key or os.environ.get("DATABENTO_API_KEY")
        if not self._api_key:
            logger.error("DataBento API key not provided and DATABENTO_API_KEY environment variable not set")
        else:
            logger.info(f"DataBento API key loaded: {bool(self._api_key)}")
        self._timeout = timeout
        self._max_retries = max_retries
        self.MAX_STORAGE_BYTES = max_memory
        self.enable_cache = enable_cache

        # Optimized data storage - lazy frames for efficiency
        self._data_store = {}  # Asset -> pl.LazyFrame
        self._eager_cache = {}  # Asset -> pl.DataFrame

        # Performance optimizations
        self._last_price_cache = {}
        self._cache_datetime = None

        # Column access optimization
        self._column_indices = {}

        # Pre-filtered data cache for massive speedup
        self._filtered_data_cache = {}

        # Cache metadata to avoid unnecessary collections
        self._cache_metadata = {}  # cache_key -> {'min_dt': datetime, 'max_dt': datetime, 'count': int}

        # Convert pandas_data to polars if provided
        if pandas_data:
            for asset, df in pandas_data.items():
                if not isinstance(df, pl.DataFrame):
                    # Convert pandas to polars
                    if hasattr(df, 'index') and hasattr(df.index, 'name'):
                        pl_df = pl.from_pandas(df.reset_index())
                    else:
                        pl_df = pl.from_pandas(df)
                    self._store_data(asset, pl_df)
                else:
                    self._store_data(asset, df)

    def _to_naive_datetime(self, dt):
        """Convert datetime to naive (no timezone) for consistent comparisons."""
        if dt is None:
            return None
        if hasattr(dt, 'tzinfo') and dt.tzinfo is not None:
            return dt.replace(tzinfo=None)
        return dt

    def _store_data(self, asset, data):
        """Store data efficiently using lazy frames."""
        # Standardize column names
        rename_map = {
            "Open": "open", "High": "high", "Low": "low", "Close": "close",
            "Volume": "volume", "Dividends": "dividend", "Stock Splits": "stock_splits",
            "Adj Close": "adj_close", "index": "datetime", "Date": "datetime"
        }

        existing_renames = {k: v for k, v in rename_map.items() if k in data.columns}
        if existing_renames:
            data = data.rename(existing_renames)

        # Use lazy evaluation
        lazy_data = data.lazy()

        # Store lazy frame
        self._data_store[asset] = lazy_data

        # DON'T cache eager version - collect on demand instead for memory efficiency
        # Remove this line: self._eager_cache[asset] = lazy_data.collect()

        # Cache column indices from schema without collecting
        try:
            schema = lazy_data.collect_schema()
            self._column_indices[asset] = {col: i for i, col in enumerate(schema.names())}
        except:
            # Fallback: collect a tiny sample for column info
            sample = lazy_data.limit(1).collect()
            self._column_indices[asset] = {col: i for i, col in enumerate(sample.columns)}

        # Enforce storage limit
        self._enforce_storage_limit(self._data_store)

        return lazy_data

    def _enforce_storage_limit(self, data_store):
        """Enforce storage limit by removing least recently used data."""
        if not self.MAX_STORAGE_BYTES:
            return

        # Estimate storage without collecting
        estimated_storage = 0
        items_with_sizes = []

        for asset, lazy_df in data_store.items():
            try:
                # Estimate size without collecting
                schema = lazy_df.collect_schema()
                # Rough estimate: 8 bytes per numeric value, 50 bytes per string
                bytes_per_row = sum(8 if str(dtype).startswith('Float') or str(dtype).startswith('Int')
                                  else 50 for dtype in schema.dtypes())

                # Try to get row count without full collect
                estimated_rows = 10000  # Default estimate
                if asset in self._filtered_data_cache:
                    # Use cached data to estimate
                    for key in self._filtered_data_cache:
                        if key[0] == asset:
                            estimated_rows = len(self._filtered_data_cache[key])
                            break

                estimated_bytes = bytes_per_row * estimated_rows
                estimated_storage += estimated_bytes
                items_with_sizes.append((asset, estimated_bytes))
            except:
                # If estimation fails, use default
                items_with_sizes.append((asset, 100000))  # 100KB default

        logger.debug(f"Estimated storage: {estimated_storage:,} bytes for {len(data_store)} items")

        # Remove items if over limit
        if estimated_storage > self.MAX_STORAGE_BYTES:
            # Sort by size and remove largest first
            items_with_sizes.sort(key=lambda x: x[1], reverse=True)
            for asset, _ in items_with_sizes[:len(items_with_sizes)//2]:
                if asset in data_store:
                    del data_store[asset]
                if asset in self._eager_cache:
                    del self._eager_cache[asset]
                if asset in self._column_indices:
                    del self._column_indices[asset]
                if asset in self._filtered_data_cache:
                    # Clear related cache entries
                    keys_to_remove = [k for k in self._filtered_data_cache if k[0] == asset]
                    for k in keys_to_remove:
                        del self._filtered_data_cache[k]
                logger.debug(f"Storage limit exceeded. Evicted data for {asset}")

    def _convert_to_polars(self, df, asset=None):
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
        asset,
        length,
        timestep="minute",
        timeshift=None,
        quote=None,
        exchange=None,
        include_after_hours=True,
        return_polars=False,
    ):
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
        logger.debug(f"[get_historical_prices] Getting historical prices for {asset.symbol}, length={length}, timestep={timestep}, current_dt={self.get_datetime()}")

        # Validate asset type
        supported_asset_types = [Asset.AssetType.FUTURE, Asset.AssetType.CONT_FUTURE]
        if asset.asset_type not in supported_asset_types:
            error_msg = f"DataBento only supports futures assets. Received '{asset.asset_type}' for '{asset.symbol}'"
            logger.error(error_msg)
            raise ValueError(error_msg)

        # Check cached data first
        cache_key = (asset, timestep)
        current_dt = self.get_datetime()
        current_dt_naive = self._to_naive_datetime(current_dt)

        if cache_key in self._filtered_data_cache:
            cached_df = self._filtered_data_cache[cache_key]

            # Quick check if cached data covers the current time range
            if 'datetime' in cached_df.columns:
                # Ensure datetime type for fast operations
                if cached_df['datetime'].dtype != pl.Datetime:
                    cached_df = cached_df.with_columns(pl.col('datetime').cast(pl.Datetime))

                # Check cache validity using metadata (much faster than collecting)
                if cache_key in self._cache_metadata:
                    max_cached_dt = self._cache_metadata[cache_key]['max_dt']
                else:
                    # First time - compute and store metadata
                    max_cached_dt = cached_df.lazy().select(pl.col('datetime').max()).collect().item()
                    min_cached_dt = cached_df.lazy().select(pl.col('datetime').min()).collect().item()
                    self._cache_metadata[cache_key] = {
                        'min_dt': min_cached_dt,
                        'max_dt': max_cached_dt,
                        'count': len(cached_df)
                    }

                # Check if we need more data
                # Add a small buffer (1 hour) to avoid edge cases
                if max_cached_dt < current_dt_naive + timedelta(hours=1):
                    logger.debug(f"Cache miss: current time {current_dt_naive} near end of cached data (max: {max_cached_dt})")
                    # Don't delete, just skip cache to fetch extended data
                    pass
                else:
                    # Cache is valid, chain operations for single collection
                    df_result = (
                        cached_df.lazy()
                        .filter(pl.col('datetime') <= current_dt_naive)
                        .tail(length)
                        .collect()
                    )

                    # Check if we have enough bars
                    if len(df_result) >= length:

                        logger.debug(f"Cache hit: Using cached data for {asset.symbol}: {len(df_result)} bars")
                        return Bars(
                            df=df_result,
                            source=self.SOURCE,
                            asset=asset,
                            quote=quote,
                            return_polars=return_polars
                        )

        # Calculate date range for data retrieval
        current_dt = self.get_datetime()
        current_dt = self._to_naive_datetime(current_dt)

        # Apply timeshift if specified
        if timeshift:
            current_dt = current_dt - timeshift

        # Calculate start date based on length and timestep
        # Add larger future buffer for aggressive caching and fewer API calls
        if timestep == "day":
            buffer_days = max(10, length // 2)
            start_dt = current_dt - timedelta(days=length + buffer_days)
            # Fetch way ahead for caching (entire backtest period if possible)
            future_end = self.datetime_end
            if future_end.tzinfo is not None:
                future_end = future_end.replace(tzinfo=None)
            end_dt = min(current_dt + timedelta(days=365), future_end)
        elif timestep == "hour":
            buffer_hours = max(24, length // 2)
            start_dt = current_dt - timedelta(hours=length + buffer_hours)
            # Fetch 30 days ahead for caching
            future_end = self.datetime_end
            if future_end.tzinfo is not None:
                future_end = future_end.replace(tzinfo=None)
            end_dt = min(current_dt + timedelta(days=30), future_end)
        else:  # minute
            buffer_minutes = max(720, length + 100)  # Reduced buffer for speed
            start_dt = current_dt - timedelta(minutes=buffer_minutes)
            # Fetch 3 days ahead for minute data caching (balance between cache hits and data size)
            future_end = self.datetime_end
            if future_end.tzinfo is not None:
                future_end = future_end.replace(tzinfo=None)
            end_dt = min(current_dt + timedelta(days=3), future_end)

        # Ensure dates are timezone-naive
        if start_dt.tzinfo is not None:
            start_dt = start_dt.replace(tzinfo=None)
        if end_dt.tzinfo is not None:
            end_dt = end_dt.replace(tzinfo=None)

        # Ensure valid date range
        if start_dt >= end_dt:
            if timestep == "day":
                end_dt = start_dt + timedelta(days=max(1, length))
            elif timestep == "hour":
                end_dt = start_dt + timedelta(hours=max(1, length))
            else:
                end_dt = start_dt + timedelta(minutes=max(1, length))

        # Get data from DataBento
        logger.debug(f"[get_historical_prices] Requesting DataBento data for {asset.symbol} from {start_dt} to {end_dt}")

        try:
            # Get polars DataFrame directly
            logger.debug(f"[get_historical_prices] Calling databento_helper_polars with api_key={bool(self._api_key)}, timestep={timestep}")
            df = databento_helper_polars.get_price_data_from_databento_polars(
                api_key=self._api_key,
                asset=asset,
                start=start_dt,
                end=end_dt,
                timestep=timestep,
                venue=exchange
            )

            if df is None:
                logger.error(f"[get_historical_prices] No data returned from DataBento for {asset.symbol} - df is None")
                return None
            elif df.is_empty():
                logger.error(f"[get_historical_prices] No data returned from DataBento for {asset.symbol} - df is empty")
                return None
            else:
                logger.debug(f"[get_historical_prices] Got {len(df)} rows from DataBento for {asset.symbol}")
                logger.debug(f"[get_historical_prices] DataBento returned columns: {df.columns}")
                logger.debug(f"[get_historical_prices] Has datetime? {'datetime' in df.columns}")

            # Store in cache - merge with existing if we have it
            if self.enable_cache:
                if cache_key in self._filtered_data_cache:
                    # Merge new data with existing cache
                    existing_df = self._filtered_data_cache[cache_key]
                    # Combine and deduplicate based on datetime
                    combined_df = pl.concat([existing_df, df]).unique(subset=['datetime']).sort('datetime')
                    self._filtered_data_cache[cache_key] = combined_df

                    # Update metadata
                    self._cache_metadata[cache_key] = {
                        'min_dt': combined_df['datetime'].min(),
                        'max_dt': combined_df['datetime'].max(),
                        'count': len(combined_df)
                    }
                    logger.debug(f"Merged cache: {len(existing_df)} + {len(df)} -> {len(combined_df)} rows")
                else:
                    self._filtered_data_cache[cache_key] = df
                    # Store initial metadata
                    self._cache_metadata[cache_key] = {
                        'min_dt': df['datetime'].min(),
                        'max_dt': df['datetime'].max(),
                        'count': len(df)
                    }
                    logger.debug(f"New cache entry: {len(df)} rows")
                self._store_data(asset, df)

            # Filter data to current backtesting time
            if 'datetime' in df.columns:
                if df['datetime'].dtype != pl.Datetime:
                    df = df.with_columns(pl.col('datetime').cast(pl.Datetime))

                # Ensure both datetimes are timezone-naive for comparison
                if current_dt.tzinfo is not None:
                    current_dt_naive = current_dt.replace(tzinfo=None)
                else:
                    current_dt_naive = current_dt

                df_filtered = df.filter(pl.col('datetime') <= current_dt_naive)
            else:
                df_filtered = df

            # Debug - check columns before tail
            logger.debug(f"[get_historical_prices] df_filtered columns before tail: {df_filtered.columns}")
            logger.debug(f"[get_historical_prices] df_filtered shape: {df_filtered.shape}")

            # Take the last 'length' bars
            df_result = df_filtered.tail(length)

            # Debug - check columns after tail
            logger.debug(f"[get_historical_prices] df_result columns after tail: {df_result.columns}")
            logger.debug(f"[get_historical_prices] df_result shape: {df_result.shape}")

            if df_result.is_empty():
                logger.warning(f"No data available for {asset.symbol} up to {current_dt}")
                return None

            # Ensure datetime column is preserved
            if 'datetime' not in df_result.columns and 'datetime' in df_filtered.columns:
                logger.warning("[get_historical_prices] datetime column was dropped by tail()! This is a bug.")
                # Try to get it back from df_filtered
                df_result = df_filtered.tail(length).select(df_filtered.columns)

            # Create and return Bars object
            logger.debug(f"[get_historical_prices] Creating Bars with df_result columns: {df_result.columns}")
            logger.debug(f"[get_historical_prices] df_result has datetime? {'datetime' in df_result.columns}")

            bars = Bars(
                df=df_result,
                source=self.SOURCE,
                asset=asset,
                quote=quote,
                return_polars=return_polars
            )

            logger.debug(f"Retrieved {len(df_result)} bars for {asset.symbol}")
            return bars

        except Exception as e:
            logger.error(f"Error getting data from DataBento for {asset.symbol}: {e}")
            return None

    def get_last_price(self, asset, quote=None, exchange=None):
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
        float or None
            Last known price of the asset
        """
        # Check cache first
        cache_key = (asset, self.get_datetime())
        if cache_key in self._last_price_cache:
            cached_price = self._last_price_cache[cache_key]
            logger.debug(f"Using cached last price for {asset.symbol}: {cached_price}")
            return cached_price

        logger.debug(f"Getting last price for {asset.symbol}")

        # Try to get from lazy data first (more memory efficient)
        if asset in self._data_store:
            lazy_df = self._data_store[asset]

            # Get current time for filtering
            current_dt = self.get_datetime()

            # Make timezone-naive for comparison
            if current_dt.tzinfo is not None:
                current_dt_naive = current_dt.replace(tzinfo=None)
            else:
                current_dt_naive = current_dt

            # Get last price with single lazy operation
            try:
                last_price = (
                    lazy_df
                    .filter(pl.col('datetime') <= current_dt_naive)
                    .select(pl.col('close').tail(1))
                    .collect()
                    .item()
                )

                if last_price is not None:
                    last_price = float(last_price)
                    cache_key = (asset, self.get_datetime())
                    self._last_price_cache[cache_key] = last_price
                    logger.debug(f"Last price from lazy data for {asset.symbol}: {last_price}")
                    return last_price
            except:
                pass  # Fall back to historical prices

        # Fall back to getting historical prices
        bars = self.get_historical_prices(asset, 1, "minute", exchange=exchange)
        if bars and not bars.empty:
            # Get the last close price - handle both index types
            df = bars.df
            if 'close' in df.columns:
                last_price = float(df['close'].iloc[-1])
                self._last_price_cache[asset] = last_price
                logger.debug(f"Last price from historical for {asset.symbol}: {last_price}")
                return last_price

        logger.warning(f"No last price available for {asset.symbol}")
        return None

    def get_chains(self, asset, quote=None):
        """DataBento doesn't provide options chain data"""
        logger.warning("DataBento does not provide options chain data")
        return {}

    def get_quote(self, asset, quote=None):
        """Get current quote for an asset"""
        return self.get_last_price(asset, quote=quote)

    def clear_cache(self):
        """Clear all cached data to free memory"""
        self._data_store.clear()
        self._eager_cache.clear()
        self._column_indices.clear()
        self._filtered_data_cache.clear()
        self._last_price_cache.clear()
        logger.info("Cleared all DataBento data caches")

    def _pull_source_symbol_bars(
        self,
        asset,
        length,
        timestep="",
        timeshift=0,
        quote=None,
        exchange=None,
        include_after_hours=True,
    ):
        """
        Pull historical bars from DataBento data source.
        
        This is the critical method that the backtesting framework calls to get data.
        It must return a pandas DataFrame for compatibility with the backtesting engine.
        
        Parameters
        ----------
        asset : Asset
            The asset to get data for
        length : int
            Number of bars to retrieve
        timestep : str
            Timestep for the data ('minute', 'hour', 'day')
        timeshift : int
            Minutes to shift back in time
        quote : Asset, optional
            Quote asset (not used for DataBento)
        exchange : str, optional
            Exchange/venue filter
        include_after_hours : bool
            Whether to include after-hours data
            
        Returns
        -------
        pandas.DataFrame
            Historical price data with datetime index
        """
        timestep = timestep if timestep else "minute"

        logger.debug(f"[_pull_source_symbol_bars] Called with asset={asset.symbol}, length={length}, timestep={timestep}, timeshift={timeshift}")

        # Get historical prices using our existing method
        bars = self.get_historical_prices(
            asset=asset,
            length=length,
            timestep=timestep,
            timeshift=timedelta(minutes=timeshift) if timeshift else None,
            quote=quote,
            exchange=exchange,
            include_after_hours=include_after_hours
        )

        if bars is None:
            logger.warning(f"[_pull_source_symbol_bars] bars is None for {asset.symbol}")
            return None

        if bars.empty:
            logger.warning(f"[_pull_source_symbol_bars] bars is empty for {asset.symbol}")
            return None

        # Return the pandas DataFrame from the Bars object
        # The Bars.df property already converts to pandas when accessed
        result_df = bars.df
        logger.debug(f"[_pull_source_symbol_bars] Returning DataFrame with shape {result_df.shape} for {asset.symbol}")
        if not result_df.empty:
            logger.debug(f"[_pull_source_symbol_bars] DataFrame columns: {result_df.columns.tolist()}")
            logger.debug(f"[_pull_source_symbol_bars] First row: {result_df.iloc[0].to_dict() if len(result_df) > 0 else 'N/A'}")
            logger.debug(f"[_pull_source_symbol_bars] Last row: {result_df.iloc[-1].to_dict() if len(result_df) > 0 else 'N/A'}")
        return result_df
