"""Ultra-optimized DataBento backtesting using pure polars"""

from datetime import timedelta

import polars as pl
from polars.datatypes import Datetime as PlDatetime
import pytz

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

    def _ensure_strategy_timezone(self, df: pl.DataFrame, column: str = "datetime") -> pl.DataFrame:
        """Ensure dataframe datetime column aligns with the strategy timezone."""
        if df is None or column not in df.columns:
            return df

        dtype = df.schema.get(column)
        strategy_tz = self.tzinfo.zone if hasattr(self.tzinfo, "zone") else str(self.tzinfo)
        expr = pl.col(column)

        if isinstance(dtype, PlDatetime):
            if dtype.time_zone is None:
                expr = expr.dt.replace_time_zone(strategy_tz)
            elif dtype.time_zone != strategy_tz:
                expr = expr.dt.convert_time_zone(strategy_tz)
        else:
            expr = expr.cast(pl.Datetime(time_unit="ns")).dt.replace_time_zone(strategy_tz)

        return df.with_columns(expr.alias(column))

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

        data = self._ensure_strategy_timezone(data)

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
        logger.info(
            "[get_historical_prices] Getting historical prices for %s, length=%s, timestep=%s, current_dt=%s, datetime_start=%s",
            asset.symbol,
            length,
            timestep,
            self.get_datetime(),
            self.datetime_start,
        )

        supported_asset_types = [Asset.AssetType.FUTURE, Asset.AssetType.CONT_FUTURE]
        if asset.asset_type not in supported_asset_types:
            error_msg = (
                f"DataBento only supports futures assets. Received '{asset.asset_type}' for '{asset.symbol}'"
            )
            logger.error(error_msg)
            raise ValueError(error_msg)

        cache_key = (asset, timestep)

        current_dt = self.get_datetime()
        if current_dt.tzinfo is None:
            current_dt = self.tzinfo.localize(current_dt)

        effective_dt = current_dt
        if timeshift:
            if isinstance(timeshift, int):
                effective_dt = effective_dt - timedelta(minutes=timeshift)
            else:
                effective_dt = effective_dt - timeshift

        current_dt_utc = effective_dt.astimezone(pytz.UTC)
        current_dt_naive_utc = current_dt_utc.replace(tzinfo=None)

        future_end = self.datetime_end
        if future_end.tzinfo is None:
            future_end = self.tzinfo.localize(future_end)
        future_end_naive = future_end.astimezone(pytz.UTC).replace(tzinfo=None)

        earliest_start = self.datetime_start
        if earliest_start.tzinfo is None:
            earliest_start = self.tzinfo.localize(earliest_start)
        earliest_start_naive = earliest_start.astimezone(pytz.UTC).replace(tzinfo=None)

        if timestep == "day":
            buffer_days = max(10, length // 2)
            dynamic_start = current_dt_naive_utc - timedelta(days=length + buffer_days)
            start_dt = min(dynamic_start, earliest_start_naive - timedelta(days=buffer_days))
            end_dt = future_end_naive
            coverage_buffer = timedelta(days=2)
            bar_delta = timedelta(days=1)
        elif timestep == "hour":
            buffer_hours = max(24, length // 2)
            start_dt = current_dt_naive_utc - timedelta(hours=length + buffer_hours)
            end_dt = min(current_dt_naive_utc + timedelta(days=30), future_end_naive)
            coverage_buffer = timedelta(hours=6)
            bar_delta = timedelta(hours=1)
        else:
            buffer_minutes = max(720, length + 100)
            start_dt = current_dt_naive_utc - timedelta(minutes=buffer_minutes)
            end_dt = min(current_dt_naive_utc + timedelta(days=3), future_end_naive)
            coverage_buffer = timedelta(minutes=30)
            bar_delta = timedelta(minutes=1)

        start_dt = self._to_naive_datetime(start_dt)
        end_dt = self._to_naive_datetime(end_dt)

        # Guarantee the requested window spans at least a full bar to avoid inverted ranges
        min_required_end = start_dt + bar_delta
        if end_dt <= start_dt:
            end_dt = min_required_end
        elif end_dt < min_required_end:
            end_dt = min_required_end

        cached_df = None
        coverage_ok = False
        if cache_key in self._filtered_data_cache:
            cached_df = self._ensure_strategy_timezone(self._filtered_data_cache[cache_key])
            self._filtered_data_cache[cache_key] = cached_df

            metadata = self._cache_metadata.get(cache_key)
            if metadata:
                cached_min = self._to_naive_datetime(metadata.get("min_dt"))
                cached_max = self._to_naive_datetime(metadata.get("max_dt"))
            else:
                cached_min = cached_df.lazy().select(pl.col("datetime").min()).collect().item()
                cached_max = cached_df.lazy().select(pl.col("datetime").max()).collect().item()
                cached_min = self._to_naive_datetime(cached_min)
                cached_max = self._to_naive_datetime(cached_max)
                self._cache_metadata[cache_key] = {
                    "min_dt": cached_min,
                    "max_dt": cached_max,
                    "count": cached_df.height,
                }

            if cached_min is not None and cached_max is not None:
                coverage_ok = cached_min <= start_dt and cached_max >= (end_dt - coverage_buffer)

            logger.debug(
                "[get_historical_prices] cache window for %s (%s): min=%s max=%s required=[%s, %s] buffer=%s",
                asset.symbol,
                timestep,
                cached_min,
                cached_max,
                start_dt,
                end_dt,
                coverage_buffer,
            )

            if coverage_ok:
                allow_current_bar = getattr(self, "_include_current_bar_for_orders", False)
                if isinstance(timeshift, int) and timeshift > 0:
                    allow_current_bar = True
                elif isinstance(timeshift, timedelta) and timeshift.total_seconds() > 0:
                    allow_current_bar = True

                cutoff_dt = effective_dt if allow_current_bar else effective_dt - bar_delta

                df_result = (
                    cached_df.lazy()
                    .filter(pl.col("datetime") <= pl.lit(cutoff_dt))
                    .sort("datetime")
                    .tail(length)
                    .collect()
                )

                if df_result.height >= length:
                    return Bars(
                        df=df_result,
                        source=self.SOURCE,
                        asset=asset,
                        quote=quote,
                        return_polars=return_polars,
                    )
            else:
                logger.debug(
                    "Cache coverage insufficient for %s (%s); requesting additional data.",
                    asset.symbol,
                    timestep,
                )

        logger.debug(
            "[get_historical_prices] Requesting DataBento data for %s from %s to %s",
            asset.symbol,
            start_dt,
            end_dt,
        )

        try:
            df = databento_helper_polars.get_price_data_from_databento_polars(
                api_key=self._api_key,
                asset=asset,
                start=start_dt,
                end=end_dt,
                timestep=timestep,
                venue=exchange,
                reference_date=effective_dt,
            )

            if df is None:
                logger.error(
                    "[get_historical_prices] No data returned from DataBento for %s - df is None",
                    asset.symbol,
                )
                return None
            if df.is_empty():
                logger.error(
                    "[get_historical_prices] No data returned from DataBento for %s - df is empty",
                    asset.symbol,
                )
                return None

            df = self._ensure_strategy_timezone(df)

            if self.enable_cache:
                if cached_df is not None:
                    combined_df = pl.concat([cached_df, df], how="vertical", rechunk=True)
                    combined_df = combined_df.unique(subset=["datetime"]).sort("datetime")
                else:
                    combined_df = df

                self._filtered_data_cache[cache_key] = combined_df

                cache_min = combined_df.lazy().select(pl.col("datetime").min()).collect().item()
                cache_max = combined_df.lazy().select(pl.col("datetime").max()).collect().item()
                cache_min = self._to_naive_datetime(cache_min)
                cache_max = self._to_naive_datetime(cache_max)
                self._cache_metadata[cache_key] = {
                    "min_dt": cache_min,
                    "max_dt": cache_max,
                    "count": combined_df.height,
                }
                df_to_use = combined_df
            else:
                df_to_use = df

            allow_current_bar = getattr(self, "_include_current_bar_for_orders", False)
            if isinstance(timeshift, int) and timeshift > 0:
                allow_current_bar = True
            elif isinstance(timeshift, timedelta) and timeshift.total_seconds() > 0:
                allow_current_bar = True

            cutoff_dt_api = effective_dt if allow_current_bar else effective_dt - bar_delta

            df_result = (
                df_to_use.lazy()
                .filter(pl.col("datetime") <= pl.lit(cutoff_dt_api))
                .sort("datetime")
                .tail(length)
                .collect()
            )

            if df_result.is_empty():
                logger.warning(
                    "No data available for %s up to %s",
                    asset.symbol,
                    effective_dt,
                )
                return None

            return Bars(
                df=df_result,
                source=self.SOURCE,
                asset=asset,
                quote=quote,
                return_polars=return_polars,
                tzinfo=self.tzinfo,
            )

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
                cutoff_dt_lp = current_dt_naive - timedelta(minutes=1)
                last_price = (
                    lazy_df
                    .filter(pl.col('datetime') <= pl.lit(cutoff_dt_lp))
                    .select(pl.col('close').tail(1))
                    .collect()
                    .item()
                )

                if last_price is not None:
                    last_price = float(last_price)
                    cache_key = (asset, self.get_datetime())
                    self._last_price_cache[asset] = last_price
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
                cache_key = (asset, self.get_datetime())
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
