"""Mixin class for shared polars functionality across data sources.

This mixin provides common polars operations without disrupting inheritance.
"""

from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import polars as pl

from lumibot.entities import Asset, Bars
from lumibot.tools.lumibot_logger import get_logger

logger = get_logger(__name__)

# DEBUG-LOG: Always-on debug logging (will be removed after debugging is complete)
_THETA_PARITY_DEBUG = False


class PolarsMixin:
    """Mixin for polars-based data sources with common functionality."""

    def _init_polars_storage(self):
        """Initialize common polars data storage structures."""
        self._data_store: Dict[Asset, pl.LazyFrame] = {}
        self._last_price_cache = {}
        self._cache_datetime = None
        self._filtered_data_cache: Dict[tuple, pl.DataFrame] = {}
        self._column_indices: Dict[Asset, Dict[str, int]] = {}
        self._cache_date = None

    def _store_data_polars(self, asset: Asset, df: pl.DataFrame, rename_columns: bool = True) -> pl.LazyFrame:
        """Store data as lazy frame with standardized column names.
        
        Parameters
        ----------
        asset : Asset
            The asset to store data for
        df : pl.DataFrame
            The dataframe to store
        rename_columns : bool
            Whether to rename columns to standard names
        """
        if df is None or df.is_empty():
            return None

        if rename_columns:
            # Standardized column mapping
            rename_map = {
                "Open": "open", "High": "high", "Low": "low", "Close": "close",
                "Volume": "volume", "Dividends": "dividend", "Stock Splits": "stock_splits",
                "Adj Close": "adj_close", "index": "datetime", "Date": "datetime",
                "Datetime": "datetime", "timestamp": "datetime", "time": "datetime",
                "o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"
            }

            # Apply renaming
            existing_renames = {k: v for k, v in rename_map.items() if k in df.columns}
            if existing_renames:
                df = df.rename(existing_renames)

        # Ensure datetime column exists and is properly typed
        if "datetime" in df.columns:
            if df["datetime"].dtype != pl.Datetime:
                df = df.with_columns(pl.col("datetime").cast(pl.Datetime("us")))

        # Store as lazy frame
        lazy_data = df.lazy()
        self._data_store[asset] = lazy_data

        # Cache column indices for fast access
        self._column_indices[asset] = {col: i for i, col in enumerate(df.columns)}

        return lazy_data

    def _get_data_lazy(self, asset: Asset) -> Optional[pl.LazyFrame]:
        """Get lazy frame for asset.

        Parameters
        ----------
        asset : Asset or tuple
            The asset to get data for (can be a tuple of (asset, quote))

        Returns
        -------
        Optional[pl.LazyFrame]
            The lazy frame or None if not found
        """
        # CRITICAL FIX: Handle both Asset and (Asset, quote) tuple keys
        # The data store uses tuple keys (asset, quote), so we need to look up by that key
        return self._data_store.get(asset)

    def _parse_source_symbol_bars_polars(
        self,
        response: pl.DataFrame,
        asset: Asset,
        source: str,
        quote: Optional[Asset] = None,
        length: Optional[int] = None,
        return_polars: bool = False
    ) -> Bars:
        """Parse bars from polars DataFrame.

        Parameters
        ----------
        response : pl.DataFrame
            The response data
        asset : Asset
            The asset
        source : str
            The data source name
        quote : Optional[Asset]
            The quote asset for forex/crypto
        length : Optional[int]
            Limit the number of bars

        Returns
        -------
        Bars
            Parsed bars object
        """
        # DEBUG-LOG: Entry with response details
        if _THETA_PARITY_DEBUG:
            logger.debug(
                "[POLARS_MIXIN][PARSE][ENTRY] asset=%s source=%s response_type=%s response_is_none=%s response_shape=%s return_polars=%s",
                getattr(asset, 'symbol', asset),
                source,
                type(response).__name__,
                response is None,
                (response.height, len(response.columns)) if response is not None and hasattr(response, 'height') else 'NO_SHAPE',
                return_polars
            )

        # DEBUG-LOG: Check for empty response
        if response is None or response.is_empty():
            if _THETA_PARITY_DEBUG:
                logger.warning(
                    "[POLARS_MIXIN][PARSE][EMPTY_INPUT] asset=%s source=%s response_is_none=%s response_is_empty=%s returning_empty_bars=True",
                    getattr(asset, 'symbol', asset),
                    source,
                    response is None,
                    response.is_empty() if response is not None else 'N/A'
                )
            return Bars(response, source, asset, raw=response)

        # Limit length if specified
        # DEBUG-LOG: Length limiting
        if length and response.height > length:
            if _THETA_PARITY_DEBUG:
                logger.debug(
                    "[POLARS_MIXIN][PARSE][BEFORE_LENGTH_LIMIT] asset=%s source=%s height=%s length=%s will_truncate=True",
                    getattr(asset, 'symbol', asset),
                    source,
                    response.height,
                    length
                )
            response = response.tail(length)
            if _THETA_PARITY_DEBUG:
                logger.debug(
                    "[POLARS_MIXIN][PARSE][AFTER_LENGTH_LIMIT] asset=%s source=%s new_height=%s",
                    getattr(asset, 'symbol', asset),
                    source,
                    response.height
                )

        # Filter to only keep OHLCV + datetime columns (remove DataBento metadata like rtype, publisher_id, etc.)
        # Required columns for strategies
        required_cols = ['open', 'high', 'low', 'close', 'volume']
        optional_cols = [
            'datetime',
            'timestamp',
            'date',
            'time',
            'dividend',
            'stock_splits',
            'symbol',
            'bid',
            'ask',
            'bid_size',
            'ask_size',
            'bid_condition',
            'ask_condition',
            'bid_exchange',
            'ask_exchange',
            'missing',
        ]

        # Determine which columns to keep
        keep_cols = []
        for col in response.columns:
            if col in required_cols or col in optional_cols:
                keep_cols.append(col)

        # Select only the relevant columns
        if keep_cols:
            response = response.select(keep_cols)

        # DEBUG-LOG: Columns after selection
        if _THETA_PARITY_DEBUG:
            logger.debug(
                "[POLARS_MIXIN][PARSE][AFTER_COLUMN_SELECT] asset=%s source=%s shape=%s columns=%s has_datetime=%s has_missing=%s",
                getattr(asset, 'symbol', asset),
                source,
                (response.height, len(response.columns)),
                response.columns,
                'datetime' in response.columns,
                'missing' in response.columns
            )

        # Create bars object
        tzinfo = getattr(self, "tzinfo", None)
        if (
            tzinfo is not None
            and isinstance(response, pl.DataFrame)
            and "datetime" in response.columns
        ):
            target_tz = getattr(tzinfo, "zone", None) or getattr(tzinfo, "key", None)
            if target_tz:
                current_dtype = response.schema.get("datetime")
                if hasattr(current_dtype, "time_zone"):
                    current_tz = current_dtype.time_zone
                else:
                    current_tz = None
                if current_tz != target_tz:
                    datetime_col = pl.col("datetime")
                    if current_tz is None:
                        response = response.with_columns(
                            datetime_col.dt.replace_time_zone(target_tz)
                        )
                    else:
                        response = response.with_columns(
                            datetime_col.dt.convert_time_zone(target_tz)
                        )

        # DEBUG-LOG: Creating Bars object
        if _THETA_PARITY_DEBUG:
            sample_data = {}
            for col in ['open', 'high', 'low', 'close', 'volume', 'missing']:
                if col in response.columns:
                    try:
                        sample_data[col] = response[col][:3].to_list()
                    except Exception:
                        sample_data[col] = 'ERROR'
            logger.debug(
                "[POLARS_MIXIN][PARSE][BEFORE_BARS] asset=%s source=%s response_type=%s response_shape=%s return_polars=%s sample_data=%s",
                getattr(asset, 'symbol', asset),
                source,
                type(response).__name__,
                (response.height, len(response.columns)),
                return_polars,
                sample_data
            )

        bars = Bars(
            response,
            source,
            asset,
            raw=response,
            quote=quote,
            return_polars=return_polars,
            tzinfo=tzinfo,
        )

        # DEBUG-LOG: Bars object created
        if _THETA_PARITY_DEBUG:
            logger.debug(
                "[POLARS_MIXIN][PARSE][AFTER_BARS] asset=%s source=%s bars_type=%s bars._df_type=%s bars._df_shape=%s bars._return_polars=%s",
                getattr(asset, 'symbol', asset),
                source,
                type(bars).__name__,
                type(bars._df).__name__ if hasattr(bars, '_df') else 'NO_DF',
                (bars._df.height, len(bars._df.columns)) if hasattr(bars, '_df') and hasattr(bars._df, 'height') else bars._df.shape if hasattr(bars, '_df') and hasattr(bars._df, 'shape') else 'NO_SHAPE',
                bars._return_polars if hasattr(bars, '_return_polars') else 'NO_ATTR'
            )

        return bars

    def _clear_cache_polars(self, asset: Optional[Asset] = None):
        """Clear cached data.
        
        Parameters
        ----------
        asset : Optional[Asset]
            Specific asset to clear, or None to clear all
        """
        if asset:
            # Clear specific asset
            if asset in self._last_price_cache:
                del self._last_price_cache[asset]
            if asset in self._filtered_data_cache:
                keys_to_remove = [k for k in self._filtered_data_cache if k[0] == asset]
                for key in keys_to_remove:
                    del self._filtered_data_cache[key]
        else:
            # Clear all caches
            self._last_price_cache.clear()
            self._filtered_data_cache.clear()
            self._cache_datetime = None
            self._cache_date = None

    def _get_cached_last_price_polars(self, asset: Asset, current_dt: datetime, timestep: str = "minute") -> Optional[float]:
        """Get last price from cache if valid.
        
        Parameters
        ----------
        asset : Asset
            The asset to get price for
        current_dt : datetime
            Current datetime for cache validation
        timestep : str
            The timestep (for cache key generation)
            
        Returns
        -------
        Optional[float]
            Cached price or None if not valid
        """
        # Build cache key based on timestep
        current_date = current_dt.date() if hasattr(current_dt, 'date') else current_dt

        if timestep == "day":
            cache_key = (asset, timestep, None, None, current_date)
        else:
            cache_key = (asset, timestep, None, None, current_dt)

        # Check if we need to clear cache
        if timestep == "day" and hasattr(self, '_cache_date') and self._cache_date != current_date:
            self._last_price_cache.clear()
            self._cache_date = current_date
        elif timestep != "day" and self._cache_datetime != current_dt:
            self._last_price_cache.clear()
            self._cache_datetime = current_dt

        return self._last_price_cache.get(cache_key)

    def _cache_last_price_polars(self, asset: Asset, price: float, current_dt: datetime, timestep: str = "minute"):
        """Cache the last price for an asset.
        
        Parameters
        ----------
        asset : Asset
            The asset
        price : float
            The price to cache
        current_dt : datetime
            The datetime for this price
        timestep : str
            The timestep (for cache key generation)
        """
        current_date = current_dt.date() if hasattr(current_dt, 'date') else current_dt

        if timestep == "day":
            cache_key = (asset, timestep, None, None, current_date)
            self._cache_date = current_date
        else:
            cache_key = (asset, timestep, None, None, current_dt)
            self._cache_datetime = current_dt

        self._last_price_cache[cache_key] = price

    def _convert_datetime_for_filtering(self, dt: Any) -> datetime:
        """Convert datetime to naive UTC datetime for filtering.

        CRITICAL FIX: Must convert to UTC BEFORE stripping timezone!
        If we strip timezone from ET datetime, we lose 5 hours of data.

        Example:
        - Input: 2024-01-02 18:00:00-05:00 (ET)
        - Convert to UTC: 2024-01-02 23:00:00+00:00
        - Strip timezone: 2024-01-02 23:00:00 (naive UTC)

        OLD BUGGY CODE:
        - Input: 2024-01-02 18:00:00-05:00 (ET)
        - Strip timezone: 2024-01-02 18:00:00 (naive, loses timezone!)
        - Compare to cached data in naive UTC: WRONG by 5 hours!

        Parameters
        ----------
        dt : Any
            Datetime-like object

        Returns
        -------
        datetime
            Naive UTC datetime object
        """
        from datetime import timezone

        # First convert to UTC if timezone-aware
        if hasattr(dt, 'tzinfo') and dt.tzinfo is not None:
            # Convert to UTC
            dt_utc = dt.astimezone(timezone.utc)
            # Then strip timezone
            return dt_utc.replace(tzinfo=None)
        elif hasattr(dt, 'tz_localize'):
            # Pandas Timestamp
            return dt.tz_convert('UTC').tz_localize(None)
        elif hasattr(dt, 'replace'):
            # Already naive
            return dt
        else:
            return dt

    def _enforce_storage_limit_polars(self, max_memory: Optional[int] = None):
        """Enforce memory storage limit by removing oldest data.
        
        Parameters
        ----------
        max_memory : Optional[int]
            Maximum memory in bytes, or None for no limit
        """
        if not max_memory:
            return

        # Calculate current memory usage
        total_memory = 0
        asset_memory = {}

        for asset, lazy_frame in self._data_store.items():
            try:
                # Estimate memory from lazy frame schema
                schema = lazy_frame.collect_schema()
                estimated_rows = 10000  # Conservative estimate
                bytes_per_row = len(schema.names()) * 8
                asset_memory[asset] = estimated_rows * bytes_per_row
                total_memory += asset_memory[asset]
            except:
                continue

        # Remove data if over limit
        if total_memory > max_memory:
            # Sort by memory usage and remove largest first
            sorted_assets = sorted(asset_memory.items(), key=lambda x: x[1], reverse=True)

            for asset, memory in sorted_assets:
                if total_memory <= max_memory * 0.8:  # Keep 20% buffer
                    break

                # Remove asset data
                if asset in self._data_store:
                    del self._data_store[asset]
                if asset in self._column_indices:
                    del self._column_indices[asset]
                if asset in self._last_price_cache:
                    # Remove all cache entries for this asset
                    keys_to_remove = [k for k in self._last_price_cache.keys() if k[0] == asset]
                    for key in keys_to_remove:
                        del self._last_price_cache[key]

                total_memory -= memory
                logger.debug(f"Removed {asset.symbol} data to free memory")

    def _filter_data_polars(
        self,
        asset: Asset,
        lazy_data: pl.LazyFrame,
        end_filter: datetime,
        length: int,
        timestep: str = "minute",
        use_strict_less_than: bool = False
    ) -> Optional[pl.DataFrame]:
        """Filter data up to end_filter and return last length rows.

        Parameters
        ----------
        asset : Asset
            The asset (for caching)
        lazy_data : pl.LazyFrame
            The lazy frame to filter
        end_filter : datetime
            Filter data up to this datetime
        length : int
            Number of rows to return
        timestep : str
            Timestep for caching strategy
        use_strict_less_than : bool
            If True, use < instead of <= for filtering (matches Pandas behavior without timeshift)

        Returns
        -------
        Optional[pl.DataFrame]
            Filtered dataframe or None
        """
        # DEBUG-LOG: Filter entry with parameters
        if _THETA_PARITY_DEBUG:
            logger.debug(
                "[POLARS_MIXIN][FILTER][ENTRY] asset=%s end_filter=%s end_filter_tz=%s length=%s timestep=%s use_strict_less_than=%s",
                getattr(asset, 'symbol', asset),
                end_filter,
                end_filter.tzinfo if hasattr(end_filter, 'tzinfo') else 'N/A',
                length,
                timestep,
                use_strict_less_than
            )

        # DEBUG
        logger.debug(f"[POLARS FILTER] end_filter={end_filter}, tzinfo={end_filter.tzinfo if hasattr(end_filter, 'tzinfo') else 'N/A'}, length={length}")

        # Convert end_filter to naive
        end_filter_naive = self._convert_datetime_for_filtering(end_filter)

        # DEBUG-LOG: Naive end filter calculation
        if _THETA_PARITY_DEBUG:
            logger.debug(
                "[POLARS_MIXIN][FILTER][END_FILTER_NAIVE] asset=%s end_filter_naive=%s",
                getattr(asset, 'symbol', asset),
                end_filter_naive
            )

        # DEBUG
        logger.debug(f"[POLARS FILTER] end_filter_naive={end_filter_naive}")

        # Derive naive UTC end filter and compute matching start filter
        if timestep == "day":
            current_date = end_filter.date() if hasattr(end_filter, 'date') else end_filter
            cache_key = (asset, current_date, timestep)
        else:
            current_date = None
            cache_key = None

        # Determine datetime column name
        schema = lazy_data.collect_schema()
        dt_col = None
        for col_name in schema.names():
            if col_name in ['datetime', 'date', 'timestamp']:
                dt_col = col_name
                break

        if dt_col is None:
            logger.error("No datetime column found")
            return None

        dt_dtype = schema[dt_col]
        if hasattr(dt_dtype, 'time_zone') and dt_dtype.time_zone:
            import pytz
            df_tz = pytz.timezone(dt_dtype.time_zone)
            end_filter_with_tz = pytz.utc.localize(end_filter_naive).astimezone(df_tz)
        else:
            end_filter_with_tz = end_filter_naive

        start_filter_with_tz = None
        if length and length > 0:
            try:
                if hasattr(self, "get_start_datetime_and_ts_unit"):
                    start_candidate, _ = self.get_start_datetime_and_ts_unit(length, timestep, start_dt=end_filter)
                else:
                    delta, unit = self.convert_timestep_str_to_timedelta(timestep)
                    if unit == "day":
                        delta = timedelta(days=length)
                    else:
                        delta *= length
                    start_candidate = end_filter - delta
            except Exception:
                delta, unit = self.convert_timestep_str_to_timedelta(timestep)
                if unit == "day":
                    delta = timedelta(days=length)
                else:
                    delta *= length
                start_candidate = end_filter - delta

            start_naive = self._convert_datetime_for_filtering(start_candidate)
            if hasattr(dt_dtype, 'time_zone') and dt_dtype.time_zone:
                import pytz
                start_filter_with_tz = pytz.utc.localize(start_naive).astimezone(df_tz)
            else:
                start_filter_with_tz = start_naive

        if timestep == "day" and cache_key in self._filtered_data_cache:
            cached = self._filtered_data_cache[cache_key]
            if len(cached) >= length:
                return cached.tail(length)

        dt_time_zone = getattr(dt_dtype, "time_zone", None)
        target_dtype = pl.Datetime(time_unit="ns", time_zone=dt_time_zone)
        end_literal = pl.lit(end_filter_with_tz).cast(target_dtype)
        filter_expr = pl.col(dt_col) <= end_literal
        if start_filter_with_tz is not None:
            start_literal = pl.lit(start_filter_with_tz).cast(target_dtype)
            if use_strict_less_than:
                filter_expr = (pl.col(dt_col) < end_literal) & (pl.col(dt_col) >= start_literal)
            else:
                filter_expr = (pl.col(dt_col) <= end_literal) & (pl.col(dt_col) >= start_literal)
        elif use_strict_less_than:
            filter_expr = pl.col(dt_col) < end_literal

        # DEBUG-LOG: Before filtering with expression
        if _THETA_PARITY_DEBUG:
            logger.debug(
                "[POLARS_MIXIN][FILTER][BEFORE_FILTER_EXPR] asset=%s start_filter_with_tz=%s end_filter_with_tz=%s use_strict_less_than=%s dt_col=%s",
                getattr(asset, 'symbol', asset),
                start_filter_with_tz,
                end_filter_with_tz,
                use_strict_less_than,
                dt_col
            )

        result = (
            lazy_data
            .filter(filter_expr)
            .sort(dt_col)
            .unique(subset=[dt_col], keep='last', maintain_order=True)
            .collect()
        )

        # DEBUG-LOG: After filtering
        if _THETA_PARITY_DEBUG:
            logger.debug(
                "[POLARS_MIXIN][FILTER][AFTER_FILTER_EXPR] asset=%s result_shape=%s result_is_empty=%s",
                getattr(asset, 'symbol', asset),
                (result.height, len(result.columns)),
                result.is_empty()
            )

        if result.is_empty() and length and length > 0:
            # DEBUG-LOG: Fallback triggered
            if _THETA_PARITY_DEBUG:
                logger.warning(
                    "[POLARS_MIXIN][FILTER][FALLBACK_TRIGGERED] asset=%s length=%s reason=empty_result_after_filter",
                    getattr(asset, 'symbol', asset),
                    length
                )
            fallback = (
                lazy_data
                .sort(dt_col)
                .unique(subset=[dt_col], keep='last', maintain_order=True)
                .tail(length)
                .collect()
            )
            if not fallback.is_empty():
                logger.debug(
                    '[POLARS-FILTER][FALLBACK] asset=%s timestep=%s length=%s rows=%s',
                    getattr(asset, 'symbol', asset) if hasattr(asset, 'symbol') else asset,
                    timestep,
                    length,
                    fallback.height,
                )
                # DEBUG-LOG: Fallback succeeded
                if _THETA_PARITY_DEBUG:
                    logger.debug(
                        "[POLARS_MIXIN][FILTER][FALLBACK_SUCCESS] asset=%s fallback_shape=%s",
                        getattr(asset, 'symbol', asset),
                        (fallback.height, len(fallback.columns))
                    )
                result = fallback
            else:
                # DEBUG-LOG: Fallback also empty
                if _THETA_PARITY_DEBUG:
                    logger.warning(
                        "[POLARS_MIXIN][FILTER][FALLBACK_EMPTY] asset=%s lazy_data_has_no_rows=True",
                        getattr(asset, 'symbol', asset)
                    )

        has_price_columns = {"open", "high", "low", "close"} <= set(result.columns)

        # DEBUG-LOG: Before missing flag computation
        if _THETA_PARITY_DEBUG:
            logger.debug(
                "[POLARS_MIXIN][FILTER][BEFORE_MISSING_FLAG] asset=%s has_price_columns=%s result_columns=%s",
                getattr(asset, 'symbol', asset),
                has_price_columns,
                result.columns
            )

        if has_price_columns:
            # CRITICAL FIX: Match pandas missing flag logic exactly
            # Pandas uses .isna().all(axis=1) which means ALL OHLCV must be NaN for missing=True
            # NOT any single column - this is a critical difference from previous implementation
            missing_price_expr = (
                (pl.col("open").is_null() | pl.col("open").is_nan()) &
                (pl.col("high").is_null() | pl.col("high").is_nan()) &
                (pl.col("low").is_null() | pl.col("low").is_nan()) &
                (pl.col("close").is_null() | pl.col("close").is_nan())
            )
            # Add volume check if it exists (pandas does this too)
            if "volume" in result.columns:
                missing_price_expr = missing_price_expr & (
                    pl.col("volume").is_null() | pl.col("volume").is_nan()
                )
        else:
            missing_price_expr = pl.lit(False)

        result = result.with_columns(missing_price_expr.alias("_lumibot_missing_price"))

        # DEBUG-LOG: After missing flag computation
        if _THETA_PARITY_DEBUG:
            try:
                missing_count = int(result.select(pl.col("_lumibot_missing_price").cast(pl.Int64).sum()).item())
                logger.debug(
                    "[POLARS_MIXIN][FILTER][AFTER_MISSING_FLAG] asset=%s missing_count=%s total_rows=%s",
                    getattr(asset, 'symbol', asset),
                    missing_count,
                    result.height
                )
            except Exception as e:
                logger.debug(
                    "[POLARS_MIXIN][FILTER][AFTER_MISSING_FLAG] asset=%s missing_count=ERROR error=%s",
                    getattr(asset, 'symbol', asset),
                    str(e)
                )

        if timestep != "day":
            if {"open", "high", "low", "close", "volume"} <= set(result.columns):
                open_ffill = pl.col("open").fill_nan(None).fill_null(strategy="forward")
                high_ffill = pl.col("high").fill_nan(None).fill_null(strategy="forward")
                low_ffill = pl.col("low").fill_nan(None).fill_null(strategy="forward")
                close_ffill = pl.col("close").fill_nan(None).fill_null(strategy="forward")
                close_fallback = pl.coalesce(
                    [close_ffill, open_ffill, high_ffill, low_ffill]
                )
                missing_price_mask = pl.col("_lumibot_missing_price")
                price_null_mask = (
                    pl.col("open").is_null()
                    | pl.col("open").is_nan()
                    | pl.col("high").is_null()
                    | pl.col("high").is_nan()
                    | pl.col("low").is_null()
                    | pl.col("low").is_nan()
                    | pl.col("close").is_null()
                    | pl.col("close").is_nan()
                )
                normalized_volume = pl.coalesce([pl.col("volume"), pl.lit(0.0)])
                has_quote_cols = {"bid", "ask"} <= set(result.columns)
                if has_quote_cols:
                    valid_mid_mask = (
                        pl.col("bid").is_not_null()
                        & ~pl.col("bid").is_nan()
                        & pl.col("ask").is_not_null()
                        & ~pl.col("ask").is_nan()
                    )
                    mid_price_expr = pl.when(valid_mid_mask).then((pl.col("bid") + pl.col("ask")) / 2.0).otherwise(close_fallback)
                else:
                    valid_mid_mask = pl.lit(False)
                    mid_price_expr = close_fallback
                adjust_condition = missing_price_mask | price_null_mask | ((normalized_volume <= 0) & valid_mid_mask)
                result = result.with_columns(
                    [
                        pl.when(adjust_condition)
                        .then(mid_price_expr)
                        .otherwise(pl.col("open"))
                        .alias("open"),
                        pl.when(adjust_condition)
                        .then(mid_price_expr)
                        .otherwise(pl.col("high"))
                        .alias("high"),
                        pl.when(adjust_condition)
                        .then(mid_price_expr)
                        .otherwise(pl.col("low"))
                        .alias("low"),
                        pl.when(adjust_condition)
                        .then(mid_price_expr)
                        .otherwise(pl.col("close"))
                        .alias("close"),
                        pl.when(missing_price_mask | normalized_volume.is_null())
                        .then(pl.lit(0.0))
                        .otherwise(normalized_volume)
                        .alias("volume"),
                    ]
                )
            elif has_price_columns:
                open_ffill = pl.col("open").fill_nan(None).fill_null(strategy="forward")
                high_ffill = pl.col("high").fill_nan(None).fill_null(strategy="forward")
                low_ffill = pl.col("low").fill_nan(None).fill_null(strategy="forward")
                close_ffill = pl.col("close").fill_nan(None).fill_null(strategy="forward")
                close_fallback = pl.coalesce(
                    [close_ffill, open_ffill, high_ffill, low_ffill]
                )
                missing_price_mask = pl.col("_lumibot_missing_price")
                result = result.with_columns(
                    [
                        pl.when(missing_price_mask)
                        .then(close_fallback)
                        .otherwise(pl.col(col_name))
                        .alias(col_name)
                        for col_name in ["open", "high", "low", "close"]
                        if col_name in result.columns
                    ]
                )

            forward_fill_columns = [
                col_name
                for col_name in ("open", "high", "low", "close", "volume", "bid", "ask")
                if col_name in result.columns
            ]
            if forward_fill_columns:
                result = result.with_columns(
                    [
                        pl.col(col_name)
                        .fill_nan(None)
                        .fill_null(strategy="forward")
                        for col_name in forward_fill_columns
                    ]
                )

        if "return" in result.columns:
            result = result.with_columns(
                pl.col("return").fill_null(0.0).fill_nan(0.0)
            )
        if "price_change" in result.columns:
            result = result.with_columns(
                pl.col("price_change").fill_null(0.0).fill_nan(0.0)
            )
        if "dividend_yield" in result.columns:
            result = result.with_columns(
                pl.col("dividend_yield").fill_null(0.0).fill_nan(0.0)
            )

        if timestep == "day" and cache_key:
            self._filtered_data_cache[cache_key] = result

        if "_lumibot_missing_price" in result.columns:
            missing_flag = pl.col("_lumibot_missing_price").cast(pl.Boolean)
            if "missing" in result.columns:
                result = result.with_columns(
                    pl.when(pl.col("missing").cast(pl.Boolean))
                    .then(True)
                    .otherwise(missing_flag)
                    .alias("missing")
                )
            else:
                result = result.with_columns(missing_flag.alias("missing"))
            result = result.drop("_lumibot_missing_price")

        if length and len(result) > length:
            result = result.tail(length)

        try:
            first_dt = result["datetime"][0] if "datetime" in result.columns and len(result) else None
        except Exception:
            first_dt = None
        try:
            last_dt = result["datetime"][-1] if "datetime" in result.columns and len(result) else None
        except Exception:
            last_dt = None
        missing_true = None
        if "missing" in result.columns and len(result):
            try:
                missing_true = int(
                    result.select(pl.col("missing").cast(pl.Int64).sum()).item()
                )
            except Exception:
                missing_true = None
        logger.debug(
            "[POLARS-FILTER] asset=%s timestep=%s length=%s rows=%s first_dt=%s last_dt=%s missing_true=%s columns=%s",
            getattr(asset, "symbol", asset) if hasattr(asset, "symbol") else asset,
            timestep,
            length,
            len(result),
            first_dt,
            last_dt,
            missing_true,
            result.columns,
        )
        return result
