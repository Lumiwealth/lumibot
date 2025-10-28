"""Mixin class for shared polars functionality across data sources.

This mixin provides common polars operations without disrupting inheritance.
"""

from datetime import datetime
from typing import Any, Dict, Optional

import polars as pl

from lumibot.entities import Asset, Bars
from lumibot.tools.lumibot_logger import get_logger

logger = get_logger(__name__)


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
        if response is None or response.is_empty():
            return Bars(response, source, asset, raw=response)

        # Limit length if specified
        if length and len(response) > length:
            response = response.tail(length)

        # Filter to only keep OHLCV + datetime columns (remove DataBento metadata like rtype, publisher_id, etc.)
        # Required columns for strategies
        required_cols = ['open', 'high', 'low', 'close', 'volume']
        optional_cols = ['datetime', 'timestamp', 'date', 'time', 'dividend', 'stock_splits', 'symbol']

        # Determine which columns to keep
        keep_cols = []
        for col in response.columns:
            if col in required_cols or col in optional_cols:
                keep_cols.append(col)

        # Select only the relevant columns
        if keep_cols:
            response = response.select(keep_cols)

        # Create bars object
        bars = Bars(response, source, asset, raw=response, quote=quote, return_polars=return_polars)
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
        # DEBUG
        logger.debug(f"[POLARS FILTER] end_filter={end_filter}, tzinfo={end_filter.tzinfo if hasattr(end_filter, 'tzinfo') else 'N/A'}, length={length}")

        # Convert end_filter to naive
        end_filter_naive = self._convert_datetime_for_filtering(end_filter)

        # DEBUG
        logger.debug(f"[POLARS FILTER] end_filter_naive={end_filter_naive}")

        # For daily timestep, use caching
        if timestep == "day":
            current_date = end_filter.date() if hasattr(end_filter, 'date') else end_filter
            cache_key = (asset, current_date, timestep)

            # Check cache first
            if cache_key in self._filtered_data_cache:
                result = self._filtered_data_cache[cache_key]
                if len(result) >= length:
                    return result.tail(length)

            # Fetch extra for caching
            fetch_length = max(length * 2, 100)

            # Find datetime column
            schema = lazy_data.collect_schema()
            dt_col = None
            for col_name in schema.names():
                if col_name in ['datetime', 'date', 'timestamp']:
                    dt_col = col_name
                    break

            if dt_col is None:
                logger.error("No datetime column found")
                return None

            # Filter and collect
            # CRITICAL FIX: Keep timezone info! Match the DataFrame's timezone
            # Get the DataFrame column's timezone from schema
            dt_dtype = schema[dt_col]

            # Convert filter to match DataFrame's timezone
            if hasattr(dt_dtype, 'time_zone') and dt_dtype.time_zone:
                # DataFrame has timezone, convert filter to match
                import pytz
                df_tz = pytz.timezone(dt_dtype.time_zone)
                end_filter_with_tz = pytz.utc.localize(end_filter_naive).astimezone(df_tz)
            else:
                # DataFrame is naive, use UTC
                from datetime import timezone as tz
                end_filter_with_tz = datetime.combine(
                    end_filter_naive.date(),
                    end_filter_naive.time(),
                    tzinfo=tz.utc
                )

            # CRITICAL FIX: Deduplicate before caching
            # Use < or <= based on use_strict_less_than flag
            if use_strict_less_than:
                filter_expr = pl.col(dt_col) < end_filter_with_tz
            else:
                filter_expr = pl.col(dt_col) <= end_filter_with_tz

            result = (
                lazy_data
                .filter(filter_expr)
                .sort(dt_col)
                .unique(subset=[dt_col], keep='last', maintain_order=True)
                .tail(fetch_length)
                .collect()
            )

            # Cache the result
            self._filtered_data_cache[cache_key] = result

            # Return requested length
            return result.tail(length) if len(result) > length else result
        else:
            # For minute data, don't cache
            schema = lazy_data.collect_schema()
            dt_col = None
            for col_name in schema.names():
                if col_name in ['datetime', 'date', 'timestamp']:
                    dt_col = col_name
                    break

            if dt_col is None:
                logger.error("No datetime column found")
                return None

            # CRITICAL FIX: Keep timezone info during filtering!
            # Match the DataFrame's timezone to avoid comparison errors
            # Get the DataFrame column's timezone from schema
            dt_dtype = schema[dt_col]

            # Convert filter to match DataFrame's timezone
            if hasattr(dt_dtype, 'time_zone') and dt_dtype.time_zone:
                # DataFrame has timezone, convert filter to match
                import pytz
                df_tz = pytz.timezone(dt_dtype.time_zone)
                end_filter_with_tz = pytz.utc.localize(end_filter_naive).astimezone(df_tz)
            else:
                # DataFrame is naive, use UTC
                from datetime import timezone as tz
                end_filter_with_tz = datetime.combine(
                    end_filter_naive.date(),
                    end_filter_naive.time(),
                    tzinfo=tz.utc
                )

            # CRITICAL FIX: Deduplicate before returning
            # Sometimes lazy operations can create duplicates
            # Use < or <= based on use_strict_less_than flag
            if use_strict_less_than:
                filter_expr = pl.col(dt_col) < end_filter_with_tz
            else:
                filter_expr = pl.col(dt_col) <= end_filter_with_tz

            result = (
                lazy_data
                .filter(filter_expr)
                .sort(dt_col)
                .unique(subset=[dt_col], keep='last', maintain_order=True)
                .tail(length)
                .collect()
            )

            return result
