"""Ultra-optimized Yahoo data source using pure polars with zero pandas conversions.

This implementation:
1. Eliminates datalines - uses polars columnar storage directly
2. Zero pandas conversions - pure polars throughout
3. Lazy evaluation for maximum performance
4. Efficient caching with parquet files
5. Vectorized operations only
"""

from datetime import datetime, timedelta
from decimal import Decimal
from typing import Union, Dict, List, Optional
import polars as pl
import numpy as np

from lumibot.tools.lumibot_logger import get_logger
from lumibot.data_sources import DataSourceBacktesting
from lumibot.entities import Asset, Bars
from lumibot.tools.yahoo_helper_polars_optimized import YahooHelperPolarsOptimized

logger = get_logger(__name__)


class YahooDataPolars(DataSourceBacktesting):
    """Ultra-optimized Yahoo data source with pure polars."""
    
    SOURCE = "YAHOO"
    MIN_TIMESTEP = "day"
    TIMESTEP_MAPPING = [
        {"timestep": "day", "representations": ["1d", "day"]},
        {"timestep": "15 minutes", "representations": ["15m", "15 minutes"]},
        {"timestep": "minute", "representations": ["1m", "1 minute"]},
    ]

    def __init__(self, auto_adjust=False, datetime_start=None, datetime_end=None, **kwargs):
        if datetime_start is None:
            datetime_start = datetime.now() - timedelta(days=365)
        if datetime_end is None:
            datetime_end = datetime.now()
            
        super().__init__(datetime_start=datetime_start, datetime_end=datetime_end, **kwargs)
        self.name = "yahoo_optimized"
        self.auto_adjust = auto_adjust
        
        # Optimized data storage - lazy frames ONLY for efficiency
        self._data_store: Dict[Asset, pl.LazyFrame] = {}
        # Remove eager cache to save memory - collect on demand
        # self._eager_cache: Dict[Asset, pl.DataFrame] = {}
        
        # Performance optimizations
        self._last_price_cache = {}
        self._cache_datetime = None
        
        # Column access optimization - pre-compute column indices
        self._column_indices: Dict[Asset, Dict[str, int]] = {}
        
        # Pre-filtered data cache for massive speedup
        self._filtered_data_cache: Dict[tuple, pl.DataFrame] = {}
        self._cache_date = None

    def _store_data(self, asset: Asset, data: pl.DataFrame) -> pl.LazyFrame:
        """Store data efficiently using lazy frames.
        
        Returns lazy frame for efficient subsequent operations.
        """
        # Process columns efficiently
        if isinstance(data, pl.DataFrame):
            # Already polars - perfect!
            pass
        else:
            # This should not happen in optimized version
            raise TypeError("Only polars DataFrames supported in optimized version")
        
        # Standardize column names
        rename_map = {
            "Open": "open", "High": "high", "Low": "low", "Close": "close",
            "Volume": "volume", "Dividends": "dividend", "Stock Splits": "stock_splits",
            "Adj Close": "adj_close", "index": "datetime", "Date": "datetime"
        }
        
        existing_renames = {k: v for k, v in rename_map.items() if k in data.columns}
        if existing_renames:
            data = data.rename(existing_renames)
        
        # Remove unnecessary columns
        if "adj_close" in data.columns and self.auto_adjust:
            data = data.drop("adj_close")
        
        # OPTIMIZATION: Calculate derived columns in single expression
        exprs = []
        
        # Always calculate price change and return
        exprs.append(pl.col("close").pct_change().alias("price_change"))
        
        if "dividend" in data.columns:
            exprs.extend([
                (pl.col("dividend") / pl.col("close")).alias("dividend_yield"),
                ((pl.col("dividend") / pl.col("close")) + pl.col("close").pct_change()).alias("return")
            ])
        else:
            exprs.extend([
                pl.lit(0.0).alias("dividend_yield"),
                pl.col("close").pct_change().alias("return")
            ])
        
        # Apply all calculations at once
        data = data.with_columns(exprs)
        
        # Store as lazy frame for efficient operations
        lazy_data = data.lazy()
        self._data_store[asset] = lazy_data
        
        # Cache column indices for fast access
        self._column_indices[asset] = {col: i for i, col in enumerate(data.columns)}
        
        return lazy_data

    def _get_data_lazy(self, asset: Asset) -> Optional[pl.LazyFrame]:
        """Get lazy frame for asset."""
        return self._data_store.get(asset)
    
    def _format_symbol(self, asset: Asset) -> str:
        """Format symbol for Yahoo Finance."""
        symbol = asset.symbol
        
        # Remove $ prefix if present
        if symbol.startswith('$'):
            symbol = symbol[1:]
        
        # Handle futures
        if asset.asset_type == Asset.AssetType.FUTURE or getattr(asset, 'asset_type', None) == 'futures':
            if symbol in ['ES', 'SPX']:
                symbol = 'ES=F'
            elif symbol == 'CL':
                symbol = 'CL=F'
            elif symbol == 'GC':
                symbol = 'GC=F'
            elif symbol == 'NQ':
                symbol = 'NQ=F'
            else:
                symbol = f"{symbol}=F"
        
        # Handle indices
        elif asset.asset_type == Asset.AssetType.INDEX or getattr(asset, 'asset_type', None) == 'index':
            index_map = {
                'SPX': '^GSPC', 'DJI': '^DJI', 'IXIC': '^IXIC',
                'NDX': '^NDX', 'RUT': '^RUT', 'VIX': '^VIX',
            }
            symbol = index_map.get(symbol, f"^{symbol}" if not symbol.startswith('^') else symbol)
        
        return symbol

    def _pull_source_symbol_bars(
        self,
        asset: Asset,
        length: int,
        timestep: str = "day",
        timeshift: Optional[timedelta] = None,
        quote: Optional[Asset] = None,
        include_after_hours: bool = False
    ) -> Optional[pl.DataFrame]:
        """Pull bars with maximum efficiency using pre-filtered cache."""
        
        if quote is not None:
            logger.warning(f"quote parameter not supported, ignoring {quote}")

        # For daily timestep, use optimized caching strategy
        if timestep == "day":
            # Check if we need to clear cache for new date
            current_date = self._datetime.date()
            
            # Try to get from filtered cache first
            cache_key = (asset, current_date, timestep)
            if cache_key in self._filtered_data_cache:
                result = self._filtered_data_cache[cache_key]
                if len(result) >= length:
                    # Cache hit!
                    return result.tail(length)

        interval = self._parse_source_timestep(timestep, reverse=True)
        
        # Check if we have data
        lazy_data = self._get_data_lazy(asset)
        
        if lazy_data is None:
            # Fetch data
            symbol = self._format_symbol(asset)
            
            # Use optimized helper
            # Fetch extra data to ensure we have data before start and after end
            fetch_start = self.datetime_start - timedelta(days=30)
            fetch_end = self.datetime_end + timedelta(days=30)
            data = YahooHelperPolarsOptimized.get_symbol_data_optimized(
                symbol,
                interval=interval,
                start=fetch_start,
                end=fetch_end,
                auto_adjust=self.auto_adjust,
            )
            
            if data is None or len(data) == 0:
                logger.error(f"No data returned for {asset.symbol}")
                return None
            
            lazy_data = self._store_data(asset, data)

        # OPTIMIZATION: Use lazy evaluation for filtering
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

        # Find datetime column efficiently using schema
        dt_col_cache_key = (asset, "dt_col")
        if dt_col_cache_key in self._last_price_cache:
            dt_col = self._last_price_cache[dt_col_cache_key]
        else:
            # Get column info from schema without collecting
            schema = lazy_data.collect_schema()
            dt_col = None
            
            # Check for datetime columns by type
            for col_name, dtype in zip(schema.names(), schema.dtypes()):
                if str(dtype) in ['Datetime', 'Date']:
                    dt_col = col_name
                    break
            
            # Fallback to name-based detection
            if dt_col is None:
                for col in ['datetime', 'date', 'timestamp']:
                    if col in schema.names():
                        dt_col = col
                        break
            
            if dt_col:
                self._last_price_cache[dt_col_cache_key] = dt_col
        
        if dt_col is None:
            logger.error("No datetime column found")
            return None

        # OPTIMIZATION: Single lazy operation for filter and tail
        # Convert end_filter to naive datetime to match data
        import pandas as pd
        if hasattr(end_filter, 'tz_localize'):
            # pandas timestamp
            end_filter_naive = end_filter.tz_localize(None)
        elif hasattr(end_filter, 'replace'):
            # datetime with timezone
            end_filter_naive = end_filter.replace(tzinfo=None)
        else:
            end_filter_naive = end_filter
            
        # Optimize collection with single operation
        if timestep == "day":
            fetch_length = max(length * 2, 100)  # Fetch extra for caching
            # Single optimized collection
            # Cast datetime column to microsecond precision to match comparison
            result = (
                lazy_data
                .with_columns(pl.col(dt_col).cast(pl.Datetime("us")))
                .filter(pl.col(dt_col) <= end_filter_naive)
                .sort(dt_col)  # Ensure sorted for tail operation
                .tail(fetch_length)
                .collect()
            )
            # Cache the filtered data
            current_date = self._datetime.date()
            cache_key = (asset, current_date, timestep)
            self._filtered_data_cache[cache_key] = result
        else:
            # For minute data, minimize memory usage
            # Cast datetime column to microsecond precision to match comparison
            result = (
                lazy_data
                .with_columns(pl.col(dt_col).cast(pl.Datetime("us")))
                .filter(pl.col(dt_col) <= end_filter_naive)
                .sort(dt_col)  # Ensure sorted
                .tail(length)
                .collect()
            )

        if len(result) < length:
            logger.debug(
                f"Requested {length} bars but only {len(result)} available "
                f"for {asset.symbol} before {end_filter}"
            )
            
        # Return only requested length
        if len(result) > length:
            result = result.tail(length)
            
        logger.debug(f"Returning {len(result)} bars for {asset.symbol}")

        return result

    def _pull_source_bars(
        self,
        assets: List[Asset],
        length: int,
        timestep: str = "day",
        timeshift: Optional[timedelta] = None,
        quote: Optional[Asset] = None,
        include_after_hours: bool = False
    ) -> Dict[Asset, pl.DataFrame]:
        """Pull bars for multiple assets efficiently."""
        
        result = {}
        
        # Group assets by whether we have data
        missing_assets = []
        for asset in assets:
            if asset not in self._data_store:
                missing_assets.append(asset)
        
        # Fetch missing data in parallel if possible
        if missing_assets:
            for asset in missing_assets:
                symbol = self._format_symbol(asset)
                fetch_start = self.datetime_start - timedelta(days=30)
                fetch_end = self.datetime_end + timedelta(days=30)
                data = YahooHelperPolarsOptimized.get_symbol_data_optimized(
                    symbol,
                    interval=self._parse_source_timestep(timestep, reverse=True),
                    start=fetch_start,
                    end=fetch_end,
                    auto_adjust=self.auto_adjust,
                )
                if data is not None and len(data) > 0:
                    self._store_data(asset, data)
        
        # Get bars for all assets
        for asset in assets:
            bars_data = self._pull_source_symbol_bars(
                asset, length, timestep=timestep, timeshift=timeshift
            )
            if bars_data is not None:
                result[asset] = bars_data
        
        return result

    def _parse_source_symbol_bars(
        self,
        response: pl.DataFrame,
        asset: Asset,
        quote: Optional[Asset] = None,
        length: Optional[int] = None
    ) -> Bars:
        """Parse bars from polars DataFrame."""
        if quote is not None:
            logger.warning(f"quote is not implemented for YahooData, but {quote} was passed as the quote")

        # Bars class will automatically use polars backend
        bars = Bars(response, self.SOURCE, asset, raw=response)
        return bars

    def get_last_price(
        self,
        asset: Asset,
        quote: Optional[Asset] = None,
        exchange: Optional[str] = None,
        timestep: Optional[str] = None,
        **kwargs
    ) -> Union[float, Decimal, None]:
        """Get last price with aggressive caching."""
        
        if timestep is None:
            timestep = self.get_timestep()

        # Check cache - use date-based caching for daily data
        current_datetime = self._datetime
        current_date = current_datetime.date()
        
        # For daily data, cache by date instead of datetime for better hit rate
        if timestep == "day":
            cache_key = (asset, timestep, quote, exchange, current_date)
        else:
            cache_key = (asset, timestep, quote, exchange, current_datetime)
        
        # Check if we need to clear cache
        if timestep == "day" and self._cache_date != current_date:
            self._last_price_cache.clear()
            self._cache_date = current_date
        elif timestep != "day" and self._cache_datetime != current_datetime:
            self._last_price_cache.clear()
            self._cache_datetime = current_datetime
        
        if cache_key in self._last_price_cache:
            return self._last_price_cache[cache_key]

        # Get price efficiently
        # For daily data, don't apply additional timeshift since _pull_source_symbol_bars
        # already handles getting the previous day's data
        # Only request 1 bar for efficiency (matching pandas implementation)
        timeshift = None if timestep == "day" else timedelta(days=-1)
        length = 1
        
        bars_data = self._pull_source_symbol_bars(
            asset, length, timestep=timestep, timeshift=timeshift
        )
        
        if bars_data is None or len(bars_data) == 0:
            logger.warning(f"No bars data for {asset.symbol} at {current_datetime}")
            self._last_price_cache[cache_key] = None
            return None

        # Direct column access - since we only request 1 bar, take the first (and only) element
        open_price = bars_data["open"][0]
        
        # Convert if needed
        if isinstance(open_price, (np.int64, np.integer)):
            open_price = Decimal(int(open_price))
        elif isinstance(open_price, (np.float64, np.floating)):
            open_price = float(open_price)
        
        self._last_price_cache[cache_key] = open_price
        return open_price

    def get_chains(self, asset: Asset, quote: Asset = None, exchange: str = None):
        """Get option chains - not implemented for Yahoo."""
        logger.warning("get_chains is not implemented for YahooData")
        return None

    def get_historical_prices(
        self,
        asset: Asset,
        length: int,
        timestep: str = None,
        timeshift: Optional[timedelta] = None,
        quote: Optional[Asset] = None,
        exchange: Optional[str] = None,
        include_after_hours: bool = False
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
        return self._parse_source_symbol_bars(bars_data, asset, quote=quote, length=length)

    def get_quote(self, asset: Asset) -> None:
        """Get quote - not implemented for Yahoo."""
        return None

    def get_strikes(self, asset):
        """Get strikes - not implemented for Yahoo."""
        raise NotImplementedError(
            "Lumibot YahooData does not support historical options data. If you need this "
            "feature, please use a different data source."
        )