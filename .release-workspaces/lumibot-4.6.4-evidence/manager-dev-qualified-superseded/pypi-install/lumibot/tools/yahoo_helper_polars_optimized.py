"""Optimized Yahoo finance helper using pure polars with minimal conversions."""

from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import polars as pl
import yfinance as yf

from lumibot.constants import LUMIBOT_CACHE_FOLDER
from lumibot.tools.lumibot_logger import get_logger

logger = get_logger(__name__)


class YahooHelperPolarsOptimized:
    """Optimized Yahoo finance helper with polars-first approach and smart caching."""

    CACHE_DIR = Path(LUMIBOT_CACHE_FOLDER) / "yahoo_data_polars"

    def __init__(self):
        # Create cache directory if it doesn't exist
        self.CACHE_DIR.mkdir(parents=True, exist_ok=True)

        # In-memory cache for frequently accessed data
        self._memory_cache: Dict[str, pl.LazyFrame] = {}
        self._cache_metadata: Dict[str, Dict[str, Any]] = {}

    @classmethod
    def _get_cache_key(cls, symbol: str, interval: str, start: datetime, end: datetime) -> str:
        """Generate cache key for data."""
        return f"{symbol}_{interval}_{start.date()}_{end.date()}"

    @classmethod
    def _get_cache_path(cls, cache_key: str) -> Path:
        """Get cache file path."""
        return cls.CACHE_DIR / f"{cache_key}.parquet"

    def _load_from_cache(self, cache_key: str) -> Optional[pl.LazyFrame]:
        """Load data from cache if available and valid."""
        # Check memory cache first
        if cache_key in self._memory_cache:
            return self._memory_cache[cache_key]

        # Check disk cache
        cache_path = self._get_cache_path(cache_key)
        if cache_path.exists():
            try:
                # Use lazy loading for efficiency
                lazy_df = pl.scan_parquet(cache_path)
                self._memory_cache[cache_key] = lazy_df
                return lazy_df
            except Exception as e:
                logger.warning(f"Failed to load cache {cache_path}: {e}")
                return None

        return None

    def _save_to_cache(self, cache_key: str, data: pl.DataFrame):
        """Save data to cache."""
        cache_path = self._get_cache_path(cache_key)
        try:
            # Save as parquet with optimized settings
            data.write_parquet(
                cache_path,
                compression="snappy",  # Faster than zstd for our use case
                statistics=True,       # Enable statistics for faster queries
            )
            # Store lazy version in memory cache
            self._memory_cache[cache_key] = pl.scan_parquet(cache_path)
        except Exception as e:
            logger.warning(f"Failed to save cache {cache_path}: {e}")

    @classmethod
    def get_symbol_data_optimized(
        cls,
        symbol: str,
        interval: str = "1d",
        start: Optional[datetime] = None,
        end: Optional[datetime] = None,
        auto_adjust: bool = True,
        prepost: bool = True,
        actions: bool = True,
    ) -> Optional[pl.DataFrame]:
        """
        Get historical data for a symbol with optimized polars processing.
        
        This method minimizes pandas usage and maximizes polars efficiency:
        1. Direct yfinance to polars conversion
        2. Lazy evaluation where possible
        3. Efficient caching with parquet
        4. Columnar operations
        """
        helper = cls()

        # Set default dates if not provided
        if end is None:
            end = datetime.now(timezone.utc)
        if start is None:
            start = end - timedelta(days=365)

        # Generate cache key
        cache_key = cls._get_cache_key(symbol, interval, start, end)

        # Try to load from cache
        cached_data = helper._load_from_cache(cache_key)
        if cached_data is not None:
            # Collect only what we need
            return cached_data.collect()

        try:
            # Fetch from yfinance
            ticker = yf.Ticker(symbol)

            # Map interval to yfinance format
            yf_interval_map = {
                "day": "1d", "1day": "1d", "1d": "1d",
                "minute": "1m", "1minute": "1m", "1m": "1m",
                "5minutes": "5m", "5m": "5m",
                "15minutes": "15m", "15m": "15m",
                "30minutes": "30m", "30m": "30m",
                "hour": "1h", "1hour": "1h", "1h": "1h",
            }
            yf_interval = yf_interval_map.get(interval, interval)

            # Fetch data from yfinance
            hist = ticker.history(
                start=start,
                end=end,
                interval=yf_interval,
                auto_adjust=auto_adjust,
                prepost=prepost,
                actions=actions,
            )

            if hist.empty:
                logger.warning(f"No data returned for {symbol}")
                return None

            # OPTIMIZATION: Direct pandas to polars conversion
            # Reset index to get datetime as column
            hist_reset = hist.reset_index()

            # Direct conversion - more efficient than column-by-column
            df = pl.from_pandas(hist_reset)

            # Set proper column types and names
            datetime_col = "Date" if "Date" in df.columns else "Datetime"

            # Rename columns to lowercase and standardize
            rename_map = {
                datetime_col: "datetime",
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume",
                "Dividends": "dividend",
                "Stock Splits": "stock_splits",
                "Adj Close": "adj_close"
            }

            df = df.rename({k: v for k, v in rename_map.items() if k in df.columns})

            # OPTIMIZATION: Ensure datetime type
            if "datetime" in df.columns:
                # Cast to datetime if not already
                if not isinstance(df["datetime"].dtype, pl.Datetime):
                    df = df.with_columns(
                        pl.col("datetime").cast(pl.Datetime("us"))
                    )

            # Sort by datetime for efficient time-based operations
            df = df.sort("datetime")

            # Save to cache
            helper._save_to_cache(cache_key, df)

            return df

        except Exception as e:
            logger.error(f"Error fetching data for {symbol}: {str(e)}")
            return None

    @classmethod
    def clear_cache(cls, older_than_days: Optional[int] = None):
        """Clear cache files."""
        if older_than_days is None:
            # Clear all cache
            for cache_file in cls.CACHE_DIR.glob("*.parquet"):
                try:
                    cache_file.unlink()
                except Exception as e:
                    logger.warning(f"Failed to delete cache file {cache_file}: {e}")
        else:
            # Clear old cache files
            cutoff_time = datetime.now() - timedelta(days=older_than_days)
            for cache_file in cls.CACHE_DIR.glob("*.parquet"):
                try:
                    if cache_file.stat().st_mtime < cutoff_time.timestamp():
                        cache_file.unlink()
                except Exception as e:
                    logger.warning(f"Failed to delete cache file {cache_file}: {e}")


# Backward compatibility alias
get_symbol_data = YahooHelperPolarsOptimized.get_symbol_data_optimized
