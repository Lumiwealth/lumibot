"""
Proposed DuckDB-based data storage implementation for Lumibot

This module demonstrates how DuckDB could be integrated to replace the current
in-memory data storage approach with a more efficient, scalable solution.
"""

import duckdb
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Union, Tuple
from abc import ABC, abstractmethod
import logging
from lumibot.entities import Asset, Bars
from lumibot import LUMIBOT_CACHE_FOLDER

logger = logging.getLogger(__name__)


class DataStorageInterface(ABC):
    """Abstract interface for data storage backends"""
    
    @abstractmethod
    def store_ohlcv_data(self, asset: Asset, data: pd.DataFrame, source: str, timeframe: str) -> None:
        """Store OHLCV data for an asset"""
        pass
    
    @abstractmethod
    def get_historical_prices(self, asset: Asset, start: datetime, end: datetime, 
                            timeframe: str = "minute", source: str = None) -> Optional[pd.DataFrame]:
        """Retrieve historical price data"""
        pass
    
    @abstractmethod
    def get_last_price(self, asset: Asset, timestamp: datetime, 
                      timeframe: str = "minute", source: str = None) -> Optional[float]:
        """Get the last available price for an asset"""
        pass
    
    @abstractmethod
    def has_data(self, asset: Asset, start: datetime, end: datetime, 
                timeframe: str = "minute", source: str = None) -> bool:
        """Check if data exists for the given parameters"""
        pass


class DuckDBDataStorage(DataStorageInterface):
    """DuckDB-based data storage implementation for Lumibot"""
    
    def __init__(self, cache_dir: str = None):
        """Initialize DuckDB storage
        
        Args:
            cache_dir: Directory for cache files. Defaults to LUMIBOT_CACHE_FOLDER
        """
        if cache_dir is None:
            cache_dir = LUMIBOT_CACHE_FOLDER
        
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        self.db_path = self.cache_dir / "lumibot_data.duckdb"
        self.conn = duckdb.connect(str(self.db_path))
        
        self._setup_schema()
        logger.info(f"Initialized DuckDB storage at {self.db_path}")
    
    def _setup_schema(self):
        """Create database schema"""
        
        # Main OHLCV data table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS ohlcv_data (
                asset_symbol VARCHAR,
                asset_type VARCHAR,
                exchange VARCHAR,
                timestamp TIMESTAMP,
                timeframe VARCHAR,
                source VARCHAR,
                open DECIMAL(18,8),
                high DECIMAL(18,8),
                low DECIMAL(18,8),
                close DECIMAL(18,8),
                volume DECIMAL(18,8),
                created_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (asset_symbol, asset_type, exchange, timestamp, timeframe, source)
            )
        """)
        
        # Asset metadata table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS assets (
                symbol VARCHAR,
                asset_type VARCHAR,
                exchange VARCHAR,
                currency VARCHAR,
                name VARCHAR,
                metadata JSON,
                PRIMARY KEY (symbol, asset_type, exchange)
            )
        """)
        
        # Cache range tracking
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS cache_ranges (
                id VARCHAR PRIMARY KEY,
                asset_symbol VARCHAR,
                asset_type VARCHAR,
                exchange VARCHAR,
                source VARCHAR,
                timeframe VARCHAR,
                start_time TIMESTAMP,
                end_time TIMESTAMP,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        # Create indexes for performance
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_ohlcv_lookup 
            ON ohlcv_data (asset_symbol, asset_type, exchange, timeframe, source, timestamp)
        """)
        
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_ohlcv_timestamp 
            ON ohlcv_data (timestamp)
        """)
    
    def store_ohlcv_data(self, asset: Asset, data: pd.DataFrame, source: str, timeframe: str) -> None:
        """Store OHLCV data for an asset
        
        Args:
            asset: Asset object
            data: DataFrame with OHLCV data, index should be datetime
            source: Data source name (e.g., 'polygon', 'yahoo', 'alpaca')
            timeframe: Timeframe string (e.g., 'minute', 'hour', 'day')
        """
        if data.empty:
            return
        
        # Prepare data for insertion
        df_insert = data.copy()
        df_insert.reset_index(inplace=True)
        
        # Add asset and metadata columns
        df_insert['asset_symbol'] = asset.symbol
        df_insert['asset_type'] = asset.asset_type
        df_insert['exchange'] = getattr(asset, 'exchange', '')
        df_insert['timeframe'] = timeframe
        df_insert['source'] = source
        
        # Ensure timestamp column exists
        if 'timestamp' not in df_insert.columns:
            df_insert.rename(columns={df_insert.columns[0]: 'timestamp'}, inplace=True)
        
        # Insert data using DuckDB's efficient bulk insert
        self.conn.execute("BEGIN TRANSACTION")
        try:
            # Use UPSERT to handle duplicates
            self.conn.execute("""
                INSERT OR REPLACE INTO ohlcv_data 
                SELECT asset_symbol, asset_type, exchange, timestamp, timeframe, source,
                       open, high, low, close, volume, NOW() as created_at
                FROM df_insert
            """)
            
            # Update cache ranges
            start_time = df_insert['timestamp'].min()
            end_time = df_insert['timestamp'].max()
            cache_id = f"{asset.symbol}_{asset.asset_type}_{source}_{timeframe}_{start_time.strftime('%Y%m%d')}_{end_time.strftime('%Y%m%d')}"
            
            self.conn.execute("""
                INSERT OR REPLACE INTO cache_ranges 
                (id, asset_symbol, asset_type, exchange, source, timeframe, start_time, end_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, [cache_id, asset.symbol, asset.asset_type, getattr(asset, 'exchange', ''), 
                  source, timeframe, start_time, end_time])
            
            # Store asset metadata if not exists
            self.conn.execute("""
                INSERT OR IGNORE INTO assets 
                (symbol, asset_type, exchange, currency, name)
                VALUES (?, ?, ?, ?, ?)
            """, [asset.symbol, asset.asset_type, getattr(asset, 'exchange', ''),
                  getattr(asset, 'currency', ''), getattr(asset, 'name', '')])
            
            self.conn.execute("COMMIT")
            logger.debug(f"Stored {len(df_insert)} records for {asset.symbol} ({timeframe}, {source})")
            
        except Exception as e:
            self.conn.execute("ROLLBACK")
            logger.error(f"Failed to store data for {asset.symbol}: {e}")
            raise
    
    def get_historical_prices(self, asset: Asset, start: datetime, end: datetime, 
                            timeframe: str = "minute", source: str = None) -> Optional[pd.DataFrame]:
        """Retrieve historical price data
        
        Args:
            asset: Asset object
            start: Start datetime
            end: End datetime
            timeframe: Timeframe string
            source: Data source name (optional, will use any source if None)
        
        Returns:
            DataFrame with OHLCV data or None if no data found
        """
        # Build query
        where_conditions = [
            "asset_symbol = ?",
            "asset_type = ?", 
            "timeframe = ?",
            "timestamp >= ?",
            "timestamp <= ?"
        ]
        params = [asset.symbol, asset.asset_type, timeframe, start, end]
        
        if source:
            where_conditions.append("source = ?")
            params.append(source)
        
        if hasattr(asset, 'exchange') and asset.exchange:
            where_conditions.append("exchange = ?")
            params.append(asset.exchange)
        
        query = f"""
            SELECT timestamp, open, high, low, close, volume, source
            FROM ohlcv_data 
            WHERE {' AND '.join(where_conditions)}
            ORDER BY timestamp ASC
        """
        
        try:
            result = self.conn.execute(query, params).df()
            
            if result.empty:
                return None
            
            # Set timestamp as index
            result.set_index('timestamp', inplace=True)
            
            # Remove source column if not needed
            if 'source' in result.columns and source:
                result.drop('source', axis=1, inplace=True)
            
            logger.debug(f"Retrieved {len(result)} records for {asset.symbol} from {start} to {end}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to retrieve data for {asset.symbol}: {e}")
            return None
    
    def get_last_price(self, asset: Asset, timestamp: datetime, 
                      timeframe: str = "minute", source: str = None) -> Optional[float]:
        """Get the last available price for an asset
        
        Args:
            asset: Asset object
            timestamp: Reference timestamp
            timeframe: Timeframe string
            source: Data source name (optional)
        
        Returns:
            Last close price or None if no data found
        """
        where_conditions = [
            "asset_symbol = ?",
            "asset_type = ?", 
            "timeframe = ?",
            "timestamp <= ?"
        ]
        params = [asset.symbol, asset.asset_type, timeframe, timestamp]
        
        if source:
            where_conditions.append("source = ?")
            params.append(source)
        
        if hasattr(asset, 'exchange') and asset.exchange:
            where_conditions.append("exchange = ?")
            params.append(asset.exchange)
        
        query = f"""
            SELECT close
            FROM ohlcv_data 
            WHERE {' AND '.join(where_conditions)}
            ORDER BY timestamp DESC
            LIMIT 1
        """
        
        try:
            result = self.conn.execute(query, params).fetchone()
            return float(result[0]) if result else None
            
        except Exception as e:
            logger.error(f"Failed to get last price for {asset.symbol}: {e}")
            return None
    
    def has_data(self, asset: Asset, start: datetime, end: datetime, 
                timeframe: str = "minute", source: str = None) -> bool:
        """Check if data exists for the given parameters
        
        Args:
            asset: Asset object
            start: Start datetime
            end: End datetime
            timeframe: Timeframe string
            source: Data source name (optional)
        
        Returns:
            True if data exists, False otherwise
        """
        where_conditions = [
            "asset_symbol = ?",
            "asset_type = ?", 
            "timeframe = ?",
            "start_time <= ?",
            "end_time >= ?"
        ]
        params = [asset.symbol, asset.asset_type, timeframe, start, end]
        
        if source:
            where_conditions.append("source = ?")
            params.append(source)
        
        if hasattr(asset, 'exchange') and asset.exchange:
            where_conditions.append("exchange = ?")
            params.append(asset.exchange)
        
        query = f"""
            SELECT COUNT(*) as count
            FROM cache_ranges 
            WHERE {' AND '.join(where_conditions)}
        """
        
        try:
            result = self.conn.execute(query, params).fetchone()
            return result[0] > 0 if result else False
            
        except Exception as e:
            logger.error(f"Failed to check data existence for {asset.symbol}: {e}")
            return False
    
    def get_assets(self, source: str = None, timeframe: str = None) -> List[Asset]:
        """Get list of available assets
        
        Args:
            source: Filter by data source (optional)
            timeframe: Filter by timeframe (optional)
        
        Returns:
            List of Asset objects
        """
        where_conditions = []
        params = []
        
        if source:
            where_conditions.append("source = ?")
            params.append(source)
        
        if timeframe:
            where_conditions.append("timeframe = ?")
            params.append(timeframe)
        
        where_clause = f"WHERE {' AND '.join(where_conditions)}" if where_conditions else ""
        
        query = f"""
            SELECT DISTINCT o.asset_symbol, o.asset_type, o.exchange,
                   a.currency, a.name
            FROM ohlcv_data o
            LEFT JOIN assets a ON o.asset_symbol = a.symbol 
                               AND o.asset_type = a.asset_type 
                               AND o.exchange = a.exchange
            {where_clause}
            ORDER BY o.asset_symbol
        """
        
        try:
            result = self.conn.execute(query, params).df()
            
            assets = []
            for _, row in result.iterrows():
                asset = Asset(
                    symbol=row['asset_symbol'],
                    asset_type=row['asset_type']
                )
                if row['exchange']:
                    asset.exchange = row['exchange']
                if row['currency']:
                    asset.currency = row['currency']
                if row['name']:
                    asset.name = row['name']
                assets.append(asset)
            
            return assets
            
        except Exception as e:
            logger.error(f"Failed to get assets: {e}")
            return []
    
    def compute_technical_indicators(self, asset: Asset, start: datetime, end: datetime,
                                   timeframe: str = "minute", source: str = None,
                                   window: int = 20) -> Optional[pd.DataFrame]:
        """Compute technical indicators using SQL window functions
        
        This demonstrates the power of using SQL for technical analysis
        
        Args:
            asset: Asset object
            start: Start datetime
            end: End datetime
            timeframe: Timeframe string
            source: Data source name (optional)
            window: Window size for indicators
        
        Returns:
            DataFrame with price data and technical indicators
        """
        where_conditions = [
            "asset_symbol = ?",
            "asset_type = ?", 
            "timeframe = ?",
            "timestamp >= ?",
            "timestamp <= ?"
        ]
        params = [asset.symbol, asset.asset_type, timeframe, start, end]
        
        if source:
            where_conditions.append("source = ?")
            params.append(source)
        
        query = f"""
            SELECT timestamp, open, high, low, close, volume,
                   -- Simple Moving Average
                   AVG(close) OVER (
                       ORDER BY timestamp 
                       ROWS BETWEEN {window-1} PRECEDING AND CURRENT ROW
                   ) as sma_{window},
                   
                   -- Exponential Moving Average (approximation)
                   close * (2.0 / ({window} + 1)) + 
                   LAG(close) OVER (ORDER BY timestamp) * (1 - 2.0 / ({window} + 1)) as ema_{window},
                   
                   -- Bollinger Bands
                   AVG(close) OVER (
                       ORDER BY timestamp 
                       ROWS BETWEEN {window-1} PRECEDING AND CURRENT ROW
                   ) + 2 * STDDEV(close) OVER (
                       ORDER BY timestamp 
                       ROWS BETWEEN {window-1} PRECEDING AND CURRENT ROW
                   ) as bb_upper,
                   
                   AVG(close) OVER (
                       ORDER BY timestamp 
                       ROWS BETWEEN {window-1} PRECEDING AND CURRENT ROW
                   ) - 2 * STDDEV(close) OVER (
                       ORDER BY timestamp 
                       ROWS BETWEEN {window-1} PRECEDING AND CURRENT ROW
                   ) as bb_lower,
                   
                   -- RSI (simplified calculation)
                   100 - (100 / (1 + 
                       AVG(CASE WHEN close > LAG(close) OVER (ORDER BY timestamp) 
                           THEN close - LAG(close) OVER (ORDER BY timestamp) 
                           ELSE 0 END) OVER (
                           ORDER BY timestamp 
                           ROWS BETWEEN {window-1} PRECEDING AND CURRENT ROW
                       ) /
                       AVG(CASE WHEN close < LAG(close) OVER (ORDER BY timestamp) 
                           THEN LAG(close) OVER (ORDER BY timestamp) - close 
                           ELSE 0 END) OVER (
                           ORDER BY timestamp 
                           ROWS BETWEEN {window-1} PRECEDING AND CURRENT ROW
                       )
                   )) as rsi_{window}
                   
            FROM ohlcv_data 
            WHERE {' AND '.join(where_conditions)}
            ORDER BY timestamp ASC
        """
        
        try:
            result = self.conn.execute(query, params).df()
            
            if result.empty:
                return None
            
            result.set_index('timestamp', inplace=True)
            logger.debug(f"Computed technical indicators for {asset.symbol}")
            return result
            
        except Exception as e:
            logger.error(f"Failed to compute technical indicators for {asset.symbol}: {e}")
            return None
    
    def cleanup_old_data(self, days_to_keep: int = 365):
        """Clean up old data to manage storage size
        
        Args:
            days_to_keep: Number of days of data to keep
        """
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        
        try:
            # Clean old OHLCV data
            result = self.conn.execute("""
                DELETE FROM ohlcv_data 
                WHERE timestamp < ?
            """, [cutoff_date])
            
            rows_deleted = result.rowcount if hasattr(result, 'rowcount') else 0
            
            # Clean old cache ranges
            self.conn.execute("""
                DELETE FROM cache_ranges 
                WHERE end_time < ?
            """, [cutoff_date])
            
            logger.info(f"Cleaned up {rows_deleted} old data records before {cutoff_date}")
            
        except Exception as e:
            logger.error(f"Failed to cleanup old data: {e}")
    
    def get_storage_stats(self) -> Dict[str, Union[int, float]]:
        """Get storage statistics
        
        Returns:
            Dictionary with storage statistics
        """
        try:
            # Get table sizes
            stats = {}
            
            # Record counts
            ohlcv_count = self.conn.execute("SELECT COUNT(*) FROM ohlcv_data").fetchone()[0]
            assets_count = self.conn.execute("SELECT COUNT(*) FROM assets").fetchone()[0]
            ranges_count = self.conn.execute("SELECT COUNT(*) FROM cache_ranges").fetchone()[0]
            
            stats['ohlcv_records'] = ohlcv_count
            stats['assets_count'] = assets_count
            stats['cache_ranges'] = ranges_count
            
            # Date ranges
            date_range = self.conn.execute("""
                SELECT MIN(timestamp) as min_date, MAX(timestamp) as max_date 
                FROM ohlcv_data
            """).fetchone()
            
            if date_range and date_range[0]:
                stats['earliest_data'] = date_range[0]
                stats['latest_data'] = date_range[1]
            
            # File size
            if self.db_path.exists():
                stats['file_size_mb'] = self.db_path.stat().st_size / (1024 * 1024)
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get storage stats: {e}")
            return {}
    
    def close(self):
        """Close database connection"""
        if self.conn:
            self.conn.close()
            logger.info("Closed DuckDB connection")


class EnhancedDataSourceBacktesting:
    """Enhanced data source that uses DuckDB for efficient storage and retrieval
    
    This demonstrates how existing data sources could be enhanced with DuckDB
    """
    
    def __init__(self, *args, use_duckdb: bool = True, **kwargs):
        """Initialize with optional DuckDB support
        
        Args:
            use_duckdb: Whether to use DuckDB storage backend
        """
        self.use_duckdb = use_duckdb
        
        if self.use_duckdb:
            self.storage = DuckDBDataStorage()
        else:
            # Fall back to traditional implementation
            self._data_store = {}
    
    def get_historical_prices(self, asset: Asset, length: int, timestep: str = "minute",
                            timeshift: timedelta = None, quote: Asset = None,
                            exchange: str = None, include_after_hours: bool = True) -> Optional[Bars]:
        """Get historical prices using DuckDB storage
        
        This method shows how existing APIs can be maintained while using DuckDB backend
        """
        if not self.use_duckdb:
            # Fall back to legacy implementation
            return self._get_historical_prices_legacy(asset, length, timestep, timeshift, quote)
        
        # Calculate time range
        current_dt = self.get_datetime()
        if timeshift:
            current_dt = current_dt - timeshift
        
        if timestep == "day":
            start_dt = current_dt - timedelta(days=length)
        else:  # minute
            start_dt = current_dt - timedelta(minutes=length)
        
        # Get data from DuckDB
        df = self.storage.get_historical_prices(
            asset=asset,
            start=start_dt,
            end=current_dt,
            timeframe=timestep,
            source=self.SOURCE if hasattr(self, 'SOURCE') else None
        )
        
        if df is None or df.empty:
            return None
        
        # Limit to requested length
        if len(df) > length:
            df = df.tail(length)
        
        # Return as Bars object
        return Bars(df, self.SOURCE if hasattr(self, 'SOURCE') else 'duckdb', asset=asset, quote=quote)
    
    def _get_historical_prices_legacy(self, asset, length, timestep, timeshift, quote):
        """Legacy implementation fallback"""
        # This would contain the original implementation
        pass
    
    def get_last_price(self, asset: Asset, timestep: str = None, quote: Asset = None, 
                      exchange: str = None, **kwargs) -> Optional[float]:
        """Get last price using DuckDB storage"""
        if not self.use_duckdb:
            return self._get_last_price_legacy(asset, timestep, quote, exchange)
        
        if timestep is None:
            timestep = getattr(self, '_timestep', 'minute')
        
        return self.storage.get_last_price(
            asset=asset,
            timestamp=self.get_datetime(),
            timeframe=timestep,
            source=self.SOURCE if hasattr(self, 'SOURCE') else None
        )
    
    def get_datetime(self) -> datetime:
        """Get current datetime - this would be implemented by the actual data source"""
        return datetime.now()  # Placeholder


# Example usage and migration guide
if __name__ == "__main__":
    # Example: Initialize DuckDB storage
    storage = DuckDBDataStorage()
    
    # Example: Store some data
    asset = Asset("SPY", "stock")
    
    # Create sample OHLCV data
    dates = pd.date_range('2023-01-01', '2023-01-10', freq='1H')
    sample_data = pd.DataFrame({
        'open': np.random.randn(len(dates)).cumsum() + 100,
        'high': np.random.randn(len(dates)).cumsum() + 102,
        'low': np.random.randn(len(dates)).cumsum() + 98,
        'close': np.random.randn(len(dates)).cumsum() + 100,
        'volume': np.random.randint(1000, 10000, len(dates))
    }, index=dates)
    
    # Store the data
    storage.store_ohlcv_data(asset, sample_data, "example", "hour")
    
    # Retrieve the data
    retrieved = storage.get_historical_prices(
        asset, 
        start=dates[0], 
        end=dates[-1], 
        timeframe="hour"
    )
    
    print(f"Stored {len(sample_data)} records, retrieved {len(retrieved) if retrieved is not None else 0}")
    
    # Compute technical indicators
    indicators = storage.compute_technical_indicators(
        asset,
        start=dates[0],
        end=dates[-1],
        timeframe="hour",
        window=5
    )
    
    if indicators is not None:
        print(f"Computed indicators with columns: {list(indicators.columns)}")
    
    # Get storage stats
    stats = storage.get_storage_stats()
    print(f"Storage stats: {stats}")
    
    # Clean up
    storage.close()
