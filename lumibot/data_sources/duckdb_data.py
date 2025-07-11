"""
DuckDB-based data source for Lumibot backtesting

This module provides a high-performance alternative to pandas-based in-memory storage
using DuckDB for data storage and retrieval during backtesting.
"""

import logging
import tempfile
import traceback
from collections import OrderedDict
from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Union, Optional, Dict, List, Tuple

import duckdb
import pandas as pd
import pytz

from lumibot.data_sources import DataSourceBacktesting
from lumibot.entities import Asset, Bars, Data, Quote
from lumibot.tools.helpers import to_datetime_aware

logger = logging.getLogger(__name__)


class DuckDBData(DataSourceBacktesting):
    """
    DuckDB-based data source for backtesting.
    
    Provides significant performance and memory improvements over pandas-based storage
    by using SQL queries for data retrieval and persistent storage.
    """

    SOURCE = "DUCKDB"
    TIMESTEP_MAPPING = [
        {"timestep": "day", "representations": ["1D", "day"]},
        {"timestep": "minute", "representations": ["1M", "minute"]},
    ]

    def __init__(
        self,
        *args,
        pandas_data=None,
        auto_adjust=True,
        db_path=None,
        memory_db=False,
        cache_size="1GB",
        threads=None,
        **kwargs
    ):
        """
        Initialize DuckDB data source.
        
        Parameters
        ----------
        pandas_data : dict or list, optional
            Initial data to load into DuckDB
        auto_adjust : bool
            Whether to automatically adjust prices for splits/dividends
        db_path : str or Path, optional
            Path to DuckDB database file. If None, creates temporary database
        memory_db : bool
            Whether to use in-memory database (faster but no persistence)
        cache_size : str
            DuckDB cache size (e.g., "1GB", "512MB")
        threads : int, optional
            Number of threads for DuckDB operations
        """
        super().__init__(*args, **kwargs)
        
        self.name = "duckdb"
        self.auto_adjust = auto_adjust
        self._timestep = "minute"
        
        # Database configuration
        self.memory_db = memory_db
        self.cache_size = cache_size
        self.threads = threads or 4
        
        # Track loaded symbols for caching optimization
        self._loaded_symbols = set()
        self._date_index = None
        
        # Initialize database
        self._setup_database(db_path)
        
        # Load initial data if provided
        if pandas_data:
            self._load_pandas_data(pandas_data)

    def _setup_database(self, db_path: Optional[str] = None):
        """Initialize DuckDB connection and schema"""
        if self.memory_db:
            self.db_path = ":memory:"
        elif db_path:
            self.db_path = Path(db_path)
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
        else:
            # Create temporary database
            self.temp_dir = tempfile.mkdtemp()
            self.db_path = Path(self.temp_dir) / "lumibot_backtest.duckdb"
        
        # Connect to database
        self.conn = duckdb.connect(str(self.db_path))
        
        # Configure DuckDB for performance
        self.conn.execute(f"SET memory_limit='{self.cache_size}'")
        self.conn.execute(f"SET threads={self.threads}")
        self.conn.execute("SET enable_progress_bar=false")
        
        # Create schema
        self._create_schema()

    def _create_schema(self):
        """Create tables and indexes for optimal performance"""
        
        # Main OHLCV data table
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS ohlcv_data (
                symbol VARCHAR NOT NULL,
                quote_symbol VARCHAR DEFAULT 'USD',
                asset_type VARCHAR DEFAULT 'stock',
                timestamp TIMESTAMP NOT NULL,
                open DECIMAL(18,8),
                high DECIMAL(18,8),
                low DECIMAL(18,8),
                close DECIMAL(18,8),
                volume DECIMAL(18,8),
                adjusted_close DECIMAL(18,8),
                dividend DECIMAL(18,8) DEFAULT 0,
                split_ratio DECIMAL(18,8) DEFAULT 1,
                timestep VARCHAR DEFAULT 'minute',
                PRIMARY KEY (symbol, quote_symbol, timestamp)
            )
        """)
        
        # Indexes for performance
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_symbol_timestamp 
            ON ohlcv_data (symbol, timestamp DESC)
        """)
        
        self.conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_symbol_quote_timestamp 
            ON ohlcv_data (symbol, quote_symbol, timestamp DESC)
        """)
        
        # Metadata table for tracking data sources and updates
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS data_metadata (
                symbol VARCHAR NOT NULL,
                quote_symbol VARCHAR DEFAULT 'USD',
                timestep VARCHAR NOT NULL,
                first_timestamp TIMESTAMP,
                last_timestamp TIMESTAMP,
                record_count INTEGER,
                last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (symbol, quote_symbol, timestep)
            )
        """)

    def _load_pandas_data(self, pandas_data):
        """Load pandas data into DuckDB"""
        
        def process_data_item(data_item: Data):
            """Process a single Data object"""
            try:
                asset = data_item.asset
                quote = getattr(data_item, 'quote', None) or Asset(symbol="USD", asset_type="forex")
                df = data_item.df.copy()
                
                if df.empty:
                    logger.warning(f"Empty dataframe for {asset.symbol}")
                    return
                
                # Prepare data for insertion with proper column order
                df_copy = df.copy()
                
                # Handle index properly
                if isinstance(df_copy.index, pd.DatetimeIndex):
                    # Reset index to get timestamp as a column
                    df_copy = df_copy.reset_index()
                    timestamp_col = df_copy.columns[0]  # First column is the timestamp
                else:
                    # Already has timestamps as first column
                    timestamp_col = df_copy.columns[0]
                
                # Create clean dataframe with required columns
                insert_data = {
                    'symbol': asset.symbol,
                    'quote_symbol': quote.symbol,  
                    'asset_type': asset.asset_type or 'stock',
                    'timestep': getattr(data_item, 'timestep', 'minute'),
                    'timestamp': pd.to_datetime(df_copy[timestamp_col]),
                    'open': df_copy['open'].astype(float),
                    'high': df_copy['high'].astype(float), 
                    'low': df_copy['low'].astype(float),
                    'close': df_copy['close'].astype(float),
                    'volume': df_copy.get('volume', 0).astype(int),
                    'adjusted_close': df_copy.get('adjusted_close', df_copy['close']).astype(float),
                    'dividend': 0.0,
                    'split_ratio': 1.0
                }
                
                insert_df = pd.DataFrame(insert_data)
                
                # Insert data with explicit column mapping
                self.conn.register("df_temp", insert_df)
                self.conn.execute("""
                    INSERT OR REPLACE INTO ohlcv_data 
                    (symbol, quote_symbol, asset_type, timestamp, open, high, low, close, volume, adjusted_close, dividend, split_ratio, timestep)
                    SELECT 
                        symbol, quote_symbol, asset_type, timestamp, open, high, low, close, volume, adjusted_close, dividend, split_ratio, timestep
                    FROM df_temp
                """)
                self.conn.unregister("df_temp")
                
                # Update metadata
                self._update_metadata(asset.symbol, quote.symbol or 'USD', data_item.timestep or 'minute', insert_df)
                
                self._loaded_symbols.add((asset.symbol, quote.symbol or 'USD'))
                logger.info(f"Loaded {len(df)} records for {asset.symbol}/{quote.symbol or 'USD'}")
                
            except Exception as e:
                symbol_name = getattr(locals().get('asset'), 'symbol', 'unknown') if 'asset' in locals() else 'unknown'
                logger.error(f"Error loading data for {symbol_name}: {e}")
                logger.error(traceback.format_exc())

        # Handle different input formats
        if isinstance(pandas_data, dict):
            for key, data_item in pandas_data.items():
                process_data_item(data_item)
        elif isinstance(pandas_data, list):
            for data_item in pandas_data:
                process_data_item(data_item)
        else:
            process_data_item(pandas_data)

    def _update_metadata(self, symbol: str, quote_symbol: str, timestep: str, df: pd.DataFrame):
        """Update metadata table with data statistics"""
        first_ts = df['timestamp'].min()
        last_ts = df['timestamp'].max()
        record_count = len(df)
        
        self.conn.execute("""
            INSERT OR REPLACE INTO data_metadata 
            (symbol, quote_symbol, timestep, first_timestamp, last_timestamp, record_count)
            VALUES (?, ?, ?, ?, ?, ?)
        """, [symbol, quote_symbol, timestep, first_ts, last_ts, record_count])

    def get_last_price(self, asset: Asset, quote: Optional[Asset] = None, exchange: Optional[str] = None) -> Union[float, Decimal, None]:
        """Get the last known price for an asset"""
        try:
            quote_symbol = quote.symbol if quote else 'USD'
            current_time = self.get_datetime()
            
            query = """
                SELECT close
                FROM ohlcv_data 
                WHERE symbol = ? AND quote_symbol = ? AND timestamp <= ?
                ORDER BY timestamp DESC
                LIMIT 1
            """
            
            result = self.conn.execute(query, [asset.symbol, quote_symbol, current_time]).fetchone()
            return float(result[0]) if result else None
            
        except Exception as e:
            logger.error(f"Error getting last price for {asset.symbol}: {e}")
            return None

    def get_quote(self, asset: Asset, quote: Optional[Asset] = None, exchange: Optional[str] = None) -> Quote:
        """Get quote information for an asset"""
        try:
            quote_symbol = quote.symbol if quote else 'USD'
            current_time = self.get_datetime()
            
            query = """
                SELECT open, high, low, close, volume, timestamp
                FROM ohlcv_data 
                WHERE symbol = ? AND quote_symbol = ? AND timestamp <= ?
                ORDER BY timestamp DESC
                LIMIT 1
            """
            
            result = self.conn.execute(query, [asset.symbol, quote_symbol, current_time]).fetchone()
            
            if result:
                return Quote(
                    asset=asset,
                    price=float(result[3]),  # close
                    bid=float(result[3]) * 0.999,  # approximate bid
                    ask=float(result[3]) * 1.001,  # approximate ask
                    volume=float(result[4]),
                    timestamp=result[5],
                    raw_data={
                        'open': float(result[0]),
                        'high': float(result[1]),
                        'low': float(result[2]),
                        'close': float(result[3]),
                        'volume': float(result[4])
                    }
                )
            else:
                return Quote(asset=asset)
                
        except Exception as e:
            logger.error(f"Error getting quote for {asset.symbol}: {e}")
            return Quote(asset=asset)

    def get_historical_prices(
        self,
        asset: Asset,
        length: int,
        timestep: str = "minute",
        timeshift: Optional[int] = None,
        quote: Optional[Asset] = None,
        exchange: Optional[str] = None,
        include_after_hours: bool = True,
    ) -> Bars:
        """Get historical price data"""
        try:
            quote_symbol = quote.symbol if quote else 'USD'
            current_time = self.get_datetime()
            
            # Apply timeshift if specified
            if timeshift and current_time:
                shift_minutes = timeshift if timestep == 'minute' else timeshift * 1440
                current_time = current_time + timedelta(minutes=shift_minutes)
            
            query = """
                SELECT timestamp, open, high, low, close, volume
                FROM ohlcv_data 
                WHERE symbol = ? AND quote_symbol = ? AND timestamp <= ?
                ORDER BY timestamp DESC
                LIMIT ?
            """
            
            result_df = self.conn.execute(query, [asset.symbol, quote_symbol, current_time, length]).df()
            
            if result_df.empty:
                logger.warning(f"No data found for {asset.symbol}/{quote_symbol}")
                # Return empty Bars object instead of None
                empty_df = pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])
                return Bars(empty_df, self.SOURCE, asset=asset, quote=quote)
            
            # Sort by timestamp ascending (oldest first)
            result_df = result_df.sort_values('timestamp')
            result_df.set_index('timestamp', inplace=True)
            
            return Bars(result_df, self.SOURCE, asset=asset, quote=quote)
            
        except Exception as e:
            logger.error(f"Error getting historical prices for {asset.symbol}: {e}")
            # Return empty Bars object instead of None
            empty_df = pd.DataFrame(columns=['open', 'high', 'low', 'close', 'volume'])
            return Bars(empty_df, self.SOURCE, asset=asset, quote=quote)

    def get_historical_prices_between_dates(
        self,
        asset: Asset,
        start_date: datetime,
        end_date: datetime,
        timestep: str = "minute",
        quote: Optional[Asset] = None,
        exchange: Optional[str] = None,
        include_after_hours: bool = True,
    ) -> Optional[pd.DataFrame]:
        """Get historical prices between specific dates"""
        try:
            quote_symbol = quote.symbol if quote else 'USD'
            
            query = """
                SELECT timestamp, open, high, low, close, volume
                FROM ohlcv_data 
                WHERE symbol = ? AND quote_symbol = ? 
                AND timestamp >= ? AND timestamp <= ?
                ORDER BY timestamp
            """
            
            result_df = self.conn.execute(query, [asset.symbol, quote_symbol, start_date, end_date]).df()
            
            if not result_df.empty:
                result_df.set_index('timestamp', inplace=True)
            
            return result_df
            
        except Exception as e:
            logger.error(f"Error getting historical prices between dates for {asset.symbol}: {e}")
            return pd.DataFrame()

    def get_last_prices(self, assets: List[Asset], quote: Optional[Asset] = None, exchange: Optional[str] = None, **kwargs) -> Dict[Asset, float]:
        """Get last prices for multiple assets efficiently"""
        result = {}
        
        # Batch query for efficiency
        try:
            quote_symbol = quote.symbol if quote else 'USD'
            current_time = self.get_datetime()
            symbols = [asset.symbol for asset in assets]
            
            # Use SQL IN clause for batch operation
            placeholders = ','.join(['?' for _ in symbols])
            query = f"""
                WITH ranked_prices AS (
                    SELECT symbol, close, 
                           ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp DESC) as rn
                    FROM ohlcv_data 
                    WHERE symbol IN ({placeholders}) 
                    AND quote_symbol = ? 
                    AND timestamp <= ?
                )
                SELECT symbol, close
                FROM ranked_prices 
                WHERE rn = 1
            """
            
            params = symbols + [quote_symbol, current_time]
            results = self.conn.execute(query, params).fetchall()
            
            # Create lookup dict
            price_lookup = {symbol: float(price) for symbol, price in results}
            
            # Map back to assets
            for asset in assets:
                result[asset] = price_lookup.get(asset.symbol)
                
        except Exception as e:
            logger.error(f"Error getting batch last prices: {e}")
            # Fallback to individual queries
            for asset in assets:
                result[asset] = self.get_last_price(asset, quote=quote, exchange=exchange)
        
        return result

    def get_symbols(self) -> List[str]:
        """Get all symbols in the database"""
        try:
            result = self.conn.execute("SELECT DISTINCT symbol FROM ohlcv_data ORDER BY symbol").fetchall()
            return [row[0] for row in result]
        except Exception as e:
            logger.error(f"Error getting symbols: {e}")
            return []

    def get_data_info(self) -> pd.DataFrame:
        """Get information about available data"""
        try:
            query = """
                SELECT symbol, quote_symbol, timestep, 
                       first_timestamp, last_timestamp, record_count,
                       last_updated
                FROM data_metadata
                ORDER BY symbol, quote_symbol, timestep
            """
            return self.conn.execute(query).df()
        except Exception as e:
            logger.error(f"Error getting data info: {e}")
            return pd.DataFrame()

    def optimize_database(self):
        """Optimize database performance"""
        try:
            logger.info("Optimizing DuckDB database...")
            self.conn.execute("ANALYZE")
            self.conn.execute("CHECKPOINT")
            logger.info("Database optimization completed")
        except Exception as e:
            logger.error(f"Error optimizing database: {e}")

    def get_database_size(self) -> float:
        """Get database file size in MB"""
        try:
            if self.memory_db or str(self.db_path) == ":memory:":
                # Estimate memory usage
                result = self.conn.execute("SELECT COUNT(*) FROM ohlcv_data").fetchone()
                if result and result[0]:
                    estimated_size = result[0] * 0.0001  # Rough estimate
                    return estimated_size
                return 0.0
            else:
                return Path(self.db_path).stat().st_size / 1024 / 1024
        except Exception:
            return 0.0

    def close(self):
        """Close database connection"""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
            logger.info("DuckDB connection closed")

    def __del__(self):
        """Cleanup on destruction"""
        self.close()

    # Compatibility methods for existing DataSource interface
    def load_data(self):
        """Load data (compatibility method)"""
        self._date_index = self._get_date_index()
        return self._get_trading_calendar()

    def _get_date_index(self):
        """Get all available timestamps"""
        try:
            result = self.conn.execute("""
                SELECT DISTINCT timestamp 
                FROM ohlcv_data 
                ORDER BY timestamp
            """).fetchall()
            
            timestamps = [row[0] for row in result]
            return pd.DatetimeIndex(timestamps)
        except Exception as e:
            logger.error(f"Error getting date index: {e}")
            # Return default range
            freq = "1min" if self._timestep == "minute" else "1D"
            return pd.date_range(start=self.datetime_start, end=self.datetime_end, freq=freq)

    def _get_trading_calendar(self):
        """Get trading calendar (simplified)"""
        try:
            # Simple implementation - can be enhanced with actual trading calendar
            date_range = pd.date_range(start=self.datetime_start, end=self.datetime_end, freq="D")
            result = pd.DataFrame(index=date_range, columns=["market_open", "market_close"])
            # Use DatetimeIndex methods properly
            market_open_times = pd.to_datetime(result.index).floor("D")
            market_close_times = pd.to_datetime(result.index).ceil("D") - pd.Timedelta("1s")
            result["market_open"] = market_open_times
            result["market_close"] = market_close_times
            return result
        except Exception as e:
            logger.error(f"Error getting trading calendar: {e}")
            return pd.DataFrame()

    def get_assets(self):
        """Get all assets (compatibility method)"""
        symbols = self.get_symbols()
        return [Asset(symbol=symbol, asset_type="stock") for symbol in symbols]

    def get_chains(self, asset: Asset, quote: Optional[Asset] = None, exchange: Optional[str] = None):
        """
        Get options chains for an asset (not implemented for DuckDB data source).
        
        This method is required by the DataSource interface but is not applicable
        for basic OHLCV data storage in DuckDB.
        """
        from lumibot.entities import Chains
        # Return empty chains since this data source doesn't support options data
        return Chains({})
