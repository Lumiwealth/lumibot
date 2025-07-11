"""
DuckDB-based backtesting implementation for Lumibot

This module provides the backtesting implementation that uses DuckDB for data storage,
offering significant performance improvements over traditional pandas-based approaches.
"""

from lumibot.data_sources.duckdb_data import DuckDBData


class DuckDBBacktesting(DuckDBData):
    """
    DuckDB-based backtesting implementation.
    
    This class extends DuckDBData specifically for backtesting scenarios,
    providing optimized data access patterns and caching strategies.
    """

    def __init__(self, datetime_start, datetime_end, **kwargs):
        """
        Initialize DuckDB backtesting data source.
        
        Parameters
        ----------
        datetime_start : datetime
            Start date for backtesting
        datetime_end : datetime
            End date for backtesting
        **kwargs
            Additional arguments passed to DuckDBData
        """
        super().__init__(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            **kwargs
        )
        
        # Optimize for backtesting patterns
        self._optimize_for_backtesting()
    
    def _optimize_for_backtesting(self):
        """Apply DuckDB optimizations specific to backtesting patterns"""
        try:
            # Create additional indexes that are beneficial for backtesting queries
            self.conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_symbol_timestamp_desc 
                ON ohlcv_data (symbol, timestamp DESC)
            """)
            
            # Enable query optimization
            self.conn.execute("SET enable_optimizer=true")
            self.conn.execute("SET enable_join_reorder=true")
            
            # Configure for read-heavy workload
            self.conn.execute("SET checkpoint_threshold='1GB'")
            
        except Exception as e:
            # Log warning but don't fail initialization
            import logging
            logger = logging.getLogger(__name__)
            logger.warning(f"Could not apply DuckDB optimizations: {e}")
    
    def prefetch_data_for_backtest(self, assets, timestep="minute"):
        """
        Prefetch and optimize data layout for backtesting.
        
        This method reorganizes data in DuckDB for optimal access patterns
        during backtesting.
        
        Parameters
        ----------
        assets : list of Asset
            Assets to prefetch data for
        timestep : str
            Time resolution for the backtest
        """
        try:
            symbols = [asset.symbol for asset in assets]
            
            # Create a materialized view for frequently accessed data
            symbols_list = "','".join(symbols)
            
            self.conn.execute(f"""
                CREATE OR REPLACE VIEW backtest_data AS
                SELECT symbol, timestamp, open, high, low, close, volume
                FROM ohlcv_data 
                WHERE symbol IN ('{symbols_list}')
                AND timestamp >= '{self.datetime_start}'
                AND timestamp <= '{self.datetime_end}'
                ORDER BY symbol, timestamp
            """)
            
            # Analyze the data for query optimization
            self.conn.execute("ANALYZE backtest_data")
            
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error(f"Error prefetching data for backtest: {e}")
