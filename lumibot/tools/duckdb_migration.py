"""
Migration utilities for converting existing Lumibot data to DuckDB format

This module provides tools to migrate existing pandas-based data stores
to the new DuckDB format, ensuring seamless transition and data integrity.
"""

import logging
import time
from pathlib import Path
from typing import Dict, List, Optional, Union

import pandas as pd

from lumibot.backtesting.duckdb_backtesting import DuckDBBacktesting
from lumibot.data_sources.duckdb_data import DuckDBData
from lumibot.data_sources.pandas_data import PandasData
from lumibot.entities import Asset, Data

logger = logging.getLogger(__name__)


class DataMigrationUtility:
    """
    Utility class for migrating data between different storage formats.
    
    Provides methods to convert pandas-based data stores to DuckDB format
    with performance optimization and data validation.
    """
    
    def __init__(self, output_db_path: Optional[str] = None):
        """
        Initialize migration utility.
        
        Parameters
        ----------
        output_db_path : str, optional
            Path where the new DuckDB database will be created
        """
        self.output_db_path = output_db_path
        self.migration_stats = {
            'total_records': 0,
            'symbols_migrated': 0,
            'start_time': None,
            'end_time': None,
            'errors': []
        }
    
    def migrate_pandas_data_to_duckdb(
        self,
        pandas_data: Union[Dict, List, PandasData],
        validate_data: bool = True,
        chunk_size: int = 10000
    ) -> DuckDBData:
        """
        Migrate pandas data to DuckDB format.
        
        Parameters
        ----------
        pandas_data : dict, list, or PandasData
            Source pandas data to migrate
        validate_data : bool
            Whether to validate data integrity after migration
        chunk_size : int
            Number of records to process in each batch
            
        Returns
        -------
        DuckDBData
            Initialized DuckDB data source with migrated data
        """
        self.migration_stats['start_time'] = time.time()
        
        logger.info("Starting migration from pandas to DuckDB...")
        
        # Initialize DuckDB data source
        # Import datetime for proper initialization
        from datetime import datetime
        dummy_start = datetime(2000, 1, 1)
        dummy_end = datetime(2030, 12, 31)
        
        duckdb_data = DuckDBData(
            db_path=self.output_db_path,
            datetime_start=dummy_start,
            datetime_end=dummy_end
        )
        
        try:
            # Handle different input types
            if isinstance(pandas_data, PandasData):
                self._migrate_pandas_data_source(pandas_data, duckdb_data, chunk_size)
            elif isinstance(pandas_data, dict):
                self._migrate_pandas_dict(pandas_data, duckdb_data, chunk_size)
            elif isinstance(pandas_data, list):
                self._migrate_pandas_list(pandas_data, duckdb_data, chunk_size)
            else:
                raise ValueError(f"Unsupported pandas_data type: {type(pandas_data)}")
            
            # Optimize database after migration
            logger.info("Optimizing database...")
            duckdb_data.optimize_database()
            
            # Validate data if requested
            if validate_data:
                self._validate_migration(pandas_data, duckdb_data)
            
            self.migration_stats['end_time'] = time.time()
            self._log_migration_summary()
            
            return duckdb_data
            
        except Exception as e:
            logger.error(f"Migration failed: {e}")
            self.migration_stats['errors'].append(str(e))
            raise
    
    def _migrate_pandas_data_source(self, pandas_data: PandasData, duckdb_data: DuckDBData, chunk_size: int):
        """Migrate from PandasData object"""
        for key, data_obj in pandas_data._data_store.items():
            self._migrate_data_object(key, data_obj, duckdb_data, chunk_size)
    
    def _migrate_pandas_dict(self, pandas_data: Dict, duckdb_data: DuckDBData, chunk_size: int):
        """Migrate from dictionary of Data objects"""
        for key, data_obj in pandas_data.items():
            self._migrate_data_object(key, data_obj, duckdb_data, chunk_size)
    
    def _migrate_pandas_list(self, pandas_data: List, duckdb_data: DuckDBData, chunk_size: int):
        """Migrate from list of Data objects"""
        for data_obj in pandas_data:
            key = (data_obj.asset, getattr(data_obj, 'quote', None))
            self._migrate_data_object(key, data_obj, duckdb_data, chunk_size)
    
    def _migrate_data_object(self, key, data_obj: Data, duckdb_data: DuckDBData, chunk_size: int):
        """Migrate a single Data object"""
        asset = None
        try:
            asset = data_obj.asset if hasattr(data_obj, 'asset') else key[0]
            quote = getattr(data_obj, 'quote', None)
            
            if quote is None and isinstance(key, tuple) and len(key) > 1:
                quote = key[1]
            
            df = data_obj.df.copy()
            
            if df.empty:
                logger.warning(f"Skipping empty data for {asset.symbol}")
                return
            
            # Process in chunks for large datasets
            total_chunks = len(df) // chunk_size + (1 if len(df) % chunk_size else 0)
            
            for i, chunk_start in enumerate(range(0, len(df), chunk_size)):
                chunk_end = min(chunk_start + chunk_size, len(df))
                chunk_df = df.iloc[chunk_start:chunk_end].copy()
                
                # Prepare chunk for insertion
                chunk_df = chunk_df.reset_index()
                chunk_df['symbol'] = asset.symbol
                chunk_df['quote_symbol'] = quote.symbol if quote else 'USD'
                chunk_df['asset_type'] = getattr(asset, 'asset_type', 'stock')
                chunk_df['timestep'] = getattr(data_obj, 'timestep', 'minute')
                
                # Handle column mapping
                self._standardize_columns(chunk_df)
                
                # Insert chunk
                duckdb_data.conn.execute("INSERT OR REPLACE INTO ohlcv_data SELECT * FROM chunk_df")
                
                if total_chunks > 1:
                    logger.info(f"Migrated chunk {i+1}/{total_chunks} for {asset.symbol}")
            
            # Update metadata
            duckdb_data._update_metadata(
                asset.symbol,
                quote.symbol if quote else 'USD',
                getattr(data_obj, 'timestep', 'minute'),
                df
            )
            
            self.migration_stats['total_records'] += len(df)
            self.migration_stats['symbols_migrated'] += 1
            
            logger.info(f"Successfully migrated {len(df)} records for {asset.symbol}")
            
        except Exception as e:
            symbol_name = getattr(asset, 'symbol', 'unknown') if asset else 'unknown'
            error_msg = f"Error migrating {symbol_name}: {e}"
            logger.error(error_msg)
            self.migration_stats['errors'].append(error_msg)
    
    def _standardize_columns(self, df: pd.DataFrame):
        """Standardize DataFrame columns for DuckDB insertion"""
        # Ensure required columns exist
        required_cols = ['open', 'high', 'low', 'close', 'volume']
        for col in required_cols:
            if col not in df.columns:
                df[col] = 0.0
        
        # Add optional columns with defaults
        if 'adjusted_close' not in df.columns:
            df['adjusted_close'] = df['close']
        if 'dividend' not in df.columns:
            df['dividend'] = 0.0
        if 'split_ratio' not in df.columns:
            df['split_ratio'] = 1.0
        
        # Rename timestamp column if needed
        if 'timestamp' not in df.columns:
            timestamp_col = df.columns[0]  # Assume first column is timestamp
            df = df.rename(columns={timestamp_col: 'timestamp'})
    
    def _validate_migration(self, source_data, target_data: DuckDBData):
        """Validate that migration was successful"""
        logger.info("Validating migration...")
        
        try:
            # Get symbols from target database
            target_symbols = set(target_data.get_symbols())
            
            # Get symbols from source data
            source_symbols = set()
            if hasattr(source_data, '_data_store'):
                for key in source_data._data_store.keys():
                    if isinstance(key, tuple):
                        source_symbols.add(key[0].symbol)
                    else:
                        source_symbols.add(key.symbol)
            
            # Compare symbol counts
            if source_symbols != target_symbols:
                missing_symbols = source_symbols - target_symbols
                extra_symbols = target_symbols - source_symbols
                
                if missing_symbols:
                    logger.warning(f"Missing symbols in target: {missing_symbols}")
                if extra_symbols:
                    logger.warning(f"Extra symbols in target: {extra_symbols}")
            else:
                logger.info("✓ All symbols migrated successfully")
            
            # Validate record counts
            result = target_data.conn.execute("SELECT COUNT(*) FROM ohlcv_data").fetchone()
            total_target_records = result[0] if result else 0
            
            if total_target_records == self.migration_stats['total_records']:
                logger.info("✓ Record counts match")
            else:
                logger.warning(f"Record count mismatch: source={self.migration_stats['total_records']}, target={total_target_records}")
            
        except Exception as e:
            logger.error(f"Validation failed: {e}")
    
    def _log_migration_summary(self):
        """Log summary of migration process"""
        duration = self.migration_stats['end_time'] - self.migration_stats['start_time']
        
        logger.info("="*50)
        logger.info("MIGRATION SUMMARY")
        logger.info("="*50)
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"Symbols migrated: {self.migration_stats['symbols_migrated']}")
        logger.info(f"Total records: {self.migration_stats['total_records']:,}")
        logger.info(f"Records/second: {self.migration_stats['total_records']/duration:,.0f}")
        
        if self.migration_stats['errors']:
            logger.info(f"Errors: {len(self.migration_stats['errors'])}")
            for error in self.migration_stats['errors']:
                logger.error(f"  - {error}")
        else:
            logger.info("✓ No errors occurred")
        
        # Database size
        if self.output_db_path and Path(self.output_db_path).exists():
            size_mb = Path(self.output_db_path).stat().st_size / 1024 / 1024
            logger.info(f"Database size: {size_mb:.2f} MB")


def migrate_backtest_to_duckdb(
    source_data,
    output_db_path: Optional[str] = None,
    datetime_start=None,
    datetime_end=None,
    **kwargs
) -> DuckDBBacktesting:
    """
    Convenience function to migrate backtest data to DuckDB.
    
    Parameters
    ----------
    source_data : pandas data
        Source data to migrate
    output_db_path : str, optional
        Path for output database
    datetime_start : datetime, optional
        Backtest start date
    datetime_end : datetime, optional
        Backtest end date
    **kwargs
        Additional arguments for DuckDBBacktesting
        
    Returns
    -------
    DuckDBBacktesting
        Ready-to-use DuckDB backtesting data source
    """
    # Migrate data
    migrator = DataMigrationUtility(output_db_path)
    duckdb_data = migrator.migrate_pandas_data_to_duckdb(source_data)
    
    # Create backtesting instance
    return DuckDBBacktesting(
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        db_path=output_db_path,
        **kwargs
    )


def create_performance_comparison_report(
    pandas_data,
    duckdb_data: DuckDBData,
    test_queries: Optional[List] = None
) -> Dict:
    """
    Generate a performance comparison report between pandas and DuckDB.
    
    Parameters
    ----------
    pandas_data : PandasData
        Pandas-based data source
    duckdb_data : DuckDBData
        DuckDB-based data source
    test_queries : list, optional
        Custom test queries to benchmark
        
    Returns
    -------
    dict
        Performance comparison results
    """
    import psutil
    import time
    
    report = {
        'memory_usage': {},
        'query_performance': {},
        'storage_size': {}
    }
    
    try:
        # Memory usage comparison
        process = psutil.Process()
        
        # Test pandas memory usage
        start_memory = process.memory_info().rss / 1024 / 1024
        # Trigger pandas operations
        if hasattr(pandas_data, '_data_store'):
            for data_obj in pandas_data._data_store.values():
                _ = data_obj.df.memory_usage(deep=True).sum()
        pandas_memory = process.memory_info().rss / 1024 / 1024 - start_memory
        
        # DuckDB memory usage
        duckdb_memory = duckdb_data.get_database_size()
        
        report['memory_usage'] = {
            'pandas_mb': pandas_memory,
            'duckdb_mb': duckdb_memory,
            'improvement_ratio': pandas_memory / duckdb_memory if duckdb_memory > 0 else float('inf')
        }
        
        # Storage size comparison
        report['storage_size'] = {
            'duckdb_mb': duckdb_memory,
            'estimated_pandas_memory_mb': pandas_memory
        }
        
        logger.info(f"Performance comparison completed: {pandas_memory:.1f}MB (pandas) vs {duckdb_memory:.1f}MB (DuckDB)")
        
    except Exception as e:
        logger.error(f"Error generating performance report: {e}")
    
    return report
