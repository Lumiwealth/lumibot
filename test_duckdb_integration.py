"""
Tests for DuckDB integration in Lumibot

This module provides comprehensive tests for the DuckDB data source
and backtesting implementations.
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import tempfile
import os

from lumibot.entities import Asset, Data
from lumibot.data_sources.duckdb_data import DuckDBData
from lumibot.backtesting.duckdb_backtesting import DuckDBBacktesting
from lumibot.tools.duckdb_migration import DataMigrationUtility


class TestDuckDBData:
    """Test cases for DuckDBData class"""
    
    @pytest.fixture
    def sample_data(self):
        """Generate sample OHLCV data for testing"""
        dates = pd.date_range(start="2023-01-01", end="2023-01-31", freq="1D")
        
        # Generate realistic price data
        np.random.seed(42)
        prices = 100 + np.cumsum(np.random.normal(0, 1, len(dates)))
        
        df = pd.DataFrame({
            'open': prices * (1 + np.random.normal(0, 0.001, len(dates))),
            'high': prices * (1 + np.abs(np.random.normal(0, 0.002, len(dates)))),
            'low': prices * (1 - np.abs(np.random.normal(0, 0.002, len(dates)))),
            'close': prices,
            'volume': np.random.randint(1000, 10000, len(dates))
        }, index=dates)
        
        # Ensure OHLC consistency
        df['high'] = np.maximum(df['high'], np.maximum(df['open'], df['close']))
        df['low'] = np.minimum(df['low'], np.minimum(df['open'], df['close']))
        
        return df
    
    @pytest.fixture
    def temp_db_path(self):
        """Create temporary database path"""
        temp_dir = tempfile.mkdtemp()
        db_path = Path(temp_dir) / "test.duckdb"
        yield str(db_path)
        # Cleanup
        if db_path.exists():
            db_path.unlink()
        os.rmdir(temp_dir)
    
    def test_database_creation(self, temp_db_path):
        """Test database creation and schema setup"""
        data_source = DuckDBData(
            db_path=temp_db_path,
            datetime_start=datetime(2023, 1, 1),
            datetime_end=datetime(2023, 12, 31)
        )
        
        # Check that database file was created
        assert Path(temp_db_path).exists()
        
        # Check that tables were created
        tables = data_source.conn.execute("SHOW TABLES").fetchall()
        table_names = [table[0] for table in tables]
        
        assert 'ohlcv_data' in table_names
        assert 'data_metadata' in table_names
        
        data_source.close()
    
    def test_data_loading(self, sample_data, temp_db_path):
        """Test loading pandas data into DuckDB"""
        asset = Asset(symbol="TEST", asset_type="stock")
        data_obj = Data(asset=asset, df=sample_data, timestep="day")
        pandas_data = {asset: data_obj}
        
        data_source = DuckDBData(
            db_path=temp_db_path,
            datetime_start=datetime(2023, 1, 1),
            datetime_end=datetime(2023, 12, 31),
            pandas_data=pandas_data
        )
        
        # Check that data was loaded
        symbols = data_source.get_symbols()
        assert "TEST" in symbols
        
        # Check record count
        result = data_source.conn.execute("SELECT COUNT(*) FROM ohlcv_data").fetchone()
        assert result and result[0] == len(sample_data)
        
        data_source.close()
    
    def test_get_last_price(self, sample_data, temp_db_path):
        """Test retrieving last price"""
        asset = Asset(symbol="TEST", asset_type="stock")
        data_obj = Data(asset=asset, df=sample_data, timestep="day")
        pandas_data = {asset: data_obj}
        
        data_source = DuckDBData(
            db_path=temp_db_path,
            datetime_start=datetime(2023, 1, 1),
            datetime_end=datetime(2023, 12, 31),
            pandas_data=pandas_data
        )
        
        # Set current datetime to last data point
        data_source._update_datetime(sample_data.index[-1])
        
        last_price = data_source.get_last_price(asset)
        expected_price = sample_data['close'].iloc[-1]
        
        assert abs(last_price - expected_price) < 0.01
        
        data_source.close()
    
    def test_get_historical_prices(self, sample_data, temp_db_path):
        """Test retrieving historical prices"""
        asset = Asset(symbol="TEST", asset_type="stock")
        data_obj = Data(asset=asset, df=sample_data, timestep="day")
        pandas_data = {asset: data_obj}
        
        data_source = DuckDBData(
            db_path=temp_db_path,
            datetime_start=datetime(2023, 1, 1),
            datetime_end=datetime(2023, 12, 31),
            pandas_data=pandas_data
        )
        
        # Set current datetime to last data point
        data_source._update_datetime(sample_data.index[-1])
        
        # Get last 10 bars
        bars = data_source.get_historical_prices(asset, length=10, timestep="day")
        
        assert bars is not None
        assert len(bars.df) == 10
        assert list(bars.df.columns) == ['open', 'high', 'low', 'close', 'volume']
        
        # Check that data is in ascending order (oldest first)
        assert bars.df.index[0] < bars.df.index[-1]
        
        data_source.close()
    
    def test_get_quote(self, sample_data, temp_db_path):
        """Test retrieving quote information"""
        asset = Asset(symbol="TEST", asset_type="stock")
        data_obj = Data(asset=asset, df=sample_data, timestep="day")
        pandas_data = {asset: data_obj}
        
        data_source = DuckDBData(
            db_path=temp_db_path,
            datetime_start=datetime(2023, 1, 1),
            datetime_end=datetime(2023, 12, 31),
            pandas_data=pandas_data
        )
        
        # Set current datetime to last data point
        data_source._update_datetime(sample_data.index[-1])
        
        quote = data_source.get_quote(asset)
        
        assert quote.asset == asset
        assert quote.price is not None
        assert quote.bid is not None
        assert quote.ask is not None
        assert quote.volume is not None
        
        data_source.close()
    
    def test_batch_operations(self, sample_data, temp_db_path):
        """Test batch operations for multiple assets"""
        assets = [
            Asset(symbol="TEST1", asset_type="stock"),
            Asset(symbol="TEST2", asset_type="stock"),
            Asset(symbol="TEST3", asset_type="stock")
        ]
        
        pandas_data = {}
        for asset in assets:
            # Create slightly different data for each asset
            df = sample_data.copy()
            df['close'] = df['close'] * (1 + np.random.normal(0, 0.1, len(df)))
            data_obj = Data(asset=asset, df=df, timestep="day")
            pandas_data[asset] = data_obj
        
        data_source = DuckDBData(
            db_path=temp_db_path,
            datetime_start=datetime(2023, 1, 1),
            datetime_end=datetime(2023, 12, 31),
            pandas_data=pandas_data
        )
        
        # Set current datetime
        data_source._update_datetime(sample_data.index[-1])
        
        # Test batch price retrieval
        prices = data_source.get_last_prices(assets)
        
        assert len(prices) == len(assets)
        for asset in assets:
            assert asset in prices
            assert prices[asset] is not None
        
        data_source.close()
    
    def test_memory_database(self, sample_data):
        """Test in-memory database functionality"""
        asset = Asset(symbol="TEST", asset_type="stock")
        data_obj = Data(asset=asset, df=sample_data, timestep="day")
        pandas_data = {asset: data_obj}
        
        data_source = DuckDBData(
            memory_db=True,
            datetime_start=datetime(2023, 1, 1),
            datetime_end=datetime(2023, 12, 31),
            pandas_data=pandas_data
        )
        
        # Test basic operations
        symbols = data_source.get_symbols()
        assert "TEST" in symbols
        
        data_source._update_datetime(sample_data.index[-1])
        last_price = data_source.get_last_price(asset)
        assert last_price is not None
        
        data_source.close()


class TestDuckDBBacktesting:
    """Test cases for DuckDBBacktesting class"""
    
    @pytest.fixture
    def sample_data(self):
        """Generate sample data for backtesting"""
        dates = pd.date_range(start="2023-01-01", end="2023-03-31", freq="1D")
        
        np.random.seed(42)
        prices = 100 + np.cumsum(np.random.normal(0, 1, len(dates)))
        
        df = pd.DataFrame({
            'open': prices,
            'high': prices * 1.02,
            'low': prices * 0.98,
            'close': prices,
            'volume': 1000000
        }, index=dates)
        
        return df
    
    def test_backtesting_initialization(self, sample_data):
        """Test DuckDB backtesting initialization"""
        asset = Asset(symbol="TEST", asset_type="stock")
        data_obj = Data(asset=asset, df=sample_data, timestep="day")
        pandas_data = {asset: data_obj}
        
        backtesting = DuckDBBacktesting(
            datetime_start=datetime(2023, 1, 1),
            datetime_end=datetime(2023, 3, 31),
            memory_db=True,
            pandas_data=pandas_data
        )
        
        # Test that backtesting-specific optimizations were applied
        symbols = backtesting.get_symbols()
        assert "TEST" in symbols
        
        backtesting.close()
    
    def test_prefetch_optimization(self, sample_data):
        """Test data prefetching for backtesting"""
        assets = [Asset(symbol="TEST", asset_type="stock")]
        data_obj = Data(asset=assets[0], df=sample_data, timestep="day")
        pandas_data = {assets[0]: data_obj}
        
        backtesting = DuckDBBacktesting(
            datetime_start=datetime(2023, 1, 1),
            datetime_end=datetime(2023, 3, 31),
            memory_db=True,
            pandas_data=pandas_data
        )
        
        # Test prefetch functionality
        backtesting.prefetch_data_for_backtest(assets, timestep="day")
        
        # Verify that view was created
        views = backtesting.conn.execute("SHOW TABLES").fetchall()
        view_names = [view[0] for view in views]
        assert 'backtest_data' in view_names
        
        backtesting.close()


class TestDataMigration:
    """Test cases for data migration utilities"""
    
    @pytest.fixture
    def sample_pandas_data(self):
        """Create sample pandas data for migration testing"""
        dates = pd.date_range(start="2023-01-01", end="2023-01-10", freq="1D")
        
        assets_data = {}
        for symbol in ["AAPL", "MSFT", "GOOGL"]:
            np.random.seed(hash(symbol) % 2**32)
            prices = 100 + np.cumsum(np.random.normal(0, 1, len(dates)))
            
            df = pd.DataFrame({
                'open': prices,
                'high': prices * 1.01,
                'low': prices * 0.99,
                'close': prices,
                'volume': 1000000
            }, index=dates)
            
            asset = Asset(symbol=symbol, asset_type="stock")
            data_obj = Data(asset=asset, df=df, timestep="day")
            assets_data[asset] = data_obj
        
        return assets_data
    
    @pytest.fixture
    def temp_migration_db(self):
        """Create temporary database for migration testing"""
        temp_dir = tempfile.mkdtemp()
        db_path = Path(temp_dir) / "migration_test.duckdb"
        yield str(db_path)
        # Cleanup
        if db_path.exists():
            db_path.unlink()
        os.rmdir(temp_dir)
    
    def test_pandas_data_migration(self, sample_pandas_data, temp_migration_db):
        """Test migration from pandas data to DuckDB"""
        migrator = DataMigrationUtility(temp_migration_db)
        
        duckdb_data = migrator.migrate_pandas_data_to_duckdb(
            sample_pandas_data,
            validate_data=True,
            chunk_size=1000
        )
        
        # Verify migration statistics
        stats = migrator.migration_stats
        assert stats['symbols_migrated'] == 3
        assert stats['total_records'] == 30  # 3 symbols * 10 days
        assert len(stats['errors']) == 0
        
        # Verify data accessibility
        symbols = duckdb_data.get_symbols()
        assert set(symbols) == {"AAPL", "MSFT", "GOOGL"}
        
        duckdb_data.close()
    
    def test_migration_validation(self, sample_pandas_data, temp_migration_db):
        """Test migration validation functionality"""
        migrator = DataMigrationUtility(temp_migration_db)
        
        # Migrate data
        duckdb_data = migrator.migrate_pandas_data_to_duckdb(
            sample_pandas_data,
            validate_data=True
        )
        
        # Check that validation passed (no errors in stats)
        assert len(migrator.migration_stats['errors']) == 0
        
        # Verify data integrity
        for asset, original_data in sample_pandas_data.items():
            # Check that all records were migrated
            query = "SELECT COUNT(*) FROM ohlcv_data WHERE symbol = ?"
            result = duckdb_data.conn.execute(query, [asset.symbol]).fetchone()
            assert result and result[0] == len(original_data.df)
        
        duckdb_data.close()


class TestIntegration:
    """Integration tests for full workflow"""
    
    def test_end_to_end_workflow(self):
        """Test complete workflow from data loading to querying"""
        # Create sample data
        dates = pd.date_range(start="2023-01-01", end="2023-01-31", freq="1D")
        np.random.seed(42)
        
        df = pd.DataFrame({
            'open': 100 + np.random.normal(0, 1, len(dates)),
            'high': 102 + np.random.normal(0, 1, len(dates)),
            'low': 98 + np.random.normal(0, 1, len(dates)),
            'close': 100 + np.random.normal(0, 1, len(dates)),
            'volume': 1000000
        }, index=dates)
        
        asset = Asset(symbol="TEST", asset_type="stock")
        data_obj = Data(asset=asset, df=df, timestep="day")
        
        # Test with memory database
        data_source = DuckDBData(
            memory_db=True,
            datetime_start=datetime(2023, 1, 1),
            datetime_end=datetime(2023, 1, 31),
            pandas_data={asset: data_obj}
        )
        
        # Test all major operations
        symbols = data_source.get_symbols()
        assert "TEST" in symbols
        
        data_source._update_datetime(dates[-1])
        
        last_price = data_source.get_last_price(asset)
        assert last_price is not None
        
        bars = data_source.get_historical_prices(asset, length=5, timestep="day")
        assert len(bars.df) == 5
        
        quote = data_source.get_quote(asset)
        assert quote.asset == asset
        
        # Test database size reporting
        size = data_source.get_database_size()
        assert size >= 0
        
        data_source.close()


if __name__ == "__main__":
    # Run basic tests if executed directly
    import sys
    
    print("Running DuckDB integration tests...")
    
    # Basic smoke test
    try:
        from lumibot.data_sources.duckdb_data import DuckDBData
        print("✓ DuckDBData import successful")
        
        from lumibot.backtesting.duckdb_backtesting import DuckDBBacktesting
        print("✓ DuckDBBacktesting import successful")
        
        from lumibot.tools.duckdb_migration import DataMigrationUtility
        print("✓ Migration utilities import successful")
        
        # Test basic functionality
        data_source = DuckDBData(
            memory_db=True,
            datetime_start=datetime(2023, 1, 1),
            datetime_end=datetime(2023, 1, 31)
        )
        
        # Test database creation
        symbols = data_source.get_symbols()
        print(f"✓ Database creation successful, {len(symbols)} symbols")
        
        data_source.close()
        print("✓ All basic tests passed")
        
    except Exception as e:
        print(f"✗ Test failed: {e}")
        sys.exit(1)
    
    print("\nRun 'pytest test_duckdb_integration.py' for comprehensive testing.")
