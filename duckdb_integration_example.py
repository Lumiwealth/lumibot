"""
Example demonstrating DuckDB integration in Lumibot

This script shows how to use the new DuckDB-based data storage for improved
performance in backtesting scenarios.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pathlib import Path
import logging

# Import Lumibot components
from lumibot.strategies import Strategy
from lumibot.backtesting import BacktestingBroker
from lumibot.entities import Asset, Data
from lumibot.data_sources.duckdb_data import DuckDBData
from lumibot.backtesting.duckdb_backtesting import DuckDBBacktesting
from lumibot.tools.duckdb_migration import DataMigrationUtility, create_performance_comparison_report

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SimpleMovingAverageStrategy(Strategy):
    """Simple strategy for demonstration"""
    
    parameters = {
        "short_window": 10,
        "long_window": 30
    }
    
    def initialize(self, parameters=None):
        self.sleeptime = "1D"  # Daily strategy
        self.asset = Asset(symbol="AAPL", asset_type="stock")  # Default asset
    
    def on_trading_iteration(self):
        # Get historical prices
        bars = self.get_historical_prices(
            self.asset, 
            length=self.parameters["long_window"], 
            timestep="day"
        )
        
        if bars is None or len(bars.df) < self.parameters["long_window"]:
            return
        
        # Calculate moving averages
        short_ma = bars.df['close'].tail(self.parameters["short_window"]).mean()
        long_ma = bars.df['close'].mean()
        
        # Simple crossover strategy
        current_position = self.get_position(self.asset)
        
        if short_ma > long_ma and (not current_position or current_position.quantity == 0):
            # Buy signal
            order = self.create_order(self.asset, 100, "BUY")
            self.submit_order(order)
        elif short_ma < long_ma and current_position and current_position.quantity > 0:
            # Sell signal
            self.sell_all()


def generate_sample_data(symbols, start_date, end_date, freq='1D'):
    """Generate sample OHLCV data for demonstration"""
    
    date_range = pd.date_range(start=start_date, end=end_date, freq=freq)
    data_dict = {}
    
    for symbol in symbols:
        # Generate realistic price data
        np.random.seed(hash(symbol) % 2**32)  # Consistent seed per symbol
        
        # Random walk for price
        n_periods = len(date_range)
        price_base = 100 + (hash(symbol) % 100)  # Different base price per symbol
        
        returns = np.random.normal(0.0005, 0.02, n_periods)  # Daily returns
        prices = price_base * np.exp(np.cumsum(returns))
        
        # Generate OHLCV data
        df = pd.DataFrame({
            'open': prices * (1 + np.random.normal(0, 0.001, n_periods)),
            'high': prices * (1 + np.abs(np.random.normal(0, 0.01, n_periods))),
            'low': prices * (1 - np.abs(np.random.normal(0, 0.01, n_periods))),
            'close': prices,
            'volume': np.random.randint(100000, 1000000, n_periods)
        }, index=date_range)
        
        # Ensure OHLC consistency
        df['high'] = np.maximum(df['high'], np.maximum(df['open'], df['close']))
        df['low'] = np.minimum(df['low'], np.minimum(df['open'], df['close']))
        
        # Create Asset and Data objects
        asset = Asset(symbol=symbol, asset_type="stock")
        data_obj = Data(asset=asset, df=df, timestep="day")
        
        data_dict[asset] = data_obj
    
    return data_dict


def demonstrate_duckdb_integration():
    """Demonstrate the DuckDB integration"""
    
    print("=" * 60)
    print("LUMIBOT DUCKDB INTEGRATION DEMONSTRATION")
    print("=" * 60)
    
    # Configuration
    symbols = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
    start_date = datetime(2022, 1, 1)
    end_date = datetime(2023, 12, 31)
    backtest_start = datetime(2023, 1, 1)
    backtest_end = datetime(2023, 6, 30)
    
    print(f"Generating sample data for {len(symbols)} symbols...")
    print(f"Data range: {start_date.date()} to {end_date.date()}")
    print(f"Backtest range: {backtest_start.date()} to {backtest_end.date()}")
    
    # Generate sample data
    sample_data = generate_sample_data(symbols, start_date, end_date)
    
    print(f"✓ Generated data for {len(sample_data)} assets")
    
    # Method 1: Direct DuckDB usage
    print("\n1. Direct DuckDB Data Source Usage")
    print("-" * 40)
    
    db_path = "lumibot_example.duckdb"
    
    # Create DuckDB data source
    duckdb_data = DuckDBData(
        db_path=db_path,
        datetime_start=start_date,
        datetime_end=end_date,
        pandas_data=sample_data
    )
    
    print(f"✓ Created DuckDB database: {db_path}")
    print(f"✓ Database size: {duckdb_data.get_database_size():.2f} MB")
    print(f"✓ Symbols loaded: {len(duckdb_data.get_symbols())}")
    
    # Test some queries
    test_asset = Asset(symbol='AAPL', asset_type='stock')
    
    # Get last price
    last_price = duckdb_data.get_last_price(test_asset)
    print(f"✓ Last price for AAPL: ${last_price:.2f}")
    
    # Get historical data
    bars = duckdb_data.get_historical_prices(test_asset, length=30, timestep="day")
    print(f"✓ Retrieved {len(bars.df)} historical bars for AAPL")
    
    # Method 2: DuckDB Backtesting
    print("\n2. DuckDB Backtesting Usage")
    print("-" * 40)
    
    # Create DuckDB backtesting instance
    duckdb_backtesting = DuckDBBacktesting(
        datetime_start=backtest_start,
        datetime_end=backtest_end,
        db_path=db_path,  # Reuse existing database
        pandas_data=sample_data
    )
    
    # Create broker and strategy
    broker = BacktestingBroker(duckdb_backtesting)
    strategy = SimpleMovingAverageStrategy()
    
    # Set up strategy
    strategy.asset = test_asset
    strategy.broker = broker
    
    print(f"✓ Created DuckDB backtesting environment")
    print(f"✓ Strategy: {strategy.__class__.__name__}")
    print(f"✓ Asset: {test_asset.symbol}")
    
    # Run a quick test (simplified)
    try:
        strategy.initialize()
        print("✓ Strategy initialized successfully")
        
        # Test a few iterations
        for i in range(5):
            test_date = backtest_start + timedelta(days=i*10)
            duckdb_backtesting._update_datetime(test_date)
            
            # Test data access
            bars = strategy.get_historical_prices(test_asset, length=10, timestep="day")
            if bars and not bars.df.empty:
                current_price = bars.df['close'].iloc[-1]
                print(f"  Day {i+1}: {test_date.date()} - Price: ${current_price:.2f}")
        
        print("✓ Backtesting data access verified")
        
    except Exception as e:
        print(f"⚠ Error during strategy test: {e}")
    
    # Method 3: Migration from existing pandas data
    print("\n3. Migration from Pandas Data")
    print("-" * 40)
    
    # Simulate migration scenario
    migration_db_path = "migrated_data.duckdb"
    migrator = DataMigrationUtility(migration_db_path)
    migrated_duckdb = None
    
    try:
        # Migrate data
        migrated_duckdb = migrator.migrate_pandas_data_to_duckdb(
            sample_data,
            validate_data=True,
            chunk_size=5000
        )
        
        print(f"✓ Migration completed successfully")
        print(f"✓ Migrated database size: {migrated_duckdb.get_database_size():.2f} MB")
        
        # Show migration statistics
        stats = migrator.migration_stats
        print(f"✓ Records migrated: {stats['total_records']:,}")
        print(f"✓ Symbols migrated: {stats['symbols_migrated']}")
        
        if stats['end_time'] and stats['start_time']:
            duration = stats['end_time'] - stats['start_time']
            print(f"✓ Migration time: {duration:.2f} seconds")
        
    except Exception as e:
        print(f"⚠ Migration error: {e}")
    
    # Method 4: Performance comparison
    print("\n4. Performance Insights")
    print("-" * 40)
    
    # Display data information
    data_info = duckdb_data.get_data_info()
    if not data_info.empty:
        print("Data Summary:")
        for _, row in data_info.iterrows():
            print(f"  {row['symbol']}: {row['record_count']:,} records "
                  f"({row['first_timestamp'].date()} to {row['last_timestamp'].date()})")
    
    # Performance benefits summary
    total_records = sum(len(data.df) for data in sample_data.values())
    estimated_pandas_memory = total_records * 0.0001  # Rough estimate
    
    print(f"\nPerformance Benefits:")
    print(f"  • Total records: {total_records:,}")
    print(f"  • DuckDB storage: {duckdb_data.get_database_size():.2f} MB")
    print(f"  • Estimated pandas memory: {estimated_pandas_memory:.2f} MB")
    print(f"  • Storage efficiency: {estimated_pandas_memory/duckdb_data.get_database_size():.1f}x")
    
    print(f"\nQuery Performance Benefits:")
    print(f"  • SQL-based filtering (vs pandas boolean indexing)")
    print(f"  • Indexed timestamp searches")
    print(f"  • Memory-efficient data access")
    print(f"  • Persistent storage across runs")
    
    # Cleanup
    duckdb_data.close()
    if migrated_duckdb and hasattr(migrated_duckdb, 'close'):
        migrated_duckdb.close()
    
    print("\n" + "=" * 60)
    print("DEMONSTRATION COMPLETED SUCCESSFULLY")
    print("=" * 60)
    
    print(f"\nNext Steps:")
    print(f"  1. Replace PandasData with DuckDBData in your strategies")
    print(f"  2. Use migration utility for existing data")
    print(f"  3. Leverage SQL queries for complex data analysis")
    print(f"  4. Monitor performance improvements in large backtests")
    
    # Clean up database files
    try:
        if Path(db_path).exists():
            Path(db_path).unlink()
        if Path(migration_db_path).exists():
            Path(migration_db_path).unlink()
        print(f"\n✓ Cleaned up demonstration files")
    except Exception as e:
        print(f"⚠ Cleanup warning: {e}")


def show_usage_examples():
    """Show practical usage examples"""
    
    print("\n" + "=" * 60)
    print("USAGE EXAMPLES")
    print("=" * 60)
    
    print("""
1. Basic DuckDB Data Source Usage:

    from lumibot.data_sources.duckdb_data import DuckDBData
    from lumibot.entities import Asset, Data
    
    # Create data source
    data_source = DuckDBData(
        db_path="my_backtest.duckdb",
        datetime_start=datetime(2023, 1, 1),
        datetime_end=datetime(2023, 12, 31)
    )
    
    # Load your pandas data
    data_source._load_pandas_data(my_pandas_data)
    
2. DuckDB Backtesting:

    from lumibot.backtesting.duckdb_backtesting import DuckDBBacktesting
    from lumibot.backtesting import BacktestingBroker
    
    # Create backtesting data source
    data_source = DuckDBBacktesting(
        datetime_start=datetime(2023, 1, 1),
        datetime_end=datetime(2023, 12, 31),
        pandas_data=my_data_dict
    )
    
    # Create broker and run strategy
    broker = BacktestingBroker(data_source)
    strategy = MyStrategy()
    # ... run backtest ...
    
3. Migration from Existing Data:

    from lumibot.tools.duckdb_migration import DataMigrationUtility
    
    # Migrate existing pandas data
    migrator = DataMigrationUtility("new_db.duckdb")
    duckdb_data = migrator.migrate_pandas_data_to_duckdb(
        existing_pandas_data,
        validate_data=True
    )
    
4. Performance Optimization:

    # Enable optimizations for large datasets
    data_source = DuckDBData(
        db_path="large_dataset.duckdb",
        cache_size="2GB",
        threads=8,
        memory_db=False  # Use disk storage
    )
    
    # Prefetch data for backtesting
    data_source.prefetch_data_for_backtest(assets, timestep="minute")
""")


if __name__ == "__main__":
    demonstrate_duckdb_integration()
    show_usage_examples()
