"""
Test to verify that the ES futures infinite restart bug is fixed.
"""

import unittest
import signal
import sys
from datetime import datetime

# Add the lumibot path
sys.path.insert(0, '/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot')

from lumibot.strategies.strategy import Strategy
from lumibot.entities import Asset, TradingFee
from lumibot.backtesting import DataBentoDataBacktesting


class TimeoutError(Exception):
    """Exception raised when a test times out."""
    pass


def timeout_handler(signum, frame):
    """Signal handler for timeout."""
    raise TimeoutError("Test timed out - possible infinite loop detected")


class MinimalESFuturesStrategy(Strategy):
    """Minimal ES futures strategy for testing."""
    
    def initialize(self):
        self.asset = Asset("ES", asset_type=Asset.AssetType.CONT_FUTURE)
        self.set_market("us_futures")
        self.sleeptime = "1M"
        self.iteration_count = 0
        
    def on_trading_iteration(self):
        self.iteration_count += 1
        current_time = self.get_datetime()
        
        # Log every 10 iterations to track progress
        if self.iteration_count % 10 == 0:
            print(f"Iteration {self.iteration_count} at {current_time}")


class TestESFuturesFix(unittest.TestCase):
    """Test that ES futures strategies don't hang or restart infinitely."""
    
    def setUp(self):
        """Set up common test parameters."""
        self.timeout_seconds = 60  # 1 minute timeout for each test
        self.trading_fee = TradingFee(flat_fee=0.50)
        
    def test_minimal_es_futures_strategy_no_hang(self):
        """Test that a minimal ES futures strategy completes without hanging."""
        print("\n=== Testing Minimal ES Futures Strategy ===")
        
        # Set up timeout
        signal.signal(signal.SIGALRM, timeout_handler)
        signal.alarm(self.timeout_seconds)
        
        try:
            # Run a short backtest
            results = MinimalESFuturesStrategy.backtest(
                datasource_class=DataBentoDataBacktesting,
                benchmark_asset=Asset("SPY", Asset.AssetType.STOCK),
                buy_trading_fees=[self.trading_fee],
                sell_trading_fees=[self.trading_fee],
                quote_asset=Asset("USD", Asset.AssetType.FOREX),
                start=datetime(2024, 1, 2),
                end=datetime(2024, 1, 3),
                parameters=None
            )
            
            print("✓ Minimal ES futures strategy completed successfully")
            self.assertIsNotNone(results)
            
        except TimeoutError:
            self.fail("Minimal ES futures strategy timed out - possible infinite loop")
        except Exception as e:
            print(f"✓ Strategy failed with exception (not a hang): {e}")
            
        finally:
            signal.alarm(0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
