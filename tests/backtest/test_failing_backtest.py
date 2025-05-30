import datetime
import pytest
import subprocess
import sys
import os
import tempfile

from lumibot.backtesting import BacktestingBroker, YahooDataBacktesting
from lumibot.strategies import Strategy
from lumibot.traders import Trader


class FailingInitializeStrategy(Strategy):
    """Strategy that fails during initialize() to test error handling"""
    
    parameters = {
        "symbol": "SPY",
    }

    def initialize(self):
        self.sleeptime = "1D"
        # This should cause the backtest to fail
        raise ValueError("Intentional error in initialize() for testing")

    def on_trading_iteration(self):
        # This shouldn't be reached due to initialize failing
        symbol = self.parameters["symbol"]
        self.last_price = self.get_last_price(symbol)


class FailingTradingIterationStrategy(Strategy):
    """Strategy that fails during on_trading_iteration() to test error handling"""
    
    parameters = {
        "symbol": "SPY",
    }

    def initialize(self):
        self.sleeptime = "1D"
        # Initialize should work fine

    def on_trading_iteration(self):
        # This should cause the backtest to fail
        raise RuntimeError("Intentional error in on_trading_iteration() for testing")


class TestFailingBacktest:
    """Test that backtests properly fail and return non-zero exit codes when errors occur"""

    def test_initialize_failure_raises_exception(self):
        """
        Test that a strategy failing in initialize() raises an exception during backtest
        """
        backtesting_start = datetime.datetime(2023, 11, 1)
        backtesting_end = datetime.datetime(2023, 11, 2)

        data_source = YahooDataBacktesting(
            datetime_start=backtesting_start,
            datetime_end=backtesting_end,
        )

        broker = BacktestingBroker(data_source=data_source)

        failing_strategy = FailingInitializeStrategy(
            broker=broker,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
        )

        trader = Trader(logfile="", backtest=True)
        trader.add_strategy(failing_strategy)
        
        # This should raise an exception
        with pytest.raises(Exception) as exc_info:
            trader.run_all(show_plot=False, show_tearsheet=False, save_tearsheet=False, tearsheet_file="")
        
        # Verify the error message contains our intentional error
        assert "Intentional error in initialize() for testing" in str(exc_info.value)

    def test_trading_iteration_failure_raises_exception(self):
        """
        Test that a strategy failing in on_trading_iteration() raises an exception during backtest
        """
        backtesting_start = datetime.datetime(2023, 11, 1)
        backtesting_end = datetime.datetime(2023, 11, 2)

        data_source = YahooDataBacktesting(
            datetime_start=backtesting_start,
            datetime_end=backtesting_end,
        )

        broker = BacktestingBroker(data_source=data_source)

        failing_strategy = FailingTradingIterationStrategy(
            broker=broker,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
        )

        trader = Trader(logfile="", backtest=True)
        trader.add_strategy(failing_strategy)
        
        # This should raise an exception
        with pytest.raises(Exception) as exc_info:
            trader.run_all(show_plot=False, show_tearsheet=False, save_tearsheet=False, tearsheet_file="")
        
        # Verify the error message contains our intentional error
        assert "Intentional error in on_trading_iteration() for testing" in str(exc_info.value)

    def test_backtest_classmethod_initialize_failure(self):
        """
        Test that using the Strategy.backtest() classmethod properly handles initialize failures
        """
        backtesting_start = datetime.datetime(2023, 11, 1)
        backtesting_end = datetime.datetime(2023, 11, 2)

        # This should raise an exception when using the backtest() classmethod
        with pytest.raises(Exception) as exc_info:
            FailingInitializeStrategy.backtest(
                datasource_class=YahooDataBacktesting,
                backtesting_start=backtesting_start,
                backtesting_end=backtesting_end,
                show_plot=False,
                show_tearsheet=False,
                save_tearsheet=False,
            )
        
        assert "Intentional error in initialize() for testing" in str(exc_info.value)

    def test_backtest_classmethod_trading_iteration_failure(self):
        """
        Test that using the Strategy.backtest() classmethod properly handles trading iteration failures
        """
        backtesting_start = datetime.datetime(2023, 11, 1)
        backtesting_end = datetime.datetime(2023, 11, 2)

        # This should raise an exception when using the backtest() classmethod
        with pytest.raises(Exception) as exc_info:
            FailingTradingIterationStrategy.backtest(
                datasource_class=YahooDataBacktesting,
                backtesting_start=backtesting_start,
                backtesting_end=backtesting_end,
                show_plot=False,
                show_tearsheet=False,
                save_tearsheet=False,
            )
        
        assert "Intentional error in on_trading_iteration() for testing" in str(exc_info.value)

    def test_script_exit_code_initialize_failure(self):
        """
        Test that a script using backtest() returns non-zero exit code when initialize fails
        This tests the actual script execution to verify exit codes
        """
        # Create a temporary script that uses the failing strategy
        script_content = '''
import sys
import os
import datetime

# Add the lumibot path (adjust as needed)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../"))

from lumibot.strategies import Strategy
from lumibot.backtesting import YahooDataBacktesting
from lumibot.entities import Asset, TradingFee

class FailingInitializeStrategy(Strategy):
    def initialize(self):
        self.sleeptime = "1D"
        raise ValueError("Intentional error in initialize() for testing")

    def on_trading_iteration(self):
        pass

if __name__ == "__main__":
    try:
        FailingInitializeStrategy.backtest(
            datasource_class=YahooDataBacktesting,
            backtesting_start=datetime.datetime(2023, 11, 1),
            backtesting_end=datetime.datetime(2023, 11, 2),
            show_plot=False,
            show_tearsheet=False,
            save_tearsheet=False,
            budget=100000
        )
        # If we get here, the backtest didn't fail as expected
        sys.exit(0)
    except Exception as e:
        print(f"Backtest failed as expected: {e}")
        sys.exit(1)  # This is what should happen
'''
        
        # Write the script to a temporary file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write(script_content)
            temp_script = f.name

        try:
            # Run the script and check the exit code
            result = subprocess.run([sys.executable, temp_script], 
                                  capture_output=True, text=True, cwd=os.path.dirname(__file__))
            
            # The script should exit with code 1 (failure) when the backtest fails
            assert result.returncode == 1, f"Expected exit code 1, got {result.returncode}. STDOUT: {result.stdout}, STDERR: {result.stderr}"
            assert "Backtest failed as expected" in result.stdout, f"Expected failure message not found in output: {result.stdout}"
            
        finally:
            # Clean up the temporary file
            os.unlink(temp_script)

    def test_script_exit_code_trading_iteration_failure(self):
        """
        Test that a script using backtest() returns non-zero exit code when trading iteration fails
        """
        # Create a temporary script that uses the failing strategy
        script_content = '''
import sys
import os
import datetime

# Add the lumibot path (adjust as needed)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../"))

from lumibot.strategies import Strategy
from lumibot.backtesting import YahooDataBacktesting
from lumibot.entities import Asset, TradingFee

class FailingTradingIterationStrategy(Strategy):
    def initialize(self):
        self.sleeptime = "1D"

    def on_trading_iteration(self):
        raise RuntimeError("Intentional error in on_trading_iteration() for testing")

if __name__ == "__main__":
    try:
        FailingTradingIterationStrategy.backtest(
            datasource_class=YahooDataBacktesting,
            backtesting_start=datetime.datetime(2023, 11, 1),
            backtesting_end=datetime.datetime(2023, 11, 2),
            show_plot=False,
            show_tearsheet=False,
            save_tearsheet=False,
            budget=100000
        )
        # If we get here, the backtest didn't fail as expected
        sys.exit(0)
    except Exception as e:
        print(f"Backtest failed as expected: {e}")
        sys.exit(1)  # This is what should happen
'''
        
        # Write the script to a temporary file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write(script_content)
            temp_script = f.name

        try:
            # Run the script and check the exit code
            result = subprocess.run([sys.executable, temp_script], 
                                  capture_output=True, text=True, cwd=os.path.dirname(__file__))
            
            # The script should exit with code 1 (failure) when the backtest fails
            assert result.returncode == 1, f"Expected exit code 1, got {result.returncode}. STDOUT: {result.stdout}, STDERR: {result.stderr}"
            assert "Backtest failed as expected" in result.stdout, f"Expected failure message not found in output: {result.stdout}"
            
        finally:
            # Clean up the temporary file
            os.unlink(temp_script)

    def test_tqqq_strategy_style_failure(self):
        """
        Test a strategy similar to TQQQ MA Filter to ensure it fails properly when errors occur
        This mimics the actual usage pattern from the TQQQ strategy
        """
        # Create a temporary script similar to TQQQ MA Filter.py
        script_content = '''
import sys
import os
import datetime

# Add the lumibot path (similar to TQQQ strategy)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../../../"))

from lumibot.strategies import Strategy
from lumibot.backtesting import YahooDataBacktesting
from lumibot.entities import Asset, TradingFee

class TestTQQQStrategy(Strategy):
    def initialize(self):
        self.tqqq_asset = Asset("TQQQ", asset_type=Asset.AssetType.STOCK)
        self.sleeptime = "1D"

    def on_trading_iteration(self):
        # Simulate the error that was happening with cash
        available_cash = self.get_cash()
        if available_cash is None:
            raise TypeError("get_cash() returned None - this should cause script to fail")
        
        # Force an error to test failure handling
        raise RuntimeError("Simulated TQQQ strategy error for testing")

if __name__ == "__main__":
    from lumibot.credentials import IS_BACKTESTING
    
    # Simulate the IS_BACKTESTING being True
    if True:  # Replace IS_BACKTESTING with True for testing
        trading_fee = TradingFee(percent_fee=0.001)

        try:
            TestTQQQStrategy.backtest(
                datasource_class=YahooDataBacktesting,
                benchmark_asset=Asset("SPY", Asset.AssetType.STOCK),
                buy_trading_fees=[trading_fee],
                sell_trading_fees=[trading_fee],
                parameters={},
                budget=100000,
                backtesting_start=datetime.datetime(2023, 11, 1),
                backtesting_end=datetime.datetime(2023, 11, 2),
                show_plot=False,
                show_tearsheet=False,
                save_tearsheet=False,
            )
            # If we reach here, the backtest didn't fail as expected
            print("ERROR: Backtest should have failed but didn't!")
            sys.exit(1)
        except Exception as e:
            print(f"Backtest failed as expected: {e}")
            sys.exit(0)  # Success - the test worked
'''
        
        # Write the script to a temporary file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
            f.write(script_content)
            temp_script = f.name

        try:
            # Run the script and check the exit code
            result = subprocess.run([sys.executable, temp_script], 
                                  capture_output=True, text=True, cwd=os.path.dirname(__file__))
            
            # For this test, exit code 0 means the test worked (backtest failed as expected)
            # If the exit code is 1, it means the backtest didn't fail when it should have
            assert result.returncode == 0, f"Expected exit code 0 (test passed), got {result.returncode}. This means the backtest didn't fail when it should have. STDOUT: {result.stdout}, STDERR: {result.stderr}"
            assert "Backtest failed as expected" in result.stdout, f"Expected failure message not found in output: {result.stdout}"
            
        finally:
            # Clean up the temporary file
            os.unlink(temp_script) 