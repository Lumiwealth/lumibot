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

