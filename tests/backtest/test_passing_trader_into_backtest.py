import datetime
import logging

from lumibot.traders.trader import Trader
from lumibot.traders.debug_log_trader import DebugLogTrader
from lumibot.backtesting import PandasDataBacktesting
from lumibot.example_strategies.lifecycle_logger import LifecycleLogger

from tests.fixtures import pandas_data_fixture

logger = logging.getLogger(__name__)


class TestPassingTraderIntoBacktest:

    def test_not_passing_trader_class_into_backtest_creates_generic_trader(self, pandas_data_fixture):
        # When we create a backtest and don't pass in a trader_class, the trader it creates should be a Trader object
        strategy_name = "LifecycleLogger_with_default_trader"
        strategy_class = LifecycleLogger
        backtesting_start = datetime.datetime(2019, 1, 14)
        backtesting_end = datetime.datetime(2019, 1, 20)

        parameters = {
            "sleeptime": "1D",
            "market": "NYSE",
        }

        # Replace the strategy name now that it's known.
        for data in pandas_data_fixture:
            data.strategy = strategy_name

        result, strat_obj = strategy_class.run_backtest(
            datasource_class=PandasDataBacktesting,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            pandas_data=pandas_data_fixture,
            risk_free_rate=0,
            show_plot=False,
            save_tearsheet=False,
            show_tearsheet=False,
            show_indicators=False,
            save_logfile=False,
            name=strategy_name,
            budget=10000,
            parameters=parameters,
            show_progress_bar=False,
            quiet_logs=True,
        )
        trader = strat_obj._trader
        assert isinstance(trader, Trader)

    def test_passing_trader_class_into_backtest_creates_trader_class(self, pandas_data_fixture):
        # When we create a backtest and pass in a trader_class, the trader it creates should be an instance of that class
        strategy_name = "LifecycleLogger_with_DebugLogTrader"
        strategy_class = LifecycleLogger
        backtesting_start = datetime.datetime(2019, 1, 14)
        backtesting_end = datetime.datetime(2019, 1, 20)

        parameters = {
            "sleeptime": "1D",
            "market": "NYSE",
        }

        # Replace the strategy name now that it's known.
        for data in pandas_data_fixture:
            data.strategy = strategy_name

        result, strat_obj = strategy_class.run_backtest(
            datasource_class=PandasDataBacktesting,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            pandas_data=pandas_data_fixture,
            risk_free_rate=0,
            show_plot=False,
            save_tearsheet=False,
            show_tearsheet=False,
            show_indicators=False,
            save_logfile=False,
            name=strategy_name,
            budget=10000,
            parameters=parameters,
            show_progress_bar=False,
            quiet_logs=True,
            trader_class=DebugLogTrader,
        )
        trader = strat_obj._trader
        assert isinstance(trader, DebugLogTrader)