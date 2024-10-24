import logging
from datetime import datetime as DateTime

from lumibot.backtesting import PandasDataBacktesting
from lumibot.strategies.strategy import Strategy

from tests.backtest.fixtures import pandas_data_fixture


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class LifecycleLogger(Strategy):

    parameters = {
        "sleeptime": "1D",
        "market": "NYSE",
    }

    def initialize(self, symbol=""):
        self.sleeptime = self.parameters["sleeptime"]
        self.set_market(self.parameters["market"])

    def before_market_opens(self):
        dt = self.get_datetime()
        logger.info(f"{dt} before_market_opens called")

    def before_starting_trading(self):
        dt = self.get_datetime()
        logger.info(f"{dt} before_starting_trading called")

    def on_trading_iteration(self):
        dt = self.get_datetime()
        logger.info(f"{dt} on_trading_iteration called")

    def before_market_closes(self):
        dt = self.get_datetime()
        logger.info(f"{dt} before_market_closes called")

    def after_market_closes(self):
        dt = self.get_datetime()
        logger.info(f"{dt} after_market_closes called")


class TestPandasBacktest:

    def test_pandas_data_fixture(self, pandas_data_fixture):
        assert pandas_data_fixture is not None

    def test_pandas_datasource_with_daily_data_in_backtest(self, pandas_data_fixture):
        strategy_name = "LifecycleLogger"
        strategy_class = LifecycleLogger
        backtesting_start = DateTime(2019, 1, 14)
        backtesting_end = DateTime(2019, 1, 20)

        # Replace the strategy name now that it's known.
        for data in pandas_data_fixture.values():
            data.strategy = strategy_name

        result = strategy_class.backtest(
            datasource_class=PandasDataBacktesting,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            pandas_data=list(pandas_data_fixture.values()),
            risk_free_rate=0,
            show_plot=False,
            save_tearsheet=False,
            show_tearsheet=False,
            show_indicators=False,
            save_logfile=False,
            name=strategy_name,
            budget=40000,
            show_progress_bar=False,
            quiet_logs=False,
        )
        logger.info(f"Result: {result}")
        assert result is not None
