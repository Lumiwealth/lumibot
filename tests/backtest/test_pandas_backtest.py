import logging
from datetime import datetime as DateTime

from lumibot.backtesting import PandasDataBacktesting
from lumibot.example_strategies.lifecycle_logger import LifecycleLogger

from tests.fixtures import pandas_data_fixture


logger = logging.getLogger(__name__)


class TestPandasBacktest:

    def test_pandas_data_fixture(self, pandas_data_fixture):
        assert pandas_data_fixture is not None

    def test_pandas_datasource_with_daily_data_in_backtest(self, pandas_data_fixture):
        strategy_name = "LifecycleLogger"
        strategy_class = LifecycleLogger
        backtesting_start = DateTime(2019, 1, 14)
        backtesting_end = DateTime(2019, 1, 20)

        result = strategy_class.backtest(
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
            budget=40000,
            show_progress_bar=False,
            quiet_logs=False,
        )
        logger.info(f"Result: {result}")
        assert result is not None
