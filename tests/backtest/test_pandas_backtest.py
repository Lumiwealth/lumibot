from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any
import logging
import pytest

from lumibot.backtesting import PandasDataBacktesting
from lumibot.example_strategies.lifecycle_logger import LifecycleLogger
from lumibot.strategies import Strategy
from lumibot.entities import Asset

from tests.fixtures import (
    pandas_data_fixture,
    pandas_data_fixture_amzn_day,
    pandas_data_fixture_amzn_minute,
    BuyOneShareTestStrategy
)

logger = logging.getLogger(__name__)


class TestPandasBacktest:

    def test_pandas_data_fixture(self, pandas_data_fixture):
        assert pandas_data_fixture is not None

    # @pytest.mark.skip()
    def test_pandas_datasource_with_daily_data_in_backtest(self, pandas_data_fixture):
        strategy_name = "LifecycleLogger"
        strategy_class = LifecycleLogger
        backtesting_start = datetime(2019, 1, 14)
        backtesting_end = datetime(2019, 1, 20)

        # Replace the strategy name now that it's known.
        for data in pandas_data_fixture:
            data.strategy = strategy_name

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
            show_progress_bar=False,
            quiet_logs=False,
            parameters={
                "sleeptime": "1D",
            }
        )

    # @pytest.mark.skip()
    def test_day_data(self, pandas_data_fixture_amzn_day):
        strategy_class = BuyOneShareTestStrategy
        backtesting_start = pandas_data_fixture_amzn_day[0].df.index[0]
        backtesting_end = pandas_data_fixture_amzn_day[0].df.index[-1] + timedelta(minutes=1)

        result, strategy = strategy_class.run_backtest(
            datasource_class=PandasDataBacktesting,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            pandas_data=pandas_data_fixture_amzn_day,
            risk_free_rate=0,
            show_plot=False,
            save_tearsheet=False,
            show_tearsheet=False,
            show_indicators=False,
            save_logfile=False,
            show_progress_bar=False,
            quiet_logs=False,
            parameters={
                "sleeptime": "1D",
                "symbol": "AMZN"
            }
        )
        tracker = strategy.tracker
        assert tracker["iteration_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert tracker["submitted_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert tracker["filled_at"].isoformat() ==    '2025-01-13T09:30:00-05:00'

        assert tracker['last_price'] == 218.46  # Close of '2025-01-13T09:30:00-05:00'
        assert tracker["avg_fill_price"] == 220.44  # Open of '2025-01-14T09:30:00-05:00'

    # @pytest.mark.skip()
    def test_minute_data(self, pandas_data_fixture_amzn_minute):
        strategy_class = BuyOneShareTestStrategy
        backtesting_start = pandas_data_fixture_amzn_minute[0].df.index[0]
        backtesting_end = pandas_data_fixture_amzn_minute[0].df.index[-1] + timedelta(minutes=1)

        result, strategy = strategy_class.run_backtest(
            datasource_class=PandasDataBacktesting,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            pandas_data=pandas_data_fixture_amzn_minute,
            risk_free_rate=0,
            show_plot=False,
            save_tearsheet=False,
            show_tearsheet=False,
            show_indicators=False,
            save_logfile=False,
            show_progress_bar=False,
            quiet_logs=False,
            parameters={
                "sleeptime": "1M",
                "symbol": "AMZN"
            }
        )

        tracker = strategy.tracker
        assert tracker["iteration_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert tracker["submitted_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert tracker["filled_at"].isoformat() ==    '2025-01-13T09:30:00-05:00'

        # current prices seem wrong to me
        assert tracker['last_price'] == 218.06  # Open price of '2025-01-13T09:30:00-05:00'
        assert tracker['avg_fill_price'] == 218.06   # Open price of '2025-01-13T09:30:00-05:00'

        # i think it should be:
        # assert tracker['last_price'] == 217.92  # Close price of '2025-01-13T09:30:00-05:00'
        # assert tracker['avg_fill_price'] == 218.0  # Open price of '2025-01-13T09:31:00-05:00'

    # @pytest.mark.skip()
    def test_minute_data_using_60M_sleeptime(self, pandas_data_fixture_amzn_minute):
        strategy_class = BuyOneShareTestStrategy
        backtesting_start = pandas_data_fixture_amzn_minute[0].df.index[0]
        backtesting_end = pandas_data_fixture_amzn_minute[0].df.index[-1] + timedelta(minutes=1)

        result, strategy = strategy_class.run_backtest(
            datasource_class=PandasDataBacktesting,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            pandas_data=pandas_data_fixture_amzn_minute,
            risk_free_rate=0,
            show_plot=False,
            save_tearsheet=False,
            show_tearsheet=False,
            show_indicators=False,
            save_logfile=False,
            show_progress_bar=False,
            quiet_logs=False,
            parameters={
                "sleeptime": "60M",
                "symbol": "AMZN"
            }
        )

        tracker = strategy.tracker
        assert tracker["iteration_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert tracker["submitted_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert tracker["filled_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert tracker['last_price'] == 218.06  # Open price of '2025-01-13T09:30:00-05:00'
        assert tracker['avg_fill_price'] == 218.06   # Open price of '2025-01-13T09:30:00-05:00'

        # i think it should be:
        # assert tracker['last_price'] == 217.92  # Close price of '2025-01-13T09:30:00-05:00'
        # assert tracker['avg_fill_price'] == 218.0  # Open price of '2025-01-13T09:31:00-05:00'



