from collections import defaultdict
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Any
import logging
from zoneinfo import ZoneInfo

import pytest

from lumibot.backtesting import PandasDataBacktesting
from lumibot.example_strategies.lifecycle_logger import LifecycleLogger
from lumibot.strategies import Strategy
from lumibot.entities import Asset

from tests.fixtures import (
    pandas_data_fixture,
    pandas_data_fixture_amzn_day,
    pandas_data_fixture_amzn_hour,
    pandas_data_fixture_amzn_minute,
    pandas_data_fixture_btc_day,
    pandas_data_fixture_btc_hour,
    pandas_data_fixture_btc_minute,
    BacktestingTestStrategy
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

        result, strategy = strategy_class.run_backtest(
            datasource_class=PandasDataBacktesting,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            pandas_data=pandas_data_fixture,
            risk_free_rate=0,
            benchmark_asset=None,
            analyze_backtest=False,
            show_progress_bar=False,
            parameters={
                "sleeptime": "1D",
            }
        )

    # @pytest.mark.skip()
    def test_pandas_datasource_with_amzn_day(self, pandas_data_fixture_amzn_day):
        strategy_class = BacktestingTestStrategy
        backtesting_start = pandas_data_fixture_amzn_day[0].df.index[5]
        backtesting_end = backtesting_start + timedelta(days=5)

        result, strategy = strategy_class.run_backtest(
            datasource_class=PandasDataBacktesting,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            pandas_data=pandas_data_fixture_amzn_day,
            benchmark_asset=None,
            analyze_backtest=False,
            show_progress_bar=False,
            parameters={
            },
        )
        # check when trading iterations happened
        last_prices = strategy.last_prices
        last_price_keys = list(last_prices.keys())
        assert len(last_prices) == 5 # number of trading iterations
        assert last_price_keys[0] == '2021-01-11T09:30:00-05:00'
        assert last_price_keys[-1] == '2021-01-15T09:30:00-05:00'
        assert last_prices['2021-01-11T09:30:00-05:00'] == 155.71  # close of '2025-01-11T09:30:00-05:00'

        order_tracker = strategy.order_tracker
        assert order_tracker["iteration_at"].isoformat() == '2021-01-11T09:30:00-05:00'
        assert order_tracker["submitted_at"].isoformat() == '2021-01-11T09:30:00-05:00'
        assert order_tracker["filled_at"].isoformat() == '2021-01-11T09:30:00-05:00'
        assert order_tracker["avg_fill_price"] == 156.0  # open of '2025-01-12T09:30:00-05:00'

    # @pytest.mark.skip()
    def test_pandas_datasource_with_amzn_minute(self, pandas_data_fixture_amzn_minute):
        strategy_class = BuyOnceTestStrategy
        backtesting_start = pandas_data_fixture_amzn_minute[0].df.index[0]
        backtesting_end = backtesting_start + timedelta(days=5)

        result, strategy = strategy_class.run_backtest(
            datasource_class=PandasDataBacktesting,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            pandas_data=pandas_data_fixture_amzn_minute,
            benchmark_asset=None,
            analyze_backtest=False,
            show_progress_bar=False,
            parameters={
                "sleeptime": "1M",
                "symbol": "AMZN"
            }
        )

        tracker = strategy.tracker
        assert tracker["iteration_at"].isoformat() == '2021-01-04T09:30:00-05:00'
        assert tracker["submitted_at"].isoformat() == '2021-01-04T09:30:00-05:00'
        assert tracker["filled_at"].isoformat() ==    '2021-01-04T09:30:00-05:00'

        # minute data uses the open price of the current bar as the last price and the fill price
        assert tracker['last_price'] == 163.45   # Open price of '2021-01-04T14:30:00-00:00'
        assert tracker['avg_fill_price'] == 163.45   # Open price of '2021-01-04T14:30:00-00:00'

    # @pytest.mark.skip()
    def test_pandas_datasource_with_amzn_hour(self, pandas_data_fixture_amzn_hour):
        strategy_class = BuyOnceTestStrategy
        backtesting_start = pandas_data_fixture_amzn_hour[0].df.index[0]
        backtesting_end = backtesting_start + timedelta(days=5)

        result, strategy = strategy_class.run_backtest(
            datasource_class=PandasDataBacktesting,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            pandas_data=pandas_data_fixture_amzn_hour,
            benchmark_asset=None,
            analyze_backtest=False,
            show_progress_bar=False,
            parameters={
                "sleeptime": "60M",
                "symbol": "AMZN"
            }
        )

        tracker = strategy.tracker
        assert tracker["iteration_at"].isoformat() == '2021-01-04T09:30:00-05:00'
        assert tracker["submitted_at"].isoformat() == '2021-01-04T09:30:00-05:00'
        assert tracker["filled_at"].isoformat() ==    '2021-01-04T09:30:00-05:00'

        # minute data uses the open price of the current bar as the last price and the fill price
        assert tracker['last_price'] == 163.9   # Open price of '2021-01-04T14:00:00+00:00'
        assert tracker['avg_fill_price'] == 163.9   # Open price of '2021-01-04T14:00:00+00:00'

    # @pytest.mark.skip()
    def test_pandas_datasource_with_amzn_minute_60M_sleeptime(self, pandas_data_fixture_amzn_minute):
        strategy_class = BuyOnceTestStrategy
        backtesting_start = pandas_data_fixture_amzn_minute[0].df.index[0]
        backtesting_end = backtesting_start + timedelta(days=5)

        result, strategy = strategy_class.run_backtest(
            datasource_class=PandasDataBacktesting,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            pandas_data=pandas_data_fixture_amzn_minute,
            benchmark_asset=None,
            analyze_backtest=False,
            show_progress_bar=False,
            parameters={
                "sleeptime": "60M",
                "symbol": "AMZN"
            }
        )

        tracker = strategy.tracker
        assert tracker["iteration_at"].isoformat() == '2021-01-04T09:30:00-05:00'
        assert tracker["submitted_at"].isoformat() == '2021-01-04T09:30:00-05:00'
        assert tracker["filled_at"].isoformat() ==    '2021-01-04T09:30:00-05:00'

        # minute data uses the open price of the current bar as the last price and the fill price
        assert tracker['last_price'] == 163.45   # Open price of '2021-01-04 14:30:00+00:00'
        assert tracker['avg_fill_price'] == 163.45   # Open price of '2021-01-04 14:30:00+00:00'

    def test_pandas_datasource_with_btc_day(self, pandas_data_fixture_btc_day):
        strategy_class = BuyOnceTestStrategy
        backtesting_start = pandas_data_fixture_btc_day[0].df.index[0]
        backtesting_end = backtesting_start + timedelta(days=5)

        result, strategy = strategy_class.run_backtest(
            datasource_class=PandasDataBacktesting,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            pandas_data=pandas_data_fixture_btc_day,
            benchmark_asset=None,
            analyze_backtest=False,
            show_progress_bar=False,
            parameters={
                "sleeptime": "1D",
                "asset": Asset(symbol="BTC", asset_type="crypto")
            }
        )

        tracker = strategy.tracker
        assert tracker["iteration_at"].isoformat() == '2021-01-04T08:30:00-06:00'
        assert tracker["submitted_at"].isoformat() == '2021-01-04T08:30:00-06:00'
        assert tracker["filled_at"].isoformat() ==    '2021-01-04T08:30:00-06:00'

        # daily data uses the close price of the current bar as the last price
        # but unlike minute data, it uses the open price of the next bar as the fill price
        assert tracker['last_price'] == 30441.57  # Close of '2021-01-04 06:00:00+00:00'
        assert tracker["avg_fill_price"] == 30461.84  # Open of '2021-01-05 06:00:00+00:00'

    def test_pandas_datasource_with_btc_hour(self, pandas_data_fixture_btc_hour):
        strategy_class = BuyOnceTestStrategy
        backtesting_start = pandas_data_fixture_btc_hour[0].df.index[0]
        backtesting_end = backtesting_start + timedelta(days=5)

        result, strategy = strategy_class.run_backtest(
            datasource_class=PandasDataBacktesting,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            pandas_data=pandas_data_fixture_btc_hour,
            benchmark_asset=None,
            analyze_backtest=False,
            show_progress_bar=False,
            parameters={
                "sleeptime": "60M",
                "asset": Asset(symbol="BTC", asset_type="crypto")
            }
        )

        tracker = strategy.tracker
        assert tracker["iteration_at"].isoformat() == '2021-01-04T08:30:00-06:00'
        assert tracker["submitted_at"].isoformat() == '2021-01-04T08:30:00-06:00'
        assert tracker["filled_at"].isoformat() == '2021-01-04T08:30:00-06:00'

        # minute data uses the open price of the current bar as the last price and the fill price
        assert tracker['last_price'] == 31418.36   # Open price of '2021-01-04 14:00:00+00:00'
        assert tracker['avg_fill_price'] == 31418.36   # Open price of '2021-01-04 14:00:00+00:00'

    def test_pandas_datasource_with_btc_minute(self, pandas_data_fixture_btc_minute):
        strategy_class = BuyOnceTestStrategy
        backtesting_start = pandas_data_fixture_btc_minute[0].df.index[0]
        backtesting_end = backtesting_start + timedelta(days=5)

        result, strategy = strategy_class.run_backtest(
            datasource_class=PandasDataBacktesting,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            pandas_data=pandas_data_fixture_btc_minute,
            benchmark_asset=None,
            analyze_backtest=False,
            show_progress_bar=False,
            parameters={
                "sleeptime": "1M",
                "asset": Asset(symbol="BTC", asset_type="crypto")
            }
        )

        tracker = strategy.tracker
        assert tracker["iteration_at"].isoformat() == '2021-01-04T08:30:00-06:00'
        assert tracker["submitted_at"].isoformat() == '2021-01-04T08:30:00-06:00'
        assert tracker["filled_at"].isoformat() == '2021-01-04T08:30:00-06:00'

        # minute data uses the open price of the current bar as the last price and the fill price
        assert tracker['last_price'] == 31762.75   # Open price of '2021-01-04 14:30:00+00:00'
        assert tracker['avg_fill_price'] == 31762.75   # Open price of '2021-01-04 14:30:00+00:00'


    def test_pandas_datasource_with_btc_minute_60M_sleeptime(self, pandas_data_fixture_btc_minute):
        strategy_class = BuyOnceTestStrategy
        backtesting_start = pandas_data_fixture_btc_minute[0].df.index[0]
        backtesting_end = backtesting_start + timedelta(days=5)

        result, strategy = strategy_class.run_backtest(
            datasource_class=PandasDataBacktesting,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            pandas_data=pandas_data_fixture_btc_minute,
            benchmark_asset=None,
            analyze_backtest=False,
            show_progress_bar=False,
            parameters={
                "sleeptime": "60M",
                "asset": Asset(symbol="BTC", asset_type="crypto")
            }
        )

        tracker = strategy.tracker
        assert tracker["iteration_at"].isoformat() == '2021-01-04T08:30:00-06:00'
        assert tracker["submitted_at"].isoformat() == '2021-01-04T08:30:00-06:00'
        assert tracker["filled_at"].isoformat() == '2021-01-04T08:30:00-06:00'

        # minute data uses the open price of the current bar as the last price and the fill price
        assert tracker['last_price'] == 31762.75   # Open price of '2021-01-04 14:30:00+00:00'
        assert tracker['avg_fill_price'] == 31762.75   # Open price of '2021-01-04 14:30:00+00:00'
