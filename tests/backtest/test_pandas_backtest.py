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
    pandas_data_fixture_amzn_minute
)

logger = logging.getLogger(__name__)


class BuyOneShareTestStrategy(Strategy):

    # Set the initial values for the strategy
    # noinspection PyAttributeOutsideInit
    def initialize(self, parameters: Any = None) -> None:
        self.set_market(self.parameters.get("market", "NYSE"))
        self.sleeptime = self.parameters.get("sleeptime", "1D")
        self.symbol = self.parameters.get("symbol", "AMZN")
        self.prices = {}
        self.market_opens = []
        self.market_closes = []
        self.trading_iterations = []

    def before_market_opens(self):
        self.log_message(f"Before market opens called at {self.get_datetime().isoformat()}")
        self.market_opens.append(self.get_datetime())

    def after_market_closes(self):
        self.log_message(f"After market closes called at {self.get_datetime().isoformat()}")
        self.market_closes.append(self.get_datetime())
        orders = self.get_orders()
        self.log_message(f"AlpacaBacktestTestStrategy: {len(orders)} orders executed today")

    def on_filled_order(self, position, order, price, quantity, multiplier):
        self.log_message(f"AlpacaBacktestTestStrategy: Filled Order: {order}")
        self.trading_iterations[0]["filled_at"] = self.get_datetime()
        self.trading_iterations[0]["avg_fill_price"] = order.avg_fill_price

    def on_new_order(self, order):
        self.log_message(f"AlpacaBacktestTestStrategy: New Order: {order}")
        self.trading_iterations[0]["submitted_at"] = self.get_datetime()

    def on_canceled_order(self, order):
        self.log_message(f"AlpacaBacktestTestStrategy: Canceled Order: {order}")

    # Trading Strategy: Backtest will only buy traded assets on first iteration
    def on_trading_iteration(self):
        if self.first_iteration:
            now = self.get_datetime()
            asset = Asset(self.parameters["symbol"])
            current_asset_price = self.get_last_price(asset)

            # Buy 1 shares of the asset for the test
            qty = 1
            self.log_message(f"Buying {qty} shares of {asset} at {current_asset_price} @ {now}")
            order = self.create_order(asset, quantity=qty, side="buy")
            submitted_order = self.submit_order(order)
            iteration_obj = {
                "symbol": self.symbol,
                "iteration_at": self.get_datetime(),
                "last_price": current_asset_price,
                "order_id": submitted_order.identifier,
            }
            self.trading_iterations.append(iteration_obj)

        # Not the 1st iteration, cancel orders.
        else:
            self.cancel_open_orders()


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

        result, strat_obj = strategy_class.run_backtest(
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
        iteration_obj = strat_obj.trading_iterations[0]
        assert iteration_obj["iteration_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert iteration_obj["submitted_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert iteration_obj["filled_at"].isoformat() ==    '2025-01-13T09:30:00-05:00'

        assert iteration_obj['last_price'] == 218.46  # Close of '2025-01-13T09:30:00-05:00'
        assert iteration_obj["avg_fill_price"] == 220.44  # Open of '2025-01-14T09:30:00-05:00'

    # @pytest.mark.skip()
    def test_minute_data(self, pandas_data_fixture_amzn_minute):
        strategy_class = BuyOneShareTestStrategy
        backtesting_start = pandas_data_fixture_amzn_minute[0].df.index[0]
        backtesting_end = pandas_data_fixture_amzn_minute[0].df.index[-1] + timedelta(minutes=1)

        result, strat_obj = strategy_class.run_backtest(
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

        iteration_obj = strat_obj.trading_iterations[0]
        assert iteration_obj["iteration_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert iteration_obj["submitted_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert iteration_obj["filled_at"].isoformat() ==    '2025-01-13T09:30:00-05:00'

        # current prices seem wrong to me
        assert iteration_obj['last_price'] == 218.06  # Open price of '2025-01-13T09:30:00-05:00'
        assert iteration_obj['avg_fill_price'] == 218.06   # Open price of '2025-01-13T09:30:00-05:00'

        # i think it should be:
        # assert iteration_obj['last_price'] == 217.92  # Close price of '2025-01-13T09:30:00-05:00'
        # assert iteration_obj['avg_fill_price'] == 218.0  # Open price of '2025-01-13T09:31:00-05:00'

    # @pytest.mark.skip()
    def test_minute_data_using_60M_sleeptime(self, pandas_data_fixture_amzn_minute):
        strategy_class = BuyOneShareTestStrategy
        backtesting_start = pandas_data_fixture_amzn_minute[0].df.index[0]
        backtesting_end = pandas_data_fixture_amzn_minute[0].df.index[-1] + timedelta(minutes=1)

        result, strat_obj = strategy_class.run_backtest(
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

        iteration_obj = strat_obj.trading_iterations[0]
        assert iteration_obj["iteration_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert iteration_obj["submitted_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert iteration_obj["filled_at"].isoformat() == '2025-01-13T09:30:00-05:00'
        assert iteration_obj['last_price'] == 218.06  # Open price of '2025-01-13T09:30:00-05:00'
        assert iteration_obj['avg_fill_price'] == 218.06   # Open price of '2025-01-13T09:30:00-05:00'

        # i think it should be:
        # assert iteration_obj['last_price'] == 217.92  # Close price of '2025-01-13T09:30:00-05:00'
        # assert iteration_obj['avg_fill_price'] == 218.0  # Open price of '2025-01-13T09:31:00-05:00'



