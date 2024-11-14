from decimal import Decimal
from typing import Any
import datetime
import pytest

import pandas as pd
import numpy as np

from lumibot.example_strategies.drift_rebalancer import DriftRebalancer
from lumibot.components import DriftCalculationLogic, LimitOrderDriftRebalancerLogic
from lumibot.backtesting import BacktestingBroker, YahooDataBacktesting, PandasDataBacktesting
from lumibot.strategies.strategy import Strategy
from tests.fixtures import pandas_data_fixture
from lumibot.tools import print_full_pandas_dataframes, set_pandas_float_precision

print_full_pandas_dataframes()
set_pandas_float_precision(precision=5)


class MockStrategy(Strategy):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.orders = []
        self.rebalancer_logic = LimitOrderDriftRebalancerLogic(strategy=self)
        self.drift_threshold = Decimal("0.05")

    def get_last_price(
            self,
            asset: Any,
            quote: Any = None,
            exchange: str = None,
            should_use_last_close: bool = True) -> float | None:
        return 100.0  # Mock price

    def update_broker_balances(self, force_update: bool = False) -> None:
        pass

    def submit_order(self, order) -> None:
        self.orders.append(order)
        return order


class TestLimitOrderDriftRebalancerLogic:

    def setup_method(self):
        date_start = datetime.datetime(2021, 7, 10)
        date_end = datetime.datetime(2021, 7, 13)
        # self.data_source = YahooDataBacktesting(date_start, date_end)
        self.data_source = PandasDataBacktesting(date_start, date_end)
        self.backtesting_broker = BacktestingBroker(self.data_source)

    def test_selling_everything(self):
        strategy = MockStrategy(broker=self.backtesting_broker)
        df = pd.DataFrame({
            "symbol": ["AAPL"],
            "is_quote_asset": False,
            "current_quantity": [Decimal("10")],
            "current_value": [Decimal("1000")],
            "current_weight": [Decimal("1.0")],
            "target_weight": Decimal("0"),
            "target_value": Decimal("0"),
            "drift": Decimal("-1")
        })

        strategy.rebalancer_logic.rebalance(drift_df=df)
        assert len(strategy.orders) == 1
        assert strategy.orders[0].side == "sell"
        assert strategy.orders[0].quantity == Decimal("10")

    def test_selling_part_of_a_holding(self):
        strategy = MockStrategy(broker=self.backtesting_broker)
        df = pd.DataFrame({
            "symbol": ["AAPL"],
            "is_quote_asset": False,
            "current_quantity": [Decimal("10")],
            "current_value": [Decimal("1000")],
            "current_weight": [Decimal("1.0")],
            "target_weight": Decimal("0.5"),
            "target_value": Decimal("500"),
            "drift": Decimal("-0.5")
        })
        strategy.rebalancer_logic.rebalance(drift_df=df)
        assert len(strategy.orders) == 1
        assert strategy.orders[0].side == "sell"
        assert strategy.orders[0].quantity == Decimal("5")

    def test_selling_short_doesnt_create_and_order_when_shorting_is_disabled(self):
        strategy = MockStrategy(broker=self.backtesting_broker)
        df = pd.DataFrame({
            "symbol": ["AAPL"],
            "is_quote_asset": False,
            "current_quantity": [Decimal("0")],
            "current_value": [Decimal("0")],
            "current_weight": [Decimal("0.0")],
            "target_weight": Decimal("-1"),
            "target_value": Decimal("-1000"),
            "drift": Decimal("-1")
        })
        strategy.rebalancer_logic.rebalance(drift_df=df)
        assert len(strategy.orders) == 0

    def test_selling_small_short_position_creates_and_order_when_shorting_is_enabled(self):
        strategy = MockStrategy(broker=self.backtesting_broker)
        df = pd.DataFrame({
            "symbol": ["AAPL"],
            "is_quote_asset": False,
            "current_quantity": [Decimal("0")],
            "current_value": [Decimal("0")],
            "current_weight": [Decimal("0.0")],
            "target_weight": Decimal("-1"),
            "target_value": Decimal("-1000"),
            "drift": Decimal("-0.25")
        })
        strategy.rebalancer_logic = LimitOrderDriftRebalancerLogic(strategy=strategy, shorting=True)
        strategy.rebalancer_logic.rebalance(drift_df=df)
        assert len(strategy.orders) == 1

    def test_selling_small_short_position_doesnt_creatne_order_when_shorting_is_disabled(self):
        strategy = MockStrategy(broker=self.backtesting_broker)
        df = pd.DataFrame({
            "symbol": ["AAPL"],
            "is_quote_asset": False,
            "current_quantity": [Decimal("0")],
            "current_value": [Decimal("0")],
            "current_weight": [Decimal("0.0")],
            "target_weight": Decimal("-1"),
            "target_value": Decimal("-1000"),
            "drift": Decimal("-0.25")
        })
        strategy.rebalancer_logic = LimitOrderDriftRebalancerLogic(strategy=strategy, shorting=False)
        strategy.rebalancer_logic.rebalance(drift_df=df)
        assert len(strategy.orders) == 0

    def test_selling_a_100_percent_short_position_creates_and_order_when_shorting_is_enabled(self):
        strategy = MockStrategy(broker=self.backtesting_broker)
        df = pd.DataFrame({
            "symbol": ["AAPL"],
            "is_quote_asset": False,
            "current_quantity": [Decimal("0")],
            "current_value": [Decimal("0")],
            "current_weight": [Decimal("0.0")],
            "target_weight": Decimal("-1"),
            "target_value": Decimal("-1000"),
            "drift": Decimal("-1")
        })
        strategy.rebalancer_logic = LimitOrderDriftRebalancerLogic(strategy=strategy, shorting=True)
        strategy.rebalancer_logic.rebalance(drift_df=df)
        assert len(strategy.orders) == 1

    def test_buying_something_when_we_have_enough_money_and_there_is_slippage(self):
        strategy = MockStrategy(broker=self.backtesting_broker)
        df = pd.DataFrame({
            "symbol": ["AAPL"],
            "is_quote_asset": False,
            "current_quantity": [Decimal("0")],
            "current_value": [Decimal("0")],
            "current_weight": [Decimal("0.0")],
            "target_weight": Decimal("1"),
            "target_value": Decimal("1000"),
            "drift": Decimal("1")
        })
        strategy.rebalancer_logic.rebalance(drift_df=df)
        assert len(strategy.orders) == 1
        assert strategy.orders[0].side == "buy"
        assert strategy.orders[0].quantity == Decimal("9")

    def test_buying_something_when_we_dont_have_enough_money_for_everything(self):
        strategy = MockStrategy(broker=self.backtesting_broker)
        strategy._set_cash_position(cash=500.0)
        df = pd.DataFrame({
            "symbol": ["AAPL"],
            "is_quote_asset": False,
            "current_quantity": [Decimal("0")],
            "current_value": [Decimal("0")],
            "current_weight": [Decimal("0.0")],
            "target_weight": Decimal("1"),
            "target_value": Decimal("1000"),
            "drift": Decimal("1")
        })
        strategy.rebalancer_logic.rebalance(drift_df=df)
        assert len(strategy.orders) == 1
        assert strategy.orders[0].side == "buy"
        assert strategy.orders[0].quantity == Decimal("4")

    def test_attempting_to_buy_when_we_dont_have_enough_money_for_even_one_share(self):
        strategy = MockStrategy(broker=self.backtesting_broker)
        strategy._set_cash_position(cash=50.0)
        df = pd.DataFrame({
            "symbol": ["AAPL"],
            "is_quote_asset": False,
            "current_quantity": [Decimal("0")],
            "current_value": [Decimal("0")],
            "current_weight": [Decimal("0.0")],
            "target_weight": Decimal("1"),
            "target_value": Decimal("1000"),
            "drift": Decimal("1")
        })
        strategy.rebalancer_logic.rebalance(drift_df=df)
        assert len(strategy.orders) == 0

    def test_attempting_to_sell_when_the_amount_we_need_to_sell_is_less_than_the_limit_price_should_not_sell(self):
        strategy = MockStrategy(broker=self.backtesting_broker)
        df = pd.DataFrame({
            "symbol": ["AAPL"],
            "is_quote_asset": False,
            "current_quantity": [Decimal("1")],
            "current_value": [Decimal("100")],
            "current_weight": [Decimal("1.0")],
            "target_weight": Decimal("0.1"),
            "target_value": Decimal("10"),
            "drift": Decimal("-0.9")
        })
        strategy.rebalancer_logic.rebalance(drift_df=df)
        assert len(strategy.orders) == 0

    def test_calculate_limit_price_when_selling(self):
        strategy = MockStrategy(broker=self.backtesting_broker)
        df = pd.DataFrame({
            "symbol": ["AAPL"],
            "is_quote_asset": False,
            "current_quantity": [Decimal("10")],
            "current_value": [Decimal("1000")],
            "current_weight": [Decimal("1.0")],
            "target_weight": Decimal("0.0"),
            "target_value": Decimal("0"),
            "drift": Decimal("-1")
        })
        strategy.rebalancer_logic = LimitOrderDriftRebalancerLogic(
            strategy=strategy,
            acceptable_slippage=Decimal("0.005")
        )
        strategy.rebalancer_logic.rebalance(drift_df=df)
        limit_price = strategy.rebalancer_logic.calculate_limit_price(last_price=Decimal("120.00"), side="sell")
        assert limit_price == Decimal("119.4")

    def test_calculate_limit_price_when_buying(self):
        strategy = MockStrategy(broker=self.backtesting_broker)
        df = pd.DataFrame({
            "symbol": ["AAPL"],
            "is_quote_asset": False,
            "current_quantity": [Decimal("0")],
            "current_value": [Decimal("0")],
            "current_weight": [Decimal("0.0")],
            "target_weight": Decimal("1.0"),
            "target_value": Decimal("1000"),
            "drift": Decimal("1")
        })
        strategy.rebalancer_logic = LimitOrderDriftRebalancerLogic(
            strategy=strategy,
            acceptable_slippage=Decimal("0.005")
        )
        strategy.rebalancer_logic.rebalance(drift_df=df)
        limit_price = strategy.rebalancer_logic.calculate_limit_price(last_price=Decimal("120.00"), side="buy")
        assert limit_price == Decimal("120.6")

