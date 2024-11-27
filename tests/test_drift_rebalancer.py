from decimal import Decimal
from typing import Any
import datetime
from decimal import Decimal
import pytest

import pandas as pd
import numpy as np

from lumibot.example_strategies.drift_rebalancer import DriftRebalancer
from lumibot.components.drift_rebalancer_logic import DriftRebalancerLogic, DriftType, DriftOrderLogic
from lumibot.components.drift_rebalancer_logic import DriftCalculationLogic  #, LimitOrderDriftRebalancerLogic
from lumibot.backtesting import BacktestingBroker, YahooDataBacktesting, PandasDataBacktesting
from lumibot.strategies.strategy import Strategy
from tests.fixtures import pandas_data_fixture
from lumibot.tools import print_full_pandas_dataframes, set_pandas_float_display_precision
from lumibot.entities import Order

print_full_pandas_dataframes()
set_pandas_float_display_precision(precision=5)


class MockStrategyWithDriftCalculationLogic(Strategy):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.orders = []
        self.target_weights = {}
        self.drift_rebalancer_logic = DriftRebalancerLogic(
            strategy=self,
            drift_threshold=kwargs.get("drift_threshold", Decimal("0.05")),
            fill_sleeptime=kwargs.get("fill_sleeptime", 15),
            acceptable_slippage=kwargs.get("acceptable_slippage", Decimal("0.005")),
            shorting=kwargs.get("shorting", False),
            drift_type=kwargs.get("drift_type", DriftType.ABSOLUTE),
            order_type=kwargs.get("order_type", Order.OrderType.LIMIT)
        )

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


class TestDriftCalculationLogic:

    def setup_method(self):
        date_start = datetime.datetime(2021, 7, 10)
        date_end = datetime.datetime(2021, 7, 13)
        self.data_source = PandasDataBacktesting(date_start, date_end)
        self.backtesting_broker = BacktestingBroker(self.data_source)

    def test_add_position(self, mocker):
        strategy = MockStrategyWithDriftCalculationLogic(broker=self.backtesting_broker)
        target_weights = {
            "AAPL": Decimal("0.5"),
            "GOOGL": Decimal("0.3"),
            "MSFT": Decimal("0.2")
        }

        def mock_add_positions(self):
            self._add_position(
                symbol="AAPL",
                is_quote_asset=False,
                current_quantity=Decimal("10"),
                current_value=Decimal("1500")
            )
            self._add_position(
                symbol="GOOGL",
                is_quote_asset=False,
                current_quantity=Decimal("5"),
                current_value=Decimal("1000")
            )
            self._add_position(
                symbol="MSFT",
                is_quote_asset=False,
                current_quantity=Decimal("8"),
                current_value=Decimal("800")
            )

        mocker.patch.object(DriftCalculationLogic, "_add_positions", mock_add_positions)
        df = strategy.drift_rebalancer_logic.calculate(target_weights=target_weights)
        assert df["symbol"].tolist() == ["AAPL", "GOOGL", "MSFT"]
        assert df["current_quantity"].tolist() == [Decimal("10"), Decimal("5"), Decimal("8")]
        assert df["current_value"].tolist() == [Decimal("1500"), Decimal("1000"), Decimal("800")]

    def test_calculate_absolute_drift(self, mocker):
        strategy = MockStrategyWithDriftCalculationLogic(
            broker= self.backtesting_broker,
            drift_threshold=Decimal("0.05"),
            drift_type=DriftType.ABSOLUTE
        )
        target_weights = {
            "AAPL": Decimal("0.5"),
            "GOOGL": Decimal("0.3"),
            "MSFT": Decimal("0.2")
        }

        def mock_add_positions(self):
            self._add_position(
                symbol="AAPL",
                is_quote_asset=False,
                current_quantity=Decimal("10"),
                current_value=Decimal("1500")
            )
            self._add_position(
                symbol="GOOGL",
                is_quote_asset=False,
                current_quantity=Decimal("5"),
                current_value=Decimal("1000")
            )
            self._add_position(
                symbol="MSFT",
                is_quote_asset=False,
                current_quantity=Decimal("8"),
                current_value=Decimal("800")
            )

        mocker.patch.object(DriftCalculationLogic, "_add_positions", mock_add_positions)
        df = strategy.drift_rebalancer_logic.calculate(target_weights=target_weights)

        pd.testing.assert_series_equal(
            df["current_weight"],
            pd.Series([
                Decimal('0.4545454545454545454545454545'),
                Decimal('0.3030303030303030303030303030'),
                Decimal('0.2424242424242424242424242424')
            ]),
            check_names=False
        )

        assert df["target_value"].tolist() == [Decimal('1650.0'), Decimal('990.0'), Decimal('660.0')]

        pd.testing.assert_series_equal(
            df["drift"],
            pd.Series([
                Decimal('0.0454545454545454545454545455'),
                Decimal('-0.0030303030303030303030303030'),
                Decimal('-0.0424242424242424242424242424')
            ]),
            check_names=False
        )

    def test_calculate_relative_drift(self, mocker):
        strategy = MockStrategyWithDriftCalculationLogic(
            broker=self.backtesting_broker,
            drift_threshold=Decimal("0.20"),
            drift_type=DriftType.RELATIVE
        )

        target_weights = {
            "AAPL": Decimal("0.60"),
            "GOOGL": Decimal("0.30"),
            "MSFT": Decimal("0.10")
        }

        def mock_add_positions(self):
            self._add_position(
                symbol="AAPL",
                is_quote_asset=False,
                current_quantity=Decimal("4"),
                current_value=Decimal("400")
            )
            self._add_position(
                symbol="GOOGL",
                is_quote_asset=False,
                current_quantity=Decimal("4"),
                current_value=Decimal("400")
            )
            self._add_position(
                symbol="MSFT",
                is_quote_asset=False,
                current_quantity=Decimal("2"),
                current_value=Decimal("200")
            )

        mocker.patch.object(DriftCalculationLogic, "_add_positions", mock_add_positions)
        df = strategy.drift_rebalancer_logic.calculate(target_weights=target_weights)
        # print(f"/n{df[['symbol', 'current_weight', 'target_weight', 'drift']]}")

        pd.testing.assert_series_equal(
            df["current_weight"],
            pd.Series([
                Decimal('0.4'),
                Decimal('0.4'),
                Decimal('0.2')
            ]),
            check_names=False
        )

        assert df["target_value"].tolist() == [Decimal('600.0'), Decimal('300.0'), Decimal('100.0')]

        pd.testing.assert_series_equal(
            df["drift"],
            pd.Series([
                Decimal('0.3333333333333333333333333333'),
                Decimal('-0.3333333333333333333333333333'),
                Decimal('-1.0')
            ]),
            check_names=False
        )

    def test_drift_is_negative_one_when_we_have_a_position_and_the_target_weights_says_to_not_have_it(self, mocker):
        strategy = MockStrategyWithDriftCalculationLogic(
            broker=self.backtesting_broker,
            drift_threshold=Decimal("0.05"),
            drift_type=DriftType.ABSOLUTE
        )
        target_weights = {
            "AAPL": Decimal("0.5"),
            "GOOGL": Decimal("0.3"),
            "MSFT": Decimal("0.0")
        }

        def mock_add_positions(self):
            self._add_position(
                symbol="AAPL",
                is_quote_asset=False,
                current_quantity=Decimal("10"),
                current_value=Decimal("1500")
            )
            self._add_position(
                symbol="GOOGL",
                is_quote_asset=False,
                current_quantity=Decimal("5"),
                current_value=Decimal("1000")
            )
            self._add_position(
                symbol="MSFT",
                is_quote_asset=False,
                current_quantity=Decimal("8"),
                current_value=Decimal("800")
            )

        mocker.patch.object(DriftCalculationLogic, "_add_positions", mock_add_positions)
        df = strategy.drift_rebalancer_logic.calculate(target_weights=target_weights)

        pd.testing.assert_series_equal(
            df["current_weight"],
            pd.Series([
                Decimal('0.4545454545454545454545454545'),
                Decimal('0.3030303030303030303030303030'),
                Decimal('0.2424242424242424242424242424')
            ]),
            check_names=False
        )

        assert df["target_value"].tolist() == [Decimal("1650"), Decimal("990"), Decimal("0")]

        pd.testing.assert_series_equal(
            df["drift"],
            pd.Series([
                Decimal('0.0454545454545454545454545455'),
                Decimal('-0.0030303030303030303030303030'),
                Decimal('-1')
            ]),
            check_names=False
        )

    def test_drift_is_one_when_we_have_none_of_an_asset_and_target_weights_says_we_should_have_some(self, mocker):
        strategy = MockStrategyWithDriftCalculationLogic(
            broker=self.backtesting_broker,
            drift_threshold=Decimal("0.05"),
            drift_type=DriftType.ABSOLUTE
        )
        target_weights = {
            "AAPL": Decimal("0.25"),
            "GOOGL": Decimal("0.25"),
            "MSFT": Decimal("0.25"),
            "AMZN": Decimal("0.25")
        }

        def mock_add_positions(self):
            self._add_position(
                symbol="AAPL",
                is_quote_asset=False,
                current_quantity=Decimal("10"),
                current_value=Decimal("1500")
            )
            self._add_position(
                symbol="GOOGL",
                is_quote_asset=False,
                current_quantity=Decimal("5"),
                current_value=Decimal("1000")
            )
            self._add_position(
                symbol="MSFT",
                is_quote_asset=False,
                current_quantity=Decimal("8"),
                current_value=Decimal("800")
            )

        mocker.patch.object(DriftCalculationLogic, "_add_positions", mock_add_positions)
        df = strategy.drift_rebalancer_logic.calculate(target_weights=target_weights)

        pd.testing.assert_series_equal(
            df["current_weight"],
            pd.Series([
                Decimal('0.4545454545454545454545454545'),
                Decimal('0.3030303030303030303030303030'),
                Decimal('0.2424242424242424242424242424'),
                Decimal('0')
            ]),
            check_names=False
        )

        assert df["target_value"].tolist() == [Decimal("825"), Decimal("825"), Decimal("825"), Decimal("825")]

        pd.testing.assert_series_equal(
            df["drift"],
            pd.Series([
                Decimal('-0.2045454545454545454545454545'),
                Decimal('-0.0530303030303030303030303030'),
                Decimal('0.0075757575757575757575757576'),
                Decimal('1')
            ]),
            check_names=False
        )

    def test_drift_is_negative_one_when_we_have_none_of_an_asset_and_target_weights_says_we_should_short_some(self, mocker):
        strategy = MockStrategyWithDriftCalculationLogic(
            broker=self.backtesting_broker,
            drift_threshold=Decimal("0.05"),
            drift_type=DriftType.ABSOLUTE,
            shorting=True
        )
        target_weights = {
            "AAPL": Decimal("0.25"),
            "GOOGL": Decimal("0.25"),
            "MSFT": Decimal("0.25"),
            "AMZN": Decimal("-0.25")
        }

        def mock_add_positions(self):
            self._add_position(
                symbol="AAPL",
                is_quote_asset=False,
                current_quantity=Decimal("10"),
                current_value=Decimal("1500")
            )
            self._add_position(
                symbol="GOOGL",
                is_quote_asset=False,
                current_quantity=Decimal("5"),
                current_value=Decimal("1000")
            )
            self._add_position(
                symbol="MSFT",
                is_quote_asset=False,
                current_quantity=Decimal("8"),
                current_value=Decimal("800")
            )

        mocker.patch.object(DriftCalculationLogic, "_add_positions", mock_add_positions)
        df = strategy.drift_rebalancer_logic.calculate(target_weights=target_weights)

        pd.testing.assert_series_equal(
            df["current_weight"],
            pd.Series([
                Decimal('0.4545454545454545454545454545'),
                Decimal('0.3030303030303030303030303030'),
                Decimal('0.2424242424242424242424242424'),
                Decimal('0')
            ]),
            check_names=False
        )

        assert df["target_value"].tolist() == [Decimal("825"), Decimal("825"), Decimal("825"), Decimal("-825")]

        pd.testing.assert_series_equal(
            df["drift"],
            pd.Series([
                Decimal('-0.2045454545454545454545454545'),
                Decimal('-0.0530303030303030303030303030'),
                Decimal('0.0075757575757575757575757576'),
                Decimal('-1')
            ]),
            check_names=False
        )


    def test_drift_is_zero_when_current_weight_and_target_weight_are_zero(self, mocker):
        strategy = MockStrategyWithDriftCalculationLogic(
            broker=self.backtesting_broker,
            drift_threshold=Decimal("0.05"),
            drift_type=DriftType.ABSOLUTE
        )
        target_weights = {
            "AAPL": Decimal("0.25"),
            "GOOGL": Decimal("0.25"),
            "MSFT": Decimal("0.25"),
            "AMZN": Decimal("0.0")
        }

        def mock_add_positions(self):
            self._add_position(
                symbol="AAPL",
                is_quote_asset=False,
                current_quantity=Decimal("10"),
                current_value=Decimal("1500")
            )
            self._add_position(
                symbol="GOOGL",
                is_quote_asset=False,
                current_quantity=Decimal("5"),
                current_value=Decimal("1000")
            )
            self._add_position(
                symbol="MSFT",
                is_quote_asset=False,
                current_quantity=Decimal("8"),
                current_value=Decimal("800")
            )

        mocker.patch.object(DriftCalculationLogic, "_add_positions", mock_add_positions)
        df = strategy.drift_rebalancer_logic.calculate(target_weights=target_weights)

        pd.testing.assert_series_equal(
            df["current_weight"],
            pd.Series([
                Decimal('0.4545454545454545454545454545'),
                Decimal('0.3030303030303030303030303030'),
                Decimal('0.2424242424242424242424242424'),
                Decimal('0')
            ]),
            check_names=False
        )

        assert df["target_value"].tolist() == [Decimal("825"), Decimal("825"), Decimal("825"), Decimal("0")]

        pd.testing.assert_series_equal(
            df["drift"],
            pd.Series([
                Decimal('-0.2045454545454545454545454545'),
                Decimal('-0.0530303030303030303030303030'),
                Decimal('0.0075757575757575757575757576'),
                Decimal('0')
            ]),
            check_names=False
        )

    def test_calculate_absolute_drift_when_quote_asset_position_exists(self, mocker):
        strategy = MockStrategyWithDriftCalculationLogic(
            broker=self.backtesting_broker,
            drift_threshold=Decimal("0.05"),
            drift_type=DriftType.ABSOLUTE
        )
        target_weights = {
            "AAPL": Decimal("0.5"),
            "GOOGL": Decimal("0.3"),
            "MSFT": Decimal("0.2")
        }

        def mock_add_positions(self):
            self._add_position(
                symbol="USD",
                is_quote_asset=True,
                current_quantity=Decimal("1000"),
                current_value=Decimal("1000")
            )
            self._add_position(
                symbol="AAPL",
                is_quote_asset=False,
                current_quantity=Decimal("10"),
                current_value=Decimal("1500")
            )
            self._add_position(
                symbol="GOOGL",
                is_quote_asset=False,
                current_quantity=Decimal("5"),
                current_value=Decimal("1000")
            )
            self._add_position(
                symbol="MSFT",
                is_quote_asset=False,
                current_quantity=Decimal("8"),
                current_value=Decimal("800")
            )

        mocker.patch.object(DriftCalculationLogic, "_add_positions", mock_add_positions)
        df = strategy.drift_rebalancer_logic.calculate(target_weights=target_weights)

        pd.testing.assert_series_equal(
            df["current_weight"],
            pd.Series([
                Decimal('0.3488372093023255813953488372'),
                Decimal('0.2325581395348837209302325581'),
                Decimal('0.1860465116279069767441860465'),
                Decimal('0.2325581395348837209302325581')
            ]),
            check_names=False
        )

        assert df["target_value"].tolist() == [Decimal("2150"), Decimal("1290"), Decimal("860"), Decimal("0")]

        pd.testing.assert_series_equal(
            df["drift"],
            pd.Series([
                Decimal('0.1511627906976744186046511628'),
                Decimal('0.0674418604651162790697674419'),
                Decimal('0.0139534883720930232558139535'),
                Decimal('0')
            ]),
            check_names=False
        )

    def test_calculate_relative_drift_when_quote_asset_position_exists(self, mocker):
        strategy = MockStrategyWithDriftCalculationLogic(
            broker=self.backtesting_broker,
            drift_threshold=Decimal("0.20"),
            drift_type=DriftType.RELATIVE
        )
        target_weights = {
            "AAPL": Decimal("0.5"),
            "GOOGL": Decimal("0.3"),
            "MSFT": Decimal("0.2")
        }

        def mock_add_positions(self):
            self._add_position(
                symbol="USD",
                is_quote_asset=True,
                current_quantity=Decimal("1000"),
                current_value=Decimal("1000")
            )
            self._add_position(
                symbol="AAPL",
                is_quote_asset=False,
                current_quantity=Decimal("10"),
                current_value=Decimal("1500")
            )
            self._add_position(
                symbol="GOOGL",
                is_quote_asset=False,
                current_quantity=Decimal("5"),
                current_value=Decimal("1000")
            )
            self._add_position(
                symbol="MSFT",
                is_quote_asset=False,
                current_quantity=Decimal("8"),
                current_value=Decimal("800")
            )

        mocker.patch.object(DriftCalculationLogic, "_add_positions", mock_add_positions)
        df = strategy.drift_rebalancer_logic.calculate(target_weights=target_weights)

        pd.testing.assert_series_equal(
            df["current_weight"],
            pd.Series([
                Decimal('0.3488372093023255813953488372'),
                Decimal('0.2325581395348837209302325581'),
                Decimal('0.1860465116279069767441860465'),
                Decimal('0.2325581395348837209302325581')
            ]),
            check_names=False
        )

        assert df["target_value"].tolist() == [Decimal("2150"), Decimal("1290"), Decimal("860"), Decimal("0")]

        pd.testing.assert_series_equal(
            df["drift"],
            pd.Series([
                Decimal('0.3023255813953488372093023256'),
                Decimal('0.2248062015503875968992248063'),
                Decimal('0.0697674418604651162790697675'),
                Decimal('0')
            ]),
            check_names=False
        )

    def test_calculate_absolute_drift_when_quote_asset_in_target_weights(self, mocker):
        strategy = MockStrategyWithDriftCalculationLogic(
            broker=self.backtesting_broker,
            drift_threshold=Decimal("0.05"),
            drift_type=DriftType.ABSOLUTE
        )
        target_weights = {
            "AAPL": Decimal("0.25"),
            "GOOGL": Decimal("0.25"),
            "USD": Decimal("0.50")
        }

        def mock_add_positions(self):
            self._add_position(
                symbol="USD",
                is_quote_asset=True,
                current_quantity=Decimal("0"),
                current_value=Decimal("0")
            )
            self._add_position(
                symbol="AAPL",
                is_quote_asset=False,
                current_quantity=Decimal("5"),
                current_value=Decimal("500")
            )
            self._add_position(
                symbol="GOOGL",
                is_quote_asset=False,
                current_quantity=Decimal("10"),
                current_value=Decimal("500")
            )

        mocker.patch.object(DriftCalculationLogic, "_add_positions", mock_add_positions)
        df = strategy.drift_rebalancer_logic.calculate(target_weights=target_weights)

        assert df["current_weight"].tolist() == [Decimal("0.5"), Decimal("0.5"), Decimal("0.0")]
        assert df["target_value"].tolist() == [Decimal("250"), Decimal("250"), Decimal("500")]
        assert df["drift"].tolist() == [Decimal("-0.25"), Decimal("-0.25"), Decimal("0")]

    def test_calculate_drift_when_we_want_short_something(self, mocker):
        strategy = MockStrategyWithDriftCalculationLogic(
            broker=self.backtesting_broker,
            drift_threshold=Decimal("0.05"),
            drift_type=DriftType.ABSOLUTE
        )
        target_weights = {
            "AAPL": Decimal("-0.50"),
            "USD": Decimal("0.50")
        }

        def mock_add_positions(self):
            self._add_position(
                symbol="USD",
                is_quote_asset=True,
                current_quantity=Decimal("1000"),
                current_value=Decimal("1000")
            )
            self._add_position(
                symbol="AAPL",
                is_quote_asset=False,
                current_quantity=Decimal("0"),
                current_value=Decimal("0")
            )

        mocker.patch.object(DriftCalculationLogic, "_add_positions", mock_add_positions)
        df = strategy.drift_rebalancer_logic.calculate(target_weights=target_weights)

        assert df["current_weight"].tolist() == [Decimal("0.0"), Decimal("1.0")]
        assert df["target_value"].tolist() == [Decimal("-500"), Decimal("500")]
        assert df["drift"].tolist() == [Decimal("-1.0"), Decimal("0")]

    def test_calculate_absolute_drift_when_we_want_a_100_percent_short_position(self, mocker):
        strategy = MockStrategyWithDriftCalculationLogic(
            broker=self.backtesting_broker,
            drift_threshold=Decimal("0.05"),
            drift_type=DriftType.ABSOLUTE
        )
        target_weights = {
            "AAPL": Decimal("0.25"),
            "GOOGL": Decimal("0.25"),
            "USD": Decimal("0.50")
        }

        def mock_add_positions(self):
            self._add_position(
                symbol="USD",
                is_quote_asset=True,
                current_quantity=Decimal("0"),
                current_value=Decimal("0")
            )
            self._add_position(
                symbol="AAPL",
                is_quote_asset=False,
                current_quantity=Decimal("5"),
                current_value=Decimal("500")
            )
            self._add_position(
                symbol="GOOGL",
                is_quote_asset=False,
                current_quantity=Decimal("10"),
                current_value=Decimal("500")
            )

        mocker.patch.object(DriftCalculationLogic, "_add_positions", mock_add_positions)
        df = strategy.drift_rebalancer_logic.calculate(target_weights=target_weights)

        assert df["current_weight"].tolist() == [Decimal("0.5"), Decimal("0.5"), Decimal("0.0")]
        assert df["target_value"].tolist() == [Decimal("250"), Decimal("250"), Decimal("500")]
        assert df["drift"].tolist() == [Decimal("-0.25"), Decimal("-0.25"), Decimal("0")]

    def test_calculate_absolute_drift_when_we_want_a_100_percent_short_position_and_cash_in_target_weights(self, mocker):
        strategy = MockStrategyWithDriftCalculationLogic(
            broker=self.backtesting_broker,
            drift_threshold=Decimal("0.05"),
            drift_type=DriftType.ABSOLUTE
        )
        target_weights = {
            "AAPL": Decimal("-1.0"),
            "USD": Decimal("0.0")
        }

        def mock_add_positions(self):
            self._add_position(
                symbol="USD",
                is_quote_asset=True,
                current_quantity=Decimal("1000"),
                current_value=Decimal("1000")
            )
            self._add_position(
                symbol="AAPL",
                is_quote_asset=False,
                current_quantity=Decimal("0"),
                current_value=Decimal("0")
            )

        mocker.patch.object(DriftCalculationLogic, "_add_positions", mock_add_positions)
        df = strategy.drift_rebalancer_logic.calculate(target_weights=target_weights)

        assert df["current_weight"].tolist() == [Decimal("0.0"), Decimal("1.0")]
        assert df["target_value"].tolist() == [Decimal("-1000"), Decimal("0")]
        assert df["drift"].tolist() == [Decimal("-1.0"), Decimal("0")]

    def test_calculate_relative_drift_when_we_want_a_100_percent_short_position(self, mocker):
        strategy = MockStrategyWithDriftCalculationLogic(
            broker=self.backtesting_broker,
            drift_threshold=Decimal("0.05"),
            drift_type=DriftType.RELATIVE
        )
        target_weights = {
            "AAPL": Decimal("-1.0"),
            "USD": Decimal("0.0")
        }

        def mock_add_positions(self):
            self._add_position(
                symbol="USD",
                is_quote_asset=True,
                current_quantity=Decimal("1000"),
                current_value=Decimal("1000")
            )
            self._add_position(
                symbol="AAPL",
                is_quote_asset=False,
                current_quantity=Decimal("0"),
                current_value=Decimal("0")
            )

        mocker.patch.object(DriftCalculationLogic, "_add_positions", mock_add_positions)
        df = strategy.drift_rebalancer_logic.calculate(target_weights=target_weights)

        assert df["current_weight"].tolist() == [Decimal("0.0"), Decimal("1.0")]
        assert df["target_value"].tolist() == [Decimal("-1000"), Decimal("0")]
        assert df["drift"].tolist() == [Decimal("-1.0"), Decimal("0")]


class MockStrategyWithOrderLogic(Strategy):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.orders = []
        self.target_weights = {}
        self.order_logic = DriftOrderLogic(
            strategy=self,
            drift_threshold=kwargs.get("drift_threshold", Decimal("0.05")),
            fill_sleeptime=kwargs.get("fill_sleeptime", 15),
            acceptable_slippage=kwargs.get("acceptable_slippage", Decimal("0.005")),
            shorting=kwargs.get("shorting", False),
            order_type=kwargs.get("order_type", Order.OrderType.LIMIT)
        )

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


class TestDriftOrderLogic:

    def setup_method(self):
        date_start = datetime.datetime(2021, 7, 10)
        date_end = datetime.datetime(2021, 7, 13)
        # self.data_source = YahooDataBacktesting(date_start, date_end)
        self.data_source = PandasDataBacktesting(date_start, date_end)
        self.backtesting_broker = BacktestingBroker(self.data_source)

    def test_selling_everything_with_limit_orders(self):
        strategy = MockStrategyWithOrderLogic(
            broker=self.backtesting_broker,
            order_type=Order.OrderType.LIMIT
        )
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

        strategy.order_logic.rebalance(drift_df=df)
        assert len(strategy.orders) == 1
        assert strategy.orders[0].side == "sell"
        assert strategy.orders[0].quantity == Decimal("10")
        assert strategy.orders[0].type == Order.OrderType.LIMIT

    def test_selling_everything_with_market_orders(self):
        strategy = MockStrategyWithOrderLogic(
            broker=self.backtesting_broker,
            order_type=Order.OrderType.MARKET
        )
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

        strategy.order_logic.rebalance(drift_df=df)
        assert len(strategy.orders) == 1
        assert strategy.orders[0].side == "sell"
        assert strategy.orders[0].quantity == Decimal("10")
        assert strategy.orders[0].type == Order.OrderType.MARKET

    def test_selling_part_of_a_holding_with_limit_order(self):
        strategy = MockStrategyWithOrderLogic(
            broker=self.backtesting_broker,
            order_type=Order.OrderType.LIMIT
        )
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
        strategy.order_logic.rebalance(drift_df=df)
        assert len(strategy.orders) == 1
        assert strategy.orders[0].side == "sell"
        assert strategy.orders[0].quantity == Decimal("5")
        assert strategy.orders[0].type == Order.OrderType.LIMIT

    def test_selling_part_of_a_holding_with_market_order(self):
        strategy = MockStrategyWithOrderLogic(
            broker=self.backtesting_broker,
            order_type=Order.OrderType.MARKET
        )
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
        strategy.order_logic.rebalance(drift_df=df)
        assert len(strategy.orders) == 1
        assert strategy.orders[0].side == "sell"
        assert strategy.orders[0].quantity == Decimal("5")
        assert strategy.orders[0].type == Order.OrderType.MARKET

    def test_selling_short_doesnt_create_and_order_when_shorting_is_disabled(self):
        strategy = MockStrategyWithOrderLogic(
            broker=self.backtesting_broker,
            order_type=Order.OrderType.LIMIT
        )
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
        strategy.order_logic.rebalance(drift_df=df)
        assert len(strategy.orders) == 0

    def test_selling_small_short_position_creates_and_order_when_shorting_is_enabled(self):
        strategy = MockStrategyWithOrderLogic(
            broker=self.backtesting_broker,
            order_type=Order.OrderType.LIMIT,
            shorting=True
        )
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
        strategy.order_logic.rebalance(drift_df=df)
        assert len(strategy.orders) == 1

    def test_selling_small_short_position_doesnt_create_order_when_shorting_is_disabled(self):
        strategy = MockStrategyWithOrderLogic(
            broker=self.backtesting_broker,
            order_type=Order.OrderType.MARKET,
            shorting=False
        )
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
        strategy.order_logic.rebalance(drift_df=df)
        assert len(strategy.orders) == 0

    def test_selling_a_100_percent_short_position_creates_an_order_when_shorting_is_enabled(self):
        strategy = MockStrategyWithOrderLogic(
            broker=self.backtesting_broker,
            order_type=Order.OrderType.LIMIT,
            shorting=True
        )
        df = pd.DataFrame([
            {
                "symbol": "AAPL",
                "is_quote_asset": False,
                "current_quantity": Decimal("0"),
                "current_value": Decimal("0"),
                "current_weight": Decimal("0.0"),
                "target_weight": Decimal("-1"),
                "target_value": Decimal("-1000"),
                "drift": Decimal("-1")
            },
            {
                "symbol": "USD",
                "is_quote_asset": True,
                "current_quantity": Decimal("1000"),
                "current_value": Decimal("1000"),
                "current_weight": Decimal("1.0"),
                "target_weight": Decimal("0.0"),
                "target_value": Decimal("0"),
                "drift": Decimal("0")
            }
        ])

        strategy.order_logic.rebalance(drift_df=df)
        assert len(strategy.orders) == 1
        assert strategy.orders[0].quantity == Decimal("10")
        assert strategy.orders[0].side == "sell"
        assert strategy.orders[0].type == Order.OrderType.LIMIT

    def test_buying_something_when_we_have_enough_money_and_there_is_slippage(self):
        strategy = MockStrategyWithOrderLogic(
            broker=self.backtesting_broker,
            order_type=Order.OrderType.LIMIT,
        )
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
        strategy.order_logic.rebalance(drift_df=df)
        assert len(strategy.orders) == 1
        assert strategy.orders[0].side == "buy"
        assert strategy.orders[0].quantity == Decimal("9")

    def test_limit_buy_when_we_dont_have_enough_money_for_everything(self):
        strategy = MockStrategyWithOrderLogic(
            broker=self.backtesting_broker,
            order_type=Order.OrderType.LIMIT,
        )
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
        strategy.order_logic.rebalance(drift_df=df)
        assert len(strategy.orders) == 1
        assert strategy.orders[0].side == "buy"
        assert strategy.orders[0].quantity == Decimal("4")
        assert strategy.orders[0].type == Order.OrderType.LIMIT

    def test_market_buy_when_we_dont_have_enough_money_for_everything(self):
        strategy = MockStrategyWithOrderLogic(
            broker=self.backtesting_broker,
            order_type=Order.OrderType.MARKET,
        )
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
        strategy.order_logic.rebalance(drift_df=df)
        assert len(strategy.orders) == 1
        assert strategy.orders[0].side == "buy"
        assert strategy.orders[0].quantity == Decimal("4")
        assert strategy.orders[0].type == Order.OrderType.MARKET

    def test_attempting_to_buy_when_we_dont_have_enough_money_for_even_one_share(self):
        strategy = MockStrategyWithOrderLogic(
            broker=self.backtesting_broker,
            order_type=Order.OrderType.LIMIT,
        )
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
        strategy.order_logic.rebalance(drift_df=df)
        assert len(strategy.orders) == 0

    def test_attempting_to_sell_when_the_amount_we_need_to_sell_is_less_than_the_limit_price_should_not_sell(self):
        strategy = MockStrategyWithOrderLogic(
            broker=self.backtesting_broker,
            order_type=Order.OrderType.LIMIT,
        )
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
        strategy.order_logic.rebalance(drift_df=df)
        assert len(strategy.orders) == 0

    def test_calculate_limit_price_when_selling(self):
        strategy = MockStrategyWithOrderLogic(
            broker=self.backtesting_broker,
            order_type=Order.OrderType.LIMIT,
        )
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
        strategy.order_logic = DriftOrderLogic(
            strategy=strategy,
            acceptable_slippage=Decimal("0.005")
        )
        strategy.order_logic.rebalance(drift_df=df)
        limit_price = strategy.order_logic.calculate_limit_price(last_price=Decimal("120.00"), side="sell")
        assert limit_price == Decimal("119.4")

    def test_calculate_limit_price_when_buying(self):
        strategy = MockStrategyWithOrderLogic(
            broker=self.backtesting_broker,
            order_type=Order.OrderType.LIMIT,
        )
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
        strategy.order_logic = DriftOrderLogic(
            strategy=strategy,
            acceptable_slippage=Decimal("0.005")
        )
        strategy.order_logic.rebalance(drift_df=df)
        limit_price = strategy.order_logic.calculate_limit_price(last_price=Decimal("120.00"), side="buy")
        assert limit_price == Decimal("120.6")


class TestDriftRebalancer:

    # Need to start two days after the first data point in pandas for backtesting
    backtesting_start = datetime.datetime(2019, 1, 2)
    backtesting_end = datetime.datetime(2019, 12, 31)

    def test_classic_60_60(self, pandas_data_fixture):

        parameters = {
            "market": "NYSE",
            "sleeptime": "1D",
            "drift_type": DriftType.ABSOLUTE,
            "drift_threshold": "0.03",
            "order_type": Order.OrderType.LIMIT,
            "acceptable_slippage": "0.005",
            "fill_sleeptime": 15,
            "target_weights": {
                "SPY": "0.60",
                "TLT": "0.40"
            },
            "shorting": False
        }

        results, strat_obj = DriftRebalancer.run_backtest(
            datasource_class=PandasDataBacktesting,
            backtesting_start=self.backtesting_start,
            backtesting_end=self.backtesting_end,
            pandas_data=list(pandas_data_fixture.values()),
            parameters=parameters,
            show_plot=False,
            show_tearsheet=False,
            save_tearsheet=False,
            show_indicators=False,
            save_logfile=False,
            show_progress_bar=False,
            # quiet_logs=False,
        )

        assert results is not None
        assert np.isclose(results["cagr"], 0.22076538945204272, atol=1e-4)
        assert np.isclose(results["volatility"], 0.06740737779031068, atol=1e-4)
        assert np.isclose(results["sharpe"], 3.051823053251843, atol=1e-4)
        assert np.isclose(results["max_drawdown"]["drawdown"], 0.025697778711759052, atol=1e-4)

    def test_with_shorting(self):
        # TODO
        pass
