from typing import Union
from datetime import datetime, timedelta
from unittest.mock import patch
from decimal import Decimal
import pytest
from unittest.mock import PropertyMock


import pandas as pd
import pytz

from lumibot.example_strategies.drift_rebalancer import DriftRebalancer
from lumibot.components.drift_rebalancer_logic import DriftType
from lumibot.components.drift_rebalancer_logic import DriftRebalancerLogic, DriftCalculationLogic, DriftOrderLogic
from lumibot.backtesting import BacktestingBroker, PandasDataBacktesting, YahooDataBacktesting, PolygonDataBacktesting, \
    AlpacaBacktesting
from lumibot.strategies.strategy import Strategy
from lumibot.traders import Trader
from tests.fixtures import pandas_data_fixture
from lumibot.tools import print_full_pandas_dataframes, set_pandas_float_display_precision
from lumibot.entities import Order, Asset, TradingFee
from lumibot.credentials import ALPACA_TEST_CONFIG, POLYGON_CONFIG
from lumibot.components.drift_rebalancer_logic import get_last_price_or_raise
from lumibot.tools.helpers import quantize_to_num_decimals

print_full_pandas_dataframes()
set_pandas_float_display_precision(precision=5)


class MockStrategyWithDriftCalculationLogic(Strategy):

    def __init__(
            self,
            broker: BacktestingBroker,
            drift_threshold: Decimal = Decimal("0.05"),
            drift_type: DriftType = DriftType.ABSOLUTE,
            order_type: Order.OrderType = Order.OrderType.LIMIT,
            shorting: bool = False,
            fractional_shares: bool = False,
    ):
        super().__init__(broker)
        self.orders = []
        self.target_weights = {}
        self.drift_rebalancer_logic = DriftRebalancerLogic(
            strategy=self,
            drift_threshold=drift_threshold,
            fill_sleeptime=15,
            acceptable_slippage=Decimal("0.005"),
            shorting=shorting,
            drift_type=drift_type,
            order_type=order_type,
            fractional_shares=fractional_shares
        )

    def get_last_price(self, asset: Union[Asset, str], quote=None, exchange=None) -> Union[float, Decimal, None]:
        return Decimal(100.0)  # Mock price

    def update_broker_balances(self, force_update: bool = False) -> None:
        pass

    def submit_order(self, order, **kwargs):
        self.orders.append(order)
        return order


# @pytest.mark.skip()
class TestDriftCalculationLogic:

    def setup_method(self):
        date_start = datetime(2021, 7, 10)
        date_end = datetime(2021, 7, 13)
        self.data_source = PandasDataBacktesting(date_start, date_end)
        self.backtesting_broker = BacktestingBroker(self.data_source)

    def test_add_position(self, mocker):
        strategy = MockStrategyWithDriftCalculationLogic(broker=self.backtesting_broker)
        portfolio_weights = [
            {"base_asset": Asset(symbol="AAPL", asset_type="stock"), "weight": Decimal("0.5")},
            {"base_asset": Asset(symbol="GOOGL", asset_type="stock"), "weight": Decimal("0.3")},
            {"base_asset": Asset(symbol="MSFT", asset_type="stock"), "weight": Decimal("0.2")}
        ]

        def mock_add_positions(mock_self):
            mock_self._add_position(
                symbol="AAPL",
                base_asset=Asset(symbol="AAPL", asset_type="stock"),
                is_quote_asset=False,
                current_quantity=Decimal("10"),
                current_value=Decimal("1500")
            )
            mock_self._add_position(
                symbol="GOOGL",
                base_asset=Asset(symbol="GOOGL", asset_type="stock"),
                is_quote_asset=False,
                current_quantity=Decimal("5"),
                current_value=Decimal("1000")
            )
            mock_self._add_position(
                symbol="MSFT",
                base_asset=Asset(symbol="MSFT", asset_type="stock"),
                is_quote_asset=False,
                current_quantity=Decimal("8"),
                current_value=Decimal("800")
            )

        mocker.patch.object(DriftCalculationLogic, "_add_positions", mock_add_positions)
        df = strategy.drift_rebalancer_logic.calculate(portfolio_weights=portfolio_weights)
        assert df["symbol"].tolist() == ["AAPL", "GOOGL", "MSFT"]
        assert df["current_quantity"].tolist() == [Decimal("10"), Decimal("5"), Decimal("8")]
        assert df["current_value"].tolist() == [Decimal("1500"), Decimal("1000"), Decimal("800")]

    def test_calculate_absolute_drift(self, mocker):
        strategy = MockStrategyWithDriftCalculationLogic(
            broker=self.backtesting_broker,
            drift_threshold=Decimal("0.05"),
            drift_type=DriftType.ABSOLUTE
        )
        portfolio_weights = [
            {"base_asset": Asset(symbol="AAPL", asset_type="stock"), "weight": Decimal("0.5")},
            {"base_asset": Asset(symbol="GOOGL", asset_type="stock"), "weight": Decimal("0.3")},
            {"base_asset": Asset(symbol="MSFT", asset_type="stock"), "weight": Decimal("0.2")}
        ]

        def mock_add_positions(mock_self):
            mock_self._add_position(
                symbol="AAPL",
                base_asset=Asset(symbol="AAPL", asset_type="stock"),
                is_quote_asset=False,
                current_quantity=Decimal("10"),
                current_value=Decimal("1500")
            )
            mock_self._add_position(
                symbol="GOOGL",
                base_asset=Asset(symbol="GOOGL", asset_type="stock"),
                is_quote_asset=False,
                current_quantity=Decimal("5"),
                current_value=Decimal("1000")
            )
            mock_self._add_position(
                symbol="MSFT",
                base_asset=Asset(symbol="MSFT", asset_type="stock"),
                is_quote_asset=False,
                current_quantity=Decimal("8"),
                current_value=Decimal("800")
            )

        mocker.patch.object(DriftCalculationLogic, "_add_positions", mock_add_positions)
        df = strategy.drift_rebalancer_logic.calculate(portfolio_weights=portfolio_weights)

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

        portfolio_weights = [
            {"base_asset": Asset(symbol="AAPL", asset_type="stock"), "weight": Decimal("0.60")},
            {"base_asset": Asset(symbol="GOOGL", asset_type="stock"), "weight": Decimal("0.30")},
            {"base_asset": Asset(symbol="MSFT", asset_type="stock"), "weight": Decimal("0.10")}
        ]

        def mock_add_positions(mock_self):
            mock_self._add_position(
                symbol="AAPL",
                base_asset=Asset(symbol="AAPL", asset_type="stock"),
                is_quote_asset=False,
                current_quantity=Decimal("4"),
                current_value=Decimal("400")
            )
            mock_self._add_position(
                symbol="GOOGL",
                base_asset=Asset(symbol="GOOGL", asset_type="stock"),
                is_quote_asset=False,
                current_quantity=Decimal("4"),
                current_value=Decimal("400")
            )
            mock_self._add_position(
                symbol="MSFT",
                base_asset=Asset(symbol="MSFT", asset_type="stock"),
                is_quote_asset=False,
                current_quantity=Decimal("2"),
                current_value=Decimal("200")
            )

        mocker.patch.object(DriftCalculationLogic, "_add_positions", mock_add_positions)
        df = strategy.drift_rebalancer_logic.calculate(portfolio_weights=portfolio_weights)
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
        portfolio_weights = [
            {"base_asset": Asset(symbol="AAPL", asset_type="stock"), "weight": Decimal("0.5")},
            {"base_asset": Asset(symbol="GOOGL", asset_type="stock"), "weight": Decimal("0.3")},
            {"base_asset": Asset(symbol="MSFT", asset_type="stock"), "weight": Decimal("0.0")}
        ]

        def mock_add_positions(mock_self):
            mock_self._add_position(
                symbol="AAPL",
                base_asset=Asset(symbol="AAPL", asset_type="stock"),
                is_quote_asset=False,
                current_quantity=Decimal("10"),
                current_value=Decimal("1500")
            )
            mock_self._add_position(
                symbol="GOOGL",
                base_asset=Asset(symbol="GOOGL", asset_type="stock"),
                is_quote_asset=False,
                current_quantity=Decimal("5"),
                current_value=Decimal("1000")
            )
            mock_self._add_position(
                symbol="MSFT",
                base_asset=Asset(symbol="MSFT", asset_type="stock"),
                is_quote_asset=False,
                current_quantity=Decimal("8"),
                current_value=Decimal("800")
            )

        mocker.patch.object(DriftCalculationLogic, "_add_positions", mock_add_positions)
        df = strategy.drift_rebalancer_logic.calculate(portfolio_weights=portfolio_weights)

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
        portfolio_weights = [
            {"base_asset": Asset(symbol="AAPL", asset_type="stock"), "weight": Decimal("0.25")},
            {"base_asset": Asset(symbol="GOOGL", asset_type="stock"), "weight": Decimal("0.25")},
            {"base_asset": Asset(symbol="MSFT", asset_type="stock"), "weight": Decimal("0.25")},
            {"base_asset": Asset(symbol="AMZN", asset_type="stock"), "weight": Decimal("0.25")}
        ]

        def mock_add_positions(mock_self):
            mock_self._add_position(
                symbol="AAPL",
                base_asset=Asset(symbol="AAPL", asset_type="stock"),
                is_quote_asset=False,
                current_quantity=Decimal("10"),
                current_value=Decimal("1500")
            )
            mock_self._add_position(
                symbol="GOOGL",
                base_asset=Asset(symbol="GOOGL", asset_type="stock"),
                is_quote_asset=False,
                current_quantity=Decimal("5"),
                current_value=Decimal("1000")
            )
            mock_self._add_position(
                symbol="MSFT",
                base_asset=Asset(symbol="MSFT", asset_type="stock"),
                is_quote_asset=False,
                current_quantity=Decimal("8"),
                current_value=Decimal("800")
            )

        mocker.patch.object(DriftCalculationLogic, "_add_positions", mock_add_positions)
        df = strategy.drift_rebalancer_logic.calculate(portfolio_weights=portfolio_weights)

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

    def test_drift_is_negative_one_when_we_have_none_of_an_asset_and_target_weights_says_we_should_short_some(self,
                                                                                                              mocker):
        strategy = MockStrategyWithDriftCalculationLogic(
            broker=self.backtesting_broker,
            drift_threshold=Decimal("0.05"),
            drift_type=DriftType.ABSOLUTE,
            shorting=True
        )
        portfolio_weights = [
            {"base_asset": Asset(symbol="AAPL", asset_type="stock"), "weight": Decimal("0.25")},
            {"base_asset": Asset(symbol="GOOGL", asset_type="stock"), "weight": Decimal("0.25")},
            {"base_asset": Asset(symbol="MSFT", asset_type="stock"), "weight": Decimal("0.25")},
            {"base_asset": Asset(symbol="AMZN", asset_type="stock"), "weight": Decimal("-0.25")}
        ]

        def mock_add_positions(mock_self):
            mock_self._add_position(
                symbol="AAPL",
                base_asset=Asset(symbol="AAPL", asset_type="stock"),
                is_quote_asset=False,
                current_quantity=Decimal("10"),
                current_value=Decimal("1500")
            )
            mock_self._add_position(
                symbol="GOOGL",
                base_asset=Asset(symbol="GOOGL", asset_type="stock"),
                is_quote_asset=False,
                current_quantity=Decimal("5"),
                current_value=Decimal("1000")
            )
            mock_self._add_position(
                symbol="MSFT",
                base_asset=Asset(symbol="MSFT", asset_type="stock"),
                is_quote_asset=False,
                current_quantity=Decimal("8"),
                current_value=Decimal("800")
            )

        mocker.patch.object(DriftCalculationLogic, "_add_positions", mock_add_positions)
        df = strategy.drift_rebalancer_logic.calculate(portfolio_weights=portfolio_weights)

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
        portfolio_weights = [
            {"base_asset": Asset(symbol="AAPL", asset_type="stock"), "weight": Decimal("0.25")},
            {"base_asset": Asset(symbol="GOOGL", asset_type="stock"), "weight": Decimal("0.25")},
            {"base_asset": Asset(symbol="MSFT", asset_type="stock"), "weight": Decimal("0.25")},
            {"base_asset": Asset(symbol="AMZN", asset_type="stock"), "weight": Decimal("0.0")}
        ]

        def mock_add_positions(mock_self):
            mock_self._add_position(
                symbol="AAPL",
                base_asset=Asset(symbol="AAPL", asset_type="stock"),
                is_quote_asset=False,
                current_quantity=Decimal("10"),
                current_value=Decimal("1500")
            )
            mock_self._add_position(
                symbol="GOOGL",
                base_asset=Asset(symbol="GOOGL", asset_type="stock"),
                is_quote_asset=False,
                current_quantity=Decimal("5"),
                current_value=Decimal("1000")
            )
            mock_self._add_position(
                symbol="MSFT",
                base_asset=Asset(symbol="MSFT", asset_type="stock"),
                is_quote_asset=False,
                current_quantity=Decimal("8"),
                current_value=Decimal("800")
            )

        mocker.patch.object(DriftCalculationLogic, "_add_positions", mock_add_positions)
        df = strategy.drift_rebalancer_logic.calculate(portfolio_weights=portfolio_weights)

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
        portfolio_weights = [
            {"base_asset": Asset(symbol="AAPL", asset_type="stock"), "weight": Decimal("0.5")},
            {"base_asset": Asset(symbol="GOOGL", asset_type="stock"), "weight": Decimal("0.3")},
            {"base_asset": Asset(symbol="MSFT", asset_type="stock"), "weight": Decimal("0.2")}
        ]

        def mock_add_positions(mock_self):
            mock_self._add_position(
                symbol="USD",
                base_asset=Asset(symbol="USD", asset_type="forex"),
                is_quote_asset=True,
                current_quantity=Decimal("1000"),
                current_value=Decimal("1000")
            )
            mock_self._add_position(
                symbol="AAPL",
                base_asset=Asset(symbol="AAPL", asset_type="stock"),
                is_quote_asset=False,
                current_quantity=Decimal("10"),
                current_value=Decimal("1500")
            )
            mock_self._add_position(
                symbol="GOOGL",
                base_asset=Asset(symbol="GOOGL", asset_type="stock"),
                is_quote_asset=False,
                current_quantity=Decimal("5"),
                current_value=Decimal("1000")
            )
            mock_self._add_position(
                symbol="MSFT",
                base_asset=Asset(symbol="MSFT", asset_type="stock"),
                is_quote_asset=False,
                current_quantity=Decimal("8"),
                current_value=Decimal("800")
            )

        mocker.patch.object(DriftCalculationLogic, "_add_positions", mock_add_positions)
        df = strategy.drift_rebalancer_logic.calculate(portfolio_weights=portfolio_weights)

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

        portfolio_weights = [
            {"base_asset": Asset(symbol="AAPL", asset_type="stock"), "weight": Decimal("0.5")},
            {"base_asset": Asset(symbol="GOOGL", asset_type="stock"), "weight": Decimal("0.3")},
            {"base_asset": Asset(symbol="MSFT", asset_type="stock"), "weight": Decimal("0.2")}
        ]

        def mock_add_positions(mock_self):
            mock_self._add_position(
                symbol="USD",
                base_asset=Asset(symbol="USD", asset_type="forex"),
                is_quote_asset=True,
                current_quantity=Decimal("1000"),
                current_value=Decimal("1000")
            )
            mock_self._add_position(
                symbol="AAPL",
                base_asset=Asset(symbol="AAPL", asset_type="stock"),
                is_quote_asset=False,
                current_quantity=Decimal("10"),
                current_value=Decimal("1500")
            )
            mock_self._add_position(
                symbol="GOOGL",
                base_asset=Asset(symbol="GOOGL", asset_type="stock"),
                is_quote_asset=False,
                current_quantity=Decimal("5"),
                current_value=Decimal("1000")
            )
            mock_self._add_position(
                symbol="MSFT",
                base_asset=Asset(symbol="MSFT", asset_type="stock"),
                is_quote_asset=False,
                current_quantity=Decimal("8"),
                current_value=Decimal("800")
            )

        mocker.patch.object(DriftCalculationLogic, "_add_positions", mock_add_positions)
        df = strategy.drift_rebalancer_logic.calculate(portfolio_weights=portfolio_weights)

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

        portfolio_weights = [
            {"base_asset": Asset(symbol="AAPL", asset_type="stock"), "weight": Decimal("0.25")},
            {"base_asset": Asset(symbol="GOOGL", asset_type="stock"), "weight": Decimal("0.25")},
            {"base_asset": Asset(symbol="USD", asset_type="forex"), "weight": Decimal("0.5")}
        ]

        def mock_add_positions(mock_self):
            mock_self._add_position(
                symbol="USD",
                base_asset=Asset(symbol="USD", asset_type="forex"),
                is_quote_asset=True,
                current_quantity=Decimal("0"),
                current_value=Decimal("0")
            )
            mock_self._add_position(
                symbol="AAPL",
                base_asset=Asset(symbol="AAPL", asset_type="stock"),
                is_quote_asset=False,
                current_quantity=Decimal("5"),
                current_value=Decimal("500")
            )
            mock_self._add_position(
                symbol="GOOGL",
                base_asset=Asset(symbol="GOOGL", asset_type="stock"),
                is_quote_asset=False,
                current_quantity=Decimal("10"),
                current_value=Decimal("500")
            )

        mocker.patch.object(DriftCalculationLogic, "_add_positions", mock_add_positions)
        df = strategy.drift_rebalancer_logic.calculate(portfolio_weights=portfolio_weights)

        assert df["current_weight"].tolist() == [Decimal("0.5"), Decimal("0.5"), Decimal("0.0")]
        assert df["target_value"].tolist() == [Decimal("250"), Decimal("250"), Decimal("500")]
        assert df["drift"].tolist() == [Decimal("-0.25"), Decimal("-0.25"), Decimal("0")]

    def test_calculate_drift_when_we_want_short_something(self, mocker):
        strategy = MockStrategyWithDriftCalculationLogic(
            broker=self.backtesting_broker,
            drift_threshold=Decimal("0.05"),
            drift_type=DriftType.ABSOLUTE
        )

        portfolio_weights = [
            {"base_asset": Asset(symbol="AAPL", asset_type="stock"), "weight": Decimal("-0.5")},
            {"base_asset": Asset(symbol="USD", asset_type="forex"), "weight": Decimal("0.5")}
        ]

        def mock_add_positions(mock_self):
            mock_self._add_position(
                symbol="USD",
                base_asset=Asset(symbol="USD", asset_type="forex"),
                is_quote_asset=True,
                current_quantity=Decimal("1000"),
                current_value=Decimal("1000")
            )
            mock_self._add_position(
                symbol="AAPL",
                base_asset=Asset(symbol="AAPL", asset_type="stock"),
                is_quote_asset=False,
                current_quantity=Decimal("0"),
                current_value=Decimal("0")
            )

        mocker.patch.object(DriftCalculationLogic, "_add_positions", mock_add_positions)
        df = strategy.drift_rebalancer_logic.calculate(portfolio_weights=portfolio_weights)

        assert df["current_weight"].tolist() == [Decimal("0.0"), Decimal("1.0")]
        assert df["target_value"].tolist() == [Decimal("-500"), Decimal("500")]
        assert df["drift"].tolist() == [Decimal("-1.0"), Decimal("0")]

    def test_shorting_more_when_price_goes_up_short_something(self, mocker):
        strategy = MockStrategyWithDriftCalculationLogic(
            broker=self.backtesting_broker,
            drift_threshold=Decimal("0.05"),
            drift_type=DriftType.ABSOLUTE
        )
        # patch the strategy so get_last_price returns 110
        mocker.patch.object(strategy, "get_last_price", return_value=110.0)

        portfolio_weights = [
            {"base_asset": Asset(symbol="AAPL", asset_type="stock"), "weight": Decimal("-0.5")},
            {"base_asset": Asset(symbol="USD", asset_type="forex"), "weight": Decimal("0.5")}
        ]

        def mock_add_positions(mock_self):
            mock_self._add_position(
                symbol="USD",
                base_asset=Asset(symbol="USD", asset_type="forex"),
                is_quote_asset=True,
                current_quantity=Decimal("1500"),  # original $1000 plus $500 from the short
                current_value=Decimal("1500")
            )
            mock_self._add_position(
                symbol="AAPL",
                base_asset=Asset(symbol="AAPL", asset_type="stock"),
                is_quote_asset=False,
                current_quantity=Decimal("-5"),
                current_value=Decimal("-550")
            )

        mocker.patch.object(DriftCalculationLogic, "_add_positions", mock_add_positions)
        df = strategy.drift_rebalancer_logic.calculate(portfolio_weights=portfolio_weights)
        assert df["current_weight"].tolist() == [Decimal('-0.5789473684210526315789473684'),
                                                 Decimal('1.578947368421052631578947368')]
        assert df["target_value"].tolist() == [Decimal("-475"), Decimal("475")]
        assert df["drift"].tolist() == [Decimal('0.0789473684210526315789473684'), Decimal('0')]

    def test_calculate_absolute_drift_when_we_want_a_100_percent_short_position(self, mocker):
        strategy = MockStrategyWithDriftCalculationLogic(
            broker=self.backtesting_broker,
            drift_threshold=Decimal("0.05"),
            drift_type=DriftType.ABSOLUTE
        )

        portfolio_weights = [
            {"base_asset": Asset(symbol="AAPL", asset_type="stock"), "weight": Decimal("0.25")},
            {"base_asset": Asset(symbol="GOOGL", asset_type="stock"), "weight": Decimal("0.25")},
            {"base_asset": Asset(symbol="USD", asset_type="forex"), "weight": Decimal("0.5")}
        ]

        def mock_add_positions(mock_self):
            mock_self._add_position(
                symbol="USD",
                base_asset=Asset(symbol="USD", asset_type="forex"),
                is_quote_asset=True,
                current_quantity=Decimal("0"),
                current_value=Decimal("0")
            )
            mock_self._add_position(
                symbol="AAPL",
                base_asset=Asset(symbol="AAPL", asset_type="stock"),
                is_quote_asset=False,
                current_quantity=Decimal("5"),
                current_value=Decimal("500")
            )
            mock_self._add_position(
                symbol="GOOGL",
                base_asset=Asset(symbol="GOOGL", asset_type="stock"),
                is_quote_asset=False,
                current_quantity=Decimal("10"),
                current_value=Decimal("500")
            )

        mocker.patch.object(DriftCalculationLogic, "_add_positions", mock_add_positions)
        df = strategy.drift_rebalancer_logic.calculate(portfolio_weights=portfolio_weights)

        assert df["current_weight"].tolist() == [Decimal("0.5"), Decimal("0.5"), Decimal("0.0")]
        assert df["target_value"].tolist() == [Decimal("250"), Decimal("250"), Decimal("500")]
        assert df["drift"].tolist() == [Decimal("-0.25"), Decimal("-0.25"), Decimal("0")]

    def test_calculate_absolute_drift_when_we_want_a_100_percent_short_position_and_cash_in_target_weights(self,
                                                                                                           mocker):
        strategy = MockStrategyWithDriftCalculationLogic(
            broker=self.backtesting_broker,
            drift_threshold=Decimal("0.05"),
            drift_type=DriftType.ABSOLUTE
        )

        portfolio_weights = [
            {"base_asset": Asset(symbol="AAPL", asset_type="stock"), "weight": Decimal("-1.0")},
            {"base_asset": Asset(symbol="USD", asset_type="forex"), "weight": Decimal("0.0")}
        ]

        def mock_add_positions(mock_self):
            mock_self._add_position(
                symbol="USD",
                base_asset=Asset(symbol="USD", asset_type="forex"),
                is_quote_asset=True,
                current_quantity=Decimal("1000"),
                current_value=Decimal("1000")
            )
            mock_self._add_position(
                symbol="AAPL",
                base_asset=Asset(symbol="AAPL", asset_type="stock"),
                is_quote_asset=False,
                current_quantity=Decimal("0"),
                current_value=Decimal("0")
            )

        mocker.patch.object(DriftCalculationLogic, "_add_positions", mock_add_positions)
        df = strategy.drift_rebalancer_logic.calculate(portfolio_weights=portfolio_weights)

        assert df["current_weight"].tolist() == [Decimal("0.0"), Decimal("1.0")]
        assert df["target_value"].tolist() == [Decimal("-1000"), Decimal("0")]
        assert df["drift"].tolist() == [Decimal("-1.0"), Decimal("0")]

    def test_calculate_relative_drift_when_we_want_a_100_percent_short_position(self, mocker):
        strategy = MockStrategyWithDriftCalculationLogic(
            broker=self.backtesting_broker,
            drift_threshold=Decimal("0.05"),
            drift_type=DriftType.RELATIVE
        )

        portfolio_weights = [
            {"base_asset": Asset(symbol="AAPL", asset_type="stock"), "weight": Decimal("-1.0")},
            {"base_asset": Asset(symbol="USD", asset_type="forex"), "weight": Decimal("0.0")}
        ]

        def mock_add_positions(mock_self):
            mock_self._add_position(
                symbol="USD",
                base_asset=Asset(symbol="USD", asset_type="forex"),
                is_quote_asset=True,
                current_quantity=Decimal("1000"),
                current_value=Decimal("1000")
            )
            mock_self._add_position(
                symbol="AAPL",
                base_asset=Asset(symbol="AAPL", asset_type="stock"),
                is_quote_asset=False,
                current_quantity=Decimal("0"),
                current_value=Decimal("0")
            )

        mocker.patch.object(DriftCalculationLogic, "_add_positions", mock_add_positions)
        df = strategy.drift_rebalancer_logic.calculate(portfolio_weights=portfolio_weights)

        assert df["current_weight"].tolist() == [Decimal("0.0"), Decimal("1.0")]
        assert df["target_value"].tolist() == [Decimal("-1000"), Decimal("0")]
        assert df["drift"].tolist() == [Decimal("-1.0"), Decimal("0")]


class MockStrategyWithOrderLogic(Strategy):

    def __init__(
            self,
            broker: BacktestingBroker,
            drift_threshold: Decimal = Decimal("0.05"),
            shorting: bool = False,
            order_type: Order.OrderType = Order.OrderType.LIMIT,
            fractional_shares: bool = False,
    ):
        super().__init__(broker)
        self.orders = []
        self.target_weights = {}
        self.order_logic = DriftOrderLogic(
            strategy=self,
            drift_threshold=drift_threshold,
            fill_sleeptime=15,
            acceptable_slippage=Decimal("0.005"),
            shorting=shorting,
            order_type=order_type,
            fractional_shares=fractional_shares
        )

    def get_last_price(self, asset: Union[Asset, str], quote=None, exchange=None) -> Union[float, Decimal, None]:
        return Decimal(100.0)  # Mock price

    def update_broker_balances(self, force_update: bool = False) -> None:
        pass

    def submit_order(self, order, **kwargs):
        self.orders.append(order)
        return order


# @pytest.mark.skip()
class TestDriftOrderLogic:

    def setup_method(self):
        date_start = datetime(2021, 7, 10)
        date_end = datetime(2021, 7, 13)
        self.data_source = PandasDataBacktesting(date_start, date_end)
        self.backtesting_broker = BacktestingBroker(self.data_source)

    def test_selling_everything_with_limit_orders(self):
        strategy = MockStrategyWithOrderLogic(
            broker=self.backtesting_broker,
            order_type=Order.OrderType.LIMIT
        )
        df = pd.DataFrame({
            "symbol": ["AAPL"],
            "base_asset": [Asset("AAPL", "stock")],
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
        assert strategy.orders[0].order_type == Order.OrderType.LIMIT

    def test_selling_everything_with_market_orders(self):
        strategy = MockStrategyWithOrderLogic(
            broker=self.backtesting_broker,
            order_type=Order.OrderType.MARKET
        )
        df = pd.DataFrame({
            "symbol": ["AAPL"],
            "base_asset": [Asset("AAPL", "stock")],
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
        assert strategy.orders[0].order_type == Order.OrderType.MARKET

    def test_selling_part_of_a_holding_with_limit_order(self):
        strategy = MockStrategyWithOrderLogic(
            broker=self.backtesting_broker,
            order_type=Order.OrderType.LIMIT
        )
        df = pd.DataFrame({
            "symbol": ["AAPL"],
            "base_asset": [Asset("AAPL", "stock")],
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
        assert strategy.orders[0].order_type == Order.OrderType.LIMIT

    def test_selling_with_only_rebalance_drifted_assets_when_over_drift_threshold(self):
        strategy = MockStrategyWithOrderLogic(
            broker=self.backtesting_broker,
            order_type=Order.OrderType.LIMIT
        )
        df = pd.DataFrame({
            "symbol": ["AAPL"],
            "base_asset": [Asset("AAPL", "stock")],
            "is_quote_asset": False,
            "current_quantity": [Decimal("10")],
            "current_value": [Decimal("1000")],
            "current_weight": [Decimal("1.0")],
            "target_weight": Decimal("0.5"),
            "target_value": Decimal("500"),
            "drift": Decimal("-0.5")
        })
        strategy.order_logic.only_rebalance_drifted_assets = True
        strategy.order_logic.rebalance(drift_df=df)
        assert len(strategy.orders) == 1
        assert strategy.orders[0].side == "sell"
        assert strategy.orders[0].quantity == Decimal("5")
        assert strategy.orders[0].order_type == Order.OrderType.LIMIT

    def test_selling_with_only_rebalance_drifted_assets_when_not_over_drift_threshold(self):
        strategy = MockStrategyWithOrderLogic(
            broker=self.backtesting_broker,
            order_type=Order.OrderType.LIMIT
        )
        df = pd.DataFrame({
            "symbol": ["AAPL"],
            "base_asset": [Asset("AAPL", "stock")],
            "is_quote_asset": False,
            "current_quantity": [Decimal("10")],
            "current_value": [Decimal("1000")],
            "current_weight": [Decimal("1.0")],
            "target_weight": Decimal("0.5"),
            "target_value": Decimal("500"),
            "drift": Decimal("-0.001")  # make drift small
        })
        strategy.order_logic.only_rebalance_drifted_assets = True
        strategy.order_logic.rebalance(drift_df=df)
        assert len(strategy.orders) == 0

    def test_selling_part_of_a_holding_with_market_order(self):
        strategy = MockStrategyWithOrderLogic(
            broker=self.backtesting_broker,
            order_type=Order.OrderType.MARKET
        )
        df = pd.DataFrame({
            "symbol": ["AAPL"],
            "base_asset": [Asset("AAPL", "stock")],
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
        assert strategy.orders[0].order_type == Order.OrderType.MARKET

    def test_selling_short_doesnt_create_and_order_when_shorting_is_disabled(self):
        strategy = MockStrategyWithOrderLogic(
            broker=self.backtesting_broker,
            order_type=Order.OrderType.LIMIT
        )
        df = pd.DataFrame({
            "symbol": ["AAPL"],
            "base_asset": [Asset("AAPL", "stock")],
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
            "base_asset": [Asset("AAPL", "stock")],
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
            "base_asset": [Asset("AAPL", "stock")],
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
            self.backtesting_broker,
            order_type=Order.OrderType.LIMIT,
            shorting=True
        )
        df = pd.DataFrame([
            {
                "symbol": "AAPL",
                "base_asset": Asset("AAPL", "stock"),
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
                "base_asset": Asset("USD", "forex"),
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
        assert strategy.orders[0].order_type == Order.OrderType.LIMIT

    def test_buying_something_when_we_have_enough_money_and_there_is_slippage(self):
        strategy = MockStrategyWithOrderLogic(
            broker=self.backtesting_broker,
            order_type=Order.OrderType.LIMIT,
        )
        df = pd.DataFrame({
            "symbol": ["AAPL"],
            "base_asset": [Asset("AAPL", "stock")],
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
            "base_asset": [Asset("AAPL", "stock")],
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
        assert strategy.orders[0].order_type == Order.OrderType.LIMIT

    def test_market_buy_when_we_dont_have_enough_money_for_everything(self):
        strategy = MockStrategyWithOrderLogic(
            broker=self.backtesting_broker,
            order_type=Order.OrderType.MARKET,
        )
        strategy._set_cash_position(cash=500.0)
        df = pd.DataFrame({
            "symbol": ["AAPL"],
            "base_asset": [Asset("AAPL", "stock")],
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
        assert strategy.orders[0].order_type == Order.OrderType.MARKET

    def test_attempting_to_buy_when_we_dont_have_enough_money_for_even_one_share(self):
        strategy = MockStrategyWithOrderLogic(
            broker=self.backtesting_broker,
            order_type=Order.OrderType.LIMIT,
        )
        strategy._set_cash_position(cash=50.0)
        df = pd.DataFrame({
            "symbol": ["AAPL"],
            "base_asset": [Asset("AAPL", "stock")],
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
            "base_asset": [Asset("AAPL", "stock")],
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
            "base_asset": [Asset("AAPL", "stock")],
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
            "base_asset": [Asset("AAPL", "stock")],
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

    def test_buying_whole_shares(self):
        strategy = MockStrategyWithOrderLogic(
            broker=self.backtesting_broker,
            order_type=Order.OrderType.LIMIT,
            fractional_shares=False
        )
        df = pd.DataFrame({
            "symbol": ["AAPL"],
            "base_asset": [Asset("AAPL", "stock")],
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

    def test_buying_fractional_shares(self):
        strategy = MockStrategyWithOrderLogic(
            broker=self.backtesting_broker,
            order_type=Order.OrderType.LIMIT,
            fractional_shares=True
        )
        strategy._set_cash_position(cash=950.0)
        df = pd.DataFrame({
            "symbol": ["AAPL"],
            "base_asset": [Asset("AAPL", "stock")],
            "is_quote_asset": False,
            "current_quantity": [Decimal("0")],
            "current_value": [Decimal("0")],
            "current_weight": [Decimal("0.0")],
            "target_weight": Decimal("1"),
            "target_value": Decimal("950"),
            "drift": Decimal("1")
        })
        strategy.order_logic.rebalance(drift_df=df)
        assert len(strategy.orders) == 1
        assert strategy.orders[0].side == "buy"
        assert strategy.orders[0].quantity == Decimal("9.452736318")

    def test_selling_everything_with_fractional_limit_orders(self):
        strategy = MockStrategyWithOrderLogic(
            broker=self.backtesting_broker,
            order_type=Order.OrderType.LIMIT,
            fractional_shares=True
        )
        df = pd.DataFrame({
            "symbol": ["AAPL"],
            "base_asset": [Asset("AAPL", "stock")],
            "is_quote_asset": False,
            "current_quantity": [Decimal("9.5")],
            "current_value": [Decimal("950")],
            "current_weight": [Decimal("1.0")],
            "target_weight": Decimal("0"),
            "target_value": Decimal("0"),
            "drift": Decimal("-1")
        })

        strategy.order_logic.rebalance(drift_df=df)
        assert len(strategy.orders) == 1
        assert strategy.orders[0].side == "sell"
        assert strategy.orders[0].quantity == Decimal("9.5")
        assert strategy.orders[0].order_type == Order.OrderType.LIMIT

    def test_selling_some_with_fractional_limit_orders(self):
        strategy = MockStrategyWithOrderLogic(
            broker=self.backtesting_broker,
            order_type=Order.OrderType.LIMIT,
            fractional_shares=True
        )
        df = pd.DataFrame({
            "symbol": ["AAPL"],
            "base_asset": [Asset("AAPL", "stock")],
            "is_quote_asset": False,
            "current_quantity": [Decimal("10")],
            "current_value": [Decimal("1000")],
            "current_weight": [Decimal("1.0")],
            "target_weight": Decimal("0.85"),
            "target_value": Decimal("850.0"),
            "drift": Decimal("-0.15")
        })

        strategy.order_logic.rebalance(drift_df=df)
        assert len(strategy.orders) == 1
        assert strategy.orders[0].side == "sell"
        assert strategy.orders[0].quantity == Decimal("1.507537688")
        assert strategy.orders[0].order_type == Order.OrderType.LIMIT

    def test_buying_with_only_rebalance_drifted_assets_when_over_drift_threshold(self):
        strategy = MockStrategyWithOrderLogic(
            broker=self.backtesting_broker,
            order_type=Order.OrderType.LIMIT
        )
        df = pd.DataFrame({
            "symbol": ["AAPL"],
            "base_asset": [Asset("AAPL", "stock")],
            "is_quote_asset": False,
            "current_quantity": [Decimal("0")],
            "current_value": [Decimal("0")],
            "current_weight": [Decimal("0.0")],
            "target_weight": Decimal("0.5"),
            "target_value": Decimal("500"),
            "drift": Decimal("0.5")
        })
        strategy.order_logic.only_rebalance_drifted_assets = True
        strategy.order_logic.rebalance(drift_df=df)
        assert len(strategy.orders) == 1
        assert strategy.orders[0].side == "buy"
        assert strategy.orders[0].quantity == Decimal("4.0")  #
        assert strategy.orders[0].order_type == Order.OrderType.LIMIT

    def test_buying_with_only_rebalance_drifted_assets_when_not_over_drift_threshold(self):
        strategy = MockStrategyWithOrderLogic(
            broker=self.backtesting_broker,
            order_type=Order.OrderType.LIMIT
        )
        df = pd.DataFrame({
            "symbol": ["AAPL"],
            "base_asset": [Asset("AAPL", "stock")],
            "is_quote_asset": False,
            "current_quantity": [Decimal("0")],
            "current_value": [Decimal("0")],
            "current_weight": [Decimal("0.0")],
            "target_weight": Decimal("0.5"),
            "target_value": Decimal("500"),
            "drift": Decimal("0.001")  # make drift small
        })
        strategy.order_logic.only_rebalance_drifted_assets = True
        strategy.order_logic.rebalance(drift_df=df)
        assert len(strategy.orders) == 0

    def test_calculate_trading_costs_with_percent_fees(self):
        strategy = MockStrategyWithOrderLogic(
            broker=self.backtesting_broker,
            order_type=Order.OrderType.LIMIT
        )
        quantity = Decimal("100")
        price = Decimal("50")

        total_cost = strategy.order_logic.calculate_trading_costs(
            quantity,
            price,
            TradingFee(percent_fee=Decimal("0.0025"))
        )
        assert total_cost == Decimal("12.50")

    def test_calculate_trading_costs_with_flat_fees(self):
        strategy = MockStrategyWithOrderLogic(
            broker=self.backtesting_broker,
            order_type=Order.OrderType.LIMIT
        )
        quantity = Decimal("100")
        price = Decimal("50")

        total_cost = strategy.order_logic.calculate_trading_costs(
            quantity,
            price,
            TradingFee(flat_fee=Decimal("1.00"))
        )
        assert total_cost == Decimal("1.00")

    def test_calculate_trading_costs_with_both_fee_types(self):
        strategy = MockStrategyWithOrderLogic(
            broker=self.backtesting_broker,
            order_type=Order.OrderType.LIMIT
        )
        quantity = Decimal("100")
        price = Decimal("50")

        total_cost = strategy.order_logic.calculate_trading_costs(
            quantity,
            price,
            [TradingFee(flat_fee=Decimal("1.00")), TradingFee(percent_fee=Decimal("0.0025"))]
        )
        assert total_cost == Decimal("13.50")

    def test_calculate_trading_costs_no_fees(self):
        strategy = MockStrategyWithOrderLogic(
            broker=self.backtesting_broker,
            order_type=Order.OrderType.LIMIT
        )
        quantity = Decimal("100")
        price = Decimal("50")

        total_cost = strategy.order_logic.calculate_trading_costs(quantity, price, [])
        assert total_cost == Decimal("0")

    def test_adjust_quantity_for_fees_buy_affordable(self):
        strategy = MockStrategyWithOrderLogic(
            broker=self.backtesting_broker,
            order_type=Order.OrderType.LIMIT
        )
        desired_quantity = Decimal("10")
        last_price = Decimal("5")
        capital_available = Decimal("100")
        side = "buy"

        adjusted_quantity = strategy.order_logic.adjust_quantity_for_fees(
            desired_quantity,
            last_price,
            side,
            TradingFee(percent_fee=Decimal("0.0025")),
            capital_available
        )
        assert adjusted_quantity == Decimal("10")

    def test_adjust_quantity_for_fees_buy_not_affordable(self):
        strategy = MockStrategyWithOrderLogic(
            broker=self.backtesting_broker,
            order_type=Order.OrderType.LIMIT
        )
        desired_quantity = Decimal("20")
        last_price = Decimal("50")
        capital_available = Decimal("950")  # Not enough to buy at the desired quantity
        side = "buy"

        adjusted_quantity = strategy.order_logic.adjust_quantity_for_fees(
            desired_quantity,
            last_price,
            side,
            TradingFee(percent_fee=Decimal("0.0025")),
            capital_available
        )

        assert adjusted_quantity == Decimal("18.9500")

    def test_adjust_quantity_for_fees_sell(self):
        strategy = MockStrategyWithOrderLogic(
            broker=self.backtesting_broker,
            order_type=Order.OrderType.LIMIT
        )
        desired_quantity = Decimal("50")
        last_price = Decimal("10")
        capital_available = Decimal("1000")
        side = "sell"

        adjusted_quantity = strategy.order_logic.adjust_quantity_for_fees(
            desired_quantity,
            last_price,
            side,
            TradingFee(percent_fee=Decimal("0.0025")),
            capital_available
        )

        assert adjusted_quantity == desired_quantity

    def test_get_current_cash_position(self, mocker):
        strategy = MockStrategyWithOrderLogic(
            broker=self.backtesting_broker,
            order_type=Order.OrderType.LIMIT
        )
        # Use mocker to mock the cash property
        mocker.patch.object(
            type(strategy), "cash", new_callable=PropertyMock(return_value=Decimal("15000.50"))
        )

        cash_position = strategy.order_logic.get_current_cash_position()
        assert cash_position == Decimal("15000.50")

        mocker.patch.object(
            type(strategy), "cash", new_callable=PropertyMock(return_value=Decimal("15000.666666"))
        )

        cash_position = strategy.order_logic.get_current_cash_position()
        assert cash_position == Decimal("15000.66")


# @pytest.mark.skip()
class TestDriftRebalancer:
    # Need to start two days after the first data point in pandas for backtesting
    backtesting_start = datetime(2019, 1, 2)
    backtesting_end = datetime(2019, 2, 28)

    # @pytest.mark.skip()
    def test_classic_60_40(self, pandas_data_fixture):
        parameters = {
            "market": "NYSE",
            "sleeptime": "1D",
            "drift_type": DriftType.ABSOLUTE,
            "drift_threshold": "0.03",
            "order_type": Order.OrderType.LIMIT,
            "acceptable_slippage": "0.005",
            "fill_sleeptime": 15,
            "portfolio_weights": [
                {
                    "base_asset": Asset(symbol='SPY', asset_type='stock'),
                    "weight": Decimal("0.6")
                },
                {
                    "base_asset": Asset(symbol='TLT', asset_type='stock'),
                    "weight": Decimal("0.4")
                }
            ],
            "shorting": False
        }

        strat_obj: Strategy
        results, strat_obj = DriftRebalancer.run_backtest(
            datasource_class=PandasDataBacktesting,
            backtesting_start=self.backtesting_start,
            backtesting_end=self.backtesting_end,
            pandas_data=pandas_data_fixture,
            parameters=parameters,
            benchmark_asset=None,
            analyze_backtest=False,
            show_progress_bar=False,
        )

        trades_df = strat_obj.broker._trade_event_log_df

        # Get all the filled limit orders
        filled_orders = trades_df[(trades_df["status"] == "fill")]

        assert filled_orders.iloc[0]["type"] == "limit"
        assert filled_orders.iloc[0]["side"] == "buy"
        assert filled_orders.iloc[0]["symbol"] == "SPY"
        assert filled_orders.iloc[0]["filled_quantity"] == 238.0

        assert filled_orders.iloc[2]["type"] == "limit"
        assert filled_orders.iloc[2]["side"] == "sell"
        assert filled_orders.iloc[2]["symbol"] == "SPY"
        assert filled_orders.iloc[2]["filled_quantity"] == 7.0

    # @pytest.mark.skip()
    def test_classic_60_40_with_fractional(self, pandas_data_fixture):
        parameters = {
            "market": "NYSE",
            "sleeptime": "1D",
            "drift_type": DriftType.ABSOLUTE,
            "drift_threshold": "0.03",
            "order_type": Order.OrderType.LIMIT,
            "acceptable_slippage": "0.005",
            "fill_sleeptime": 15,
            "portfolio_weights": [
                {
                    "base_asset": Asset(symbol='SPY', asset_type='stock'),
                    "weight": Decimal("0.6")
                },
                {
                    "base_asset": Asset(symbol='TLT', asset_type='stock'),
                    "weight": Decimal("0.4")
                }
            ],
            "shorting": False,
            "fractional_shares": True
        }

        strat_obj: Strategy
        results, strat_obj = DriftRebalancer.run_backtest(
            datasource_class=PandasDataBacktesting,
            backtesting_start=self.backtesting_start,
            backtesting_end=self.backtesting_end,
            pandas_data=pandas_data_fixture,
            parameters=parameters,
            benchmark_asset=None,
            analyze_backtest=False,
            show_progress_bar=False,
        )

        trades_df = strat_obj.broker._trade_event_log_df

        # Get all the filled limit orders
        filled_orders = trades_df[(trades_df["status"] == "fill")]

        assert filled_orders.iloc[0]["type"] == "limit"
        assert filled_orders.iloc[0]["side"] == "buy"
        assert filled_orders.iloc[0]["symbol"] == "SPY"
        assert filled_orders.iloc[0]["filled_quantity"] == 238.634160544

        assert filled_orders.iloc[2]["type"] == "limit"
        assert filled_orders.iloc[2]["side"] == "sell"
        assert filled_orders.iloc[2]["symbol"] == "SPY"
        assert filled_orders.iloc[2]["filled_quantity"] == 8.346738266

    # @pytest.mark.skip()
    def test_crypto_50_50_with_yahoo(self):
        parameters = {
            "market": "24/7",
            "sleeptime": "1D",
            "drift_type": DriftType.ABSOLUTE,
            "drift_threshold": "0.03",
            "order_type": Order.OrderType.LIMIT,
            "acceptable_slippage": "0.005",
            "fill_sleeptime": 15,
            "portfolio_weights": [
                {
                    "base_asset": Asset(symbol='BTC-USD', asset_type='stock'),
                    "weight": Decimal("0.5")
                },
                {
                    "base_asset": Asset(symbol='ETH-USD', asset_type='stock'),
                    "weight": Decimal("0.5")
                }
            ],
            "shorting": False,
            "fractional_shares": True
        }

        end_date = datetime.now() - timedelta(days=1)
        start_date = end_date - timedelta(days=5)

        strat_obj: Strategy
        results, strat_obj = DriftRebalancer.run_backtest(
            datasource_class=YahooDataBacktesting,
            backtesting_start=start_date,
            backtesting_end=end_date,
            parameters=parameters,
            benchmark_asset=None,
            analyze_backtest=False,
            show_progress_bar=False,
        )

        trades_df = strat_obj.broker._trade_event_log_df

        # Get all the filled limit orders
        filled_orders = trades_df[(trades_df["status"] == "fill")]

        assert filled_orders.iloc[0]["type"] == "limit"
        assert filled_orders.iloc[0]["side"] == "buy"
        assert filled_orders.iloc[0]["symbol"] == "BTC-USD"
        assert filled_orders.iloc[1]["type"] == "limit"
        assert filled_orders.iloc[1]["side"] == "buy"
        assert filled_orders.iloc[1]["symbol"] == "ETH-USD"

    # @pytest.mark.skip()
    @pytest.mark.skipif(
        not POLYGON_CONFIG["API_KEY"],
        reason="This test requires a Polygon.io API key"
    )
    @pytest.mark.skipif(
        POLYGON_CONFIG['API_KEY'] == '<your key here>',
        reason="This test requires a Polygon.io API key"
    )
    def test_crypto_50_50_with_polygon(self):
        parameters = {
            "market": "24/7",
            "sleeptime": "1D",
            "drift_type": DriftType.ABSOLUTE,
            "drift_threshold": "0.03",
            "order_type": Order.OrderType.LIMIT,
            "acceptable_slippage": "0.005",
            "fill_sleeptime": 15,
            "portfolio_weights": [
                {
                    "base_asset": Asset(symbol='BTC', asset_type='crypto'),
                    "weight": Decimal("0.5")
                },
                {
                    "base_asset": Asset(symbol='ETH', asset_type='crypto'),
                    "weight": Decimal("0.5")
                }
            ],
            "shorting": False,
            "fractional_shares": True
        }

        # Expensive polygon subscriptions required if we go back to 2019. Just use recent dates.
        end_date = datetime.now() - timedelta(days=1)
        start_date = end_date - timedelta(days=5)

        strat_obj: Strategy
        results, strat_obj = DriftRebalancer.run_backtest(
            datasource_class=PolygonDataBacktesting,
            polygon_api_key=POLYGON_CONFIG["API_KEY"],
            backtesting_start=start_date,
            backtesting_end=end_date,
            parameters=parameters,
            benchmark_asset=None,
            analyze_backtest=False,
            show_progress_bar=False,
        )

        trades_df = strat_obj.broker._trade_event_log_df

        # Get all the filled limit orders
        filled_orders = trades_df[(trades_df["status"] == "fill")]

        assert filled_orders.iloc[0]["type"] == "limit"
        assert filled_orders.iloc[0]["side"] == "buy"
        assert filled_orders.iloc[0]["symbol"] == "BTC"
        assert filled_orders.iloc[1]["type"] == "limit"
        assert filled_orders.iloc[1]["side"] == "buy"
        assert filled_orders.iloc[1]["symbol"] == "ETH"

    @pytest.mark.skipif(
        not ALPACA_TEST_CONFIG['API_KEY'] or ALPACA_TEST_CONFIG['API_KEY'] == '<your key here>',
        reason="This test requires an alpaca API key"
    )
    def test_crypto_50_50_with_alpaca(
            self,
            market: str = 'NYSE',
            timestep: str = 'day',
            sleeptime: str = '1D',
            tzinfo: pytz.tzinfo = pytz.timezone('America/Chicago'),
            auto_adjust: bool = True,
            warm_up_trading_days: int = 0,
    ):
        backtesting_start = tzinfo.localize(datetime(2025, 1, 13))
        backtesting_end = tzinfo.localize(datetime(2025, 1, 17))
        refresh_cache = False
        parameters = {
            "market": "24/7",
            "sleeptime": sleeptime,
            "drift_type": DriftType.ABSOLUTE,
            "drift_threshold": "0.03",
            "order_type": Order.OrderType.LIMIT,
            "acceptable_slippage": "0.005",
            "fill_sleeptime": 15,
            "portfolio_weights": [
                {
                    "base_asset": Asset(symbol='BTC', asset_type='crypto'),
                    "weight": Decimal("0.5")
                },
                {
                    "base_asset": Asset(symbol='ETH', asset_type='crypto'),
                    "weight": Decimal("0.5")
                }
            ],
            "shorting": False,
            "fractional_shares": True
        }

        strat_obj: Strategy
        results, strat_obj = DriftRebalancer.run_backtest(
            datasource_class=AlpacaBacktesting,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            minutes_before_closing=0,
            benchmark_asset=None,
            analyze_backtest=False,
            show_progress_bar=False,
            parameters=parameters,

            # AlpacaBacktesting kwargs
            timestep=timestep,
            market=market,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
            warm_up_trading_days=warm_up_trading_days,
            auto_adjust=auto_adjust,
        )

        trades_df = strat_obj.broker._trade_event_log_df

        # Get all the filled limit orders
        filled_orders = trades_df[(trades_df["status"] == "fill")]

        assert filled_orders.iloc[0]["type"] == "limit"
        assert filled_orders.iloc[0]["side"] == "buy"
        assert filled_orders.iloc[0]["symbol"] == "BTC"
        assert filled_orders.iloc[1]["type"] == "limit"
        assert filled_orders.iloc[1]["side"] == "buy"
        assert filled_orders.iloc[1]["symbol"] == "ETH"

    @pytest.mark.skipif(
        not ALPACA_TEST_CONFIG['API_KEY'] or ALPACA_TEST_CONFIG['API_KEY'] == '<your key here>',
        reason="This test requires an alpaca API key"
    )
    def test_crypto_50_50_with_alpaca_modern_way(
            self,
            market: str = '24/7',
            timestep: str = 'day',
            sleeptime: str = '1D',
            tzinfo: pytz.tzinfo = pytz.timezone('America/Chicago'),
            auto_adjust: bool = True,
            warm_up_trading_days: int = 0,
    ):
        backtesting_start = tzinfo.localize(datetime(2025, 1, 13))
        backtesting_end = tzinfo.localize(datetime(2025, 1, 17))
        refresh_cache = False
        parameters = {
            "market": market,
            "sleeptime": sleeptime,
            "drift_type": DriftType.ABSOLUTE,
            "drift_threshold": "0.03",
            "order_type": Order.OrderType.LIMIT,
            "acceptable_slippage": "0.005",
            "fill_sleeptime": 15,
            "portfolio_weights": [
                {
                    "base_asset": Asset(symbol='BTC', asset_type='crypto'),
                    "weight": Decimal("0.5")
                },
                {
                    "base_asset": Asset(symbol='ETH', asset_type='crypto'),
                    "weight": Decimal("0.5")
                }
            ],
            "shorting": False,
            "fractional_shares": True
        }

        data_source = AlpacaBacktesting(
            datetime_start=backtesting_start,
            datetime_end=backtesting_end,
            show_progress_bar=False,
            parameters=parameters,

            timestep=timestep,
            market=market,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
            warm_up_trading_days=warm_up_trading_days,
            auto_adjust=auto_adjust,
        )
        broker = BacktestingBroker(data_source=data_source)
        strat_obj = DriftRebalancer(
            broker=broker,
            parameters=parameters,
            minutes_before_closing=0,
            benchmark_asset=None,
            analyze_backtest=False,
        )
        trader = Trader(logfile="", backtest=True)
        trader.add_strategy(strat_obj)
        results = trader.run_all(show_plot=False, show_tearsheet=False, save_tearsheet=True)
        assert results

        trades_df = strat_obj.broker._trade_event_log_df

        # Get all the filled limit orders
        filled_orders = trades_df[(trades_df["status"] == "fill")]

        assert filled_orders.iloc[0]["type"] == "limit"
        assert filled_orders.iloc[0]["side"] == "buy"
        assert filled_orders.iloc[0]["symbol"] == "BTC"
        assert filled_orders.iloc[1]["type"] == "limit"
        assert filled_orders.iloc[1]["side"] == "buy"
        assert filled_orders.iloc[1]["symbol"] == "ETH"

        assert strat_obj.stats['portfolio_value'][-1] == 105021.76805867575

    @pytest.mark.skipif(
        not ALPACA_TEST_CONFIG['API_KEY'] or ALPACA_TEST_CONFIG['API_KEY'] == '<your key here>',
        reason="This test requires an alpaca API key"
    )
    def test_crypto_50_50_with_alpaca_with_fees(
            self,
            market: str = '24/7',
            timestep: str = 'day',
            sleeptime: str = '1D',
            tzinfo: pytz.tzinfo = pytz.timezone('America/Chicago'),
            auto_adjust: bool = True,
            warm_up_trading_days: int = 0,
    ):
        backtesting_start = tzinfo.localize(datetime(2025, 1, 13))
        backtesting_end = tzinfo.localize(datetime(2025, 1, 17))
        trading_fee = TradingFee(percent_fee=0.0025)
        refresh_cache = False
        parameters = {
            "market": market,
            "sleeptime": sleeptime,
            "drift_type": DriftType.ABSOLUTE,
            "drift_threshold": "0.03",
            "order_type": Order.OrderType.LIMIT,
            "acceptable_slippage": "0.005",
            "fill_sleeptime": 15,
            "portfolio_weights": [
                {
                    "base_asset": Asset(symbol='BTC', asset_type='crypto'),
                    "weight": Decimal("0.5")
                },
                {
                    "base_asset": Asset(symbol='ETH', asset_type='crypto'),
                    "weight": Decimal("0.5")
                }
            ],
            "shorting": False,
            "fractional_shares": True
        }

        data_source = AlpacaBacktesting(
            datetime_start=backtesting_start,
            datetime_end=backtesting_end,
            show_progress_bar=False,
            parameters=parameters,

            timestep=timestep,
            market=market,
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
            warm_up_trading_days=warm_up_trading_days,
            auto_adjust=auto_adjust,
        )
        broker = BacktestingBroker(data_source=data_source)
        strat_obj = DriftRebalancer(
            broker=broker,
            parameters=parameters,
            minutes_before_closing=0,
            benchmark_asset=None,
            analyze_backtest=False,
            buy_trading_fees=[trading_fee],
            sell_trading_fees=[trading_fee],
        )
        trader = Trader(logfile="", backtest=True)
        trader.add_strategy(strat_obj)
        results = trader.run_all(show_plot=False, show_tearsheet=False, save_tearsheet=True)
        assert results

        trades_df = strat_obj.broker._trade_event_log_df
        assert trades_df.iloc[-1]["trade_cost"] >= 0.0

        # Get all the filled limit orders
        filled_orders = trades_df[(trades_df["status"] == "fill")]

        assert filled_orders.iloc[0]["type"] == "limit"
        assert filled_orders.iloc[0]["side"] == "buy"
        assert filled_orders.iloc[0]["symbol"] == "BTC"
        assert filled_orders.iloc[1]["type"] == "limit"
        assert filled_orders.iloc[1]["side"] == "buy"
        assert filled_orders.iloc[1]["symbol"] == "ETH"

        assert strat_obj.stats['portfolio_value'][-1] == 104767.7476530826

    @patch("lumibot.strategies.Strategy")
    def test_get_last_price_or_raise_returns_decimal(self, MockStrategy):
        mock_strategy = MockStrategy()
        mock_strategy.get_last_price.return_value = 123.45

        asset = Asset(symbol="AAPL")
        quote = Asset(symbol="USD", asset_type=Asset.AssetType.FOREX)
        price = get_last_price_or_raise(mock_strategy, asset, quote)

        assert price == Decimal("123.45")

    @patch("lumibot.strategies.Strategy")
    def test_get_last_price_or_raise_raises_value_error_on_none(self, MockStrategy):
        mock_strategy = MockStrategy()
        mock_strategy.get_last_price.return_value = None

        asset = Asset(symbol="AAPL")
        quote = Asset(symbol="USD", asset_type=Asset.AssetType.FOREX)

        with pytest.raises(ValueError, match="DriftRebalancer could not get_last_price for AAPL-USD."):
            get_last_price_or_raise(mock_strategy, asset, quote)

