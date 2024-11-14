from decimal import Decimal
import datetime

import pandas as pd

from lumibot.components import DriftCalculationLogic
from lumibot.backtesting import BacktestingBroker, PandasDataBacktesting
from lumibot.strategies.strategy import Strategy
from lumibot.tools import print_full_pandas_dataframes, set_pandas_float_precision

print_full_pandas_dataframes()
set_pandas_float_precision(precision=5)


class MockStrategy(Strategy):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.drift_calculation_logic = DriftCalculationLogic(self)


class TestDriftCalculationLogic:

    def setup_method(self):
        date_start = datetime.datetime(2021, 7, 10)
        date_end = datetime.datetime(2021, 7, 13)
        self.data_source = PandasDataBacktesting(date_start, date_end)
        self.backtesting_broker = BacktestingBroker(self.data_source)

    def test_add_position(self, mocker):
        strategy = MockStrategy(broker=self.backtesting_broker)
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
        df = strategy.drift_calculation_logic.calculate(target_weights=target_weights)
        assert df["symbol"].tolist() == ["AAPL", "GOOGL", "MSFT"]
        assert df["current_quantity"].tolist() == [Decimal("10"), Decimal("5"), Decimal("8")]
        assert df["current_value"].tolist() == [Decimal("1500"), Decimal("1000"), Decimal("800")]

    def test_calculate_drift(self, mocker):
        strategy = MockStrategy(broker=self.backtesting_broker)
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
        df = strategy.drift_calculation_logic.calculate(target_weights=target_weights)

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

    def test_drift_is_negative_one_when_we_have_a_position_and_the_target_weights_says_to_not_have_it(self, mocker):
        strategy = MockStrategy(broker=self.backtesting_broker)
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
        df = strategy.drift_calculation_logic.calculate(target_weights=target_weights)

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
        strategy = MockStrategy(broker=self.backtesting_broker)
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
        df = strategy.drift_calculation_logic.calculate(target_weights=target_weights)

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

    def test_calculate_drift_when_quote_asset_position_exists(self, mocker):
        strategy = MockStrategy(broker=self.backtesting_broker)
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
        df = strategy.drift_calculation_logic.calculate(target_weights=target_weights)

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

    def test_calculate_drift_when_quote_asset_in_target_weights(self, mocker):
        strategy = MockStrategy(broker=self.backtesting_broker)
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
        df = strategy.drift_calculation_logic.calculate(target_weights=target_weights)

        assert df["current_weight"].tolist() == [Decimal("0.5"), Decimal("0.5"), Decimal("0.0")]
        assert df["target_value"].tolist() == [Decimal("250"), Decimal("250"), Decimal("500")]
        assert df["drift"].tolist() == [Decimal("-0.25"), Decimal("-0.25"), Decimal("0")]

    def test_calculate_drift_when_we_want_short_something(self, mocker):
        strategy = MockStrategy(broker=self.backtesting_broker)
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
        df = strategy.drift_calculation_logic.calculate(target_weights=target_weights)

        assert df["current_weight"].tolist() == [Decimal("0.0"), Decimal("1.0")]
        assert df["target_value"].tolist() == [Decimal("-500"), Decimal("500")]
        assert df["drift"].tolist() == [Decimal("-0.50"), Decimal("0")]

    def test_calculate_drift_when_we_want_a_100_percent_short_position(self, mocker):
        strategy = MockStrategy(broker=self.backtesting_broker)
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
        df = strategy.drift_calculation_logic.calculate(target_weights=target_weights)

        assert df["current_weight"].tolist() == [Decimal("0.5"), Decimal("0.5"), Decimal("0.0")]
        assert df["target_value"].tolist() == [Decimal("250"), Decimal("250"), Decimal("500")]
        assert df["drift"].tolist() == [Decimal("-0.25"), Decimal("-0.25"), Decimal("0")]

    def test_calculate_drift_when_we_want_short_something_else(self, mocker):
        strategy = MockStrategy(broker=self.backtesting_broker)
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
        df = strategy.drift_calculation_logic.calculate(target_weights=target_weights)

        assert df["current_weight"].tolist() == [Decimal("0.0"), Decimal("1.0")]
        assert df["target_value"].tolist() == [Decimal("-1000"), Decimal("0")]
        assert df["drift"].tolist() == [Decimal("-1.0"), Decimal("0")]

