from decimal import Decimal
from typing import Any
import datetime
import logging

import pandas as pd
import numpy as np

from lumibot.example_strategies.drift_rebalancer import DriftCalculationLogic, LimitOrderRebalanceLogic, DriftRebalancer
from lumibot.backtesting import BacktestingBroker, YahooDataBacktesting, PandasDataBacktesting
from lumibot.strategies.strategy import Strategy
from tests.fixtures import pandas_data_fixture
from lumibot.tools import print_full_pandas_dataframes, set_pandas_float_precision

print_full_pandas_dataframes()
set_pandas_float_precision(precision=5)


# @pytest.mark.skip()
class TestDriftCalculationLogic:

    def test_add_position(self):
        target_weights = {
            "AAPL": Decimal("0.5"),
            "GOOGL": Decimal("0.3"),
            "MSFT": Decimal("0.2")
        }
        self.calculator = DriftCalculationLogic(target_weights=target_weights)
        self.calculator.add_position(
            symbol="AAPL",
            is_quote_asset=False,
            current_quantity=Decimal("10"),
            current_value=Decimal("1500")
        )
        self.calculator.add_position(
            symbol="GOOGL",
            is_quote_asset=False,
            current_quantity=Decimal("5"),
            current_value=Decimal("1000")
        )
        self.calculator.add_position(
            symbol="MSFT",
            is_quote_asset=False,
            current_quantity=Decimal("8"),
            current_value=Decimal("800")
        )

        df = self.calculator.df

        assert df["symbol"].tolist() == ["AAPL", "GOOGL", "MSFT"]
        assert df["current_quantity"].tolist() == [Decimal("10"), Decimal("5"), Decimal("8")]
        assert df["current_value"].tolist() == [Decimal("1500"), Decimal("1000"), Decimal("800")]

    def test_calculate_drift(self):
        target_weights = {
            "AAPL": Decimal("0.5"),
            "GOOGL": Decimal("0.3"),
            "MSFT": Decimal("0.2")
        }
        self.calculator = DriftCalculationLogic(target_weights=target_weights)
        self.calculator.add_position(
            symbol="AAPL",
            is_quote_asset=False,
            current_quantity=Decimal("10"),
            current_value=Decimal("1500")
        )
        self.calculator.add_position(
            symbol="GOOGL",
            is_quote_asset=False,
            current_quantity=Decimal("5"),
            current_value=Decimal("1000")
        )
        self.calculator.add_position(
            symbol="MSFT",
            is_quote_asset=False,
            current_quantity=Decimal("8"),
            current_value=Decimal("800")
        )

        df = self.calculator.calculate()
        # print(f"\n{df}")

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

        assert df["drift"].tolist() == [
            Decimal('0.0454545454545454545454545455'),
            Decimal('-0.0030303030303030303030303030'),
            Decimal('-0.0424242424242424242424242424')
        ]

    def test_drift_is_negative_one_when_we_have_a_position_and_the_target_weights_says_to_not_have_it(self):
        target_weights = {
            "AAPL": Decimal("0.5"),
            "GOOGL": Decimal("0.3"),
            "MSFT": Decimal("0.0")
        }
        self.calculator = DriftCalculationLogic(target_weights=target_weights)
        self.calculator.add_position(
            symbol="AAPL",
            is_quote_asset=False,
            current_quantity=Decimal("10"),
            current_value=Decimal("1500")
        )
        self.calculator.add_position(
            symbol="GOOGL",
            is_quote_asset=False,
            current_quantity=Decimal("5"),
            current_value=Decimal("1000")
        )
        self.calculator.add_position(
            symbol="MSFT",
            is_quote_asset=False,
            current_quantity=Decimal("8"),
            current_value=Decimal("800")
        )

        df = self.calculator.calculate()
        # print(f"\n{df}")

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

    def test_drift_is_one_when_we_have_none_of_an_asset_and_target_weights_says_we_should_have_some(self):
        target_weights = {
            "AAPL": Decimal("0.25"),
            "GOOGL": Decimal("0.25"),
            "MSFT": Decimal("0.25"),
            "AMZN": Decimal("0.25")
        }
        self.calculator = DriftCalculationLogic(target_weights=target_weights)
        self.calculator.add_position(
            symbol="AAPL",
            is_quote_asset=False,
            current_quantity=Decimal("10"),
            current_value=Decimal("1500")
        )
        self.calculator.add_position(
            symbol="GOOGL",
            is_quote_asset=False,
            current_quantity=Decimal("5"),
            current_value=Decimal("1000")
        )
        self.calculator.add_position(
            symbol="MSFT",
            is_quote_asset=False,
            current_quantity=Decimal("8"),
            current_value=Decimal("800")
        )

        df = self.calculator.calculate()
        # print(f"\n{df}")

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

    def test_calculate_drift_when_quote_asset_position_exists(self):
        target_weights = {
            "AAPL": Decimal("0.5"),
            "GOOGL": Decimal("0.3"),
            "MSFT": Decimal("0.2")
        }
        self.calculator = DriftCalculationLogic(target_weights=target_weights)
        self.calculator.add_position(
            symbol="USD",
            is_quote_asset=True,
            current_quantity=Decimal("1000"),
            current_value=Decimal("1000")
        )
        self.calculator.add_position(
            symbol="AAPL",
            is_quote_asset=False,
            current_quantity=Decimal("10"),
            current_value=Decimal("1500")
        )
        self.calculator.add_position(
            symbol="GOOGL",
            is_quote_asset=False,
            current_quantity=Decimal("5"),
            current_value=Decimal("1000")
        )
        self.calculator.add_position(
            symbol="MSFT",
            is_quote_asset=False,
            current_quantity=Decimal("8"),
            current_value=Decimal("800")
        )

        df = self.calculator.calculate()
        # print(f"\n{df}")

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

    def test_calculate_drift_when_quote_asset_in_target_weights(self):
        target_weights = {
            "AAPL": Decimal("0.25"),
            "GOOGL": Decimal("0.25"),
            "USD": Decimal("0.50")
        }
        self.calculator = DriftCalculationLogic(target_weights=target_weights)
        self.calculator.add_position(
            symbol="USD",
            is_quote_asset=True,
            current_quantity=Decimal("0"),
            current_value=Decimal("0")
        )
        self.calculator.add_position(
            symbol="AAPL",
            is_quote_asset=False,
            current_quantity=Decimal("5"),
            current_value=Decimal("500")
        )
        self.calculator.add_position(
            symbol="GOOGL",
            is_quote_asset=False,
            current_quantity=Decimal("10"),
            current_value=Decimal("500")
        )

        df = self.calculator.calculate()
        # print(f"\n{df}")

        assert df["current_weight"].tolist() == [Decimal("0.5"), Decimal("0.5"), Decimal("0.0")]
        assert df["target_value"].tolist() == [Decimal("250"), Decimal("250"), Decimal("500")]
        assert df["drift"].tolist() == [Decimal("-0.25"), Decimal("-0.25"), Decimal("0")]

    def test_calculate_drift_when_we_want_short_something(self):
        target_weights = {
            "AAPL": Decimal("-0.50"),
            "USD": Decimal("0.50")
        }
        self.calculator = DriftCalculationLogic(target_weights=target_weights)
        self.calculator.add_position(
            symbol="USD",
            is_quote_asset=True,
            current_quantity=Decimal("1000"),
            current_value=Decimal("1000")
        )
        self.calculator.add_position(
            symbol="AAPL",
            is_quote_asset=False,
            current_quantity=Decimal("0"),
            current_value=Decimal("0")
        )

        df = self.calculator.calculate()
        # print(f"\n{df}")

        assert df["current_weight"].tolist() == [Decimal("0.0"), Decimal("1.0")]
        assert df["target_value"].tolist() == [Decimal("-500"), Decimal("500")]
        assert df["drift"].tolist() == [Decimal("-0.50"), Decimal("0")]

    def test_calculate_drift_when_we_want_a_100_percent_short_position(self):
        target_weights = {
            "AAPL": Decimal("-1.0"),
            "USD": Decimal("0.0")
        }
        self.calculator = DriftCalculationLogic(target_weights=target_weights)
        self.calculator.add_position(
            symbol="USD",
            is_quote_asset=True,
            current_quantity=Decimal("1000"),
            current_value=Decimal("1000")
        )
        self.calculator.add_position(
            symbol="AAPL",
            is_quote_asset=False,
            current_quantity=Decimal("0"),
            current_value=Decimal("0")
        )

        df = self.calculator.calculate()

        assert df["current_weight"].tolist() == [Decimal("0.0"), Decimal("1.0")]
        assert df["target_value"].tolist() == [Decimal("-1000"), Decimal("0")]
        assert df["drift"].tolist() == [Decimal("-1.0"), Decimal("0")]


class MockStrategy(Strategy):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.orders = []

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


class TestLimitOrderRebalance:

    def setup_method(self):
        date_start = datetime.datetime(2021, 7, 10)
        date_end = datetime.datetime(2021, 7, 13)
        self.data_source = YahooDataBacktesting(date_start, date_end)
        self.backtesting_broker = BacktestingBroker(self.data_source)

    def test_selling_everything(self):
        strategy = MockStrategy(broker=self.backtesting_broker)
        df = pd.DataFrame({
            "symbol": ["AAPL"],
            "current_quantity": [Decimal("10")],
            "current_value": [Decimal("1000")],
            "target_value": [Decimal("0")],
            "drift": [Decimal("-1")]
        })
        executor = LimitOrderRebalanceLogic(strategy=strategy, df=df)
        executor.rebalance()
        assert len(strategy.orders) == 1
        assert strategy.orders[0].side == "sell"
        assert strategy.orders[0].quantity == Decimal("10")

    def test_selling_part_of_a_holding(self):
        strategy = MockStrategy(broker=self.backtesting_broker)
        df = pd.DataFrame({
            "symbol": ["AAPL"],
            "current_quantity": [Decimal("10")],
            "current_value": [Decimal("1000")],
            "target_value": [Decimal("500")],
            "drift": [Decimal("-0.5")]
        })
        executor = LimitOrderRebalanceLogic(strategy=strategy, df=df)
        executor.rebalance()
        assert len(strategy.orders) == 1
        assert strategy.orders[0].side == "sell"
        assert strategy.orders[0].quantity == Decimal("5")

    def test_selling_short_doesnt_create_and_order_when_shorting_is_disabled(self):
        strategy = MockStrategy(broker=self.backtesting_broker)
        df = pd.DataFrame({
            "symbol": ["AAPL"],
            "current_quantity": [Decimal("0")],
            "current_value": [Decimal("0")],
            "target_value": [Decimal("-1000")],
            "drift": [Decimal("-1")]
        })
        executor = LimitOrderRebalanceLogic(strategy=strategy, df=df)
        executor.rebalance()
        assert len(strategy.orders) == 0

    def test_selling_small_short_position_creates_and_order_when_shorting_is_enabled(self):
        strategy = MockStrategy(broker=self.backtesting_broker)
        df = pd.DataFrame({
            "symbol": ["AAPL"],
            "current_quantity": [Decimal("0")],
            "current_value": [Decimal("0")],
            "target_value": [Decimal("-1000")],
            "drift": [Decimal("-0.25")]
        })
        executor = LimitOrderRebalanceLogic(strategy=strategy, df=df, shorting=True)
        executor.rebalance()
        assert len(strategy.orders) == 1

    def test_selling_small_short_position_doesnt_creatne_order_when_shorting_is_disabled(self):
        strategy = MockStrategy(broker=self.backtesting_broker)
        df = pd.DataFrame({
            "symbol": ["AAPL"],
            "current_quantity": [Decimal("0")],
            "current_value": [Decimal("0")],
            "target_value": [Decimal("-1000")],
            "drift": [Decimal("-0.25")]
        })
        executor = LimitOrderRebalanceLogic(strategy=strategy, df=df, shorting=False)
        executor.rebalance()
        assert len(strategy.orders) == 0

    def test_selling_a_100_percent_short_position_creates_and_order_when_shorting_is_enabled(self):
        strategy = MockStrategy(broker=self.backtesting_broker)
        df = pd.DataFrame({
            "symbol": ["AAPL"],
            "current_quantity": [Decimal("0")],
            "current_value": [Decimal("0")],
            "target_value": [Decimal("-1000")],
            "drift": [Decimal("-1")]
        })
        executor = LimitOrderRebalanceLogic(strategy=strategy, df=df, shorting=True)
        executor.rebalance()
        assert len(strategy.orders) == 1

    def test_buying_something_when_we_have_enough_money_and_there_is_slippage(self):
        strategy = MockStrategy(broker=self.backtesting_broker)
        df = pd.DataFrame({
            "symbol": ["AAPL"],
            "current_quantity": [Decimal("0")],
            "current_value": [Decimal("0")],
            "target_value": [Decimal("1000")],
            "drift": [Decimal("1")]
        })
        executor = LimitOrderRebalanceLogic(strategy=strategy, df=df)
        executor.rebalance()
        assert len(strategy.orders) == 1
        assert strategy.orders[0].side == "buy"
        assert strategy.orders[0].quantity == Decimal("9")

    def test_buying_something_when_we_dont_have_enough_money_for_everything(self):
        strategy = MockStrategy(broker=self.backtesting_broker)
        strategy._set_cash_position(cash=500.0)
        # mock the update_broker_balances method
        df = pd.DataFrame({
            "symbol": ["AAPL"],
            "current_quantity": [Decimal("0")],
            "current_value": [Decimal("0")],
            "target_value": [Decimal("1000")],
            "drift": [Decimal("1")]
        })
        executor = LimitOrderRebalanceLogic(strategy=strategy, df=df)
        executor.rebalance()
        assert len(strategy.orders) == 1
        assert strategy.orders[0].side == "buy"
        assert strategy.orders[0].quantity == Decimal("4")

    def test_attempting_to_buy_when_we_dont_have_enough_money_for_even_one_share(self):
        strategy = MockStrategy(broker=self.backtesting_broker)
        strategy._set_cash_position(cash=50.0)
        df = pd.DataFrame({
            "symbol": ["AAPL"],
            "current_quantity": [Decimal("0")],
            "current_value": [Decimal("0")],
            "target_value": [Decimal("1000")],
            "drift": [Decimal("1")]
        })
        executor = LimitOrderRebalanceLogic(strategy=strategy, df=df)
        executor.rebalance()
        assert len(strategy.orders) == 0

    def test_attempting_to_sell_when_the_amount_we_need_to_sell_is_less_than_the_limit_price_should_not_sell(self):
        strategy = MockStrategy(broker=self.backtesting_broker)
        df = pd.DataFrame({
            "symbol": ["AAPL"],
            "current_quantity": [Decimal("1")],
            "current_value": [Decimal("100")],
            "target_value": [Decimal("10")],
            "drift": [Decimal("-0.5")]
        })
        executor = LimitOrderRebalanceLogic(strategy=strategy, df=df)
        executor.rebalance()
        assert len(strategy.orders) == 0

    def test_calculate_limit_price_when_selling(self):
        strategy = MockStrategy(broker=self.backtesting_broker)
        df = pd.DataFrame({
            "symbol": ["AAPL"],
            "current_quantity": [Decimal("10")],
            "current_value": [Decimal("1000")],
            "target_value": [Decimal("0")],
            "drift": [Decimal("-1")]
        })
        executor = LimitOrderRebalanceLogic(strategy=strategy, df=df, acceptable_slippage=Decimal("0.005"))
        limit_price = executor.calculate_limit_price(last_price=Decimal("120.00"), side="sell")
        assert limit_price == Decimal("119.4")

    def test_calculate_limit_price_when_buying(self):
        strategy = MockStrategy(broker=self.backtesting_broker)
        df = pd.DataFrame({
            "symbol": ["AAPL"],
            "current_quantity": [Decimal("10")],
            "current_value": [Decimal("1000")],
            "target_value": [Decimal("0")],
            "drift": [Decimal("-1")]
        })
        executor = LimitOrderRebalanceLogic(strategy=strategy, df=df, acceptable_slippage=Decimal("0.005"))
        limit_price = executor.calculate_limit_price(last_price=Decimal("120.00"), side="buy")
        assert limit_price == Decimal("120.6")


# @pytest.mark.skip()
class TestDriftRebalancer:

    # Need to start two days after the first data point in pandas for backtesting
    backtesting_start = datetime.datetime(2019, 1, 2)
    backtesting_end = datetime.datetime(2019, 12, 31)

    def test_classic_60_60(self, pandas_data_fixture):

        parameters = {
            "market": "NYSE",
            "sleeptime": "1D",
            "drift_threshold": "0.03",
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
