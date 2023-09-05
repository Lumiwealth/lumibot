import datetime
import os

from lumibot.backtesting import YahooDataBacktesting
from lumibot.example_strategies.stock_buy_and_hold import BuyAndHold
from lumibot.example_strategies.stock_diversified_leverage import \
    DiversifiedLeverage
from lumibot.example_strategies.stock_limit_and_trailing_stops import \
    LimitAndTrailingStop

# Global parameters
# API Key for testing Polygon.io
POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY")


class TestExampleStrategies:
    def test_stock_buy_and_hold(self):
        """
        Test the example strategy BuyAndHold by running a backtest and checking that the strategy object is returned
        along with the correct results
        """

        # Parameters
        backtesting_start = datetime.datetime(2023, 7, 10)
        backtesting_end = datetime.datetime(2023, 7, 13)

        # Execute Backtest
        results, strat_obj = BuyAndHold.run_backtest(
            YahooDataBacktesting,
            backtesting_start,
            backtesting_end,
            benchmark_asset="SPY",
            show_plot=False,
            show_tearsheet=False,
            save_tearsheet=False,
        )
        assert results
        assert isinstance(strat_obj, BuyAndHold)

        # Check that the results are correct
        assert round(results["cagr"] * 100, 1) == 155.7
        assert round(results["volatility"] * 100, 1) == 7.0
        assert round(results["sharpe"], 2) == 21.60
        assert round(results["total_return"] * 100, 1) == 0.5
        assert round(results["max_drawdown"]["drawdown"] * 100, 1) == 0.0

    def test_stock_diversified_leverage(self):
        """
        Test the example strategy DiversifiedLeverage by running a backtest and checking that the strategy object is
        returned along with the correct results.
        """

        # Parameters
        backtesting_start = datetime.datetime(2023, 7, 10)
        backtesting_end = datetime.datetime(2023, 7, 13)

        # Execute Backtest
        results, strat_obj = DiversifiedLeverage.run_backtest(
            YahooDataBacktesting,
            backtesting_start,
            backtesting_end,
            benchmark_asset="SPY",
            show_plot=False,
            show_tearsheet=False,
            save_tearsheet=False,
        )
        assert results
        assert isinstance(strat_obj, DiversifiedLeverage)

        # Check that the results are correct
        assert round(results["cagr"] * 100, 1) == 2907.9
        assert round(results["volatility"] * 100, 0) == 25
        assert round(results["sharpe"], 2) == 114.17
        assert round(results["total_return"] * 100, 1) == 1.9
        assert round(results["max_drawdown"]["drawdown"] * 100, 1) == 0.0

    def test_limit_and_trailing_stops(self):
        """
        Test the example strategy LimitAndTrailingStop by running a backtest and checking that the strategy object is
        returned along with the correct results.
        """

        # Parameters
        backtesting_start = datetime.datetime(2023, 3, 3)
        backtesting_end = datetime.datetime(2023, 3, 10)

        # Execute Backtest
        results, strat_obj = LimitAndTrailingStop.run_backtest(
            YahooDataBacktesting,
            backtesting_start,
            backtesting_end,
            benchmark_asset="SPY",
            show_plot=False,
            show_tearsheet=False,
            save_tearsheet=False,
        )

        assert results
        assert isinstance(strat_obj, LimitAndTrailingStop)

        trades_df = strat_obj.broker._trade_event_log_df

        # Get all the filled limit orders
        filled_limit_orders = trades_df[(trades_df["status"] == "fill") & (trades_df["type"] == "limit")]

        # The first limit order should have filled at $399.71 and a quantity of 100
        assert round(filled_limit_orders.iloc[0]["price"], 2) == 399.71
        assert filled_limit_orders.iloc[0]["filled_quantity"] == 100

        # The second limit order should have filled at $399.74 and a quantity of 100
        assert round(filled_limit_orders.iloc[1]["price"], 2) == 407
        assert filled_limit_orders.iloc[1]["filled_quantity"] == 100

        # Get all the filled trailing stop orders
        filled_trailing_stop_orders = trades_df[(trades_df["status"] == "fill")
                                                & (trades_df["type"] == "trailing_stop")]

        # The first order should have filled at $399.30 and a quantity of 100
        assert round(filled_trailing_stop_orders.iloc[0]["price"], 2) == 399.30
        assert filled_trailing_stop_orders.iloc[0]["filled_quantity"] == 100

        # The second order should have filled at $400.45 and a quantity of 50
        assert round(filled_trailing_stop_orders.iloc[1]["price"], 2) == 400.45
        assert filled_trailing_stop_orders.iloc[1]["filled_quantity"] == 50

        # Check that the results are correct
        assert round(results["cagr"] * 100, 1) == 75
        assert round(results["volatility"] * 100, 1) == 11.3
        assert round(results["sharpe"], 2) == 6.17
        assert round(results["total_return"] * 100, 1) == 0.9
        assert round(results["max_drawdown"]["drawdown"] * 100, 1) == 0.7
