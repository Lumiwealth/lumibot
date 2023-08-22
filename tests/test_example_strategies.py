import datetime
import os

from lumibot.backtesting import YahooDataBacktesting
from lumibot.example_strategies.buy_and_hold import BuyAndHold

# Global parameters
# API Key for testing Polygon.io
POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY")


class TestExampleStrategies:
    def test_buy_and_hold(self):
        """
        Test the example strategy BuyAndHold by running a backtest and checking that the strategy object is returned
        along with the correct results
        """

        # Parameters: True = Live Trading | False = Backtest
        # trade_live = False
        backtesting_start = datetime.datetime(2023, 7, 10)
        backtesting_end = datetime.datetime(2023, 7, 13)

        # Execute Backtest | Polygon.io API Connection
        results, poly_strat_obj = BuyAndHold.run_backtest(
            YahooDataBacktesting,
            backtesting_start,
            backtesting_end,
            benchmark_asset="SPY",
            show_plot=False,
            show_tearsheet=False,
            save_tearsheet=False,
        )
        assert results
        assert isinstance(poly_strat_obj, BuyAndHold)

        # Check that the results are correct
        assert round(results["cagr"], 3) * 100 == 38.7
        assert round(results["volatility"], 3) * 100 == 11.5
        assert round(results["sharpe"], 3) == 2.90
        assert round(results["total_return"], 3) * 100 == 0.2
        assert round(results["max_drawdown"]["drawdown"], 3) == 0.0

    # def test_polygon_legacy_backtest(self):
    #     """Test that the legacy backtest() function call works without returning the startegy object"""
    #     # Parameters: True = Live Trading | False = Backtest
    #     # trade_live = False
    #     backtesting_start = datetime.datetime(2023, 8, 1)
    #     backtesting_end = datetime.datetime(2023, 8, 4)

    #     # Execute Backtest | Polygon.io API Connection
    #     results = PolygonBacktestStrat.backtest(
    #         PolygonDataBacktesting,
    #         backtesting_start,
    #         backtesting_end,
    #         benchmark_asset="SPY",
    #         show_plot=False,
    #         show_tearsheet=False,
    #         save_tearsheet=False,
    #         polygon_api_key=POLYGON_API_KEY,  # TODO Replace with Lumibot owned API Key
    #         # Painfully slow with free subscription setting b/c lumibot is over querying and imposing a very
    #         # strict rate limit
    #         polygon_has_paid_subscription=True,
    #     )
    #     assert results
