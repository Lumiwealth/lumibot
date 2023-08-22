import datetime
import os

from lumibot.backtesting import YahooDataBacktesting
from lumibot.example_strategies.stock_buy_and_hold import BuyAndHold
from lumibot.example_strategies.stock_diversified_leverage import \
    DiversifiedLeverage

# Global parameters
# API Key for testing Polygon.io
POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY")


class TestExampleStrategies:
    def test_stock_buy_and_hold(self):
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
        assert round(results["cagr"] * 100, 1) == 38.7
        assert round(results["volatility"] * 100, 1) == 11.5
        assert round(results["sharpe"], 3) == 2.90
        assert round(results["total_return"] * 100, 1) == 0.2
        assert round(results["max_drawdown"]["drawdown"] * 100, 1) == 0.0

    def test_stock_diversified_leverage(self):
        """
        Test the example strategy DiversifiedLeverage by running a backtest and checking that the strategy object is
        returned along with the correct results.
        """

        # Parameters: True = Live Trading | False = Backtest
        # trade_live = False
        backtesting_start = datetime.datetime(2023, 7, 10)
        backtesting_end = datetime.datetime(2023, 7, 13)

        # Execute Backtest | Polygon.io API Connection
        results, poly_strat_obj = DiversifiedLeverage.run_backtest(
            YahooDataBacktesting,
            backtesting_start,
            backtesting_end,
            benchmark_asset="SPY",
            show_plot=False,
            show_tearsheet=False,
            save_tearsheet=False,
        )
        assert results
        assert isinstance(poly_strat_obj, DiversifiedLeverage)

        # Check that the results are correct
        assert round(results["cagr"] * 100, 1) == 289.7
        assert round(results["volatility"] * 100, 0) == 41.0
        assert round(results["sharpe"], 3) == 6.932
        assert round(results["total_return"] * 100, 1) == 0.7
        assert round(results["max_drawdown"]["drawdown"] * 100, 1) == 0.0
