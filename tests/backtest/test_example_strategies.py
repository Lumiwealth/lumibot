import datetime
import os

import pytest

from lumibot.backtesting import PolygonDataBacktesting, YahooDataBacktesting, CcxtBacktesting
from lumibot.example_strategies.options_hold_to_expiry import OptionsHoldToExpiry
from lumibot.example_strategies.stock_bracket import StockBracket
from lumibot.example_strategies.stock_buy_and_hold import BuyAndHold
from lumibot.example_strategies.stock_diversified_leverage import DiversifiedLeverage
from lumibot.example_strategies.stock_limit_and_trailing_stops import (
    LimitAndTrailingStop,
)
from lumibot.example_strategies.stock_oco import StockOco
from lumibot.example_strategies.ccxt_backtesting_example import CcxtBacktestingExampleStrategy
from lumibot.entities import Asset, Order

# Global parameters
# API Key for testing Polygon.io
from lumibot.credentials import POLYGON_CONFIG

class TestExampleStrategies:

    @pytest.mark.xfail(reason="yahoo sucks")
    def test_stock_bracket(self):
        """
        Test the example strategy StockBracket by running a backtest and checking that the strategy object is returned
        along with the correct results
        """

        # Parameters
        backtesting_start = datetime.datetime(2023, 3, 3)
        backtesting_end = datetime.datetime(2023, 3, 10)

        # Execute Backtest
        results, strat_obj = StockBracket.run_backtest(
            YahooDataBacktesting,
            backtesting_start,
            backtesting_end,
            benchmark_asset=None,
            show_plot=False,
            show_tearsheet=False,
            save_tearsheet=False,
            show_indicators=False,
        )
        assert results
        assert isinstance(strat_obj, StockBracket)
        assert strat_obj.submitted_bracket_order is not None

        trades_df = strat_obj.broker._trade_event_log_df

        # Get all the filled limit orders
        filled_orders = trades_df[(trades_df["status"] == "fill")]

        # Check that the second order was a lmit order with a price of $405 or more and a quantity of 10
        assert filled_orders.iloc[1]["type"] == "limit"
        assert filled_orders.iloc[1]["filled_quantity"] == 10
        assert filled_orders.iloc[1]["price"] >= 405

        all_orders = strat_obj.broker.get_all_orders()
        assert len(all_orders) == 3
        entry_order = [o for o in all_orders if o.order_type == Order.OrderType.MARKET][0]
        limit_order = [o for o in all_orders if o.order_type == Order.OrderType.LIMIT][0]
        stop_order = [o for o in all_orders if o.order_type == Order.OrderType.STOP][0]

        assert entry_order.quantity == 10
        assert limit_order.quantity == 10
        assert stop_order.quantity == 10

        assert strat_obj.submitted_bracket_order.is_filled(), "Should be same as entry order"
        assert entry_order.is_filled()
        assert limit_order.is_filled()
        assert stop_order.is_canceled()

        assert entry_order.get_fill_price() > 1
        assert limit_order.get_fill_price() >= 405

    @pytest.mark.xfail(reason="yahoo sucks")
    def test_stock_oco(self):
        """
        Test the example strategy StockOco by running a backtest and checking that the strategy object is returned
        along with the correct results
        """

        # Parameters
        backtesting_start = datetime.datetime(2023, 3, 3)
        backtesting_end = datetime.datetime(2023, 3, 10)

        # Execute Backtest
        results, strat_obj = StockOco.run_backtest(
            YahooDataBacktesting,
            backtesting_start,
            backtesting_end,
            benchmark_asset=None,
            show_plot=False,
            show_tearsheet=False,
            save_tearsheet=False,
        )
        assert results
        assert isinstance(strat_obj, StockOco)

        trades_df = strat_obj.broker._trade_event_log_df

        # Get all the filled limit orders
        filled_orders = trades_df[(trades_df["status"] == "fill")]

        # Check that the second order was a lmit order with a price of $405 or more and a quantity of 10
        assert filled_orders.iloc[1]["type"] == "limit"
        assert filled_orders.iloc[1]["filled_quantity"] == 10
        assert filled_orders.iloc[1]["price"] >= 405

        all_orders = strat_obj.broker.get_all_orders()
        assert len(all_orders) == 4
        entry_order = [o for o in all_orders if o.order_type == Order.OrderType.MARKET][0]
        limit_order = [o for o in all_orders if o.order_type == Order.OrderType.LIMIT][0]
        stop_order = [o for o in all_orders if o.order_type == Order.OrderType.STOP][0]
        oco_order = [oco for oco in all_orders if oco.order_class == Order.OrderClass.OCO][0]

        assert entry_order.quantity == 10
        assert limit_order.quantity == 10
        assert stop_order.quantity == 10

        assert entry_order.is_filled()
        assert limit_order.is_filled()
        assert stop_order.is_canceled()
        assert oco_order.is_filled()

        assert entry_order.get_fill_price() > 1
        assert limit_order.get_fill_price() >= 405

    @pytest.mark.xfail(reason="yahoo sucks")
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
            benchmark_asset=None,
            show_plot=False,
            show_tearsheet=False,
            save_tearsheet=False,
        )
        assert results
        assert isinstance(strat_obj, BuyAndHold)

        # Check that the results are correct
        assert round(results["cagr"] * 100, 1) >= 2500.0
        assert round(results["total_return"] * 100, 1) >= 1.9
        assert round(results["max_drawdown"]["drawdown"] * 100, 1) == 0.0

    @pytest.mark.xfail(reason="yahoo sucks")
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
            benchmark_asset=None,
            show_plot=False,
            show_tearsheet=False,
            save_tearsheet=False,
        )
        assert results
        assert isinstance(strat_obj, DiversifiedLeverage)

        # Check that the results are correct
        assert round(results["cagr"] * 100, 1) >= 400000.0
        assert round(results["total_return"] * 100, 1) >= 5.3
        assert round(results["max_drawdown"]["drawdown"] * 100, 1) == 0.0

    @pytest.mark.xfail(reason="yahoo sucks")
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
            benchmark_asset=None,
            show_plot=False,
            show_tearsheet=False,
            save_tearsheet=False,
        )

        assert results
        assert isinstance(strat_obj, LimitAndTrailingStop)

        trades_df = strat_obj.broker._trade_event_log_df
        assert not trades_df.empty

        # Get all the filled limit orders
        filled_limit_orders = trades_df[(trades_df["status"] == "fill") & (trades_df["type"] == "limit")]

        # The first limit order should have filled at $399.71 and a quantity of 100
        assert round(filled_limit_orders.iloc[0]["price"], 2) == 399.71
        assert filled_limit_orders.iloc[0]["filled_quantity"] == 100

        # The second limit order should have filled at $399.74 and a quantity of 100
        assert round(filled_limit_orders.iloc[1]["price"], 2) == 407
        assert filled_limit_orders.iloc[1]["filled_quantity"] == 100

        # Get all the filled trailing stop orders
        filled_trailing_stop_orders = trades_df[
            (trades_df["status"] == "fill") & (trades_df["type"] == "trailing_stop")
        ]

        # Check if we have an order with a rounded price of 2 decimals of 400.45 and a quantity of 50
        order1 = filled_trailing_stop_orders[
            (round(filled_trailing_stop_orders["price"], 2) == 400.45)
            & (filled_trailing_stop_orders["filled_quantity"] == 50)
        ]
        assert len(order1) == 1

        # Check if we have an order with a price of 399.30 and a quantity of 100
        order2 = filled_trailing_stop_orders[
            (round(filled_trailing_stop_orders["price"], 2) == 399.30)
            & (filled_trailing_stop_orders["filled_quantity"] == 100)
        ]
        assert len(order2) == 1

        # Check that the results are correct
        # assert round(results["cagr"] * 100, 1) == 54.8
        assert round(results["volatility"] * 100, 1) >= 6.2
        assert round(results["total_return"] * 100, 1) >= 0.7
        assert round(results["max_drawdown"]["drawdown"] * 100, 1) <= 0.2

    @pytest.mark.skipif(
        not POLYGON_CONFIG["API_KEY"],
        reason="This test requires a Polygon.io API key"
    )
    @pytest.mark.skipif(
        POLYGON_CONFIG['API_KEY'] == '<your key here>',
        reason="This test requires a Polygon.io API key"
    )
    def test_options_hold_to_expiry(self):
        """
        Test the example strategy OptionsHoldToExpiry by running a backtest and checking that the strategy object is
        returned along with the correct results.
        """
        # Parameters
        backtesting_start = datetime.datetime(2023, 10, 16)
        # Extend backtesting_end to allow settlement on the next trading day (Monday, Oct 23rd)
        # for options expiring on Friday, Oct 20th.
        backtesting_end = datetime.datetime(2023, 10, 23, 23, 59, 59)

        # Execute Backtest
        results, strat_obj = OptionsHoldToExpiry.run_backtest(
            PolygonDataBacktesting,
            backtesting_start,
            backtesting_end,
            benchmark_asset=None,
            show_plot=False,
            show_tearsheet=False,
            save_tearsheet=False,
            polygon_api_key=POLYGON_CONFIG["API_KEY"],
        )

        trades_df = strat_obj.broker._trade_event_log_df
        assert not trades_df.empty

        # Get all the cash settled orders
        cash_settled_orders = trades_df[(trades_df["status"] == "cash_settled") & (trades_df["type"] == "cash_settled")]

        # The first limit order should have filled at $399.71 and a quantity of 100
        assert round(cash_settled_orders.iloc[0]["price"], 0) == 0
        assert cash_settled_orders.iloc[0]["filled_quantity"] == 10

    @pytest.mark.skip()  # Skip this test; it works locally but i can't get it to work on github actions
    def test_ccxt_backtesting(self):
        """
        Test the example strategy StockBracket by running a backtest and checking that the strategy object is returned
        along with the correct results
        """

        base_symbol = "ETH"
        quote_symbol = "USDT"
        backtesting_start = datetime.datetime(2023,2,11)
        backtesting_end = datetime.datetime(2024,2,12)
        asset = (Asset(symbol=base_symbol, asset_type="crypto"),
                Asset(symbol=quote_symbol, asset_type="crypto"))

        exchange_id = "kraken"  #"kucoin" #"bybit" #"okx" #"bitmex" # "binance"

        # CcxtBacktesting default data download limit is 50,000
        # If you want to change the maximum data download limit, you can do so by using 'max_data_download_limit'.
        kwargs = {
            # "max_data_download_limit":10000, # optional
            "exchange_id":exchange_id,
        }
        CcxtBacktesting.MIN_TIMESTEP = "day"
        results, strat_obj = CcxtBacktestingExampleStrategy.run_backtest(
            CcxtBacktesting,
            backtesting_start,
            backtesting_end,
            benchmark_asset=f"{base_symbol}/{quote_symbol}",
            show_plot=False,
            show_tearsheet=False,
            save_tearsheet=False,
            risk_free_rate=0.0,
            parameters={
            "asset":asset,
            "cash_at_risk":.25,
            "window":21},
            **kwargs
        )
        assert results
        assert isinstance(strat_obj, CcxtBacktestingExampleStrategy)

        trades_df = strat_obj.broker._trade_event_log_df

        # Get all the filled market orders
        filled_orders = trades_df[(trades_df["status"] == "fill")]

        # Check that the second order was a market order with a price of $1828 or more and a quantity of 17.0
        assert filled_orders.iloc[1]["type"] == "market"
        assert filled_orders.iloc[1]["filled_quantity"] == 17.0
        assert filled_orders.iloc[1]["price"] >= 1828
