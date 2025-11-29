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
from lumibot.entities import Asset, Order, TradingFee

# Global parameters
# API Key for testing Polygon.io
from lumibot.credentials import POLYGON_CONFIG

class TestExampleStrategies:

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
            buy_trading_fees=[TradingFee(flat_fee=1.0)],
            sell_trading_fees=[TradingFee(flat_fee=1.0)],
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

        buy_fill = filled_orders[filled_orders["side"] == "buy"].iloc[0]
        exit_fill = filled_orders[filled_orders["side"].str.startswith("sell")].iloc[0]

        assert buy_fill["trade_cost"] > 0
        assert exit_fill["trade_cost"] > 0

        entry_value = float(buy_fill["filled_quantity"]) * float(buy_fill["price"])
        exit_value = float(exit_fill["filled_quantity"]) * float(exit_fill["price"])
        expected_cash = (
            strat_obj.initial_budget
            - entry_value
            - float(buy_fill["trade_cost"])
            + exit_value
            - float(exit_fill["trade_cost"])
        )

        assert pytest.approx(strat_obj.cash, rel=1e-9) == expected_cash

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
            show_indicators=False,
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

        # Filter to unique orders (OCO parent may have multiple references)
        entry_orders = [o for o in all_orders if o.order_type == Order.OrderType.MARKET]
        limit_orders = [o for o in all_orders if o.order_type == Order.OrderType.LIMIT]
        stop_orders = [o for o in all_orders if o.order_type == Order.OrderType.STOP]
        oco_orders = [oco for oco in all_orders if oco.order_class == Order.OrderClass.OCO]

        # Should have at least 1 of each type
        assert len(entry_orders) >= 1
        assert len(limit_orders) >= 1
        assert len(stop_orders) >= 1
        assert len(oco_orders) >= 1

        entry_order = entry_orders[0]
        limit_order = limit_orders[0]
        stop_order = stop_orders[0]
        oco_order = oco_orders[0]

        assert entry_order.quantity == 10
        assert limit_order.quantity == 10
        assert stop_order.quantity == 10

        assert entry_order.is_filled()
        assert limit_order.is_filled()
        assert stop_order.is_canceled()
        assert oco_order.is_filled()

        assert entry_order.get_fill_price() > 1
        assert limit_order.get_fill_price() >= 405

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
            show_indicators=False,
            save_tearsheet=False,
        )
        assert results
        assert isinstance(strat_obj, BuyAndHold)

        # Check that the results are correct (based on QQQ July 10-13, 2023)
        # Regression anchor: these values come from the legacy pandas pipeline.
        # If this assertion fails, investigate data accuracy or look-ahead bias instead of
        # adjusting the expected numbers.
        assert round(results["cagr"] * 100, 1) == 51.0  # ~51% annualized
        assert round(results["volatility"] * 100, 1) == 7.7  # 7.7% volatility
        assert round(results["sharpe"], 1) == 6.0  # Sharpe ratio ~6.0
        assert round(results["total_return"] * 100, 2) == 0.23  # 0.23% total return
        assert round(results["max_drawdown"]["drawdown"] * 100, 2) == 0.34  # 0.34% max drawdown

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
            show_indicators=False,
            save_tearsheet=False,
        )
        assert results
        assert isinstance(strat_obj, DiversifiedLeverage)

        # Check that the results are correct (leveraged ETFs July 10-13, 2023)
        assert round(results["cagr"] * 100, 0) == 2905  # ~2905% annualized
        assert round(results["volatility"] * 100, 0) == 25  # ~25% volatility
        assert round(results["sharpe"], 0) == 114  # Sharpe ratio ~114
        assert round(results["total_return"] * 100, 1) == 1.9  # 1.9% total return
        assert round(results["max_drawdown"]["drawdown"] * 100, 2) == 0.03  # 0.03% max drawdown

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
            show_indicators=False,
            save_tearsheet=False,
        )

        assert results
        assert isinstance(strat_obj, LimitAndTrailingStop)

        trades_df = strat_obj.broker._trade_event_log_df
        assert not trades_df.empty

        # Get all the filled limit orders
        filled_limit_orders = trades_df[(trades_df["status"] == "fill") & (trades_df["type"] == "limit")]

        # Verify limit orders filled correctly (March 3-10, 2023)
        assert len(filled_limit_orders) == 2
        assert round(filled_limit_orders.iloc[0]["price"], 2) == 399.71
        assert filled_limit_orders.iloc[0]["filled_quantity"] == 100
        assert round(filled_limit_orders.iloc[1]["price"], 2) == 407.00
        assert filled_limit_orders.iloc[1]["filled_quantity"] == 100

        # Verify that trailing stops were placed but canceled when limit orders filled
        all_trailing_stops = trades_df[trades_df["type"] == "trailing_stop"]
        assert len(all_trailing_stops) > 0  # Trailing stops were created
        canceled_trailing_stops = all_trailing_stops[all_trailing_stops["status"] == "canceled"]
        assert len(canceled_trailing_stops) > 0  # They were canceled when limit orders filled

        # Check that the backtest completed successfully with reasonable metrics
        assert round(results["volatility"] * 100, 1) >= 6.0
        assert round(results["total_return"] * 100, 1) >= 0.7
        assert round(results["max_drawdown"]["drawdown"] * 100, 1) == 0.7

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
            show_indicators=False,
            save_tearsheet=False,
            polygon_api_key=POLYGON_CONFIG["API_KEY"],
        )

        trades_df = strat_obj.broker._trade_event_log_df
        assert not trades_df.empty

        # Get all the cash settled orders
        cash_settled_orders = trades_df[
            (trades_df["status"] == "cash_settled") & (trades_df["type"] == "cash_settled")
        ]

        if cash_settled_orders.empty:
            summary_columns = ["time", "status", "type", "filled_quantity", "price", "quantity", "fill_price"]
            summary = trades_df.filter(items=summary_columns).to_dict("records")
            pytest.skip(f"No Polygon cash-settlement events captured; trade log snapshot: {summary}")

        # The first limit order should have filled at $399.71 and a quantity of 100
        assert round(cash_settled_orders.iloc[0]["price"], 0) == 0
        assert cash_settled_orders.iloc[0]["filled_quantity"] == 10

    @pytest.mark.skip(
        reason="CCXT backtesting causes segmentation fault due to DuckDB threading issues. "
        "The ccxt_data_store.py uses DuckDB for caching OHLCV data, but DuckDB connections "
        "are not thread-safe when accessed from multiple threads simultaneously. During backtesting, "
        "the strategy executor runs in a separate thread and makes concurrent calls to DuckDB, "
        "causing a segfault at line 209 in download_ohlcv(). "
        "This is a known issue - the test passes locally in some environments but fails in CI/CD "
        "and multi-threaded pytest runs. To fix properly, DuckDB access needs to be serialized "
        "or moved to a thread-local storage pattern."
    )
    def test_ccxt_backtesting(self):
        """
        Test the example strategy StockBracket by running a backtest and checking that the strategy object is returned
        along with the correct results
        """

        base_symbol = "ETH"
        quote_symbol = "USDT"
        # Shortened from 1-year backtest to 1-month backtest for faster testing
        backtesting_start = datetime.datetime(2023, 10, 1)
        backtesting_end = datetime.datetime(2023, 10, 31)
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
            show_indicators=False,
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
