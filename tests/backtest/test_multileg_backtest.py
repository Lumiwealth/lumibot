from decimal import Decimal

import pandas as pd
import pytest

from lumibot.backtesting import BacktestingBroker, PandasDataBacktesting
from lumibot.entities import Asset, Data
from lumibot.entities.order import Order
from lumibot.strategies.strategy import Strategy
from lumibot.traders import Trader


class _MultiLegStrategy(Strategy):
    def initialize(self, parameters=None):
        params = parameters or getattr(self, "parameters", {}) or {}
        self.long_asset = params["long_asset"]
        self.short_asset = params["short_asset"]
        self.quote = params["quote"]
        self.traded = False
        self.include_cash_positions = True
        self.sleeptime = "1D"

    def on_trading_iteration(self):
        if self.traded:
            return

        long_leg = self.create_order(
            self.long_asset,
            Decimal("10"),
            Order.OrderSide.BUY,
            order_type=Order.OrderType.MARKET,
            quote=self.quote,
        )
        short_leg = self.create_order(
            self.short_asset,
            Decimal("5"),
            Order.OrderSide.SELL,
            order_type=Order.OrderType.MARKET,
            quote=self.quote,
        )

        parent = Order(
            strategy=self,
            asset=self.long_asset,
            quantity=Decimal("0"),
            side=Order.OrderSide.BUY,
            order_type=Order.OrderType.MARKET,
            order_class=Order.OrderClass.MULTILEG,
            child_orders=[long_leg, short_leg],
        )

        self.submit_order(long_leg)
        self.submit_order(short_leg)
        self.broker._new_orders.append(parent)

        self.traded = True


def _make_data_for_assets(price_map):
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    pandas_data = {}
    for symbol, prices in price_map.items():
        asset = Asset(symbol, asset_type=Asset.AssetType.STOCK)
        index = pd.date_range("2025-01-13 09:30", periods=len(prices), freq="1D", tz="America/New_York")
        df = pd.DataFrame(
            {
                "open": prices,
                "high": [p * 1.01 for p in prices],
                "low": [p * 0.99 for p in prices],
                "close": prices,
                "volume": [1_000] * len(prices),
            },
            index=index,
        )
        local_df = df.tz_convert("America/New_York").tz_localize(None)
        pandas_data[(asset, quote)] = Data(
            asset=asset,
            df=local_df,
            quote=quote,
            timestep="day",
            timezone="America/New_York",
        )
    data_source = PandasDataBacktesting(
        pandas_data=pandas_data,
        datetime_start=list(pandas_data.values())[0].datetime_start,
        datetime_end=list(pandas_data.values())[0].datetime_end,
        show_progress_bar=False,
        market="24/7",
        auto_adjust=True,
    )
    data_source.load_data()
    return quote, data_source


def test_multileg_spread_backtest_cash_and_parent_fill():
    price_map = {"AAA": [100.0, 101.0], "BBB": [50.0, 51.0]}
    quote, data_source = _make_data_for_assets(price_map)

    broker = BacktestingBroker(data_source=data_source)
    broker.initialize_market_calendars(data_source.get_trading_days_pandas())

    long_asset = Asset("AAA", asset_type=Asset.AssetType.STOCK)
    short_asset = Asset("BBB", asset_type=Asset.AssetType.STOCK)

    strategy = _MultiLegStrategy(
        broker=broker,
        budget=100_000.0,
        analyze_backtest=False,
        parameters={
            "long_asset": long_asset,
            "short_asset": short_asset,
            "quote": quote,
        },
    )

    trader = Trader(logfile="", backtest=True)
    trader.add_strategy(strategy)
    trader.run_all(show_plot=False, show_tearsheet=False, show_indicators=False, save_tearsheet=False)

    fills = strategy.broker._trade_event_log_df
    # Filter for actual fills (not new orders)
    actual_fills = fills[fills['status'] == 'fill']
    assert len(actual_fills) == 2

    # Parent fill should not move cash beyond the sum of the legs.
    # Use actual fill prices from the backtesting data (AAA: $101, BBB: $51)
    expected_cash = 100_000.0 - (10 * 101.0) + (5 * 51.0)
    assert strategy.cash == pytest.approx(expected_cash, rel=1e-9)

    parent_orders = [
        order
        for order in strategy.broker._filled_orders.get_list()
        if getattr(order, "order_class", None) == Order.OrderClass.MULTILEG
    ]
    assert len(parent_orders) == 1
    parent_order = parent_orders[0]

    assert parent_order.trade_cost == pytest.approx(0.0, abs=1e-9)
    assert parent_order.avg_fill_price == pytest.approx((101.0 - 51.0), abs=1e-9)

    # Check that both child orders were filled (no parent fill event currently generated)
    assert len(actual_fills) == 2
    assert float(actual_fills.iloc[0]["filled_quantity"]) == pytest.approx(10.0)  # AAA buy
    assert float(actual_fills.iloc[1]["filled_quantity"]) == pytest.approx(5.0)   # BBB sell
