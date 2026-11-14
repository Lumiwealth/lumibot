"""A multi-leg option order fills as one package in backtests.

Illiquid option contracts print at different, sparse minutes. A spread or condor
submitted with ``submit_orders(..., is_multileg=True)`` must fill every leg on
the same bar, or none of them, so the backtest never holds a naked short leg
while it waits for the protecting long leg to print.
"""

from datetime import date, datetime
from decimal import Decimal

import pandas as pd
import pytest

from lumibot.backtesting import BacktestingBroker, PandasDataBacktesting
from lumibot.entities import Asset, Data
from lumibot.entities.order import Order
from lumibot.strategies.strategy import Strategy
from lumibot.traders import Trader

QUOTE = Asset("USD", asset_type=Asset.AssetType.FOREX)
EXPIRY = date(2025, 2, 21)
START = pd.Timestamp("2025-01-13 09:30")
MINUTES = 60


def _option(strike, right):
    return Asset("SPY", asset_type=Asset.AssetType.OPTION, expiration=EXPIRY, strike=strike, right=right)


LONG_PUT = _option(560, "put")
SHORT_PUT = _option(570, "put")
SHORT_CALL = _option(610, "call")
LONG_CALL = _option(620, "call")


def _data(asset, prints):
    """Minute bars only at the given minute offsets (sparse, like an illiquid contract)."""
    index = [START + pd.Timedelta(minutes=m) for m in sorted(prints)]
    opens = [prints[m] for m in sorted(prints)]
    df = pd.DataFrame(
        {"open": opens, "high": opens, "low": opens, "close": opens, "volume": [10] * len(opens)},
        index=pd.DatetimeIndex(index),
    )
    return Data(asset=asset, df=df, quote=QUOTE, timestep="minute", timezone="America/New_York")


def _run(prints_by_asset, legs, leg_kwargs=None, close_after=None, **submit_kwargs):
    pandas_data = {(asset, QUOTE): _data(asset, prints) for asset, prints in prints_by_asset.items()}
    # A liquid underlying keeps the clock ticking every minute.
    spy = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    pandas_data[(spy, QUOTE)] = _data(spy, {m: 590.0 for m in range(MINUTES)})
    data_source = PandasDataBacktesting(
        pandas_data=pandas_data,
        datetime_start=datetime(2025, 1, 13, 9, 30),
        datetime_end=datetime(2025, 1, 13, 10, 30),
        show_progress_bar=False,
        market="24/7",
    )
    data_source.load_data()
    broker = BacktestingBroker(data_source=data_source)
    broker.initialize_market_calendars(data_source.get_trading_days_pandas())

    class PackageStrategy(Strategy):
        def initialize(self):
            self.sleeptime = "1M"
            self.include_cash_positions = True
            self.submitted = None
            self.closing = None
            self.fill_times = {}
            self.close_fill_times = {}

        def on_trading_iteration(self):
            if self.submitted is None:
                orders = [
                    self.create_order(asset, Decimal("1"), side, quote=QUOTE, **(leg_kwargs or {}))
                    for asset, side in legs
                ]
                self.submitted = self.submit_orders(orders, is_multileg=True, **submit_kwargs)
            elif (
                close_after is not None
                and self.closing is None
                and self.submitted[0].is_filled()
                and self.get_datetime() >= START.tz_localize("America/New_York") + pd.Timedelta(minutes=close_after)
            ):
                flip = {Order.OrderSide.BUY: Order.OrderSide.SELL, Order.OrderSide.SELL: Order.OrderSide.BUY}
                orders = [self.create_order(asset, Decimal("1"), flip[side], quote=QUOTE) for asset, side in legs]
                self.closing = self.submit_orders(orders, is_multileg=True)

        def on_filled_order(self, position, order, price, quantity, multiplier):
            if order.order_class is not Order.OrderClass.MULTILEG:
                target = self.close_fill_times if self.closing is not None else self.fill_times
                target[order.asset] = self.get_datetime()

    strategy = PackageStrategy(broker=broker, budget=100_000.0, analyze_backtest=False)
    trader = Trader(logfile="", backtest=True)
    trader.add_strategy(strategy)
    trader.run_all(show_plot=False, show_tearsheet=False, show_indicators=False, save_tearsheet=False)
    return strategy


def _minute(ts):
    ts = pd.Timestamp(ts)
    if ts.tzinfo is not None:
        ts = ts.tz_convert("America/New_York").tz_localize(None)
    return int((ts - START).total_seconds() // 60)


CONDOR = [
    (LONG_PUT, Order.OrderSide.BUY),
    (SHORT_PUT, Order.OrderSide.SELL),
    (SHORT_CALL, Order.OrderSide.SELL),
    (LONG_CALL, Order.OrderSide.BUY),
]


def test_condor_with_sparse_legs_fills_every_leg_on_one_bar():
    # Each leg prints at different minutes; minute 40 is the first where all four print.
    prints = {
        LONG_PUT: {2: 1.00, 40: 1.10},
        SHORT_PUT: {5: 2.00, 40: 2.10},
        SHORT_CALL: {10: 2.50, 40: 2.40},
        LONG_CALL: {20: 1.20, 40: 1.00},
    }
    strategy = _run(prints, CONDOR)
    minutes = {asset: _minute(ts) for asset, ts in strategy.fill_times.items()}
    assert set(minutes) == {LONG_PUT, SHORT_PUT, SHORT_CALL, LONG_CALL}
    assert set(minutes.values()) == {40}
    parent = strategy.submitted[0]
    assert parent.is_filled()
    # Net of the leg prices on that bar: buys pay, sells receive (a credit is negative).
    assert float(parent.avg_fill_price) == pytest.approx(1.10 - 2.10 - 2.40 + 1.00)


def test_vertical_does_not_fill_while_one_leg_has_no_price_and_expires_whole():
    # The long leg never prints after the short leg's bar, so no bar has both.
    prints = {
        SHORT_PUT: {3: 2.00, 30: 2.10},
        LONG_PUT: {7: 1.00},
    }
    legs = [(SHORT_PUT, Order.OrderSide.SELL), (LONG_PUT, Order.OrderSide.BUY)]
    strategy = _run(
        prints,
        legs,
        leg_kwargs={"time_in_force": "gtd", "good_till_date": pd.Timestamp("2025-01-13 10:00", tz="America/New_York").to_pydatetime()},
    )
    assert strategy.fill_times == {}
    parent = strategy.submitted[0]
    assert parent.is_canceled()
    assert all(child.is_canceled() for child in parent.child_orders)
    assert "multi-leg package canceled as a whole" in (parent.error_message or "")
    assert strategy.get_position(SHORT_PUT) is None
    assert strategy.get_position(LONG_PUT) is None


def test_credit_package_limit_applies_to_the_net_and_waits_for_a_bar_that_meets_it():
    # Minute 10: both print, net credit 0.50 (below the 0.80 limit) -> no fill.
    # Minute 25: both print, net credit 1.00 -> fill both on that bar.
    prints = {
        SHORT_PUT: {10: 2.00, 25: 2.20},
        LONG_PUT: {10: 1.50, 25: 1.20},
    }
    legs = [(SHORT_PUT, Order.OrderSide.SELL), (LONG_PUT, Order.OrderSide.BUY)]
    strategy = _run(prints, legs, order_type="credit", price=0.80)
    minutes = {asset: _minute(ts) for asset, ts in strategy.fill_times.items()}
    assert minutes == {SHORT_PUT: 25, LONG_PUT: 25}


def test_closing_condor_is_one_package_too():
    # Opens at minute 5 (all four print). Close is submitted at minute 10; the legs then
    # print at 15, 22, 30 and together only at 45, so the close must happen at 45.
    prints = {
        LONG_PUT: {5: 1.00, 15: 0.90, 45: 0.80},
        SHORT_PUT: {5: 2.00, 22: 1.80, 45: 1.50},
        SHORT_CALL: {5: 2.50, 30: 2.00, 45: 1.70},
        LONG_CALL: {5: 1.20, 45: 0.70},
    }
    strategy = _run(prints, CONDOR, close_after=10)
    assert {_minute(ts) for ts in strategy.fill_times.values()} == {5}
    assert {_minute(ts) for ts in strategy.close_fill_times.values()} == {45}
    assert len(strategy.close_fill_times) == 4
    for asset in (LONG_PUT, SHORT_PUT, SHORT_CALL, LONG_CALL):
        assert strategy.get_position(asset) is None
