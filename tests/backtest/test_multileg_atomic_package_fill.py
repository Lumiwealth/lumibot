"""Option fill prices in backtests always belong to the fill bar.

Illiquid option contracts trade at different, sparse minutes. For every option order the
fill price for a bar is, in order: a real bid/ask on that bar, a real trade on that bar,
or a Black-Scholes estimate (underlying price on that bar, implied volatility from the
contract's last real trade, or the nearest strike's when it never traded). Estimates pay a
spread penalty and are marked ``price_source="estimated"``. A multi-leg order fills every
leg on the same bar or none of them, so a backtest never holds a naked short leg while it
waits for a protecting leg to trade.
"""

from datetime import date, datetime
from decimal import Decimal

import pandas as pd
import pytest

from lumibot.backtesting import BacktestingBroker, PandasDataBacktesting
from lumibot.backtesting.data_provenance import build_backtest_data_provenance
from lumibot.entities import Asset, Data
from lumibot.entities.order import Order
from lumibot.strategies.strategy import Strategy
from lumibot.traders import Trader

QUOTE = Asset("USD", asset_type=Asset.AssetType.FOREX)
EXPIRY = date(2025, 2, 21)
START = pd.Timestamp("2025-01-13 09:30")
MINUTES = 60
NY = "America/New_York"


def _option(strike, right):
    return Asset("SPY", asset_type=Asset.AssetType.OPTION, expiration=EXPIRY, strike=strike, right=right)


LONG_PUT = _option(560, "put")
SHORT_PUT = _option(570, "put")
SHORT_CALL = _option(610, "call")
LONG_CALL = _option(620, "call")
CONDOR = [
    (LONG_PUT, Order.OrderSide.BUY),
    (SHORT_PUT, Order.OrderSide.SELL),
    (SHORT_CALL, Order.OrderSide.SELL),
    (LONG_CALL, Order.OrderSide.BUY),
]
PUT_SPREAD = [(SHORT_PUT, Order.OrderSide.SELL), (LONG_PUT, Order.OrderSide.BUY)]


def _data(asset, prints):
    """Minute bars only at the given minute offsets (sparse, like an illiquid contract)."""
    index = [START + pd.Timedelta(minutes=m) for m in sorted(prints)]
    opens = [prints[m] for m in sorted(prints)]
    df = pd.DataFrame(
        {"open": opens, "high": opens, "low": opens, "close": opens, "volume": [10] * len(opens)},
        index=pd.DatetimeIndex(index),
    )
    return Data(asset=asset, df=df, quote=QUOTE, timestep="minute", timezone=NY)


def _run(prints_by_asset, legs, *, leg_kwargs=None, submit_after=0, close_after=None, multileg=True, **submit_kwargs):
    pandas_data = {(asset, QUOTE): _data(asset, prints) for asset, prints in prints_by_asset.items()}
    # A liquid underlying trades every minute.
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
    start = START.tz_localize(NY)

    class PackageStrategy(Strategy):
        def initialize(self):
            self.sleeptime = "1M"
            self.include_cash_positions = True
            self.submitted = None
            self.closing = None
            self.fills = {}
            self.close_fills = {}

        def on_trading_iteration(self):
            now = self.get_datetime()
            if self.submitted is None and now >= start + pd.Timedelta(minutes=submit_after):
                orders = [
                    self.create_order(asset, Decimal("1"), side, quote=QUOTE, **(leg_kwargs or {}))
                    for asset, side in legs
                ]
                if multileg:
                    self.submitted = self.submit_orders(orders, is_multileg=True, **submit_kwargs)
                else:
                    self.submitted = [self.submit_order(order) for order in orders]
            elif (
                close_after is not None
                and self.closing is None
                and self.submitted is not None
                and self.submitted[0].is_filled()
                and now >= start + pd.Timedelta(minutes=close_after)
            ):
                flip = {Order.OrderSide.BUY: Order.OrderSide.SELL, Order.OrderSide.SELL: Order.OrderSide.BUY}
                orders = [self.create_order(asset, Decimal("1"), flip[side], quote=QUOTE) for asset, side in legs]
                self.closing = self.submit_orders(orders, is_multileg=True)

        def on_filled_order(self, position, order, price, quantity, multiplier):
            if order.order_class is not Order.OrderClass.MULTILEG:
                target = self.close_fills if self.closing is not None else self.fills
                target[order.asset] = (_minute(self.get_datetime()), float(price), order.identifier)

    strategy = PackageStrategy(broker=broker, budget=100_000.0, analyze_backtest=False)
    trader = Trader(logfile="", backtest=True)
    trader.add_strategy(strategy)
    trader.run_all(show_plot=False, show_tearsheet=False, show_indicators=False, save_tearsheet=False)
    # Read each fill's price source back from the trade log, where it is recorded.
    log = strategy.broker._trade_event_log_df
    fill_rows = log[log["status"] == "fill"]
    source_by_id = dict(zip(fill_rows["identifier"], fill_rows["price_source"]))
    for book in (strategy.fills, strategy.close_fills):
        for asset, (minute, price, identifier) in list(book.items()):
            book[asset] = (minute, price, source_by_id.get(identifier))
    return strategy


def _minute(ts):
    ts = pd.Timestamp(ts)
    if ts.tzinfo is not None:
        ts = ts.tz_convert(NY).tz_localize(None)
    return int((ts - START).total_seconds() // 60)


def test_condor_fills_every_leg_on_the_bar_where_they_all_trade():
    # Nothing traded before minute 5, so nothing can be priced until then.
    prints = {LONG_PUT: {5: 1.10}, SHORT_PUT: {5: 2.10}, SHORT_CALL: {5: 2.40}, LONG_CALL: {5: 1.00}}
    strategy = _run(prints, CONDOR)
    assert {asset: fill[0] for asset, fill in strategy.fills.items()} == {a: 5 for a, _ in CONDOR}
    assert {fill[2] for fill in strategy.fills.values()} == {"trade"}
    parent = strategy.submitted[0]
    assert parent.is_filled()
    # Net of the leg prices on that bar: buys pay, sells receive (a credit is negative).
    assert float(parent.avg_fill_price) == pytest.approx(1.10 - 2.10 - 2.40 + 1.00)


def test_leg_without_a_trade_on_the_bar_gets_a_marked_estimate_from_its_last_trade():
    # Submitted at minute 6; neither leg trades at 6, both traded at 3.
    prints = {SHORT_PUT: {3: 2.00, 30: 2.10}, LONG_PUT: {3: 1.00}}
    strategy = _run(prints, PUT_SPREAD, submit_after=6)
    short_put, long_put = strategy.fills[SHORT_PUT], strategy.fills[LONG_PUT]
    assert short_put[0] == long_put[0] == 6
    assert short_put[2] == long_put[2] == "estimated"
    # Model value is the minute-3 price (same underlying, 4 minutes of time decay); the default
    # 5% spread penalty is paid on top: the sale receives less, the purchase pays more.
    assert 1.88 <= short_put[1] <= 1.91
    assert 1.04 <= long_put[1] <= 1.06


def test_estimate_penalty_is_a_setting(monkeypatch):
    monkeypatch.setenv("LUMIBOT_BACKTEST_OPTION_ESTIMATE_PENALTY_PCT", "0")
    prints = {SHORT_PUT: {3: 2.00}, LONG_PUT: {3: 1.00}}
    strategy = _run(prints, PUT_SPREAD, submit_after=6)
    assert strategy.fills[SHORT_PUT][1] == pytest.approx(2.00, abs=0.02)
    assert strategy.fills[LONG_PUT][1] == pytest.approx(1.00, abs=0.02)


def test_contract_that_never_traded_uses_the_nearest_strike_volatility():
    # The 620 call never trades; its estimate uses the 610 call's implied volatility.
    prints = {LONG_PUT: {3: 1.00}, SHORT_PUT: {3: 2.00}, SHORT_CALL: {3: 2.50}}
    strategy = _run(prints, CONDOR, submit_after=6)
    assert {fill[0] for fill in strategy.fills.values()} == {6}
    assert {fill[2] for fill in strategy.fills.values()} == {"estimated"}
    long_call, short_call = strategy.fills[LONG_CALL][1], strategy.fills[SHORT_CALL][1]
    assert 0 < long_call < short_call


def test_nothing_fills_without_any_price_and_the_package_expires_whole():
    prints = {SHORT_PUT: {40: 2.10}, LONG_PUT: {40: 1.00}}
    strategy = _run(
        prints,
        PUT_SPREAD,
        leg_kwargs={"time_in_force": "gtd", "good_till_date": pd.Timestamp("2025-01-13 10:00", tz=NY).to_pydatetime()},
    )
    assert strategy.fills == {}
    parent = strategy.submitted[0]
    assert parent.is_canceled()
    assert all(child.is_canceled() for child in parent.child_orders)
    assert "multi-leg package canceled as a whole" in (parent.error_message or "")
    assert strategy.get_position(SHORT_PUT) is None
    assert strategy.get_position(LONG_PUT) is None


def test_credit_package_limit_applies_to_the_net_including_estimates():
    # Minute 10: both trade, net credit 0.50 (below the 0.80 limit). Minutes 11 to 24 are
    # estimated from minute 10 and pay the penalty, so the net is even lower.
    # Minute 25: both trade, net credit 1.00, so both fill there.
    prints = {SHORT_PUT: {10: 2.00, 25: 2.20}, LONG_PUT: {10: 1.50, 25: 1.20}}
    strategy = _run(prints, PUT_SPREAD, order_type="credit", price=0.80)
    assert {asset: fill[0] for asset, fill in strategy.fills.items()} == {SHORT_PUT: 25, LONG_PUT: 25}
    assert {fill[2] for fill in strategy.fills.values()} == {"trade"}


def test_closing_condor_is_one_package_too():
    # Opens at minute 5 where all four trade. Closed at minute 12: no leg trades at 12,
    # so every leg is estimated on that same bar and the position is flat.
    prints = {LONG_PUT: {5: 1.00}, SHORT_PUT: {5: 2.00}, SHORT_CALL: {5: 2.50}, LONG_CALL: {5: 1.20}}
    strategy = _run(prints, CONDOR, close_after=12)
    assert {fill[0] for fill in strategy.fills.values()} == {5}
    assert {fill[0] for fill in strategy.close_fills.values()} == {12}
    assert {fill[2] for fill in strategy.close_fills.values()} == {"estimated"}
    for asset, _ in CONDOR:
        assert strategy.get_position(asset) is None


def test_single_leg_option_market_order_never_fills_at_an_older_bar_price():
    prints = {SHORT_PUT: {3: 2.00}}
    strategy = _run(prints, [(SHORT_PUT, Order.OrderSide.SELL)], submit_after=6, multileg=False)
    minute, price, source = strategy.fills[SHORT_PUT]
    assert minute == 6
    assert source == "estimated"
    assert 1.88 <= price <= 1.91


def test_estimated_fills_are_marked_in_the_trade_log_and_counted_in_provenance():
    prints = {SHORT_PUT: {3: 2.00}, LONG_PUT: {3: 1.00}}
    strategy = _run(prints, PUT_SPREAD, submit_after=6)
    log = strategy.broker._trade_event_log_df
    filled = log[(log["status"] == "fill") & (log["symbol"] == "SPY")]
    assert set(filled["price_source"].dropna()) == {"estimated"}
    counts = strategy.broker._option_fill_price_counts
    assert counts == {"estimated": 2}
    provenance = build_backtest_data_provenance(strategy.broker.data_source, counts)
    assert provenance["optionFillPrices"] == {"estimated": 2}


def test_condor_with_one_leg_not_trading_on_the_fill_bar_still_fills_all_four_there():
    # A strategy that guards against half-open condors must never see one. Three legs trade
    # at minute 6; the long call last traded at minute 3. All four fill at 6, the long call
    # at a marked estimate, so the package is never partly filled.
    prints = {
        LONG_PUT: {3: 1.00, 6: 1.05},
        SHORT_PUT: {6: 2.10},
        SHORT_CALL: {6: 2.40},
        LONG_CALL: {3: 1.20},
    }
    strategy = _run(prints, CONDOR, submit_after=6)
    assert {asset: fill[0] for asset, fill in strategy.fills.items()} == {a: 6 for a, _ in CONDOR}
    sources = {asset: fill[2] for asset, fill in strategy.fills.items()}
    assert sources == {LONG_PUT: "trade", SHORT_PUT: "trade", SHORT_CALL: "trade", LONG_CALL: "estimated"}
    parent = strategy.submitted[0]
    assert parent.is_filled()
    assert all(child.is_filled() for child in parent.child_orders)
    for asset, _ in CONDOR:
        assert strategy.get_position(asset) is not None
