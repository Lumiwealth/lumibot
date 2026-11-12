"""Opt-in wake hints: a backtest may skip on_trading_iteration only where the strategy said so.

The rule these tests pin: a strategy that sets an honest hint makes exactly the same trades,
trade events and stats rows as the same strategy without it, and the engine still wakes it
for everything it could see change (fills, cancels, position or cash changes, lifecycle
methods, a price it cannot read). Live trading ignores hints.
"""

from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

from lumibot.backtesting import PandasDataBacktesting
from lumibot.entities import Asset, Data
from lumibot.strategies import Strategy

QUOTE = Asset("USD", Asset.AssetType.FOREX)


def _frame(start, end, tz, seed, base=100.0):
    index = pd.date_range(start, end, freq="1min", tz=tz)
    rng = np.random.default_rng(seed)
    close = base + np.cumsum(rng.normal(0, 0.05, len(index)))
    return pd.DataFrame({"open": close, "high": close + 0.02, "low": close - 0.02, "close": close,
                         "volume": 100.0}, index=index)


def _run(strategy_class, pandas_data, start, end, hints, tmp_path, monkeypatch, **kwargs):
    tmp_path.mkdir(parents=True, exist_ok=True)
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("BACKTESTING_DATA_SOURCE", "none")
    calls = []

    class Runner(strategy_class):
        parameters = {"hints": hints}

        def on_trading_iteration(self):
            calls.append(self.get_datetime())
            super().on_trading_iteration()

    _results, strategy = Runner.run_backtest(
        PandasDataBacktesting, start, end, pandas_data=pandas_data, benchmark_asset=None,
        show_plot=False, show_tearsheet=False, save_tearsheet=False, show_indicators=False,
        quiet_logs=True, show_progress_bar=False, budget=100_000, **kwargs)
    events = strategy.broker._trade_event_log_df
    keep = [c for c in ("time", "strategy", "symbol", "type", "status", "price", "filled_quantity",
                        "multiplier", "trade_cost", "asset.right", "asset.strike", "asset.expiration",
                        "asset.asset_type", "side") if c in events.columns]
    stats = pd.DataFrame(strategy._stats_list)
    stats_keep = [c for c in ("datetime", "portfolio_value", "cash", "positions") if c in stats.columns]
    return {"events": events[keep].reset_index(drop=True), "stats": stats[stats_keep].astype(str),
            "calls": calls, "strategy": strategy}


def _assert_same(a, b):
    pd.testing.assert_frame_equal(a["events"], b["events"])
    pd.testing.assert_frame_equal(a["stats"], b["stats"])


# --------------------------------------------------------------------------- stock: time + price levels


class _StopTarget(Strategy):
    """Buys at 10:00, sells at a 0.3% stop, a 0.3% target or 15:50; one trade a day."""

    def initialize(self):
        self.sleeptime = "1M"
        self.vars.day = None
        self.vars.entry = None

    def on_trading_iteration(self):
        asset = Asset("SPY")
        dt = self.get_datetime()
        position = self.get_position(asset)
        price = self.get_last_price(asset)
        hints = self.parameters.get("hints")
        if position is not None and float(position.quantity) > 0:
            entry = self.vars.entry
            if price <= entry * 0.997 or price >= entry * 1.003 or (dt.hour, dt.minute) >= (15, 50):
                self.submit_order(self.create_order(asset, position.quantity, "sell"))
                return
            if hints:
                # Wake a cent early on each side: an early wake is always safe.
                self.wake_when(assets=asset, price_below=entry * 0.997 + 0.01, price_above=entry * 1.003 - 0.01,
                               at=dt.replace(hour=15, minute=50, second=0, microsecond=0))
            return
        if self.vars.day == dt.date():
            if hints:
                self.wake_when(at=(dt + timedelta(days=1)).replace(hour=10, minute=0, second=0, microsecond=0))
            return
        if (dt.hour, dt.minute) < (10, 0):
            if hints:
                self.wake_when(at=dt.replace(hour=10, minute=0, second=0, microsecond=0))
            return
        self.vars.day = dt.date()
        self.vars.entry = price
        self.submit_order(self.create_order(asset, 10, "buy"))


def _stock_data():
    asset = Asset("SPY")
    frame = _frame("2026-08-03 09:30", "2026-08-07 16:00", "America/New_York", seed=7)
    return {(asset, QUOTE): Data(asset, frame, timestep="minute", quote=QUOTE)}


def test_stock_stop_target_strategy_is_identical_with_hints(tmp_path, monkeypatch):
    data = _stock_data()
    start, end = datetime(2026, 8, 3), datetime(2026, 8, 7)
    plain = _run(_StopTarget, data, start, end, False, tmp_path / "plain", monkeypatch)
    hinted = _run(_StopTarget, data, start, end, True, tmp_path / "hinted", monkeypatch)
    assert len(plain["events"]) >= 8  # it really traded every day
    _assert_same(plain, hinted)
    assert len(hinted["calls"]) * 5 < len(plain["calls"])  # most minutes were skipped


# --------------------------------------------------------------------------- crypto: interval + value levels


class _CryptoBand(Strategy):
    """24/7: holds 0.5 BTC, rebalances every 6 hours, exits if the position value leaves a band."""

    def initialize(self):
        self.sleeptime = "1M"
        self.set_market("24/7")
        self.vars.last = None

    def on_trading_iteration(self):
        btc = Asset("BTC", Asset.AssetType.CRYPTO)
        dt = self.get_datetime()
        position = self.get_position(btc)
        held = float(position.quantity) if position is not None else 0.0
        price = float(self.get_last_price(btc, quote=QUOTE))
        value = held * price
        hints = self.parameters.get("hints")
        due = self.vars.last is None or dt - self.vars.last >= timedelta(hours=6)
        if held and (value <= 0.5 * 99.0 or value >= 0.5 * 101.0):
            self.submit_order(self.create_order(btc, held, "sell", quote=QUOTE))
            self.vars.last = dt
            return
        if due:
            self.vars.last = dt
            if not held:
                self.submit_order(self.create_order(btc, 0.5, "buy", quote=QUOTE))
            return
        if hints:
            kwargs = {"at": self.vars.last + timedelta(hours=6)}
            if held:
                kwargs.update(assets=btc, value_below=0.5 * 99.0 + 0.001, value_above=0.5 * 101.0 - 0.001)
            self.wake_when(**kwargs)


def test_crypto_interval_and_value_band_is_identical_with_hints(tmp_path, monkeypatch):
    btc = Asset("BTC", Asset.AssetType.CRYPTO)
    frame = _frame("2026-08-03 00:00", "2026-08-05 23:59", "UTC", seed=3)
    data = {(btc, QUOTE): Data(btc, frame, timestep="minute", quote=QUOTE)}
    start, end = datetime(2026, 8, 3), datetime(2026, 8, 5, 23)
    plain = _run(_CryptoBand, data, start, end, False, tmp_path / "plain", monkeypatch)
    hinted = _run(_CryptoBand, data, start, end, True, tmp_path / "hinted", monkeypatch)
    assert len(plain["events"]) >= 6
    _assert_same(plain, hinted)
    assert len(hinted["calls"]) * 5 < len(plain["calls"])


# --------------------------------------------------------------------------- engine wakes for events


class _FarHint(Strategy):
    """Places a resting limit order, then hints a wake far in the future.

    The fill must still wake it: it records when it first saw the position.
    """

    def initialize(self):
        self.sleeptime = "1M"
        self.vars.placed = False
        self.vars.seen = None
        self.vars.wakes = []

    def on_trading_iteration(self):
        asset = Asset("SPY")
        position = self.get_position(asset)
        if position is not None and float(position.quantity) > 0 and self.vars.seen is None:
            self.vars.seen = self.get_datetime()
        if not self.vars.placed:
            self.vars.placed = True
            price = float(self.get_last_price(asset))
            self.submit_order(self.create_order(asset, 1, "buy", limit_price=price - 0.15))
        if self.parameters.get("hints"):
            self.wake_when(at=self.get_datetime() + timedelta(days=30))


def test_a_fill_wakes_the_strategy_despite_a_far_hint(tmp_path, monkeypatch):
    data = _stock_data()
    start, end = datetime(2026, 8, 3), datetime(2026, 8, 4)
    plain = _run(_FarHint, data, start, end, False, tmp_path / "plain", monkeypatch)
    hinted = _run(_FarHint, data, start, end, True, tmp_path / "hinted", monkeypatch)
    assert plain["strategy"].vars.seen is not None
    assert hinted["strategy"].vars.seen == plain["strategy"].vars.seen
    _assert_same(plain, hinted)
    # It slept again after the fill: far fewer callbacks than minutes.
    assert len(hinted["calls"]) < 10


def test_lifecycle_methods_and_new_sessions_wake_the_strategy(tmp_path, monkeypatch):
    data = _stock_data()
    start, end = datetime(2026, 8, 3), datetime(2026, 8, 7)
    hinted = _run(_FarHint, data, start, end, True, tmp_path / "hinted", monkeypatch)
    days = {dt.date() for dt in hinted["calls"]}
    assert len(days) >= 4  # each session's first iteration ran


class _MissingPrice(Strategy):
    def initialize(self):
        self.sleeptime = "1M"

    def on_trading_iteration(self):
        self.wake_when(assets=Asset("NOPE"), price_above=1e9)


def test_a_price_the_engine_cannot_read_wakes_every_iteration(tmp_path, monkeypatch):
    data = _stock_data()
    start, end = datetime(2026, 8, 3), datetime(2026, 8, 4)
    hinted = _run(_MissingPrice, data, start, end, True, tmp_path / "hinted", monkeypatch)
    plain = _run(_StopTarget, data, start, end, False, tmp_path / "plain", monkeypatch)
    assert len(hinted["calls"]) == len(plain["calls"])


def test_a_hint_lasts_until_the_next_run_only(tmp_path, monkeypatch):
    class Once(Strategy):
        def initialize(self):
            self.sleeptime = "1M"
            self.vars.n = 0

        def on_trading_iteration(self):
            self.vars.n += 1
            if self.vars.n == 1:
                self.wake_when(at=self.get_datetime() + timedelta(minutes=30))

    data = _stock_data()
    hinted = _run(Once, data, datetime(2026, 8, 3), datetime(2026, 8, 4), True, tmp_path, monkeypatch)
    calls = hinted["calls"]
    assert calls[1] - calls[0] == timedelta(minutes=30)
    assert calls[2] - calls[1] == timedelta(minutes=1)


def test_bad_hints_are_rejected():
    strategy = Strategy.__new__(Strategy)
    with pytest.raises(ValueError):
        Strategy.wake_when(strategy, price_above=10)  # a price level needs assets
    with pytest.raises(ValueError):
        Strategy.wake_when(strategy, assets=Asset("SPY"))  # assets without a level


# --------------------------------------------------------------------------- live trading


def test_live_trading_ignores_hints():
    class Live:
        is_backtesting = False
        _wake_condition = None

    from lumibot.strategies.wake_hints import WakeCondition, store_wake_condition

    live = Live()
    store_wake_condition(live, WakeCondition(at=datetime(2030, 1, 1)))
    assert live._wake_condition is None


def test_a_cash_or_position_change_without_an_order_event_wakes_the_strategy():
    """Expiry, settlement, dividends and splits change the account without an order event."""
    from types import SimpleNamespace

    from lumibot.strategies.wake_hints import WakeCondition, should_skip, store_wake_condition

    positions = []
    broker = SimpleNamespace(get_tracked_positions=lambda name: positions)
    strategy = SimpleNamespace(is_backtesting=True, name="s", cash=1000.0, broker=broker,
                               get_datetime=lambda: datetime(2026, 8, 3, 10, 0), _wake_condition=None)
    executor = SimpleNamespace(_wake_event=False)
    store_wake_condition(strategy, WakeCondition(at=datetime(2026, 9, 1, 10, 0)))
    assert should_skip(strategy, executor)
    strategy.cash = 1012.5  # a dividend landed
    assert not should_skip(strategy, executor)
    assert strategy._wake_condition is None

    store_wake_condition(strategy, WakeCondition(at=datetime(2026, 9, 1, 10, 0)))
    assert should_skip(strategy, executor)
    positions.append(SimpleNamespace(asset=Asset("SPY"), quantity=5))  # a split or settlement
    assert not should_skip(strategy, executor)
