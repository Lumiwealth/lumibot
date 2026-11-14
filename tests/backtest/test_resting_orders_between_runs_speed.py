"""Engine benchmark: checking resting orders between runs must stay cheap.

Same synthetic 5-minute stock strategy as ``test_routed_intraday_metadata_speed`` (the
4.6.62 engine benchmark), run with and without the between-run checks:

* market orders only: no resting orders, so trades and final value are identical and the
  broker is never asked an extra time;
* bracket entries (a target and a stop rest between runs): the broker is asked only on
  bars where an order can fill, never once per bar.

Elapsed seconds are recorded as diagnostics only, never asserted. Operation counts are
asserted, so a change that brings per-bar Python work back fails.

Default window is 1 month. Set ``LUMIBOT_SPEED_BENCH_MONTHS=12`` for the full-year
measurement (2026-11-14, Apple M-series laptop, 12 months of 5-minute bars:
market-only 36.5 s without checks and 34.5 s with; brackets 71.0 s and 66.7 s with 580 extra
broker passes for 1,417 fills over 19,392 sleeps. The differences are run-to-run noise).
"""
from time import perf_counter

import numpy as np
import pandas as pd
import pytest

from lumibot.backtesting import BacktestingBroker
from lumibot.entities import Asset, Order
from lumibot.strategies import Strategy
from tests.backtest.test_routed_intraday_metadata_speed import (
    END,
    MONTHS,
    START,
    _FiveMinuteAverage,
    _synthetic_stock,
)
from lumibot.tools.ibkr_equity_basis import decorate_adjusted_frame


def _with_real_minute_bars(frames):
    """Give the minute series its own path inside each 5-minute bar.

    The 4.6.62 benchmark repeats each 5-minute bar on its five minutes, so nothing new can
    happen between two runs. Here every minute moves, and the 5-minute bars are rebuilt
    from the minutes.
    """
    index = frames["minute"].index
    close = 500 + np.cumsum(np.random.default_rng(7).normal(0, 0.12, len(index)))
    minute = pd.DataFrame({"open": close - 0.02, "high": close + 0.15, "low": close - 0.15,
                           "close": close, "volume": 1000.0}, index=index)
    blocks = np.searchsorted(frames["5minute"].index.asi8, index.asi8, side="right") - 1
    grouped = minute.groupby(blocks)
    five = pd.DataFrame({"open": grouped["open"].first().to_numpy(), "high": grouped["high"].max().to_numpy(),
                         "low": grouped["low"].min().to_numpy(), "close": grouped["close"].last().to_numpy(),
                         "volume": grouped["volume"].sum().to_numpy()}, index=frames["5minute"].index)
    return {"day": frames["day"], "5minute": five, "minute": minute}


class _FiveMinuteBracket(Strategy):
    """Enters with a bracket whenever flat: a target and a stop rest between runs."""

    def initialize(self):
        self.sleeptime = "5M"

    def on_trading_iteration(self):
        asset = Asset("SPY")
        bars = self.get_historical_prices(asset, 21, "5 minutes")
        if self.get_position(asset) is not None or any(order.is_active() for order in self.get_orders()):
            return
        price = self.get_last_price(asset)
        if price is None:
            return
        self.submit_order(
            self.create_order(
                asset,
                10,
                "buy",
                order_class=Order.OrderClass.BRACKET,
                secondary_limit_price=round(price + 1.5, 2),
                secondary_stop_price=round(price - 1.5, 2),
            )
        )


def _run(monkeypatch, tmp_path, name, strategy_class, frames, snapshot, *, checks):
    from lumibot.backtesting import routed_backtesting
    from lumibot.tools import ibkr_helper

    folder = tmp_path / name
    folder.mkdir()
    monkeypatch.chdir(folder)
    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "test-key")
    monkeypatch.setenv("LUMIBOT_CACHE_BACKEND", "local")
    monkeypatch.setattr(BacktestingBroker, "_check_resting_orders_between_runs", checks)

    def prices(**kwargs):
        frame = frames[str(kwargs["timestep"])]
        window = frame.loc[(frame.index >= kwargs["start_dt"]) & (frame.index <= kwargs["end_dt"])].copy()
        return decorate_adjusted_frame(window, snapshot, provider_prices_verified=True)

    monkeypatch.setattr(ibkr_helper, "get_price_data", prices)
    monkeypatch.setattr(ibkr_helper, "_get_cached_equity_actions", lambda *a, **k: snapshot.actions_frame())

    counts = {"sleeps": 0, "extra_broker_passes": 0}
    advance = BacktestingBroker._advance_checking_resting_orders
    next_event = BacktestingBroker._next_between_run_event

    def counting_advance(self, *args, **kwargs):
        counts["sleeps"] += 1
        return advance(self, *args, **kwargs)

    def counting_event(self, *args, **kwargs):
        event = next_event(self, *args, **kwargs)
        counts["extra_broker_passes"] += event is not None
        return event

    monkeypatch.setattr(BacktestingBroker, "_advance_checking_resting_orders", counting_advance)
    monkeypatch.setattr(BacktestingBroker, "_next_between_run_event", counting_event)

    started = perf_counter()
    _, strategy = strategy_class.run_backtest(
        routed_backtesting.RoutedBacktestingPandas, START, END,
        config={"backtesting_data_routing": {"default": "ibkr", "stock": "ibkr"}},
        benchmark_asset=None, risk_free_rate=0, show_plot=False, show_tearsheet=False,
        show_indicators=False, save_tearsheet=False, show_progress_bar=False, quiet_logs=True,
    )
    seconds = perf_counter() - started
    trades = pd.read_csv(next((folder / "logs").glob("*_trades.csv")))
    trades = trades[trades["status"] == "fill"] if "status" in trades.columns else trades
    columns = [c for c in ("time", "symbol", "side", "status", "price", "filled_quantity") if c in trades.columns]
    monkeypatch.undo()
    return {"seconds": seconds, "trades": trades[columns].reset_index(drop=True),
            "final_value": float(strategy.get_portfolio_value()), **counts}


@pytest.mark.usefixtures("disable_datasource_override")
def test_market_only_strategy_is_identical_and_never_asks_the_broker_again(monkeypatch, tmp_path, record_property):
    frames, snapshot = _synthetic_stock()
    frames = _with_real_minute_bars(frames)
    before = _run(monkeypatch, tmp_path, "off", _FiveMinuteAverage, frames, snapshot, checks=False)
    after = _run(monkeypatch, tmp_path, "on", _FiveMinuteAverage, frames, snapshot, checks=True)

    record_property("months", MONTHS)
    record_property("seconds_without_checks", round(before["seconds"], 2))
    record_property("seconds_with_checks", round(after["seconds"], 2))
    print(f"\n[between-run benchmark, market only] months={MONTHS} trades={len(after['trades'])} "
          f"without={before['seconds']:.1f}s with={after['seconds']:.1f}s")

    assert len(after["trades"]) > 20
    pd.testing.assert_frame_equal(after["trades"], before["trades"])
    assert after["final_value"] == before["final_value"]
    assert after["sleeps"] > 1000
    assert after["extra_broker_passes"] == 0


@pytest.mark.usefixtures("disable_datasource_override")
def test_resting_brackets_ask_the_broker_only_on_bars_that_can_fill(monkeypatch, tmp_path, record_property):
    frames, snapshot = _synthetic_stock()
    frames = _with_real_minute_bars(frames)
    before = _run(monkeypatch, tmp_path, "off", _FiveMinuteBracket, frames, snapshot, checks=False)
    after = _run(monkeypatch, tmp_path, "on", _FiveMinuteBracket, frames, snapshot, checks=True)

    record_property("months", MONTHS)
    record_property("seconds_without_checks", round(before["seconds"], 2))
    record_property("seconds_with_checks", round(after["seconds"], 2))
    print(f"\n[between-run benchmark, brackets] months={MONTHS} fills={len(after['trades'])} "
          f"sleeps={after['sleeps']} extra_passes={after['extra_broker_passes']} "
          f"without={before['seconds']:.1f}s with={after['seconds']:.1f}s")

    fills = len(after["trades"])
    assert fills > 20
    assert after["sleeps"] > 1000
    # At most a couple of extra passes per fill (the fill bar, then the bar after it for
    # orders created by the fill). Per-bar checking would be four passes per sleep.
    assert 0 < after["extra_broker_passes"] <= 2 * fills
    assert after["extra_broker_passes"] < after["sleeps"] / 4
