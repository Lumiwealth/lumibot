"""Intraday stock backtest: frame metadata must not be rebuilt on every price lookup.

pandas deep-copies ``DataFrame.attrs`` whenever a column, row or slice is taken.  The
routed stock loader keeps the corporate-action ledger (one entry per dividend or split)
in ``attrs``, so every ``get_historical_prices`` / ``get_last_price`` call rebuilt that
ledger many times: about 70% of simulation time in a profiled 12-month run.

This benchmark runs the same synthetic 5-minute strategy twice, once with the ledger held
as a plain nested dict (the old behavior) and once shared, and requires identical trades
and final value.  Elapsed seconds are recorded as diagnostics only, never asserted.

Default window is 1 month so the suite stays fast.  Set ``LUMIBOT_SPEED_BENCH_MONTHS=12``
for the full-year measurement (2026-11-14, Apple M-series laptop, 12 months, 1,340 trades:
98.6 s before, 29.8 s after).
"""
import os
from datetime import datetime
from time import perf_counter

import numpy as np
import pandas as pd
import pandas_market_calendars as mcal
import pytest

from lumibot.constants import LUMIBOT_DEFAULT_PYTZ
from lumibot.entities import Asset
from lumibot.strategies import Strategy
from lumibot.tools.ibkr_equity_basis import EquityBasisSnapshot, decorate_adjusted_frame

MONTHS = int(os.environ.get("LUMIBOT_SPEED_BENCH_MONTHS", "1"))
START = datetime(2025, 1, 2)
END = (pd.Timestamp(START) + pd.DateOffset(months=MONTHS)).to_pydatetime()


def _session_index(schedule, freq):
    parts = [pd.date_range(row.market_open, row.market_close, freq=freq, inclusive="left")
             for row in schedule.itertuples()]
    return pd.DatetimeIndex(np.concatenate([part.values for part in parts]), tz="UTC").tz_convert(LUMIBOT_DEFAULT_PYTZ)


def _synthetic_stock():
    schedule = mcal.get_calendar("NYSE").schedule(pd.Timestamp(START) - pd.Timedelta(days=120), END)
    five = _session_index(schedule, "5min")
    close = 500 + np.cumsum(np.random.default_rng(7).normal(0, 0.25, len(five)))
    bars_5m = pd.DataFrame({"open": close - 0.05, "high": close + 0.3, "low": close - 0.3,
                            "close": close, "volume": 1000.0}, index=five)
    bars_1m = bars_5m.reindex(_session_index(schedule, "1min"), method="ffill")
    daily_close = bars_5m.close.groupby(bars_5m.index.date).last().to_numpy()
    days = pd.DatetimeIndex(schedule.market_close).tz_convert(LUMIBOT_DEFAULT_PYTZ)[:len(daily_close)]
    bars_1d = pd.DataFrame({"open": daily_close, "high": daily_close + 1, "low": daily_close - 1,
                            "close": daily_close, "volume": 1e6}, index=days)
    # Thirty years of quarterly dividends: the size of a real large-cap ETF ledger.
    dividend_days = pd.date_range("1993-03-19", "2024-12-20", freq="QS-MAR") + pd.Timedelta(days=18)
    actions = pd.DataFrame({"Dividends": 1.5, "Stock Splits": 0.0}, index=pd.DatetimeIndex(dividend_days))
    snapshot = EquityBasisSnapshot.from_verified_actions(
        actions, as_of=(pd.Timestamp(END, tz="UTC") + pd.Timedelta(days=2)).isoformat(),
        coverage_through=str(pd.Timestamp(END).date()))
    return {"day": bars_1d, "5minute": bars_5m, "minute": bars_1m}, snapshot


class _FiveMinuteAverage(Strategy):
    def initialize(self):
        self.sleeptime = "5M"
        self.vars.ledger_identities = set()

    def on_trading_iteration(self):
        asset = Asset("SPY")
        bars = self.get_historical_prices(asset, 21, "5 minutes")
        if bars is None or len(bars.df) < 21:
            return
        self.vars.ledger_identities.add(id(bars.df.attrs.get("ibkr_equity_basis")))
        close = bars.df["close"]
        fast, slow = float(close.iloc[-5:].mean()), float(close.mean())
        position = self.get_position(asset)
        self.get_last_price(asset)
        if fast > slow * 1.0005 and position is None:
            self.submit_order(self.create_order(asset, 10, "buy"))
        elif fast < slow * 0.9995 and position is not None:
            self.sell_all()


def _run(monkeypatch, tmp_path, name, frames, snapshot, *, shared):
    from lumibot.backtesting import interactive_brokers_rest_backtesting, routed_backtesting
    from lumibot.tools import ibkr_helper

    folder = tmp_path / name
    folder.mkdir()
    monkeypatch.chdir(folder)
    if not shared:
        monkeypatch.setattr(routed_backtesting, "share_frame_attrs", lambda frame: frame)
        monkeypatch.setattr(interactive_brokers_rest_backtesting, "share_frame_attrs", lambda frame: frame)

    def prices(**kwargs):
        frame = frames[str(kwargs["timestep"])]
        window = frame.loc[(frame.index >= kwargs["start_dt"]) & (frame.index <= kwargs["end_dt"])].copy()
        return decorate_adjusted_frame(window, snapshot, provider_prices_verified=True)

    monkeypatch.setattr(ibkr_helper, "get_price_data", prices)
    monkeypatch.setattr(ibkr_helper, "_get_cached_equity_actions", lambda *a, **k: snapshot.actions_frame())

    started = perf_counter()
    _, strategy = _FiveMinuteAverage.run_backtest(
        routed_backtesting.RoutedBacktestingPandas, START, END,
        config={"backtesting_data_routing": {"default": "ibkr", "stock": "ibkr"}},
        benchmark_asset=None, risk_free_rate=0, show_plot=False, show_tearsheet=False,
        show_indicators=False, save_tearsheet=False, show_progress_bar=False, quiet_logs=True,
    )
    seconds = perf_counter() - started
    trades = pd.read_csv(next((folder / "logs").glob("*_trades.csv")))
    columns = [c for c in ("time", "symbol", "side", "status", "price", "filled_quantity") if c in trades.columns]
    return {"seconds": seconds, "trades": trades[columns].reset_index(drop=True),
            "final_value": float(strategy.get_portfolio_value()),
            "ledger_objects": len(strategy.vars.ledger_identities)}


@pytest.mark.usefixtures("disable_datasource_override")
def test_shared_ledger_metadata_gives_identical_backtest_without_rebuilding_it(monkeypatch, tmp_path, record_property):
    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "test-key")
    monkeypatch.setenv("LUMIBOT_CACHE_BACKEND", "local")
    frames, snapshot = _synthetic_stock()

    before = _run(monkeypatch, tmp_path, "plain", frames, snapshot, shared=False)
    monkeypatch.undo()
    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "test-key")
    monkeypatch.setenv("LUMIBOT_CACHE_BACKEND", "local")
    after = _run(monkeypatch, tmp_path, "shared", frames, snapshot, shared=True)

    record_property("months", MONTHS)
    record_property("seconds_before", round(before["seconds"], 2))
    record_property("seconds_after", round(after["seconds"], 2))
    print(f"\n[intraday metadata benchmark] months={MONTHS} trades={len(after['trades'])} "
          f"before={before['seconds']:.1f}s after={after['seconds']:.1f}s")

    assert len(after["trades"]) > 20
    pd.testing.assert_frame_equal(after["trades"], before["trades"])
    assert after["final_value"] == before["final_value"]
    # Plain metadata is rebuilt for every returned window; shared metadata is one object.
    assert before["ledger_objects"] > 100
    assert after["ledger_objects"] == 1
