"""Binance perpetual funding and mark lookups return the same answers without a full scan per bar.

Why: a BTC perpetual backtest on 5-minute bars asks for funding events and the mark price on
every simulated bar. Each funding call normalized every funding row of the whole window and
scanned them; each mark call built a boolean mask over every hourly mark. On a three-month
run that was about 79 of 454 profiled seconds.

Invariant under test: for any window, funding events are the same rows in the same order (fresh
dicts, so a caller changing one cannot change a later answer) and the mark is the same value
with the same exactness flag, compared against the original whole-scan logic. Counts, never
seconds.
"""

import random
from decimal import Decimal
from types import SimpleNamespace

import pandas as pd

from lumibot.backtesting.routed_backtesting import _BinanceBulkRoutingAdapter
from lumibot.data_sources.data_source_backtesting import DataSourceBacktesting


def _adapter(rows, marks):
    adapter = _BinanceBulkRoutingAdapter.__new__(_BinanceBulkRoutingAdapter)
    adapter._funding = {"BTCUSDT": rows}
    adapter._marks = {"BTCUSDT": marks}
    adapter.resolve = lambda asset, quote=None: ("um", "BTCUSDT", None)
    adapter._router = SimpleNamespace(datetime_start=pd.Timestamp("2026-08-01", tz="UTC"),
                                      datetime_end=pd.Timestamp("2026-11-01", tz="UTC"))
    return adapter


def _fixture():
    start = pd.Timestamp("2026-08-01", tz="UTC")
    rows = [{"timestamp": int((start + pd.Timedelta(hours=8 * i)).timestamp() * 1000),
             "fundingRate": 0.0001 * ((i % 5) - 2), "markPrice": 60000.0 + i} for i in range(276)]
    index = pd.date_range(start, periods=24 * 92, freq="1h", tz="UTC")
    marks = pd.DataFrame({"open": [60000.0 + 0.5 * i for i in range(len(index))]}, index=index)
    return rows, marks


def _old_funding(rows, start, end):
    return [row for row in DataSourceBacktesting._perpetual_rows(rows) if start <= row["timestamp"] <= end]


def _old_mark(marks, at):
    at_ts = pd.Timestamp(at)
    at_ts = at_ts.tz_localize("UTC") if at_ts.tzinfo is None else at_ts.tz_convert("UTC")
    visible = marks[marks.index <= at_ts]
    if visible.empty:
        return None, False
    return Decimal(str(visible.iloc[-1]["open"])), visible.index[-1] == at_ts


def test_funding_and_mark_match_the_whole_scan_for_random_bars():
    rows, marks = _fixture()
    adapter = _adapter(rows, marks)
    rng = random.Random(7)
    base = pd.Timestamp("2026-07-30", tz="UTC")
    for _ in range(600):
        start = base + pd.Timedelta(minutes=5 * rng.randrange(0, 30000))
        end = start + pd.Timedelta(minutes=rng.choice([0, 5, 60, 480, 1440, 20000]))
        assert adapter.funding_events(None, start, end) == _old_funding(rows, start, end)
        at = start + pd.Timedelta(minutes=rng.choice([0, 1, 59, 60]))
        assert adapter.mark_at(None, at) == _old_mark(marks, at)
    assert adapter.mark_at(None, pd.Timestamp("2026-07-01", tz="UTC")) == (None, False)


def test_returned_funding_rows_are_fresh_dicts():
    rows, marks = _fixture()
    adapter = _adapter(rows, marks)
    window = (pd.Timestamp("2026-08-02", tz="UTC"), pd.Timestamp("2026-08-03", tz="UTC"))
    first = adapter.funding_events(None, *window)
    first[0]["fundingRate"] = 99.0
    assert adapter.funding_events(None, *window) == _old_funding(rows, *window)


def test_normalizes_the_rows_once_per_loaded_list(monkeypatch):
    rows, marks = _fixture()
    adapter = _adapter(rows, marks)
    calls = {"n": 0}
    original = DataSourceBacktesting._perpetual_rows

    def counting(records):
        calls["n"] += 1
        return original(records)

    monkeypatch.setattr(DataSourceBacktesting, "_perpetual_rows", staticmethod(counting))
    for day in range(1, 20):
        stamp = pd.Timestamp(f"2026-08-{day:02d}", tz="UTC")
        adapter.funding_events(None, stamp, stamp + pd.Timedelta(hours=1))
    assert calls["n"] == 1
