"""Confirmed-empty answers in the shared bar cache.

A provider that answered successfully and explicitly with zero bars for a fully closed past
range is remembered as "no bars" for that range, so the next run asks nothing. Only that is
remembered. An error, timeout, rate limit, partial or truncated answer, an answer the caller
cannot vouch for (for example a contract the provider never listed), and anything less than one
full day after the range closed are never remembered and are asked again next run. A marker
holds no rows, never reaches a strategy as a bar, and is stored apart from bar coverage.
Markers for recent ranges are re-checked once after a few days so a provider backfill appears.
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from lumibot.tools import shared_bar_cache as sbc
from lumibot.tools.backtest_cache import BacktestCacheManager

NOW = pd.Timestamp("2026-03-18T15:00:00Z")
COLUMNS = ["open", "high", "low", "close", "volume"]


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    from lumibot import constants
    from lumibot.tools import backtest_cache, parquet_series_cache

    monkeypatch.setattr(constants, "LUMIBOT_CACHE_FOLDER", str(tmp_path / "local"))
    monkeypatch.setattr(backtest_cache, "LUMIBOT_CACHE_FOLDER", str(tmp_path / "local"))
    monkeypatch.setattr(parquet_series_cache, "get_backtest_cache", lambda: BacktestCacheManager(None))
    for name in ("LUMIBOT_SHARED_BARS", "LUMIBOT_SHARED_BARS_SCOPE", "LUMIBOT_SHARED_BARS_SETTLE_MINUTES"):
        monkeypatch.delenv(name, raising=False)
    sbc._reset_process_memory()
    yield tmp_path


def series(symbol="SPY260320P00600000"):
    return sbc.BarSeries.make("synthetic", symbol, "1minute", scope="connection-1", adjusted=False)


def counted(answer):
    calls = []

    def fetch(start, stop):
        calls.append((start, stop))
        return answer(start, stop)

    return fetch, calls


def empty(_start, _stop):
    return pd.DataFrame(columns=COLUMNS)


def vouched(_item):
    return True


def read(item, start, end, fetch, now=NOW, **kwargs):
    kwargs.setdefault("confirmed_empty", vouched)
    return sbc.read_bars(item, start, end, fetch, now=now, **kwargs)


def test_settled_true_empty_is_remembered_and_costs_nothing_next_run():
    item = series()
    fetch, calls = counted(empty)
    assert read(item, "2026-02-02", "2026-02-03", fetch).empty
    sbc._reset_process_memory()  # a new process reads the files
    assert read(item, "2026-02-02", "2026-02-03", fetch, now=NOW + pd.Timedelta(days=1)).empty
    assert len(calls) == 1


@pytest.mark.parametrize("failure", [
    TimeoutError("read timed out"),
    ConnectionError("connection reset"),
    RuntimeError("HTTP 429 Too Many Requests"),
    RuntimeError("HTTP 500 Internal Server Error"),
    PermissionError("HTTP 403 not entitled to this feed"),
    RuntimeError("HTTP 401 unauthorized"),
    RuntimeError("pacing violation"),
])
def test_a_failed_answer_is_never_remembered_and_is_asked_again(failure):
    item = series()
    calls = []

    def failing(start, stop):
        calls.append(1)
        raise failure

    for _ in range(2):
        with pytest.raises(type(failure)):
            read(item, "2026-02-02", "2026-02-03", failing)
    assert len(calls) == 2
    fetch, ok_calls = counted(empty)
    read(item, "2026-02-02", "2026-02-03", fetch)
    assert len(ok_calls) == 1, "an earlier failure must not have been stored as empty"


def test_an_answer_the_caller_cannot_vouch_for_is_never_remembered():
    # For example a contract the provider never listed: an empty answer may mean a wrong symbol.
    item = series("SPY260320P99900000")
    fetch, calls = counted(empty)
    for _ in range(2):
        read(item, "2026-02-02", "2026-02-03", fetch, confirmed_empty=lambda _s: False)
    assert len(calls) == 2


def test_without_the_opt_in_an_empty_answer_is_still_never_remembered():
    item = series()
    fetch, calls = counted(empty)
    for _ in range(2):
        sbc.read_bars(item, "2026-02-02", "2026-02-03", fetch, now=NOW)
    assert len(calls) == 2


def test_a_provider_that_can_truncate_answers_never_records_empty():
    item = series()
    fetch, calls = counted(empty)
    for _ in range(2):
        read(item, "2026-02-02", "2026-02-03", fetch, coverage_from_rows=True)
    assert len(calls) == 2


def test_today_and_the_last_full_day_are_never_remembered_as_empty():
    item = series()
    fetch, calls = counted(empty)
    # The range closed 20 hours ago: less than one full day, so a revision may still come.
    start, end = NOW - pd.Timedelta(hours=30), NOW - pd.Timedelta(hours=20)
    for _ in range(2):
        read(item, start, end, fetch)
    assert len(calls) == 2
    # Today's open range is never remembered either.
    fetch, calls = counted(empty)
    for _ in range(2):
        read(item, NOW.normalize(), NOW, fetch)
    assert len(calls) == 2


def test_only_the_part_older_than_one_day_is_remembered():
    item = series()
    fetch, calls = counted(empty)
    start, end = NOW - pd.Timedelta(days=3), NOW - pd.Timedelta(hours=2)
    read(item, start, end, fetch)
    read(item, start, end, fetch)
    assert len(calls) == 2
    assert calls[1][0] >= NOW - pd.Timedelta(days=1, minutes=5), calls[1]


def test_a_partial_answer_records_only_bars_never_an_empty_gap():
    item = series()
    index = pd.date_range("2026-02-02T14:30Z", periods=30, freq="1min")
    frame = pd.DataFrame({"open": 1.0, "high": 1.1, "low": 0.9, "close": 1.0, "volume": 3.0}, index=index)
    fetch, calls = counted(lambda a, b: frame[(frame.index >= a) & (frame.index < b)])
    result = read(item, "2026-02-02", "2026-02-03", fetch)
    assert len(result) == 30
    meta = _stored_meta()
    assert all(not m.get("confirmedEmpty") for m in meta), "a non-empty answer must not write empty markers"


def test_a_marker_never_surfaces_as_a_bar_and_is_stored_apart_from_coverage():
    item = series()
    fetch, _ = counted(empty)
    read(item, "2026-02-02", "2026-02-03", fetch)
    sbc._reset_process_memory()
    again = read(item, "2026-02-02", "2026-02-03", fetch, now=NOW + pd.Timedelta(days=1))
    assert again.empty and list(again.columns) == COLUMNS
    files = list(Path(sbc.constants.LUMIBOT_CACHE_FOLDER).rglob("*.parquet"))
    assert files and all(pd.read_parquet(f).empty for f in files)
    markers = [m for meta in _stored_meta() for m in meta.get("confirmedEmpty") or []]
    assert markers and all(meta.get("coverage") == [] for meta in _stored_meta())
    marker = markers[0]
    assert marker["provider"] == "synthetic" and marker["symbol"] == item.symbol
    assert marker["timestep"] == "1minute" and marker["reason"] == "provider_success_zero_rows"
    assert pd.Timestamp(marker["recordedAt"]) == NOW
    assert pd.Timestamp(marker["start"]) < pd.Timestamp(marker["endExclusive"])


def test_a_recent_marker_is_rechecked_once_so_a_backfill_appears():
    item = series()
    start, end = NOW - pd.Timedelta(days=3), NOW - pd.Timedelta(days=2)
    fetch, calls = counted(empty)
    read(item, start, end, fetch)
    read(item, start, end, fetch, now=NOW + pd.Timedelta(days=1))
    assert len(calls) == 1
    index = pd.date_range(start + pd.Timedelta(hours=15), periods=5, freq="1min")
    backfill = pd.DataFrame({"open": 2.0, "high": 2.1, "low": 1.9, "close": 2.0, "volume": 1.0}, index=index)
    fetch, calls = counted(lambda a, b: backfill[(backfill.index >= a) & (backfill.index < b)])
    later = NOW + sbc.EMPTY_RECHECK_AFTER + pd.Timedelta(hours=1)
    result = read(item, start, end, fetch, now=later)
    assert len(calls) == 1 and len(result) == 5
    read(item, start, end, fetch, now=later)
    assert len(calls) == 1, "after the backfill the bars are cached as usual"


def test_old_history_markers_are_permanent():
    item = series()
    fetch, calls = counted(empty)
    read(item, "2025-06-02", "2025-06-03", fetch)
    read(item, "2025-06-02", "2025-06-03", fetch, now=NOW + pd.Timedelta(days=400))
    assert len(calls) == 1


def test_markers_never_cross_series():
    fetch, calls = counted(empty)
    read(series("SPY260320P00600000"), "2026-02-02", "2026-02-03", fetch)
    read(series("SPY260320P00605000"), "2026-02-02", "2026-02-03", fetch)
    assert len(calls) == 2


def _stored_meta():
    out = []
    for path in Path(sbc.constants.LUMIBOT_CACHE_FOLDER).rglob("*.parquet"):
        import pyarrow.parquet as pq

        stored = (pq.read_schema(path).metadata or {}).get(b"PANDAS_ATTRS")
        out.append(json.loads(stored)["sharedBars"] if stored else {})
    return out
