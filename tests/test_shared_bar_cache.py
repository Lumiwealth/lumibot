"""Contract of the broker-agnostic shared bar cache (lumibot.tools.shared_bar_cache)."""

from __future__ import annotations

import threading
import time
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from lumibot.data_sources.exceptions import InvalidBars
from lumibot.tools import shared_bar_cache as sbc
from lumibot.tools.backtest_cache import BacktestCacheManager, BacktestCacheSettings, CacheMode


@pytest.fixture(autouse=True)
def isolated(tmp_path, monkeypatch):
    from lumibot import constants
    from lumibot.tools import backtest_cache, parquet_series_cache

    monkeypatch.setattr(constants, "LUMIBOT_CACHE_FOLDER", str(tmp_path / "local"))
    monkeypatch.setattr(backtest_cache, "LUMIBOT_CACHE_FOLDER", str(tmp_path / "local"))
    monkeypatch.setattr(parquet_series_cache, "get_backtest_cache", lambda: BacktestCacheManager(None))
    for name in ("LUMIBOT_SHARED_BARS", "LUMIBOT_SHARED_BARS_SCOPE", "LUMIBOT_SHARED_BARS_SETTLE_MINUTES"):
        monkeypatch.delenv(name, raising=False)
    sbc._Store._memory.clear()
    yield tmp_path


NOW = pd.Timestamp("2026-03-18T15:00:00Z")


def provider(scale=1.0, freq="1min", start="2026-01-01", end="2026-03-31"):
    index = pd.date_range(start, end, freq=freq, tz="UTC")
    close = (100 + np.arange(len(index)) % 50) * scale
    frame = pd.DataFrame(
        {"open": close, "high": close + 1, "low": close - 1, "close": close, "volume": 10.0}, index=index
    )
    calls = []

    def fetch(start, stop):
        calls.append((start, stop))
        return frame[(frame.index >= start) & (frame.index < stop) & (frame.index <= NOW)]

    return fetch, calls, frame


def series(**kwargs):
    defaults = dict(scope="connection-1", adjusted=False)
    defaults.update(kwargs)
    return sbc.BarSeries.make("synthetic", "SPY", defaults.pop("timestep", "1minute"), **defaults)


def test_settled_bars_are_stored_and_never_expire():
    fetch, calls, _ = provider()
    item = series()
    first = sbc.read_bars(item, "2026-03-02", "2026-03-03", fetch, now=NOW)
    later = sbc.read_bars(item, "2026-03-02", "2026-03-03", fetch, now=NOW + pd.Timedelta(days=400))
    assert len(calls) == 1
    pd.testing.assert_frame_equal(first, later)


def test_open_tail_is_fetched_every_time_and_never_stored():
    fetch, calls, _ = provider()
    item = series()
    sbc.read_bars(item, "2026-03-18T10:00Z", NOW, fetch, now=NOW)
    second = sbc.read_bars(item, "2026-03-18T10:00Z", NOW, fetch, now=NOW)
    assert len(calls) == 2
    assert calls[1][0] >= NOW - pd.Timedelta(minutes=18)  # settle margin + one bar + one bar back
    assert second.index[-1] == NOW
    stored = pd.concat(pd.read_parquet(p) for p in Path(sbc.constants.LUMIBOT_CACHE_FOLDER).rglob("*.parquet"))
    assert stored.index.max() < NOW - pd.Timedelta(minutes=15)


def test_partial_range_reads_only_the_missing_piece():
    fetch, calls, frame = provider()
    item = series()
    sbc.read_bars(item, "2026-03-10", "2026-03-12", fetch, now=NOW)
    result = sbc.read_bars(item, "2026-03-09", "2026-03-12", fetch, now=NOW)
    assert len(calls) == 2
    assert calls[1][1] <= pd.Timestamp("2026-03-10T00:01Z")
    expected = frame.loc["2026-03-09":"2026-03-12 00:00"]
    pd.testing.assert_frame_equal(result[["close"]], expected[["close"]], check_freq=False, check_names=False)


def test_empty_answer_is_never_remembered():
    item = series()
    calls = []

    def empty(start, stop):
        calls.append(1)
        return pd.DataFrame(columns=["open", "high", "low", "close", "volume"])

    for _ in range(2):
        assert sbc.read_bars(item, "2026-02-01", "2026-02-02", empty, now=NOW).empty
    assert len(calls) == 2


def test_invalid_provider_bars_raise_and_write_nothing(isolated):
    fetch, _, frame = provider()
    bad = frame.copy()
    bad.loc[pd.Timestamp("2026-03-02T10:05Z"), "high"] = 1.0
    with pytest.raises(InvalidBars):
        sbc.read_bars(
            series(), "2026-03-02", "2026-03-03", lambda a, b: bad[(bad.index >= a) & (bad.index < b)], now=NOW
        )
    assert not list(isolated.rglob("*.parquet"))


def test_provider_errors_propagate_and_write_nothing(isolated):
    def fail(start, stop):
        raise RuntimeError("provider down")

    with pytest.raises(RuntimeError, match="provider down"):
        sbc.read_bars(series(), "2026-03-02", "2026-03-03", fail, now=NOW)
    assert not list(isolated.rglob("*.parquet"))


def test_split_readjusted_history_is_detected_and_rebuilt():
    """Adjusted series: the provider rewrote old prices (2:1 split). Old bars must not mix in."""
    item = series(adjusted=True, timestep="1day")
    before, calls, _ = provider(freq="1D")
    sbc.read_bars(item, "2026-01-05", "2026-03-01", before, now=NOW)
    after, after_calls, after_frame = provider(scale=0.5, freq="1D")
    result = sbc.read_bars(item, "2026-01-05", "2026-03-15", after, now=NOW)
    expected = after_frame.loc["2026-01-05":"2026-03-15"]
    pd.testing.assert_frame_equal(result[["close"]], expected[["close"]], check_freq=False, check_names=False)
    # And the rebuilt series is served from cache afterwards.
    reads = len(after_calls)
    again = sbc.read_bars(item, "2026-01-05", "2026-03-15", after, now=NOW)
    assert len(after_calls) == reads
    pd.testing.assert_frame_equal(again, result)


def test_fully_cached_adjusted_series_is_rechecked_once_a_day():
    item = series(adjusted=True, timestep="1day")
    fetch, calls, _ = provider(freq="1D")
    sbc.read_bars(item, "2026-01-05", "2026-03-01", fetch, now=NOW)
    sbc.read_bars(item, "2026-01-05", "2026-03-01", fetch, now=NOW + pd.Timedelta(hours=2))
    assert len(calls) == 1
    sbc.read_bars(item, "2026-01-05", "2026-03-01", fetch, now=NOW + pd.Timedelta(hours=25))
    assert len(calls) == 2 and calls[1][0] >= pd.Timestamp("2026-02-25", tz="UTC")


def test_contract_and_bar_size_never_share_files():
    fetch, calls, _ = provider()
    sbc.read_bars(series(), "2026-03-02", "2026-03-03", fetch, now=NOW)
    sbc.read_bars(series(extendedHours=False), "2026-03-02", "2026-03-03", fetch, now=NOW)
    sbc.read_bars(series(timestep="5minute"), "2026-03-02", "2026-03-03", fetch, now=NOW)
    assert len(calls) == 3


def test_connections_share_only_when_configured(monkeypatch):
    fetch, calls, _ = provider()
    sbc.read_bars(series(scope="a"), "2026-03-02", "2026-03-03", fetch, now=NOW)
    sbc.read_bars(series(scope="b"), "2026-03-02", "2026-03-03", fetch, now=NOW)
    assert len(calls) == 2
    monkeypatch.setenv("LUMIBOT_SHARED_BARS_SCOPE", "provider")
    sbc.read_bars(series(scope="a"), "2026-03-04", "2026-03-05", fetch, now=NOW)
    sbc.read_bars(series(scope="b"), "2026-03-04", "2026-03-05", fetch, now=NOW)
    assert len(calls) == 3


def test_missing_scope_reads_directly_and_stores_nothing(isolated):
    fetch, calls, _ = provider()
    for _ in range(2):
        sbc.read_bars(series(scope=None), "2026-03-02", "2026-03-03", fetch, now=NOW)
    assert len(calls) == 2
    assert not list(isolated.rglob("*.parquet"))


def test_disabled_cache_reads_directly(monkeypatch):
    monkeypatch.setenv("LUMIBOT_SHARED_BARS", "false")
    fetch, calls, _ = provider()
    for _ in range(2):
        sbc.read_bars(series(), "2026-03-02", "2026-03-03", fetch, now=NOW)
    assert len(calls) == 2


def test_coverage_from_rows_does_not_claim_a_truncated_start():
    """Providers that can drop the start of an answer only cover what they returned."""
    item = series()
    full_fetch, _, frame = provider()
    calls = []

    def truncated(start, stop):
        calls.append((start, stop))
        rows = frame[(frame.index >= start) & (frame.index < stop)]
        return rows.iloc[len(rows) // 2 :] if len(calls) == 1 else rows

    sbc.read_bars(item, "2026-03-02", "2026-03-03", truncated, now=NOW, coverage_from_rows=True)
    result = sbc.read_bars(item, "2026-03-02", "2026-03-03", truncated, now=NOW, coverage_from_rows=True)
    assert len(calls) == 2
    expected = frame.loc["2026-03-02":"2026-03-03 00:00"]
    assert len(result) == len(expected)


class SlowRemote:
    """In-memory S3 whose uploads take a long time."""

    def __init__(self, delay=1.5):
        self.objects = {}
        self.delay = delay
        self.lock = threading.Lock()

    def get_object(self, Bucket, Key):  # noqa: N803 - boto3 signature
        with self.lock:
            if Key not in self.objects:
                raise FileNotFoundError(Key)
            body = self.objects[Key]

        class Body:
            def read(self_inner):
                return body

        return {"Body": Body()}

    def put_object(self, Bucket, Key, Body):  # noqa: N803
        time.sleep(self.delay)
        with self.lock:
            self.objects[Key] = Body


def remote_manager(monkeypatch, remote):
    from lumibot.tools import parquet_series_cache

    manager = BacktestCacheManager(
        BacktestCacheSettings(
            backend="s3",
            mode=CacheMode.S3_READWRITE,
            bucket="synthetic",
            prefix="env/cache",
            region="us-east-1",
            version="v44",
        ),
        client_factory=lambda _: remote,
    )
    monkeypatch.setattr(parquet_series_cache, "get_backtest_cache", lambda: manager)
    return manager


def test_remote_save_never_blocks_the_caller(monkeypatch):
    remote = SlowRemote(delay=1.5)
    remote_manager(monkeypatch, remote)
    fetch, _, _ = provider()
    started = time.perf_counter()
    result = sbc.read_bars(series(), "2026-03-02", "2026-03-03", fetch, now=NOW)
    elapsed = time.perf_counter() - started
    assert len(result) > 0
    assert elapsed < 1.0, f"caller waited {elapsed:.2f}s for the S3 upload"
    assert sbc.pending_shared_bar_writes() >= 1
    flushed = sbc.flush_shared_bar_writes(10)
    assert flushed["flushed"] and flushed["pending"] == 0
    assert any(key.startswith("env/cache/v44/bars/v2/synthetic/") for key in remote.objects)


def test_second_process_reads_remote_without_any_provider_call(monkeypatch, isolated):
    from lumibot import constants
    from lumibot.tools import backtest_cache

    remote = SlowRemote(delay=0)
    remote_manager(monkeypatch, remote)
    fetch, calls, _ = provider()
    first = sbc.read_bars(series(), "2026-03-02", "2026-03-04", fetch, now=NOW)
    sbc.flush_shared_bar_writes(10)
    # A new process: empty local folder, empty memory, same S3.
    monkeypatch.setattr(constants, "LUMIBOT_CACHE_FOLDER", str(isolated / "other-process"))
    monkeypatch.setattr(backtest_cache, "LUMIBOT_CACHE_FOLDER", str(isolated / "other-process"))
    sbc._Store._memory.clear()
    second = sbc.read_bars(series(), "2026-03-02", "2026-03-04", fetch, now=NOW)
    assert len(calls) == 1
    pd.testing.assert_frame_equal(first, second)
    assert second.attrs["historyCache"]["hit"] is True


def test_concurrent_remote_writers_keep_each_others_coverage(monkeypatch, isolated):
    from lumibot import constants
    from lumibot.tools import backtest_cache

    remote = SlowRemote(delay=0)
    remote_manager(monkeypatch, remote)
    fetch, calls, _ = provider()
    for name, (a, b) in {"p1": ("2026-03-02", "2026-03-03"), "p2": ("2026-03-05", "2026-03-06")}.items():
        monkeypatch.setattr(constants, "LUMIBOT_CACHE_FOLDER", str(isolated / name))
        monkeypatch.setattr(backtest_cache, "LUMIBOT_CACHE_FOLDER", str(isolated / name))
        sbc._Store._memory.clear()
        sbc.read_bars(series(), a, b, fetch, now=NOW)
        sbc.flush_shared_bar_writes(10)
    monkeypatch.setattr(constants, "LUMIBOT_CACHE_FOLDER", str(isolated / "p3"))
    monkeypatch.setattr(backtest_cache, "LUMIBOT_CACHE_FOLDER", str(isolated / "p3"))
    sbc._Store._memory.clear()
    reads = len(calls)
    sbc.read_bars(series(), "2026-03-02", "2026-03-03", fetch, now=NOW)
    sbc.read_bars(series(), "2026-03-05", "2026-03-06", fetch, now=NOW)
    assert len(calls) == reads


def test_remote_partitions_load_in_parallel(monkeypatch, isolated):
    """A hit across many partitions must not pay one S3 round trip after another."""
    from lumibot import constants
    from lumibot.tools import backtest_cache

    remote = SlowRemote(delay=0)
    remote_manager(monkeypatch, remote)
    fetch, _, _ = provider(freq="1h", start="2025-01-01", end="2026-03-01")
    sbc.read_bars(series(timestep="1hour"), "2025-01-01", "2026-02-27", fetch, now=NOW)
    sbc.flush_shared_bar_writes(10)
    monkeypatch.setattr(constants, "LUMIBOT_CACHE_FOLDER", str(isolated / "fresh"))
    monkeypatch.setattr(backtest_cache, "LUMIBOT_CACHE_FOLDER", str(isolated / "fresh"))
    sbc._Store._memory.clear()
    original_get = remote.get_object

    def slow_get(Bucket, Key):  # noqa: N803
        time.sleep(0.2)
        return original_get(Bucket, Key)

    remote.get_object = slow_get
    started = time.perf_counter()
    result = sbc.read_bars(series(timestep="1hour"), "2025-01-01", "2026-02-27", fetch, now=NOW)
    elapsed = time.perf_counter() - started
    assert result.attrs["historyCache"]["hit"] is True
    assert result.attrs["historyCache"]["partitions"] >= 14
    assert elapsed < 1.5, f"{elapsed:.2f}s: partitions were fetched one after another"
