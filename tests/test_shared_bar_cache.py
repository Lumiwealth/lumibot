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
    sbc._reset_process_memory()
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


def test_invalid_provider_bars_raise_and_write_nothing_for_strict_adapters(isolated):
    fetch, _, frame = provider()
    bad = frame.copy()
    bad.loc[pd.Timestamp("2026-03-02T10:05Z"), "high"] = 1.0
    with pytest.raises(InvalidBars):
        sbc.read_bars(
            series(),
            "2026-03-02",
            "2026-03-03",
            lambda a, b: bad[(bad.index >= a) & (bad.index < b)],
            now=NOW,
            strict_ohlcv=True,
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
    sbc._reset_process_memory()
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
        sbc._reset_process_memory()
        sbc.read_bars(series(), a, b, fetch, now=NOW)
        sbc.flush_shared_bar_writes(10)
    monkeypatch.setattr(constants, "LUMIBOT_CACHE_FOLDER", str(isolated / "p3"))
    monkeypatch.setattr(backtest_cache, "LUMIBOT_CACHE_FOLDER", str(isolated / "p3"))
    sbc._reset_process_memory()
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
    sbc._reset_process_memory()
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


def test_provider_quirks_pass_through_unchanged_and_are_cached():
    """Real broker rows the old path returned (Tradier SPY 2024-12-19: close below low) must still return.

    Only adapters that opt into ``strict_ohlcv`` (Alpaca, its existing contract) reject such rows.
    """
    fetch, calls, frame = provider(freq="1D")
    quirky = frame.copy()
    day = pd.Timestamp("2026-02-10", tz="UTC")
    quirky.loc[day, ["open", "high", "low", "close"]] = [591.36, 593.0, 585.85, 584.134452]

    def read(start, stop):
        calls.append((start, stop))
        return quirky[(quirky.index >= start) & (quirky.index < stop)]

    item = series(timestep="1day")
    first = sbc.read_bars(item, "2026-02-01", "2026-02-28", read, now=NOW)
    second = sbc.read_bars(item, "2026-02-01", "2026-02-28", read, now=NOW)
    assert first.loc[day, "close"] == 584.134452 and first.loc[day, "low"] == 585.85
    pd.testing.assert_frame_equal(first, second)
    assert len(calls) == 1
    with pytest.raises(InvalidBars):
        sbc.read_bars(series(timestep="1day", scope="strict"), "2026-02-01", "2026-02-28", read, now=NOW,
                      strict_ohlcv=True)


def test_slow_local_write_never_blocks_the_caller(monkeypatch):
    """Workers with a remote store (one CPU on Lambda) must not wait on the Parquet write either."""
    remote_manager(monkeypatch, SlowRemote(delay=0))
    original = sbc._encode_partition

    def slow(part):
        time.sleep(1.5)
        return original(part)

    monkeypatch.setattr(sbc, "_encode_partition", slow)
    fetch, calls, _ = provider()
    started = time.perf_counter()
    result = sbc.read_bars(series(), "2026-03-02", "2026-03-03", fetch, now=NOW)
    assert len(result) > 0
    assert time.perf_counter() - started < 1.0, "caller waited for the local partition write"
    assert sbc.flush_shared_bar_writes(10)["flushed"]
    # Once written, the same process and a fresh process both reuse it.
    again = sbc.read_bars(series(), "2026-03-02", "2026-03-03", fetch, now=NOW)
    pd.testing.assert_frame_equal(result, again)
    assert len(calls) == 1


def test_prewarm_is_safe_to_call_repeatedly():
    sbc.prewarm()
    sbc.prewarm()


def test_shared_scope_names_only_the_provider_and_its_data_tier():
    assert sbc.shared_scope("schwab") == sbc.shared_scope("schwab", None, "")
    assert sbc.shared_scope("tradier", "paper") != sbc.shared_scope("tradier", "live")
    assert sbc.shared_scope("alpaca") != sbc.shared_scope("schwab")
    # Never collides with a per-connection scope of the same words.
    assert sbc.shared_scope("ccxt", "kraken") != sbc.credential_scope("ccxt", "kraken")


def test_scope_needs_an_identity_beyond_the_provider_name():
    assert sbc.credential_scope("ibkr", None) is None
    assert sbc.credential_scope("ibkr", "") is None
    assert sbc.credential_scope("ibkr", "DU1") != sbc.credential_scope("ibkr", "DU2")
    assert sbc.credential_scope("ibkr", "DU1") != sbc.credential_scope("tradier", "DU1")


def test_uploader_stops_trying_the_remote_store_after_repeated_failures(caplog, monkeypatch):
    """A store that keeps failing must not hold a finished process open.

    A 2.5-year option backtest finished in 15 minutes and then spent 40 more minutes failing
    962 queued uploads one by one before the process could exit (production, 2026-11-05).
    The local write of each partition still happens; only the remote step is given up.
    """
    import logging

    # One worker, so exactly the first ten jobs reach the remote store (with several
    # workers a few more can already be in flight when the limit trips).
    monkeypatch.setenv("LUMIBOT_SHARED_BARS_UPLOAD_WORKERS", "1")
    uploader = sbc._Uploader()
    local, remote = [], []

    def job(index):
        def run(remote_allowed):
            local.append(index)
            if remote_allowed:
                remote.append(index)
                raise RuntimeError("store unavailable for this run")
        return run

    with caplog.at_level(logging.WARNING, logger=sbc.logger.name):
        for index in range(200):
            uploader.submit(f"key-{index}", job(index))
        assert uploader.flush(30)

    assert sorted(local) == list(range(200))
    assert len(remote) == sbc.UPLOAD_FAILURES_BEFORE_GIVING_UP
    messages = [record.getMessage() for record in caplog.records]
    assert any("store unavailable for this run" in message for message in messages)  # the reason is visible
    assert sum("giving up" in message for message in messages) == 1
    failures = [record for record in caplog.records if "upload failed" in record.getMessage()]
    # Only the first failure carries the traceback, so the failing call is visible once.
    assert failures[0].exc_info
    assert not any(record.exc_info for record in failures[1:])


def test_uploader_keeps_using_the_remote_store_when_failures_are_isolated(monkeypatch):
    monkeypatch.setenv("LUMIBOT_SHARED_BARS_UPLOAD_WORKERS", "1")
    uploader = sbc._Uploader()
    remote = []

    def job(index):
        def run(remote_allowed):
            if remote_allowed:
                remote.append(index)
                if index % 3 == 0:
                    raise OSError("one bad object")
        return run

    for index in range(60):
        uploader.submit(f"key-{index}", job(index))
    assert uploader.flush(30)
    assert sorted(remote) == list(range(60))


def test_partition_encoding_works_after_interpreter_shutdown_starts(monkeypatch):
    """Uploads that run after the main thread returns must still encode their partition.

    pandas' to_parquet converts columns on a concurrent.futures thread pool. Once Python
    starts shutting down (the main thread returned, the non-daemon uploader keeps going),
    that pool refuses work with "RuntimeError: cannot schedule new futures after interpreter
    shutdown", so every queued upload failed in production from the end of the simulation
    on (2026-11-11). Encoding must not need a new thread.
    """
    import concurrent.futures.thread as cf_thread

    import pandas as pd

    # Enough rows that pyarrow converts columns in parallel (rows > 100 x columns).
    index = pd.date_range("2026-08-03T13:30:00Z", periods=1000, freq="min", name="timestamp")
    rows = pd.DataFrame(
        {"open": 1.0, "high": 1.5, "low": 0.5, "close": 1.2, "volume": 10.0}, index=index,
    )
    series = sbc.BarSeries.make("alpaca", "SPY", "1minute", scope="scope")
    start = pd.Timestamp("2026-08-03T00:00:00Z")
    stop = pd.Timestamp("2026-08-04T00:00:00Z")
    part = sbc._Partition(series, start, stop, None, rows, [(start, stop)], 1, True)
    expected = sbc._encode_partition(part)

    monkeypatch.setattr(cf_thread, "_shutdown", True)
    encoded = sbc._encode_partition(part)

    assert encoded == expected
    decoded = sbc._decode_partition(sbc._Partition(series, start, stop, None), encoded)
    assert decoded.rows.equals(sbc._normalize(rows))


def test_uploader_runs_jobs_in_parallel_but_keeps_one_path_in_order(monkeypatch):
    """A finished backtest waits for its queued uploads; one at a time that was minutes.

    Jobs for different paths run on several workers. Jobs for the same path always run on
    the same worker, one after another, in the order they were submitted.
    """
    import threading
    import time

    monkeypatch.setenv("LUMIBOT_SHARED_BARS_UPLOAD_WORKERS", "8")
    uploader = sbc._Uploader()
    running, seen, lock = {}, {}, threading.Lock()

    def job(key, index):
        def run(remote_allowed):
            with lock:
                assert running.get(key, 0) == 0, "two jobs for one path ran at the same time"
                running[key] = 1
            time.sleep(0.05)
            with lock:
                running[key] = 0
                seen.setdefault(key, []).append(index)
        return run

    started = time.monotonic()
    for index in range(10):
        for key in ("a", "b", "c", "d", "e", "f", "g", "h"):
            uploader.submit(key, job(key, index))
    assert uploader.flush(30)
    elapsed = time.monotonic() - started

    assert all(seen[key] == list(range(10)) for key in seen)
    assert elapsed < 2.0  # 80 jobs x 0.05 s took 4 s on one worker


class CountingRemote(SlowRemote):
    def __init__(self):
        super().__init__(delay=0)
        self.gets = []

    def get_object(self, Bucket, Key):  # noqa: N803
        self.gets.append(Key)
        return super().get_object(Bucket, Key)


def test_unadjusted_series_never_look_up_a_manifest(monkeypatch, isolated):
    """Only adjusted series ever write series.json, so an unadjusted read never asks S3 for one.

    On a fresh cloud container every option contract cost one extra S3 request that always
    missed (1,712 of 3,421 requests on a repeat one-month iron condor).
    """
    remote = CountingRemote()
    remote_manager(monkeypatch, remote)
    fetch, calls, _ = provider()
    first = sbc.read_bars(series(adjusted=False), "2026-03-02", "2026-03-04", fetch, now=NOW)
    sbc.flush_shared_bar_writes(10)
    assert not any(key.endswith("series.json") for key in remote.objects)
    remote.gets.clear()
    sbc._reset_process_memory()
    second = sbc.read_bars(series(adjusted=False), "2026-03-02", "2026-03-04", fetch, now=NOW)
    pd.testing.assert_frame_equal(first, second)
    assert len(calls) == 1
    assert [k for k in remote.gets if k.endswith("series.json")] == []


def test_adjusted_series_still_read_their_manifest(monkeypatch, isolated):
    remote = CountingRemote()
    remote_manager(monkeypatch, remote)
    fetch, _, _ = provider()
    sbc.read_bars(series(adjusted=True), "2026-03-02", "2026-03-04", fetch, now=NOW)
    sbc.flush_shared_bar_writes(10)
    remote.gets.clear()
    sbc._reset_process_memory()
    sbc.read_bars(series(adjusted=True), "2026-03-02", "2026-03-04", fetch, now=NOW)
    assert any(k.endswith("series.json") for k in remote.gets)


def test_run_counters_report_remote_misses_uploads_and_hits(monkeypatch, isolated, caplog):
    """The shared bar cache reads and writes S3 itself, outside BacktestCacheManager's counters.

    Production showed downloads=0 misses=0 uploads=0 for every Alpaca run while repeats were
    90 s faster, because nothing counted these reads. The run counters must show them.
    """
    import logging

    from lumibot import constants
    from lumibot.tools import backtest_cache

    remote = SlowRemote(delay=0)
    remote_manager(monkeypatch, remote)
    sbc.reset_run_stats()
    fetch, calls, _ = provider()
    sbc.read_bars(series(), "2026-03-02", "2026-03-04", fetch, now=NOW)
    sbc.flush_shared_bar_writes(10)
    cold = sbc.run_stats_snapshot()
    assert cold["provider_reads"] == 1
    assert cold["remote_misses"] >= 1 and cold["remote_hits"] == 0
    assert cold["uploads"] >= 1 and cold["upload_bytes"] > 0

    monkeypatch.setattr(constants, "LUMIBOT_CACHE_FOLDER", str(isolated / "other-process"))
    monkeypatch.setattr(backtest_cache, "LUMIBOT_CACHE_FOLDER", str(isolated / "other-process"))
    sbc._reset_process_memory()
    sbc.reset_run_stats()
    sbc.read_bars(series(), "2026-03-02", "2026-03-04", fetch, now=NOW)
    warm = sbc.run_stats_snapshot()
    assert len(calls) == 1
    assert warm["provider_reads"] == 0
    assert warm["remote_hits"] >= 1 and warm["download_bytes"] > 0
    assert warm["uploads"] == 0

    with caplog.at_level(logging.INFO, logger=sbc.logger.name):
        sbc.log_run_summary()
    line = next(r.getMessage() for r in caplog.records if "[SHARED_BARS][SUMMARY]" in r.getMessage())
    assert "provider_reads=0" in line and "remote_hits=" in line


# --------------------------------------------------------------------------- local disk before S3
#
# Hosted Alpaca options backtests made 2,000 to 4,100 S3 GETs for 35 to 80 contracts: every
# read asked S3 first even when this process had already downloaded the same partition to
# local disk, so each file was downloaded 10 to 25 times (2026-11-13 cold-start breakdown).


def _option_series(index):
    # Same identity shape the Alpaca history path builds (one provider-wide shared scope).
    return sbc.BarSeries.make(
        "alpaca", f"SYN260320C{index:08d}", "1minute", scope=sbc.shared_scope("alpaca"), adjusted=False,
        requestType="OptionBarsRequest", timeframe="1Min",
    )


def _use_local_folder(monkeypatch, folder):
    from lumibot import constants
    from lumibot.tools import backtest_cache

    monkeypatch.setattr(constants, "LUMIBOT_CACHE_FOLDER", str(folder))
    monkeypatch.setattr(backtest_cache, "LUMIBOT_CACHE_FOLDER", str(folder))


def _warm_remote_then_new_process(monkeypatch, isolated, contracts):
    """Fill S3 from one process, then start a second process with an empty disk and memory."""
    remote = CountingRemote()
    remote_manager(monkeypatch, remote)
    fetch, calls, _ = provider()

    def fetch_many(members, start, stop):
        return {item: fetch(start, stop) for item in members}

    sbc.read_bars_many(contracts, "2026-03-02", "2026-03-03", fetch_many, now=NOW)
    assert sbc.flush_shared_bar_writes(30)["pending"] == 0
    _use_local_folder(monkeypatch, isolated / "fresh-container")
    sbc._reset_process_memory()
    remote.gets.clear()
    calls.clear()
    return remote, fetch_many, calls


def test_partition_already_downloaded_is_read_from_disk_not_downloaded_again(monkeypatch, isolated):
    from collections import Counter

    contracts = [_option_series(i) for i in range(40)]
    remote, fetch_many, calls = _warm_remote_then_new_process(monkeypatch, isolated, contracts)
    first = {}
    for _ in range(20):
        for item in contracts:
            frame = sbc.read_bars_many([item], "2026-03-02", "2026-03-03", fetch_many, now=NOW)[item]
            assert not frame.empty
            if item in first:
                pd.testing.assert_frame_equal(frame, first[item], check_freq=False)
            first[item] = frame
    per_key = Counter(remote.gets)
    assert calls == []  # every answer came from the cache, never the provider
    assert len(per_key) == 40
    assert max(per_key.values()) == 1, (
        f"{len(remote.gets)} S3 GETs for 40 partitions; worst file downloaded {max(per_key.values())} times"
    )


def test_local_copy_that_lacks_the_requested_range_still_asks_s3(monkeypatch, isolated):
    """Disk first never hides newer coverage another process published to S3."""
    contracts = [_option_series(0)]
    remote, fetch_many, calls = _warm_remote_then_new_process(monkeypatch, isolated, contracts)
    sbc.read_bars_many(contracts, "2026-03-02", "2026-03-03", fetch_many, now=NOW)
    assert len(remote.gets) == 1
    # Another process extends the same S3 partition (same month, later days).
    _use_local_folder(monkeypatch, isolated / "other-process")
    sbc.read_bars_many(contracts, "2026-03-02", "2026-03-06", fetch_many, now=NOW)
    assert sbc.flush_shared_bar_writes(30)["pending"] == 0
    provider_calls = len(calls)
    assert provider_calls >= 1
    # Back in this process: its disk copy covers only Mar 2-3, so it must ask S3 again
    # and get the newer coverage there instead of calling the provider.
    _use_local_folder(monkeypatch, isolated / "fresh-container")
    remote.gets.clear()
    frame = sbc.read_bars_many(contracts, "2026-03-02", "2026-03-06", fetch_many, now=NOW)[contracts[0]]
    assert len(remote.gets) == 1
    assert len(calls) == provider_calls
    assert frame.index.max() >= pd.Timestamp("2026-03-05T23:00:00Z")


def test_memory_layer_is_bounded_by_bytes_not_a_count_of_64(monkeypatch, isolated):
    fetch, _, _ = provider()
    items = [sbc.BarSeries.make("synthetic", f"SYM{i}", "1minute", scope="c", adjusted=False) for i in range(100)]
    for item in items:
        sbc.read_bars(item, "2026-03-02", "2026-03-02 01:00", fetch, now=NOW)
    # 100 small partitions fit the default byte budget: none is dropped by a fixed count.
    assert len(sbc._Store._memory) == 100

    sbc._reset_process_memory()
    monkeypatch.setenv("LUMIBOT_SHARED_BARS_MEMORY_MB", "0.05")
    items = [sbc.BarSeries.make("synthetic", f"NEW{i}", "1minute", scope="c", adjusted=False) for i in range(100)]
    for item in items:
        sbc.read_bars(item, "2026-03-02", "2026-03-02 01:00", fetch, now=NOW)
    assert 0 < len(sbc._Store._memory) < 100
    assert sbc._Store._memory_bytes <= 0.05 * 1024 * 1024
