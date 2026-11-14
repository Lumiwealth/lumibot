"""One historical price-bar cache for every broker and data provider.

Live reads (charts, trading bots) and backtests that read bars from the same
provider connection share these files. The cache stores only real provider
observations. It never fills, carries forward or invents a bar.

How it works
------------
A series is one provider + symbol + bar size + data contract (adjustment, feed,
extended hours, ...) + a scope shared by every connection of the provider (see
shared_scope; never an account or key). Its bars live in period partitions:
one Parquet file per calendar month for intraday bars, one per calendar year for
daily and longer bars. Each partition records the exact time ranges the
provider answered ("coverage"). A request is served from coverage; only the
uncovered pieces are fetched from the provider.

* Settled bars never expire. A bar is settled once it ended at least
  ``LUMIBOT_SHARED_BARS_SETTLE_MINUTES`` (default 15) before the provider clock.
  The still-open tail is always fetched again and never stored.
* Adjusted series (split or dividend adjusted prices) can change in the past.
  Every fetch adjacent to cached bars re-reads a few cached bars. If the
  provider's prices for them differ, the series generation ("epoch") is bumped
  and the old partitions are ignored. A fully cached adjusted read re-checks
  once a day.
* A provider answer with zero rows never records coverage. An empty answer can
  be an outage or an entitlement gap, so by default it is never remembered.
  Only when the caller vouches for the answer (``confirmed_empty``: the provider
  answered successfully and completely, for a contract it lists) is a zero-row
  answer for a range closed at least ``EMPTY_SETTLE`` ago stored as a
  "confirmed empty" marker. Markers hold no rows and live apart from coverage, so
  they never become bars. A marker for a range that closed less than
  ``EMPTY_RECENT`` before it was recorded is re-checked once after
  ``EMPTY_RECHECK_AFTER`` (providers backfill recent data); older ones are permanent.
* Local files are written before the result is returned (milliseconds).
  Remote (S3) uploads run on a background thread. ``flush_shared_bar_writes``
  waits for them; it also runs at interpreter exit.

The remote store is the existing BacktestCacheManager S3 namespace, so these
files sit next to the backtest cache under ``<prefix>/<version>/bars/``.
"""

from __future__ import annotations

import atexit
import contextvars
import hashlib
import io
import json
import logging
import os
import queue
import tempfile
import threading
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Sequence

import numpy as np
import pandas as pd

from lumibot import constants
from lumibot.data_sources.exceptions import InvalidBars

logger = logging.getLogger(__name__)

SCHEMA = 2
PRICE_FIELDS = ["open", "high", "low", "close"]
OHLCV_FIELDS = PRICE_FIELDS + ["volume"]
# Re-read this many cached bars next to every fetch of an adjusted series.
OVERLAP_BARS = 3
# A fully cached adjusted series is re-checked against the provider this often.
VERIFY_EVERY = timedelta(hours=24)
_EPSILON = pd.Timedelta(microseconds=1)
_UNITS = {
    "minute": pd.Timedelta(minutes=1),
    "hour": pd.Timedelta(hours=1),
    "day": pd.Timedelta(days=1),
    "week": pd.Timedelta(days=7),
    "month": pd.Timedelta(days=31),
}


# --------------------------------------------------------------------------- settings


def enabled() -> bool:
    return os.environ.get("LUMIBOT_SHARED_BARS", "true").strip().lower() not in {"0", "false", "off", "no"}


def share_across_credentials() -> bool:
    """``provider`` shares bars between connections of one provider; default keeps each connection apart."""
    return os.environ.get("LUMIBOT_SHARED_BARS_SCOPE", "credential").strip().lower() == "provider"


def settle_margin() -> pd.Timedelta:
    raw = os.environ.get("LUMIBOT_SHARED_BARS_SETTLE_MINUTES")
    try:
        minutes = float(raw) if raw else 15.0
    except ValueError:
        minutes = 15.0
    return pd.Timedelta(minutes=max(0.0, minutes))


DEFAULT_MEMORY_MB = 256.0


def memory_budget_bytes() -> int:
    """Byte budget for partitions kept in this process's memory (``LUMIBOT_SHARED_BARS_MEMORY_MB``)."""
    raw = os.environ.get("LUMIBOT_SHARED_BARS_MEMORY_MB")
    try:
        megabytes = float(raw) if raw else DEFAULT_MEMORY_MB
    except ValueError:
        megabytes = DEFAULT_MEMORY_MB
    return int(max(0.0, megabytes) * 1024 * 1024)


def _estimated_bytes(part) -> int:
    """Rough in-memory size of a partition: its rows plus a small fixed overhead."""
    try:
        rows = int(part.rows.memory_usage(index=True, deep=False).sum())
    except Exception:
        rows = 0
    return rows + 1024 + 256 * len(part.empty)


def credential_scope(provider, *identity) -> str | None:
    """Stable one-way scope for a provider connection (never store the raw credential).

    ``None`` (do not share) unless at least one identifying value besides the provider name is
    present: a scope built from the provider name alone would merge unrelated connections.

    Kept for reading series written before :func:`shared_scope` (see ``legacy_scopes`` on
    :meth:`BarSeries.make`). New series should use :func:`shared_scope`.
    """
    values = [str(part) for part in identity if part not in (None, "")]
    if not values:
        return None
    return hashlib.sha256("\x1f".join([str(provider), *values]).encode()).hexdigest()


def shared_scope(provider, *tier) -> str:
    """Scope shared by every connection of ``provider`` that receives the same numbers.

    ``tier`` names only what changes the data a provider returns (for example a sandbox
    endpoint with different prices). Never pass an account, login, token, user, strategy or
    bot: two requests that would get the same numbers from the provider share one series.
    """
    values = [str(part) for part in tier if part not in (None, "")]
    return hashlib.sha256("\x1f".join(["shared", str(provider), *values]).encode()).hexdigest()


# --------------------------------------------------------------------------- series


def normalize_timestep(quantity, unit: str) -> str:
    unit = str(unit).lower().rstrip("s")
    if unit == "min":
        unit = "minute"
    if unit not in _UNITS:
        raise ValueError(f"Unsupported bar size: {quantity} {unit}")
    return f"{int(quantity)}{unit}"


def bar_duration(timestep: str) -> pd.Timedelta:
    digits = "".join(ch for ch in timestep if ch.isdigit()) or "1"
    unit = timestep[len(digits) :] if timestep.startswith(digits) else timestep
    return _UNITS[unit] * int(digits)


@dataclass(frozen=True)
class BarSeries:
    """Identity of one cached bar series.

    ``contract`` holds everything else that changes the numbers a provider
    returns (asset type, adjustment, feed, extended hours, quote currency...).
    ``adjusted`` marks series whose past prices can be rewritten by the provider.
    """

    provider: str
    symbol: str
    timestep: str
    contract: tuple[tuple[str, str], ...] = ()
    scope: str | None = None
    adjusted: bool = True
    # Older identities of the same data (per-connection scopes written before the cache was
    # shared). Read-only fallback: a partition missing under this series is adopted from one of
    # these and written under this series. Not part of the identity.
    legacy: tuple = field(default=(), compare=False, hash=False, repr=False)

    @staticmethod
    def make(provider, symbol, timestep, *, scope=None, adjusted=True, legacy_scopes=(), **contract) -> BarSeries:
        items = tuple(sorted((str(k), json.dumps(v, sort_keys=True, default=str)) for k, v in contract.items()))
        legacy = tuple(
            BarSeries(str(provider), str(symbol), str(timestep), items, old, bool(adjusted))
            for old in dict.fromkeys(legacy_scopes or ())
            if old and old != scope
        )
        return BarSeries(str(provider), str(symbol), str(timestep), items, scope, bool(adjusted), legacy)

    @property
    def duration(self) -> pd.Timedelta:
        return bar_duration(self.timestep)

    @property
    def effective_scope(self) -> str | None:
        return "provider" if share_across_credentials() else self.scope

    def identity(self) -> dict:
        return {
            "schema": SCHEMA,
            "provider": self.provider,
            "symbol": self.symbol,
            "timestep": self.timestep,
            "contract": dict(self.contract),
            "scope": self.effective_scope,
        }

    @property
    def digest(self) -> str:
        return hashlib.sha256(json.dumps(self.identity(), sort_keys=True).encode()).hexdigest()


def _utc(value) -> pd.Timestamp:
    stamp = pd.Timestamp(value)
    return stamp.tz_localize("UTC") if stamp.tzinfo is None else stamp.tz_convert("UTC")


def _partition_start(stamp: pd.Timestamp, intraday: bool) -> pd.Timestamp:
    stamp = stamp.tz_convert("UTC")
    if intraday:
        return pd.Timestamp(year=stamp.year, month=stamp.month, day=1, tz="UTC")
    return pd.Timestamp(year=stamp.year, month=1, day=1, tz="UTC")


def _partition_end(start: pd.Timestamp, intraday: bool) -> pd.Timestamp:
    return start + (pd.offsets.MonthBegin(1) if intraday else pd.offsets.YearBegin(1))


def partitions_for(
    series: BarSeries, start: pd.Timestamp, end_excl: pd.Timestamp
) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    intraday = series.duration < pd.Timedelta(days=1)
    cursor = _partition_start(start, intraday)
    out = []
    while cursor < end_excl:
        stop = _partition_end(cursor, intraday)
        out.append((cursor, stop))
        cursor = stop
    return out


def _label(start: pd.Timestamp, series: BarSeries) -> str:
    return f"{start:%Y-%m}" if series.duration < pd.Timedelta(days=1) else f"{start:%Y}"


def _series_dir(series: BarSeries) -> Path:
    digest = series.digest
    provider = "".join(ch if ch.isalnum() or ch in "-_" else "-" for ch in series.provider.lower())
    return Path(constants.LUMIBOT_CACHE_FOLDER) / "bars" / f"v{SCHEMA}" / provider / digest[:2] / digest


# --------------------------------------------------------------------------- intervals


def _union(intervals: Iterable[tuple[pd.Timestamp, pd.Timestamp]]) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    merged: list[list[pd.Timestamp]] = []
    for start, stop in sorted((s, e) for s, e in intervals if e > s):
        if merged and start <= merged[-1][1]:
            merged[-1][1] = max(merged[-1][1], stop)
        else:
            merged.append([start, stop])
    return [(s, e) for s, e in merged]


def _subtract(start, stop, covered) -> list[tuple[pd.Timestamp, pd.Timestamp]]:
    gaps, cursor = [], start
    for cov_start, cov_stop in covered:
        if cov_stop <= cursor or cov_start >= stop:
            continue
        if cov_start > cursor:
            gaps.append((cursor, min(cov_start, stop)))
        cursor = max(cursor, cov_stop)
        if cursor >= stop:
            break
    if cursor < stop:
        gaps.append((cursor, stop))
    return gaps


# --------------------------------------------------------------------------- validation


def validate_rows(rows: pd.DataFrame) -> bool:
    """Real OHLCV contract: finite, positive prices, consistent high/low, non-negative volume."""
    if rows.empty:
        return True
    if not set(PRICE_FIELDS) <= set(rows.columns):
        return False
    try:
        prices = rows[PRICE_FIELDS].to_numpy(dtype=float)
        # Columns of ``prices``: open, high, low, close. Prices are checked finite before the
        # high/low comparisons, so numpy minimum/maximum match the pandas row-wise min/max.
        opens, highs, lows, closes = prices[:, 0], prices[:, 1], prices[:, 2], prices[:, 3]
        ok = (
            isinstance(rows.index, pd.DatetimeIndex)
            and not rows.index.hasnans
            and not rows.index.has_duplicates
            and bool(np.isfinite(prices).all())
            and bool((prices > 0).all())
            and bool((lows <= np.minimum(np.minimum(opens, closes), highs)).all())
            and bool((highs >= np.maximum(np.maximum(opens, closes), lows)).all())
        )
        if ok and "volume" in rows.columns:
            volume = rows["volume"].to_numpy(dtype=float)
            ok = bool(np.isfinite(volume).all()) and bool((volume >= 0).all())
        return ok
    except (TypeError, ValueError):
        return False


def structurally_valid(rows: pd.DataFrame) -> bool:
    """Usable as stored bars: unique timestamps and price columns. Values are the provider's own.

    Brokers sometimes publish internally inconsistent bars (Tradier SPY 2024-12-19 has its
    close below its low). Those are real provider observations that LumiBot always returned,
    so only adapters that opt into ``strict_ohlcv`` reject them.
    """
    if rows.empty:
        return True
    return (
        isinstance(rows.index, pd.DatetimeIndex)
        and not rows.index.hasnans
        and not rows.index.has_duplicates
        and set(PRICE_FIELDS) <= set(rows.columns)
    )


def _normalize(frame: pd.DataFrame | None) -> pd.DataFrame:
    if frame is None:
        return pd.DataFrame(columns=OHLCV_FIELDS, index=pd.DatetimeIndex([], tz="UTC"))
    rows = frame.copy()
    if not isinstance(rows.index, pd.DatetimeIndex):
        rows.index = pd.to_datetime(rows.index, utc=True)
    rows.index = rows.index.tz_localize("UTC") if rows.index.tz is None else rows.index.tz_convert("UTC")
    rows.index.name = "timestamp"
    # PERF: stored partitions are already sorted and unique; dedupe and sort only when needed.
    if not (rows.index.is_monotonic_increasing and rows.index.is_unique):
        rows = rows[~rows.index.duplicated(keep="last")].sort_index()
    rows.index = pd.DatetimeIndex(rows.index, freq=None, name="timestamp")
    rows.attrs = {}
    return rows


# --------------------------------------------------------------------------- remote store


def _remote():
    """(client, manager) when the S3 cache is configured read/write or read-only, else (None, None)."""
    try:
        from lumibot.tools import parquet_series_cache

        manager = parquet_series_cache.get_backtest_cache()
    except Exception:
        return None, None
    if manager is None or not getattr(manager, "enabled", False):
        return None, None
    try:
        return manager._get_client(), manager
    except Exception as exc:
        logger.warning("Shared bar cache: remote client unavailable (%s)", type(exc).__name__)
        return None, None


# --------------------------------------------------------------------------- run counters
#
# This cache reads and writes S3 itself, so BacktestCacheManager's [REMOTE_CACHE][SUMMARY]
# counters never see it: production Alpaca runs reported downloads=0 misses=0 uploads=0 while
# their repeats were served from here. These counters are logged as [SHARED_BARS][SUMMARY].

_RUN_STATS_KEYS = (
    "provider_reads", "provider_ms", "remote_hits", "remote_misses", "download_bytes", "download_ms",
    "uploads", "upload_bytes", "upload_ms", "upload_errors", "legacy_hits",
)
_RUN_STATS: dict[str, float] = dict.fromkeys(_RUN_STATS_KEYS, 0.0)
_RUN_STATS_LOCK = threading.Lock()


def _count(**values: float) -> None:
    with _RUN_STATS_LOCK:
        for name, value in values.items():
            _RUN_STATS[name] += value


def reset_run_stats() -> None:
    with _RUN_STATS_LOCK:
        for name in _RUN_STATS_KEYS:
            _RUN_STATS[name] = 0.0


def run_stats_snapshot() -> dict:
    with _RUN_STATS_LOCK:
        return {name: (round(value, 3) if name.endswith("_ms") else int(value)) for name, value in _RUN_STATS.items()}


def log_run_summary() -> None:
    """One INFO line with this process's shared bar cache work (numbers only)."""
    snap = run_stats_snapshot()
    if not any(snap.values()):
        return
    logger.info(
        "[SHARED_BARS][SUMMARY] provider_reads=%d provider_ms=%.0f remote_hits=%d remote_misses=%d "
        "download_bytes=%d download_ms=%.0f uploads=%d upload_bytes=%d upload_ms=%.0f upload_errors=%d "
        "legacy_hits=%d pending_uploads=%d",
        snap["provider_reads"], snap["provider_ms"], snap["remote_hits"], snap["remote_misses"],
        snap["download_bytes"], snap["download_ms"], snap["uploads"], snap["upload_bytes"],
        snap["upload_ms"], snap["upload_errors"], snap["legacy_hits"], pending_shared_bar_writes(),
    )


def _path_provider(path: Path) -> str | None:
    """The provider directory in a partition path (``bars/v<schema>/<provider>/...``)."""
    parts = Path(path).parts
    for index, part in enumerate(parts[:-2]):
        if part == "bars" and parts[index + 1] == f"v{SCHEMA}":
            return parts[index + 2]
    return None


def _telemetry(event: str, layer: str, path: Path, nbytes: int = 0, seconds: float = 0.0) -> None:
    try:
        from lumibot.tools import backtest_telemetry

        backtest_telemetry.record_cache(event, layer=layer, source=_path_provider(path), nbytes=nbytes, seconds=seconds)
    except Exception:
        pass


def _remote_get(client, manager, path: Path) -> bytes | None:
    key = manager.remote_key_for(path)
    if key is None:
        return None
    bucket = manager._settings.bucket
    started = time.perf_counter()
    try:
        if hasattr(client, "get_object"):
            response = client.get_object(Bucket=bucket, Key=key)
            body = response["Body"].read()
        else:
            with tempfile.TemporaryDirectory() as tmp:
                target = Path(tmp) / "object"
                client.download_file(bucket, key, str(target))
                body = target.read_bytes()
    except Exception as exc:
        if manager._is_not_found_error(exc) or "AccessDenied" in str(exc) or "403" in str(exc):
            _count(remote_misses=1, download_ms=(time.perf_counter() - started) * 1000)
            _telemetry("miss", "shared_bars_s3", path, seconds=time.perf_counter() - started)
            return None
        raise
    _count(remote_hits=1, download_bytes=len(body or b""), download_ms=(time.perf_counter() - started) * 1000)
    _telemetry("hit", "shared_bars_s3", path, nbytes=len(body or b""), seconds=time.perf_counter() - started)
    return body


def _remote_put(client, manager, path: Path, body: bytes) -> None:
    key = manager.remote_key_for(path)
    if key is None:
        return
    bucket = manager._settings.bucket
    started = time.perf_counter()
    try:
        if hasattr(client, "put_object"):
            client.put_object(Bucket=bucket, Key=key, Body=body)
        else:
            with tempfile.TemporaryDirectory() as tmp:
                source = Path(tmp) / path.name
                source.write_bytes(body)
                client.upload_file(str(source), bucket, key)
    except Exception:
        _count(upload_errors=1)
        _telemetry("upload_error", "shared_bars_s3", path)
        raise
    _count(uploads=1, upload_bytes=len(body), upload_ms=(time.perf_counter() - started) * 1000)
    _telemetry("upload", "shared_bars_s3", path, nbytes=len(body), seconds=time.perf_counter() - started)


# --------------------------------------------------------------------------- partition files


@dataclass
class _Partition:
    series: BarSeries
    start: pd.Timestamp
    stop: pd.Timestamp
    path: Path
    rows: pd.DataFrame = field(default_factory=lambda: _normalize(None))
    coverage: list[tuple[pd.Timestamp, pd.Timestamp]] = field(default_factory=list)
    epoch: int = 0
    # Written by an adapter that rejects inconsistent OHLC (see ``strict_ohlcv``).
    strict: bool = False
    # Confirmed-empty markers: dicts with start/endExclusive/recordedAt (UTC Timestamps),
    # provider, symbol, timestep and reason. Never rows, never coverage.
    empty: list = field(default_factory=list)
    # Filled from an older identity of the same data (``BarSeries.legacy``); written under the
    # shared identity once loaded so later readers find it there.
    adopted: bool = False


@dataclass
class _Manifest:
    epoch: int = 0
    verified_at: pd.Timestamp | None = None


def _encode_partition(part: _Partition) -> bytes:
    rows = part.rows.copy()
    rows.attrs = {
        "sharedBars": {
            "schema": SCHEMA,
            "series": part.series.digest,
            "epoch": part.epoch,
            "start": part.start.isoformat(),
            "endExclusive": part.stop.isoformat(),
            "coverage": [[s.isoformat(), e.isoformat()] for s, e in part.coverage],
            "strict": bool(part.strict),
        }
    }
    if part.empty:
        rows.attrs["sharedBars"]["confirmedEmpty"] = [
            {**m, "start": m["start"].isoformat(), "endExclusive": m["endExclusive"].isoformat(),
             "recordedAt": m["recordedAt"].isoformat()}
            for m in part.empty
        ]
    # Same bytes as ``rows.to_parquet(buffer)``, but the column conversion runs on this thread
    # (nthreads=1). pandas' to_parquet lets pyarrow convert columns on a concurrent.futures
    # pool; once Python starts shutting down (the main thread returned while this non-daemon
    # uploader still has jobs) that pool raises "cannot schedule new futures after interpreter
    # shutdown", and every remaining upload of a finished backtest failed (production,
    # 2026-11-11).
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.Table.from_pandas(rows, nthreads=1)
    if rows.attrs:
        table = table.replace_schema_metadata(
            {**(table.schema.metadata or {}), "PANDAS_ATTRS": json.dumps(rows.attrs)}
        )
    buffer = io.BytesIO()
    pq.write_table(table, buffer, compression="snappy")
    return buffer.getvalue()


def _read_partition_frame(body: bytes) -> pd.DataFrame:
    """The partition frame and its ``attrs``, as ``pd.read_parquet`` returns them, with less overhead.

    A delta pick decodes about 57 small partitions; ``pd.read_parquet`` spent about 0.9 ms
    each, mostly fixed overhead. Reading the table with pyarrow and restoring pandas' stored
    ``attrs`` gives the same frame (pinned by a test) in about 0.7 ms. Anything unexpected
    falls back to ``pd.read_parquet``.
    """
    try:
        import pyarrow.parquet as pq

        table = pq.read_table(io.BytesIO(body))
        frame = table.to_pandas()
        stored = (table.schema.metadata or {}).get(b"PANDAS_ATTRS")
        if stored:
            frame.attrs = json.loads(stored)
        return frame
    except Exception:
        return pd.read_parquet(io.BytesIO(body))


def _decode_partition(part: _Partition, body: bytes | None) -> _Partition:
    """Fill ``part`` from bytes. Anything unusable becomes an empty partition (a real fetch follows)."""
    if not body:
        return part
    try:
        rows = _read_partition_frame(body)
        meta = rows.attrs.get("sharedBars") or {}
        if (
            meta.get("schema") != SCHEMA
            or meta.get("series") != part.series.digest
            or meta.get("start") != part.start.isoformat()
            or meta.get("endExclusive") != part.stop.isoformat()
        ):
            return part
        coverage = [(_utc(s), _utc(e)) for s, e in meta.get("coverage") or []]
        rows = _normalize(rows)
        strict = bool(meta.get("strict"))
        if not (validate_rows(rows) if strict else structurally_valid(rows)):
            return part
        if any(s < part.start or e > part.stop for s, e in coverage):
            return part
        empty = []
        for marker in meta.get("confirmedEmpty") or []:
            marker = {**marker, "start": _utc(marker["start"]), "endExclusive": _utc(marker["endExclusive"]),
                      "recordedAt": _utc(marker["recordedAt"])}
            if marker.get("reason") != EMPTY_REASON or marker["start"] < part.start or marker["endExclusive"] > part.stop:
                return part
            empty.append(marker)
        part.empty = empty
        part.rows = rows
        part.coverage = _union(coverage)
        part.epoch = int(meta.get("epoch") or 0)
        part.strict = strict
    except Exception:
        return _Partition(part.series, part.start, part.stop, part.path)
    return part


def _write_local(path: Path, body: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(body)
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def _merge_partitions(base: _Partition, other: _Partition) -> _Partition:
    """Union of two views of one partition, same epoch only (newest epoch wins otherwise)."""
    if other.epoch > base.epoch:
        return other
    if other.epoch < base.epoch:
        return base
    rows = pd.concat([other.rows, base.rows]) if not other.rows.empty else base.rows
    rows = rows[~rows.index.duplicated(keep="last")].sort_index()
    base.strict = base.strict or other.strict
    merged = _Partition(
        base.series, base.start, base.stop, base.path, rows, _union(base.coverage + other.coverage), base.epoch,
        base.strict, _merge_markers(base.empty, other.empty, _union(base.coverage + other.coverage)),
    )
    return merged


def _merge_markers(first: list, second: list, coverage) -> list:
    """Both writers' markers, newest record per range, minus ranges that now hold real bars."""
    newest = {}
    for marker in list(first) + list(second):
        key = (marker["start"], marker["endExclusive"])
        if key not in newest or marker["recordedAt"] > newest[key]["recordedAt"]:
            newest[key] = marker
    markers = list(newest.values())
    for lo, hi in coverage:
        markers = _trim_markers(markers, lo, hi)
    return markers


# --------------------------------------------------------------------------- background uploads


# A remote store that fails this many jobs in a row is given up for the rest of the process.
# Without a limit a finished backtest stayed alive for 40 minutes failing 962 queued uploads.
UPLOAD_FAILURES_BEFORE_GIVING_UP = 10


class _Uploader:
    """Background workers that write partition files locally and send them to S3.

    A job takes one argument, ``remote_allowed``. It always does its local write and touches
    the remote store only when ``remote_allowed`` is true.

    Jobs for one path always go to the same worker, so they run one at a time in the order
    they were submitted; different paths upload in parallel. A finished backtest waits for
    its queued uploads before it exits, and one worker made that wait minutes long.
    ``LUMIBOT_SHARED_BARS_UPLOAD_WORKERS`` sets the number of workers (default 8).
    """

    def __init__(self):
        try:
            workers = int(os.environ.get("LUMIBOT_SHARED_BARS_UPLOAD_WORKERS", "8"))
        except ValueError:
            workers = 8
        self._workers = max(1, workers)
        self._queues = [queue.Queue() for _ in range(self._workers)]
        self._threads = [None] * self._workers
        self._pending: dict[str, int] = {}
        self._lock = threading.Lock()
        self._idle = threading.Condition(self._lock)
        self.errors: list[str] = []
        self._failures_in_a_row = 0
        self._remote_given_up = False

    def _slot(self, key: str) -> int:
        return int(hashlib.sha256(key.encode()).hexdigest()[:8], 16) % self._workers

    def submit(self, key: str, job: Callable[[bool], None]) -> None:
        slot = self._slot(key)
        with self._lock:
            self._pending[key] = self._pending.get(key, 0) + 1
            thread = self._threads[slot]
            if thread is None or not thread.is_alive():
                # Not a daemon: a process must not exit with unsaved uploads.
                thread = threading.Thread(
                    target=self._run, args=(slot,), name=f"lumibot-shared-bars-upload-{slot}", daemon=False
                )
                self._threads[slot] = thread
                thread.start()
            # Queue under the lock so an idle worker cannot exit between the liveness
            # check above and this put.
            # Run the job in the submitter's context so run telemetry keeps its asset class.
            context = contextvars.copy_context()
            self._queues[slot].put((key, lambda remote_allowed, _job=job: context.run(_job, remote_allowed)))

    def _run(self, slot: int):
        jobs = self._queues[slot]
        while True:
            try:
                key, job = jobs.get(timeout=5)
            except queue.Empty:
                with self._lock:
                    if jobs.empty():
                        self._threads[slot] = None
                        return
                continue
            try:
                job(not self._remote_given_up)
                with self._lock:
                    self._failures_in_a_row = 0
            except Exception as exc:  # never lose the caller's bars to a cache upload problem
                with self._lock:
                    self.errors.append(type(exc).__name__)
                    self._failures_in_a_row += 1
                    failures = self._failures_in_a_row
                    report = not self._remote_given_up
                    give_up = report and failures >= UPLOAD_FAILURES_BEFORE_GIVING_UP
                    if give_up:
                        self._remote_given_up = True
                if report:
                    # The first failure carries its traceback, so the failing call is visible in
                    # one log line instead of only an exception type.
                    logger.warning(
                        "Shared bar cache upload failed: %s: %s", type(exc).__name__, exc,
                        exc_info=failures == 1,
                    )
                    if give_up:
                        logger.warning(
                            "Shared bar cache: %d uploads failed in a row; giving up on the remote store "
                            "for this run. Local files are still written and results are not affected.",
                            failures,
                        )
            finally:
                with self._lock:
                    self._pending[key] -= 1
                    if self._pending[key] <= 0:
                        del self._pending[key]
                    self._idle.notify_all()

    def pending(self) -> int:
        with self._lock:
            return sum(self._pending.values())

    def flush(self, timeout: float | None = None) -> bool:
        deadline = None if timeout is None else time.monotonic() + timeout
        with self._lock:
            while self._pending:
                remaining = None if deadline is None else deadline - time.monotonic()
                if remaining is not None and remaining <= 0:
                    return False
                self._idle.wait(remaining)
        return True


_UPLOADER = _Uploader()


def flush_shared_bar_writes(timeout: float | None = 60.0) -> dict:
    """Wait for background uploads. Returns ``{"flushed", "pending", "ms", "errors"}``."""
    started = time.perf_counter()
    flushed = _UPLOADER.flush(timeout)
    return {
        "flushed": flushed,
        "pending": _UPLOADER.pending(),
        "ms": round((time.perf_counter() - started) * 1000, 3),
        "errors": list(_UPLOADER.errors),
    }


def pending_shared_bar_writes() -> int:
    return _UPLOADER.pending()


def _flush_at_exit() -> None:
    had_pending = _UPLOADER.pending() > 0
    _UPLOADER.flush(120)
    if had_pending:
        # The end-of-backtest summary ran before these uploads finished; report the final counts.
        try:
            log_run_summary()
        except Exception:
            pass


atexit.register(_flush_at_exit)


_PREWARM_LOCK = threading.Lock()
_PREWARMED = False


def prewarm() -> None:
    """Load the Parquet engine and the remote client on a background thread.

    Short-lived workers (one process per chart read) call this at startup so the first
    cache lookup does not pay these one-time costs. Safe to call more than once.
    """
    global _PREWARMED
    with _PREWARM_LOCK:
        if _PREWARMED:
            return
        _PREWARMED = True

    def warm():
        try:
            import pyarrow.parquet  # noqa: F401

            _remote()
        except Exception:
            pass

    threading.Thread(target=warm, name="lumibot-shared-bars-prewarm", daemon=True).start()


# --------------------------------------------------------------------------- store

_THREAD_LOCKS: dict[str, threading.Lock] = {}
_THREAD_LOCKS_GUARD = threading.Lock()


class _path_lock:
    """Exclusive lock on one partition file across threads and processes."""

    def __init__(self, path: Path):
        self.path = path
        with _THREAD_LOCKS_GUARD:
            self.thread_lock = _THREAD_LOCKS.setdefault(str(path), threading.Lock())
        self.handle = None

    def __enter__(self):
        self.thread_lock.acquire()
        try:
            import fcntl

            self.handle = open(str(self.path) + ".lock", "a+")
            fcntl.flock(self.handle.fileno(), fcntl.LOCK_EX)
        except (ImportError, OSError):
            self.handle = None
        return self

    def __exit__(self, *exc):
        try:
            if self.handle is not None:
                import fcntl

                fcntl.flock(self.handle.fileno(), fcntl.LOCK_UN)
                self.handle.close()
        finally:
            self.thread_lock.release()
        return False


class _Store:
    """Loads and saves partitions locally and (if configured) remotely."""

    # Recently written partitions, so a running bot or backtest skips re-reading its own files.
    # Bounded by an estimate of their in-memory size (``LUMIBOT_SHARED_BARS_MEMORY_MB``), not a
    # count: an options run touches hundreds of small partitions, and a count of 64 dropped
    # them long before memory mattered.
    _memory: OrderedDict[str, tuple[float | None, _Partition, int]] = OrderedDict()
    _memory_bytes = 0
    _memory_lock = threading.Lock()
    # Manifests written by this process (their files may still be queued for writing).
    _manifests: dict[str, _Manifest] = {}
    # Local partition files this process downloaded or wrote itself. Such a file is read from
    # disk instead of S3 whenever it already answers the requested range (see ``_local_covering``).
    _downloaded: set[str] = set()

    def __init__(self):
        self.client, self.manager = _remote()
        mode = getattr(self.manager, "mode", None)
        self.writable = bool(self.manager is not None and str(getattr(mode, "value", mode)).endswith("readwrite"))

    # Loading ---------------------------------------------------------------

    def _load_bytes(self, path: Path) -> bytes | None:
        if self.client is not None:
            body = _remote_get(self.client, self.manager, path)
            if body is not None:
                try:
                    _write_local(path, body)
                    with self._memory_lock:
                        self._downloaded.add(str(path))
                except OSError:
                    pass
                return body
        try:
            return path.read_bytes() if path.exists() else None
        except OSError:
            return None

    def _local_covering(self, part: _Partition, want, clock) -> _Partition | None:
        """This process's local copy of ``part`` when it already answers ``want``, else None.

        Only files this process downloaded or wrote qualify, so a stale file left by an older
        run is never trusted. A copy that lacks any piece of the requested range (for example
        the open, unsettled end of today, which is never in coverage) returns None, and the
        caller asks S3 exactly as before: newer coverage published by another process is still
        found there.
        """
        if self.client is None or want is None:
            return None
        with self._memory_lock:
            known = str(part.path) in self._downloaded
        if not known:
            return None
        try:
            body = part.path.read_bytes()
        except OSError:
            return None
        local = _decode_partition(_Partition(part.series, part.start, part.stop, part.path), body)
        lo, hi = max(want[0], part.start), min(want[1], part.stop)
        if hi > lo:
            answered = _union(local.coverage + _live_empty([local], clock))
            if _subtract(lo, hi, answered):
                return None
        return local

    def load(
        self, items: Sequence[tuple[BarSeries, pd.Timestamp, pd.Timestamp]], *, want=None, clock=None
    ) -> tuple[list[_Partition], dict[BarSeries, _Manifest]]:
        """Load partitions. ``want`` is the requested ``(start, end_exclusive)``; with it, a
        partition this process already holds on disk is served locally when it covers that range."""
        parts = [_Partition(s, a, b, _series_dir(s) / f"{_label(a, s)}.parquet") for s, a, b in items]
        series = list(dict.fromkeys(p.series for p in parts))
        clock = clock if clock is not None else pd.Timestamp(datetime.now(timezone.utc))

        def load_part(part):
            cached = self._memory_get(part.path)
            if cached is not None:
                cached.series = part.series  # same file, possibly another equal-identity series object
                _telemetry("hit", "shared_bars", part.path)
                return cached
            local = self._local_covering(part, want, clock)
            if local is not None:
                _telemetry("hit", "shared_bars_disk", part.path)
                return local
            body = self._load_bytes(part.path)
            _telemetry("hit" if body else "miss", "shared_bars", part.path, nbytes=len(body or b""))
            if not body and part.series.legacy:
                adopted = adopt_legacy(part)
                if adopted is not None:
                    return adopted
            return _decode_partition(part, body)

        adopted_verified: dict = {}

        def adopt_legacy(part):
            """Fill a missing shared partition from the same data stored under an older identity.

            Cost only on a shared miss: one extra GET per legacy identity (plus its manifest for
            adjusted series); an adopted partition is then written once under the shared path.
            """
            for old in part.series.legacy:
                old_path = _series_dir(old) / part.path.name
                old_body = self._load_bytes(old_path)
                if not old_body:
                    continue
                old_part = _decode_partition(_Partition(old, part.start, part.stop, old_path), old_body)
                if not old_part.coverage and not old_part.empty:
                    continue
                if old.adjusted:
                    old_manifest = load_manifest(old)
                    if old_part.epoch != old_manifest.epoch:
                        continue  # an older generation of re-adjusted prices: never reuse
                    if old_manifest.verified_at is not None:
                        # Keep the old check time so the daily re-check runs on its usual day.
                        verified = adopted_verified.get(part.series)
                        if verified is None or old_manifest.verified_at < verified:
                            adopted_verified[part.series] = old_manifest.verified_at
                part.rows, part.coverage, part.empty = old_part.rows, list(old_part.coverage), list(old_part.empty)
                part.strict, part.adopted = old_part.strict, True
                _count(legacy_hits=1)
                _telemetry("hit", "shared_bars_legacy", old_path, nbytes=len(old_body))
                return part
            return None

        def load_manifest(item):
            path = _series_dir(item) / "series.json"
            with self._memory_lock:
                known = self._manifests.get(str(path))
            if known is not None:
                return known
            if not item.adjusted:
                # Only adjusted series ever write a manifest (a re-adjusted history starts a new
                # epoch), so looking one up for unadjusted bars always misses: on a fresh
                # container that was one extra S3 request per option contract.
                return _Manifest()
            body = self._load_bytes(path)
            if not body:
                return _Manifest()
            try:
                data = json.loads(body)
                verified = data.get("verifiedAt")
                return _Manifest(int(data.get("epoch") or 0), _utc(verified) if verified else None)
            except Exception:
                return _Manifest()

        jobs = len(parts) + len(series)
        if self.client is not None and jobs > 1:
            with ThreadPoolExecutor(max_workers=min(32, jobs)) as pool:
                part_futures = [pool.submit(contextvars.copy_context().run, load_part, p) for p in parts]
                manifest_futures = {s: pool.submit(contextvars.copy_context().run, load_manifest, s) for s in series}
                loaded = [f.result() for f in part_futures]
                manifests = {s: f.result() for s, f in manifest_futures.items()}
        else:
            loaded = [load_part(p) for p in parts]
            manifests = {s: load_manifest(s) for s in series}
        for item, verified in adopted_verified.items():
            manifest = manifests[item]
            if manifest.verified_at is None:
                manifests[item] = _Manifest(manifest.epoch, verified)
                self.save_manifest(item, manifests[item])
        for part in loaded:
            if part.adopted:
                # Current rows of the older identity become the shared series' current generation.
                part.epoch = manifests[part.series].epoch
                part.adopted = False
                self.save(part)
                continue
            if part.epoch != manifests[part.series].epoch:
                part.rows, part.coverage, part.empty = _normalize(None), [], []
                part.epoch = manifests[part.series].epoch
        return loaded, manifests

    def _memory_get(self, path: Path) -> _Partition | None:
        with self._memory_lock:
            entry = self._memory.get(str(path))
        if entry is None:
            return None
        if entry[0] is not None:
            try:
                if path.stat().st_mtime != entry[0]:
                    return None
            except OSError:
                return None
        part = entry[1]  # entry = (mtime, partition, estimated bytes)
        return _Partition(
            part.series, part.start, part.stop, part.path, part.rows, list(part.coverage), part.epoch, part.strict,
            list(part.empty),
        )

    def _remember(self, part: _Partition, mtime: float | None) -> None:
        size = _estimated_bytes(part)
        budget = memory_budget_bytes()
        with _Store._memory_lock:
            key = str(part.path)
            previous = _Store._memory.pop(key, None)
            if previous is not None:
                _Store._memory_bytes -= previous[2]
            _Store._memory[key] = (mtime, part, size)
            _Store._memory_bytes += size
            while _Store._memory and _Store._memory_bytes > budget:
                _, (_, _, dropped) = _Store._memory.popitem(last=False)
                _Store._memory_bytes -= dropped

    # Saving ----------------------------------------------------------------

    def _write_merged(self, part: _Partition) -> None:
        """Merge ``part`` into its local file under a lock."""
        part.path.parent.mkdir(parents=True, exist_ok=True)
        with _path_lock(part.path):
            try:
                current_bytes = part.path.read_bytes() if part.path.exists() else None
            except OSError:
                current_bytes = None
            current = _decode_partition(_Partition(part.series, part.start, part.stop, part.path), current_bytes)
            merged = _merge_partitions(part, current)
            _write_local(part.path, _encode_partition(merged))
            with self._memory_lock:
                self._downloaded.add(str(part.path))
            self._remember(merged, part.path.stat().st_mtime)

    def save(self, part: _Partition) -> None:
        """Keep the partition for this process now; write it (and publish it) off the caller's path.

        With a remote store (Lambda workers, cloud backtests) the local file write runs on the
        background uploader too: on a one-CPU worker even the Parquet encode delayed answers by
        seconds. Without a remote store the local write stays synchronous (cheap on a desktop).
        """
        if not self.writable:
            self._write_merged(part)
            return
        self._remember(part, None)
        client, manager, path = self.client, self.manager, part.path

        def write_and_upload(remote_allowed: bool = True):
            self._write_merged(part)
            if not remote_allowed:
                return
            local = _decode_partition(_Partition(part.series, part.start, part.stop, path), path.read_bytes())
            remote = _decode_partition(
                _Partition(part.series, part.start, part.stop, path), _remote_get(client, manager, path)
            )
            _remote_put(client, manager, path, _encode_partition(_merge_partitions(local, remote)))

        _UPLOADER.submit(str(path), write_and_upload)

    def save_manifest(self, series: BarSeries, manifest: _Manifest) -> None:
        path = _series_dir(series) / "series.json"
        body = json.dumps(
            {
                "schema": SCHEMA,
                "series": series.digest,
                "epoch": manifest.epoch,
                "verifiedAt": manifest.verified_at.isoformat() if manifest.verified_at is not None else None,
            },
            sort_keys=True,
        ).encode()
        with self._memory_lock:
            self._manifests[str(path)] = manifest
        if not self.writable:
            _write_local(path, body)
            return
        client, manager = self.client, self.manager

        def upload(remote_allowed: bool = True):
            _write_local(path, body)
            if not remote_allowed:
                return
            current = _remote_get(client, manager, path)
            if current:
                try:
                    if int(json.loads(current).get("epoch") or 0) > manifest.epoch:
                        return  # another reader already moved to a newer generation
                except Exception:
                    pass
            _remote_put(client, manager, path, body)

        _UPLOADER.submit(str(path), upload)


# --------------------------------------------------------------------------- reads


FetchMany = Callable[[List[BarSeries], pd.Timestamp, pd.Timestamp], Dict[BarSeries, Optional[pd.DataFrame]]]

# Confirmed-empty markers (see module docstring).
EMPTY_SETTLE = pd.Timedelta(days=1)
EMPTY_RECENT = pd.Timedelta(days=30)
EMPTY_RECHECK_AFTER = pd.Timedelta(days=7)
EMPTY_REASON = "provider_success_zero_rows"


def _marker_live(marker: dict, clock: pd.Timestamp) -> bool:
    """A marker counts as answered unless it is a recent range due for its one re-check."""
    recorded = marker["recordedAt"]
    if recorded - marker["endExclusive"] >= EMPTY_RECENT:
        return True
    return clock - recorded < EMPTY_RECHECK_AFTER


def _live_empty(parts, clock) -> list:
    return [(m["start"], m["endExclusive"]) for p in parts for m in p.empty if _marker_live(m, clock)]


def _trim_markers(markers: list, lo, hi) -> list:
    """Markers with ``[lo, hi)`` removed (real bars or a fresh answer now own that range)."""
    out = []
    for marker in markers:
        for a, b in _subtract(marker["start"], marker["endExclusive"], [(lo, hi)]):
            out.append({**marker, "start": a, "endExclusive": b})
    return out


def _settled_cut(series: BarSeries, now: pd.Timestamp) -> pd.Timestamp:
    """Bars starting before this instant are complete (ended a margin ago)."""
    return now - series.duration - settle_margin()


def _same_prices(cached: pd.DataFrame, fresh: pd.DataFrame) -> bool:
    common = cached.index.intersection(fresh.index)
    if common.empty:
        return True
    a = cached.loc[common, PRICE_FIELDS].to_numpy(dtype=float)
    b = fresh.loc[common, PRICE_FIELDS].to_numpy(dtype=float)
    return bool(np.allclose(a, b, rtol=1e-6, atol=0.0))


def read_bars_many(
    series_list: Sequence[BarSeries],
    start,
    end,
    fetch: FetchMany,
    *,
    now=None,
    refresh: bool = False,
    coverage_from_rows: bool = False,
    strict_ohlcv: bool = False,
    confirmed_empty: Callable[[BarSeries], bool] | None = None,
) -> dict[BarSeries, pd.DataFrame]:
    """Bars with start in ``[start, end]`` for each series, served from the shared cache.

    ``fetch(series_subset, gap_start, gap_end_exclusive)`` must return real provider
    rows (UTC or tz-aware index, OHLCV columns) for each series and may raise. Its
    errors propagate and nothing is cached. ``now`` is the provider clock (pass
    ``now - delay`` for delayed feeds). ``coverage_from_rows`` is for providers
    that can silently truncate the start of an answer: coverage then starts at the
    first returned bar instead of the requested start.

    ``confirmed_empty(series)`` returning True is the caller's statement that ``fetch``
    returns only complete, successful provider answers for that series (errors, rate
    limits, timeouts and truncated pages raise) and that the provider lists the
    instrument, so a zero-row answer means it really had no bars. Only then is a
    zero-row answer for a range closed ``EMPTY_SETTLE`` ago remembered (never together
    with ``coverage_from_rows``). See the module docstring.
    """
    started = time.perf_counter()
    start, end = _utc(start), _utc(end)
    clock = _utc(now) if now is not None else pd.Timestamp(datetime.now(timezone.utc))
    end_excl = end + _EPSILON
    series_list = list(dict.fromkeys(series_list))
    usable = [s for s in series_list if s.effective_scope is not None]
    if not enabled() or end < start or not usable:
        frames = fetch(series_list, start, end_excl) if series_list else {}
        return {s: _normalize(frames.get(s)) for s in series_list}

    store = _Store()
    plan = [(s, a, b) for s in usable for a, b in partitions_for(s, start, end_excl)]
    load_started = time.perf_counter()
    parts, manifests = store.load(plan, want=None if refresh else (start, end_excl), clock=clock)
    lookup_ms = (time.perf_counter() - load_started) * 1000
    by_series: dict[BarSeries, list[_Partition]] = {}
    for part in parts:
        by_series.setdefault(part.series, []).append(part)

    gaps: dict[BarSeries, list[tuple[pd.Timestamp, pd.Timestamp]]] = {}
    overlap_checks: dict[BarSeries, bool] = {}
    for item in usable:
        item_parts = by_series[item]
        covered = [] if refresh else _union(
            [c for p in item_parts for c in p.coverage] + _live_empty(item_parts, clock)
        )
        cached_rows = _rows_in(item_parts, start, end_excl)
        item_gaps = _subtract(start, end_excl, covered)
        manifest = manifests[item]
        verify = (
            item.adjusted
            and not item_gaps
            and not cached_rows.empty
            and (manifest.verified_at is None or clock - manifest.verified_at >= VERIFY_EVERY)
        )
        if verify:
            # Daily re-check of a fully cached adjusted series: re-read the last bars.
            tail = cached_rows.index[-OVERLAP_BARS:]
            item_gaps = [(tail[0], tail[-1] + _EPSILON)]
        elif item.adjusted and item_gaps and not cached_rows.empty:
            # Extend each gap back over a few cached bars to detect re-adjusted history.
            extended = []
            for gap_start, gap_stop in item_gaps:
                before = cached_rows.index[cached_rows.index < gap_start]
                if len(before):
                    gap_start = before[-OVERLAP_BARS:][0]
                extended.append((gap_start, gap_stop))
            item_gaps = _union(extended)
        # A settled gap is read one bar further (still settled). Repeated requests for
        # "now - timeshift" drift by milliseconds; this keeps them inside coverage.
        cut = _settled_cut(item, clock)
        # Likewise one bar earlier: callers in other time zones start "the same day"
        # a few hours apart.
        item_gaps = [(a - item.duration, max(b, min(b + item.duration, cut))) for a, b in item_gaps]
        gaps[item] = _union(item_gaps)
        overlap_checks[item] = bool(item.adjusted and not cached_rows.empty)

    # One provider call per distinct gap, shared by every series needing exactly it.
    calls: dict[tuple[pd.Timestamp, pd.Timestamp], list[BarSeries]] = {}
    for item, item_gaps in gaps.items():
        for gap in item_gaps:
            calls.setdefault(gap, []).append(item)
    fetched: dict[BarSeries, list[tuple[tuple[pd.Timestamp, pd.Timestamp], pd.DataFrame]]] = {}
    provider_ms = 0.0
    for (gap_start, gap_stop), members in sorted(calls.items(), key=lambda kv: kv[0]):
        logger.debug(
            "Shared bar cache: provider read %s..%s for %s (request %s..%s)",
            gap_start,
            gap_stop,
            [m.symbol for m in members],
            start,
            end,
        )
        provider_started = time.perf_counter()
        answer = fetch(members, gap_start, gap_stop) or {}
        provider_ms += (time.perf_counter() - provider_started) * 1000
        for item in members:
            rows = _normalize(answer.get(item))
            valid = validate_rows(rows) if strict_ohlcv else structurally_valid(rows)
            rows = rows[(rows.index >= gap_start) & (rows.index < gap_stop)]
            if not valid:
                raise InvalidBars(f"{item.provider} returned invalid OHLCV bars; shared cache was not written")
            fetched.setdefault(item, []).append(((gap_start, gap_stop), rows))

    # Re-adjusted history: start a new generation and fetch the whole request again.
    restarted = []
    for item in usable:
        if not overlap_checks[item] or item not in fetched:
            continue
        cached_rows = _rows_in(by_series[item], start, end_excl)
        fresh = pd.concat([rows for _, rows in fetched[item]]) if fetched[item] else _normalize(None)
        if not _same_prices(cached_rows, fresh):
            logger.info(
                "Shared bar cache: %s %s history changed at the provider; rebuilding", item.provider, item.symbol
            )
            restarted.append(item)
    if restarted:
        for item in restarted:
            manifests[item] = _Manifest(manifests[item].epoch + 1, None)
            for part in by_series[item]:
                part.rows, part.coverage, part.epoch = _normalize(None), [], manifests[item].epoch
                part.empty = []
            store.save_manifest(item, manifests[item])
        provider_started = time.perf_counter()
        answer = fetch(restarted, start, end_excl) or {}
        provider_ms += (time.perf_counter() - provider_started) * 1000
        for item in restarted:
            rows = _normalize(answer.get(item))
            valid = validate_rows(rows) if strict_ohlcv else structurally_valid(rows)
            rows = rows[(rows.index >= start) & (rows.index < end_excl)]
            if not valid:
                raise InvalidBars(f"{item.provider} returned invalid OHLCV bars; shared cache was not written")
            fetched[item] = [((start, end_excl), rows)]
            calls[(start, end_excl)] = [item]

    # Record settled answers. Write local files now; uploads continue in the background.
    write_ms = 0.0
    results: dict[BarSeries, pd.DataFrame] = {}
    for item in usable:
        item_parts = by_series[item]
        cut = _settled_cut(item, clock)
        empty_cut = clock - item.duration - EMPTY_SETTLE
        vouched = (confirmed_empty is not None and not coverage_from_rows and item not in restarted
                   and bool(confirmed_empty(item)))
        changed = set()
        for (gap_start, gap_stop), rows in fetched.get(item, []):
            if rows.empty:
                # An empty answer is remembered only when the caller vouches for it, and only
                # for the part of the range that closed at least EMPTY_SETTLE ago.
                stop = min(gap_stop, empty_cut)
                if not vouched or stop <= gap_start:
                    continue
                for part in item_parts:
                    lo, hi = max(gap_start, part.start), min(stop, part.stop)
                    if hi <= lo:
                        continue
                    part.empty = _trim_markers(part.empty, lo, hi) + [{
                        "start": lo, "endExclusive": hi, "recordedAt": clock, "provider": item.provider,
                        "symbol": item.symbol, "timestep": item.timestep, "reason": EMPTY_REASON,
                    }]
                    changed.add(id(part))
                continue
            cov_start = max(gap_start, rows.index[0]) if coverage_from_rows else gap_start
            cov_stop = gap_stop if gap_stop <= cut else min(gap_stop, cut)
            if cov_stop <= cov_start:
                continue
            settled = rows[(rows.index >= cov_start) & (rows.index < cov_stop)]
            for part in item_parts:
                lo, hi = max(cov_start, part.start), min(cov_stop, part.stop)
                if hi <= lo:
                    continue
                piece = settled[(settled.index >= lo) & (settled.index < hi)]
                keep = part.rows[(part.rows.index < lo) | (part.rows.index >= hi)]
                merged = pd.concat([keep, piece]) if not keep.empty else piece
                part.rows = merged[~merged.index.duplicated(keep="last")].sort_index()
                part.coverage = _union(part.coverage + [(lo, hi)])
                part.empty = _trim_markers(part.empty, lo, hi)
                part.strict = part.strict or strict_ohlcv
                changed.add(id(part))
        for part in item_parts:
            if id(part) in changed:
                save_started = time.perf_counter()
                try:
                    store.save(part)
                except Exception as exc:
                    logger.warning("Shared bar cache write failed; provider bars retained: %s", type(exc).__name__)
                write_ms += (time.perf_counter() - save_started) * 1000
        if item in fetched and item.adjusted:
            manifest = manifests[item]
            if manifest.verified_at is None or clock - manifest.verified_at >= VERIFY_EVERY or item in restarted:
                manifests[item] = _Manifest(manifest.epoch, clock)
                save_started = time.perf_counter()
                try:
                    store.save_manifest(item, manifests[item])
                except Exception as exc:
                    logger.warning("Shared bar cache manifest write failed: %s", type(exc).__name__)
                write_ms += (time.perf_counter() - save_started) * 1000

        pieces = [_rows_in(item_parts, start, end_excl)] if item not in restarted else []
        pieces += [rows for _, rows in fetched.get(item, [])]
        pieces = [p for p in pieces if not p.empty]
        combined = pd.concat(pieces) if pieces else _normalize(None)
        combined = combined[~combined.index.duplicated(keep="last")].sort_index()
        combined = combined[(combined.index >= start) & (combined.index < end_excl)]
        combined.index = pd.DatetimeIndex(combined.index, freq=None, name="timestamp")
        results[item] = combined

    for item in series_list:
        if item not in results:  # no credential scope: never shared, read directly
            frames = fetch([item], start, end_excl) or {}
            results[item] = _normalize(frames.get(item))
    _count(provider_reads=len(calls), provider_ms=provider_ms)
    stats = {
        "kind": "lumibot_shared_bars",
        "hit": not calls,
        "providerReads": len(calls),
        "partitions": len(parts),
        "lookupMs": round(lookup_ms, 3),
        "providerMs": round(provider_ms, 3),
        "writeMs": round(write_ms, 3),
        "totalMs": round((time.perf_counter() - started) * 1000, 3),
        "pendingUploads": _UPLOADER.pending(),
        "remote": store.client is not None,
    }
    for frame in results.values():
        frame.attrs["historyCache"] = dict(stats)
    return results


def _rows_in(parts: Sequence[_Partition], start, end_excl) -> pd.DataFrame:
    pieces = [p.rows[(p.rows.index >= start) & (p.rows.index < end_excl)] for p in parts if not p.rows.empty]
    pieces = [p for p in pieces if not p.empty]
    if not pieces:
        return _normalize(None)
    rows = pieces[0] if len(pieces) == 1 else pd.concat(pieces)
    # PERF: partitions hold sorted, unique, non-overlapping rows, so this is usually a no-op.
    if rows.index.is_monotonic_increasing and rows.index.is_unique:
        return rows.sort_index()
    return rows[~rows.index.duplicated(keep="last")].sort_index()


def read_bars(
    series: BarSeries, start, end, fetch: Callable[[pd.Timestamp, pd.Timestamp], pd.DataFrame | None], **kwargs
) -> pd.DataFrame:
    """Single-series form of :func:`read_bars_many`. ``fetch(gap_start, gap_end_exclusive)``."""
    return read_bars_many([series], start, end, lambda _members, a, b: {series: fetch(a, b)}, **kwargs)[series]


def to_timezone(frame: pd.DataFrame, tz) -> pd.DataFrame:
    """Return ``frame`` in ``tz`` keeping cache measurements."""
    attrs = dict(frame.attrs)
    out = frame.copy()
    out.index = out.index.tz_convert(tz)
    out.attrs = attrs
    return out


def _reset_process_memory() -> None:
    """Forget this process's in-memory partitions and manifests (tests simulate a new process)."""
    with _Store._memory_lock:
        _Store._memory.clear()
        _Store._memory_bytes = 0
        _Store._manifests.clear()
        _Store._downloaded.clear()
