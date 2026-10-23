"""One historical price-bar cache for every broker and data provider.

Live reads (charts, trading bots) and backtests that read bars from the same
provider connection share these files. The cache stores only real provider
observations. It never fills, carries forward or invents a bar.

How it works
------------
A series is one provider + symbol + bar size + data contract (adjustment, feed,
extended hours, ...) + credential scope. Its bars live in period partitions:
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
  be an outage or an entitlement gap, so it is never remembered as "no bars".
* Local files are written before the result is returned (milliseconds).
  Remote (S3) uploads run on a background thread. ``flush_shared_bar_writes``
  waits for them; it also runs at interpreter exit.

The remote store is the existing BacktestCacheManager S3 namespace, so these
files sit next to the backtest cache under ``<prefix>/<version>/bars/``.
"""

from __future__ import annotations

import atexit
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


def credential_scope(*parts) -> str | None:
    """Stable one-way scope for a provider connection (never store the raw credential)."""
    values = [str(part) for part in parts if part not in (None, "")]
    if not values:
        return None
    return hashlib.sha256("\x1f".join(values).encode()).hexdigest()


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

    @staticmethod
    def make(provider, symbol, timestep, *, scope=None, adjusted=True, **contract) -> BarSeries:
        items = tuple(sorted((str(k), json.dumps(v, sort_keys=True, default=str)) for k, v in contract.items()))
        return BarSeries(str(provider), str(symbol), str(timestep), items, scope, bool(adjusted))

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
        ok = (
            isinstance(rows.index, pd.DatetimeIndex)
            and not rows.index.hasnans
            and not rows.index.has_duplicates
            and bool(np.isfinite(prices).all())
            and bool((prices > 0).all())
            and bool((rows["low"] <= rows[["open", "close", "high"]].min(axis=1)).all())
            and bool((rows["high"] >= rows[["open", "close", "low"]].max(axis=1)).all())
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


def _remote_get(client, manager, path: Path) -> bytes | None:
    key = manager.remote_key_for(path)
    if key is None:
        return None
    bucket = manager._settings.bucket
    try:
        if hasattr(client, "get_object"):
            response = client.get_object(Bucket=bucket, Key=key)
            return response["Body"].read()
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / "object"
            client.download_file(bucket, key, str(target))
            return target.read_bytes()
    except Exception as exc:
        if manager._is_not_found_error(exc) or "AccessDenied" in str(exc) or "403" in str(exc):
            return None
        raise


def _remote_put(client, manager, path: Path, body: bytes) -> None:
    key = manager.remote_key_for(path)
    if key is None:
        return
    bucket = manager._settings.bucket
    if hasattr(client, "put_object"):
        client.put_object(Bucket=bucket, Key=key, Body=body)
        return
    with tempfile.TemporaryDirectory() as tmp:
        source = Path(tmp) / path.name
        source.write_bytes(body)
        client.upload_file(str(source), bucket, key)


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
    buffer = io.BytesIO()
    rows.to_parquet(buffer)
    return buffer.getvalue()


def _decode_partition(part: _Partition, body: bytes | None) -> _Partition:
    """Fill ``part`` from bytes. Anything unusable becomes an empty partition (a real fetch follows)."""
    if not body:
        return part
    try:
        rows = pd.read_parquet(io.BytesIO(body))
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
        base.strict,
    )
    return merged


# --------------------------------------------------------------------------- background uploads


class _Uploader:
    """One background thread that publishes local partition files to S3."""

    def __init__(self):
        self._queue: queue.Queue[tuple[str, Callable[[], None]]] = queue.Queue()
        self._pending: dict[str, int] = {}
        self._lock = threading.Lock()
        self._idle = threading.Condition(self._lock)
        self._thread: threading.Thread | None = None
        self.errors: list[str] = []

    def submit(self, key: str, job: Callable[[], None]) -> None:
        with self._lock:
            self._pending[key] = self._pending.get(key, 0) + 1
            if self._thread is None or not self._thread.is_alive():
                # Not a daemon: a process must not exit with unsaved uploads.
                self._thread = threading.Thread(target=self._run, name="lumibot-shared-bars-upload", daemon=False)
                self._thread.start()
        self._queue.put((key, job))

    def _run(self):
        while True:
            try:
                key, job = self._queue.get(timeout=5)
            except queue.Empty:
                with self._lock:
                    if not self._pending:
                        self._thread = None
                        return
                continue
            try:
                job()
            except Exception as exc:  # never lose the caller's bars to a cache upload problem
                self.errors.append(type(exc).__name__)
                logger.warning("Shared bar cache upload failed: %s", type(exc).__name__)
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


atexit.register(lambda: _UPLOADER.flush(120))


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
    _memory: OrderedDict[str, tuple[float, _Partition]] = OrderedDict()
    _memory_lock = threading.Lock()
    _MEMORY_LIMIT = 64
    # Manifests written by this process (their files may still be queued for writing).
    _manifests: dict[str, _Manifest] = {}

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
                except OSError:
                    pass
                return body
        try:
            return path.read_bytes() if path.exists() else None
        except OSError:
            return None

    def load(
        self, items: Sequence[tuple[BarSeries, pd.Timestamp, pd.Timestamp]]
    ) -> tuple[list[_Partition], dict[BarSeries, _Manifest]]:
        parts = [_Partition(s, a, b, _series_dir(s) / f"{_label(a, s)}.parquet") for s, a, b in items]
        series = list(dict.fromkeys(p.series for p in parts))

        def load_part(part):
            cached = self._memory_get(part.path)
            if cached is not None:
                cached.series = part.series  # same file, possibly another equal-identity series object
                return cached
            return _decode_partition(part, self._load_bytes(part.path))

        def load_manifest(item):
            path = _series_dir(item) / "series.json"
            with self._memory_lock:
                known = self._manifests.get(str(path))
            if known is not None:
                return known
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
                part_futures = [pool.submit(load_part, p) for p in parts]
                manifest_futures = {s: pool.submit(load_manifest, s) for s in series}
                loaded = [f.result() for f in part_futures]
                manifests = {s: f.result() for s, f in manifest_futures.items()}
        else:
            loaded = [load_part(p) for p in parts]
            manifests = {s: load_manifest(s) for s in series}
        for part in loaded:
            if part.epoch != manifests[part.series].epoch:
                part.rows, part.coverage = _normalize(None), []
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
        part = entry[1]
        return _Partition(
            part.series, part.start, part.stop, part.path, part.rows, list(part.coverage), part.epoch, part.strict
        )

    def _remember(self, part: _Partition, mtime: float | None) -> None:
        with self._memory_lock:
            self._memory[str(part.path)] = (mtime, part)
            self._memory.move_to_end(str(part.path))
            while len(self._memory) > self._MEMORY_LIMIT:
                self._memory.popitem(last=False)

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

        def write_and_upload():
            self._write_merged(part)
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

        def upload():
            _write_local(path, body)
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
) -> dict[BarSeries, pd.DataFrame]:
    """Bars with start in ``[start, end]`` for each series, served from the shared cache.

    ``fetch(series_subset, gap_start, gap_end_exclusive)`` must return real provider
    rows (UTC or tz-aware index, OHLCV columns) for each series and may raise. Its
    errors propagate and nothing is cached. ``now`` is the provider clock (pass
    ``now - delay`` for delayed feeds). ``coverage_from_rows`` is for providers
    that can silently truncate the start of an answer: coverage then starts at the
    first returned bar instead of the requested start.
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
    parts, manifests = store.load(plan)
    lookup_ms = (time.perf_counter() - load_started) * 1000
    by_series: dict[BarSeries, list[_Partition]] = {}
    for part in parts:
        by_series.setdefault(part.series, []).append(part)

    gaps: dict[BarSeries, list[tuple[pd.Timestamp, pd.Timestamp]]] = {}
    overlap_checks: dict[BarSeries, bool] = {}
    for item in usable:
        item_parts = by_series[item]
        covered = [] if refresh else _union(c for p in item_parts for c in p.coverage)
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
        changed = set()
        for (gap_start, gap_stop), rows in fetched.get(item, []):
            if rows.empty:
                continue  # an empty answer is never remembered as "no bars"
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
    rows = pd.concat(pieces)
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
        _Store._manifests.clear()
