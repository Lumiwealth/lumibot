"""Always-on, cheap run telemetry for backtests: one small JSON file per run.

Every backtest writes ``<run>_telemetry.json`` beside its other output files. It answers
"where did the time go" without a profiler:

* wall seconds per phase: data setup (start until the first trading iteration), simulation,
  end-of-run reports, and waiting for background cache uploads;
* trading iterations: count, total seconds, the slowest one and its simulated time;
* per data source and asset class: provider requests, seconds waiting on the provider, rows
  fetched, retries, and rate-limit or backoff waits (count and seconds);
* per cache layer, source and asset class: reads, hits, misses, bytes, uploads.

The file is public-safe: it holds counts, seconds, byte totals, source names (``alpaca``,
``ibkr``, ``thetadata``...) and asset classes. It never holds hostnames, bucket names, keys,
credentials, symbols or account data. Field names are a stable contract (``SCHEMA``); add
fields, never rename them. See ``docs/BACKTEST_TELEMETRY.md``.

Recording is a dictionary update under a lock, about a microsecond per call. Data sources
mark which source and asset class they are fetching with :func:`provider_scope`; generic
transports (the downloader queue, the shared bar cache) record under that scope.
"""

from __future__ import annotations

import contextvars
import json
import os
import threading
import time
from contextlib import contextmanager
from typing import Any, Iterator

SCHEMA = "lumibot.backtest_telemetry/1"

_LOCK = threading.Lock()
_SCOPE: contextvars.ContextVar[tuple[str | None, str | None]] = contextvars.ContextVar(
    "lumibot_backtest_telemetry_scope", default=(None, None)
)

_PROVIDER_FIELDS = (
    "requests", "provider_seconds", "rows", "retries",
    "rate_limit_waits", "rate_limit_wait_seconds", "backoff_waits", "backoff_wait_seconds",
)
_CACHE_FIELDS = (
    "reads", "hits", "misses", "read_bytes", "read_seconds",
    "uploads", "upload_bytes", "upload_seconds", "upload_errors",
)


class _State:
    def __init__(self) -> None:
        self.started = time.perf_counter()
        self.first_iteration_started: float | None = None
        self.simulation_ended: float | None = None
        self.phases: dict[str, float] = {}
        self.iterations = 0
        self.iteration_seconds = 0.0
        self.slowest_seconds = 0.0
        self.slowest_at: str | None = None
        self.providers: dict[tuple[str, str], dict[str, float]] = {}
        self.caches: dict[tuple[str, str, str], dict[str, float]] = {}


_STATE = _State()


def reset() -> None:
    """Start a new run: clear every counter and restart the wall clock."""
    global _STATE
    with _LOCK:
        _STATE = _State()


# --------------------------------------------------------------------------- scope


@contextmanager
def provider_scope(source: str | None = None, asset_class: str | None = None) -> Iterator[None]:
    """Attribute provider and cache work inside the block to ``source`` / ``asset_class``.

    An inner scope fills only what it names; the rest comes from the outer scope.
    """
    outer_source, outer_class = _SCOPE.get()
    token = _SCOPE.set((source or outer_source, asset_class or outer_class))
    try:
        yield
    finally:
        _SCOPE.reset(token)


def current_scope() -> tuple[str | None, str | None]:
    return _SCOPE.get()


def _key(source: str | None, asset_class: str | None) -> tuple[str, str]:
    scoped_source, scoped_class = _SCOPE.get()
    return (str(source or scoped_source or "unknown").lower(), str(asset_class or scoped_class or "unknown").lower())


def _provider_row(source, asset_class) -> dict[str, float]:
    key = _key(source, asset_class)
    row = _STATE.providers.get(key)
    if row is None:
        row = _STATE.providers[key] = dict.fromkeys(_PROVIDER_FIELDS, 0.0)
    return row


def _cache_row(layer, source, asset_class) -> dict[str, float]:
    source_key, class_key = _key(source, asset_class)
    key = (str(layer), source_key, class_key)
    row = _STATE.caches.get(key)
    if row is None:
        row = _STATE.caches[key] = dict.fromkeys(_CACHE_FIELDS, 0.0)
    return row


# --------------------------------------------------------------------------- recording


def record_provider_request(seconds: float = 0.0, *, source: str | None = None, asset_class: str | None = None,
                            rows: int | None = None) -> None:
    """One request answered (or failed) by a data provider, and the seconds spent waiting for it."""
    with _LOCK:
        row = _provider_row(source, asset_class)
        row["requests"] += 1
        row["provider_seconds"] += float(seconds or 0.0)
        if rows:
            row["rows"] += int(rows)


def record_provider_rows(rows: int, *, source: str | None = None, asset_class: str | None = None) -> None:
    """Rows a provider returned, where the rows are counted apart from the request."""
    if not rows:
        return
    with _LOCK:
        _provider_row(source, asset_class)["rows"] += int(rows)


def record_retry(*, source: str | None = None, asset_class: str | None = None) -> None:
    with _LOCK:
        _provider_row(source, asset_class)["retries"] += 1


def record_wait(kind: str, seconds: float, *, source: str | None = None, asset_class: str | None = None) -> None:
    """A deliberate wait: ``kind`` is ``rate_limit`` (provider limit or client throttle) or ``backoff``."""
    name = "rate_limit" if kind == "rate_limit" else "backoff"
    with _LOCK:
        row = _provider_row(source, asset_class)
        row[f"{name}_waits"] += 1
        row[f"{name}_wait_seconds"] += float(seconds or 0.0)


def record_cache(event: str, *, layer: str, source: str | None = None, asset_class: str | None = None,
                 nbytes: int = 0, seconds: float = 0.0) -> None:
    """One cache event: ``hit``, ``miss``, ``upload`` or ``upload_error`` in cache ``layer``."""
    with _LOCK:
        row = _cache_row(layer, source, asset_class)
        if event in ("hit", "miss"):
            row["reads"] += 1
            row["hits" if event == "hit" else "misses"] += 1
            row["read_bytes"] += int(nbytes or 0)
            row["read_seconds"] += float(seconds or 0.0)
        elif event == "upload":
            row["uploads"] += 1
            row["upload_bytes"] += int(nbytes or 0)
            row["upload_seconds"] += float(seconds or 0.0)
        elif event == "upload_error":
            row["upload_errors"] += 1


def iteration_started() -> float:
    """Call at the start of a trading iteration; returns the start time for :func:`iteration_finished`."""
    now = time.perf_counter()
    if _STATE.first_iteration_started is None:
        _STATE.first_iteration_started = now
    return now


def iteration_finished(started: float, simulated_at: Any = None) -> None:
    seconds = time.perf_counter() - started
    state = _STATE
    with _LOCK:
        state.iterations += 1
        state.iteration_seconds += seconds
        if seconds > state.slowest_seconds:
            state.slowest_seconds = seconds
            state.slowest_at = simulated_at.isoformat() if hasattr(simulated_at, "isoformat") else None


def mark_simulation_end() -> None:
    if _STATE.simulation_ended is None:
        _STATE.simulation_ended = time.perf_counter()


def add_phase(name: str, seconds: float) -> None:
    with _LOCK:
        _STATE.phases[name] = _STATE.phases.get(name, 0.0) + float(seconds)


@contextmanager
def phase(name: str) -> Iterator[None]:
    started = time.perf_counter()
    try:
        yield
    finally:
        add_phase(name, time.perf_counter() - started)


# --------------------------------------------------------------------------- output


def _round(value: float, field: str) -> float | int:
    return round(value, 3) if field.endswith("seconds") else int(value)


def snapshot() -> dict:
    state = _STATE
    now = time.perf_counter()
    with _LOCK:
        first = state.first_iteration_started
        end = state.simulation_ended
        phases = {
            "data_setup_seconds": round((first if first is not None else (end or now)) - state.started, 3),
            "simulation_seconds": round(((end or now) - first), 3) if first is not None else 0.0,
            "reports_seconds": round(state.phases.get("reports", 0.0), 3),
            "uploads_seconds": round(state.phases.get("uploads", 0.0), 3),
        }
        providers = [
            {"source": source, "asset_class": asset_class, **{f: _round(v, f) for f, v in row.items()}}
            for (source, asset_class), row in sorted(state.providers.items())
        ]
        caches = [
            {"layer": layer, "source": source, "asset_class": asset_class, **{f: _round(v, f) for f, v in row.items()}}
            for (layer, source, asset_class), row in sorted(state.caches.items())
        ]
        iterations = {
            "count": state.iterations,
            "total_seconds": round(state.iteration_seconds, 3),
            "slowest_seconds": round(state.slowest_seconds, 3),
            "slowest_at": state.slowest_at,
        }
    try:
        from lumibot import __version__ as version
    except Exception:
        version = None
    return {
        "schema": SCHEMA,
        "lumibot_version": version,
        "wall_seconds": round(now - state.started, 3),
        "phases": phases,
        "iterations": iterations,
        "providers": providers,
        "caches": caches,
    }


def write(path: str) -> dict:
    """Write the snapshot to ``path`` (atomically) and return it."""
    data = snapshot()
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as handle:
        json.dump(data, handle, indent=1, sort_keys=False)
    os.replace(tmp, path)
    return data
