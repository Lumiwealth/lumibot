from __future__ import annotations

import csv
import json
import os
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd

from lumibot.constants import LUMIBOT_CACHE_FOLDER
from lumibot.tools.lumibot_logger import get_logger
from lumibot.tools.parquet_utils import (
    coerce_object_columns_to_json_strings,
    is_parquet_required,
    write_parquet_with_logging,
)

logger = get_logger(__name__)

_TRUTHY = {"1", "true", "yes", "on", "enabled"}
_FALSY = {"0", "false", "no", "off", "disabled"}
_DEFAULT_MAX_EVENTS = 100_000
_EVENT_COLUMNS = [
    "event_index",
    "wall_time_utc",
    "monotonic_s",
    "category",
    "action",
    "provider",
    "symbol",
    "asset_type",
    "timestep",
    "source",
    "exchange",
    "cache_result",
    "result",
    "start_dt",
    "end_dt",
    "elapsed_s",
    "rows",
    "bytes",
    "local_path",
    "cache_relative_path",
    "remote_key",
    "request_id",
    "status",
    "status_code",
    "queue_position",
    "attempts",
    "error_type",
    "error",
    "extra_json",
]

_LOCK = threading.Lock()
_EVENTS: list[Dict[str, Any]] = []
_EVENT_COUNTER = 0
_DROPPED_EVENTS = 0
_START_MONOTONIC = time.monotonic()


def data_source_telemetry_enabled() -> bool:
    """Return whether detailed data-source telemetry should be collected.

    Default behavior is intentionally conservative: full row-level telemetry is enabled when
    explicitly requested, or when cache-miss debug is enabled for a diagnostic run. The existing
    aggregate counters remain available even when this stream is off.
    """

    raw = os.environ.get("LUMIBOT_DATA_SOURCE_TELEMETRY")
    if raw is None:
        return _is_truthy(os.environ.get("LUMIBOT_CACHE_MISS_DEBUG"))
    normalized = str(raw).strip().lower()
    if normalized in _FALSY:
        return False
    return normalized in _TRUTHY


def record_data_source_event(
    *,
    category: str,
    action: str,
    provider: Optional[Any] = None,
    symbol: Optional[Any] = None,
    asset_type: Optional[Any] = None,
    timestep: Optional[Any] = None,
    source: Optional[Any] = None,
    exchange: Optional[Any] = None,
    cache_result: Optional[Any] = None,
    result: Optional[Any] = None,
    start_dt: Optional[Any] = None,
    end_dt: Optional[Any] = None,
    elapsed_s: Optional[Any] = None,
    rows: Optional[Any] = None,
    bytes: Optional[Any] = None,
    local_path: Optional[Any] = None,
    cache_relative_path: Optional[Any] = None,
    remote_key: Optional[Any] = None,
    request_id: Optional[Any] = None,
    status: Optional[Any] = None,
    status_code: Optional[Any] = None,
    queue_position: Optional[Any] = None,
    attempts: Optional[Any] = None,
    error_type: Optional[Any] = None,
    error: Optional[Any] = None,
    **extra: Any,
) -> None:
    """Append a single normalized data-source telemetry event.

    Events are safe for artifacts: the caller must avoid secret-bearing values, and this helper
    also JSON-coerces unknown extras so binary/non-serializable payloads do not crash a backtest.
    """

    if not data_source_telemetry_enabled():
        return

    global _EVENT_COUNTER, _DROPPED_EVENTS

    if cache_relative_path is None and local_path is not None:
        cache_relative_path = cache_path_relative_to_root(local_path)

    row: Dict[str, Any] = {
        "wall_time_utc": datetime.now(timezone.utc).isoformat(),
        "monotonic_s": round(time.monotonic() - _START_MONOTONIC, 6),
        "category": _scalar(category),
        "action": _scalar(action),
        "provider": _scalar(provider),
        "symbol": _scalar(symbol),
        "asset_type": _scalar(asset_type),
        "timestep": _scalar(timestep),
        "source": _scalar(source),
        "exchange": _scalar(exchange),
        "cache_result": _scalar(cache_result),
        "result": _scalar(result),
        "start_dt": _isoish(start_dt),
        "end_dt": _isoish(end_dt),
        "elapsed_s": _float_or_none(elapsed_s),
        "rows": _int_or_none(rows),
        "bytes": _int_or_none(bytes),
        "local_path": _scalar(local_path),
        "cache_relative_path": _scalar(cache_relative_path),
        "remote_key": _scalar(remote_key),
        "request_id": _scalar(request_id),
        "status": _scalar(status),
        "status_code": _int_or_none(status_code),
        "queue_position": _int_or_none(queue_position),
        "attempts": _int_or_none(attempts),
        "error_type": _scalar(error_type),
        "error": _truncate(_scalar(error), 1000),
        "extra_json": _json_dumps(extra),
    }

    with _LOCK:
        _EVENT_COUNTER += 1
        row["event_index"] = _EVENT_COUNTER
        if len(_EVENTS) >= _max_events():
            _DROPPED_EVENTS += 1
            return
        _EVENTS.append(row)


def data_source_telemetry_snapshot() -> Dict[str, Any]:
    """Return aggregate telemetry useful for settings.json and quick diagnosis."""

    with _LOCK:
        rows = list(_EVENTS)
        dropped = _DROPPED_EVENTS
        total_seen = _EVENT_COUNTER

    by_action: Dict[str, Dict[str, Any]] = {}
    by_provider: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        key = f"{row.get('category')}.{row.get('action')}"
        _accumulate(by_action, key, row)
        provider = row.get("provider") or "unknown"
        _accumulate(by_provider, str(provider), row)

    return {
        "enabled": data_source_telemetry_enabled(),
        "events_recorded": len(rows),
        "events_seen": int(total_seen),
        "events_dropped": int(dropped),
        "max_events": _max_events(),
        "elapsed_s_by_action": by_action,
        "elapsed_s_by_provider": by_provider,
    }


def write_data_source_telemetry_artifacts(settings_file: str) -> Dict[str, Any]:
    """Write CSV + Parquet telemetry artifacts beside the normal backtest settings file."""

    if not data_source_telemetry_enabled():
        return {"enabled": False, "rows": 0}

    with _LOCK:
        rows = list(_EVENTS)

    if not rows:
        return {"enabled": True, "rows": 0}

    settings_path = Path(settings_file)
    base = settings_path.name
    if base.endswith("_settings.json"):
        prefix = base[: -len("_settings.json")]
    else:
        prefix = settings_path.stem

    csv_path = settings_path.with_name(f"{prefix}_data_source_telemetry.csv")
    parquet_path = settings_path.with_name(f"{prefix}_data_source_telemetry.parquet")
    csv_path.parent.mkdir(parents=True, exist_ok=True)

    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=_EVENT_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column) for column in _EVENT_COLUMNS})

    parquet_written = False
    parquet_error = None
    try:
        df = pd.DataFrame(rows, columns=_EVENT_COLUMNS)
        write_parquet_with_logging(
            df=df,
            path=str(parquet_path),
            artifact="data_source_telemetry",
            logger=logger,
            index=False,
            required=is_parquet_required(),
            sanitizer=coerce_object_columns_to_json_strings,
        )
        parquet_written = True
    except Exception as exc:
        parquet_error = str(exc)
        if is_parquet_required():
            raise
        logger.warning(
            "Failed to write data_source_telemetry parquet artifact: %s", exc
        )

    return {
        "enabled": True,
        "rows": len(rows),
        "csv": str(csv_path),
        "parquet": str(parquet_path) if parquet_written else None,
        "parquet_error": parquet_error,
    }


def reset_data_source_telemetry(for_testing: bool = False) -> None:
    global _EVENT_COUNTER, _DROPPED_EVENTS, _START_MONOTONIC
    with _LOCK:
        _EVENTS.clear()
        _EVENT_COUNTER = 0
        _DROPPED_EVENTS = 0
        _START_MONOTONIC = time.monotonic()
    if not for_testing:
        logger.debug("[DATA_SOURCE_TELEMETRY] reset requested")


def cache_path_relative_to_root(path: Any) -> Optional[str]:
    try:
        return Path(path).resolve().relative_to(Path(LUMIBOT_CACHE_FOLDER).resolve()).as_posix()
    except Exception:
        return None


def _accumulate(target: Dict[str, Dict[str, Any]], key: str, row: Dict[str, Any]) -> None:
    item = target.setdefault(
        key,
        {
            "count": 0,
            "elapsed_s": 0.0,
            "rows": 0,
            "bytes": 0,
        },
    )
    item["count"] += 1
    item["elapsed_s"] += float(row.get("elapsed_s") or 0.0)
    item["rows"] += int(row.get("rows") or 0)
    item["bytes"] += int(row.get("bytes") or 0)


def _max_events() -> int:
    raw = os.environ.get("LUMIBOT_DATA_SOURCE_TELEMETRY_MAX_EVENTS")
    if not raw:
        return _DEFAULT_MAX_EVENTS
    try:
        return max(0, int(raw))
    except Exception:
        return _DEFAULT_MAX_EVENTS


def _is_truthy(value: Optional[Any]) -> bool:
    return str(value or "").strip().lower() in _TRUTHY


def _scalar(value: Optional[Any]) -> Optional[str]:
    if value is None:
        return None
    try:
        rendered = str(value)
    except Exception:
        return "<unprintable>"
    return _truncate(rendered, 2000)


def _isoish(value: Optional[Any]) -> Optional[str]:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        try:
            return str(value.isoformat())
        except Exception:
            pass
    return _scalar(value)


def _float_or_none(value: Optional[Any]) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _int_or_none(value: Optional[Any]) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def _json_dumps(value: Dict[str, Any]) -> str:
    clean = {
        str(key): _json_safe(val)
        for key, val in value.items()
        if val is not None
    }
    if not clean:
        return "{}"
    try:
        return json.dumps(clean, sort_keys=True, separators=(",", ":"))
    except Exception:
        return json.dumps({key: _scalar(val) for key, val in clean.items()}, sort_keys=True)


def _json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            pass
    return _scalar(value)


def _truncate(value: Optional[str], max_len: int) -> Optional[str]:
    if value is None:
        return None
    if len(value) <= max_len:
        return value
    return f"{value[:max_len]}...(truncated)"
