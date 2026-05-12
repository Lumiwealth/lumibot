from __future__ import annotations

import json
import logging
import os
import sys
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

TELEMETRY_PREFIX = "LUMIBOT_TELEMETRY"


def _truthy_env(value: str | None) -> bool:
    return (value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _read_text(path: str) -> str | None:
    try:
        with open(path, encoding="utf-8") as f:
            return f.read()
    except Exception:
        return None


def _read_int(path: str) -> int | None:
    raw = _read_text(path)
    if raw is None:
        return None
    try:
        return int(raw.strip())
    except Exception:
        return None


def read_proc_self_status() -> dict[str, Any]:
    """
    Best-effort process stats.

    On Linux this reads `/proc/self/status`. On other platforms it returns {}.
    """
    raw = _read_text("/proc/self/status")
    if not raw:
        return {}

    out: dict[str, Any] = {}
    for line in raw.splitlines():
        try:
            key, val = line.split(":", 1)
        except ValueError:
            continue
        key = key.strip()
        val = val.strip()

        if key in {"VmRSS", "VmSize"}:
            # Example: "12345 kB"
            parts = val.split()
            if len(parts) >= 2 and parts[1].lower() == "kb":
                try:
                    bytes_val = int(parts[0]) * 1024
                except Exception:
                    continue
                if key == "VmRSS":
                    out["process_rss_bytes"] = bytes_val
                else:
                    out["process_vms_bytes"] = bytes_val
        elif key == "Threads":
            try:
                out["threads_count"] = int(val)
            except Exception:
                continue

    return out


def read_fd_count() -> int | None:
    """
    Best-effort open FD count (Linux only).

    `/proc/self/fd` does not exist on macOS; returns None there.
    """
    try:
        return len(os.listdir("/proc/self/fd"))
    except Exception:
        return None


def read_ru_maxrss_bytes() -> int | None:
    """
    Best-effort peak RSS.

    - Linux: ru_maxrss is in KB
    - macOS: ru_maxrss is in bytes
    """
    try:
        import resource  # stdlib, optional on some platforms
    except Exception:
        return None

    try:
        value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        if sys.platform == "darwin":
            return int(value)
        return int(value) * 1024
    except Exception:
        return None


def read_cgroup_memory() -> dict[str, Any]:
    """
    Best-effort container memory telemetry.

    Supports cgroup v2 and v1.
    """
    # cgroup v2
    current = _read_int("/sys/fs/cgroup/memory.current")
    limit_raw = _read_text("/sys/fs/cgroup/memory.max")
    if current is not None and limit_raw is not None:
        out: dict[str, Any] = {"cgroup_version": 2, "cgroup_mem_current_bytes": current}
        limit_raw = limit_raw.strip()
        if limit_raw != "max":
            try:
                limit = int(limit_raw)
                out["cgroup_mem_limit_bytes"] = limit
                if limit > 0:
                    out["cgroup_mem_pct"] = current / limit
            except Exception:
                pass
        return out

    # cgroup v1 (common on older hosts)
    current = _read_int("/sys/fs/cgroup/memory/memory.usage_in_bytes")
    limit = _read_int("/sys/fs/cgroup/memory/memory.limit_in_bytes")
    if current is None or limit is None:
        return {}

    # Some environments report a huge number for "no limit".
    unlimited_sentinel = 1 << 60
    out = {"cgroup_version": 1, "cgroup_mem_current_bytes": current}
    if limit < unlimited_sentinel:
        out["cgroup_mem_limit_bytes"] = limit
        if limit > 0:
            out["cgroup_mem_pct"] = current / limit
    return out


@dataclass(frozen=True)
class RuntimeTelemetryConfig:
    enabled: bool
    base_interval_s: float = 300.0
    burst_threshold_pct: float = 0.80
    burst_interval_s: float = 15.0
    deep_enabled: bool = False
    deep_threshold_pct: float = 0.90
    deep_cooldown_s: float = 3600.0

    @staticmethod
    def from_env(*, is_backtesting: bool) -> RuntimeTelemetryConfig:
        raw = os.environ.get("LUMIBOT_TELEMETRY")
        if raw is None:
            # Default: enabled for live runs, but must stay off under test runners to avoid background
            # threads/log noise during unit tests.
            in_pytest = ("PYTEST_CURRENT_TEST" in os.environ) or ("pytest" in sys.modules)
            enabled = (not is_backtesting) and (not in_pytest)
        else:
            enabled = _truthy_env(raw)

        base_interval_s = 300.0
        raw_interval = os.environ.get("LUMIBOT_TELEMETRY_INTERVAL_SECONDS")
        if raw_interval:
            try:
                base_interval_s = float(raw_interval)
            except Exception:
                base_interval_s = 300.0
        base_interval_s = max(5.0, base_interval_s)

        deep_enabled = _truthy_env(os.environ.get("LUMIBOT_TELEMETRY_DEEP"))

        return RuntimeTelemetryConfig(
            enabled=enabled,
            base_interval_s=base_interval_s,
            burst_threshold_pct=0.80,
            burst_interval_s=15.0,
            deep_enabled=deep_enabled,
            deep_threshold_pct=0.90,
            deep_cooldown_s=3600.0,
        )


class RuntimeTelemetryEmitter:
    """
    Always-on, lightweight runtime telemetry emitter.

    Emits JSON lines prefixed with `LUMIBOT_TELEMETRY`. All telemetry is best-effort and must never
    crash trading.
    """

    def __init__(
        self,
        *,
        broker: Any,
        stop_event: threading.Event,
        config: RuntimeTelemetryConfig,
        logger: logging.Logger | None = None,
        snapshot_fn: Callable[[], dict[str, Any]] | None = None,
    ):
        self._broker = broker
        self._stop_event = stop_event
        self._config = config
        self._logger = logger or logging.getLogger(__name__)
        self._snapshot_fn = snapshot_fn
        self._thread: threading.Thread | None = None
        self._last_deep_snapshot_s: float = 0.0

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._run,
            name="lumibot-runtime-telemetry",
            daemon=True,
        )
        self._thread.start()

    def join(self, timeout: float | None = None) -> None:
        if self._thread is None:
            return
        try:
            self._thread.join(timeout=timeout)
        except Exception:
            return

    def _build_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {"v": 1, "ts": time.time()}

        try:
            payload["broker"] = getattr(self._broker, "name", None) or None
            payload["strategy_name"] = getattr(self._broker, "_strategy_name", None) or None
        except Exception:
            pass

        try:
            payload.update(read_cgroup_memory())
        except Exception:
            pass
        try:
            payload.update(read_proc_self_status())
        except Exception:
            pass

        try:
            fd_count = read_fd_count()
            if fd_count is not None:
                payload["fd_count"] = fd_count
        except Exception:
            pass

        try:
            ru_maxrss = read_ru_maxrss_bytes()
            if ru_maxrss is not None:
                payload["ru_maxrss_bytes"] = ru_maxrss
        except Exception:
            pass

        try:
            snap: dict[str, Any] | None = None
            if self._snapshot_fn is not None:
                snap = self._snapshot_fn()
            elif hasattr(self._broker, "_telemetry_snapshot"):
                snap = self._broker._telemetry_snapshot()  # noqa: SLF001
            if snap:
                payload["broker_snapshot"] = snap
        except Exception:
            pass

        return payload

    def _should_burst(self, payload: dict[str, Any]) -> bool:
        try:
            pct = payload.get("cgroup_mem_pct")
            if pct is None:
                return False
            return float(pct) >= float(self._config.burst_threshold_pct)
        except Exception:
            return False

    def _maybe_add_deep_snapshot(self, payload: dict[str, Any]) -> None:
        if not self._config.deep_enabled:
            return
        should_fire = False
        try:
            pct = payload.get("cgroup_mem_pct")
            if pct is not None and float(pct) >= float(self._config.deep_threshold_pct):
                should_fire = True
        except Exception:
            pass
        # Local/Mac fallback: cgroup stats absent, and /proc/self/status does not
        # exist on macOS → process_rss_bytes is None. Fall back to ru_maxrss_bytes
        # (populated on both Linux and macOS via resource.getrusage) so the deep
        # tracemalloc snapshot still fires when memory pressure is real.
        if not should_fire:
            try:
                rss = payload.get("process_rss_bytes") or payload.get("ru_maxrss_bytes")
                threshold = int(os.environ.get("LUMIBOT_TELEMETRY_DEEP_RSS_BYTES", str(1 * 1024**3)))
                if rss is not None and int(rss) >= threshold:
                    should_fire = True
            except Exception:
                pass
        if not should_fire:
            return

        now = time.time()
        if now - self._last_deep_snapshot_s < float(self._config.deep_cooldown_s):
            return

        try:
            import tracemalloc

            if not tracemalloc.is_tracing():
                tracemalloc.start(10)
            snap = tracemalloc.take_snapshot()
            top = snap.statistics("lineno")[:10]
            payload["deep"] = {
                "mode": "tracemalloc",
                "tracemalloc_top": [
                    {
                        "file": str(stat.traceback[0].filename),
                        "line": int(stat.traceback[0].lineno),
                        "size_bytes": int(stat.size),
                        "count": int(stat.count),
                    }
                    for stat in top
                ],
            }
            self._last_deep_snapshot_s = now
        except Exception:
            return

    def _emit(self, payload: dict[str, Any]) -> None:
        try:
            line = f"{TELEMETRY_PREFIX} {json.dumps(payload, separators=(',', ':'), sort_keys=True)}"
            self._logger.info(line)
        except Exception:
            return

    def _run(self) -> None:
        # Initial delay keeps startup logs clean and avoids front-loading a container near boot.
        self._stop_event.wait(timeout=2.0)
        while not self._stop_event.is_set():
            try:
                payload = self._build_payload()
                self._maybe_add_deep_snapshot(payload)
                self._emit(payload)
                burst = self._should_burst(payload)
                interval = self._config.burst_interval_s if burst else self._config.base_interval_s
            except Exception:
                interval = self._config.base_interval_s
            self._stop_event.wait(timeout=max(1.0, float(interval)))
