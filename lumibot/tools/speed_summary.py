"""Live speed summary: how long iterations, broker requests and order steps take.

Every ``LUMIBOT_SPEED_SUMMARY_SECONDS`` (default 60; 0 turns it off) a live strategy
logs one line that starts with ``[SpeedSummary]`` followed by JSON:

* ``iterations``: count, and seconds per iteration (median, 95th percentile, max).
* ``requests``: broker requests per minute by kind and seconds spent waiting for the
  broker's request budget, when the broker adapter reports them (Schwab does).
* ``orders`` (milliseconds, median and max): submit call to broker answer
  (``submit_ms``), submit to the fill being seen (``submit_to_fill_ms``), fill seen to
  the strategy's fill callback starting (``fill_to_callback_ms``), callback running
  time (``callback_ms``), and fill seen to the strategy's next order being submitted
  (``fill_to_next_order_ms``).

Nothing here is sent anywhere; it is a log line. Backtests never record or log it.
"""
from __future__ import annotations

import json
import os
import threading
import time
from collections import Counter, deque
from statistics import median
from typing import Callable, Optional

SCHEMA = "lumibot.speed_summary/1"
_MAX_SAMPLES = 2000


def _interval_seconds() -> float:
    raw = os.environ.get("LUMIBOT_SPEED_SUMMARY_SECONDS")
    if raw is None or not str(raw).strip():
        return 60.0
    try:
        value = float(raw)
    except ValueError:
        return 60.0
    return value if value > 0 else 0.0


def _stats(values, scale: float = 1.0, digits: int = 3) -> Optional[dict]:
    values = [v * scale for v in values]
    if not values:
        return None
    ordered = sorted(values)
    p95 = ordered[min(len(ordered) - 1, int(round(0.95 * (len(ordered) - 1))))]
    return {"n": len(values), "median": round(median(values), digits), "p95": round(p95, digits),
            "max": round(ordered[-1], digits)}


class SpeedSummary:
    """Thread-safe recorder for one live strategy process."""

    def __init__(self, clock: Callable[[], float] = time.monotonic, interval: Optional[float] = None):
        self._clock = clock
        self._lock = threading.Lock()
        self.interval = _interval_seconds() if interval is None else interval
        self._reset(clock())
        self._submitted_at: dict[str, float] = {}
        self._last_fill_seen: Optional[float] = None
        self._waiting_next_order = False

    def _reset(self, now: float) -> None:
        self._window_start = now
        self._iterations: deque = deque(maxlen=_MAX_SAMPLES)
        self._requests: Counter = Counter()
        self._budget_wait = 0.0
        self._orders = {k: deque(maxlen=_MAX_SAMPLES) for k in (
            "submit_ms", "submit_to_fill_ms", "fill_to_callback_ms", "callback_ms", "fill_to_next_order_ms")}

    # ----- recording -------------------------------------------------------
    def iteration(self, seconds: float) -> None:
        with self._lock:
            self._iterations.append(float(seconds))

    def request(self, kind: str, waited_seconds: float = 0.0) -> None:
        with self._lock:
            self._requests[str(kind)] += 1
            self._budget_wait += max(0.0, float(waited_seconds))

    def order_submitting(self) -> float:
        now = self._clock()
        with self._lock:
            if self._waiting_next_order and self._last_fill_seen is not None:
                self._orders["fill_to_next_order_ms"].append(now - self._last_fill_seen)
                self._waiting_next_order = False
        return now

    def order_submitted(self, identifier, started: float) -> None:
        now = self._clock()
        with self._lock:
            self._orders["submit_ms"].append(now - started)
            if identifier is not None:
                if len(self._submitted_at) > _MAX_SAMPLES:
                    self._submitted_at.clear()
                self._submitted_at[str(identifier)] = started

    def fill_seen(self, identifier) -> float:
        now = self._clock()
        with self._lock:
            started = self._submitted_at.pop(str(identifier), None) if identifier is not None else None
            if started is not None:
                self._orders["submit_to_fill_ms"].append(now - started)
            self._last_fill_seen = now
            self._waiting_next_order = True
        return now

    def fill_callback(self, started: float, ended: float) -> None:
        with self._lock:
            if self._last_fill_seen is not None:
                self._orders["fill_to_callback_ms"].append(max(0.0, started - self._last_fill_seen))
            self._orders["callback_ms"].append(ended - started)

    # ----- reporting -------------------------------------------------------
    def snapshot(self, reset: bool = False) -> dict:
        now = self._clock()
        with self._lock:
            minutes = max((now - self._window_start) / 60.0, 1e-9)
            payload = {
                "schema": SCHEMA,
                "window_seconds": round(now - self._window_start, 1),
                "iterations": {"count": len(self._iterations), "seconds": _stats(self._iterations)},
                "requests": {
                    "per_minute": {k: round(v / minutes, 1) for k, v in sorted(self._requests.items())},
                    "total": sum(self._requests.values()),
                    "budget_wait_seconds": round(self._budget_wait, 2),
                },
                "orders": {k: _stats(v, scale=1000.0, digits=0) for k, v in self._orders.items()},
            }
            if reset:
                self._reset(now)
            return payload

    def maybe_emit(self, logger) -> Optional[dict]:
        if self.interval <= 0 or self._clock() - self._window_start < self.interval:
            return None
        payload = self.snapshot(reset=True)
        try:
            logger.info("[SpeedSummary] " + json.dumps(payload, separators=(",", ":")))
        except Exception:
            pass
        return payload
