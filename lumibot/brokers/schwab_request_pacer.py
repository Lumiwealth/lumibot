"""Client-side request pacing for the Schwab Trader API.

Schwab counts requests per minute and answers HTTP 429 when a limit is
crossed. A developer app can be shared by several bots or accounts, so one
process should spend only its own share. This module keeps, per Schwab client:

* a budget of ``requests_per_minute`` that holds over ANY 60 seconds, start-up
  included (0 disables pacing). It is a token bucket whose burst is
  ``burst_seconds`` worth of requests (default 6 s) and whose refill is lowered by
  that burst, so burst plus refill never exceeds the setting in a minute;
* headroom of ``order_reserved_per_minute`` that market-data and account
  reads may not use (a second bucket of ``requests_per_minute -
  order_reserved_per_minute`` for them), so order sends, replaces, cancels and
  order reads are never starved by quote polling;
* one ``Retry-After`` cooldown per endpoint family (all market data shares
  ``market_data``).

Market data waits up to ``market_data_max_wait_seconds`` for budget, then
raises :class:`~lumibot.tools.http_retry_after.RateLimitedError`. Orders never
fail on the local budget; at worst they wait for the next token.

Configuration (config dict wins over environment):
``SCHWAB_REQUESTS_PER_MINUTE`` (default 100),
``SCHWAB_ORDER_RESERVED_REQUESTS_PER_MINUTE`` (default 20),
``SCHWAB_MARKET_DATA_MAX_WAIT_SECONDS`` (default 2),
``SCHWAB_REQUEST_BURST_SECONDS`` (default 6).

The Schwab application limit (about 120 per minute) is shared by every process
that uses the same Schwab app; this pacer only holds one process's share.
"""

import os
import threading
import time
import weakref
from typing import Callable, Optional

from lumibot._lazy_imports import LazyLogger
from lumibot.tools.http_retry_after import RateLimitedError, retry_after_seconds

logger = LazyLogger(__name__)

DEFAULT_REQUESTS_PER_MINUTE = 100
DEFAULT_ORDER_RESERVED_PER_MINUTE = 20
DEFAULT_MARKET_DATA_MAX_WAIT_SECONDS = 2.0
DEFAULT_BURST_SECONDS = 6.0
_EPSILON = 1e-9
ACCOUNT_MAX_WAIT_SECONDS = 30.0
RETRY_AFTER_MIN_SECONDS = 0.25
RETRY_AFTER_MAX_SECONDS = 60.0

ORDER = "order"
DATA = "data"
ACCOUNT = "account"


def _setting(config, name, default, cast):
    value = None
    if config:
        try:
            value = config.get(name)
        except AttributeError:
            value = None
    if value is None or value == "":
        value = os.environ.get(name)
    if value is None or value == "":
        return default
    try:
        return cast(value)
    except (TypeError, ValueError):
        logger.warning(f"[Schwab] Ignoring invalid {name}={value!r}; using {default}.")
        return default


class SchwabRequestPacer:
    def __init__(
        self,
        requests_per_minute: float = DEFAULT_REQUESTS_PER_MINUTE,
        order_reserved_per_minute: float = DEFAULT_ORDER_RESERVED_PER_MINUTE,
        market_data_max_wait_seconds: float = DEFAULT_MARKET_DATA_MAX_WAIT_SECONDS,
        clock: Optional[Callable[[], float]] = None,
        sleep: Optional[Callable[[float], None]] = None,
        burst_seconds: float = DEFAULT_BURST_SECONDS,
    ):
        self.requests_per_minute = max(0.0, float(requests_per_minute))
        self.order_reserved_per_minute = min(
            max(0.0, float(order_reserved_per_minute)), self.requests_per_minute
        )
        self.market_data_max_wait_seconds = max(0.0, float(market_data_max_wait_seconds))
        self.burst_seconds = min(60.0, max(0.0, float(burst_seconds)))
        self._clock = clock or time.monotonic
        self._sleep = sleep or time.sleep
        self._lock = threading.Lock()
        # Two buckets. "all" gates every request; "data" also gates market data and
        # account reads, which may use at most requests_per_minute minus the order
        # reserve. Each bucket's burst plus a minute of refill equals its per-minute
        # limit, so no 60-second window (start-up included) can exceed it.
        self._buckets = {}
        for name, per_minute in (
            ("all", self.requests_per_minute),
            ("data", self.requests_per_minute - self.order_reserved_per_minute),
        ):
            capacity = min(per_minute, max(1.0, per_minute * self.burst_seconds / 60.0)) if per_minute > 0 else 0.0
            self._buckets[name] = {
                "capacity": capacity,
                "rate": max(0.0, per_minute - capacity) / 60.0,
                "tokens": capacity,
            }
        self._updated = self._clock()
        self._cooldowns = {}
        self._cooldown_attempts = {}
        # Callables (kind, seconds_waited) told about each request; see lumibot.tools.speed_summary.
        self.listeners = []
        for attr in ("requests_per_minute", "order_reserved_per_minute"):
            value = getattr(self, attr)
            if float(value).is_integer():
                setattr(self, attr, int(value))

    @classmethod
    def from_config(cls, config=None, **kwargs) -> "SchwabRequestPacer":
        return cls(
            requests_per_minute=_setting(config, "SCHWAB_REQUESTS_PER_MINUTE", DEFAULT_REQUESTS_PER_MINUTE, float),
            order_reserved_per_minute=_setting(
                config, "SCHWAB_ORDER_RESERVED_REQUESTS_PER_MINUTE", DEFAULT_ORDER_RESERVED_PER_MINUTE, float
            ),
            market_data_max_wait_seconds=_setting(
                config, "SCHWAB_MARKET_DATA_MAX_WAIT_SECONDS", DEFAULT_MARKET_DATA_MAX_WAIT_SECONDS, float
            ),
            burst_seconds=_setting(config, "SCHWAB_REQUEST_BURST_SECONDS", DEFAULT_BURST_SECONDS, float),
            **kwargs,
        )

    @property
    def enabled(self) -> bool:
        return self.requests_per_minute > 0

    def _refill(self, now: float) -> None:
        elapsed = max(0.0, now - self._updated)
        self._updated = now
        for bucket in self._buckets.values():
            bucket["tokens"] = min(bucket["capacity"], bucket["tokens"] + elapsed * bucket["rate"])

    @staticmethod
    def _wait_for(bucket) -> float:
        """Seconds until this bucket holds one whole token (inf when it never refills)."""
        missing = 1.0 - bucket["tokens"]
        if missing <= _EPSILON:
            return 0.0
        # At least a millisecond: a wait too small to move the clock would spin.
        return max(0.001, missing / bucket["rate"]) if bucket["rate"] > 0 else float("inf")

    def acquire(self, kind: str = DATA) -> None:
        """Take one request from the budget, waiting or raising as described above."""
        started = self._clock()
        try:
            self._acquire(kind)
        except Exception:
            raise
        else:
            waited = max(0.0, self._clock() - started)
            for listener in list(getattr(self, "listeners", ()) or ()):
                try:
                    listener(kind, waited)
                except Exception:
                    pass

    def _acquire(self, kind: str = DATA) -> None:
        if not self.enabled:
            return
        if kind == ORDER:
            # Orders never fail on the local budget: take the token now (the balance
            # may go below zero) and wait until it is paid back, so the per-minute
            # limit still holds and later reads wait behind the order.
            with self._lock:
                self._refill(self._clock())
                bucket = self._buckets["all"]
                bucket["tokens"] -= 1.0
                wait = (-bucket["tokens"] / bucket["rate"]) if bucket["tokens"] < 0 and bucket["rate"] > 0 else 0.0
            if wait > 0:
                logger.info(f"[Schwab] Order request waiting {wait:.2f} s for the local request budget.")
                self._sleep(wait)
            return

        max_wait = ACCOUNT_MAX_WAIT_SECONDS if kind == ACCOUNT else self.market_data_max_wait_seconds
        deadline = self._clock() + max_wait
        while True:
            with self._lock:
                now = self._clock()
                self._refill(now)
                everything, data = self._buckets["all"], self._buckets["data"]
                if everything["tokens"] >= 1.0 - _EPSILON and data["tokens"] >= 1.0 - _EPSILON:
                    everything["tokens"] -= 1.0
                    data["tokens"] -= 1.0
                    return
                wait = max(self._wait_for(everything), self._wait_for(data))
            remaining = deadline - now
            if wait > remaining + 1e-9:
                retry = wait if wait != float("inf") else 60.0
                raise RateLimitedError(
                    f"Schwab local request budget used up ({self.requests_per_minute:g} requests per minute, "
                    f"{self.order_reserved_per_minute:g} kept for orders); retry after {retry:.1f} seconds",
                    retry_after_seconds=retry,
                )
            self._sleep(wait)

    # ----- Retry-After cooldowns ------------------------------------------

    def cooldown_remaining(self, family: str, now: Optional[float] = None) -> float:
        now = self._clock() if now is None else now
        with self._lock:
            return max(0.0, float(self._cooldowns.get(family, 0.0)) - now)

    def check_cooldown(self, family: str, now: Optional[float] = None, label: str = "request") -> None:
        remaining = self.cooldown_remaining(family, now)
        if remaining > 0:
            raise RateLimitedError(
                f"Schwab {label} rate limit (HTTP 429) still in effect; retry after {remaining:.1f} seconds",
                retry_after_seconds=remaining,
            )

    def record_429(self, family: str, response, now: Optional[float] = None) -> float:
        now = self._clock() if now is None else now
        wait = retry_after_seconds(getattr(response, "headers", None) or {})
        with self._lock:
            attempts = int(self._cooldown_attempts.get(family, 0)) + 1
            self._cooldown_attempts[family] = attempts
            if wait is None:
                wait = float(min(2 ** min(attempts, 5), 30))
            wait = max(RETRY_AFTER_MIN_SECONDS, min(float(wait), RETRY_AFTER_MAX_SECONDS))
            self._cooldowns[family] = now + wait
        logger.warning(f"[Schwab] {family} requests rate limited by Schwab; waiting {wait:.1f} seconds.")
        return wait

    def clear_cooldown(self, family: str) -> None:
        with self._lock:
            self._cooldowns.pop(family, None)
            self._cooldown_attempts.pop(family, None)


_FALLBACK = weakref.WeakKeyDictionary()
_FALLBACK_LOCK = threading.Lock()
_ATTR = "_lumibot_request_pacer"


def attach_pacer(client, pacer: SchwabRequestPacer) -> SchwabRequestPacer:
    """Use ``pacer`` for every request made through ``client``."""
    try:
        setattr(client, _ATTR, pacer)
    except (AttributeError, TypeError):
        with _FALLBACK_LOCK:
            _FALLBACK[client] = pacer
    return pacer


def pacer_for_client(client, config=None) -> SchwabRequestPacer:
    """Return the pacer shared by everything that uses this Schwab client.

    The broker and its data source share one client in live trading, so they
    also share one budget and one set of cooldowns.
    """
    pacer = getattr(client, _ATTR, None)
    if isinstance(pacer, SchwabRequestPacer):
        return pacer
    with _FALLBACK_LOCK:
        pacer = getattr(client, _ATTR, None)
        if isinstance(pacer, SchwabRequestPacer):
            return pacer
        try:
            existing = _FALLBACK.get(client)
        except TypeError:
            existing = None
        if existing is not None:
            return existing
        pacer = SchwabRequestPacer.from_config(config)
        try:
            setattr(client, _ATTR, pacer)
        except (AttributeError, TypeError):
            try:
                _FALLBACK[client] = pacer
            except TypeError:
                pass
        return pacer
