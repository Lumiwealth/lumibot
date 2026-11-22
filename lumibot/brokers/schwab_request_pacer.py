"""Client-side request pacing for the Schwab Trader API.

Schwab counts requests per minute and answers HTTP 429 when a limit is
crossed. A developer app can be shared by several bots or accounts, so one
process should spend only its own share. This module keeps, per Schwab client:

* a token bucket of ``requests_per_minute`` (0 disables pacing);
* headroom of ``order_reserved_per_minute`` that market-data and account
  reads may not use, so order sends, replaces, cancels and order reads are
  never starved by quote polling;
* one ``Retry-After`` cooldown per endpoint family (all market data shares
  ``market_data``).

Market data waits up to ``market_data_max_wait_seconds`` for budget, then
raises :class:`~lumibot.tools.http_retry_after.RateLimitedError`. Orders never
fail on the local budget; at worst they wait for the next token.

Configuration (config dict wins over environment):
``SCHWAB_REQUESTS_PER_MINUTE`` (default 100),
``SCHWAB_ORDER_RESERVED_REQUESTS_PER_MINUTE`` (default 20),
``SCHWAB_MARKET_DATA_MAX_WAIT_SECONDS`` (default 2).
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
    ):
        self.requests_per_minute = max(0.0, float(requests_per_minute))
        self.order_reserved_per_minute = min(
            max(0.0, float(order_reserved_per_minute)), self.requests_per_minute
        )
        self.market_data_max_wait_seconds = max(0.0, float(market_data_max_wait_seconds))
        self._clock = clock or time.monotonic
        self._sleep = sleep or time.sleep
        self._lock = threading.Lock()
        self._tokens = self.requests_per_minute
        self._updated = self._clock()
        self._cooldowns = {}
        self._cooldown_attempts = {}
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
            **kwargs,
        )

    @property
    def enabled(self) -> bool:
        return self.requests_per_minute > 0

    def _refill(self, now: float) -> None:
        elapsed = max(0.0, now - self._updated)
        self._updated = now
        self._tokens = min(self.requests_per_minute, self._tokens + elapsed * self.requests_per_minute / 60.0)

    def acquire(self, kind: str = DATA) -> None:
        """Take one request from the budget, waiting or raising as described above."""
        if not self.enabled:
            return
        rate = self.requests_per_minute / 60.0
        if kind == ORDER:
            with self._lock:
                self._refill(self._clock())
                self._tokens -= 1.0
                wait = max(0.0, -self._tokens / rate)
            if wait > 0:
                logger.info(f"[Schwab] Order request waiting {wait:.2f} s for the local request budget.")
                self._sleep(wait)
            return

        max_wait = ACCOUNT_MAX_WAIT_SECONDS if kind == ACCOUNT else self.market_data_max_wait_seconds
        floor = self.order_reserved_per_minute + 1.0
        deadline = self._clock() + max_wait
        while True:
            with self._lock:
                now = self._clock()
                self._refill(now)
                if self._tokens >= floor:
                    self._tokens -= 1.0
                    return
                wait = (floor - self._tokens) / rate
            remaining = deadline - now
            if wait > remaining + 1e-9:
                raise RateLimitedError(
                    f"Schwab local request budget used up ({self.requests_per_minute:g} requests per minute, "
                    f"{self.order_reserved_per_minute:g} kept for orders); retry after {wait:.1f} seconds",
                    retry_after_seconds=wait,
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
