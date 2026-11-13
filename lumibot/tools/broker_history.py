"""Broker price history for backtests: one small adapter per broker, one generic contract.

A backtest on broker history reads the bars the user's own broker connection keeps, through
the same shared bar cache as that broker's live charts and bots (one cache per connection).
Each adapter states what its broker keeps and how fast it may be asked:

* asset types it serves (stocks and ETFs only),
* how far back each bar size goes,
* requests per minute.

Anything outside those limits raises :class:`BrokerHistoryUnavailable` with a message that
names the limit. Nothing switches to another source, and nothing fills a missing bar: a bar
the broker did not return is missing.

Prices are what the broker returns. Schwab and Tradier price history is split-adjusted only
(not adjusted for cash dividends), and these adapters return no dividend column, so cash
dividends are not credited in broker-history backtests (see docs/BROKER_HISTORY_BACKTESTING.md).
"""

from __future__ import annotations

import time
from collections import deque
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Callable

import pandas as pd

NY = "America/New_York"
SUPPORTED_ASSET_TYPES = frozenset({"stock"})  # LumiBot models ETFs as stock assets


class BrokerHistoryUnavailable(LookupError):
    """The request is outside what this broker's history provides. The message names the limit."""


class BrokerHistoryRateLimited(RuntimeError):
    """The broker kept refusing requests for being over its rate limit."""


@dataclass(frozen=True)
class BrokerHistoryLimits:
    """What one broker's price-history API keeps and allows (from the broker's own documentation)."""

    requests_per_minute: int
    earliest_daily: date | None = None  # None: full listing history
    intraday_days: dict[int, int] = field(default_factory=dict)  # bar minutes -> days kept back from today
    notes: str = ""


def asset_type_name(asset) -> str:
    value = getattr(asset, "asset_type", "")
    return str(getattr(value, "value", value) or "").lower()


def daily_labels(index) -> pd.DatetimeIndex:
    """Daily bar timestamps at the session close, 16:00 New York (as the Yahoo daily source does).

    A daily bar holds the day's close, so it must not be visible before the session ends: a
    strategy running during day D sees bars through D-1, as with Yahoo daily bars.

    Brokers stamp a daily bar with a date string or with midnight in their own time zone.
    Adding 12 hours to the UTC instant and taking the date gives the trading date for any of
    those midnights (New York, Chicago, UTC) without assuming which one a broker uses.
    """
    stamps = pd.DatetimeIndex(pd.to_datetime(index))
    if stamps.tz is None:
        days = stamps.normalize()
    else:
        days = (stamps.tz_convert("UTC") + pd.Timedelta(hours=12)).tz_localize(None).normalize()
    return (pd.DatetimeIndex(days) + pd.Timedelta(hours=16)).tz_localize(NY)


class RateLimiter:
    """Sliding one-minute window: wait before a request that would exceed ``per_minute``."""

    def __init__(self, per_minute: int, *, clock: Callable[[], float] = time.monotonic,
                 sleep: Callable[[float], None] = time.sleep):
        self.per_minute = max(1, int(per_minute))
        self.clock = clock
        self.sleep = sleep
        self.sent: deque[float] = deque()

    def acquire(self) -> float:
        """Wait if needed; return the seconds waited."""
        now = self.clock()
        while self.sent and now - self.sent[0] >= 60.0:
            self.sent.popleft()
        waited = 0.0
        if len(self.sent) >= self.per_minute:
            waited = 60.0 - (now - self.sent[0]) + 0.05
            if waited > 0:
                self.sleep(waited)
            now = self.clock()
            while self.sent and now - self.sent[0] >= 60.0:
                self.sent.popleft()
        self.sent.append(self.clock())
        return max(0.0, waited)


class BrokerHistoryAdapter:
    """One broker's price history. Subclasses implement :meth:`_daily` and :meth:`is_rate_limited`."""

    name = "broker"
    limits = BrokerHistoryLimits(requests_per_minute=60)
    # Waits after the broker refuses a request for its rate limit (seconds), then give up.
    RATE_LIMIT_WAITS = (15.0, 30.0, 60.0)

    def __init__(self, *, scope: str | None, sleep: Callable[[float], None] = time.sleep,
                 clock: Callable[[], float] = time.monotonic):
        self.scope = scope
        self.sleep = sleep
        self.limiter = RateLimiter(self.limits.requests_per_minute, clock=clock, sleep=sleep)

    # -- limits -----------------------------------------------------------------------------

    def check_asset(self, asset) -> None:
        kind = asset_type_name(asset)
        if kind not in SUPPORTED_ASSET_TYPES:
            raise BrokerHistoryUnavailable(
                f"{self.title} history backtests support stocks and ETFs only; {getattr(asset, 'symbol', asset)} "
                f"is a {kind or 'unknown'} asset. Choose a data source that has {kind or 'this'} history. "
                "The backtest does not switch sources on its own."
            )

    def check_daily_window(self, start: datetime) -> None:
        earliest = self.limits.earliest_daily
        if earliest is not None and pd.Timestamp(start).date() < earliest:
            raise BrokerHistoryUnavailable(
                f"{self.title} daily history starts {earliest.isoformat()}; this backtest needs daily bars from "
                f"{pd.Timestamp(start).date().isoformat()} (backtest start plus lookback). Start the backtest later "
                "or shorten the lookback."
            )

    def intraday_unavailable(self, minutes: int) -> BrokerHistoryUnavailable:
        kept = self.limits.intraday_days.get(int(minutes))
        depth = f"keeps {minutes}-minute bars for about {kept} days back from today" if kept else (
            f"has no {minutes}-minute bars")
        return BrokerHistoryUnavailable(
            f"Broker-history backtests use daily bars for now. {self.title} {depth}, so intraday history "
            "cannot cover most backtests. Use a daily sleeptime and daily bars, or another data source "
            "for intraday bars."
        )

    @property
    def title(self) -> str:
        return self.name.capitalize()

    # -- reads ------------------------------------------------------------------------------

    def daily_bars(self, symbol: str, start: datetime, end: datetime) -> pd.DataFrame | None:
        """Real daily bars for ``[start, end]``, stamped at the 16:00 New York session close, or None."""
        from lumibot.tools import backtest_telemetry

        with backtest_telemetry.provider_scope(self.name, "stock"):
            for attempt in range(len(self.RATE_LIMIT_WAITS) + 1):
                waited = self.limiter.acquire()
                if waited:
                    backtest_telemetry.record_wait("rate_limit", waited)
                started = time.perf_counter()
                try:
                    frame = self._daily(symbol, start, end)
                except Exception as exc:
                    backtest_telemetry.record_provider_request(time.perf_counter() - started)
                    if not self.is_rate_limited(exc):
                        raise
                    if attempt >= len(self.RATE_LIMIT_WAITS):
                        raise BrokerHistoryRateLimited(
                            f"{self.title} kept refusing price-history requests for its rate limit "
                            f"({self.limits.requests_per_minute} requests per minute) after "
                            f"{len(self.RATE_LIMIT_WAITS)} waits. Retry the backtest later."
                        ) from exc
                    wait = self.RATE_LIMIT_WAITS[attempt]
                    backtest_telemetry.record_retry()
                    backtest_telemetry.record_wait("rate_limit", wait)
                    self.sleep(wait)
                    continue
                rows = 0 if frame is None else len(frame)
                backtest_telemetry.record_provider_request(time.perf_counter() - started, rows=rows)
                return frame
        return None

    def _daily(self, symbol: str, start: datetime, end: datetime) -> pd.DataFrame | None:
        raise NotImplementedError

    def is_rate_limited(self, exc: Exception) -> bool:
        return False


def _ohlcv(frame: pd.DataFrame) -> pd.DataFrame:
    columns = [c for c in ("open", "high", "low", "close", "volume") if c in frame.columns]
    out = frame[columns].astype(float)
    return out[~out.index.duplicated(keep="last")].sort_index()


class TradierHistory(BrokerHistoryAdapter):
    """Tradier ``GET /v1/markets/history`` through ``lumiwealth_tradier``."""

    name = "tradier"
    # Tradier documentation (checked 2026-11-11): daily and weekly bars for the full listing,
    # 1-minute time and sales for 20 days, 5 and 15 minutes for 40 days, 120 market-data requests
    # per minute; over the limit Tradier answers HTTP 400 "Quota Violation". No expired options.
    limits = BrokerHistoryLimits(requests_per_minute=120, earliest_daily=None,
                                 intraday_days={1: 20, 5: 40, 15: 40})

    def __init__(self, client, *, scope: str | None, **kwargs):
        super().__init__(scope=scope, **kwargs)
        self.client = client  # lumiwealth_tradier.Tradier

    def _daily(self, symbol, start, end):
        try:
            frame = self.client.market.get_historical_quotes(
                symbol, interval="daily", session_filter="open",
                start_date=pd.Timestamp(start).date(), end_date=pd.Timestamp(end).date(),
            )
        except LookupError:
            return None  # Tradier answered with no history for the range
        if frame is None or frame.empty:
            return None
        frame = frame.copy()
        frame.index = daily_labels(frame.index)
        return _ohlcv(frame)

    def is_rate_limited(self, exc):
        text = str(exc).lower()
        return "quota violation" in text or " 429" in text or "too many requests" in text


class SchwabHistory(BrokerHistoryAdapter):
    """Schwab ``GET /marketdata/v1/pricehistory`` through ``schwab-py``."""

    name = "schwab"
    # Schwab documentation (checked 2026-11-11): daily and weekly bars back to 1985, 5 to 30
    # minutes about 9 months, 1 minute about 48 days, 120 requests per minute, prices
    # split-adjusted only. Refresh tokens expire after 7 days.
    limits = BrokerHistoryLimits(requests_per_minute=120, earliest_daily=date(1985, 1, 1),
                                 intraday_days={1: 48, 5: 270, 10: 270, 15: 270, 30: 270})

    def __init__(self, client, *, scope: str | None, **kwargs):
        super().__init__(scope=scope, **kwargs)
        self.client = client  # schwab.client.Client

    def _daily(self, symbol, start, end):
        response = self.client.get_price_history_every_day(
            symbol, start_datetime=pd.Timestamp(start).to_pydatetime(),
            end_datetime=pd.Timestamp(end).to_pydatetime(), need_extended_hours_data=False,
        )
        status = getattr(response, "status_code", 200)
        if status != 200:
            raise RuntimeError(f"Schwab price history returned {status}: {getattr(response, 'text', '')}")
        data = response.json() if hasattr(response, "json") else response
        if not isinstance(data, dict) or "candles" not in data:
            raise RuntimeError("Schwab price history response has no candles field")
        frame = pd.DataFrame(data["candles"])
        if frame.empty:
            return None
        # Epoch milliseconds are UTC instants; daily candles are stamped at a local midnight.
        frame.index = daily_labels(pd.to_datetime(frame["datetime"], unit="ms", utc=True))
        return _ohlcv(frame)

    def is_rate_limited(self, exc):
        text = str(exc).lower()
        return "returned 429" in text or "too many requests" in text


def make_adapter(name: str, config=None, **kwargs) -> BrokerHistoryAdapter:
    """Build the adapter for ``name`` ("schwab" or "tradier") from config or the environment."""
    from lumibot.tools.shared_bar_cache import credential_scope

    name = str(name).strip().lower()
    config = dict(config or {})
    if name == "tradier":
        from lumibot.credentials import TRADIER_CONFIG

        token = config.get("ACCESS_TOKEN") or TRADIER_CONFIG.get("ACCESS_TOKEN")
        account = config.get("ACCOUNT_NUMBER") or TRADIER_CONFIG.get("ACCOUNT_NUMBER")
        paper = config.get("PAPER", TRADIER_CONFIG.get("PAPER", True))
        if not token or not account:
            raise BrokerHistoryUnavailable(
                "Tradier history backtests need TRADIER_ACCESS_TOKEN and TRADIER_ACCOUNT_NUMBER.")
        from lumiwealth_tradier import Tradier

        # Same scope as TradierData: charts, bots and backtests on one connection share bars.
        scope = credential_scope("tradier", account, "paper" if paper else "live")
        return TradierHistory(Tradier(account, token, paper), scope=scope, **kwargs)
    if name == "schwab":
        from lumibot.credentials import SCHWAB_CONFIG
        from lumibot.data_sources.schwab_data import SchwabData

        account = config.get("SCHWAB_ACCOUNT_NUMBER") or SCHWAB_CONFIG.get("SCHWAB_ACCOUNT_NUMBER")
        client = config.get("client") or SchwabData.create_schwab_client(
            config.get("SCHWAB_APP_KEY") or SCHWAB_CONFIG.get("SCHWAB_APP_KEY"),
            config.get("SCHWAB_APP_SECRET") or SCHWAB_CONFIG.get("SCHWAB_APP_SECRET"),
            account,
        )
        if client is None or not account:
            raise BrokerHistoryUnavailable(
                "Schwab history backtests need a connected Schwab client (SCHWAB_APP_KEY, SCHWAB_APP_SECRET, "
                "SCHWAB_ACCOUNT_NUMBER and a token). Schwab refresh tokens expire after 7 days.")
        # Same scope as the Schwab broker's data source.
        return SchwabHistory(client, scope=credential_scope("schwab", account), **kwargs)
    raise ValueError(f"Unknown broker history source {name!r}; choose 'schwab' or 'tradier'.")
