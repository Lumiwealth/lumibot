"""Crypto history from before a coin was listed is asked for once, not every bar.

A daily crypto backtest whose window starts before the coin existed on the
exchange (for example a ten-year window for a coin listed four years ago) asked
the exchange again, on every daily iteration, for minute candles from the
window start up to the listing. The exchange answered "nothing" each time, and
an empty answer was never remembered, so every simulated day repeated dozens of
exchange requests. These tests use a fake exchange that has no candles before a
listing time, the way real exchanges answer.
"""

from __future__ import annotations

import logging
import threading
from datetime import datetime, timedelta, timezone

import ccxt
import pandas as pd
import pytest

from lumibot.tools import CcxtCacheDB
from tests.test_ccxt_shared_bar_cache import shared  # noqa: F401  (pytest fixture)

NOW = datetime(2026, 3, 20, 12, 0)
LISTED = datetime(2024, 6, 1)
MINUTE_MS = 60_000
DAY_MS = 86_400_000


def _ms(value: datetime) -> int:
    return int(pd.Timestamp(value, tz="UTC").value // 1_000_000)


class ListedLaterExchange:
    """A CCXT-shaped exchange that lists COIN/USDT at ``LISTED``.

    Like Binance, a request whose ``since`` is before the listing answers with
    the first candles that exist (from the listing on).
    """

    has = {"fetchOHLCV": True}

    def __init__(self, exchange_id="binance"):
        self.id = exchange_id
        self.markets = {"COIN/USDT": {"symbol": "COIN/USDT", "base": "COIN", "quote": "USDT", "spot": True}}
        self.calls = []
        self.lock = threading.Lock()

    parse8601 = staticmethod(ccxt.Exchange.parse8601)

    def fetch_ohlcv(self, symbol, timeframe="1m", since=None, limit=None, params=None):
        with self.lock:
            self.calls.append({"timeframe": timeframe, "since": since, "limit": limit})
        step = DAY_MS if timeframe == "1d" else MINUTE_MS
        stamp = max(since - since % step, _ms(LISTED))
        rows = []
        while len(rows) < limit and stamp < _ms(NOW):
            base = 100 + (stamp // step) % 97
            rows.append([stamp, base, base + 1, base - 1, base + 0.5, 3.0])
            stamp += step
        return rows

    def before_listing_calls(self):
        return [c for c in self.calls if c["since"] < _ms(LISTED)]


def _cache(api):
    cache = CcxtCacheDB.__new__(CcxtCacheDB)
    cache.logger = logging.getLogger("test-ccxt-prelisting")
    cache.exchange_id = api.id
    cache.api = api
    cache.max_download_limit = 50_000
    cache._current_utc_naive = lambda: NOW
    return cache


def test_second_request_for_a_window_that_starts_before_listing_makes_zero_exchange_calls(shared):  # noqa: F811
    api = ListedLaterExchange()
    cache = _cache(api)
    start, end = datetime(2024, 3, 1), datetime(2024, 6, 3)
    first = cache.download_ohlcv("COIN/USDT", "1m", start, end)
    assert not first.empty and first.index.min() == LISTED
    assert api.before_listing_calls()

    api.calls.clear()
    again = cache.download_ohlcv("COIN/USDT", "1m", start, end)

    assert api.calls == []
    pd.testing.assert_frame_equal(first, again)


def test_a_window_entirely_before_listing_is_asked_once(shared):  # noqa: F811
    api = ListedLaterExchange()
    cache = _cache(api)
    start, end = datetime(2024, 1, 1), datetime(2024, 4, 1)
    assert cache.download_ohlcv("COIN/USDT", "1m", start, end).empty
    assert api.calls

    api.calls.clear()
    assert cache.download_ohlcv("COIN/USDT", "1m", start, end).empty
    assert api.calls == []


def test_a_new_task_reuses_the_remembered_listing_gap(shared):  # noqa: F811
    first_api = ListedLaterExchange()
    _cache(first_api).download_ohlcv("COIN/USDT", "1m", datetime(2024, 3, 1), datetime(2024, 6, 3))

    shared("task-2")  # empty local disk and memory, same remote store
    second_api = ListedLaterExchange()
    _cache(second_api).download_ohlcv("COIN/USDT", "1m", datetime(2024, 3, 1), datetime(2024, 6, 3))
    assert second_api.calls == []


def test_real_candles_inside_a_partly_answered_chunk_are_still_fetched(shared):  # noqa: F811
    """A first answer that starts late must not hide candles that do exist.

    Some exchanges can return a page that skips its start. The skipped range is
    asked for again on its own; only a zero-candle answer for it is remembered.
    """

    class SkipsStartOnce(ListedLaterExchange):
        def __init__(self):
            super().__init__()
            self.skipped = False

        def fetch_ohlcv(self, symbol, timeframe="1m", since=None, limit=None, params=None):
            rows = super().fetch_ohlcv(symbol, timeframe, since, limit, params)
            if not self.skipped and rows:
                self.skipped = True
                return rows[10:]
            return rows

    api = SkipsStartOnce()
    cache = _cache(api)
    start, end = LISTED, LISTED + timedelta(hours=2)
    frame = cache.download_ohlcv("COIN/USDT", "1m", start, end)
    assert frame.index.min() == LISTED
    assert len(frame) == 121


def test_daily_last_price_never_asks_again_for_minutes_before_listing(shared, monkeypatch, tmp_path):  # noqa: F811
    """A daily strategy reads its price each day of a window that starts years before the listing."""
    from lumibot.backtesting.routed_backtesting import RoutedBacktestingPandas
    from lumibot.backtesting.thetadata_backtesting_pandas import ThetaDataBacktestingPandas
    from lumibot.entities import Asset
    import lumibot.tools.thetadata_helper as theta

    monkeypatch.setattr(ThetaDataBacktestingPandas, "kill_processes_by_name", lambda *_a, **_k: None)
    monkeypatch.setattr(theta, "reset_theta_terminal_tracking", lambda *_a, **_k: None)
    router = RoutedBacktestingPandas(
        datetime_start=datetime(2024, 5, 20, tzinfo=timezone.utc),
        datetime_end=datetime(2024, 6, 5, tzinfo=timezone.utc),
        config={"backtesting_data_routing": {"crypto": "okx"}},
        username="test", password="test", use_quote_data=False,
        show_progress_bar=False, log_backtest_progress_to_file=False,
    )
    api = ListedLaterExchange("okx")
    router._registry._adapters["ccxt"]._cache_by_exchange["okx"] = _cache(api)
    coin, usdt = Asset("COIN", "crypto"), Asset("USDT", "crypto")

    prices = {}
    day = datetime(2024, 5, 20, tzinfo=timezone.utc)
    while day <= datetime(2024, 6, 4, tzinfo=timezone.utc):
        router._datetime = day
        before = len(api.before_listing_calls())
        prices[day.date()] = router.get_last_price(coin, quote=usdt)
        if day > datetime(2024, 5, 21, tzinfo=timezone.utc):
            assert len(api.before_listing_calls()) == before, f"asked again for pre-listing minutes on {day.date()}"
        day += timedelta(days=1)

    assert prices[datetime(2024, 5, 25).date()] is None
    assert prices[datetime(2024, 6, 4).date()] is not None
