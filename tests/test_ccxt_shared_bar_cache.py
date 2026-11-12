"""Crypto backtest bars from public CCXT exchanges go through the shared bar cache.

Production: a BTC/USD one-minute backtest downloaded every bar from the exchange
again on every run (34 s for one month on the first run and on the repeat; 357 s
for 12 months), because the CCXT backtest cache lived only on the task's own disk.
Public exchange candles are the same for everyone, so they belong in the shared
cache (local file plus S3), keyed by exchange and symbol.
"""

from __future__ import annotations

import logging
import threading
from datetime import datetime, timedelta

import ccxt
import pandas as pd
import pytest

import lumibot.tools.ccxt_data_store as ccxt_data_store
from lumibot import constants
from lumibot.tools import CcxtCacheDB, backtest_cache, parquet_series_cache
from lumibot.tools import shared_bar_cache as sbc
from lumibot.tools.backtest_cache import BacktestCacheManager, BacktestCacheSettings, CacheMode

# Long after every requested window, so every bar in these tests is settled.
NOW = datetime(2026, 3, 20, 12, 0)
MINUTE_MS = 60_000


class FakeExchange:
    """Shaped like a CCXT exchange: fetch_ohlcv returns [[ms, o, h, l, c, v], ...]."""

    has = {"fetchOHLCV": True}

    def __init__(self, exchange_id="coinbase", price_offset=0.0, missing_minutes=(7,)):
        self.id = exchange_id
        self.markets = {"BTC/USD": {"symbol": "BTC/USD", "base": "BTC", "quote": "USD", "spot": True}}
        self.price_offset = price_offset
        self.missing_minutes = set(missing_minutes)
        self.calls = []
        self.lock = threading.Lock()

    parse8601 = staticmethod(ccxt.Exchange.parse8601)

    def fetch_ohlcv(self, symbol, timeframe="1m", since=None, limit=None, params=None):
        with self.lock:
            self.calls.append({"symbol": symbol, "timeframe": timeframe, "since": since, "limit": limit})
        now_ms = int(pd.Timestamp(NOW, tz="UTC").value // 1_000_000)
        rows = []
        stamp = since - since % MINUTE_MS
        while len(rows) < limit and stamp < now_ms:
            minute = (stamp // MINUTE_MS) % 60
            if minute not in self.missing_minutes:  # no trade in that minute: the exchange sends no candle
                base = 40_000 + (stamp // MINUTE_MS) % 997 + self.price_offset
                # More than three decimals on purpose (small coins and fractional volume).
                rows.append([stamp, base + 0.123456, base + 1.5, base - 1.25, base + 0.00001234, 0.00012345])
            stamp += MINUTE_MS
        return rows


def _cache(api, exchange_id="coinbase"):
    cache = CcxtCacheDB.__new__(CcxtCacheDB)
    cache.logger = logging.getLogger("test-ccxt-shared")
    cache.exchange_id = exchange_id
    cache.api = api
    cache.max_download_limit = 50_000
    cache._current_utc_naive = lambda: NOW
    return cache


class Remote:
    def __init__(self):
        self.objects = {}
        self.lock = threading.Lock()

    def get_object(self, Bucket, Key):  # noqa: N803 - boto3 signature
        with self.lock:
            if Key not in self.objects:
                raise FileNotFoundError(Key)
            body = self.objects[Key]

        class Body:
            def read(self_inner):
                return body

        return {"Body": Body()}

    def put_object(self, Bucket, Key, Body):  # noqa: N803
        with self.lock:
            self.objects[Key] = Body


def _use_folder(monkeypatch, folder):
    for module in (constants, backtest_cache, ccxt_data_store):
        monkeypatch.setattr(module, "LUMIBOT_CACHE_FOLDER", str(folder))
    sbc._reset_process_memory()


@pytest.fixture
def shared(tmp_path, monkeypatch):
    for name in ("LUMIBOT_SHARED_BARS_SCOPE", "LUMIBOT_SHARED_BARS_SETTLE_MINUTES"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("LUMIBOT_SHARED_BARS", "true")
    remote = Remote()
    manager = BacktestCacheManager(
        BacktestCacheSettings(
            backend="s3", mode=CacheMode.S3_READWRITE, bucket="synthetic",
            prefix="env/cache", region="us-east-1", version="v1",
        ),
        client_factory=lambda _: remote,
    )
    monkeypatch.setattr(parquet_series_cache, "get_backtest_cache", lambda: manager)
    _use_folder(monkeypatch, tmp_path / "task-1")

    def new_task(name):
        sbc.flush_shared_bar_writes(10)
        _use_folder(monkeypatch, tmp_path / name)

    yield new_task
    sbc.flush_shared_bar_writes(10)
    sbc._reset_process_memory()


def _download(cache, start, end):
    return cache.download_ohlcv("BTC/USD", "1m", start, end)


def test_second_task_makes_zero_exchange_requests_and_returns_identical_bars(shared):
    first_api = FakeExchange()
    start, end = datetime(2026, 3, 2), datetime(2026, 3, 3, 23, 59)
    first = _download(_cache(first_api), start, end)
    assert len(first) > 0 and first_api.calls

    shared("task-2")  # new task: empty local disk, empty memory, same S3
    second_api = FakeExchange()
    second = _download(_cache(second_api), start, end)

    assert second_api.calls == []
    pd.testing.assert_frame_equal(first, second)


def test_bars_are_real_exchange_rows_with_full_precision(shared):
    api = FakeExchange()
    frame = _download(_cache(api), datetime(2026, 3, 2), datetime(2026, 3, 2, 23, 59))
    # The exchange sent no candle for minute 7 of each hour: none is invented.
    assert not any(stamp.minute == 7 for stamp in frame.index)
    assert len(frame) == 24 * 59
    assert list(frame.columns) == ["open", "high", "low", "close", "volume", "missing"]
    assert frame.index.name == "datetime" and frame.index.tz is None
    first = frame.iloc[0]
    stamp_ms = int(pd.Timestamp(frame.index[0], tz="UTC").value // 1_000_000)
    base = 40_000 + (stamp_ms // MINUTE_MS) % 997
    assert first["open"] == pytest.approx(base + 0.123456, abs=1e-9)
    assert first["close"] == pytest.approx(base + 0.00001234, abs=1e-9)
    assert first["volume"] == pytest.approx(0.00012345, abs=1e-12)
    assert set(frame["missing"]) == {0}


def test_longer_window_fetches_only_the_missing_tail(shared):
    _download(_cache(FakeExchange()), datetime(2026, 3, 2), datetime(2026, 3, 3, 23, 59))
    shared("task-2")
    api = FakeExchange()
    frame = _download(_cache(api), datetime(2026, 3, 2), datetime(2026, 3, 5, 23, 59))
    assert api.calls
    first_missing_ms = int(pd.Timestamp("2026-03-04", tz="UTC").value // 1_000_000)
    # One bar of overlap before the gap is allowed (the cache widens gaps by one bar).
    assert min(call["since"] for call in api.calls) >= first_missing_ms - MINUTE_MS
    assert frame.index[0] == pd.Timestamp("2026-03-02 00:00")
    assert frame.index[-1] == pd.Timestamp("2026-03-05 23:59")


def test_one_day_costs_a_bounded_number_of_exchange_requests(shared):
    api = FakeExchange()
    _download(_cache(api), datetime(2026, 3, 2), datetime(2026, 3, 2, 23, 59))
    # 1,440 minutes at 300 candles per request: 5 pages, plus edge pages.
    assert len(api.calls) <= 7


def test_still_forming_bars_are_not_cached(shared, monkeypatch):
    monkeypatch.setenv("LUMIBOT_SHARED_BARS_SETTLE_MINUTES", "15")
    day = datetime(NOW.year, NOW.month, NOW.day)
    _download(_cache(FakeExchange()), day, NOW)
    shared("task-2")
    api = FakeExchange()
    _download(_cache(api), day, NOW)
    assert api.calls, "the unsettled tail near the exchange clock must be fetched again"
    cut_ms = int(pd.Timestamp(NOW - timedelta(minutes=16), tz="UTC").value // 1_000_000)
    assert min(call["since"] for call in api.calls) >= cut_ms - 2 * MINUTE_MS


def test_two_exchanges_never_share_bars(shared):
    start, end = datetime(2026, 3, 2), datetime(2026, 3, 2, 23, 59)
    coinbase = _download(_cache(FakeExchange("coinbase")), start, end)
    kraken_api = FakeExchange("kraken", price_offset=500.0)
    kraken = _download(_cache(kraken_api, exchange_id="kraken"), start, end)
    assert kraken_api.calls
    assert (kraken["open"] - coinbase["open"]).round(6).eq(500.0).all()


def test_disabled_shared_cache_keeps_the_local_cache(shared, monkeypatch):
    monkeypatch.setenv("LUMIBOT_SHARED_BARS", "false")
    api = FakeExchange()
    cache = _cache(api)
    _download(cache, datetime(2026, 3, 2), datetime(2026, 3, 2, 23, 59))
    assert (ccxt_data_store.os.path.exists(cache.get_cache_file_name("BTC/USD", "1m")))
