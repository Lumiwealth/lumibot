from lumibot.tools import CcxtCacheDB
import pytest
import duckdb
from datetime import datetime
import os
import logging
import pandas as pd
import pytz

import lumibot.tools.ccxt_data_store as ccxt_data_store


# PYTHONWARNINGS="ignore::DeprecationWarning"; pytest test/test_ccxt_store.py


def _cache_without_live_exchange(tmp_path, monkeypatch):
    monkeypatch.setattr(ccxt_data_store, "LUMIBOT_CACHE_FOLDER", str(tmp_path))
    cache = CcxtCacheDB.__new__(CcxtCacheDB)
    cache.logger = logging.getLogger("test-ccxt-cache")
    cache.exchange_id = "coinbase"
    cache.max_download_limit = 50_000
    return cache


def _bars(*rows):
    return pd.DataFrame(
        rows,
        columns=["datetime", "open", "high", "low", "close", "volume"],
    )


def test_ccxt_cache_retries_transient_load_markets_failure(monkeypatch):
    attempts = {"count": 0}

    class FakeExchange:
        def load_markets(self):
            attempts["count"] += 1
            if attempts["count"] < 3:
                raise RuntimeError("temporary timeout")

    monkeypatch.setattr(ccxt_data_store.ccxt, "retryexchange", FakeExchange, raising=False)

    cache = CcxtCacheDB("retryexchange")

    assert cache.exchange_id == "retryexchange"
    assert attempts["count"] == 3


def test_ccxt_cache_does_not_forward_fill_missing_minute_rows(tmp_path, monkeypatch):
    cache = _cache_without_live_exchange(tmp_path, monkeypatch)

    def fake_get_barset(symbol, timeframe, limit, start, end):
        assert symbol == "BTC/USDT"
        assert timeframe == "1m"
        return _bars(
            (datetime(2026, 1, 1, 0, 0), 100.0, 101.0, 99.0, 100.5, 10.0),
            (datetime(2026, 1, 1, 0, 2), 102.0, 103.0, 101.0, 102.5, 12.0),
        )

    monkeypatch.setattr(cache, "_get_barset_from_api", fake_get_barset)

    df = cache.download_ohlcv(
        "BTC/USDT",
        "1m",
        datetime(2026, 1, 1, 0, 0),
        datetime(2026, 1, 1, 0, 2),
    )

    assert list(df.index) == [pd.Timestamp("2026-01-01 00:00:00"), pd.Timestamp("2026-01-01 00:02:00")]
    assert pd.Timestamp("2026-01-01 00:01:00") not in df.index
    assert df["missing"].tolist() == [0, 0]


def test_ccxt_cache_supports_hour_timeframe_without_synthesizing_gaps(tmp_path, monkeypatch):
    cache = _cache_without_live_exchange(tmp_path, monkeypatch)
    calls = []

    def fake_get_barset(symbol, timeframe, limit, start, end):
        calls.append({"symbol": symbol, "timeframe": timeframe, "limit": limit, "start": start, "end": end})
        return _bars(
            (datetime(2026, 1, 1, 0, 0), 100.0, 101.0, 99.0, 100.5, 10.0),
            (datetime(2026, 1, 1, 2, 0), 102.0, 103.0, 101.0, 102.5, 12.0),
        )

    monkeypatch.setattr(cache, "_get_barset_from_api", fake_get_barset)

    df = cache.download_ohlcv(
        "BTC/USDT",
        "1h",
        datetime(2026, 1, 1, 0, 0),
        datetime(2026, 1, 1, 2, 0),
    )

    assert calls[0]["timeframe"] == "1h"
    assert calls[0]["limit"] <= 24
    assert list(df.index) == [pd.Timestamp("2026-01-01 00:00:00"), pd.Timestamp("2026-01-01 02:00:00")]
    assert pd.Timestamp("2026-01-01 01:00:00") not in df.index


def test_ccxt_cache_converts_aware_request_bounds_to_utc_before_query(tmp_path, monkeypatch):
    cache = _cache_without_live_exchange(tmp_path, monkeypatch)
    ny = pytz.timezone("America/New_York")
    calls = []

    def fake_get_barset(symbol, timeframe, limit, start, end):
        calls.append({"symbol": symbol, "timeframe": timeframe, "start": start, "end": end})
        return _bars(
            (datetime(2026, 3, 15, 0, 0), 90.0, 91.0, 89.0, 90.5, 9.0),
            (datetime(2026, 3, 15, 4, 0), 100.0, 101.0, 99.0, 100.5, 10.0),
            (datetime(2026, 3, 15, 5, 0), 101.0, 102.0, 100.0, 101.5, 11.0),
        )

    monkeypatch.setattr(cache, "_get_barset_from_api", fake_get_barset)

    requested_start = ny.localize(datetime(2026, 3, 15, 0, 0))
    requested_end = ny.localize(datetime(2026, 3, 15, 0, 0))
    df = cache.download_ohlcv("BTC/USD", "1h", requested_start, requested_end)

    assert calls
    assert calls[0]["start"] == datetime(2026, 3, 15, 0, 0)
    assert calls[0]["end"] == datetime(2026, 3, 15, 23, 59, 59, 999999)
    assert list(df.index) == [pd.Timestamp("2026-03-15 04:00:00")]


def test_ccxt_cache_clamps_current_utc_day_download_end_to_now(tmp_path, monkeypatch):
    cache = _cache_without_live_exchange(tmp_path, monkeypatch)
    ny = pytz.timezone("America/New_York")
    now = datetime(2026, 6, 24, 18, 15, 0, 123456)
    requested_end_utc = datetime(2026, 6, 24, 4, 0)
    calls = []

    monkeypatch.setattr(cache, "_current_utc_naive", lambda: now)

    def fake_get_barset(symbol, timeframe, limit, start, end):
        calls.append({"symbol": symbol, "timeframe": timeframe, "start": start, "end": end})
        return _bars(
            (datetime(2026, 6, 17, 0, 0), 99.0, 100.0, 98.0, 99.5, 9.0),
            (requested_end_utc, 100.0, 101.0, 99.0, 100.5, 10.0),
            (now.replace(microsecond=0), 110.0, 111.0, 109.0, 110.5, 11.0),
        )

    monkeypatch.setattr(cache, "_get_barset_from_api", fake_get_barset)

    df = cache.download_ohlcv(
        "BTC/USDT",
        "1m",
        ny.localize(datetime(2026, 6, 17, 0, 0)),
        ny.localize(datetime(2026, 6, 24, 0, 0)),
    )

    assert calls
    assert calls[0]["start"] == datetime(2026, 6, 17, 0, 0)
    assert calls[0]["end"] == now
    assert pd.Timestamp("2026-06-24 04:00:00") in df.index
    assert pd.Timestamp(now.replace(microsecond=0)) not in df.index

    with duckdb.connect(cache.get_cache_file_name("BTC/USDT", "1m")) as con:
        ranges = con.execute("select start_dt, end_dt from cache_dt_ranges").fetch_df()
    assert ranges.iloc[0].start_dt == datetime(2026, 6, 17, 0, 0)
    assert ranges.iloc[0].end_dt == now


def test_ccxt_cache_future_only_window_returns_empty_without_provider_call(tmp_path, monkeypatch):
    cache = _cache_without_live_exchange(tmp_path, monkeypatch)
    now = datetime(2026, 6, 24, 18, 15, 0)
    calls = {"count": 0}

    monkeypatch.setattr(cache, "_current_utc_naive", lambda: now)

    def fake_get_barset(symbol, timeframe, limit, start, end):
        calls["count"] += 1
        raise AssertionError("future-only requests must not reach the provider")

    monkeypatch.setattr(cache, "_get_barset_from_api", fake_get_barset)

    df = cache.download_ohlcv(
        "BTC/USDT",
        "1m",
        datetime(2026, 6, 25, 0, 0),
        datetime(2026, 6, 25, 1, 0),
    )

    assert df.empty
    assert calls["count"] == 0
    assert not os.path.exists(cache.get_cache_file_name("BTC/USDT", "1m"))


def test_ccxt_cache_uses_native_day_timeframe_without_synthesizing_missing_days(tmp_path, monkeypatch):
    cache = _cache_without_live_exchange(tmp_path, monkeypatch)
    calls = []

    def fake_get_barset(symbol, timeframe, limit, start, end):
        calls.append({"symbol": symbol, "timeframe": timeframe, "limit": limit, "start": start, "end": end})
        return _bars(
            (datetime(2026, 1, 1), 100.0, 101.0, 99.0, 100.5, 10.0),
            (datetime(2026, 1, 3), 103.0, 104.0, 102.0, 103.5, 13.0),
        )

    monkeypatch.setattr(cache, "_get_barset_from_api", fake_get_barset)

    df = cache.download_ohlcv(
        "BTC/USDT",
        "1d",
        datetime(2026, 1, 1),
        datetime(2026, 1, 3),
    )

    assert calls[0]["timeframe"] == "1d"
    assert list(df.index) == [pd.Timestamp("2026-01-01 00:00:00"), pd.Timestamp("2026-01-03 00:00:00")]
    assert pd.Timestamp("2026-01-02 00:00:00") not in df.index


def test_ccxt_cache_empty_response_does_not_record_fake_coverage(tmp_path, monkeypatch):
    cache = _cache_without_live_exchange(tmp_path, monkeypatch)
    calls = {"count": 0}

    def fake_get_barset(symbol, timeframe, limit, start, end):
        calls["count"] += 1
        return pd.DataFrame(columns=["datetime", "open", "high", "low", "close", "volume"])

    monkeypatch.setattr(cache, "_get_barset_from_api", fake_get_barset)

    start = datetime(2026, 1, 1, 0, 0)
    end = datetime(2026, 1, 1, 0, 2)
    first = cache.download_ohlcv("BTC/USDT", "1m", start, end)
    first_call_count = calls["count"]
    second = cache.download_ohlcv("BTC/USDT", "1m", start, end)

    assert first.empty
    assert second.empty
    assert calls["count"] > first_call_count

    with duckdb.connect(cache.get_cache_file_name("BTC/USDT", "1m")) as con:
        ranges = con.execute("select * from cache_dt_ranges").fetch_df()
    assert ranges.empty


def test_ccxt_cache_partial_underfill_does_not_mark_missing_tail_cached(tmp_path, monkeypatch):
    cache = _cache_without_live_exchange(tmp_path, monkeypatch)
    calls = []

    def fake_get_barset(symbol, timeframe, limit, start, end):
        calls.append({"start": start, "end": end})
        if len(calls) == 1:
            return _bars(
                (datetime(2026, 6, 16, 0, 0), 100.0, 101.0, 99.0, 100.5, 10.0),
                (datetime(2026, 6, 16, 23, 59), 110.0, 111.0, 109.0, 110.5, 11.0),
            )
        return _bars(
            (datetime(2026, 6, 17, 0, 0), 111.0, 112.0, 110.0, 111.5, 12.0),
            (datetime(2026, 6, 17, 23, 59), 120.0, 121.0, 119.0, 120.5, 13.0),
        )

    monkeypatch.setattr(cache, "_get_barset_from_api", fake_get_barset)

    first = cache.download_ohlcv("BTC/USDT", "1m", datetime(2026, 6, 16), datetime(2026, 6, 17))

    assert pd.Timestamp("2026-06-16 23:59:00") in first.index
    assert pd.Timestamp("2026-06-17 00:00:00") not in first.index

    with duckdb.connect(cache.get_cache_file_name("BTC/USDT", "1m")) as con:
        ranges = con.execute("select start_dt, end_dt from cache_dt_ranges").fetch_df()
    assert len(ranges) == 1
    assert ranges.iloc[0].start_dt == datetime(2026, 6, 16, 0, 0)
    assert ranges.iloc[0].end_dt == datetime(2026, 6, 16, 23, 59, 59, 999999)

    second = cache.download_ohlcv("BTC/USDT", "1m", datetime(2026, 6, 17), datetime(2026, 6, 17))

    assert len(calls) == 2
    assert calls[1]["start"] > datetime(2026, 6, 16, 23, 59)
    assert pd.Timestamp("2026-06-17 00:00:00") in second.index

    with duckdb.connect(cache.get_cache_file_name("BTC/USDT", "1m")) as con:
        ranges = con.execute("select start_dt, end_dt from cache_dt_ranges order by start_dt").fetch_df()
    assert len(ranges) == 2
    assert ranges.iloc[0].start_dt == datetime(2026, 6, 16, 0, 0)
    assert ranges.iloc[0].end_dt == datetime(2026, 6, 16, 23, 59, 59, 999999)
    assert ranges.iloc[1].start_dt == datetime(2026, 6, 17, 0, 0)
    assert ranges.iloc[1].end_dt == datetime(2026, 6, 17, 23, 59, 59, 999999)


def test_ccxt_api_pagination_advances_across_empty_sparse_pages(tmp_path, monkeypatch):
    cache = _cache_without_live_exchange(tmp_path, monkeypatch)
    calls = []

    class FakeApi:
        has = {"fetchOHLCV": True}
        markets = {"BTC/USDT": {}}

        @staticmethod
        def parse8601(value):
            return int(pd.Timestamp(value, tz="UTC").timestamp() * 1000)

        def fetch_ohlcv(self, symbol, timeframe, since=None, limit=None, params=None):
            calls.append(pd.Timestamp(since, unit="ms"))
            current = pd.Timestamp(since, unit="ms")
            if current == pd.Timestamp("2026-01-01 00:00:00"):
                return [[since, 100.0, 101.0, 99.0, 100.5, 10.0]]
            if current < pd.Timestamp("2026-01-01 05:01:00"):
                return []
            if current <= pd.Timestamp("2026-01-01 06:00:00"):
                later = int(pd.Timestamp("2026-01-01 06:00:00", tz="UTC").timestamp() * 1000)
                return [[later, 106.0, 107.0, 105.0, 106.5, 11.0]]
            return []

    cache.api = FakeApi()

    df = cache._get_barset_from_api(
        "BTC/USDT",
        "1m",
        limit=500,
        start=datetime(2026, 1, 1, 0, 0),
        end=datetime(2026, 1, 1, 6, 30),
    )

    assert calls[0] == pd.Timestamp("2026-01-01 00:00:00")
    assert any(call >= pd.Timestamp("2026-01-01 05:01:00") for call in calls)
    assert list(df["datetime"]) == [
        pd.Timestamp("2026-01-01 00:00:00"),
        pd.Timestamp("2026-01-01 06:00:00"),
    ]


def test_ccxt_cache_partial_overlap_downloads_only_uncached_window(tmp_path, monkeypatch):
    cache = _cache_without_live_exchange(tmp_path, monkeypatch)
    calls = []

    def fake_get_barset(symbol, timeframe, limit, start, end):
        calls.append({"start": start, "end": end})
        if len(calls) == 1:
            return _bars((datetime(2026, 1, 1, 0, 0), 100.0, 101.0, 99.0, 100.5, 10.0))
        return _bars((datetime(2026, 1, 2, 0, 0), 200.0, 201.0, 199.0, 200.5, 20.0))

    monkeypatch.setattr(cache, "_get_barset_from_api", fake_get_barset)

    cache.download_ohlcv("BTC/USDT", "1m", datetime(2026, 1, 1), datetime(2026, 1, 1, 0, 1))
    df = cache.download_ohlcv("BTC/USDT", "1m", datetime(2026, 1, 1), datetime(2026, 1, 2, 0, 1))

    assert len(calls) == 2
    assert calls[1]["start"] > calls[0]["start"]
    assert calls[1]["end"] > calls[0]["end"]
    assert pd.Timestamp("2026-01-01 00:00:00") in df.index
    assert pd.Timestamp("2026-01-02 00:00:00") in df.index


def test_ccxt_cache_provider_error_does_not_create_fake_rows(tmp_path, monkeypatch):
    cache = _cache_without_live_exchange(tmp_path, monkeypatch)

    def fake_get_barset(symbol, timeframe, limit, start, end):
        raise RuntimeError("provider unavailable")

    monkeypatch.setattr(cache, "_get_barset_from_api", fake_get_barset)

    with pytest.raises(RuntimeError, match="provider unavailable"):
        cache.download_ohlcv("BTC/USDT", "1m", datetime(2026, 1, 1), datetime(2026, 1, 1, 0, 1))

    assert not os.path.exists(cache.get_cache_file_name("BTC/USDT", "1m"))


def test_ccxt_cache_filters_legacy_missing_nan_and_duplicate_rows(tmp_path, monkeypatch):
    cache = _cache_without_live_exchange(tmp_path, monkeypatch)
    legacy = pd.DataFrame(
        [
            (datetime(2026, 1, 1, 0, 0), 100.0, 101.0, 99.0, 100.5, 10.0, 0),
            (datetime(2026, 1, 1, 0, 0), 100.0, 101.0, 99.0, 100.5, 10.0, 0),
            (datetime(2026, 1, 1, 0, 1), 100.5, 100.5, 100.5, 100.5, 0.0, 1),
            (datetime(2026, 1, 1, 0, 2), None, 103.0, 101.0, 102.5, 12.0, 0),
            (datetime(2026, 1, 1, 0, 3), 103.0, 104.0, 102.0, 103.5, 13.0, 0),
        ],
        columns=["datetime", "open", "high", "low", "close", "volume", "missing"],
    )
    cache._cache_ohlcv("BTC/USDT", legacy, "1m")

    df = cache.get_data_from_cache(
        "BTC/USDT",
        "1m",
        datetime(2026, 1, 1, 0, 0),
        datetime(2026, 1, 1, 0, 3),
    )

    assert list(df.index) == [pd.Timestamp("2026-01-01 00:00:00"), pd.Timestamp("2026-01-01 00:03:00")]
    assert df["missing"].tolist() == [0, 0]


@pytest.mark.parametrize(
    "timeframe,expected_step",
    [
        ("1m", pd.Timedelta(minutes=1)),
        ("1h", pd.Timedelta(hours=1)),
        ("1d", pd.Timedelta(days=1)),
    ],
)
def test_ccxt_cache_warm_reads_do_not_refetch_supported_timeframes(
    tmp_path,
    monkeypatch,
    timeframe,
    expected_step,
):
    cache = _cache_without_live_exchange(tmp_path, monkeypatch)
    calls = []

    def fake_get_barset(symbol, requested_timeframe, limit, start, end):
        calls.append(
            {
                "symbol": symbol,
                "timeframe": requested_timeframe,
                "limit": limit,
                "start": start,
                "end": end,
            }
        )
        rows = [
            (datetime(2026, 1, 1), 100.0, 101.0, 99.0, 100.5, 10.0),
            (datetime(2026, 1, 1) + expected_step, 101.0, 102.0, 100.0, 101.5, 11.0),
        ]
        if requested_timeframe == "1m":
            rows.append((datetime(2026, 1, 1, 23, 59), 199.0, 200.0, 198.0, 199.5, 19.0))
        elif requested_timeframe == "1h":
            rows.append((datetime(2026, 1, 1, 23, 0), 199.0, 200.0, 198.0, 199.5, 19.0))
        return _bars(*rows)

    monkeypatch.setattr(cache, "_get_barset_from_api", fake_get_barset)

    start = datetime(2026, 1, 1)
    end = datetime(2026, 1, 1) + expected_step
    first = cache.download_ohlcv("BTC/USDT", timeframe, start, end)
    second = cache.download_ohlcv("BTC/USDT", timeframe, start, end)

    assert not first.empty
    assert first.equals(second)
    assert len(calls) == 1
    assert calls[0]["timeframe"] == timeframe


def test_ccxt_cache_keeps_quote_pairs_separate_without_usd_fallback(tmp_path, monkeypatch):
    cache = _cache_without_live_exchange(tmp_path, monkeypatch)
    calls = []

    def fake_get_barset(symbol, timeframe, limit, start, end):
        calls.append(symbol)
        if symbol == "BTC/USDT":
            return _bars((datetime(2026, 1, 1), 100.0, 101.0, 99.0, 100.5, 10.0))
        if symbol == "BTC/USD":
            return _bars((datetime(2026, 1, 1), 200.0, 201.0, 199.0, 200.5, 20.0))
        raise AssertionError(f"unexpected symbol {symbol}")

    monkeypatch.setattr(cache, "_get_barset_from_api", fake_get_barset)

    usdt = cache.download_ohlcv("BTC/USDT", "1m", datetime(2026, 1, 1), datetime(2026, 1, 1, 0, 1))
    usd = cache.download_ohlcv("BTC/USD", "1m", datetime(2026, 1, 1), datetime(2026, 1, 1, 0, 1))

    assert calls == ["BTC/USDT", "BTC/USD"]
    assert usdt["open"].iloc[0] == 100.0
    assert usd["open"].iloc[0] == 200.0
    assert cache.get_cache_file_name("BTC/USDT", "1m") != cache.get_cache_file_name("BTC/USD", "1m")

@pytest.mark.skip(reason="CCXT integration test requires stable network connection and external API availability")
@pytest.mark.parametrize("exchange_id,symbol,timeframe,start,end",
                         [ ("bitmex","ETH/USDT","1d",datetime(2022, 8, 1),datetime(2022, 10, 30))
                         ])
def test_cache_download_data(exchange_id:str, symbol:str, timeframe:str, start:datetime, end:datetime)->None:
    cache = CcxtCacheDB(exchange_id)
    cache_file_path = cache.get_cache_file_name(symbol,timeframe)

    # Remove cache file if exists.
    if os.path.exists(cache_file_path):
        os.remove(cache_file_path)

    # Download data and store in cache.
    try:
        df1 = cache.download_ohlcv(symbol,timeframe,start,end)
    except Exception as e:
        pytest.skip(f"Failed to download data from {exchange_id}: {str(e)}")

    assert os.path.exists(cache_file_path)

    # Counting data for the requested time period.
    dt = end - start
    if timeframe == "1d":
        request_data_length = dt.days
    else:
        request_data_length = dt.total_seconds() / 60

    # The cached data must be greater than or equal to the requested data.
    assert len(df1) >= request_data_length
    # The last time of the cached data must be equal to or greater than the requested time.
    assert df1.index.max() >= end
    # The first time of the cached data must be equal to or less than the requested time.
    assert df1.index.min() <= start

    # Fetch data stored in cache.
    df2 = cache.get_data_from_cache(symbol,timeframe,start,end)
    assert len(df2) >= request_data_length
    assert df2.index.max() >= end
    assert df2.index.min() <= start



@pytest.mark.skip(reason="CCXT integration test requires stable network connection and external API availability")
@pytest.mark.parametrize("exchange_id,symbol,timeframe,start,end",
                         [ ("bitmex","ETH/USDT","1d",datetime(2022, 9, 1),datetime(2024, 1, 30))
                         ])
def test_cache_download_data_without_overap(exchange_id:str, symbol:str, timeframe:str, start:datetime, end:datetime)->None:
    """Test for cases where the requested time range is partially covered by cache, but not partially covered by cache, if cache already exists.
    In this case, you need to combine the data in the cache with the newly downloaded data to create the data for the requested time range.
    Therefore, the existing start range must be larger than the requested start range and the existing end range must be smaller than the requested end range.
    The final range of updated data should be from the existing start range to the requested end range.
    """

    cache = CcxtCacheDB(exchange_id)
    cache_file_path = cache.get_cache_file_name(symbol,timeframe)

    # Read the cache_dt_ranges table before caching new data to duckdb
    with duckdb.connect(cache_file_path) as con:
        df_down_range = con.execute("SELECT * from cache_dt_ranges").fetch_df()
    prev_start_dt = df_down_range.iloc[0].start_dt
    prev_end_dt = df_down_range.iloc[0].end_dt

    # Download data and store in cache.
    try:
        df_cache = cache.download_ohlcv(symbol,timeframe,start,end)
    except Exception as e:
        pytest.skip(f"Failed to download data from {exchange_id}: {str(e)}")

    # Read the cache_dt_ranges table after caching new data to duckdb
    with duckdb.connect(cache_file_path) as con:
        df_down_range = con.execute("SELECT * from cache_dt_ranges").fetch_df()

    # Verify that the existing data range has been updated with the new data range
    # The number of data ranges should be 1.
    assert len(df_down_range) == 1

    cur_start_dt = df_down_range.iloc[0].start_dt
    cur_end_dt = df_down_range.iloc[0].end_dt

    # The new data range must be larger than the existing data range.
    assert cur_start_dt <= prev_start_dt
    assert cur_end_dt >= prev_end_dt

    # The new data range must be larger than the requested data range.
    assert cur_end_dt >= end
    assert cur_start_dt <= start

    # Counting data for the requested time period.
    dt = end - start
    if timeframe == "1d":
        request_data_length = dt.days
    else:
        request_data_length = dt.total_seconds() / 60

    # The cached data must be greater than or equal to the requested data.
    assert len(df_cache) >= request_data_length
    # The last time of the cached data must be equal to or greater than the requested time.
    assert df_cache.index.max() >= end
    # The first time of the cached data must be equal to or less than the requested time.
    assert df_cache.index.min() <= start

    # Remove cache file if exists.
    if os.path.exists(cache_file_path):
        os.remove(cache_file_path)
