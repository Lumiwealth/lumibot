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


def test_ccxt_cache_empty_response_records_coverage_without_fake_rows(tmp_path, monkeypatch):
    cache = _cache_without_live_exchange(tmp_path, monkeypatch)
    calls = {"count": 0}

    def fake_get_barset(symbol, timeframe, limit, start, end):
        calls["count"] += 1
        return pd.DataFrame(columns=["datetime", "open", "high", "low", "close", "volume"])

    monkeypatch.setattr(cache, "_get_barset_from_api", fake_get_barset)

    start = datetime(2026, 1, 1, 0, 0)
    end = datetime(2026, 1, 1, 0, 2)
    first = cache.download_ohlcv("BTC/USDT", "1m", start, end)
    second = cache.download_ohlcv("BTC/USDT", "1m", start, end)

    assert first.empty
    assert second.empty
    assert calls["count"] == 1

    with duckdb.connect(cache.get_cache_file_name("BTC/USDT", "1m")) as con:
        ranges = con.execute("select * from cache_dt_ranges").fetch_df()
    assert len(ranges) == 1


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
