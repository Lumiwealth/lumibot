from lumibot.tools import CcxtCacheDB
import pytest
import duckdb
from datetime import datetime
import os


# PYTHONWARNINGS="ignore::DeprecationWarning"; pytest test/test_ccxt_store.py

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
    df1 = cache.download_ohlcv(symbol,timeframe,start,end)

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
    df_cache = cache.download_ohlcv(symbol,timeframe,start,end)

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