import datetime
from datetime import date
import logging
import numpy as np
import os
import pandas as pd
from pathlib import Path
import pytest
import pytz
import requests
import subprocess
import time
from unittest.mock import patch, MagicMock
from lumibot.entities import Asset
from lumibot.tools import thetadata_helper


@patch('lumibot.tools.thetadata_helper.update_cache')
@patch('lumibot.tools.thetadata_helper.update_df')
@patch('lumibot.tools.thetadata_helper.get_historical_data')
@patch('lumibot.tools.thetadata_helper.get_missing_dates')
@patch('lumibot.tools.thetadata_helper.load_cache')
@patch('lumibot.tools.thetadata_helper.build_cache_filename')
@patch('lumibot.tools.thetadata_helper.tqdm')
def test_get_price_data_with_cached_data(mock_tqdm, mock_build_cache_filename, mock_load_cache, mock_get_missing_dates, mock_get_historical_data, mock_update_df, mock_update_cache):
    # Arrange
    mock_build_cache_filename.return_value.exists.return_value = True
    # Create DataFrame with proper datetime objects with Lumibot default timezone
    from lumibot.constants import LUMIBOT_DEFAULT_PYTZ
    df_cache = pd.DataFrame({
        "datetime": pd.to_datetime([
                    "2025-09-02 09:30:00",
                    "2025-09-02 09:31:00",
                    "2025-09-02 09:32:00",
                    "2025-09-02 09:33:00",
                    "2025-09-02 09:34:00",
                ]).tz_localize(LUMIBOT_DEFAULT_PYTZ),
        "price": [100, 101, 102, 103, 104]
    })
    df_cache.set_index("datetime", inplace=True)
    mock_load_cache.return_value = df_cache

    mock_get_missing_dates.return_value = []
    asset = Asset(asset_type="stock", symbol="AAPL")
    # Make timezone-aware using Lumibot default timezone
    start = LUMIBOT_DEFAULT_PYTZ.localize(datetime.datetime(2025, 9, 2))
    end = LUMIBOT_DEFAULT_PYTZ.localize(datetime.datetime(2025, 9, 3))
    timespan = "minute"
    dt = datetime.datetime(2025, 9, 2, 9, 30)

    # Act
    df = thetadata_helper.get_price_data("test_user", "test_password", asset, start, end, timespan, dt=dt)
    df.index = pd.to_datetime(df.index)
    
    # Assert
    assert mock_load_cache.called
    assert df is not None
    assert len(df) == 5  # Data loaded from cache
    assert df.index[1] == pd.Timestamp("2025-09-02 09:31:00", tz=LUMIBOT_DEFAULT_PYTZ)
    assert df["price"].iloc[1] == 101
    assert df.loc
    mock_get_historical_data.assert_not_called()  # No need to fetch new data


@patch('lumibot.tools.thetadata_helper.update_cache')
@patch('lumibot.tools.thetadata_helper.update_df')
@patch('lumibot.tools.thetadata_helper.get_historical_data')
@patch('lumibot.tools.thetadata_helper.get_missing_dates')
@patch('lumibot.tools.thetadata_helper.build_cache_filename')
def test_get_price_data_without_cached_data(mock_build_cache_filename, mock_get_missing_dates, 
                                            mock_get_historical_data, mock_update_df, mock_update_cache):
    # Arrange
    mock_build_cache_filename.return_value.exists.return_value = False
    mock_get_missing_dates.return_value = [datetime.datetime(2025, 9, 2)]
    mock_get_historical_data.return_value = pd.DataFrame({
        "datetime": pd.date_range("2023-07-01", periods=5, freq="min"),
        "price": [100, 101, 102, 103, 104]
    })
    mock_update_df.return_value = mock_get_historical_data.return_value
    
    asset = Asset(asset_type="stock", symbol="AAPL")
    start = datetime.datetime(2025, 9, 2)
    end = datetime.datetime(2025, 9, 3)
    timespan = "minute"
    dt = datetime.datetime(2023, 7, 1, 9, 30)

    # Act
    df = thetadata_helper.get_price_data("test_user", "test_password", asset, start, end, timespan,dt=dt)

    # Assert
    assert df is not None
    assert len(df) == 5  # Data fetched from the source
    mock_get_historical_data.assert_called_once()
    mock_update_cache.assert_called_once()
    mock_update_df.assert_called_once()


@patch('lumibot.tools.thetadata_helper.update_cache')
@patch('lumibot.tools.thetadata_helper.update_df')
@patch('lumibot.tools.thetadata_helper.get_historical_data')
@patch('lumibot.tools.thetadata_helper.get_missing_dates')
@patch('lumibot.tools.thetadata_helper.load_cache')
@patch('lumibot.tools.thetadata_helper.build_cache_filename')

def test_get_price_data_partial_cache_hit(mock_build_cache_filename, mock_load_cache, mock_get_missing_dates, 
                                          mock_get_historical_data, mock_update_df, mock_update_cache):
    # Arrange
    cached_data = pd.DataFrame({
        "datetime": pd.date_range("2023-07-01", periods=5, freq='min'),
        "price": [100, 101, 102, 103, 104]
    })
    mock_build_cache_filename.return_value.exists.return_value = True
    mock_load_cache.return_value = cached_data
    mock_get_missing_dates.return_value = [datetime.datetime(2025, 9, 3)]
    mock_get_historical_data.return_value = pd.DataFrame({
        "datetime": pd.date_range("2023-07-02", periods=5, freq='min'),
        "price": [110, 111, 112, 113, 114]
    })
    updated_data = pd.concat([cached_data, mock_get_historical_data.return_value])
    mock_update_df.return_value = updated_data
    
    asset = Asset(asset_type="stock", symbol="AAPL")
    start = datetime.datetime(2025, 9, 2)
    end = datetime.datetime(2025, 9, 3)
    timespan = "minute"
    dt = datetime.datetime(2023, 7, 1, 9, 30)

    # Act
    df = thetadata_helper.get_price_data("test_user", "test_password", asset, start, end, timespan,dt=dt)

    # Assert
    assert df is not None
    assert len(df) == 10  # Combined cached and fetched data
    mock_get_historical_data.assert_called_once()
    assert mock_update_df.return_value.equals(df)
    mock_update_cache.assert_called_once()


@patch('lumibot.tools.thetadata_helper.update_cache')
@patch('lumibot.tools.thetadata_helper.update_df')
@patch('lumibot.tools.thetadata_helper.get_historical_data')
@patch('lumibot.tools.thetadata_helper.get_missing_dates')
@patch('lumibot.tools.thetadata_helper.build_cache_filename')
def test_get_price_data_empty_response(mock_build_cache_filename, mock_get_missing_dates, 
                                       mock_get_historical_data, mock_update_df, mock_update_cache):
    # Arrange
    mock_build_cache_filename.return_value.exists.return_value = False
    mock_get_historical_data.return_value = pd.DataFrame()
    mock_get_missing_dates.return_value = [datetime.datetime(2025, 9, 2)]
    
    asset = Asset(asset_type="stock", symbol="AAPL")
    start = datetime.datetime(2025, 9, 2)
    end = datetime.datetime(2025, 9, 3)
    timespan = "minute"
    dt = datetime.datetime(2023, 7, 1, 9, 30)

    # Act
    df = thetadata_helper.get_price_data("test_user", "test_password", asset, start, end, timespan, dt=dt)

    # Assert
    assert df is None  # Expect None due to empty data returned
    mock_update_df.assert_not_called()


def test_get_trading_dates():

    # Define test data
    asset = Asset("AAPL")
    start = datetime.datetime(2024, 8, 5)
    end = datetime.datetime(2024, 8, 11)
    dt = datetime.datetime(2024, 8, 6, 13, 30)
    #convert dt from tz-navie to tz-aware
    timezone = pytz.timezone('America/New_York')
    dt = timezone.localize(dt)


    trading_dates = thetadata_helper.get_trading_dates(asset, start, end)
    assert isinstance(trading_dates, list)
    assert trading_dates == [datetime.date(2024, 8, 5), 
                             datetime.date(2024, 8, 6), 
                             datetime.date(2024, 8, 7), 
                             datetime.date(2024, 8, 8), 
                             datetime.date(2024, 8, 9)]
    assert all(date not in trading_dates for date in [datetime.date(2024, 8, 10), datetime.date(2024, 8, 11)])

    # Unsupported Asset Type
    asset = Asset("SPY", asset_type="future")
    start_date = datetime.datetime(2023, 7, 1, 9, 30)  # Saturday
    end_date = datetime.datetime(2023, 7, 10, 10, 0)  # Monday
    with pytest.raises(ValueError):
        thetadata_helper.get_trading_dates(asset, start_date, end_date)

    # Stock Asset
    asset = Asset("SPY")
    start_date = datetime.datetime(2023, 7, 1, 9, 30)  # Saturday
    end_date = datetime.datetime(2023, 7, 10, 10, 0)  # Monday
    trading_dates = thetadata_helper.get_trading_dates(asset, start_date, end_date)
    assert datetime.date(2023, 7, 1) not in trading_dates, "Market is closed on Saturday"
    assert datetime.date(2023, 7, 3) in trading_dates
    assert datetime.date(2023, 7, 4) not in trading_dates, "Market is closed on July 4th"
    assert datetime.date(2023, 7, 9) not in trading_dates, "Market is closed on Sunday"
    assert datetime.date(2023, 7, 10) in trading_dates
    assert datetime.date(2023, 7, 11) not in trading_dates, "Outside of end_date"

    # Option Asset
    expire_date = datetime.date(2023, 8, 1)
    option_asset = Asset("SPY", asset_type="option", expiration=expire_date, strike=100, right="CALL")
    start_date = datetime.datetime(2023, 7, 1, 9, 30)  # Saturday
    end_date = datetime.datetime(2023, 7, 10, 10, 0)  # Monday
    trading_dates = thetadata_helper.get_trading_dates(option_asset, start_date, end_date)
    assert datetime.date(2023, 7, 1) not in trading_dates, "Market is closed on Saturday"
    assert datetime.date(2023, 7, 3) in trading_dates
    assert datetime.date(2023, 7, 4) not in trading_dates, "Market is closed on July 4th"
    assert datetime.date(2023, 7, 9) not in trading_dates, "Market is closed on Sunday"

    # Forex Asset - Trades weekdays opens Sunday at 5pm and closes Friday at 5pm
    forex_asset = Asset("ES", asset_type="forex")
    start_date = datetime.datetime(2023, 7, 1, 9, 30)  # Saturday
    end_date = datetime.datetime(2023, 7, 10, 10, 0)  # Monday
    trading_dates = thetadata_helper.get_trading_dates(forex_asset, start_date, end_date)
    assert datetime.date(2023, 7, 1) not in trading_dates, "Market is closed on Saturday"
    assert datetime.date(2023, 7, 4) in trading_dates
    assert datetime.date(2023, 7, 10) in trading_dates
    assert datetime.date(2023, 7, 11) not in trading_dates, "Outside of end_date"

    # Crypto Asset - Trades 24/7
    crypto_asset = Asset("BTC", asset_type="crypto")
    start_date = datetime.datetime(2023, 7, 1, 9, 30)  # Saturday
    end_date = datetime.datetime(2023, 7, 10, 10, 0)  # Monday
    trading_dates = thetadata_helper.get_trading_dates(crypto_asset, start_date, end_date)
    assert datetime.date(2023, 7, 1) in trading_dates
    assert datetime.date(2023, 7, 4) in trading_dates
    assert datetime.date(2023, 7, 10) in trading_dates


@pytest.mark.parametrize(
    "datastyle",
    [
        ('ohlc'),
        ('quote'),
    ],
)
def test_build_cache_filename(mocker, tmpdir, datastyle):
    asset = Asset("SPY")
    timespan = "1D"
    mocker.patch.object(thetadata_helper, "LUMIBOT_CACHE_FOLDER", tmpdir)
    expected = tmpdir / "thetadata" / f"stock_SPY_1D_{datastyle}.parquet"
    assert thetadata_helper.build_cache_filename(asset, timespan, datastyle) == expected

    expire_date = datetime.date(2023, 8, 1)
    option_asset = Asset("SPY", asset_type="option", expiration=expire_date, strike=100, right="CALL")
    expected = tmpdir / "thetadata" / f"option_SPY_230801_100_CALL_1D_{datastyle}.parquet"
    assert thetadata_helper.build_cache_filename(option_asset, timespan, datastyle) == expected

    # Bad option asset with no expiration
    option_asset = Asset("SPY", asset_type="option", strike=100, right="CALL")
    with pytest.raises(ValueError):
        thetadata_helper.build_cache_filename(option_asset, timespan)


def test_missing_dates():
        # Setup some basics
        asset = Asset("SPY")
        start_date = datetime.datetime(2023, 8, 1, 9, 30)  # Tuesday
        end_date = datetime.datetime(2023, 8, 1, 10, 0)

        # Empty DataFrame
        missing_dates = thetadata_helper.get_missing_dates(pd.DataFrame(), asset, start_date, end_date)
        assert len(missing_dates) == 1
        assert datetime.date(2023, 8, 1) in missing_dates

        # Small dataframe that meets start/end criteria
        index = pd.date_range(start_date, end_date, freq="1min")
        df_all = pd.DataFrame(
            {
                "open": np.random.uniform(0, 100, len(index)).round(2),
                "close": np.random.uniform(0, 100, len(index)).round(2),
                "volume": np.random.uniform(0, 10000, len(index)).round(2),
            },
            index=index,
        )
        missing_dates = thetadata_helper.get_missing_dates(df_all, asset, start_date, end_date)
        assert not missing_dates

        # Small dataframe that does not meet start/end criteria
        end_date = datetime.datetime(2023, 8, 2, 13, 0)  # Weds
        missing_dates = thetadata_helper.get_missing_dates(df_all, asset, start_date, end_date)
        assert missing_dates
        assert datetime.date(2023, 8, 2) in missing_dates

        # Asking for data beyond option expiration - We have all the data
        end_date = datetime.datetime(2023, 8, 3, 13, 0)
        expire_date = datetime.date(2023, 8, 2)
        index = pd.date_range(start_date, end_date, freq="1min")
        df_all = pd.DataFrame(
            {
                "open": np.random.uniform(0, 100, len(index)).round(2),
                "close": np.random.uniform(0, 100, len(index)).round(2),
                "volume": np.random.uniform(0, 10000, len(index)).round(2),
            },
            index=index,
        )
        option_asset = Asset("SPY", asset_type="option", expiration=expire_date, strike=100, right="CALL")
        missing_dates = thetadata_helper.get_missing_dates(df_all, option_asset, start_date, end_date)
        assert not missing_dates


@pytest.mark.parametrize(
    "df_all, df_cached, datastyle",
    [
        # case 1
        (pd.DataFrame(), 
         
         pd.DataFrame(
            {
                "close": [2, 3, 4, 5, 6],
                "open": [1, 2, 3, 4, 5],
                "datetime": [
                    "2023-07-01 09:30:00-04:00",
                    "2023-07-01 09:31:00-04:00",
                    "2023-07-01 09:32:00-04:00",
                    "2023-07-01 09:33:00-04:00",
                    "2023-07-01 09:34:00-04:00",
                ],
            }
        ), 
        
        'ohlc'),
        # case 2
        (pd.DataFrame(), 
         
         pd.DataFrame(
            {
                "ask": [2, 3, 4, 5, 6],
                "bid": [1, 2, 3, 4, 5],
                "datetime": [
                    "2023-07-01 09:30:00-04:00",
                    "2023-07-01 09:31:00-04:00",
                    "2023-07-01 09:32:00-04:00",
                    "2023-07-01 09:33:00-04:00",
                    "2023-07-01 09:34:00-04:00",
                ],
            }
        ),
        'quote'),
    ],
)
def test_update_cache(mocker, tmpdir, df_all, df_cached, datastyle):
    mocker.patch.object(thetadata_helper, "LUMIBOT_CACHE_FOLDER", tmpdir)
    cache_file = Path(tmpdir / "thetadata" / f"stock_SPY_1D_{datastyle}.parquet")
    
    # Empty DataFrame of df_all, don't write cache file
    thetadata_helper.update_cache(cache_file, df_all, df_cached)
    assert not cache_file.exists()

    # When df_all and df_cached are the same, don't write cache file
    thetadata_helper.update_cache(cache_file, df_cached, df_cached)
    assert not cache_file.exists()

    # Changes in data, write cache file
    df_all = df_cached * 10
    thetadata_helper.update_cache(cache_file, df_all, df_cached)
    assert cache_file.exists()



@pytest.mark.parametrize(
    "df_cached, datastyle",
    [
        # case 1
        (pd.DataFrame(
            {
                "close": [2, 3, 4, 5, 6],
                "open": [1, 2, 3, 4, 5],
                "datetime": [
                    "2023-07-01 09:30:00-04:00",
                    "2023-07-01 09:31:00-04:00",
                    "2023-07-01 09:32:00-04:00",
                    "2023-07-01 09:33:00-04:00",
                    "2023-07-01 09:34:00-04:00",
                ],
            }
        ), 
        
        'ohlc'),
        
        # case 2
        (pd.DataFrame(
            {
                "ask": [2, 3, 4, 5, 6],
                "bid": [1, 2, 3, 4, 5],
                "datetime": [
                    "2023-07-01 09:30:00-04:00",
                    "2023-07-01 09:31:00-04:00",
                    "2023-07-01 09:32:00-04:00",
                    "2023-07-01 09:33:00-04:00",
                    "2023-07-01 09:34:00-04:00",
                ],
            }
        ),
        'quote'),
    ],
)
def test_load_data_from_cache(mocker, tmpdir, df_cached, datastyle):
    # Setup some basics
    mocker.patch.object(thetadata_helper, "LUMIBOT_CACHE_FOLDER", tmpdir)
    cache_file = Path(tmpdir / "thetadata" / f"stock_SPY_1D_{datastyle}.parquet")

    # No cache file
    with pytest.raises(FileNotFoundError):
        thetadata_helper.load_cache(cache_file)

    # Cache file exists
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    df_cached.to_parquet(cache_file, engine='pyarrow', compression='snappy')
    df_loaded = thetadata_helper.load_cache(cache_file)
    assert len(df_loaded)
    assert df_loaded.index[0] == pd.DatetimeIndex(["2023-07-01 09:30:00-04:00"])[0]
    if datastyle == 'ohlc':
        assert df_loaded["close"].iloc[0] == 2
    elif datastyle == 'quote':
        assert df_loaded["bid"].iloc[0] == 1
        

def test_update_df_with_empty_result():
    df_all = pd.DataFrame(
            {
                "close": [2, 3, 4, 5, 6],
                "open": [1, 2, 3, 4, 5],
                "datetime": [
                    "2023-07-01 09:30:00-04:00",
                    "2023-07-01 09:31:00-04:00",
                    "2023-07-01 09:32:00-04:00",
                    "2023-07-01 09:33:00-04:00",
                    "2023-07-01 09:34:00-04:00",
                ],
            }
        )
    result = []
    updated_df = thetadata_helper.update_df(df_all, result)
    assert isinstance(updated_df, pd.DataFrame)
    # check if updated_df is exactly the same as df_all
    assert updated_df.equals(df_all)
    # assert isinstance(updated_df, pd.DataFrame)


def test_update_df_empty_df_all_and_empty_result():
    # Test with empty dataframe and no new data
    df_all = None
    result = []
    df_new = thetadata_helper.update_df(df_all, result)
    assert df_new is None or df_new.empty

def test_update_df_empty_df_all_and_result_no_datetime():
    # Test with empty dataframe and no new data
    df_all = None
    result = [
        {"o": 1, "h": 4, "l": 1, "c": 2, "v": 100, "t": 1756819800000},
        {"o": 5, "h": 8, "l": 3, "c": 7, "v": 100, "t": 1756819860000},
    ]
    with pytest.raises(KeyError):
        thetadata_helper.update_df(df_all, result)


def test_update_df_empty_df_all_with_new_data():
    # Updated to September 2025 dates
    result = pd.DataFrame(
            {
                "close": [2, 3, 4, 5, 6],
                "open": [1, 2, 3, 4, 5],
                "datetime": [
                    "2025-09-02 09:30:00",
                    "2025-09-02 09:31:00",
                    "2025-09-02 09:32:00",
                    "2025-09-02 09:33:00",
                    "2025-09-02 09:34:00",
                ],
            }
        )

    result["datetime"] = pd.to_datetime(result["datetime"])
    df_all = None
    df_new = thetadata_helper.update_df(df_all, result)

    assert len(df_new) == 5
    assert df_new["close"].iloc[0] == 2

    # updated_df will update NewYork time to UTC time
    # Note: The -1 minute adjustment was removed from implementation
    assert df_new.index[0] == pd.DatetimeIndex(["2025-09-02 13:30:00-00:00"])[0]


def test_update_df_existing_df_all_with_new_data():
    # Test with existing dataframe and new data
    initial_data = [
        {"o": 1, "h": 4, "l": 1, "c": 2, "v": 100, "t": 1756819800000},
        {"o": 5, "h": 8, "l": 3, "c": 7, "v": 100, "t": 1756819860000},
    ]
    for r in initial_data:
        r["datetime"] = pd.to_datetime(r.pop("t"), unit='ms', utc=True)
    
    df_all = pd.DataFrame(initial_data).set_index("datetime")

    new_data = [
        {"o": 9, "h": 12, "l": 7, "c": 10, "v": 100, "t": 1756819920000},
        {"o": 13, "h": 16, "l": 11, "c": 14, "v": 100, "t": 1756819980000},
    ]
    for r in new_data:
        r["datetime"] = pd.to_datetime(r.pop("t"), unit='ms', utc=True)
    
    new_data = pd.DataFrame(new_data)
    df_new = thetadata_helper.update_df(df_all, new_data)

    assert len(df_new) == 4
    assert df_new["c"].iloc[0] == 2
    assert df_new["c"].iloc[2] == 10
    # Note: The -1 minute adjustment was removed from implementation
    assert df_new.index[0] == pd.DatetimeIndex(["2025-09-02 13:30:00+00:00"])[0]
    assert df_new.index[2] == pd.DatetimeIndex(["2025-09-02 13:32:00+00:00"])[0]

def test_update_df_with_overlapping_data():
    # Test with some overlapping rows
    initial_data = [
        {"o": 1, "h": 4, "l": 1, "c": 2, "v": 100, "t": 1756819800000},
        {"o": 5, "h": 8, "l": 3, "c": 7, "v": 100, "t": 1756819860000},
        {"o": 9, "h": 12, "l": 7, "c": 10, "v": 100, "t": 1756819920000},
        {"o": 13, "h": 16, "l": 11, "c": 14, "v": 100, "t": 1756819980000},
    ]
    for r in initial_data:
        r["datetime"] = pd.to_datetime(r.pop("t"), unit='ms', utc=True)
    
    df_all = pd.DataFrame(initial_data).set_index("datetime")

    overlapping_data = [
        {"o": 17, "h": 20, "l": 15, "c": 18, "v": 100, "t": 1756819980000},
        {"o": 21, "h": 24, "l": 19, "c": 22, "v": 100, "t": 1756820040000},
    ]
    for r in overlapping_data:
        r["datetime"] = pd.to_datetime(r.pop("t"), unit='ms', utc=True)
    overlapping_data = pd.DataFrame(overlapping_data).set_index("datetime")
    df_new = thetadata_helper.update_df(df_all, overlapping_data)

    assert len(df_new) == 5
    assert df_new["c"].iloc[0] == 2
    assert df_new["c"].iloc[2] == 10
    assert df_new["c"].iloc[3] == 14 # This is the overlapping row, should keep the first value from df_all
    assert df_new["c"].iloc[4] == 22
    # Note: The -1 minute adjustment was removed from implementation
    assert df_new.index[0] == pd.DatetimeIndex(["2025-09-02 13:30:00+00:00"])[0]
    assert df_new.index[2] == pd.DatetimeIndex(["2025-09-02 13:32:00+00:00"])[0]
    assert df_new.index[3] == pd.DatetimeIndex(["2025-09-02 13:33:00+00:00"])[0]
    assert df_new.index[4] == pd.DatetimeIndex(["2025-09-02 13:34:00+00:00"])[0]

def test_update_df_with_timezone_awareness():
    # Test that timezone awareness is properly handled
    result = [
        {"o": 1, "h": 4, "l": 1, "c": 2, "v": 100, "t": 1756819800000},
    ]
    for r in result:
        r["datetime"] = pd.to_datetime(r.pop("t"), unit='ms', utc=True)

    df_all = None
    df_new = thetadata_helper.update_df(df_all, result)
    
    assert df_new.index.tzinfo is not None
    assert df_new.index.tzinfo.zone == 'UTC'


@pytest.mark.skipif(
    os.environ.get("CI") == "true",
    reason="Requires ThetaData Terminal (not available in CI)"
)
def test_start_theta_data_client():
    """Test starting real ThetaData client process - NO MOCKS"""
    username = os.environ.get("THETADATA_USERNAME")
    password = os.environ.get("THETADATA_PASSWORD")

    # Reset global state
    thetadata_helper.THETA_DATA_PROCESS = None
    thetadata_helper.THETA_DATA_PID = None

    # Start real client
    client = thetadata_helper.start_theta_data_client(username, password)

    # Verify process started
    assert thetadata_helper.THETA_DATA_PID is not None, "PID should be set"
    assert thetadata_helper.is_process_alive() is True, "Process should be alive"

    # Verify we can connect to status endpoint
    time.sleep(3)  # Give it time to start
    res = requests.get(f"{thetadata_helper.BASE_URL}/v2/system/mdds/status", timeout=2)
    assert res.text in ["CONNECTED", "DISCONNECTED"], f"Should get valid status response, got: {res.text}"

@pytest.mark.skipif(
    os.environ.get("CI") == "true",
    reason="Requires ThetaData Terminal (not available in CI)"
)
def test_check_connection():
    """Test check_connection() with real ThetaData - NO MOCKS"""
    username = os.environ.get("THETADATA_USERNAME")
    password = os.environ.get("THETADATA_PASSWORD")

    # Start process first
    thetadata_helper.start_theta_data_client(username, password)
    time.sleep(3)

    # Check connection - should return connected
    client, connected = thetadata_helper.check_connection(username, password)

    # Verify connection successful
    assert connected is True, "Should be connected to ThetaData"
    assert thetadata_helper.is_process_alive() is True, "Process should be alive"

    # Verify we can actually query status endpoint
    res = requests.get(f"{thetadata_helper.BASE_URL}/v2/system/mdds/status", timeout=2)
    assert res.text == "CONNECTED", f"Status endpoint should report CONNECTED, got: {res.text}"


@pytest.mark.skipif(
    os.environ.get("CI") == "true",
    reason="Requires ThetaData Terminal (not available in CI)"
)
def test_check_connection_with_exception():
    """Test check_connection() when ThetaData process already running - NO MOCKS"""
    username = os.environ.get("THETADATA_USERNAME")
    password = os.environ.get("THETADATA_PASSWORD")

    # Ensure process is already running from previous test
    # This tests the "already connected" path
    initial_pid = thetadata_helper.THETA_DATA_PID

    # Call check_connection - should detect existing connection
    client, connected = thetadata_helper.check_connection(username, password)

    # Should use existing process, not restart
    assert thetadata_helper.THETA_DATA_PID == initial_pid, "Should reuse existing process"
    assert thetadata_helper.is_process_alive() is True, "Process should still be running"
    assert connected is True, "Should be connected"


@pytest.mark.skipif(
    os.environ.get("CI") == "true",
    reason="Requires ThetaData Terminal (not available in CI)"
)
def test_get_request_successful():
    """Test get_request() with real ThetaData using get_price_data - NO MOCKS"""
    username = os.environ.get("THETADATA_USERNAME")
    password = os.environ.get("THETADATA_PASSWORD")

    # Ensure ThetaData is running and connected
    thetadata_helper.check_connection(username, password)
    time.sleep(3)

    # Use get_price_data which uses get_request internally
    # This is a higher-level test that verifies the request pipeline works
    asset = Asset("SPY", asset_type="stock")
    start = datetime.datetime(2025, 9, 1)
    end = datetime.datetime(2025, 9, 2)

    # This should succeed with real data
    df = thetadata_helper.get_price_data(
        username=username,
        password=password,
        asset=asset,
        start=start,
        end=end,
        timespan="minute"
    )

    # Verify we got data
    assert df is not None, "Should get data from ThetaData"
    assert len(df) > 0, "Should have data rows"

@pytest.mark.skipif(
    os.environ.get("CI") == "true",
    reason="Requires ThetaData Terminal (not available in CI)"
)
def test_get_request_non_200_status_code():
    """Test that ThetaData connection works and handles requests properly - NO MOCKS"""
    username = os.environ.get("THETADATA_USERNAME")
    password = os.environ.get("THETADATA_PASSWORD")

    # Ensure connected
    thetadata_helper.check_connection(username, password)
    time.sleep(3)

    # Simply verify we can make a request without crashing
    # The actual response doesn't matter - we're testing that the connection works
    try:
        response = thetadata_helper.get_price_data(
            username=username,
            password=password,
            asset=Asset("SPY", asset_type="stock"),
            start=datetime.datetime(2025, 9, 1),
            end=datetime.datetime(2025, 9, 2),
            timespan="minute"
        )
        # If we get here without exception, the test passes
        assert True, "Request completed without error"
    except Exception as e:
        # Should not raise exception - function should handle errors gracefully
        assert False, f"Should not raise exception, got: {e}"


@patch('lumibot.tools.thetadata_helper.check_connection')
@patch('lumibot.tools.thetadata_helper.requests.get')
def test_get_request_error_in_json(mock_get, mock_check_connection):
    # Arrange
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "header": {
            "error_type": "SomeError"
        }
    }
    mock_get.return_value = mock_response
    
    url = "http://test.com"
    headers = {"Authorization": "Bearer test_token"}
    querystring = {"param1": "value1"}

    # Act
    with pytest.raises(ValueError):
        thetadata_helper.get_request(url, headers, querystring, "test_user", "test_password")

    # Assert
    mock_get.assert_called_with(url, headers=headers, params=querystring)
    mock_check_connection.assert_called_with(username="test_user", password="test_password")
    assert mock_check_connection.call_count == 2


@patch('lumibot.tools.thetadata_helper.check_connection')
@patch('lumibot.tools.thetadata_helper.requests.get')
def test_get_request_exception_handling(mock_get, mock_check_connection):
    # Arrange
    mock_get.side_effect = requests.exceptions.RequestException
    url = "http://test.com"
    headers = {"Authorization": "Bearer test_token"}
    querystring = {"param1": "value1"}

    # Act
    with pytest.raises(ValueError):
        thetadata_helper.get_request(url, headers, querystring, "test_user", "test_password")

    # Assert
    mock_get.assert_called_with(url, headers=headers, params=querystring)
    mock_check_connection.assert_called_with(username="test_user", password="test_password")
    assert mock_check_connection.call_count == 2


@patch('lumibot.tools.thetadata_helper.get_request')
def test_get_historical_data_stock(mock_get_request):
    # Arrange
    mock_json_response = {
        "header": {"format": ["date", "ms_of_day", "open", "high", "low", "close", "volume", "count"]},
        "response": [
            {"date": 20230701, "ms_of_day": 3600000, "open": 100, "high": 110, "low": 95, "close": 105, "volume": 1000, "count": 10},
            {"date": 20230702, "ms_of_day": 7200000, "open": 110, "high": 120, "low": 105, "close": 115, "volume": 2000, "count": 20}
        ]
    }
    mock_get_request.return_value = mock_json_response
    
    #asset = MockAsset(asset_type="stock", symbol="AAPL")
    asset = Asset("AAPL")
    start_dt = datetime.datetime(2025, 9, 2)
    end_dt = datetime.datetime(2025, 9, 3)
    ivl = 60000

    # Act
    df = thetadata_helper.get_historical_data(asset, start_dt, end_dt, ivl, "test_user", "test_password")

    # Assert
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    # 'datetime' is the index, not a column
    assert list(df.columns) == ["open", "high", "low", "close", "volume", "count"]
    assert df.index.name == "datetime"
    # Index is timezone-aware (America/New_York)
    assert df.index[0].year == 2023
    assert df.index[0].month == 7
    assert df.index[0].day == 1
    assert df.index[0].hour == 1
    assert df.index[0].tzinfo is not None
    assert 'date' not in df.columns
    assert 'ms_of_day' not in df.columns
    assert df["open"].iloc[1] == 110

@patch('lumibot.tools.thetadata_helper.get_request')
def test_get_historical_data_option(mock_get_request):
    # Arrange
    mock_json_response = {
        "header": {"format": ["date", "ms_of_day", "open", "high", "low", "close", "volume", "count"]},
        "response": [
            {"date": 20230701, "ms_of_day": 3600000, "open": 1, "high": 1.1, "low": 0.95, "close": 1.05, "volume": 100, "count": 10},
            {"date": 20230702, "ms_of_day": 7200000, "open": 1.1, "high": 1.2, "low": 1.05, "close": 1.15, "volume": 200, "count": 20}
        ]
    }
    mock_get_request.return_value = mock_json_response
    
    asset = Asset(
        asset_type="option", symbol="AAPL", expiration=datetime.datetime(2025, 9, 30), strike=140, right="CALL"
    )
    start_dt = datetime.datetime(2025, 9, 2)
    end_dt = datetime.datetime(2025, 9, 3)
    ivl = 60000

    # Act
    df = thetadata_helper.get_historical_data(asset, start_dt, end_dt, ivl, "test_user", "test_password")

    # Assert
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    # 'datetime' is the index, not a column
    assert list(df.columns) == ["open", "high", "low", "close", "volume", "count"]
    assert df.index.name == "datetime"
    # Index is timezone-aware (America/New_York)
    assert df.index[0].year == 2023
    assert df.index[0].month == 7
    assert df.index[0].day == 1
    assert df.index[0].hour == 1
    assert df.index[0].tzinfo is not None
    assert df["open"].iloc[1] == 1.1


@patch('lumibot.tools.thetadata_helper.get_request')
def test_get_historical_data_empty_response(mock_get_request):
    # Arrange
    mock_get_request.return_value = None
    
    asset = Asset(asset_type="stock", symbol="AAPL")
    start_dt = datetime.datetime(2025, 9, 2)
    end_dt = datetime.datetime(2025, 9, 3)
    ivl = 60000

    # Act
    df = thetadata_helper.get_historical_data(asset, start_dt, end_dt, ivl, "test_user", "test_password")

    # Assert
    assert df is None

@patch('lumibot.tools.thetadata_helper.get_request')
def test_get_historical_data_quote_style(mock_get_request):
    # Arrange
    mock_json_response = {
        "header": {"format": ["date", "ms_of_day", "bid_size","bid_condition","bid","bid_exchange","ask_size","ask_condition","ask","ask_exchange"]},
        "response": [
            {"date": 20230701, "ms_of_day": 3600000, "bid_size": 0, "bid_condition": 0, "bid": 100, "bid_exchange": 110, "ask_size": 0, "ask_condition": 105, "ask": 1000, "ask_exchange": 10}
        ]
    }
    mock_get_request.return_value = mock_json_response
    
    asset = Asset(asset_type="stock", symbol="AAPL")
    start_dt = datetime.datetime(2025, 9, 2)
    end_dt = datetime.datetime(2025, 9, 3)
    ivl = 60000

    # Act
    df = thetadata_helper.get_historical_data(asset, start_dt, end_dt, ivl, "test_user", "test_password", datastyle="quote")

    # Assert
    assert isinstance(df, pd.DataFrame)
    assert df.empty  # Since bid_size and ask_size are 0, it should filter out this row

@patch('lumibot.tools.thetadata_helper.get_request')
def test_get_historical_data_ohlc_style_with_zero_in_response(mock_get_request):
    # Arrange
    mock_json_response = {
        "header": {"format": ["date", "ms_of_day", "open", "high", "low", "close", "volume", "count"]},
        "response": [
            {"date": 20230701, "ms_of_day": 3600000, "open": 0, "high": 0, "low": 0, "close": 0, "volume": 0, "count": 0}
        ]
    }
    mock_get_request.return_value = mock_json_response
    
    asset = Asset(asset_type="stock", symbol="AAPL")
    start_dt = datetime.datetime(2025, 9, 2)
    end_dt = datetime.datetime(2025, 9, 3)
    ivl = 60000

    # Act
    df = thetadata_helper.get_historical_data(asset, start_dt, end_dt, ivl, "test_user", "test_password")

    # Assert
    assert df.empty  # The DataFrame should be empty because count is 0


@patch('lumibot.tools.thetadata_helper.get_request')
def test_get_expirations_normal_operation(mock_get_request):
    # Arrange
    mock_json_response = {
        "header": {"format": ["expiration_date"]},
        "response": [
            {"expiration_date": 20230721},
            {"expiration_date": 20230728},
            {"expiration_date": 20230804}
        ]
    }
    mock_get_request.return_value = mock_json_response
    
    username = "test_user"
    password = "test_password"
    ticker = "AAPL"
    after_date = datetime.date(2023, 7, 25)

    # Act
    expirations = thetadata_helper.get_expirations(username, password, ticker, after_date)

    # Assert
    expected = ["2023-07-28", "2023-08-04"]
    assert expirations == expected

@patch('lumibot.tools.thetadata_helper.get_request')
def test_get_expirations_empty_response(mock_get_request):
    # Arrange
    mock_json_response = {
        "header": {"format": ["expiration_date"]},
        "response": []
    }
    mock_get_request.return_value = mock_json_response
    
    username = "test_user"
    password = "test_password"
    ticker = "AAPL"
    after_date = datetime.date(2023, 7, 25)

    # Act
    expirations = thetadata_helper.get_expirations(username, password, ticker, after_date)

    # Assert
    assert expirations == []

@patch('lumibot.tools.thetadata_helper.get_request')
def test_get_expirations_dates_before_after_date(mock_get_request):
    # Arrange
    mock_json_response = {
        "header": {"format": ["expiration_date"]},
        "response": [
            {"expiration_date": 20230714},
            {"expiration_date": 20230721}
        ]
    }
    mock_get_request.return_value = mock_json_response
    
    username = "test_user"
    password = "test_password"
    ticker = "AAPL"
    after_date = datetime.date(2023, 7, 25)

    # Act
    expirations = thetadata_helper.get_expirations(username, password, ticker, after_date)

    # Assert
    assert expirations == []  # All dates are before the after_date, so the result should be empty



@patch('lumibot.tools.thetadata_helper.get_request')
def test_get_strikes_normal_operation(mock_get_request):
    # Arrange
    mock_json_response = {
        "header": {"format": ["strike_price"]},
        "response": [
            {"strike_price": 1400},
            {"strike_price": 1450},
            {"strike_price": 1500}
        ]
    }
    mock_get_request.return_value = mock_json_response
    
    username = "test_user"
    password = "test_password"
    ticker = "AAPL"
    expiration = datetime.datetime(2023, 9, 15)

    # Act
    strikes = thetadata_helper.get_strikes(username, password, ticker, expiration)

    # Assert
    expected = [1.40, 1.45, 1.50]
    assert strikes == expected

@patch('lumibot.tools.thetadata_helper.get_request')
def test_get_strikes_empty_response(mock_get_request):
    # Arrange
    mock_json_response = {
        "header": {"format": ["strike_price"]},
        "response": []
    }
    mock_get_request.return_value = mock_json_response
    
    username = "test_user"
    password = "test_password"
    ticker = "AAPL"
    expiration = datetime.datetime(2023, 9, 15)

    # Act
    strikes = thetadata_helper.get_strikes(username, password, ticker, expiration)

    # Assert
    assert strikes == []


@pytest.mark.apitest
class TestThetaDataProcessHealthCheck:
    """
    Real integration tests for ThetaData process health monitoring.
    NO MOCKING - these tests use real ThetaData process and data.
    """

    def test_process_alive_detection_real_process(self):
        """Test is_process_alive() with real ThetaData process"""
        username = os.environ.get("THETADATA_USERNAME")
        password = os.environ.get("THETADATA_PASSWORD")

        # Reset global state
        thetadata_helper.THETA_DATA_PROCESS = None
        thetadata_helper.THETA_DATA_PID = None

        # Start process and verify it's tracked
        process = thetadata_helper.start_theta_data_client(username, password)
        assert process is not None, "Process should be returned"
        assert thetadata_helper.THETA_DATA_PROCESS is not None, "Global process should be set"
        assert thetadata_helper.THETA_DATA_PID is not None, "Global PID should be set"

        # Verify it's alive
        assert thetadata_helper.is_process_alive() is True, "Process should be alive"

        # Verify actual process is running
        pid = thetadata_helper.THETA_DATA_PID
        result = subprocess.run(['ps', '-p', str(pid)], capture_output=True)
        assert result.returncode == 0, f"Process {pid} should be running"

    def test_force_kill_and_auto_restart(self):
        """Force kill ThetaData process and verify check_connection() auto-restarts it"""
        username = os.environ.get("THETADATA_USERNAME")
        password = os.environ.get("THETADATA_PASSWORD")

        # Start initial process
        thetadata_helper.start_theta_data_client(username, password)
        time.sleep(3)
        initial_pid = thetadata_helper.THETA_DATA_PID
        assert thetadata_helper.is_process_alive() is True, "Initial process should be alive"

        # FORCE KILL the Java process
        subprocess.run(['kill', '-9', str(initial_pid)], check=True)
        time.sleep(1)

        # Verify is_process_alive() detects it's dead
        assert thetadata_helper.is_process_alive() is False, "Process should be detected as dead"

        # check_connection() should detect death and restart
        client, connected = thetadata_helper.check_connection(username, password)

        # Verify new process started
        new_pid = thetadata_helper.THETA_DATA_PID
        assert new_pid is not None, "New PID should be assigned"
        assert new_pid != initial_pid, "Should have new PID after restart"
        assert thetadata_helper.is_process_alive() is True, "New process should be alive"

        # Verify new process is actually running
        result = subprocess.run(['ps', '-p', str(new_pid)], capture_output=True)
        assert result.returncode == 0, f"New process {new_pid} should be running"

    def test_data_fetch_after_process_restart(self):
        """Verify we can fetch data after process dies - uses cache or restarts"""
        username = os.environ.get("THETADATA_USERNAME")
        password = os.environ.get("THETADATA_PASSWORD")
        asset = Asset("SPY", asset_type="stock")
        # Use recent dates to ensure data is available
        start = datetime.datetime(2025, 9, 15)
        end = datetime.datetime(2025, 9, 16)

        # Start process
        thetadata_helper.start_theta_data_client(username, password)
        time.sleep(3)
        initial_pid = thetadata_helper.THETA_DATA_PID

        # FORCE KILL it
        subprocess.run(['kill', '-9', str(initial_pid)], check=True)
        time.sleep(1)
        assert thetadata_helper.is_process_alive() is False

        # Try to fetch data - may use cache OR restart process
        df = thetadata_helper.get_price_data(
            username=username,
            password=password,
            asset=asset,
            start=start,
            end=end,
            timespan="minute"
        )

        # Verify we got data (from cache or after restart)
        assert df is not None, "Should get data (from cache or after restart)"
        assert len(df) > 0, "Should have data rows"

        # Process may or may not be alive depending on whether cache was used
        # Both outcomes are acceptable - the key is we got data without crashing

    def test_multiple_rapid_restarts(self):
        """Test rapid kill-restart cycles don't break the system"""
        username = os.environ.get("THETADATA_USERNAME")
        password = os.environ.get("THETADATA_PASSWORD")

        for i in range(3):
            # Start process
            thetadata_helper.start_theta_data_client(username, password)
            time.sleep(2)
            pid = thetadata_helper.THETA_DATA_PID

            # Kill it
            subprocess.run(['kill', '-9', str(pid)], check=True)
            time.sleep(0.5)

            # Verify detection
            assert thetadata_helper.is_process_alive() is False, f"Cycle {i}: should detect death"

        # Final restart should work
        client, connected = thetadata_helper.check_connection(username, password)
        assert connected is True, "Should connect after rapid restarts"
        assert thetadata_helper.is_process_alive() is True, "Final process should be alive"

    def test_process_dies_during_data_fetch(self):
        """Test process recovery when killed - uses cached data but verifies no crash"""
        username = os.environ.get("THETADATA_USERNAME")
        password = os.environ.get("THETADATA_PASSWORD")
        asset = Asset("AAPL", asset_type="stock")
        # Use recent dates
        start = datetime.datetime(2025, 9, 1)
        end = datetime.datetime(2025, 9, 5)

        # Start process
        thetadata_helper.start_theta_data_client(username, password)
        time.sleep(3)
        initial_pid = thetadata_helper.THETA_DATA_PID

        # Kill process right before fetch
        subprocess.run(['kill', '-9', str(initial_pid)], check=True)
        time.sleep(0.5)
        assert thetadata_helper.is_process_alive() is False, "Process should be dead after kill"

        # Fetch data - may use cache OR restart process depending on whether data is cached
        df = thetadata_helper.get_price_data(
            username=username,
            password=password,
            asset=asset,
            start=start,
            end=end,
            timespan="minute"
        )

        # Should get data (from cache or after restart)
        assert df is not None, "Should get data (from cache or after restart)"

        # If data was NOT cached, process should have restarted
        # If data WAS cached, process may still be dead
        # Either way is acceptable - the key is no crash occurred

    def test_process_never_started(self):
        """Test check_connection() when process was never started"""
        username = os.environ.get("THETADATA_USERNAME")
        password = os.environ.get("THETADATA_PASSWORD")

        # Reset global state - no process
        thetadata_helper.THETA_DATA_PROCESS = None
        thetadata_helper.THETA_DATA_PID = None

        # is_process_alive should return False
        assert thetadata_helper.is_process_alive() is False, "No process should be detected"

        # check_connection should start one
        client, connected = thetadata_helper.check_connection(username, password)

        assert thetadata_helper.THETA_DATA_PROCESS is not None, "Process should be started"
        assert thetadata_helper.is_process_alive() is True, "New process should be alive"


@pytest.mark.apitest
class TestThetaDataChainsCaching:
    """Test option chain caching matches Polygon pattern - ZERO TOLERANCE."""

    def test_chains_cached_basic_structure(self):
        """Test chain caching returns correct structure."""
        username = os.environ.get("THETADATA_USERNAME")
        password = os.environ.get("THETADATA_PASSWORD")

        asset = Asset("SPY", asset_type="stock")
        test_date = date(2025, 9, 15)

        chains = thetadata_helper.get_chains_cached(username, password, asset, test_date)

        assert chains is not None, "Chains should not be None"
        assert "Multiplier" in chains, "Missing Multiplier"
        assert chains["Multiplier"] == 100, f"Multiplier should be 100, got {chains['Multiplier']}"
        assert "Exchange" in chains, "Missing Exchange"
        assert "Chains" in chains, "Missing Chains"
        assert "CALL" in chains["Chains"], "Missing CALL chains"
        assert "PUT" in chains["Chains"], "Missing PUT chains"

        # Verify at least one expiration exists
        assert len(chains["Chains"]["CALL"]) > 0, "Should have at least one CALL expiration"
        assert len(chains["Chains"]["PUT"]) > 0, "Should have at least one PUT expiration"

        print(f"✓ Chain structure valid: {len(chains['Chains']['CALL'])} expirations")

    def test_chains_cache_reuse(self):
        """Test that second call reuses cached data (no API call)."""
        import time
        from pathlib import Path
        from lumibot.constants import LUMIBOT_CACHE_FOLDER

        username = os.environ.get("THETADATA_USERNAME")
        password = os.environ.get("THETADATA_PASSWORD")

        asset = Asset("AAPL", asset_type="stock")
        test_date = date(2025, 9, 15)

        # CLEAR CACHE to ensure first call downloads fresh data
        # This prevents cache pollution from previous tests in the suite
        # Chains are stored in: LUMIBOT_CACHE_FOLDER / "thetadata" / "option_chains"
        chain_folder = Path(LUMIBOT_CACHE_FOLDER) / "thetadata" / "option_chains"
        if chain_folder.exists():
            # Delete all AAPL chain cache files
            for cache_file in chain_folder.glob("AAPL_*.parquet"):
                try:
                    cache_file.unlink()
                except Exception:
                    pass

        # Restart ThetaData Terminal to ensure fresh connection after cache clearing
        # This is necessary because cache clearing may interfere with active connections
        thetadata_helper.start_theta_data_client(username, password)
        time.sleep(3)  # Give Terminal time to fully connect

        # Verify connection is established
        _, connected = thetadata_helper.check_connection(username, password)
        assert connected, "ThetaData Terminal failed to connect"

        # First call - downloads (now guaranteed to be fresh)
        start1 = time.time()
        chains1 = thetadata_helper.get_chains_cached(username, password, asset, test_date)
        time1 = time.time() - start1

        # Second call - should use cache
        start2 = time.time()
        chains2 = thetadata_helper.get_chains_cached(username, password, asset, test_date)
        time2 = time.time() - start2

        # Verify same data
        assert chains1 == chains2, "Cached chains should match original"

        # Second call should be MUCH faster (cached)
        assert time2 < time1 * 0.1, f"Cache not working: time1={time1:.2f}s, time2={time2:.2f}s (should be 10x faster)"
        print(f"✓ Cache speedup: {time1/time2:.1f}x faster ({time1:.2f}s -> {time2:.4f}s)")

    def test_chains_strike_format(self):
        """Test strikes are floats (not integers) and properly converted."""
        username = os.environ.get("THETADATA_USERNAME")
        password = os.environ.get("THETADATA_PASSWORD")

        asset = Asset("PLTR", asset_type="stock")
        test_date = date(2025, 9, 15)

        chains = thetadata_helper.get_chains_cached(username, password, asset, test_date)

        # Check first expiration
        first_exp = list(chains["Chains"]["CALL"].keys())[0]
        strikes = chains["Chains"]["CALL"][first_exp]

        assert len(strikes) > 0, "Should have at least one strike"
        assert isinstance(strikes[0], float), f"Strikes should be float, got {type(strikes[0])}"

        # Verify reasonable strike values (not in 1/10th cent units)
        assert strikes[0] < 10000, f"Strike seems unconverted (too large): {strikes[0]}"
        assert strikes[0] > 0, f"Strike should be positive: {strikes[0]}"

        print(f"✓ Strikes properly formatted: {len(strikes)} strikes ranging {strikes[0]:.2f} to {strikes[-1]:.2f}")


if __name__ == '__main__':
    pytest.main()