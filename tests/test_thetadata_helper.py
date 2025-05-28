import datetime
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
    mock_load_cache.return_value = pd.DataFrame({
        "datetime": [
                    "2023-07-01 09:30:00",
                    "2023-07-01 09:31:00",
                    "2023-07-01 09:32:00",
                    "2023-07-01 09:33:00",
                    "2023-07-01 09:34:00",
                ],
        "price": [100, 101, 102, 103, 104]
    })

    # mock_load_cache.return_value["datetime"] = pd.to_datetime(mock_load_cache.return_value["datetime"])
    mock_get_missing_dates.return_value = []
    asset = Asset(asset_type="stock", symbol="AAPL")
    start = datetime.datetime(2023, 7, 1)
    end = datetime.datetime(2023, 7, 2)
    timespan = "minute"
    dt = datetime.datetime(2023, 7, 1, 9, 30)
    mock_load_cache.return_value.set_index("datetime", inplace=True)

    # Act
    df = thetadata_helper.get_price_data("test_user", "test_password", asset, start, end, timespan, dt=dt)
    df.index = pd.to_datetime(df.index)
    
    # Assert
    assert mock_load_cache.called
    assert df is not None
    assert len(df) == 5  # Data loaded from cache
    assert df.index[1] == pd.Timestamp("2023-07-01 09:31:00")
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
    mock_get_missing_dates.return_value = [datetime.datetime(2023, 7, 1)]
    mock_get_historical_data.return_value = pd.DataFrame({
        "datetime": pd.date_range("2023-07-01", periods=5, freq="min"),
        "price": [100, 101, 102, 103, 104]
    })
    mock_update_df.return_value = mock_get_historical_data.return_value
    
    asset = Asset(asset_type="stock", symbol="AAPL")
    start = datetime.datetime(2023, 7, 1)
    end = datetime.datetime(2023, 7, 2)
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
    mock_get_missing_dates.return_value = [datetime.datetime(2023, 7, 2)]
    mock_get_historical_data.return_value = pd.DataFrame({
        "datetime": pd.date_range("2023-07-02", periods=5, freq='min'),
        "price": [110, 111, 112, 113, 114]
    })
    updated_data = pd.concat([cached_data, mock_get_historical_data.return_value])
    mock_update_df.return_value = updated_data
    
    asset = Asset(asset_type="stock", symbol="AAPL")
    start = datetime.datetime(2023, 7, 1)
    end = datetime.datetime(2023, 7, 2)
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
    mock_get_missing_dates.return_value = [datetime.datetime(2023, 7, 1)]
    
    asset = Asset(asset_type="stock", symbol="AAPL")
    start = datetime.datetime(2023, 7, 1)
    end = datetime.datetime(2023, 7, 2)
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
    expected = tmpdir / "thetadata" / f"stock_SPY_1D_{datastyle}.feather"
    assert thetadata_helper.build_cache_filename(asset, timespan, datastyle) == expected

    expire_date = datetime.date(2023, 8, 1)
    option_asset = Asset("SPY", asset_type="option", expiration=expire_date, strike=100, right="CALL")
    expected = tmpdir / "thetadata" / f"option_SPY_230801_100_CALL_1D_{datastyle}.feather"
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
    "df_all, df_feather, datastyle",
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
def test_update_cache(mocker, tmpdir, df_all, df_feather, datastyle):
    mocker.patch.object(thetadata_helper, "LUMIBOT_CACHE_FOLDER", tmpdir)
    cache_file = Path(tmpdir / "thetadata" / f"stock_SPY_1D_{datastyle}.feather")
    
    # Empty DataFrame of df_all, don't write cache file
    thetadata_helper.update_cache(cache_file, df_all, df_feather)
    assert not cache_file.exists()

    # When df_all and df_feather are the same, don't write cache file
    thetadata_helper.update_cache(cache_file, df_feather, df_feather)
    assert not cache_file.exists()

    # Changes in data, write cache file
    df_all = df_feather * 10
    thetadata_helper.update_cache(cache_file, df_all, df_feather)
    assert cache_file.exists()



@pytest.mark.parametrize(
    "df_feather, datastyle",
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
def test_load_data_from_cache(mocker, tmpdir, df_feather, datastyle):
    # Setup some basics
    mocker.patch.object(thetadata_helper, "LUMIBOT_CACHE_FOLDER", tmpdir)
    cache_file = Path(tmpdir / "thetadata" / f"stock_SPY_1D_{datastyle}.feather")

    # No cache file
    with pytest.raises(FileNotFoundError):
        thetadata_helper.load_cache(cache_file)

    # Cache file exists
    cache_file.parent.mkdir(parents=True, exist_ok=True)
    df_feather.to_feather(cache_file)
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
        {"o": 1, "h": 4, "l": 1, "c": 2, "v": 100, "t": 1690896600000},
        {"o": 5, "h": 8, "l": 3, "c": 7, "v": 100, "t": 1690896660000},
    ]
    with pytest.raises(KeyError):
        thetadata_helper.update_df(df_all, result)


def test_update_df_empty_df_all_with_new_data():
    result = pd.DataFrame(
            {
                "close": [2, 3, 4, 5, 6],
                "open": [1, 2, 3, 4, 5],
                "datetime": [
                    "2023-07-01 09:30:00",
                    "2023-07-01 09:31:00",
                    "2023-07-01 09:32:00",
                    "2023-07-01 09:33:00",
                    "2023-07-01 09:34:00",
                ],
            }
        )
    
    result["datetime"] = pd.to_datetime(result["datetime"])
    df_all = None
    df_new = thetadata_helper.update_df(df_all, result)
    
    assert len(df_new) == 5
    assert df_new["close"].iloc[0] == 2
    
    # updated_df will update NewYork time to UTC time, and minus 1 min to match with polygon data
    assert df_new.index[0] == pd.DatetimeIndex(["2023-07-01 13:29:00-00:00"])[0]


def test_update_df_existing_df_all_with_new_data():
    # Test with existing dataframe and new data
    initial_data = [
        {"o": 1, "h": 4, "l": 1, "c": 2, "v": 100, "t": 1690896600000},
        {"o": 5, "h": 8, "l": 3, "c": 7, "v": 100, "t": 1690896660000},
    ]
    for r in initial_data:
        r["datetime"] = pd.to_datetime(r.pop("t"), unit='ms', utc=True)
    
    df_all = pd.DataFrame(initial_data).set_index("datetime")

    new_data = [
        {"o": 9, "h": 12, "l": 7, "c": 10, "v": 100, "t": 1690896720000},
        {"o": 13, "h": 16, "l": 11, "c": 14, "v": 100, "t": 1690896780000},
    ]
    for r in new_data:
        r["datetime"] = pd.to_datetime(r.pop("t"), unit='ms', utc=True)
    
    new_data = pd.DataFrame(new_data)
    df_new = thetadata_helper.update_df(df_all, new_data)

    assert len(df_new) == 4
    assert df_new["c"].iloc[0] == 2
    assert df_new["c"].iloc[2] == 10
    assert df_new.index[0] == pd.DatetimeIndex(["2023-08-01 13:29:00+00:00"])[0]
    assert df_new.index[2] == pd.DatetimeIndex(["2023-08-01 13:31:00+00:00"])[0]

def test_update_df_with_overlapping_data():
    # Test with some overlapping rows
    initial_data = [
        {"o": 1, "h": 4, "l": 1, "c": 2, "v": 100, "t": 1690896600000},
        {"o": 5, "h": 8, "l": 3, "c": 7, "v": 100, "t": 1690896660000},
        {"o": 9, "h": 12, "l": 7, "c": 10, "v": 100, "t": 1690896720000},
        {"o": 13, "h": 16, "l": 11, "c": 14, "v": 100, "t": 1690896780000},
    ]
    for r in initial_data:
        r["datetime"] = pd.to_datetime(r.pop("t"), unit='ms', utc=True)
    
    df_all = pd.DataFrame(initial_data).set_index("datetime")

    overlapping_data = [
        {"o": 17, "h": 20, "l": 15, "c": 18, "v": 100, "t": 1690896780000},
        {"o": 21, "h": 24, "l": 19, "c": 22, "v": 100, "t": 1690896840000},
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
    assert df_new.index[0] == pd.DatetimeIndex(["2023-08-01 13:29:00+00:00"])[0]
    assert df_new.index[2] == pd.DatetimeIndex(["2023-08-01 13:31:00+00:00"])[0]
    assert df_new.index[3] == pd.DatetimeIndex(["2023-08-01 13:32:00+00:00"])[0]
    assert df_new.index[4] == pd.DatetimeIndex(["2023-08-01 13:33:00+00:00"])[0]

def test_update_df_with_timezone_awareness():
    # Test that timezone awareness is properly handled
    result = [
        {"o": 1, "h": 4, "l": 1, "c": 2, "v": 100, "t": 1690896600000},
    ]
    for r in result:
        r["datetime"] = pd.to_datetime(r.pop("t"), unit='ms', utc=True)

    df_all = None
    df_new = thetadata_helper.update_df(df_all, result)
    
    assert df_new.index.tzinfo is not None
    assert df_new.index.tzinfo.zone == 'UTC'


@patch('lumibot.tools.thetadata_helper.requests.get')  # Mock the requests.get call
@patch('lumibot.tools.thetadata_helper.ThetaClient')  # Mock the ThetaClient class
def test_start_theta_data_client(mock_ThetaClient,mock_get):
    # Arrange
    mock_get.return_value = MagicMock(status_code=200)
    mock_client_instance = MagicMock()
    mock_ThetaClient.return_value = mock_client_instance
    BASE_URL = "http://127.0.0.1:25510"
    # Act
    client = thetadata_helper.start_theta_data_client("test_user", "test_password")

    # Assert
    mock_get.assert_called_once_with(f"{BASE_URL}/v2/system/terminal/shutdown")
    mock_ThetaClient.assert_called_once_with(username="test_user", passwd="test_password")
    time.sleep(1)  # This is to ensure that the sleep call is executed.
    assert client == mock_client_instance

@patch('lumibot.tools.thetadata_helper.start_theta_data_client')  # Mock the start_theta_data_client function
@patch('lumibot.tools.thetadata_helper.requests.get')  # Mock the requests.get call
@patch('lumibot.tools.thetadata_helper.time.sleep', return_value=None)  # Mock time.sleep to skip actual sleeping
def test_check_connection(mock_sleep, mock_get, mock_start_client):
    # Arrange
    mock_start_client.return_value = MagicMock()  # Mock the client that would be returned
    mock_get.side_effect = [
        MagicMock(text="DISCONNECTED"),  # First call returns DISCONNECTED
        MagicMock(text="RandomWords"),  # Second call force into else condition
        MagicMock(text="CONNECTED"),  # third call returns CONNECTED
    ]

    # Act
    client, connected = thetadata_helper.check_connection("test_user", "test_password")

    # Assert
    assert connected is True
    assert client == mock_start_client.return_value
    assert mock_get.call_count == 3
    assert mock_start_client.call_count == 1
    mock_sleep.assert_called_with(0.5)


@patch('lumibot.tools.thetadata_helper.start_theta_data_client')
@patch('lumibot.tools.thetadata_helper.requests.get')
@patch('lumibot.tools.thetadata_helper.time.sleep', return_value=None)
def test_check_connection_with_exception(mock_sleep, mock_get, mock_start_client):
    # Arrange
    mock_start_client.return_value = MagicMock()
    mock_get.side_effect = [requests.exceptions.RequestException]  # Simulate a request exception
    
    # Act
    client, connected = thetadata_helper.check_connection("test_user", "test_password")

    # Assert
    assert connected is False  # Should not be connected due to the exception
    assert mock_start_client.call_count == 16
    assert mock_get.call_count == 16
    assert client == mock_start_client.return_value
    mock_sleep.assert_called_with(0.5)


@patch('lumibot.tools.thetadata_helper.check_connection')
@patch('lumibot.tools.thetadata_helper.requests.get')
def test_get_request_successful(mock_get, mock_check_connection):
    # Arrange
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "header": {
            "error_type": "null"
        },
        "data": "some_data"
    }
    mock_get.return_value = mock_response
    
    url = "http://test.com"
    headers = {"Authorization": "Bearer test_token"}
    querystring = {"param1": "value1"}

    # Act
    response = thetadata_helper.get_request(url, headers, querystring, "test_user", "test_password")

    # Assert
    mock_get.assert_called_once_with(url, headers=headers, params=querystring)
    assert response == {"header": {"error_type": "null"}, "data": "some_data"}
    mock_check_connection.assert_not_called()

@patch('lumibot.tools.thetadata_helper.check_connection')
@patch('lumibot.tools.thetadata_helper.requests.get')
def test_get_request_non_200_status_code(mock_get, mock_check_connection):
    # Arrange
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.json.return_value = None
    mock_get.return_value = mock_response
    
    url = "http://test.com"
    headers = {"Authorization": "Bearer test_token"}
    querystring = {"param1": "value1"}

    # Act
    # get_request should raise a ValueError if the status code is not 200
    with pytest.raises(ValueError):
        json_resp = thetadata_helper.get_request(url, headers, querystring, "test_user", "test_password")

    expected_call = ((url,), {'headers': headers, 'params': querystring})
    
    # Assert
    assert mock_get.call_count == 2
    assert mock_get.mock_calls[0] == expected_call
    assert mock_get.mock_calls[1] == expected_call
    
    # json_resp should never be defined, so it should raise UnboundLocalError: 
    # local variable 'json_resp' referenced before assignment
    with pytest.raises(UnboundLocalError):
        json_resp  

    assert mock_check_connection.call_count == 2


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
    start_dt = datetime.datetime(2023, 7, 1)
    end_dt = datetime.datetime(2023, 7, 2)
    ivl = 60000

    # Act
    df = thetadata_helper.get_historical_data(asset, start_dt, end_dt, ivl, "test_user", "test_password")

    # Assert
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert list(df.columns) == ["open", "high", "low", "close", "volume", "count", "datetime"]
    assert df["datetime"].iloc[0] == datetime.datetime(2023, 7, 1, 1, 0, 0)
    assert df["datetime"].iloc[0].tzinfo is None
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
        asset_type="option", symbol="AAPL", expiration=datetime.datetime(2023, 9, 30), strike=140, right="CALL"
    )
    start_dt = datetime.datetime(2023, 7, 1)
    end_dt = datetime.datetime(2023, 7, 2)
    ivl = 60000

    # Act
    df = thetadata_helper.get_historical_data(asset, start_dt, end_dt, ivl, "test_user", "test_password")

    # Assert
    assert isinstance(df, pd.DataFrame)
    assert not df.empty
    assert list(df.columns) == ["open", "high", "low", "close", "volume", "count", "datetime"]
    assert df["datetime"].iloc[0] == datetime.datetime(2023, 7, 1, 1, 0, 0)
    assert df["open"].iloc[1] == 1.1


@patch('lumibot.tools.thetadata_helper.get_request')
def test_get_historical_data_empty_response(mock_get_request):
    # Arrange
    mock_get_request.return_value = None
    
    asset = Asset(asset_type="stock", symbol="AAPL")
    start_dt = datetime.datetime(2023, 7, 1)
    end_dt = datetime.datetime(2023, 7, 2)
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
    start_dt = datetime.datetime(2023, 7, 1)
    end_dt = datetime.datetime(2023, 7, 2)
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
    start_dt = datetime.datetime(2023, 7, 1)
    end_dt = datetime.datetime(2023, 7, 2)
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


if __name__ == '__main__':
    pytest.main()