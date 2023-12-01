import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import pytz
from lumibot.entities import Asset
from lumibot.tools import polygon_helper as ph


class FakeContract:
    def __init__(self, ticker):
        self.ticker = ticker


class TestPolygonHelpers:
    def test_build_cache_filename(self, mocker, tmpdir):
        asset = Asset("SPY")
        timespan = "1D"
        mocker.patch.object(ph, "LUMIBOT_CACHE_FOLDER", tmpdir)
        expected = tmpdir / "polygon" / "stock_SPY_1D.csv"
        assert ph.build_cache_filename(asset, timespan) == expected

        expire_date = datetime.date(2023, 8, 1)
        option_asset = Asset("SPY", asset_type="option", expiration=expire_date, strike=100, right="CALL")
        expected = tmpdir / "polygon" / "option_SPY_230801_100_CALL_1D.csv"
        assert ph.build_cache_filename(option_asset, timespan) == expected

        # Bad option asset with no expiration
        option_asset = Asset("SPY", asset_type="option", strike=100, right="CALL")
        with pytest.raises(ValueError):
            ph.build_cache_filename(option_asset, timespan)

    def test_missing_dates(self):
        # Setup some basics
        asset = Asset("SPY")
        start_date = datetime.datetime(2023, 8, 1, 9, 30)  # Tuesday
        end_date = datetime.datetime(2023, 8, 1, 10, 0)

        # Empty DataFrame
        missing_dates = ph.get_missing_dates(pd.DataFrame(), asset, start_date, end_date)
        assert len(missing_dates) == 1
        assert datetime.date(2023, 8, 1) in missing_dates

        # Small dataframe that meets start/end criteria
        index = pd.date_range(start_date, end_date, freq="1min")
        df_all = pd.DataFrame({
            "open": np.random.uniform(0, 100, len(index)).round(2),
            "close": np.random.uniform(0, 100, len(index)).round(2),
            "volume": np.random.uniform(0, 10000, len(index)).round(2),
        }, index=index)
        missing_dates = ph.get_missing_dates(df_all, asset, start_date, end_date)
        assert not missing_dates

        # Small dataframe that does not meet start/end criteria
        end_date = datetime.datetime(2023, 8, 2, 13, 0)  # Weds
        missing_dates = ph.get_missing_dates(df_all, asset, start_date, end_date)
        assert missing_dates
        assert datetime.date(2023, 8, 2) in missing_dates

        # Asking for data beyond option expiration - We have all the data
        end_date = datetime.datetime(2023, 8, 3, 13, 0)
        expire_date = datetime.date(2023, 8, 2)
        index = pd.date_range(start_date, end_date, freq="1min")
        df_all = pd.DataFrame({
            "open": np.random.uniform(0, 100, len(index)).round(2),
            "close": np.random.uniform(0, 100, len(index)).round(2),
            "volume": np.random.uniform(0, 10000, len(index)).round(2),
        }, index=index)
        option_asset = Asset("SPY", asset_type="option", expiration=expire_date, strike=100, right="CALL")
        missing_dates = ph.get_missing_dates(df_all, option_asset, start_date, end_date)
        assert not missing_dates

    def test_get_trading_dates(self):

        # Unsupported Asset Type
        asset = Asset("SPY", asset_type="future")
        start_date = datetime.datetime(2023, 7, 1, 9, 30)  # Saturday
        end_date = datetime.datetime(2023, 7, 10, 10, 0)  # Monday
        with pytest.raises(ValueError):
            ph.get_trading_dates(asset, start_date, end_date)

        # Stock Asset
        asset = Asset("SPY")
        start_date = datetime.datetime(2023, 7, 1, 9, 30)  # Saturday
        end_date = datetime.datetime(2023, 7, 10, 10, 0)  # Monday
        trading_dates = ph.get_trading_dates(asset, start_date, end_date)
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
        trading_dates = ph.get_trading_dates(option_asset, start_date, end_date)
        assert datetime.date(2023, 7, 1) not in trading_dates, "Market is closed on Saturday"
        assert datetime.date(2023, 7, 3) in trading_dates
        assert datetime.date(2023, 7, 4) not in trading_dates, "Market is closed on July 4th"
        assert datetime.date(2023, 7, 9) not in trading_dates, "Market is closed on Sunday"

        # Forex Asset - Trades weekdays opens Sunday at 5pm and closes Friday at 5pm
        forex_asset = Asset("ES", asset_type="forex")
        start_date = datetime.datetime(2023, 7, 1, 9, 30)  # Saturday
        end_date = datetime.datetime(2023, 7, 10, 10, 0)  # Monday
        trading_dates = ph.get_trading_dates(forex_asset, start_date, end_date)
        assert datetime.date(2023, 7, 1) not in trading_dates, "Market is closed on Saturday"
        assert datetime.date(2023, 7, 4) in trading_dates
        assert datetime.date(2023, 7, 10) in trading_dates
        assert datetime.date(2023, 7, 11) not in trading_dates, "Outside of end_date"

        # Crypto Asset - Trades 24/7
        crypto_asset = Asset("BTC", asset_type="crypto")
        start_date = datetime.datetime(2023, 7, 1, 9, 30)  # Saturday
        end_date = datetime.datetime(2023, 7, 10, 10, 0)  # Monday
        trading_dates = ph.get_trading_dates(crypto_asset, start_date, end_date)
        assert datetime.date(2023, 7, 1) in trading_dates
        assert datetime.date(2023, 7, 4) in trading_dates
        assert datetime.date(2023, 7, 10) in trading_dates

    def test_get_polygon_symbol(self, mocker):
        polygon_client = mocker.MagicMock()

        # ------- Unsupported Asset Type
        asset = Asset("SPY", asset_type="future")
        with pytest.raises(ValueError):
            ph.get_polygon_symbol(asset, polygon_client)

        # ------- Stock
        asset = Asset("SPY")
        assert ph.get_polygon_symbol(asset, polygon_client) == "SPY"

        # ------- Option
        expire_date = datetime.date(2023, 8, 1)
        option_asset = Asset("SPY", asset_type="option", expiration=expire_date, strike=100, right="CALL")
        # Option with no contracts - Error
        polygon_client.list_options_contracts.return_value = []
        with pytest.raises(LookupError):
            ph.get_polygon_symbol(option_asset, polygon_client)

        # Option with contracts - Works
        expected_ticker = "O:SPY230801C00100000"
        polygon_client.list_options_contracts.return_value = [FakeContract(expected_ticker)]
        assert ph.get_polygon_symbol(option_asset, polygon_client) == expected_ticker

        # -------- Crypto
        crypto_asset = Asset("BTC", asset_type="crypto")
        assert ph.get_polygon_symbol(crypto_asset, polygon_client) == "X:BTCUSD"

        # -------- Forex
        forex_asset = Asset("ES", asset_type="forex")
        # Errors without a Quote Asset
        with pytest.raises(ValueError):
            ph.get_polygon_symbol(forex_asset, polygon_client)
        # Works with a Quote Asset
        quote_asset = Asset("USD", asset_type='forex')
        assert ph.get_polygon_symbol(forex_asset, polygon_client, quote_asset) == "C:ESUSD"

    def test_load_data_from_cache(self, tmpdir):
        # Setup some basics
        cache_file = tmpdir / "stock_SPY_1D.csv"

        # No cache file
        with pytest.raises(FileNotFoundError):
            ph.load_cache(cache_file)

        # Cache file exists
        df = pd.DataFrame({"close": [2, 3, 4, 5, 6],
                           "open": [1, 2, 3, 4, 5],
                           "datetime": ["2023-07-01 09:30:00-04:00", "2023-07-01 09:31:00-04:00",
                                        "2023-07-01 09:32:00-04:00", "2023-07-01 09:33:00-04:00",
                                        "2023-07-01 09:34:00-04:00"],
                           })
        df.to_csv(cache_file)
        df_loaded = ph.load_cache(cache_file)
        assert len(df_loaded)
        assert df_loaded["close"].iloc[0] == 2
        assert df_loaded.index[0] == pd.DatetimeIndex(["2023-07-01 09:30:00-04:00"])[0]

        # Dataframe with no Timezone
        df = pd.DataFrame({"close": [2, 3, 4, 5, 6],
                           "open": [1, 2, 3, 4, 5],
                           "datetime": ["2023-07-01 09:30:00", "2023-07-01 09:31:00",
                                        "2023-07-01 09:32:00", "2023-07-01 09:33:00",
                                        "2023-07-01 09:34:00"],
                           })
        df.to_csv(cache_file)
        df_loaded = ph.load_cache(cache_file)
        assert len(df_loaded)
        assert df_loaded["close"].iloc[0] == 2
        assert df_loaded.index[0] == pd.DatetimeIndex(["2023-07-01 09:30:00-00:00"])[0]

    def test_update_cache(self, tmpdir):
        cache_file = Path(tmpdir / "polygon" / "stock_SPY_1D.csv")
        df = pd.DataFrame({"close": [2, 3, 4, 5, 6],
                           "open": [1, 2, 3, 4, 5],
                           "datetime": ["2023-07-01 09:30:00-04:00", "2023-07-01 09:31:00-04:00",
                                        "2023-07-01 09:32:00-04:00", "2023-07-01 09:33:00-04:00",
                                        "2023-07-01 09:34:00-04:00"],
                           })

        # No changes in data, don't write cache file
        ph.update_cache(cache_file, df_all=df, df_csv=df)
        assert not cache_file.exists()

        # Empty DataFrame, don't write cache file
        ph.update_cache(cache_file, df_all=pd.DataFrame(), df_csv=df)
        assert not cache_file.exists()

        # Changes in data, write cache file
        ph.update_cache(cache_file, df_all=df, df_csv=df.iloc[1:])
        assert cache_file.exists()

    def test_update_polygon_data(self):
        # Test with empty dataframe and no new data
        df_all = None
        poly_result = []
        df_new = ph.update_polygon_data(df_all, poly_result)
        assert not df_new

        # Test with empty dataframe and new data
        poly_result = [
            {"o": 1, "h": 4, "l": 1, "c": 2, "v": 100, "t": 1690896600000},
            {"o": 5, "h": 8, "l": 3, "c": 7, "v": 100, "t": 1690896660000},
        ]
        df_all = None
        df_new = ph.update_polygon_data(df_all, poly_result)
        assert len(df_new) == 2
        assert df_new["close"].iloc[0] == 2
        assert df_new.index[0] == pd.DatetimeIndex(["2023-08-01 13:30:00-00:00"])[0]

        # Test with existing dataframe and new data
        df_all = df_new
        poly_result = [
            {"o": 9, "h": 12, "l": 7, "c": 10, "v": 100, "t": 1690896720000},
            {"o": 13, "h": 16, "l": 11, "c": 14, "v": 100, "t": 1690896780000},
        ]
        df_new = ph.update_polygon_data(df_all, poly_result)
        assert len(df_new) == 4
        assert df_new["close"].iloc[0] == 2
        assert df_new["close"].iloc[2] == 10
        assert df_new.index[0] == pd.DatetimeIndex(["2023-08-01 13:30:00-00:00"])[0]
        assert df_new.index[2] == pd.DatetimeIndex(["2023-08-01 13:32:00-00:00"])[0]

        # Test with some overlapping rows
        df_all = df_new
        poly_result = [
            {"o": 17, "h": 20, "l": 15, "c": 18, "v": 100, "t": 1690896780000},
            {"o": 21, "h": 24, "l": 19, "c": 22, "v": 100, "t": 1690896840000},
        ]
        df_new = ph.update_polygon_data(df_all, poly_result)
        assert len(df_new) == 5
        assert df_new["close"].iloc[0] == 2
        assert df_new["close"].iloc[2] == 10
        assert df_new["close"].iloc[4] == 22
        assert df_new.index[0] == pd.DatetimeIndex(["2023-08-01 13:30:00-00:00"])[0]
        assert df_new.index[2] == pd.DatetimeIndex(["2023-08-01 13:32:00-00:00"])[0]
        assert df_new.index[4] == pd.DatetimeIndex(["2023-08-01 13:34:00-00:00"])[0]


class TestPolygonPriceData:
    def test_get_price_data_from_polygon(self, mocker, tmpdir):
        # Ensure we don't accidentally call the real Polygon API
        mock_polyclient = mocker.MagicMock()
        mocker.patch.object(ph, "RESTClient", mock_polyclient)
        mocker.patch.object(ph, "WAIT_TIME", 0)
        mocker.patch.object(ph, "LUMIBOT_CACHE_FOLDER", tmpdir)

        # Options Contracts to return
        option_ticker = "O:SPY230801C00100000"
        mock_polyclient().list_options_contracts.return_value = [FakeContract(option_ticker)]

        # Basic Setup
        api_key = "abc123"
        asset = Asset("SPY")
        tz_e = pytz.timezone("US/Eastern")
        start_date = tz_e.localize(datetime.datetime(2023, 8, 2, 6, 30))  # Include PreMarket
        end_date = tz_e.localize(datetime.datetime(2023, 8, 2, 13, 0))
        timespan = 'minute'
        expected_cachefile = ph.build_cache_filename(asset, timespan)

        assert not expected_cachefile.exists()
        assert not expected_cachefile.parent.exists()

        # Fake some data from Polygon
        mock_polyclient().get_aggs.return_value = [
            {"o": 1, "h": 4, "l": 1, "c": 2, "v": 100, "t": 1690876800000},  # 8/1/2023 8am UTC (start - 1day)
            {"o": 5, "h": 8, "l": 3, "c": 7, "v": 100, "t": 1690876860000},
            {"o": 9, "h": 12, "l": 7, "c": 10, "v": 100, "t": 1690876920000},
            {"o": 13, "h": 16, "l": 11, "c": 14, "v": 100, "t": 1690986600000},  # 8/2/2023 at least 1 entry per date
            {"o": 17, "h": 20, "l": 15, "c": 18, "v": 100, "t": 1690986660000},
            {"o": 21, "h": 24, "l": 19, "c": 22, "v": 100, "t": 1691105400000},  # 8/3/2023 11pm UTC (end + 1day)
        ]

        df = ph.get_price_data_from_polygon(api_key, asset, start_date, end_date, timespan)
        assert len(df) == 6
        assert df["close"].iloc[0] == 2
        assert mock_polyclient().get_aggs.call_count == 1
        assert expected_cachefile.exists()

        # Do the same query, but this time we should get the data from the cache
        mock_polyclient().get_aggs.reset_mock()
        df = ph.get_price_data_from_polygon(api_key, asset, start_date, end_date, timespan)
        assert len(df) == 6
        assert df["close"].iloc[0] == 2
        assert mock_polyclient().get_aggs.call_count == 0

        # End time is moved out by a few hours, but it doesn't matter because we have all the data we need
        mock_polyclient().get_aggs.reset_mock()
        end_date = tz_e.localize(datetime.datetime(2023, 8, 2, 16, 0))
        df = ph.get_price_data_from_polygon(api_key, asset, start_date, end_date, timespan)
        assert len(df) == 6
        assert mock_polyclient().get_aggs.call_count == 0

        # New day, new data
        mock_polyclient().get_aggs.reset_mock()
        start_date = tz_e.localize(datetime.datetime(2023, 8, 4, 6, 30))
        end_date = tz_e.localize(datetime.datetime(2023, 8, 4, 13, 0))
        mock_polyclient().get_aggs.return_value = [
            {"o": 5, "h": 8, "l": 3, "c": 7, "v": 100, "t": 1691136000000},  # 8/2/2023 8am UTC (start - 1day)
            {"o": 9, "h": 12, "l": 7, "c": 10, "v": 100, "t": 1691191800000},
        ]
        df = ph.get_price_data_from_polygon(api_key, asset, start_date, end_date, timespan)
        assert len(df) == 6 + 2
        assert mock_polyclient().get_aggs.call_count == 1

        # Error case: Polygon returns nothing - like for a future date it doesn't know about
        mock_polyclient().get_aggs.reset_mock()
        mock_polyclient().get_aggs.return_value = []
        end_date = tz_e.localize(datetime.datetime(2023, 8, 31, 13, 0))

        # Query a large range of dates and ensure we break up the Polygon API calls into
        # multiple queries.
        expected_cachefile.unlink()
        mock_polyclient().get_aggs.reset_mock()
        mock_polyclient().get_aggs.side_effect = [
            # First call for Auguest Data
            [
                {"o": 5, "h": 8, "l": 3, "c": 7, "v": 100, "t": 1690876800000},  # 8/1/2023 8am UTC
                {"o": 9, "h": 12, "l": 7, "c": 10, "v": 100, "t": 1693497600000},  # 8/31/2023 8am UTC
            ],
            # Second call for September Data
            [
                {"o": 13, "h": 16, "l": 11, "c": 14, "v": 100, "t": 1693584000000},  # 9/1/2023 8am UTC
                {"o": 17, "h": 20, "l": 15, "c": 18, "v": 100, "t": 1696176000000},  # 10/1/2023 8am UTC
            ],
            # Third call for October Data
            [
                {"o": 21, "h": 24, "l": 19, "c": 22, "v": 100, "t": 1696262400000},  # 10/2/2023 8am UTC
                {"o": 25, "h": 28, "l": 23, "c": 26, "v": 100, "t": 1698768000000},  # 10/31/2023 8am UTC
            ],
        ]
        start_date = tz_e.localize(datetime.datetime(2023, 8, 1, 6, 30))
        end_date = tz_e.localize(datetime.datetime(2023, 10, 31, 13, 0))  # ~90 days
        df = ph.get_price_data_from_polygon(api_key, asset, start_date, end_date, timespan)
        assert mock_polyclient().get_aggs.call_count == 3
        assert len(df) == 2 + 2 + 2
