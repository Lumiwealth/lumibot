"""
test_polygon_helper.py
----------------------
Updated tests for the new DuckDB-based 'polygon_helper.py', removing old references
to feather-file caching (build_cache_filename, load_cache, update_cache, etc.).
These tests now focus on verifying:
  - get_missing_dates()
  - get_trading_dates()
  - get_polygon_symbol()
  - get_price_data_from_polygon() mocking the real Polygon calls
... etc.

If you wish to test the actual DuckDB logic, you can add tests for:
  - _load_from_duckdb()
  - _store_in_duckdb()
  - _fill_partial_days()
  - _store_placeholder_day()
... as needed.

Author: <Your Name>
Date: <Date>
"""

import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import pytz

from lumibot.entities import Asset
from lumibot.tools import polygon_helper as ph

# Mock contract used in test_get_polygon_symbol for "OPTION" logic
class FakeContract:
    """
    A fake contract object that simulates the contract object returned by
    polygon_client.list_options_contracts(...). This ensures we can test
    get_polygon_symbol(...) for an option scenario without real network calls.
    """
    def __init__(self, ticker: str):
        self.ticker = ticker


class TestPolygonHelpers:
    """
    Tests that verify logic in polygon_helper.py, primarily focusing on
    get_missing_dates, get_trading_dates, get_polygon_symbol, etc.
    Note that references to old feather-based caching have been removed,
    since the new code uses DuckDB.
    """

    def test_missing_dates(self):
        """
        Test get_missing_dates(...) with typical stock dataframes:
        - Ensuring days outside the loaded df are considered missing
        - Confirming that if we have all data for a given range, no days are missing
        """
        asset = Asset("SPY")
        start_date = datetime.datetime(2023, 8, 1, 9, 30)  # Tuesday
        end_date = datetime.datetime(2023, 8, 1, 10, 0)

        # 1) Empty DataFrame => entire date is missing
        missing_dates = ph.get_missing_dates(pd.DataFrame(), asset, start_date, end_date)
        assert len(missing_dates) == 1
        assert datetime.date(2023, 8, 1) in missing_dates

        # 2) DataFrame that covers the entire range => no missing days
        index = pd.date_range(start_date, end_date, freq="1min")
        df_all = pd.DataFrame(
            {
                "open": np.random.uniform(0, 100, len(index)).round(2),
                "close": np.random.uniform(0, 100, len(index)).round(2),
                "volume": np.random.uniform(0, 10000, len(index)).round(2),
            },
            index=index,
        )
        missing_dates = ph.get_missing_dates(df_all, asset, start_date, end_date)
        assert not missing_dates

        # 3) Extended end_date => that extra day is missing
        end_date2 = datetime.datetime(2023, 8, 2, 13, 0)  # Weds
        missing_dates = ph.get_missing_dates(df_all, asset, start_date, end_date2)
        assert missing_dates
        assert datetime.date(2023, 8, 2) in missing_dates

        # 4) Option expiration scenario
        end_date3 = datetime.datetime(2023, 8, 3, 13, 0)
        expire_date = datetime.date(2023, 8, 2)
        index2 = pd.date_range(start_date, end_date3, freq="1min")
        df_all2 = pd.DataFrame(
            {
                "open": np.random.uniform(0, 100, len(index2)).round(2),
                "close": np.random.uniform(0, 100, len(index2)).round(2),
                "volume": np.random.uniform(0, 10000, len(index2)).round(2),
            },
            index=index2,
        )
        option_asset = Asset("SPY", asset_type="option", expiration=expire_date, strike=100, right="CALL")
        missing_dates2 = ph.get_missing_dates(df_all2, option_asset, start_date, end_date3)
        # Because the option expires 2023-08-02 => data after that is irrelevant => no missing
        assert not missing_dates2

    def test_get_trading_dates(self):
        """
        Test get_trading_dates(...) with different asset types:
         - future -> raises ValueError
         - stock -> standard NYSE schedule
         - option -> also uses NYSE schedule but up to expiration
         - forex -> uses CME_FX schedule
         - crypto -> 24/7
        """
        # 1) Unsupported Asset Type -> 'future'
        asset = Asset("SPY", asset_type="future")
        start_date = datetime.datetime(2023, 7, 1, 9, 30)  # Saturday
        end_date = datetime.datetime(2023, 7, 10, 10, 0)   # Monday
        with pytest.raises(ValueError):
            ph.get_trading_dates(asset, start_date, end_date)

        # 2) Stock Asset
        asset2 = Asset("SPY")
        start_date2 = datetime.datetime(2023, 7, 1, 9, 30)  # Saturday
        end_date2 = datetime.datetime(2023, 7, 10, 10, 0)   # Monday
        trading_dates = ph.get_trading_dates(asset2, start_date2, end_date2)
        assert datetime.date(2023, 7, 1) not in trading_dates
        assert datetime.date(2023, 7, 3) in trading_dates
        assert datetime.date(2023, 7, 4) not in trading_dates  # July 4th closed
        assert datetime.date(2023, 7, 9) not in trading_dates  # Sunday
        assert datetime.date(2023, 7, 10) in trading_dates

        # 3) Option Asset
        expire_date = datetime.date(2023, 8, 1)
        option_asset = Asset("SPY", asset_type="option", expiration=expire_date, strike=100, right="CALL")
        trading_dates2 = ph.get_trading_dates(option_asset, start_date2, end_date2)
        assert datetime.date(2023, 7, 1) not in trading_dates2
        assert datetime.date(2023, 7, 3) in trading_dates2
        assert datetime.date(2023, 7, 4) not in trading_dates2
        assert datetime.date(2023, 7, 9) not in trading_dates2

        # 4) Forex Asset
        forex_asset = Asset("ES", asset_type="forex")
        trading_dates3 = ph.get_trading_dates(forex_asset, start_date2, end_date2)
        assert datetime.date(2023, 7, 1) not in trading_dates3
        assert datetime.date(2023, 7, 4) in trading_dates3
        assert datetime.date(2023, 7, 10) in trading_dates3

        # 5) Crypto Asset
        crypto_asset = Asset("BTC", asset_type="crypto")
        trading_dates4 = ph.get_trading_dates(crypto_asset, start_date2, end_date2)
        assert datetime.date(2023, 7, 1) in trading_dates4
        assert datetime.date(2023, 7, 4) in trading_dates4
        assert datetime.date(2023, 7, 10) in trading_dates4

    def test_get_polygon_symbol(self, mocker):
        """
        Test get_polygon_symbol(...) for all asset types:
         - future => raises ValueError
         - stock => returns e.g. "SPY"
         - index => "I:SPX"
         - option => queries polygon_client.list_options_contracts(...)
         - crypto => "X:BTCUSD"
         - forex => "C:ESUSD"
        """
        polygon_client = mocker.MagicMock()

        # 1) Unsupported Asset Type => future
        asset = Asset("SPY", asset_type="future")
        with pytest.raises(ValueError):
            ph.get_polygon_symbol(asset, polygon_client)

        # 2) Stock
        asset2 = Asset("SPY")
        assert ph.get_polygon_symbol(asset2, polygon_client) == "SPY"

        # 3) Index
        asset3 = Asset("SPX", asset_type="index")
        assert ph.get_polygon_symbol(asset3, polygon_client) == "I:SPX"

        # 4) Option with no contracts
        expire_date = datetime.date(2023, 8, 1)
        option_asset = Asset("SPY", asset_type="option", expiration=expire_date, strike=100, right="CALL")
        polygon_client.list_options_contracts.return_value = []
        with pytest.raises(AssertionError):  # or check for None
            # The code might return None and log an error; or raise. Adjust as needed:
            assert ph.get_polygon_symbol(option_asset, polygon_client)

        # 5) Option with a valid contract
        expected_ticker = "O:SPY230801C00100000"
        polygon_client.list_options_contracts.return_value = [FakeContract(expected_ticker)]
        assert ph.get_polygon_symbol(option_asset, polygon_client) == expected_ticker

        # 6) Crypto => "X:BTCUSD"
        crypto_asset = Asset("BTC", asset_type="crypto")
        assert ph.get_polygon_symbol(crypto_asset, polygon_client) == "X:BTCUSD"

        # 7) Forex
        forex_asset = Asset("ES", asset_type="forex")
        with pytest.raises(ValueError):
            ph.get_polygon_symbol(forex_asset, polygon_client)
        quote_asset = Asset("USD", asset_type="forex")
        assert ph.get_polygon_symbol(forex_asset, polygon_client, quote_asset) == "C:ESUSD"

class TestPolygonPriceData:
    """
    Tests for get_price_data_from_polygon using mock PolygonClient, verifying
    that we handle aggregator calls and caching logic (in DuckDB) properly.
    """

    def test_get_price_data_from_polygon(self, mocker, tmpdir):
        """
        Mocks calls to PolygonClient and ensures we fetch data from aggregator
        once, then rely on the local DuckDB cache for subsequent calls.
        """
        mock_polyclient = mocker.MagicMock()
        mocker.patch.object(ph, "PolygonClient", mock_polyclient)
        # If your code references LUMIBOT_CACHE_FOLDER for DuckDB, you can override it:
        mocker.patch.object(ph, "LUMIBOT_CACHE_FOLDER", tmpdir)

        # Return a fake contract for an option scenario if tested
        option_ticker = "O:SPY230801C00100000"
        mock_polyclient().list_options_contracts.return_value = [FakeContract(option_ticker)]

        api_key = "abc123"
        asset = Asset("SPY")
        tz_e = pytz.timezone("US/Eastern")
        start_date = tz_e.localize(datetime.datetime(2023, 8, 2, 6, 30))
        end_date = tz_e.localize(datetime.datetime(2023, 8, 2, 13, 0))
        timespan = "minute"

        # 1) Fake aggregator data from Polygon
        mock_polyclient.create().get_aggs.return_value = [
            {"o": 1, "h": 4, "l": 1, "c": 2, "v": 100, "t": 1690876800000},
            {"o": 5, "h": 8, "l": 3, "c": 7, "v": 100, "t": 1690876860000},
            {"o": 9, "h": 12, "l": 7, "c": 10, "v": 100, "t": 1690876920000},
            {"o": 13, "h": 16, "l": 11, "c": 14, "v": 100, "t": 1690986600000},
            {"o": 17, "h": 20, "l": 15, "c": 18, "v": 100, "t": 1690986660000},
            {"o": 21, "h": 24, "l": 19, "c": 22, "v": 100, "t": 1691105400000},
        ]

        df = ph.get_price_data_from_polygon(api_key, asset, start_date, end_date, timespan)
        # We confirm aggregator was called once
        assert mock_polyclient.create().get_aggs.call_count == 1
        # We can confirm we got 6 bars
        assert len(df) == 6

        # 2) Reset aggregator calls, run the same query => it should skip aggregator
        mock_polyclient.create().get_aggs.reset_mock()
        df2 = ph.get_price_data_from_polygon(api_key, asset, start_date, end_date, timespan)
        # No aggregator calls now (we rely on DuckDB cache)
        assert mock_polyclient.create().get_aggs.call_count == 0
        assert len(df2) == 6
        # Ensure we still get the same data
        assert df2["close"].iloc[0] == 2

        # 3) If we nudge end_date out but we have the data => still no aggregator call
        mock_polyclient.create().get_aggs.reset_mock()
        end_date_extended = tz_e.localize(datetime.datetime(2023, 8, 2, 16, 0))
        df3 = ph.get_price_data_from_polygon(api_key, asset, start_date, end_date_extended, timespan)
        assert mock_polyclient.create().get_aggs.call_count == 0
        assert len(df3) == 6

        # 4) If we shift the date to a new day => aggregator call again
        mock_polyclient.create().get_aggs.reset_mock()
        new_start = tz_e.localize(datetime.datetime(2023, 8, 4, 6, 30))
        new_end = tz_e.localize(datetime.datetime(2023, 8, 4, 13, 0))
        mock_polyclient.create().get_aggs.return_value = [
            {"o": 9, "h": 12, "l": 7, "c": 10, "v": 100, "t": 1691191800000},
        ]
        df4 = ph.get_price_data_from_polygon(api_key, asset, new_start, new_end, timespan)
        # aggregator is called once for the new day
        assert mock_polyclient.create().get_aggs.call_count == 1
        assert len(df4) == 1 + 6  # if it merges new day with old? or just 1 bar new

        # 5) Large range => aggregator in multiple chunks
        mock_polyclient.create().get_aggs.reset_mock()
        new_end2 = tz_e.localize(datetime.datetime(2023, 8, 31, 13, 0))
        mock_polyclient.create().get_aggs.side_effect = [
            [{"o": 5, "h": 8, "c": 7, "l": 3, "v": 100, "t": 1690876800000}],
            [{"o": 9, "h": 12, "c": 10, "l": 7, "v": 100, "t": 1690986660000}],
            [{"o": 13, "h": 16, "c": 14, "l": 11, "v": 100, "t": 1691105400000}],
        ]
        df5 = ph.get_price_data_from_polygon(api_key, asset, start_date, new_end2, timespan)
        # We chunk out the range => aggregator calls multiple times
        calls = mock_polyclient.create().get_aggs.call_count
        assert calls >= 2  # depends on how you group missing days, but typically 3 in side_effect
        # The returned data is side_effect merged
        assert len(df5) == 6 + 1 + 1 + 1  # if we retained the prior 6 from earlier

    @pytest.mark.parametrize("timespan", ["day", "minute"])
    @pytest.mark.parametrize("force_cache_update", [True, False])
    def test_polygon_missing_day_caching(self, mocker, tmpdir, timespan, force_cache_update):
        """
        Test that get_price_data_from_polygon(...) properly caches days in DuckDB
        and doesn't re-fetch them unless force_cache_update=True. Mocks aggregator calls
        for a date range, ensures we see 1 aggregator call first time, then 0 if repeated
        (unless force_cache_update => then calls aggregator again).
        """
        mock_polyclient = mocker.MagicMock()
        mocker.patch.object(ph, "PolygonClient", mock_polyclient)
        mocker.patch.object(ph, "LUMIBOT_CACHE_FOLDER", tmpdir)

        api_key = "abc123"
        asset = Asset("SPY")
        tz_e = pytz.timezone("US/Eastern")
        start_date = tz_e.localize(datetime.datetime(2023, 8, 2, 6, 30))  
        end_date = tz_e.localize(datetime.datetime(2023, 8, 2, 13, 0))

        # We pretend aggregator returns 20 bars (day or minute doesn't matter for the test).
        bars = []
        cur = start_date
        while cur <= end_date:
            bars.append({"o": 1, "h": 2, "l": 0, "c": 1.5, "v": 100, "t": int(cur.timestamp() * 1000)})
            if timespan == "minute":
                cur += datetime.timedelta(minutes=1)
            else:
                cur += datetime.timedelta(days=1)

        mock_polyclient.create().get_aggs.return_value = bars

        df = ph.get_price_data_from_polygon(api_key, asset, start_date, end_date, timespan, force_cache_update=force_cache_update)
        # first call => aggregator once
        assert mock_polyclient.create().get_aggs.call_count == 1
        assert len(df) == len(bars)

        # second call => aggregator zero times if force_cache_update=False
        mock_polyclient.create().get_aggs.reset_mock()
        df2 = ph.get_price_data_from_polygon(api_key, asset, start_date, end_date, timespan, force_cache_update=force_cache_update)
        if force_cache_update:
            # aggregator is called again
            assert mock_polyclient.create().get_aggs.call_count == 1
        else:
            # aggregator not called
            assert mock_polyclient.create().get_aggs.call_count == 0
        assert len(df2) == len(bars)