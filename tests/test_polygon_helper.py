"""
test_polygon_helper.py
----------------------
Tests for the new DuckDB-based 'polygon_helper.py'. These tests:
  1) Check missing dates, trading dates, and get_polygon_symbol as before.
  2) Validate get_price_data_from_polygon(...) with a mock PolygonClient, ensuring it
     stores data in DuckDB and then reads from DuckDB (caching).
  3) Provide coverage for the DuckDB-specific helpers (like _asset_key, _load_from_duckdb,
     _store_in_duckdb, and _transform_polygon_data).
  4) Remove references to the old feather-based caching logic (build_cache_filename,
     load_cache, update_cache, update_polygon_data) that no longer exist in the new code.
"""

import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pytest
import pytz

from lumibot.entities import Asset
# We'll import everything as `ph` for polygon_helper
from lumibot.tools import polygon_helper as ph

###############################################################################
# HELPER CLASSES / FIXTURES
###############################################################################


class FakeContract:
    """Fake contract object simulating a contract returned by polygon_client.list_options_contracts(...)"""
    def __init__(self, ticker: str):
        self.ticker = ticker


@pytest.fixture
def ephemeral_duckdb(tmp_path):
    """
    A fixture that points polygon_helper's DUCKDB_DB_PATH at a temporary file
    within 'tmp_path'. Ensures each test runs with a blank ephemeral DB.
    Restores the original DUCKDB_DB_PATH afterwards.
    """
    original_path = ph.DUCKDB_DB_PATH
    test_db_path = tmp_path / "polygon_cache.duckdb"
    ph.DUCKDB_DB_PATH = test_db_path
    yield test_db_path
    ph.DUCKDB_DB_PATH = original_path


###############################################################################
# TEST: Missing Dates, Trading Dates, get_polygon_symbol
###############################################################################


class TestPolygonHelpersBasic:
    """
    Tests for get_missing_dates, get_trading_dates, get_polygon_symbol.
    """

    def test_get_missing_dates(self):
        """Check that get_missing_dates(...) handles typical stock data and option expiration logic."""
        asset = Asset("SPY")
        start_date = datetime.datetime(2023, 8, 1, 9, 30)
        end_date = datetime.datetime(2023, 8, 1, 10, 0)

        # 1) With empty DataFrame => entire date is missing
        missing = ph.get_missing_dates(pd.DataFrame(), asset, start_date, end_date)
        assert len(missing) == 1
        assert datetime.date(2023, 8, 1) in missing

        # 2) Full coverage => no missing
        idx = pd.date_range(start_date, end_date, freq="1min")
        df_cover = pd.DataFrame({
            "open": np.random.uniform(0, 100, len(idx)),
            "close": np.random.uniform(0, 100, len(idx)),
            "volume": np.random.uniform(0, 10000, len(idx))
        }, index=idx)
        missing2 = ph.get_missing_dates(df_cover, asset, start_date, end_date)
        assert not missing2

        # 3) Extended range => next day missing
        end_date2 = datetime.datetime(2023, 8, 2, 13, 0)
        missing3 = ph.get_missing_dates(df_cover, asset, start_date, end_date2)
        assert len(missing3) == 1
        assert datetime.date(2023, 8, 2) in missing3

        # 4) Option expiration scenario
        option_exp_date = datetime.date(2023, 8, 2)
        option_asset = Asset("SPY", asset_type="option", expiration=option_exp_date,
                             strike=100, right="CALL")
        extended_end = datetime.datetime(2023, 8, 3, 13, 0)
        idx2 = pd.date_range(start_date, extended_end, freq="1min")
        df_all2 = pd.DataFrame({
            "open": np.random.uniform(0, 100, len(idx2)),
            "close": np.random.uniform(0, 100, len(idx2)),
            "volume": np.random.uniform(0, 10000, len(idx2))
        }, index=idx2)

        missing_opt = ph.get_missing_dates(df_all2, option_asset, start_date, extended_end)
        # Because option expires 8/2 => no missing for 8/3 even though there's data for that day
        assert not missing_opt

    def test_get_trading_dates(self):
        """Test get_trading_dates(...) with stock, option, forex, crypto, plus an unsupported type."""
        # 1) Future => raises ValueError
        asset_fut = Asset("SPY", asset_type="future")
        sdate = datetime.datetime(2023, 7, 1, 9, 30)
        edate = datetime.datetime(2023, 7, 10, 10, 0)
        with pytest.raises(ValueError):
            ph.get_trading_dates(asset_fut, sdate, edate)

        # 2) Stock => NYSE
        asset_stk = Asset("SPY")
        tdates = ph.get_trading_dates(asset_stk, sdate, edate)
        assert datetime.date(2023, 7, 1) not in tdates  # Saturday
        assert datetime.date(2023, 7, 3) in tdates
        assert datetime.date(2023, 7, 4) not in tdates  # Holiday
        assert datetime.date(2023, 7, 9) not in tdates  # Sunday
        assert datetime.date(2023, 7, 10) in tdates

        # 3) Option => same as stock, but eventually truncated by expiration in get_missing_dates
        op_asset = Asset("SPY", asset_type="option", expiration=datetime.date(2023, 8, 1),
                         strike=100, right="CALL")
        tdates_op = ph.get_trading_dates(op_asset, sdate, edate)
        assert datetime.date(2023, 7, 3) in tdates_op

        # 4) Forex => "CME_FX"
        fx_asset = Asset("EURUSD", asset_type="forex")
        tdates_fx = ph.get_trading_dates(fx_asset, sdate, edate)
        # e.g. 7/1 is Saturday => not included
        assert datetime.date(2023, 7, 1) not in tdates_fx

        # 5) Crypto => 24/7
        c_asset = Asset("BTC", asset_type="crypto")
        tdates_c = ph.get_trading_dates(c_asset, sdate, edate)
        assert datetime.date(2023, 7, 1) in tdates_c  # Saturday => included for crypto

    def test_get_polygon_symbol(self, mocker):
        """Test get_polygon_symbol(...) for Stock, Index, Forex, Crypto, and Option."""
        poly_mock = mocker.MagicMock()

        # 1) Future => ValueError
        fut_asset = Asset("ZB", asset_type="future")
        with pytest.raises(ValueError):
            ph.get_polygon_symbol(fut_asset, poly_mock)

        # 2) Stock => "SPY"
        st_asset = Asset("SPY", asset_type="stock")
        assert ph.get_polygon_symbol(st_asset, poly_mock) == "SPY"

        # 3) Index => "I:SPX"
        idx_asset = Asset("SPX", asset_type="index")
        assert ph.get_polygon_symbol(idx_asset, poly_mock) == "I:SPX"

        # 4) Forex => must pass quote_asset or error
        fx_asset = Asset("EUR", asset_type="forex")
        with pytest.raises(ValueError):
            ph.get_polygon_symbol(fx_asset, poly_mock)
        quote = Asset("USD", asset_type="forex")
        sym_fx = ph.get_polygon_symbol(fx_asset, poly_mock, quote_asset=quote)
        assert sym_fx == "C:EURUSD"

        # 5) Crypto => "X:BTCUSD" if no quote
        crypto_asset = Asset("BTC", asset_type="crypto")
        assert ph.get_polygon_symbol(crypto_asset, poly_mock) == "X:BTCUSD"

        # 6) Option => if no contracts => returns None
        poly_mock.list_options_contracts.return_value = []
        op_asset = Asset("SPY", asset_type="option", expiration=datetime.date(2024, 1, 14),
                         strike=577, right="CALL")
        sym_none = ph.get_polygon_symbol(op_asset, poly_mock)
        assert sym_none is None

        # 7) Option => valid => returns the first
        poly_mock.list_options_contracts.return_value = [FakeContract("O:SPY240114C00577000")]
        sym_op = ph.get_polygon_symbol(op_asset, poly_mock)
        assert sym_op == "O:SPY240114C00577000"


###############################################################################
# TEST: get_price_data_from_polygon(...) with a Mock PolygonClient
###############################################################################


class TestPriceDataCache:
    """
    Tests get_price_data_from_polygon(...) to confirm:
      - It queries Polygon on first call
      - It caches data in DuckDB
      - It does not re-query Polygon on second call (unless force_cache_update=True)
    """

    def test_get_price_data_from_polygon(self, mocker, tmp_path, ephemeral_duckdb):
        """Ensures we store data on first call, then read from DuckDB on second call."""
        # Mock the PolygonClient class
        poly_mock = mocker.MagicMock()
        mocker.patch.object(ph, "PolygonClient", poly_mock)

        # We'll override the LUMIBOT_CACHE_FOLDER if needed, in case your code references it
        mocker.patch.object(ph, "LUMIBOT_CACHE_FOLDER", tmp_path)

        # If it's an option, let's pretend there's a valid contract
        poly_mock().list_options_contracts.return_value = [FakeContract("O:SPY230801C00100000")]

        # aggregator bars
        bars = [
            {"o": 10, "h": 11, "l": 9, "c": 10.5, "v": 500, "t": 1690876800000},
            {"o": 12, "h": 14, "l": 10, "c": 13, "v": 600, "t": 1690876860000},
        ]
        poly_mock.create().get_aggs.return_value = bars

        asset = Asset("SPY")
        start = datetime.datetime(2023, 8, 2, 9, 30, tzinfo=pytz.UTC)
        end = datetime.datetime(2023, 8, 2, 16, 0, tzinfo=pytz.UTC)
        timespan = "minute"

        # 1) First call => queries aggregator once
        df_first = ph.get_price_data_from_polygon("fake_api", asset, start, end, timespan)
        assert poly_mock.create().get_aggs.call_count == 1
        assert len(df_first) == 2

        # 2) Second call => aggregator not called again if missing days=0
        poly_mock.create().get_aggs.reset_mock()
        df_second = ph.get_price_data_from_polygon("fake_api", asset, start, end, timespan)
        assert poly_mock.create().get_aggs.call_count == 0
        assert len(df_second) == 2

    @pytest.mark.parametrize("force_update", [True, False])
    def test_force_cache_update(self, mocker, tmp_path, ephemeral_duckdb, force_update):
        """force_cache_update => second call re-queries aggregator."""
        poly_mock = mocker.MagicMock()
        mocker.patch.object(ph, "PolygonClient", poly_mock)
        mocker.patch.object(ph, "LUMIBOT_CACHE_FOLDER", tmp_path)

        # aggregator data
        bars = [{"o": 1, "h": 2, "l": 0.5, "c": 1.5, "v": 100, "t": 1690876800000}]
        poly_mock.create().get_aggs.return_value = bars

        asset = Asset("SPY")
        start = datetime.datetime(2023, 8, 2, 9, 30, tzinfo=pytz.UTC)
        end = datetime.datetime(2023, 8, 2, 10, 0, tzinfo=pytz.UTC)

        # first call
        df1 = ph.get_price_data_from_polygon("key", asset, start, end, "minute")
        assert len(df1) == 1
        # aggregator called once
        assert poly_mock.create().get_aggs.call_count == 1

        # second call => aggregator depends on force_update
        poly_mock.create().get_aggs.reset_mock()
        df2 = ph.get_price_data_from_polygon("key", asset, start, end, "minute", force_cache_update=force_update)

        if force_update:
            # aggregator called again
            assert poly_mock.create().get_aggs.call_count == 1
        else:
            # aggregator not called again
            assert poly_mock.create().get_aggs.call_count == 0

        assert len(df2) == 1


###############################################################################
# TEST: DuckDB-Specific Internals
###############################################################################


class TestDuckDBInternals:
    """
    Tests for internal DuckDB methods: _asset_key, _transform_polygon_data,
    _store_in_duckdb, _load_from_duckdb. We use ephemeral_duckdb to ensure
    a fresh DB each test.
    """

    def test_asset_key(self):
        """Check if _asset_key(...) returns the correct unique key for stocks vs. options."""
        st = Asset("SPY", asset_type="stock")
        assert ph._asset_key(st) == "SPY"

        op = Asset("SPY", asset_type="option",
                   expiration=datetime.date(2024, 1, 14),
                   strike=577.0, right="CALL")
        # e.g. => "SPY_240114_577.0_CALL"
        opt_key = ph._asset_key(op)
        assert "SPY_240114_577.0_CALL" == opt_key

        # Missing expiration => error
        bad_opt = Asset("SPY", asset_type="option", strike=100, right="CALL")
        with pytest.raises(ValueError):
            ph._asset_key(bad_opt)

    def test_transform_polygon_data(self):
        """_transform_polygon_data(...) should parse aggregator JSON into a DataFrame with columns & UTC index."""
        # empty => empty DataFrame
        empty_df = ph._transform_polygon_data([])
        assert empty_df.empty

        # non-empty
        results = [
            {"o": 10, "h": 12, "l": 9, "c": 11, "v": 100, "t": 1690896600000},
            {"o": 12, "h": 15, "l": 11, "c": 14, "v": 200, "t": 1690896660000},
        ]
        df = ph._transform_polygon_data(results)
        assert len(df) == 2
        assert "open" in df.columns and "close" in df.columns
        assert df.index[0] == pd.to_datetime(1690896600000, unit="ms", utc=True)

    def test_store_and_load_duckdb(self, ephemeral_duckdb):
        """
        Full test for _store_in_duckdb(...) + _load_from_duckdb(...). 
        1) Insert a small DF. 2) Load it, check correctness. 3) Insert overlap => no duplication.
        """
        asset_stk = Asset("SPY", asset_type="stock")
        timespan = "minute"

        idx = pd.date_range("2025-01-01 09:30:00", periods=3, freq="1min", tz="UTC")
        df_in = pd.DataFrame({
            "open": [10.0, 11.0, 12.0],
            "high": [11.0, 12.0, 13.0],
            "low": [9.0, 10.0, 11.0],
            "close": [10.5, 11.5, 12.5],
            "volume": [100, 200, 300],
        }, index=idx)

        # 1) Store
        ph._store_in_duckdb(asset_stk, timespan, df_in)

        # 2) Load
        loaded = ph._load_from_duckdb(asset_stk, timespan, idx[0], idx[-1])
        assert len(loaded) == 3
        assert (loaded["open"] == df_in["open"]).all()

        # 3) Partial range
        partial = ph._load_from_duckdb(asset_stk, timespan, idx[1], idx[2])
        assert len(partial) == 2

        # 4) Insert overlap => no duplication
        ph._store_in_duckdb(asset_stk, timespan, df_in)
        reloaded = ph._load_from_duckdb(asset_stk, timespan, idx[0], idx[-1])
        assert len(reloaded) == 3  # still 3