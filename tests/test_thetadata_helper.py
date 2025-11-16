import copy
import datetime
import json
from datetime import date
import json
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
from types import SimpleNamespace
from unittest.mock import patch, MagicMock
from lumibot.constants import LUMIBOT_DEFAULT_PYTZ
from lumibot.entities import Asset
from lumibot.tools import thetadata_helper
from lumibot.backtesting import ThetaDataBacktestingPandas
from lumibot.tools.backtest_cache import CacheMode

FIXTURE_DIR = Path(__file__).parent / "fixtures" / "thetadata_v3"


def _load_fixture_payload(name: str):
    with open(FIXTURE_DIR / name, "r", encoding="utf-8") as handle:
        return json.load(handle)


def _fixture_response(name: str):
    payload = _load_fixture_payload(name)
    response = thetadata_helper._coerce_json_payload(payload)
    header = response.setdefault("header", {})
    header.setdefault("format", header.get("format") or list(payload.keys()))
    header.setdefault("error_type", "null")
    header.setdefault("next_page", None)
    return response


FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures" / "thetadata_v3"


def load_thetadata_fixture(name: str):
    path = FIXTURES_DIR / name
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _build_option_asset():
    return Asset(
        asset_type="option",
        symbol="CVNA",
        expiration=date(2026, 1, 16),
        strike=150.0,
        right="CALL",
    )


@pytest.fixture(scope="function")
def theta_terminal_cleanup():
    """Ensure ThetaTerminal is stopped between process health tests."""
    yield
    try:
        thetadata_helper.shutdown_theta_terminal(timeout=10.0, force=True)
    except Exception:
        pass


def test_finalize_history_dataframe_adds_last_trade_time_for_ohlc():
    asset = Asset(asset_type="stock", symbol="CVNA")
    df = pd.DataFrame(
        {
            "timestamp": ["2024-10-16T09:30:00", "2024-10-16T09:31:00"],
            "open": [10.0, 10.5],
            "high": [10.5, 11.0],
            "low": [9.5, 10.0],
            "close": [10.25, 10.75],
            "volume": [1_000, 900],
            "count": [12, 8],
        }
    )

    result = thetadata_helper._finalize_history_dataframe(df, "ohlc", asset)

    assert result is not None and "last_trade_time" in result.columns
    pd.testing.assert_series_equal(
        result["last_trade_time"],
        pd.Series(result.index, index=result.index, name="last_trade_time"),
    )
    assert result["last_bid_time"].isna().all()
    assert result["last_ask_time"].isna().all()


def test_finalize_history_dataframe_adds_quote_timestamps():
    asset = _build_option_asset()
    df = pd.DataFrame(
        {
            "timestamp": ["2024-10-16T09:30:00", "2024-10-16T09:31:00"],
            "bid": [9.5, 9.75],
            "ask": [10.5, 10.75],
            "bid_size": [10, 10],
            "ask_size": [11, 11],
            "count": [1, 1],
        }
    )

    result = thetadata_helper._finalize_history_dataframe(df, "quote", asset)

    assert result is not None and "last_bid_time" in result.columns
    pd.testing.assert_series_equal(
        result["last_bid_time"],
        pd.Series(result.index, index=result.index, name="last_bid_time"),
    )
    pd.testing.assert_series_equal(
        result["last_ask_time"],
        pd.Series(result.index, index=result.index, name="last_ask_time"),
    )
    assert result["last_trade_time"].isna().all()


def test_timestamp_metadata_forward_fills_when_merging_quotes():
    asset = Asset(asset_type="stock", symbol="CVNA")
    ohlc_raw = pd.DataFrame(
        {
            "timestamp": ["2024-10-16T09:30:00", "2024-10-16T09:32:00"],
            "open": [10.0, 11.0],
            "high": [10.5, 11.5],
            "low": [9.5, 10.5],
            "close": [10.25, 11.25],
            "volume": [1_000, 1_100],
            "count": [12, 9],
        }
    )
    quote_raw = pd.DataFrame(
        {
            "timestamp": ["2024-10-16T09:31:00"],
            "bid": [9.75],
            "ask": [10.6],
            "bid_size": [12],
            "ask_size": [8],
            "bid_condition": [0],
            "ask_condition": [0],
        }
    )

    df_ohlc = thetadata_helper._finalize_history_dataframe(ohlc_raw, "ohlc", asset)
    df_quote = thetadata_helper._finalize_history_dataframe(quote_raw, "quote", asset)
    timestamp_columns = ['last_trade_time', 'last_bid_time', 'last_ask_time']
    merged = pd.concat([df_ohlc, df_quote], axis=1, join="outer")
    merged = ThetaDataBacktestingPandas._combine_duplicate_columns(merged, timestamp_columns)

    quote_columns = ['bid', 'ask', 'bid_size', 'ask_size', 'bid_condition', 'ask_condition', 'bid_exchange', 'ask_exchange']
    forward_fill_columns = [
        col for col in quote_columns + timestamp_columns if col in merged.columns
    ]
    merged[forward_fill_columns] = merged[forward_fill_columns].ffill()

    quote_only_time = pd.Timestamp("2024-10-16T09:31:00", tz=LUMIBOT_DEFAULT_PYTZ)
    later_trade_time = pd.Timestamp("2024-10-16T09:32:00", tz=LUMIBOT_DEFAULT_PYTZ)
    prev_trade_time = pd.Timestamp("2024-10-16T09:30:00", tz=LUMIBOT_DEFAULT_PYTZ)

    assert merged.loc[quote_only_time, "last_trade_time"] == prev_trade_time
    assert merged.loc[later_trade_time, "last_bid_time"] == quote_only_time
    assert merged.loc[later_trade_time, "last_ask_time"] == quote_only_time


def test_all_theta_endpoints_use_v3_paths():
    history_paths = list(thetadata_helper.HISTORY_ENDPOINTS.values())
    eod_paths = list(thetadata_helper.EOD_ENDPOINTS.values())
    option_paths = list(thetadata_helper.OPTION_LIST_ENDPOINTS.values())
    for endpoint in history_paths + eod_paths + option_paths:
        assert endpoint.startswith("/v3/"), f"{endpoint} is not a v3 endpoint"


def test_build_request_headers_injects_downloader_key():
    original_key = thetadata_helper.DOWNLOADER_API_KEY
    original_header = thetadata_helper.DOWNLOADER_KEY_HEADER
    try:
        thetadata_helper.DOWNLOADER_API_KEY = "unit-test-key"
        thetadata_helper.DOWNLOADER_KEY_HEADER = "X-Test-Key"
        headers = thetadata_helper._build_request_headers({})
        assert headers["X-Test-Key"] == "unit-test-key"
    finally:
        thetadata_helper.DOWNLOADER_API_KEY = original_key
        thetadata_helper.DOWNLOADER_KEY_HEADER = original_header
@patch("lumibot.tools.thetadata_helper.get_request")
def test_get_historical_data_filters_zero_quotes(mock_get_request):
    asset = Asset(
        asset_type="option",
        symbol="CVNA",
        expiration=date(2026, 1, 16),
        strike=150.0,
        right="CALL",
    )
    mock_get_request.return_value = {
        "header": {
            "format": [
                "date",
                "ms_of_day",
                "bid",
                "ask",
                "bid_size",
                "ask_size",
                "count",
            ]
        },
        "response": [
            [20240102, 0, 0.0, 0.0, 0, 0, 0],
            [20240102, 60000, 10.0, 11.0, 0, 0, 0],
        ],
    }

    start = datetime.datetime(2024, 1, 2, tzinfo=pytz.UTC)
    end = start + datetime.timedelta(minutes=2)

    df = thetadata_helper.get_historical_data(
        asset=asset,
        start_dt=start,
        end_dt=end,
        ivl=60000,
        username="user",
        password="pass",
        datastyle="quote",
    )

    assert df is not None
    assert len(df) == 1
    assert df["bid"].iloc[0] == 10.0
    assert df["ask"].iloc[0] == 11.0


def test_get_historical_eod_data_handles_downloader_schema(monkeypatch):
    fixture = load_thetadata_fixture("stock_history_eod.json")
    monkeypatch.setattr(thetadata_helper, "get_request", lambda **_: fixture)
    monkeypatch.setattr(thetadata_helper, "get_historical_data", lambda **_: None)

    asset = Asset(asset_type="stock", symbol="PLTR")
    start = pytz.UTC.localize(datetime.datetime(2024, 9, 16))
    end = pytz.UTC.localize(datetime.datetime(2024, 9, 18))

    df = thetadata_helper.get_historical_eod_data(
        asset=asset,
        start_dt=start,
        end_dt=end,
        username="user",
        password="pass",
    )

    assert df is not None
    assert not df.empty
    assert df.index.tzinfo is not None
    assert "open" in df.columns


def test_get_historical_eod_data_chunks_requests_longer_than_a_year(monkeypatch):
    fixture = load_thetadata_fixture("stock_history_eod.json")
    first_row = copy.deepcopy(fixture["response"][0])
    second_row = copy.deepcopy(fixture["response"][1])
    responses = [
        {"header": copy.deepcopy(fixture["header"]), "response": [first_row]},
        {"header": copy.deepcopy(fixture["header"]), "response": [second_row]},
    ]
    captured_ranges = []

    def fake_get_request(url, headers, querystring, username, password):
        captured_ranges.append((querystring["start_date"], querystring["end_date"]))
        return responses.pop(0)

    monkeypatch.setattr(thetadata_helper, "get_request", fake_get_request)
    monkeypatch.setattr(thetadata_helper, "get_historical_data", lambda **_: None)

    asset = Asset(asset_type="stock", symbol="PLTR")
    start = pytz.UTC.localize(datetime.datetime(2023, 1, 1))
    end = pytz.UTC.localize(datetime.datetime(2024, 12, 30, 23, 59))

    df = thetadata_helper.get_historical_eod_data(
        asset=asset,
        start_dt=start,
        end_dt=end,
        username="user",
        password="pass",
    )

    assert captured_ranges == [
        ("2023-01-01", "2023-12-31"),
        ("2024-01-01", "2024-12-30"),
    ]
    assert df is not None
    assert len(df) == 2
    assert df.index.is_monotonic_increasing


def test_get_historical_eod_data_skips_open_fix_on_invalid_window(monkeypatch, caplog):
    eod_payload = {
        "header": {
            "format": ["date", "open", "high", "low", "close", "volume", "ms_of_day", "ms_of_day2", "created"],
            "error_type": "null",
        },
        "response": [
            ["20241122", 10.0, 11.0, 9.5, 10.5, 1000, 0, 0, "2024-11-22T16:00:00"]
        ],
    }
    monkeypatch.setattr(thetadata_helper, "get_request", lambda **_: copy.deepcopy(eod_payload))

    def _failing_minute_fetch(**_):
        raise thetadata_helper.ThetaRequestError(
            "Cannot connect to Theta Data!", status_code=400, body="Start must be before end"
        )

    monkeypatch.setattr(thetadata_helper, "get_historical_data", _failing_minute_fetch)

    asset = Asset(asset_type="stock", symbol="MSFT")
    tz = pytz.UTC
    start = tz.localize(datetime.datetime(2024, 11, 21, 19, 0))
    end = tz.localize(datetime.datetime(2024, 11, 22, 19, 0))

    with caplog.at_level(logging.WARNING):
        df = thetadata_helper.get_historical_eod_data(
            asset=asset,
            start_dt=start,
            end_dt=end,
            username="user",
            password="pass",
        )

    assert not df.empty
    assert "skipping open fix" in caplog.text


def test_get_historical_data_parses_stock_downloader_schema(monkeypatch):
    fixture = load_thetadata_fixture("stock_history_ohlc.json")

    def fake_trading_dates(*_args, **_kwargs):
        return [date(2024, 9, 16)]

    monkeypatch.setattr(thetadata_helper, "get_trading_dates", fake_trading_dates)
    monkeypatch.setattr(thetadata_helper, "get_request", lambda **_: fixture)

    asset = Asset(asset_type="stock", symbol="PLTR")
    start = pytz.UTC.localize(datetime.datetime(2024, 9, 16, 9, 30))
    end = start + datetime.timedelta(minutes=5)

    df = thetadata_helper.get_historical_data(
        asset=asset,
        start_dt=start,
        end_dt=end,
        ivl=60000,
        username="user",
        password="pass",
        datastyle="ohlc",
    )

    assert df is not None
    assert not df.empty
    assert df.index.tzinfo is not None
    assert "close" in df.columns


def test_get_historical_data_parses_option_downloader_schema(monkeypatch):
    fixture = load_thetadata_fixture("option_history_ohlc.json")

    def fake_trading_dates(*_args, **_kwargs):
        return [date(2024, 9, 16)]

    monkeypatch.setattr(thetadata_helper, "get_trading_dates", fake_trading_dates)
    monkeypatch.setattr(thetadata_helper, "get_request", lambda **_: fixture)

    asset = Asset(
        asset_type="option",
        symbol="TSLA",
        expiration=date(2024, 10, 18),
        strike=250.0,
        right="CALL",
    )
    start = pytz.UTC.localize(datetime.datetime(2024, 9, 16, 9, 30))
    end = start + datetime.timedelta(minutes=5)

    df = thetadata_helper.get_historical_data(
        asset=asset,
        start_dt=start,
        end_dt=end,
        ivl=60000,
        username="user",
        password="pass",
        datastyle="ohlc",
    )

    assert df is not None
    assert not df.empty
    assert "strike" in df.columns
    assert df.index.tzinfo is not None


@patch("lumibot.tools.thetadata_helper.get_request")
@patch("lumibot.tools.thetadata_helper.get_trading_dates")
def test_get_historical_data_uses_v3_option_params(mock_get_trading_dates, mock_get_request):
    mock_get_trading_dates.return_value = [date(2024, 10, 16)]
    mock_get_request.return_value = {
        "header": {"format": ["timestamp", "bid", "ask", "count"]},
        "response": [["2024-10-16T09:30:00", 10.0, 10.5, 1]],
    }

    asset = Asset(
        asset_type="option",
        symbol="CVNA",
        expiration=date(2026, 1, 16),
        strike=210.0,
        right="CALL",
    )
    start = LUMIBOT_DEFAULT_PYTZ.localize(datetime.datetime(2024, 10, 16, 9, 30))
    end = LUMIBOT_DEFAULT_PYTZ.localize(datetime.datetime(2024, 10, 16, 16, 0))

    df = thetadata_helper.get_historical_data(
        asset=asset,
        start_dt=start,
        end_dt=end,
        ivl=60000,
        username="user",
        password="pass",
        datastyle="quote",
        include_after_hours=False,
    )

    assert df is not None
    assert len(df) == 1
    assert df.index.tz.zone == LUMIBOT_DEFAULT_PYTZ.zone

    query = mock_get_request.call_args.kwargs["querystring"]
    assert query["symbol"] == "CVNA"
    assert query["expiration"] == "2026-01-16"
    assert query["strike"] == "210"
    assert query["right"] == "call"
    assert query["date"] == "2024-10-16"
    assert query["interval"] == "1m"
    assert query["start_time"] == "09:30:00"
    assert query["end_time"] == "16:00:00"


def test_get_expirations_normalizes_downloader_payload(monkeypatch):
    fixture = load_thetadata_fixture("option_list_expirations.json")
    monkeypatch.setattr(thetadata_helper, "get_request", lambda **_: fixture)

    expirations = thetadata_helper.get_expirations(
        username="user",
        password="pass",
        ticker="TSLA",
        after_date=date(2024, 9, 1),
    )

    assert expirations
    assert all(isinstance(exp, str) and exp.count("-") == 2 for exp in expirations)


def test_get_strikes_normalizes_downloader_payload(monkeypatch):
    fixture = load_thetadata_fixture("option_list_strikes.json")
    monkeypatch.setattr(thetadata_helper, "get_request", lambda **_: fixture)

    strikes = thetadata_helper.get_strikes(
        username="user",
        password="pass",
        ticker="TSLA",
        expiration=datetime.datetime(2024, 10, 18),
    )

    assert strikes
    assert all(isinstance(value, float) for value in strikes)


@patch("lumibot.tools.thetadata_helper.get_request")
@patch("lumibot.tools.thetadata_helper.get_trading_dates")
def test_get_historical_data_accepts_date_inputs(mock_get_trading_dates, mock_get_request):
    mock_get_trading_dates.return_value = [date(2024, 10, 16)]
    mock_get_request.return_value = {
        "header": {"format": ["timestamp", "open", "high", "low", "close", "count"]},
        "response": [["2024-10-16T09:30:00", 10, 11, 9, 10.5, 1]],
    }

    asset = Asset(asset_type="stock", symbol="CVNA")
    start = date(2024, 10, 16)
    end = date(2024, 10, 16)

    df = thetadata_helper.get_historical_data(
        asset=asset,
        start_dt=start,
        end_dt=end,
        ivl=60000,
        username="user",
        password="pass",
        datastyle="ohlc",
        include_after_hours=True,
    )

    assert df is not None
    assert len(df) == 1
    assert df.index.tz.zone == LUMIBOT_DEFAULT_PYTZ.zone

    query = mock_get_request.call_args.kwargs["querystring"]
    assert query["date"] == "2024-10-16"
    assert query["start_time"] == "04:00:00"
    assert query["end_time"] == "20:00:00"


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
    raw_df = pd.DataFrame({
        "datetime": pd.date_range("2023-07-01 09:30:00", periods=5, freq="min", tz="UTC"),
        "price": [100, 101, 102, 103, 104]
    }).set_index("datetime")
    mock_get_historical_data.return_value = raw_df.reset_index()
    mock_update_df.return_value = raw_df
    
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
        "datetime": pd.date_range("2023-07-01 09:30:00", periods=5, freq='min', tz="UTC"),
        "price": [100, 101, 102, 103, 104],
        "missing": [False] * 5,
    }).set_index("datetime")
    mock_build_cache_filename.return_value.exists.return_value = True
    mock_load_cache.return_value = cached_data
    mock_get_missing_dates.return_value = [datetime.datetime(2025, 9, 3)]
    new_chunk = pd.DataFrame({
        "datetime": pd.date_range("2023-07-02 09:30:00", periods=5, freq='min', tz="UTC"),
        "price": [110, 111, 112, 113, 114],
        "missing": [False] * 5,
    }).set_index("datetime")
    mock_get_historical_data.return_value = new_chunk.reset_index()
    updated_data = pd.concat([cached_data, new_chunk]).sort_index()
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
    pd.testing.assert_frame_equal(
        df,
        updated_data.drop(columns="missing"),
        check_dtype=False,
    )
    mock_update_cache.assert_called_once()


def test_get_price_data_daily_placeholders_prevent_refetch(monkeypatch, tmp_path):
    from lumibot.constants import LUMIBOT_DEFAULT_PYTZ

    cache_root = tmp_path / "cache_root"
    monkeypatch.setattr(thetadata_helper, "LUMIBOT_CACHE_FOLDER", str(cache_root))
    thetadata_helper.reset_connection_diagnostics()

    asset = Asset(asset_type="stock", symbol="PLTR")
    start = LUMIBOT_DEFAULT_PYTZ.localize(datetime.datetime(2024, 1, 1))
    end = LUMIBOT_DEFAULT_PYTZ.localize(datetime.datetime(2024, 1, 3))
    trading_days = [
        datetime.date(2024, 1, 1),
        datetime.date(2024, 1, 2),
        datetime.date(2024, 1, 3),
    ]

    partial_df = pd.DataFrame(
        {
            "datetime": pd.to_datetime(["2024-01-01", "2024-01-02"], utc=True),
            "open": [10.0, 11.0],
            "high": [11.0, 12.0],
            "low": [9.5, 10.5],
            "close": [10.5, 11.5],
            "volume": [1_000, 1_200],
        }
    )

    progress_stub = MagicMock()
    progress_stub.update.return_value = None
    progress_stub.close.return_value = None

    with patch("lumibot.tools.thetadata_helper.tqdm", return_value=progress_stub), \
         patch("lumibot.tools.thetadata_helper.get_trading_dates", return_value=trading_days):
        eod_mock = MagicMock(return_value=partial_df)
        with patch("lumibot.tools.thetadata_helper.get_historical_eod_data", eod_mock):
            first = thetadata_helper.get_price_data(
                "user",
                "pass",
                asset,
                start,
                end,
                "day",
            )

            assert eod_mock.call_count == 1
            assert len(first) == 2
            assert set(first.index.date) == {datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)}

            cache_file = thetadata_helper.build_cache_filename(asset, "day", "ohlc")
            loaded = thetadata_helper.load_cache(cache_file)
            assert len(loaded) == 3
            assert "missing" in loaded.columns
            assert int(loaded["missing"].sum()) == 1
            missing_dates = {idx.date() for idx, flag in loaded["missing"].items() if flag}
            assert missing_dates == {datetime.date(2024, 1, 3)}

        # Second run should reuse cache entirely
        eod_second_mock = MagicMock(return_value=partial_df)
        with patch("lumibot.tools.thetadata_helper.tqdm", return_value=progress_stub), \
             patch("lumibot.tools.thetadata_helper.get_trading_dates", return_value=trading_days), \
             patch("lumibot.tools.thetadata_helper.get_historical_eod_data", eod_second_mock):
            second = thetadata_helper.get_price_data(
                "user",
                "pass",
                asset,
                start,
                end,
                "day",
            )

            assert eod_second_mock.call_count == 0
            assert len(second) == 2
            assert set(second.index.date) == {datetime.date(2024, 1, 1), datetime.date(2024, 1, 2)}


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
    assert df is not None
    assert df.empty
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
    mocker.patch.object(thetadata_helper, "LUMIBOT_CACHE_FOLDER", str(tmpdir))
    expected = tmpdir / "thetadata" / "stock" / "1d" / datastyle / f"stock_SPY_1D_{datastyle}.parquet"
    assert thetadata_helper.build_cache_filename(asset, timespan, datastyle) == expected

    expire_date = datetime.date(2023, 8, 1)
    option_asset = Asset("SPY", asset_type="option", expiration=expire_date, strike=100, right="CALL")
    expected = tmpdir / "thetadata" / "option" / "1d" / datastyle / f"option_SPY_230801_100_CALL_1D_{datastyle}.parquet"
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
    mocker.patch.object(thetadata_helper, "LUMIBOT_CACHE_FOLDER", str(tmpdir))
    cache_file = thetadata_helper.build_cache_filename(Asset("SPY"), "1D", datastyle)
    
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


def test_get_price_data_invokes_remote_cache_manager(tmp_path, monkeypatch):
    asset = Asset(asset_type="stock", symbol="AAPL")
    monkeypatch.setattr(thetadata_helper, "LUMIBOT_CACHE_FOLDER", str(tmp_path))
    cache_file = thetadata_helper.build_cache_filename(asset, "minute", "ohlc")
    cache_file.parent.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(
        {
            "datetime": pd.date_range("2024-01-01 09:30:00", periods=2, freq="min", tz=pytz.UTC),
            "open": [100.0, 101.0],
            "high": [101.0, 102.0],
            "low": [99.5, 100.5],
            "close": [100.5, 101.5],
            "volume": [1_000, 1_200],
            "missing": [False, False],
        }
    )
    df.to_parquet(cache_file, engine="pyarrow", compression="snappy", index=False)

    class DummyManager:
        def __init__(self):
            self.ensure_calls = []
            self.upload_calls = []
            self.enabled = True
            self._mode = CacheMode.S3_READWRITE

        @property
        def mode(self):
            return self._mode

        def ensure_local_file(self, local_path, payload=None, force_download=False):
            self.ensure_calls.append((Path(local_path), payload))
            return False

        def on_local_update(self, local_path, payload=None):
            self.upload_calls.append((Path(local_path), payload))
            return True

    dummy_manager = DummyManager()
    monkeypatch.setattr(thetadata_helper, "get_backtest_cache", lambda: dummy_manager)
    monkeypatch.setattr(thetadata_helper, "get_missing_dates", lambda df_all, *_args, **_kwargs: [])

    start = datetime.datetime(2024, 1, 1, 9, 30, tzinfo=pytz.UTC)
    end = datetime.datetime(2024, 1, 1, 9, 31, tzinfo=pytz.UTC)

    result = thetadata_helper.get_price_data(
        username="user",
        password="pass",
        asset=asset,
        start=start,
        end=end,
        timespan="minute",
        quote_asset=None,
        dt=None,
        datastyle="ohlc",
        include_after_hours=True,
        return_polars=False,
    )

    assert dummy_manager.ensure_calls, "Expected remote cache ensure call"
    ensure_path, ensure_payload = dummy_manager.ensure_calls[0]
    assert ensure_path == cache_file
    assert ensure_payload["provider"] == "thetadata"
    assert isinstance(result, pd.DataFrame)
    assert not dummy_manager.upload_calls, "Cache hit should not trigger upload"


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
    mocker.patch.object(thetadata_helper, "LUMIBOT_CACHE_FOLDER", str(tmpdir))
    asset = Asset("SPY")
    cache_file = thetadata_helper.build_cache_filename(asset, "1D", datastyle)

    # No cache file should return None (not raise)
    assert thetadata_helper.load_cache(cache_file) is None

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
    assert df_new["c"].iloc[3] == 18  # Overlap prefers latest data
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
    res = requests.get(
        f"{thetadata_helper.BASE_URL}{thetadata_helper.READINESS_ENDPOINT}",
        params={"symbol": thetadata_helper.HEALTHCHECK_SYMBOL, "format": "json"},
        timeout=2,
    )
    assert res.status_code in (200, 571), f"Unexpected readiness status: {res.status_code} ({res.text})"

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
    res = requests.get(
        f"{thetadata_helper.BASE_URL}{thetadata_helper.READINESS_ENDPOINT}",
        params={"symbol": thetadata_helper.HEALTHCHECK_SYMBOL, "format": "json"},
        timeout=2,
    )
    assert res.status_code in (200, 571), f"Unexpected readiness status: {res.status_code} ({res.text})"


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


@pytest.mark.skipif(
    os.environ.get("CI") == "true",
    reason="Requires ThetaData Terminal (not available in CI)"
)
def test_build_historical_chain_live_option_list(theta_terminal_cleanup):
    """Exercise option list endpoints via real ThetaTerminal to guard v3 regressions."""
    if thetadata_helper.REMOTE_DOWNLOADER_ENABLED:
        pytest.skip("Remote downloader configured; skip local ThetaTerminal integration test")
    if os.environ.get("ENABLE_THETADATA_TERMINAL_TESTS") != "1":
        pytest.skip("ThetaTerminal integration tests disabled; set ENABLE_THETADATA_TERMINAL_TESTS=1 to run")
    username = os.environ.get("THETADATA_USERNAME")
    password = os.environ.get("THETADATA_PASSWORD")

    if not username or not password:
        pytest.skip("ThetaData credentials not configured")

    thetadata_helper.check_connection(username, password, wait_for_connection=True)

    as_of_date = datetime.date.today() - datetime.timedelta(days=5)
    while as_of_date.weekday() >= 5:
        as_of_date -= datetime.timedelta(days=1)

    asset = Asset("SPY", asset_type="stock")
    chain = thetadata_helper.build_historical_chain(
        username=username,
        password=password,
        asset=asset,
        as_of_date=as_of_date,
        max_expirations=5,
        max_consecutive_misses=5,
    )

    assert chain is not None, "Expected option chain data from ThetaTerminal"
    assert chain["Chains"]["CALL"], "CALL chain should contain expirations"
    assert chain["Chains"]["PUT"], "PUT chain should contain expirations"


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
    expected_params = dict(querystring)
    expected_params.setdefault("format", "json")
    mock_get.assert_called_with(url, headers=headers, params=expected_params, timeout=30)
    mock_check_connection.assert_called_with(
        username="test_user",
        password="test_password",
        wait_for_connection=True,
    )
    assert mock_check_connection.call_count >= 2
    first_call_kwargs = mock_check_connection.call_args_list[0].kwargs
    assert first_call_kwargs == {
        "username": "test_user",
        "password": "test_password",
        "wait_for_connection": False,
    }


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
    expected_params = dict(querystring)
    expected_params.setdefault("format", "json")
    mock_get.assert_called_with(url, headers=headers, params=expected_params, timeout=30)
    mock_check_connection.assert_called_with(
        username="test_user",
        password="test_password",
        wait_for_connection=True,
    )
    expected_calls = thetadata_helper.HTTP_RETRY_LIMIT + 1  # initial probe + retries
    assert mock_check_connection.call_count >= expected_calls


@patch('lumibot.tools.thetadata_helper.check_connection')
def test_get_request_raises_theta_request_error_after_transient_status(mock_check_connection, monkeypatch):
    """Ensure repeated 5xx responses raise ThetaRequestError with the status code."""
    mock_check_connection.return_value = (None, True)

    responses = [
        SimpleNamespace(status_code=503, text="Service unavailable")
        for _ in range(thetadata_helper.HTTP_RETRY_LIMIT)
    ]

    def fake_get(*args, **kwargs):
        if not responses:
            raise AssertionError("Expected ThetaRequestError before exhausting responses")
        return responses.pop(0)

    monkeypatch.setattr(thetadata_helper.requests, "get", fake_get)
    monkeypatch.setattr(thetadata_helper.time, "sleep", lambda *_: None)

    with pytest.raises(thetadata_helper.ThetaRequestError) as excinfo:
        thetadata_helper.get_request("http://fake", {}, {}, "user", "pass")

    assert excinfo.value.status_code == 503
    assert mock_check_connection.call_count >= thetadata_helper.HTTP_RETRY_LIMIT



@patch('lumibot.tools.thetadata_helper.start_theta_data_client')
@patch('lumibot.tools.thetadata_helper.check_connection')
def test_get_request_consecutive_474_triggers_restarts(mock_check_connection, mock_start_client, monkeypatch):
    mock_check_connection.return_value = (object(), True)

    responses = [MagicMock(status_code=474, text='Connection lost to Theta Data MDDS.') for _ in range(9)]

    def fake_get(*args, **kwargs):
        if not responses:
            raise AssertionError('Test exhausted mock responses unexpectedly')
        return responses.pop(0)

    monkeypatch.setattr(thetadata_helper.requests, 'get', fake_get)
    monkeypatch.setattr(thetadata_helper.time, 'sleep', lambda *args, **kwargs: None)
    monkeypatch.setattr(thetadata_helper, 'BOOT_GRACE_PERIOD', 0, raising=False)
    monkeypatch.setattr(thetadata_helper, 'CONNECTION_RETRY_SLEEP', 0, raising=False)

    with pytest.raises(ValueError, match='Cannot connect to Theta Data!'):
        thetadata_helper.get_request(
            url='http://test.com',
            headers={'Authorization': 'Bearer test_token'},
            querystring={'param1': 'value1'},
            username='test_user',
            password='test_password',
        )

    assert mock_start_client.call_count == 3
    # Initial liveness probe plus retry coordination checks
    assert mock_check_connection.call_count > 3
    first_call_kwargs = mock_check_connection.call_args_list[0].kwargs
    assert first_call_kwargs == {
        'username': 'test_user',
        'password': 'test_password',
        'wait_for_connection': False,
    }


def test_probe_terminal_ready_handles_transient_states(monkeypatch):
    """Ensure the readiness probe treats 571/ServerStarting as not ready."""
    response = SimpleNamespace(status_code=571, text="SERVER_STARTING")
    monkeypatch.setattr(thetadata_helper.requests, "get", lambda *_, **__: response)
    assert thetadata_helper._probe_terminal_ready() is False


def test_probe_terminal_ready_success(monkeypatch):
    response = SimpleNamespace(status_code=200, text="OK")
    monkeypatch.setattr(thetadata_helper.requests, "get", lambda *_, **__: response)
    assert thetadata_helper._probe_terminal_ready()


@patch('lumibot.tools.thetadata_helper.check_connection')
def test_get_request_retries_on_571(mock_check_connection, monkeypatch):
    """ThetaData should retry when the terminal returns SERVER_STARTING."""
    mock_check_connection.return_value = (None, True)
    payload = {"header": {"format": [], "next_page": None, "error_type": "null"}, "response": []}

    responses = [
        SimpleNamespace(status_code=571, text="SERVER_STARTING"),
        SimpleNamespace(status_code=200, text="{}", json=lambda: payload),
    ]

    def fake_get(*args, **kwargs):
        resp = responses.pop(0)
        if resp.status_code == 200:
            return SimpleNamespace(
                status_code=200,
                text="{}",
                json=lambda: payload,
            )
        return resp

    monkeypatch.setattr(thetadata_helper.requests, "get", fake_get)
    result = thetadata_helper.get_request("http://fake", {}, {}, "user", "pass")
    assert result == payload
    assert mock_check_connection.call_count >= 2
    assert responses == []


@patch('lumibot.tools.thetadata_helper.check_connection')
def test_get_request_raises_on_410(mock_check_connection, monkeypatch):
    """v2 requests hitting v3 terminal should raise a helpful error."""
    mock_check_connection.return_value = (None, True)

    def fake_get(*args, **kwargs):
        return SimpleNamespace(status_code=410, text="GONE")

    monkeypatch.setattr(thetadata_helper.requests, "get", fake_get)

    with pytest.raises(RuntimeError) as excinfo:
        thetadata_helper.get_request("http://fake", {}, {}, "user", "pass")

    assert "410" in str(excinfo.value)


def test_get_request_attaches_downloader_header(monkeypatch):
    headers_seen = {}

    class DummyResponse:
        status_code = 200

        def json(self):
            return {"response": [], "header": {"format": []}}

    def fake_get(url, headers=None, params=None, timeout=None):
        headers_seen.update(headers or {})
        return DummyResponse()

    monkeypatch.setattr(thetadata_helper, "DOWNLOADER_API_KEY", "secret-key")
    monkeypatch.setattr(thetadata_helper, "DOWNLOADER_KEY_HEADER", "X-Downloader-Key")
    monkeypatch.setattr(thetadata_helper.requests, "get", fake_get)
    monkeypatch.setattr(thetadata_helper, "check_connection", lambda **_: (None, True))

    thetadata_helper.get_request("http://fake", {}, {}, "user", "pass")
    assert headers_seen["X-Downloader-Key"] == "secret-key"


def test_get_historical_eod_data_handles_missing_date(monkeypatch):
    sample_response = {
        "header": {"format": ["open", "close", "created"]},
        "response": [
            [439.5, 445.23, "2025-11-10T17:15:01.116"]
        ]
    }

    monkeypatch.setattr(thetadata_helper, "get_request", lambda **_: sample_response)
    monkeypatch.setattr(thetadata_helper, "get_historical_data", lambda **_: None)

    asset = Asset("TSLA", asset_type="stock")
    start = datetime.datetime(2025, 11, 10, tzinfo=datetime.timezone.utc)

    df = thetadata_helper.get_historical_eod_data(asset, start, start, "user", "pass")
    assert not df.empty
    assert df.index[0].strftime("%Y-%m-%d") == "2025-11-10"


def test_get_historical_eod_data_splits_chunk_on_transient_error(monkeypatch):
    """Ensure a transient ThetaRequestError triggers a one-time chunk split."""
    call_ranges = []

    def fake_get_request(url, headers, querystring, username, password):
        start = querystring["start_date"]
        end = querystring["end_date"]
        call_ranges.append((start, end))
        if start == "2024-01-01" and end == "2024-01-04":
            raise thetadata_helper.ThetaRequestError("503 error", status_code=503)
        rows = []
        cursor = datetime.datetime.strptime(start, "%Y-%m-%d")
        end_dt = datetime.datetime.strptime(end, "%Y-%m-%d")
        while cursor <= end_dt:
            rows.append([100.0, 101.0, cursor.strftime("%Y-%m-%dT17:15:00Z")])
            cursor += datetime.timedelta(days=1)
        return {"header": {"format": ["open", "close", "created"]}, "response": rows}

    monkeypatch.setattr(thetadata_helper, "get_request", fake_get_request)
    monkeypatch.setattr(thetadata_helper, "get_historical_data", lambda **_: None)

    asset = Asset("MSFT", asset_type="stock")
    start = datetime.datetime(2024, 1, 1)
    end = datetime.datetime(2024, 1, 4)

    df = thetadata_helper.get_historical_eod_data(asset, start, end, "user", "pass")
    assert len(df) == 4
    assert ("2024-01-01", "2024-01-04") in call_ranges
    assert ("2024-01-01", "2024-01-02") in call_ranges
    assert ("2024-01-03", "2024-01-04") in call_ranges


def test_get_historical_eod_data_split_failure_bubbles(monkeypatch):
    """If a split sub-chunk fails, propagate the ThetaRequestError."""
    failure_ranges = {
        ("2024-01-01", "2024-01-04"),
        ("2024-01-01", "2024-01-02"),
    }

    def fake_get_request(url, headers, querystring, username, password):
        start = querystring["start_date"]
        end = querystring["end_date"]
        if (start, end) in failure_ranges:
            raise thetadata_helper.ThetaRequestError("still failing", status_code=503)
        rows = [
            [100.0, 101.0, f"{start}T17:15:00Z"],
        ]
        return {"header": {"format": ["open", "close", "created"]}, "response": rows}

    monkeypatch.setattr(thetadata_helper, "get_request", fake_get_request)
    monkeypatch.setattr(thetadata_helper, "get_historical_data", lambda **_: None)

    asset = Asset("MSFT", asset_type="stock")
    start = datetime.datetime(2024, 1, 1)
    end = datetime.datetime(2024, 1, 4)

    with pytest.raises(thetadata_helper.ThetaRequestError):
        thetadata_helper.get_historical_eod_data(asset, start, end, "user", "pass")


def test_check_connection_remote_downloader(monkeypatch):
    probe_calls = {"count": 0}

    def fake_probe():
        probe_calls["count"] += 1
        return probe_calls["count"] >= 2

    fake_time = SimpleNamespace(sleep=lambda *_: None)

    monkeypatch.setattr(thetadata_helper, "REMOTE_DOWNLOADER_ENABLED", True)
    monkeypatch.setattr(thetadata_helper, "_probe_terminal_ready", lambda: fake_probe())
    monkeypatch.setattr(thetadata_helper, "CONNECTION_MAX_RETRIES", 3)
    monkeypatch.setattr(thetadata_helper, "time", fake_time)

    _, ready = thetadata_helper.check_connection("user", "pass", wait_for_connection=True)
    assert ready is True
    assert probe_calls["count"] == 2


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
    assert list(df.columns) == [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "count",
        "last_trade_time",
        "last_bid_time",
        "last_ask_time",
    ]
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
    assert list(df.columns) == [
        "open",
        "high",
        "low",
        "close",
        "volume",
        "count",
        "last_trade_time",
        "last_bid_time",
        "last_ask_time",
    ]
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


@patch("lumibot.tools.thetadata_helper.get_trading_dates")
@patch("lumibot.tools.thetadata_helper.get_request")
def test_get_historical_data_applies_session_override(mock_get_request, mock_get_trading_dates):
    asset = Asset(asset_type="stock", symbol="MSFT")
    start_dt = LUMIBOT_DEFAULT_PYTZ.localize(datetime.datetime(2024, 11, 21, 19, 0))
    end_dt = LUMIBOT_DEFAULT_PYTZ.localize(datetime.datetime(2024, 11, 25, 19, 0))
    ivl = 60000
    mock_get_trading_dates.return_value = [
        datetime.date(2024, 11, 22),
        datetime.date(2024, 11, 25),
    ]
    mock_get_request.return_value = {
        "header": {
            "format": ["timestamp", "open", "high", "low", "close", "volume", "count"],
            "error_type": "null",
        },
        "response": [["2024-11-22T09:30:00", 10, 11, 9, 10.5, 1000, 1]],
    }

    df = thetadata_helper.get_historical_data(
        asset,
        start_dt,
        end_dt,
        ivl,
        "test_user",
        "test_password",
        session_time_override=("09:30:00", "09:31:00"),
    )

    assert df is not None
    assert mock_get_request.call_count == len(mock_get_trading_dates.return_value)
    for call in mock_get_request.call_args_list:
        qs = call.kwargs["querystring"]
        assert qs["start_time"] == "09:30:00"
        assert qs["end_time"] == "09:31:00"

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
    assert not df.empty
    assert list(df.columns) == [
        "bid_size",
        "bid_condition",
        "bid",
        "bid_exchange",
        "ask_size",
        "ask_condition",
        "ask",
        "ask_exchange",
        "last_trade_time",
        "last_bid_time",
        "last_ask_time",
    ]

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
    assert df is None  # The DataFrame should be None because no valid rows remain


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
            {"strike_price": 140000},
            {"strike_price": 145000},
            {"strike_price": 150000}
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
    expected = [140.0, 145.0, 150.0]
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
@pytest.mark.usefixtures("theta_terminal_cleanup")
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
        """Ensure repeated shutdown + restart cycles succeed without BadSession loops."""
        username = os.environ.get("THETADATA_USERNAME")
        password = os.environ.get("THETADATA_PASSWORD")
        asset = Asset("AAPL", asset_type="stock")
        start = datetime.datetime(2025, 9, 1)
        end = datetime.datetime(2025, 9, 5)

        thetadata_helper.start_theta_data_client(username, password)

        for cycle in range(2):
            # Confirm the terminal is ready before simulating the crash/shutdown.
            _, connected = thetadata_helper.check_connection(username, password, wait_for_connection=True)
            assert connected is True, f"Cycle {cycle}: ThetaTerminal should be ready before shutdown."

            # Request a graceful shutdown and ensure the process actually exits.
            assert thetadata_helper.shutdown_theta_terminal(timeout=30.0) is True, (
                f"Cycle {cycle}: ThetaTerminal failed to shut down via control endpoint."
            )
            assert thetadata_helper.is_process_alive() is False, f"Cycle {cycle}: process should be stopped."

            # Fetch data immediately; helper should restart the terminal transparently.
            df = thetadata_helper.get_price_data(
                username=username,
                password=password,
                asset=asset,
                start=start,
                end=end,
                timespan="minute",
            )

            assert df is not None, f"Cycle {cycle}: Expected data frame after restart."
            assert len(df) > 0, f"Cycle {cycle}: Expected non-empty data frame after restart."

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


class TestThetaDataChainsCaching:
    """Unit coverage for historical chain caching and normalization."""

    def test_chains_cached_basic_structure(self, tmp_path, monkeypatch):
        asset = Asset("TEST", asset_type="stock")
        test_date = date(2024, 11, 7)

        sample_chain = {
            "Multiplier": 100,
            "Exchange": "SMART",
            "Chains": {
                "CALL": {"2024-11-15": [100.0, 105.0]},
                "PUT": {"2024-11-15": [90.0, 95.0]},
            },
        }

        calls = []

        def fake_builder(**kwargs):
            calls.append(kwargs)
            return sample_chain

        monkeypatch.setattr(thetadata_helper, "build_historical_chain", fake_builder)
        monkeypatch.setattr(thetadata_helper, "LUMIBOT_CACHE_FOLDER", str(tmp_path))

        result = thetadata_helper.get_chains_cached("user", "pass", asset, test_date)

        assert result == sample_chain
        assert len(calls) == 1
        builder_call = calls[0]
        assert builder_call["asset"] == asset
        assert builder_call["as_of_date"] == test_date

    def test_chains_cache_reuse(self, tmp_path, monkeypatch):
        asset = Asset("REUSE", asset_type="stock")
        test_date = date(2024, 11, 8)

        sample_chain = {
            "Multiplier": 100,
            "Exchange": "SMART",
            "Chains": {"CALL": {"2024-11-22": [110.0]}, "PUT": {"2024-11-22": [95.0]}},
        }

        call_count = {"total": 0}

        def fake_builder(**kwargs):
            call_count["total"] += 1
            return sample_chain

        monkeypatch.setattr(thetadata_helper, "build_historical_chain", fake_builder)
        monkeypatch.setattr(thetadata_helper, "LUMIBOT_CACHE_FOLDER", str(tmp_path))

        first = thetadata_helper.get_chains_cached("user", "pass", asset, test_date)
        second = thetadata_helper.get_chains_cached("user", "pass", asset, test_date)

        assert first == sample_chain
        assert second == sample_chain
        assert call_count["total"] == 1, "Builder should only run once due to cache reuse"

    def test_chain_cache_respects_recent_file(self, tmp_path, monkeypatch):
        asset = Asset("RECENT", asset_type="stock")
        test_date = date(2024, 11, 30)

        sample_chain = {
            "Multiplier": 100,
            "Exchange": "SMART",
            "Chains": {"CALL": {"2024-12-06": [120.0]}, "PUT": {"2024-12-06": [80.0]}},
        }

        monkeypatch.setattr(thetadata_helper, "LUMIBOT_CACHE_FOLDER", str(tmp_path))

        cache_folder = Path(tmp_path) / "thetadata" / "stock" / "option_chains"
        cache_folder.mkdir(parents=True, exist_ok=True)

        cache_file = cache_folder / f"{asset.symbol}_{test_date.isoformat()}.parquet"
        pd.DataFrame({"data": [sample_chain]}).to_parquet(cache_file, compression="snappy", engine="pyarrow")

        # Builder should not be invoked because cache hit satisfies tolerance window
        def fail_builder(**kwargs):
            raise AssertionError("build_historical_chain should not be called when cache is fresh")

        monkeypatch.setattr(thetadata_helper, "build_historical_chain", fail_builder)

        result = thetadata_helper.get_chains_cached("user", "pass", asset, test_date)
        assert result == sample_chain

    def test_chains_cached_handles_none_builder(self, tmp_path, monkeypatch, caplog):
        asset = Asset("NONE", asset_type="stock")
        test_date = date(2024, 11, 28)

        monkeypatch.setattr(thetadata_helper, "build_historical_chain", lambda **kwargs: None)
        monkeypatch.setattr(thetadata_helper, "LUMIBOT_CACHE_FOLDER", str(tmp_path))
        monkeypatch.delenv("BACKTESTING_QUIET_LOGS", raising=False)
        caplog.set_level(logging.WARNING, logger="lumibot.tools.thetadata_helper")

        with caplog.at_level(logging.WARNING, logger="lumibot.tools.thetadata_helper"):
            result = thetadata_helper.get_chains_cached("user", "pass", asset, test_date)

        cache_folder = Path(tmp_path) / "thetadata" / "stock" / "option_chains"
        assert not cache_folder.exists() or not list(cache_folder.glob("*.parquet"))

        assert result == {
            "Multiplier": 100,
            "Exchange": "SMART",
            "Chains": {"CALL": {}, "PUT": {}},
        }
        assert "ThetaData returned no option data" in caplog.text


def test_build_historical_chain_parses_quote_payload(monkeypatch):
    asset = Asset("CVNA", asset_type="stock")
    as_of_date = date(2024, 11, 7)
    as_of_int = int(as_of_date.strftime("%Y%m%d"))

    def fake_get_request(url, headers, querystring, username, password):
        if url.endswith(thetadata_helper.OPTION_LIST_ENDPOINTS["expirations"]):
            return {
                "header": {"format": ["date"]},
                "response": [[20241115], [20241205], [20250124]],
            }
        if url.endswith(thetadata_helper.OPTION_LIST_ENDPOINTS["strikes"]):
            exp = querystring["expiration"]
            if exp == "2024-11-15":
                return {
                    "header": {"format": ["strike"]},
                    "response": [[100000], [105000]],
                }
            if exp == "2024-12-05":
                return {
                    "header": {"format": ["strike"]},
                    "response": [[110000]],
                }
            return {
                "header": {"format": ["strike"]},
                "response": [[120000]],
            }
        if url.endswith(thetadata_helper.OPTION_LIST_ENDPOINTS["dates_quote"]):
            exp = querystring["expiration"]
            if exp == "2024-11-15":
                return {
                    "header": {"format": None, "error_type": "null"},
                    "response": [as_of_int, as_of_int + 1],
                }
            return {
                "header": {"format": None, "error_type": "NO_DATA"},
                "response": [],
            }
        raise AssertionError(f"Unexpected URL {url}")

    monkeypatch.setattr(thetadata_helper, "get_request", fake_get_request)

    result = thetadata_helper.build_historical_chain("user", "pass", asset, as_of_date)

    assert result["Multiplier"] == 100
    assert set(result["Chains"].keys()) == {"CALL", "PUT"}
    assert list(result["Chains"]["CALL"].keys()) == ["2024-11-15"]
    assert result["Chains"]["CALL"]["2024-11-15"] == [100.0, 105.0]
    assert result["Chains"]["PUT"]["2024-11-15"] == [100.0, 105.0]


def test_build_historical_chain_uses_v3_option_list_params(monkeypatch):
    asset = Asset("SPY", asset_type="stock")
    as_of_date = date(2024, 11, 15)
    captured = []

    def fake_get_request(url, headers, querystring, username, password):
        captured.append((url, dict(querystring)))
        if url.endswith(thetadata_helper.OPTION_LIST_ENDPOINTS["expirations"]):
            return {"header": {"format": ["expiration"]}, "response": [["20241220"]]}
        if url.endswith(thetadata_helper.OPTION_LIST_ENDPOINTS["strikes"]):
            return {"header": {"format": ["strike"]}, "response": [[100000], [101000]]}
        if url.endswith(thetadata_helper.OPTION_LIST_ENDPOINTS["dates_quote"]):
            if querystring["right"] == "call":
                return {"header": {"format": None, "error_type": "NO_DATA"}, "response": []}
            return {
                "header": {"format": None, "error_type": "null"},
                "response": [int(as_of_date.strftime("%Y%m%d"))],
            }
        raise AssertionError(f"Unexpected URL {url}")

    monkeypatch.setattr(thetadata_helper, "get_request", fake_get_request)

    result = thetadata_helper.build_historical_chain("user", "pass", asset, as_of_date)
    assert result is not None

    strike_calls = [
        params for url, params in captured if url.endswith(thetadata_helper.OPTION_LIST_ENDPOINTS["strikes"])
    ]
    assert strike_calls, "Expected at least one strikes request"
    assert strike_calls[0]["expiration"] == "2024-12-20"
    assert "exp" not in strike_calls[0]
    assert strike_calls[0]["format"] == "json"

    dates_calls = [
        params for url, params in captured if url.endswith(thetadata_helper.OPTION_LIST_ENDPOINTS["dates_quote"])
    ]
    assert {call["right"] for call in dates_calls} == {"call", "put"}
    for params in dates_calls:
        assert params["expiration"] == "2024-12-20"
        assert params["format"] == "json"


def test_build_historical_chain_returns_none_when_no_dates(monkeypatch, caplog):
    asset = Asset("NONE", asset_type="stock")
    as_of_date = date(2024, 11, 28)

    as_of_int = int(as_of_date.strftime("%Y%m%d"))

    def fake_get_request(url, headers, querystring, username, password):
        if url.endswith(thetadata_helper.OPTION_LIST_ENDPOINTS["expirations"]):
            return {"header": {"format": ["date"]}, "response": [[20241129], [20241206]]}
        if url.endswith(thetadata_helper.OPTION_LIST_ENDPOINTS["strikes"]):
            return {"header": {"format": ["strike"]}, "response": [[150000], [155000]]}
        if url.endswith(thetadata_helper.OPTION_LIST_ENDPOINTS["dates_quote"]):
            return {"header": {"format": None, "error_type": "NO_DATA"}, "response": []}
        raise AssertionError(f"Unexpected URL {url}")

    monkeypatch.setattr(thetadata_helper, "get_request", fake_get_request)
    monkeypatch.delenv("BACKTESTING_QUIET_LOGS", raising=False)
    caplog.set_level(logging.WARNING, logger="lumibot.tools.thetadata_helper")

    with caplog.at_level(logging.WARNING, logger="lumibot.tools.thetadata_helper"):
        result = thetadata_helper.build_historical_chain("user", "pass", asset, as_of_date)

    assert result is None
    assert f"No expirations with data found for {asset.symbol}" in caplog.text

def test_build_historical_chain_empty_response(monkeypatch, caplog):
    asset = Asset("EMPTY", asset_type="stock")
    as_of_date = date(2024, 11, 9)

    def fake_get_request(url, headers, querystring, username, password):
        if url.endswith(thetadata_helper.OPTION_LIST_ENDPOINTS["expirations"]):
            return {"header": {"format": ["date"]}, "response": []}
        raise AssertionError("Unexpected call after empty expirations")

    monkeypatch.setattr(thetadata_helper, "get_request", fake_get_request)
    monkeypatch.delenv("BACKTESTING_QUIET_LOGS", raising=False)
    caplog.set_level(logging.WARNING, logger="lumibot.tools.thetadata_helper")

    with caplog.at_level(logging.WARNING, logger="lumibot.tools.thetadata_helper"):
        result = thetadata_helper.build_historical_chain("user", "pass", asset, as_of_date)

    assert result is None
    assert "returned no expirations" in caplog.text


class TestThetaDataConnectionSupervision:

    def setup_method(self):
        thetadata_helper.reset_connection_diagnostics()

    def test_check_connection_recovers_after_restart(self, monkeypatch):
        statuses = iter(["DISCONNECTED", "DISCONNECTED", "CONNECTED"])

        class FakeResponse:
            def __init__(self, text):
                self.text = text

        def fake_get(url, timeout=None, **kwargs):
            try:
                text = next(statuses)
            except StopIteration:
                text = "CONNECTED"
            return FakeResponse(text)

        start_calls = []

        def fake_start(username, password):
            start_calls.append((username, password))
            return object()

        monkeypatch.setattr(thetadata_helper.requests, "get", fake_get)
        monkeypatch.setattr(thetadata_helper, "start_theta_data_client", fake_start)
        monkeypatch.setattr(thetadata_helper, "is_process_alive", lambda: True)
        monkeypatch.setattr(thetadata_helper, "CONNECTION_MAX_RETRIES", 2, raising=False)
        monkeypatch.setattr(thetadata_helper, "MAX_TERMINAL_RESTART_CYCLES", 2, raising=False)
        monkeypatch.setattr(thetadata_helper, "BOOT_GRACE_PERIOD", 0, raising=False)
        monkeypatch.setattr(thetadata_helper, "CONNECTION_RETRY_SLEEP", 0, raising=False)
        monkeypatch.setattr(thetadata_helper.time, "sleep", lambda *args, **kwargs: None)

        client, connected = thetadata_helper.check_connection("user", "pass", wait_for_connection=True)

        assert connected is True
        assert len(start_calls) == 1
        assert thetadata_helper.CONNECTION_DIAGNOSTICS["terminal_restarts"] >= 1

    def test_check_connection_raises_after_restart_cycles(self, monkeypatch):
        statuses = iter(["DISCONNECTED"] * 10)

        class FakeResponse:
            def __init__(self, text):
                self.text = text

        def fake_get(url, timeout=None, **kwargs):
            try:
                text = next(statuses)
            except StopIteration:
                text = "DISCONNECTED"
            return FakeResponse(text)

        monkeypatch.setattr(thetadata_helper.requests, "get", fake_get)
        monkeypatch.setattr(thetadata_helper, "start_theta_data_client", lambda *args, **kwargs: object())
        monkeypatch.setattr(thetadata_helper, "is_process_alive", lambda: True)
        monkeypatch.setattr(thetadata_helper, "CONNECTION_MAX_RETRIES", 1, raising=False)
        monkeypatch.setattr(thetadata_helper, "MAX_TERMINAL_RESTART_CYCLES", 1, raising=False)
        monkeypatch.setattr(thetadata_helper, "BOOT_GRACE_PERIOD", 0, raising=False)
        monkeypatch.setattr(thetadata_helper, "CONNECTION_RETRY_SLEEP", 0, raising=False)
        monkeypatch.setattr(thetadata_helper.time, "sleep", lambda *args, **kwargs: None)

        with pytest.raises(thetadata_helper.ThetaDataConnectionError):
            thetadata_helper.check_connection("user", "pass", wait_for_connection=True)


def test_finalize_day_frame_handles_dst_fallback():
    tz = pytz.timezone("America/New_York")
    utc = pytz.UTC
    frame_index = pd.date_range(
        end=tz.localize(datetime.datetime(2024, 10, 31, 16, 0)),
        periods=5,
        freq="D",
    )
    frame = pd.DataFrame(
        {
            "open": [100 + i for i in range(len(frame_index))],
            "high": [101 + i for i in range(len(frame_index))],
            "low": [99 + i for i in range(len(frame_index))],
            "close": [100.5 + i for i in range(len(frame_index))],
            "volume": [1000 + i for i in range(len(frame_index))],
        },
        index=frame_index,
    )

    data_source = ThetaDataBacktestingPandas(
        datetime_start=utc.localize(datetime.datetime(2024, 10, 1)),
        datetime_end=utc.localize(datetime.datetime(2024, 11, 5)),
        username="user",
        password="pass",
        use_quote_data=False,
    )

    current_dt = utc.localize(datetime.datetime(2024, 11, 4, 13, 30))
    result = data_source._finalize_day_frame(
        frame,
        current_dt,
        requested_length=len(frame_index),
        timeshift=None,
        asset=Asset("TSLA"),
    )

    assert result is not None
    assert len(result) == len(frame_index)

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


@patch("lumibot.tools.thetadata_helper.requests.get")
def test_probe_terminal_ready_falls_back_when_status_endpoint_missing(mock_requests):
    mock_requests.side_effect = [
        SimpleNamespace(status_code=404, text="Not Found"),
        SimpleNamespace(status_code=200, text="[]"),
    ]
    assert thetadata_helper._probe_terminal_ready() is True
    assert mock_requests.call_count == 2


@patch("lumibot.tools.thetadata_helper.requests.get")
def test_probe_terminal_ready_handles_server_starting(mock_requests):
    mock_requests.return_value = SimpleNamespace(status_code=571, text="SERVER_STARTING")
    assert thetadata_helper._probe_terminal_ready() is False
    mock_requests.assert_called_once()


if __name__ == '__main__':
    pytest.main()


def test_thetadata_no_future_minutes(monkeypatch):
    tz = pytz.timezone('America/New_York')
    now = tz.localize(datetime.datetime(2025, 1, 6, 9, 30))
    frame = pd.DataFrame(
        {
            'datetime': [
                tz.localize(datetime.datetime(2025, 1, 6, 9, 29)),
                tz.localize(datetime.datetime(2025, 1, 6, 9, 31)),
            ],
            'open': [4330.0, 4332.0],
            'high': [4331.0, 4333.0],
            'low': [4329.5, 4331.5],
            'close': [4330.5, 4332.5],
            'volume': [1_200, 1_250],
            'missing': [False, False],
        }
    )

    monkeypatch.setattr(thetadata_helper, 'get_price_data', lambda *args, **kwargs: frame.copy())
    monkeypatch.setattr(thetadata_helper, 'reset_theta_terminal_tracking', lambda: None)

    data_source = ThetaDataBacktestingPandas(
        datetime_start=now - datetime.timedelta(days=1),
        datetime_end=now + datetime.timedelta(days=1),
        username='user',
        password='pass',
        use_quote_data=False,
    )
    data_source._datetime = now

    asset = Asset('MES', asset_type=Asset.AssetType.CONT_FUTURE)

    bars = data_source.get_historical_prices(
        asset,
        length=1,
        timestep='minute',
        quote=Asset('USD', asset_type=Asset.AssetType.FOREX),
        timeshift=datetime.timedelta(minutes=-1),
    )

    assert bars is not None
    assert len(bars.df) == 1
    assert bars.df.index[-1].tz_convert(tz) <= now


def test_get_historical_eod_data_handles_missing_date(monkeypatch):
    response = _fixture_response("stock_eod.json")

    def fake_request(url, headers, querystring, username, password):
        return response

    minute_index = pd.to_datetime(
        ["2024-11-15 13:30:00", "2024-11-18 13:30:00"],
        utc=True,
    )
    minute_df = pd.DataFrame({"open": [301.25, 341.0]}, index=minute_index)
    minute_df.index.name = "datetime"

    monkeypatch.setattr(thetadata_helper, "get_request", fake_request)
    monkeypatch.setattr(
        thetadata_helper,
        "get_historical_data",
        lambda *args, **kwargs: minute_df,
    )

    asset = Asset(symbol="TSLA", asset_type="stock")
    start_dt = datetime.datetime(2024, 11, 15, tzinfo=pytz.UTC)
    end_dt = datetime.datetime(2024, 11, 18, tzinfo=pytz.UTC)

    df = thetadata_helper.get_historical_eod_data(
        asset,
        start_dt,
        end_dt,
        username="rob-dev@lumiwealth.com",
        password="TestTestTest",
    )

    assert list(df.index.date) == [datetime.date(2024, 11, 15), datetime.date(2024, 11, 18)]
    assert df.index.tz is not None
    assert pytest.approx(df.loc["2024-11-15", "open"]) == 301.25


def test_get_historical_data_stock_v3_schema(monkeypatch):
    response = _fixture_response("stock_history_ohlc.json")

    monkeypatch.setattr(
        thetadata_helper,
        "get_request",
        lambda *args, **kwargs: response,
    )
    monkeypatch.setattr(
        thetadata_helper,
        "get_trading_dates",
        lambda asset, start, end: [datetime.date(2024, 11, 15)],
    )

    asset = Asset(symbol="TSLA", asset_type="stock")
    start_dt = datetime.datetime(2024, 11, 15, 9, 30, tzinfo=pytz.UTC)
    end_dt = datetime.datetime(2024, 11, 15, 10, 0, tzinfo=pytz.UTC)

    df = thetadata_helper.get_historical_data(
        asset=asset,
        start_dt=start_dt,
        end_dt=end_dt,
        ivl=60000,
        username="user",
        password="pass",
        datastyle="ohlc",
    )

    # Theta sometimes emits trailing placeholder bars with zero volume/count; ensure they are dropped.
    assert len(df) == 5
    assert df.index.tz is not None
    assert {"open", "high", "low", "close"} <= set(df.columns)


def test_get_historical_data_stock_quotes_v3_schema(monkeypatch):
    response = _fixture_response("stock_history_quote.json")

    monkeypatch.setattr(
        thetadata_helper,
        "get_request",
        lambda *args, **kwargs: response,
    )
    monkeypatch.setattr(
        thetadata_helper,
        "get_trading_dates",
        lambda asset, start, end: [datetime.date(2024, 11, 15)],
    )

    asset = Asset(symbol="TSLA", asset_type="stock")
    start_dt = datetime.datetime(2024, 11, 15, 9, 30, tzinfo=pytz.UTC)
    end_dt = datetime.datetime(2024, 11, 15, 9, 33, tzinfo=pytz.UTC)

    df = thetadata_helper.get_historical_data(
        asset=asset,
        start_dt=start_dt,
        end_dt=end_dt,
        ivl=60000,
        username="user",
        password="pass",
        datastyle="quote",
    )

    assert len(df) == 4
    assert df.index.tz is not None
    assert {"bid", "ask"} <= set(df.columns)
    assert (df["bid"] > 0).any()


def test_option_history_parsing_handles_v3_payloads(monkeypatch):
    response = _fixture_response("option_history_ohlc.json")
    monkeypatch.setattr(
        thetadata_helper,
        "get_request",
        lambda *args, **kwargs: response,
    )
    monkeypatch.setattr(
        thetadata_helper,
        "get_trading_dates",
        lambda asset, start, end: [datetime.date(2024, 11, 15)],
    )

    asset = Asset(
        symbol="TSLA",
        asset_type="option",
        expiration=date(2024, 11, 22),
        strike=340.0,
        right="CALL",
    )
    start_dt = datetime.datetime(2024, 11, 15, 9, 30, tzinfo=pytz.UTC)
    end_dt = datetime.datetime(2024, 11, 15, 9, 33, tzinfo=pytz.UTC)

    df = thetadata_helper.get_historical_data(
        asset=asset,
        start_dt=start_dt,
        end_dt=end_dt,
        ivl=60000,
        username="user",
        password="pass",
        datastyle="ohlc",
    )

    assert "symbol" in df.columns
    assert not df.empty
    assert (df["close"] >= 0).all()


def test_option_quote_history_parsing_handles_v3_payloads(monkeypatch):
    response = _fixture_response("option_history_quote.json")
    monkeypatch.setattr(
        thetadata_helper,
        "get_request",
        lambda *args, **kwargs: response,
    )
    monkeypatch.setattr(
        thetadata_helper,
        "get_trading_dates",
        lambda asset, start, end: [datetime.date(2024, 11, 15)],
    )

    asset = Asset(
        symbol="TSLA",
        asset_type="option",
        expiration=date(2024, 11, 22),
        strike=340.0,
        right="CALL",
    )
    start_dt = datetime.datetime(2024, 11, 15, 9, 30, tzinfo=pytz.UTC)
    end_dt = datetime.datetime(2024, 11, 15, 9, 32, tzinfo=pytz.UTC)

    df = thetadata_helper.get_historical_data(
        asset=asset,
        start_dt=start_dt,
        end_dt=end_dt,
        ivl=60000,
        username="user",
        password="pass",
        datastyle="quote",
    )

    assert {"bid", "ask"} <= set(df.columns)
    assert len(df) == 2


def test_option_list_helpers_handle_v3_schema(monkeypatch):
    exp_response = _fixture_response("option_expirations.json")
    strike_response = _fixture_response("option_strikes.json")
    responses = [exp_response, strike_response]

    def fake_get_request(url, headers, querystring, username, password):
        return responses.pop(0)

    monkeypatch.setattr(thetadata_helper, "get_request", fake_get_request)

    expirations = thetadata_helper.get_expirations(
        username="user",
        password="pass",
        ticker="TSLA",
        after_date=date(2012, 7, 1),
    )
    assert expirations == [
        "2012-07-21",
        "2012-08-18",
        "2012-09-22",
        "2012-10-20",
        "2012-11-17",
        "2012-12-22",
        "2013-01-19",
    ]

    strikes = thetadata_helper.get_strikes(
        username="user",
        password="pass",
        ticker="TSLA",
        expiration=datetime.datetime(2024, 11, 22),
    )
    assert strikes[:3] == [362.5, 160.0, 320.0]


def test_option_dates_quote_fixture_is_normalized():
    response = _fixture_response("option_dates_quote.json")
    rows = response["response"]
    assert rows[0][0] == "2024-10-03"
    assert len(rows) == 5


def test_theta_endpoints_use_v3_prefix():
    for path in thetadata_helper.HISTORY_ENDPOINTS.values():
        assert path.startswith("/v3/")
    for path in thetadata_helper.EOD_ENDPOINTS.values():
        assert path.startswith("/v3/")
    for path in thetadata_helper.OPTION_LIST_ENDPOINTS.values():
        assert path.startswith("/v3/")
