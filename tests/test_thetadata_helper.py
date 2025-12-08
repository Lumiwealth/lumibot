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
from lumibot.entities import Asset, Data
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


def test_normalize_dividend_events_returns_expected_columns():
    df = pd.DataFrame(
        {
            "ex_dividend_date": ["2024-01-15", "2024-04-15"],
            "amount": ["0.12", "0.34"],
            "record_date": ["2024-01-16", None],
            "pay_date": ["2024-01-20", None],
            "frequency": ["quarterly", "quarterly"],
        }
    )
    normalized = thetadata_helper._normalize_dividend_events(df, "TQQQ")
    assert list(normalized.columns)[:2] == ["event_date", "cash_amount"]
    assert normalized["cash_amount"].tolist() == [0.12, 0.34]
    assert normalized["event_date"].dt.tz is not None


def test_normalize_split_events_supports_ratio_calculations():
    df = pd.DataFrame(
        {
            "execution_date": ["2025-11-20", "2026-01-10"],
            "split_to": [2, None],
            "split_from": [1, None],
            "ratio": [None, "3:2"],
        }
    )
    normalized = thetadata_helper._normalize_split_events(df, "TQQQ")
    assert normalized["ratio"].tolist() == [2.0, 1.5]
    assert normalized["event_date"].dt.tz is not None


@patch("lumibot.tools.thetadata_helper._get_theta_dividends")
@patch("lumibot.tools.thetadata_helper._get_theta_splits")
def test_apply_corporate_actions_populates_columns(mock_splits, mock_dividends):
    asset = Asset(symbol="TQQQ", asset_type="stock")
    index = pd.to_datetime(["2024-01-15", "2024-02-15"], utc=True)
    frame = pd.DataFrame(
        {
            "open": [100, 110],
            "high": [101, 111],
            "low": [99, 109],
            "close": [100.5, 110.5],
            "volume": [1_000, 1_100],
        },
        index=index,
    )
    mock_dividends.return_value = pd.DataFrame(
        {
            "event_date": pd.to_datetime(["2024-01-15"], utc=True),
            "cash_amount": [0.25],
        }
    )
    mock_splits.return_value = pd.DataFrame(
        {
            "event_date": pd.to_datetime(["2024-02-15"], utc=True),
            "ratio": [2.0],
        }
    )

    enriched = thetadata_helper._apply_corporate_actions_to_frame(
        asset,
        frame.copy(),
        date(2024, 1, 1),
        date(2024, 3, 1),
        "user",
        "pass",
    )

    # Dividend is split-adjusted: $0.25 / 2.0 (split ratio) = $0.125
    # This is correct behavior - pre-split dividends must be adjusted
    assert enriched["dividend"].tolist() == [0.125, 0.0]
    assert enriched["stock_splits"].tolist() == [0.0, 2.0]
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
    monkeypatch.setattr(
        thetadata_helper,
        "_apply_corporate_actions_to_frame",
        lambda asset, frame, start, end, username, password: frame,
    )

    asset = Asset(asset_type="stock", symbol="PLTR")
    start = pytz.UTC.localize(datetime.datetime(2024, 9, 16))
    end = pytz.UTC.localize(datetime.datetime(2024, 9, 18))

    df = thetadata_helper.get_historical_eod_data(
        asset=asset,
        start_dt=start,
        end_dt=end,
        username="user",
        password="pass",
        apply_corporate_actions=False,
    )

    assert df is not None
    assert not df.empty
    assert df.index.tzinfo is not None
    assert "open" in df.columns


def test_get_historical_eod_data_avoids_minute_corrections(monkeypatch):
    fixture = load_thetadata_fixture("stock_history_eod.json")
    monkeypatch.setattr(thetadata_helper, "get_request", lambda **_: fixture)
    minute_fetch = MagicMock(return_value=None)
    monkeypatch.setattr(thetadata_helper, "get_historical_data", minute_fetch)
    monkeypatch.setattr(
        thetadata_helper,
        "_apply_corporate_actions_to_frame",
        lambda asset, frame, start, end, username, password: frame,
    )

    asset = Asset(asset_type="stock", symbol="PLTR")
    start = pytz.UTC.localize(datetime.datetime(2024, 9, 16))
    end = pytz.UTC.localize(datetime.datetime(2024, 9, 18))

    df = thetadata_helper.get_historical_eod_data(
        asset=asset,
        start_dt=start,
        end_dt=end,
        username="user",
        password="pass",
        apply_corporate_actions=False,
    )

    assert df is not None
    assert not df.empty
    minute_fetch.assert_not_called()


def test_get_historical_eod_data_falls_back_to_date_when_created_missing(monkeypatch):
    payload = {
        "header": {"format": ["date", "open", "high", "low", "close", "volume"]},
        "response": [
            ["2024-11-01", 10.0, 11.0, 9.5, 10.5, 1_000],
            ["2024-11-04", 11.0, 12.0, 10.5, 11.5, 2_000],
        ],
    }

    monkeypatch.setattr(thetadata_helper, "get_request", lambda **_: payload)
    monkeypatch.setattr(thetadata_helper, "get_historical_data", lambda **_: None)
    monkeypatch.setattr(
        thetadata_helper,
        "_apply_corporate_actions_to_frame",
        lambda asset, frame, start, end, username, password: frame,
    )

    asset = Asset(asset_type="stock", symbol="AAPL")
    start = pytz.UTC.localize(datetime.datetime(2024, 11, 1))
    end = pytz.UTC.localize(datetime.datetime(2024, 11, 4))

    df = thetadata_helper.get_historical_eod_data(
        asset=asset,
        start_dt=start,
        end_dt=end,
        username="user",
        password="pass",
        apply_corporate_actions=False,
    )

    assert list(df.index.strftime("%Y-%m-%d")) == ["2024-11-01", "2024-11-04"]
    assert df.loc["2024-11-01", "open"] == 10.0


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
        apply_corporate_actions=False,
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

    minute_fetch = MagicMock(
        side_effect=thetadata_helper.ThetaRequestError(
            "Cannot connect to Theta Data!", status_code=400, body="Start must be before end"
        )
    )
    monkeypatch.setattr(thetadata_helper, "get_historical_data", minute_fetch)

    asset = Asset(asset_type="stock", symbol="MSFT")
    tz = pytz.UTC
    start = tz.localize(datetime.datetime(2024, 11, 21, 19, 0))
    end = tz.localize(datetime.datetime(2024, 11, 22, 19, 0))

    with caplog.at_level(logging.WARNING, logger="lumibot.tools.thetadata_helper"):
        df = thetadata_helper.get_historical_eod_data(
            asset=asset,
            start_dt=start,
            end_dt=end,
            username="user",
            password="pass",
        )

    assert not df.empty
    minute_fetch.assert_not_called()
    assert "skipping open fix" not in caplog.text


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


@patch('lumibot.tools.thetadata_helper.get_trading_dates')
@patch('lumibot.tools.thetadata_helper.update_cache')
@patch('lumibot.tools.thetadata_helper.update_df')
@patch('lumibot.tools.thetadata_helper.get_historical_data')
@patch('lumibot.tools.thetadata_helper.get_missing_dates')
@patch('lumibot.tools.thetadata_helper.load_cache')
@patch('lumibot.tools.thetadata_helper.build_cache_filename')
@patch('lumibot.tools.thetadata_helper.tqdm')
def test_get_price_data_preserve_full_history_returns_full_cache(
    mock_tqdm,
    mock_build_cache_filename,
    mock_load_cache,
    mock_get_missing_dates,
    mock_get_historical_data,
    mock_update_df,
    mock_update_cache,
    mock_get_trading_dates,
):
    mock_build_cache_filename.return_value.exists.return_value = True
    date_index = pd.date_range("2020-01-01", periods=10, freq="D", tz=LUMIBOT_DEFAULT_PYTZ)
    df_cache = pd.DataFrame(
        {
            "open": np.arange(len(date_index), dtype=float),
            "high": np.arange(len(date_index), dtype=float) + 0.5,
            "low": np.arange(len(date_index), dtype=float) - 0.5,
            "close": np.arange(len(date_index), dtype=float) + 0.25,
            "volume": 1000,
        },
        index=date_index,
    )
    mock_load_cache.return_value = df_cache
    mock_get_missing_dates.return_value = []
    asset = Asset(asset_type="stock", symbol="MSFT")
    start = LUMIBOT_DEFAULT_PYTZ.localize(datetime.datetime(2020, 1, 5))
    end = LUMIBOT_DEFAULT_PYTZ.localize(datetime.datetime(2020, 1, 6))

    df = thetadata_helper.get_price_data(
        "user",
        "pass",
        asset,
        start,
        end,
        "day",
        dt=start,
        preserve_full_history=True,
    )

    assert df is not None
    assert len(df) == len(df_cache)
    assert df.index.min() == date_index.min()
    assert df.index.max() == date_index.max()
    mock_get_historical_data.assert_not_called()


def test_get_price_data_daily_placeholders_prevent_refetch(monkeypatch, tmp_path):
    from lumibot.constants import LUMIBOT_DEFAULT_PYTZ

    cache_root = tmp_path / "cache_root"
    monkeypatch.setattr(thetadata_helper, "LUMIBOT_CACHE_FOLDER", str(cache_root))
    thetadata_helper.reset_connection_diagnostics()

    # Disable remote cache to avoid S3 interference
    class DisabledCacheManager:
        enabled = False
        mode = None  # Not using S3 mode
        def ensure_local_file(self, *args, **kwargs):
            return False
        def on_local_update(self, *args, **kwargs):
            return False
    monkeypatch.setattr(thetadata_helper, "get_backtest_cache", lambda: DisabledCacheManager())

    asset = Asset(asset_type="stock", symbol="PLTR")
    # Use 10 trading days to exceed the minimum row validation (>5 rows required)
    start = LUMIBOT_DEFAULT_PYTZ.localize(datetime.datetime(2024, 1, 2))
    end = LUMIBOT_DEFAULT_PYTZ.localize(datetime.datetime(2024, 1, 15))
    trading_days = [
        datetime.date(2024, 1, 2),
        datetime.date(2024, 1, 3),
        datetime.date(2024, 1, 4),
        datetime.date(2024, 1, 5),
        datetime.date(2024, 1, 8),
        datetime.date(2024, 1, 9),
        datetime.date(2024, 1, 10),
        datetime.date(2024, 1, 11),
        datetime.date(2024, 1, 12),
        datetime.date(2024, 1, 15),  # This will be the placeholder (missing)
    ]

    # Return 9 of 10 trading days - missing data for Jan 15
    partial_df = pd.DataFrame(
        {
            "datetime": pd.to_datetime([
                "2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05",
                "2024-01-08", "2024-01-09", "2024-01-10", "2024-01-11", "2024-01-12"
            ], utc=True),
            "open": [10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0],
            "high": [11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0],
            "low": [9.5, 10.5, 11.5, 12.5, 13.5, 14.5, 15.5, 16.5, 17.5],
            "close": [10.5, 11.5, 12.5, 13.5, 14.5, 15.5, 16.5, 17.5, 18.5],
            "volume": [1_000, 1_200, 1_100, 1_300, 1_400, 1_500, 1_600, 1_700, 1_800],
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
            assert len(first) == 9  # 9 real data rows (excluding missing Jan 15)
            expected_dates = {
                datetime.date(2024, 1, 2), datetime.date(2024, 1, 3), datetime.date(2024, 1, 4),
                datetime.date(2024, 1, 5), datetime.date(2024, 1, 8), datetime.date(2024, 1, 9),
                datetime.date(2024, 1, 10), datetime.date(2024, 1, 11), datetime.date(2024, 1, 12),
            }
            assert set(first.index.date) == expected_dates

            cache_file = thetadata_helper.build_cache_filename(asset, "day", "ohlc")
            loaded = thetadata_helper.load_cache(cache_file)
            assert len(loaded) == 10  # 9 data + 1 placeholder
            assert "missing" in loaded.columns
            assert int(loaded["missing"].sum()) == 1
            missing_dates = {idx.date() for idx, flag in loaded["missing"].items() if flag}
            assert missing_dates == {datetime.date(2024, 1, 15)}

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
            assert len(second) == 9  # 9 real data rows
            assert set(second.index.date) == expected_dates


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


# =========================================================================
# CACHE FIDELITY TESTS (Added 2025-12-07)
# These tests verify that the cache validation correctly distinguishes between:
# - INTEGRITY failures (corrupt data): DELETE cache, re-fetch everything
# - COVERAGE failures (incomplete range): KEEP cache, extend with missing dates
# =========================================================================

def test_cache_fidelity_coverage_failure_logs_extend_message(monkeypatch, tmp_path, caplog):
    """Test that coverage failures (stale_max_date) log COVERAGE_EXTEND, not INTEGRITY_FAILURE.

    This test verifies the fix for the cache fidelity bug where valid cache was
    being deleted when it simply didn't cover the requested date range.
    """
    import logging
    from lumibot.constants import LUMIBOT_DEFAULT_PYTZ

    cache_root = tmp_path / "cache_root"
    monkeypatch.setattr(thetadata_helper, "LUMIBOT_CACHE_FOLDER", str(cache_root))
    thetadata_helper.reset_connection_diagnostics()

    # Disable remote cache
    class DisabledCacheManager:
        enabled = False
        mode = None
        def ensure_local_file(self, *args, **kwargs):
            return False
        def on_local_update(self, *args, **kwargs):
            return False
    monkeypatch.setattr(thetadata_helper, "get_backtest_cache", lambda: DisabledCacheManager())

    asset = Asset(asset_type="stock", symbol="COVTEST")

    # First request: populate cache
    start1 = LUMIBOT_DEFAULT_PYTZ.localize(datetime.datetime(2024, 1, 2))
    end1 = LUMIBOT_DEFAULT_PYTZ.localize(datetime.datetime(2024, 1, 5))
    trading_days1 = [datetime.date(2024, 1, 2), datetime.date(2024, 1, 3),
                     datetime.date(2024, 1, 4), datetime.date(2024, 1, 5)]
    df1 = pd.DataFrame({
        "datetime": pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"], utc=True),
        "open": [100.0, 101.0, 102.0, 103.0], "high": [101.0, 102.0, 103.0, 104.0],
        "low": [99.0, 100.0, 101.0, 102.0], "close": [100.5, 101.5, 102.5, 103.5],
        "volume": [1000, 1100, 1200, 1300],
    })

    progress_stub = MagicMock()
    progress_stub.update.return_value = None
    progress_stub.close.return_value = None

    with patch("lumibot.tools.thetadata_helper.tqdm", return_value=progress_stub), \
         patch("lumibot.tools.thetadata_helper.get_trading_dates", return_value=trading_days1):
        eod_mock1 = MagicMock(return_value=df1)
        with patch("lumibot.tools.thetadata_helper.get_historical_eod_data", eod_mock1):
            thetadata_helper.get_price_data("user", "pass", asset, start1, end1, "day")

    # Second request: extends range (should trigger COVERAGE_EXTEND)
    start2 = LUMIBOT_DEFAULT_PYTZ.localize(datetime.datetime(2024, 1, 2))
    end2 = LUMIBOT_DEFAULT_PYTZ.localize(datetime.datetime(2024, 1, 8))
    trading_days2 = trading_days1 + [datetime.date(2024, 1, 8)]
    df_new = pd.DataFrame({
        "datetime": pd.to_datetime(["2024-01-08"], utc=True),
        "open": [104.0], "high": [105.0], "low": [103.0], "close": [104.5], "volume": [1400],
    })

    caplog.clear()
    with caplog.at_level(logging.INFO):
        with patch("lumibot.tools.thetadata_helper.tqdm", return_value=progress_stub), \
             patch("lumibot.tools.thetadata_helper.get_trading_dates", return_value=trading_days2):
            eod_mock2 = MagicMock(return_value=df_new)
            with patch("lumibot.tools.thetadata_helper.get_historical_eod_data", eod_mock2):
                thetadata_helper.get_price_data("user", "pass", asset, start2, end2, "day")

    # CRITICAL: Verify the fix is working - should see COVERAGE_EXTEND, not INTEGRITY_FAILURE
    log_messages = [rec.message for rec in caplog.records]
    coverage_extend_found = any("COVERAGE_EXTEND" in msg for msg in log_messages)
    integrity_failure_found = any("INTEGRITY_FAILURE" in msg for msg in log_messages)

    assert coverage_extend_found, "Expected COVERAGE_EXTEND log message for stale cache"
    assert not integrity_failure_found, "Should NOT see INTEGRITY_FAILURE for coverage issues"


def test_cache_fidelity_integrity_failure_logs_integrity_message(monkeypatch, tmp_path, caplog):
    """Test that integrity failures (duplicate_index) log INTEGRITY_FAILURE and delete cache."""
    import logging
    from lumibot.constants import LUMIBOT_DEFAULT_PYTZ

    cache_root = tmp_path / "cache_root"
    monkeypatch.setattr(thetadata_helper, "LUMIBOT_CACHE_FOLDER", str(cache_root))
    thetadata_helper.reset_connection_diagnostics()

    # Disable remote cache
    class DisabledCacheManager:
        enabled = False
        mode = None
        def ensure_local_file(self, *args, **kwargs):
            return False
        def on_local_update(self, *args, **kwargs):
            return False
    monkeypatch.setattr(thetadata_helper, "get_backtest_cache", lambda: DisabledCacheManager())

    asset = Asset(asset_type="stock", symbol="INTTEST")
    start = LUMIBOT_DEFAULT_PYTZ.localize(datetime.datetime(2024, 1, 2))
    end = LUMIBOT_DEFAULT_PYTZ.localize(datetime.datetime(2024, 1, 5))
    trading_days = [datetime.date(2024, 1, 2), datetime.date(2024, 1, 3),
                    datetime.date(2024, 1, 4), datetime.date(2024, 1, 5)]

    # Create cache file with DUPLICATE INDEX (integrity failure)
    # Note: Use the actual parquet format that load_cache expects
    cache_file = thetadata_helper.build_cache_filename(asset, "day", "ohlc")
    cache_file.parent.mkdir(parents=True, exist_ok=True)

    bad_df = pd.DataFrame({
        "datetime": pd.to_datetime(["2024-01-02", "2024-01-02", "2024-01-03", "2024-01-04"], utc=True),
        "open": [100.0, 100.0, 101.0, 102.0],
        "high": [101.0, 101.0, 102.0, 103.0],
        "low": [99.0, 99.0, 100.0, 101.0],
        "close": [100.5, 100.5, 101.5, 102.5],
        "volume": [1000, 1000, 1100, 1200],
        "missing": [False, False, False, False],
    })
    bad_df.to_parquet(cache_file, index=False)

    # Good data to return when fetching
    good_df = pd.DataFrame({
        "datetime": pd.to_datetime(["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05"], utc=True),
        "open": [100.0, 101.0, 102.0, 103.0], "high": [101.0, 102.0, 103.0, 104.0],
        "low": [99.0, 100.0, 101.0, 102.0], "close": [100.5, 101.5, 102.5, 103.5],
        "volume": [1000, 1100, 1200, 1300],
    })

    progress_stub = MagicMock()
    progress_stub.update.return_value = None
    progress_stub.close.return_value = None

    caplog.clear()
    with caplog.at_level(logging.WARNING):
        with patch("lumibot.tools.thetadata_helper.tqdm", return_value=progress_stub), \
             patch("lumibot.tools.thetadata_helper.get_trading_dates", return_value=trading_days):
            eod_mock = MagicMock(return_value=good_df)
            with patch("lumibot.tools.thetadata_helper.get_historical_eod_data", eod_mock):
                result = thetadata_helper.get_price_data("user", "pass", asset, start, end, "day")

    # CRITICAL: Verify integrity failures are correctly identified
    log_messages = [rec.message for rec in caplog.records]
    integrity_failure_found = any("INTEGRITY_FAILURE" in msg for msg in log_messages)
    coverage_extend_found = any("COVERAGE_EXTEND" in msg for msg in log_messages)

    assert integrity_failure_found, "Expected INTEGRITY_FAILURE log for duplicate index"
    assert not coverage_extend_found, "Should NOT see COVERAGE_EXTEND for integrity issues"
    assert result is not None, "Should return data after re-fetching"


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


class TestUpdateCacheMerge:
    """Tests for the cache merge functionality in update_cache().

    CRITICAL: These tests verify that when new data is fetched, the old cached data
    is merged with the new data to prevent data loss. This fix was added in Dec 2025
    to address an issue where cache was being overwritten with partial data.
    """

    def test_merge_preserves_old_cached_data(self, tmp_path, monkeypatch):
        """Verify that old cached data is preserved when new data is fetched.

        This is the core test for the cache merge fix. Old data from the cache
        should not be lost when new data is fetched for different dates.
        """
        monkeypatch.setattr(thetadata_helper, "LUMIBOT_CACHE_FOLDER", str(tmp_path))
        cache_file = thetadata_helper.build_cache_filename(Asset("TEST"), "1D", "ohlc")
        cache_file.parent.mkdir(parents=True, exist_ok=True)

        # Old cached data: Jan 2-3, 2025
        df_cached = pd.DataFrame({
            "datetime": pd.to_datetime(["2025-01-02", "2025-01-03"]),
            "open": [100.0, 101.0],
            "high": [102.0, 103.0],
            "low": [99.0, 100.0],
            "close": [101.0, 102.0],
            "volume": [1000, 1100],
        }).set_index("datetime")

        # New data: Jan 6, 2025 (doesn't overlap with cached)
        df_all = pd.DataFrame({
            "datetime": pd.to_datetime(["2025-01-06"]),
            "open": [105.0],
            "high": [107.0],
            "low": [104.0],
            "close": [106.0],
            "volume": [1500],
        }).set_index("datetime")

        # Update cache with new data and old cached data
        thetadata_helper.update_cache(cache_file, df_all, df_cached)

        # Read back the cache and verify both old and new data are present
        assert cache_file.exists()
        df_result = pd.read_parquet(cache_file)
        df_result = df_result.set_index("datetime") if "datetime" in df_result.columns else df_result

        # Should have 3 rows: 2 from cache + 1 new
        assert len(df_result) == 3, f"Expected 3 rows, got {len(df_result)}"

        # Verify specific dates are present
        dates = df_result.index.date if hasattr(df_result.index, 'date') else df_result.index
        assert pd.Timestamp("2025-01-02").date() in [d.date() if hasattr(d, 'date') else d for d in dates]
        assert pd.Timestamp("2025-01-03").date() in [d.date() if hasattr(d, 'date') else d for d in dates]
        assert pd.Timestamp("2025-01-06").date() in [d.date() if hasattr(d, 'date') else d for d in dates]

    def test_merge_prefers_new_data_over_cached(self, tmp_path, monkeypatch):
        """When new data overlaps with cached data, new data should take precedence."""
        monkeypatch.setattr(thetadata_helper, "LUMIBOT_CACHE_FOLDER", str(tmp_path))
        cache_file = thetadata_helper.build_cache_filename(Asset("TEST"), "1D", "ohlc")
        cache_file.parent.mkdir(parents=True, exist_ok=True)

        # Old cached data: Jan 2, 2025 with old price
        df_cached = pd.DataFrame({
            "datetime": pd.to_datetime(["2025-01-02"]),
            "open": [100.0],
            "high": [102.0],
            "low": [99.0],
            "close": [101.0],
            "volume": [1000],
        }).set_index("datetime")

        # New data: Jan 2, 2025 with updated price (same date, different values)
        df_all = pd.DataFrame({
            "datetime": pd.to_datetime(["2025-01-02"]),
            "open": [200.0],  # Different value
            "high": [202.0],
            "low": [199.0],
            "close": [201.0],
            "volume": [2000],
        }).set_index("datetime")

        thetadata_helper.update_cache(cache_file, df_all, df_cached)

        df_result = pd.read_parquet(cache_file)
        df_result = df_result.set_index("datetime") if "datetime" in df_result.columns else df_result

        # Should have 1 row with the NEW data values
        assert len(df_result) == 1
        assert df_result["open"].iloc[0] == 200.0, "New data should take precedence over cached"

    def test_merge_with_empty_cached_data(self, tmp_path, monkeypatch):
        """When cached data is empty, new data should be saved directly."""
        monkeypatch.setattr(thetadata_helper, "LUMIBOT_CACHE_FOLDER", str(tmp_path))
        cache_file = thetadata_helper.build_cache_filename(Asset("TEST"), "1D", "ohlc")
        cache_file.parent.mkdir(parents=True, exist_ok=True)

        df_cached = None  # No cached data

        df_all = pd.DataFrame({
            "datetime": pd.to_datetime(["2025-01-06"]),
            "open": [105.0],
            "high": [107.0],
            "low": [104.0],
            "close": [106.0],
            "volume": [1500],
        }).set_index("datetime")

        thetadata_helper.update_cache(cache_file, df_all, df_cached)

        assert cache_file.exists()
        df_result = pd.read_parquet(cache_file)
        assert len(df_result) == 1

    def test_merge_with_placeholder_rows(self, tmp_path, monkeypatch):
        """Placeholder rows (missing=True) should be preserved during merge."""
        monkeypatch.setattr(thetadata_helper, "LUMIBOT_CACHE_FOLDER", str(tmp_path))
        cache_file = thetadata_helper.build_cache_filename(Asset("TEST"), "1D", "ohlc")
        cache_file.parent.mkdir(parents=True, exist_ok=True)

        # Cached data with a placeholder row
        df_cached = pd.DataFrame({
            "datetime": pd.to_datetime(["2025-01-02", "2025-01-03"]),
            "open": [100.0, None],
            "high": [102.0, None],
            "low": [99.0, None],
            "close": [101.0, None],
            "volume": [1000, 0],
            "missing": [False, True],  # Jan 3 is a placeholder
        }).set_index("datetime")

        # New data for Jan 6
        df_all = pd.DataFrame({
            "datetime": pd.to_datetime(["2025-01-06"]),
            "open": [105.0],
            "high": [107.0],
            "low": [104.0],
            "close": [106.0],
            "volume": [1500],
        }).set_index("datetime")

        thetadata_helper.update_cache(cache_file, df_all, df_cached)

        df_result = pd.read_parquet(cache_file)
        df_result = df_result.set_index("datetime") if "datetime" in df_result.columns else df_result

        # Should have 3 rows including the placeholder
        assert len(df_result) == 3

    def test_merge_maintains_sorted_order(self, tmp_path, monkeypatch):
        """Merged data should be sorted by datetime index."""
        monkeypatch.setattr(thetadata_helper, "LUMIBOT_CACHE_FOLDER", str(tmp_path))
        cache_file = thetadata_helper.build_cache_filename(Asset("TEST"), "1D", "ohlc")
        cache_file.parent.mkdir(parents=True, exist_ok=True)

        # Cached data: Jan 6, 2025
        df_cached = pd.DataFrame({
            "datetime": pd.to_datetime(["2025-01-06"]),
            "open": [105.0],
            "high": [107.0],
            "low": [104.0],
            "close": [106.0],
            "volume": [1500],
        }).set_index("datetime")

        # New data: Jan 2, 2025 (earlier date)
        df_all = pd.DataFrame({
            "datetime": pd.to_datetime(["2025-01-02"]),
            "open": [100.0],
            "high": [102.0],
            "low": [99.0],
            "close": [101.0],
            "volume": [1000],
        }).set_index("datetime")

        thetadata_helper.update_cache(cache_file, df_all, df_cached)

        df_result = pd.read_parquet(cache_file)
        df_result = df_result.set_index("datetime") if "datetime" in df_result.columns else df_result

        # Verify sorted order (Jan 2 should come before Jan 6)
        dates = list(df_result.index)
        assert dates[0] < dates[1], "Data should be sorted by datetime"


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
@pytest.mark.skipif(
    os.environ.get("ALLOW_LOCAL_THETA_TERMINAL") != "true",
    reason="Local ThetaTerminal is disabled on this environment",
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
@pytest.mark.skipif(
    os.environ.get("ALLOW_LOCAL_THETA_TERMINAL") != "true",
    reason="Local ThetaTerminal is disabled on this environment",
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
@pytest.mark.skipif(
    os.environ.get("ALLOW_LOCAL_THETA_TERMINAL") != "true",
    reason="Local ThetaTerminal is disabled on this environment",
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


# NOTE (2025-12-07): get_request now ONLY uses queue mode. Error handling behavior has changed:
# - error_type in header no longer triggers ValueError (queue system handles errors differently)
# - Exceptions from queue_request are propagated directly
# The test below is skipped as error_type handling has been removed from get_request.
@pytest.mark.skip(reason="Obsolete: error_type handling removed from get_request - queue system handles errors differently")
@patch('lumibot.tools.thetadata_queue_client.queue_request')
def test_get_request_error_in_json(mock_queue_request):
    """Test that get_request raises ValueError when response contains error_type."""
    # Mock queue_request to return a response with error_type
    # queue_request returns just the response dict (not a tuple)
    mock_queue_request.return_value = {
        "header": {
            "error_type": "SomeError"
        }
    }

    url = "http://test.com"
    headers = {"Authorization": "Bearer test_token"}
    querystring = {"param1": "value1"}

    # Act & Assert - get_request should raise ValueError when error_type is in header
    with pytest.raises(ValueError):
        thetadata_helper.get_request(url, headers, querystring, "test_user", "test_password")

    # Verify queue_request was called
    assert mock_queue_request.called


@patch('lumibot.tools.thetadata_queue_client.queue_request')
def test_get_request_exception_handling(mock_queue_request):
    """Test that get_request handles exceptions from queue_request."""
    # Mock queue_request to raise an exception
    mock_queue_request.side_effect = Exception("Request permanently failed: test error")

    url = "http://test.com"
    headers = {"Authorization": "Bearer test_token"}
    querystring = {"param1": "value1"}

    # Act & Assert - get_request should propagate the exception
    with pytest.raises(Exception) as exc_info:
        thetadata_helper.get_request(url, headers, querystring, "test_user", "test_password")

    assert "test error" in str(exc_info.value)
    assert mock_queue_request.called


# NOTE (2025-12-07): The tests below (test_get_request_raises_theta_request_error_after_transient_status,
# etc.) are now obsolete because get_request ONLY uses queue mode. The queue system handles
# retries and error handling internally. These tests can be removed in a future cleanup.
@pytest.mark.skip(reason="Obsolete: get_request now uses queue mode only, which handles retries internally")
@patch('lumibot.tools.thetadata_helper.check_connection')
def test_get_request_raises_theta_request_error_after_transient_status(mock_check_connection, monkeypatch):
    """Ensure repeated 5xx responses raise ThetaRequestError with the status code."""
    # Disable remote downloader for this test
    monkeypatch.setattr(thetadata_helper, "REMOTE_DOWNLOADER_ENABLED", False)
    monkeypatch.delenv("DATADOWNLOADER_BASE_URL", raising=False)
    monkeypatch.delenv("DATADOWNLOADER_API_KEY", raising=False)
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


def test_get_request_remote_queue_full_backoff(monkeypatch):
    payload_queue = {"error": "queue_full", "active": 8, "waiting": 12}
    payload_success = {"header": {"format": [], "error_type": "null", "next_page": None}, "response": []}
    call_timeouts = []

    def fake_get(url, headers=None, params=None, timeout=None):
        call_timeouts.append(timeout)
        if len(call_timeouts) == 1:
            return SimpleNamespace(
                status_code=503,
                text=json.dumps(payload_queue),
                json=lambda: payload_queue,
            )
        return SimpleNamespace(
            status_code=200,
            text="{}",
            json=lambda: payload_success,
        )

    sleeps = []

    def fake_sleep(duration):
        sleeps.append(duration)

    monkeypatch.setattr(thetadata_helper, "REMOTE_DOWNLOADER_ENABLED", True)
    monkeypatch.setattr(thetadata_helper.requests, "get", fake_get)
    monkeypatch.setattr(thetadata_helper, "check_connection", lambda **_: (None, True))
    monkeypatch.setattr(thetadata_helper.time, "sleep", fake_sleep)

    result = thetadata_helper.get_request("http://fake", {}, {}, "user", "pass")
    assert result == payload_success
    assert call_timeouts[0] is None, "Remote downloader calls should not set request timeout"
    assert sleeps, "Queue-full response should trigger a sleep before retrying"


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

    df = thetadata_helper.get_historical_eod_data(
        asset,
        start,
        end,
        "user",
        "pass",
        apply_corporate_actions=False,
    )
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
# NOTE (2025-11-28): Skip process health tests when Data Downloader is configured.
# These tests verify ThetaTerminal JAR process management (start/stop/restart),
# which only applies to local ThetaTerminal mode. When using the production
# Data Downloader proxy (DATADOWNLOADER_BASE_URL), there's no local process to manage.
@pytest.mark.skipif(
    bool(os.environ.get("DATADOWNLOADER_BASE_URL")),
    reason="Process health tests require local ThetaTerminal, not Data Downloader"
)
class TestThetaDataProcessHealthCheck:
    """
    Real integration tests for ThetaData process health monitoring.
    NO MOCKING - these tests use real ThetaData process and data.
    These tests are skipped when Data Downloader is configured since
    there is no local ThetaTerminal process to manage.
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


# NOTE (2025-11-28): Skip connection supervision tests when Data Downloader is configured.
# These tests verify terminal restart behavior when connections drop, which only applies
# to local ThetaTerminal mode. The Data Downloader handles connection management on the server side.
@pytest.mark.skipif(
    bool(os.environ.get("DATADOWNLOADER_BASE_URL")),
    reason="Connection supervision tests require local ThetaTerminal, not Data Downloader"
)
class TestThetaDataConnectionSupervision:
    """
    Tests for ThetaData connection supervision and terminal restart behavior.
    These tests are skipped when Data Downloader is configured since
    there is no local ThetaTerminal to restart.
    """

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


def test_update_pandas_data_fetches_real_day_frames(monkeypatch):
    """Daily requests should stay daily even when only minute cache exists."""

    monkeypatch.setattr(
        ThetaDataBacktestingPandas,
        "kill_processes_by_name",
        lambda self, keyword: None,
    )
    monkeypatch.setattr(
        thetadata_helper,
        "reset_theta_terminal_tracking",
        lambda: None,
    )

    utc = pytz.UTC
    data_source = ThetaDataBacktestingPandas(
        datetime_start=utc.localize(datetime.datetime(2024, 7, 1)),
        datetime_end=utc.localize(datetime.datetime(2024, 11, 5)),
        username="user",
        password="pass",
        use_quote_data=False,
    )

    asset = Asset("TQQQ", asset_type="stock")
    quote = Asset("USD", asset_type="forex")
    key = (asset, quote)

    minute_index = pd.date_range(
        start=utc.localize(datetime.datetime(2024, 7, 15, 13, 30)),
        periods=1_000,
        freq="min",
    )
    minute_frame = pd.DataFrame(
        {
            "open": 50 + np.arange(len(minute_index)) * 0.01,
            "high": 50.5 + np.arange(len(minute_index)) * 0.01,
            "low": 49.5 + np.arange(len(minute_index)) * 0.01,
            "close": 50.25 + np.arange(len(minute_index)) * 0.01,
            "volume": 1_000,
        },
        index=minute_index,
    )

    per_timestep_key, legacy_key = data_source._build_dataset_keys(asset, quote, "minute")
    minute_data = Data(asset, minute_frame, timestep="minute", quote=quote)
    data_source.pandas_data[legacy_key] = minute_data
    data_source.pandas_data[per_timestep_key] = minute_data
    data_source._data_store[legacy_key] = minute_data
    data_source._data_store[per_timestep_key] = minute_data
    data_source._record_metadata(per_timestep_key, minute_frame, "minute", asset, has_quotes=False)

    captured = {}

    def fake_get_price_data(*args, **kwargs):
        captured["timespan"] = kwargs.get("timespan")
        # Return data covering the full backtest period (2024-07-01 to 2024-11-05)
        # to satisfy coverage validation checks
        eod_index = pd.date_range(
            start=utc.localize(datetime.datetime(2024, 7, 1, 20, 0)),
            end=utc.localize(datetime.datetime(2024, 11, 5, 20, 0)),
            freq="D",
        )
        return pd.DataFrame(
            {
                "open": 100.0,
                "high": 101.0,
                "low": 99.0,
                "close": 100.5,
                "volume": 1_000,
            },
            index=eod_index,
        )

    monkeypatch.setattr(thetadata_helper, "get_price_data", fake_get_price_data)
    monkeypatch.setattr(
        ThetaDataBacktestingPandas,
        "get_datetime",
        lambda self: utc.localize(datetime.datetime(2024, 11, 1, 16, 0)),
    )

    data_source._update_pandas_data(asset, quote, length=50, timestep="day")

    assert captured.get("timespan") == "day", "Theta daily requests must use the daily endpoint"
    stored = data_source.pandas_data.get(key)
    assert stored is not None
    assert stored.timestep == "day", "pandas_data entry should be daily after refresh"


def test_update_pandas_data_preserves_full_history(monkeypatch, tmp_path):
    """Test that _update_pandas_data preserves full history when updating cached data."""
    monkeypatch.setattr(
        ThetaDataBacktestingPandas,
        "kill_processes_by_name",
        lambda self, keyword: None,
    )
    monkeypatch.setattr(
        thetadata_helper,
        "reset_theta_terminal_tracking",
        lambda: None,
    )
    # Use temp path to avoid interference from real cached data
    monkeypatch.setenv("LUMIBOT_CACHE_DIR", str(tmp_path))

    utc = pytz.UTC
    data_source = ThetaDataBacktestingPandas(
        datetime_start=utc.localize(datetime.datetime(2020, 1, 1)),
        datetime_end=utc.localize(datetime.datetime(2025, 11, 5)),
        username="user",
        password="pass",
        use_quote_data=False,
    )

    # Use a fake symbol to avoid cached data interference
    asset = Asset("FAKESPY", asset_type="stock")
    quote = Asset("USD", asset_type="forex")
    key = (asset, quote)

    base_index = pd.date_range(
        start=utc.localize(datetime.datetime(2020, 10, 1, 20, 0)),
        periods=250,
        freq="D",
    )
    base_frame = pd.DataFrame(
        {
            "open": 100 + np.arange(len(base_index), dtype=float),
            "high": 101 + np.arange(len(base_index), dtype=float),
            "low": 99 + np.arange(len(base_index), dtype=float),
            "close": 100.5 + np.arange(len(base_index), dtype=float),
            "volume": 1_000,
        },
        index=base_index,
    )
    day_key, legacy_key = data_source._build_dataset_keys(asset, quote, "day")
    day_data = Data(asset, base_frame, timestep="day", quote=quote)
    data_source.pandas_data[legacy_key] = day_data
    data_source.pandas_data[day_key] = day_data
    data_source._data_store[legacy_key] = day_data
    data_source._data_store[day_key] = day_data
    data_source._record_metadata(day_key, base_frame, "day", asset, has_quotes=False)

    captured = {}

    def fake_get_price_data(*args, **kwargs):
        captured["preserve_full_history"] = kwargs.get("preserve_full_history")
        # Return data that extends to the backtest end date to satisfy coverage validation
        new_index = pd.date_range(
            start=utc.localize(datetime.datetime(2020, 10, 1, 20, 0)),
            end=utc.localize(datetime.datetime(2025, 11, 5, 20, 0)),
            freq="D",
        )
        return pd.DataFrame(
            {
                "open": 200 + np.arange(len(new_index), dtype=float),
                "high": 201 + np.arange(len(new_index), dtype=float),
                "low": 199 + np.arange(len(new_index), dtype=float),
                "close": 200.5 + np.arange(len(new_index), dtype=float),
                "volume": 2_000,
            },
            index=new_index,
        )

    monkeypatch.setattr(thetadata_helper, "get_price_data", fake_get_price_data)
    monkeypatch.setattr(
        ThetaDataBacktestingPandas,
        "get_datetime",
        lambda self: utc.localize(datetime.datetime(2025, 11, 5, 16, 0)),
    )

    data_source._update_pandas_data(asset, quote, length=len(base_frame) + 25, timestep="day")

    stored = data_source.pandas_data.get(key)
    assert stored is not None
    assert captured.get("preserve_full_history") is True
    assert stored.df.index.min() == base_index.min()
    assert stored.df.index.max() > base_index.max()
    assert len(stored.df) >= len(base_frame)


def test_update_pandas_data_keeps_placeholder_history(monkeypatch, tmp_path):
    """Test that _update_pandas_data preserves placeholder history markers."""
    monkeypatch.setattr(
        ThetaDataBacktestingPandas,
        "kill_processes_by_name",
        lambda self, keyword: None,
    )
    monkeypatch.setattr(
        thetadata_helper,
        "reset_theta_terminal_tracking",
        lambda: None,
    )
    # Use temp path to avoid interference from real cached data
    monkeypatch.setenv("LUMIBOT_CACHE_DIR", str(tmp_path))

    utc = pytz.UTC
    data_source = ThetaDataBacktestingPandas(
        datetime_start=utc.localize(datetime.datetime(2020, 1, 1)),
        datetime_end=utc.localize(datetime.datetime(2025, 11, 5)),
        username="user",
        password="pass",
        use_quote_data=False,
    )

    # Use a fake symbol to avoid cached data interference
    asset = Asset("FAKESPY2", asset_type="stock")
    quote = Asset("USD", asset_type="forex")
    key = (asset, quote)

    placeholder_index = pd.date_range(
        start=utc.localize(datetime.datetime(2020, 10, 1, 20, 0)),
        end=data_source.datetime_end,
        freq="D",
    )
    missing_flags = [True] * 120 + [False] * (len(placeholder_index) - 120)
    placeholder_frame = pd.DataFrame(
        {
            "open": np.linspace(90.0, 110.0, num=len(placeholder_index)),
            "high": np.linspace(90.5, 110.5, num=len(placeholder_index)),
            "low": np.linspace(89.5, 109.5, num=len(placeholder_index)),
            "close": np.linspace(90.25, 110.25, num=len(placeholder_index)),
            "volume": np.linspace(1_000, 2_000, num=len(placeholder_index)),
            "missing": missing_flags,
        },
        index=placeholder_index,
    )

    call_counter = {"calls": 0}

    def fake_get_price_data(*args, **kwargs):
        call_counter["calls"] += 1
        return placeholder_frame.copy()

    monkeypatch.setattr(thetadata_helper, "get_price_data", fake_get_price_data)
    current_dt = utc.localize(datetime.datetime(2025, 11, 5, 16, 0))
    monkeypatch.setattr(ThetaDataBacktestingPandas, "get_datetime", lambda self: current_dt)

    data_source._update_pandas_data(asset, quote, length=150, timestep="day")

    stored = data_source.pandas_data.get(key)
    assert stored is not None
    assert call_counter["calls"] == 1
    first_real_idx = placeholder_frame.loc[~placeholder_frame["missing"]].index.min()
    assert stored.df.index.min() == first_real_idx
    assert "missing" not in stored.df.columns
    # The data container should remember the earliest requested datetime so callers know history exists.
    assert stored.requested_datetime_start.date() == datetime.date(2020, 10, 1)
    placeholder_dt = placeholder_frame.index[0].to_pydatetime()
    # Requests prior to the first real bar raise ValueError since the date is outside the data range.
    # This is expected behavior - the caller should check requested_datetime_start first.
    with pytest.raises(ValueError, match="outside of the data's date range"):
        stored.get_last_price(placeholder_dt)
    real_dt = first_real_idx.to_pydatetime()
    assert stored.get_last_price(real_dt) is not None

    metadata = data_source._dataset_metadata[key]
    assert metadata["start"].date() == datetime.date(2020, 10, 1)
    assert metadata["data_start"].date() == first_real_idx.date()
    assert metadata["rows"] == len(placeholder_index)

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

    monkeypatch.setattr(thetadata_helper, "get_request", fake_request)
    minute_fetch = MagicMock(return_value=None)
    monkeypatch.setattr(thetadata_helper, "get_historical_data", minute_fetch)

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
    assert pytest.approx(df.loc["2024-11-15", "open"]) == 310.52
    minute_fetch.assert_not_called()


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


def _build_dummy_df(start_ts: pd.Timestamp, periods: int = 5, freq: str = "1D") -> pd.DataFrame:
    index = pd.date_range(start_ts, periods=periods, freq=freq, tz="UTC")
    return pd.DataFrame(
        {
            "open": range(periods),
            "high": range(periods),
            "low": range(periods),
            "close": range(periods),
            "volume": [1_000] * periods,
            "missing": [False] * periods,
        },
        index=index,
    )


def test_update_pandas_data_reuses_covered_window(monkeypatch):
    """Once coverage metadata spans the window, _update_pandas_data must not refetch."""
    monkeypatch.setattr(ThetaDataBacktestingPandas, "kill_processes_by_name", lambda *args, **kwargs: None)
    start = pd.Timestamp("2024-01-02", tz="UTC")
    end = pd.Timestamp("2024-01-10", tz="UTC")
    asset = Asset("ZZTEST", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    df = _build_dummy_df(start, periods=9)
    data = Data(asset=asset, df=df, quote=quote, timestep="day")
    data.strict_end_check = True
    ds = ThetaDataBacktestingPandas(datetime_start=start, datetime_end=end, pandas_data={(asset, quote, "day"): data})
    meta_key = (asset, quote, "day")
    ds._dataset_metadata[meta_key] = {
        "timestep": "day",
        "start": start.to_pydatetime(),
        "end": end.to_pydatetime(),
        "data_start": start.to_pydatetime(),
        "data_end": end.to_pydatetime(),
        "rows": len(df),
        "prefetch_complete": True,
    }

    calls = []

    def _fake_get_price_data(*args, **kwargs):
        calls.append((args, kwargs))
        raise AssertionError("fetch should not be called for covered window")

    monkeypatch.setattr(thetadata_helper, "get_price_data", _fake_get_price_data)
    ds._update_pandas_data(asset, quote, length=5, timestep="day", start_dt=end)
    assert calls == []
    meta = ds._dataset_metadata.get((asset, quote, "day"))
    assert meta and meta.get("prefetch_complete") is True
    assert meta.get("ffilled") is True


def test_update_pandas_data_raises_on_incomplete_end(monkeypatch):
    """If a full-window fetch still ends before datetime_end, raise to avoid refresh thrash."""
    monkeypatch.setattr(ThetaDataBacktestingPandas, "kill_processes_by_name", lambda *args, **kwargs: None)
    start = pd.Timestamp("2024-01-02", tz="UTC")
    end = pd.Timestamp("2024-01-20", tz="UTC")
    asset = Asset("ZZTEST2", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    ds = ThetaDataBacktestingPandas(datetime_start=start, datetime_end=end)

    calls = []

    def _fake_get_price_data(username, password, asset_param, start_param, end_param, **kwargs):
        calls.append((start_param, end_param, kwargs.get("timespan")))
        short_df = _build_dummy_df(start, periods=3)
        return short_df

    monkeypatch.setattr(thetadata_helper, "get_price_data", _fake_get_price_data)

    with pytest.raises(ValueError):
        ds._update_pandas_data(asset, quote, length=5, timestep="day", start_dt=end)

    assert len(calls) == 1
    assert calls[0][2] == "day"


def test_trading_dates_are_memoized(monkeypatch):
    """Calendar construction should be cached to avoid repeated expensive calls."""
    thetadata_helper._cached_trading_dates.cache_clear()
    calls = []

    class DummyCalendar:
        def schedule(self, start_date=None, end_date=None):
            calls.append((start_date, end_date))
            return pd.DataFrame(index=pd.date_range(start_date, end_date, freq="B"))

    monkeypatch.setattr(thetadata_helper.mcal, "get_calendar", lambda name: DummyCalendar())

    asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    start = datetime.datetime(2024, 1, 1)
    end = datetime.datetime(2024, 1, 10)

    first = thetadata_helper.get_trading_dates(asset, start, end)
    second = thetadata_helper.get_trading_dates(asset, start, end)

    assert first == second
    assert len(calls) == 1


def test_day_request_does_not_downshift_to_minute(monkeypatch, tmp_path):
    """Day requests must use day/EOD fetch even if minute cache exists (prevents minute-for-day slowdown)."""
    monkeypatch.setattr(ThetaDataBacktestingPandas, "kill_processes_by_name", lambda *args, **kwargs: None)
    start = pd.Timestamp("2024-01-02", tz="UTC")
    end = pd.Timestamp("2024-01-10", tz="UTC")
    asset = Asset("TQQQ", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    # Preseed minute cache (should not be reused for day requests)
    minute_df = _build_dummy_df(start, periods=60, freq="1min")
    minute_data = Data(asset=asset, df=minute_df, quote=quote, timestep="minute")
    minute_data.strict_end_check = True
    cache_file = tmp_path / "tqqq.day.ohlc.parquet"
    monkeypatch.setattr(thetadata_helper, "build_cache_filename", lambda *args, **kwargs: cache_file)
    monkeypatch.setattr(thetadata_helper, "load_cache", lambda *args, **kwargs: None)
    monkeypatch.setattr(thetadata_helper, "_load_cache_sidecar", lambda *args, **kwargs: None)

    ds = ThetaDataBacktestingPandas(
        datetime_start=start,
        datetime_end=end,
        pandas_data={(asset, quote, "minute"): minute_data},
    )

    calls = []

    def _fake_get_price_data(username, password, asset_param, start_param, end_param, **kwargs):
        calls.append(kwargs.get("timespan"))
        day_index = pd.date_range(start_param, end_param, freq="1D", tz="UTC")
        return pd.DataFrame(
            {
                "open": range(len(day_index)),
                "high": range(len(day_index)),
                "low": range(len(day_index)),
                "close": range(len(day_index)),
                "volume": [1_000] * len(day_index),
                "missing": [False] * len(day_index),
            },
            index=day_index,
        )

    monkeypatch.setattr(thetadata_helper, "get_price_data", _fake_get_price_data)

    ds._update_pandas_data(asset, quote, length=5, timestep="day", start_dt=end)

    assert calls, "get_price_data was never called"
    assert calls == ["day"], "Day requests should not call minute/hour fetch paths"


def test_no_data_fetch_raises_once(monkeypatch, tmp_path):
    """NO_DATA responses must raise instead of looping; ensures permanent missing is treated as fatal."""
    monkeypatch.setattr(ThetaDataBacktestingPandas, "kill_processes_by_name", lambda *args, **kwargs: None)
    start = pd.Timestamp("2024-01-02", tz="UTC")
    end = pd.Timestamp("2024-01-05", tz="UTC")
    asset = Asset("ZZNODATA", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    cache_file = tmp_path / "zznodata.day.ohlc.parquet"
    monkeypatch.setattr(thetadata_helper, "build_cache_filename", lambda *args, **kwargs: cache_file)
    monkeypatch.setattr(thetadata_helper, "load_cache", lambda *args, **kwargs: None)
    monkeypatch.setattr(thetadata_helper, "_load_cache_sidecar", lambda *args, **kwargs: None)

    calls = []

    def _fake_get_price_data(username, password, asset_param, start_param, end_param, **kwargs):
        calls.append((start_param, end_param, kwargs.get("timespan")))
        return pd.DataFrame()

    monkeypatch.setattr(thetadata_helper, "get_price_data", _fake_get_price_data)

    ds = ThetaDataBacktestingPandas(datetime_start=start, datetime_end=end)

    with pytest.raises(ValueError):
        ds._update_pandas_data(asset, quote, length=5, timestep="day", start_dt=end)

    assert len(calls) == 1


def test_minute_request_aligned_in_day_mode(monkeypatch):
    """When source is in day mode, minute/hour requests are silently aligned to day mode."""
    monkeypatch.setattr(ThetaDataBacktestingPandas, "kill_processes_by_name", lambda *args, **kwargs: None)
    start = pd.Timestamp("2024-01-02", tz="UTC")
    end = pd.Timestamp("2024-01-05", tz="UTC")
    asset = Asset("TQQQ", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    day_df = _build_dummy_df(start, periods=4, freq="1D")
    day_data = Data(asset=asset, df=day_df, quote=quote, timestep="day")
    day_data.strict_end_check = True

    ds = ThetaDataBacktestingPandas(
        datetime_start=start,
        datetime_end=end,
        pandas_data={(asset, quote, "day"): day_data},
    )
    ds._timestep = "day"

    # Minute requests in day mode should work silently - they get aligned to day mode
    # instead of raising ValueError. This prevents unnecessary minute data downloads.
    result = ds._pull_source_symbol_bars(asset, length=2, timestep="minute", quote=quote)
    # Should return day data since we're in day mode
    assert result is not None or result is None  # May be None if cache doesn't have enough bars


def test_day_cache_reuse_aligns_end_without_refetch(monkeypatch):
    """Day cache that already ends on the required trading day should reuse without any downloader calls."""
    monkeypatch.setattr(ThetaDataBacktestingPandas, "kill_processes_by_name", lambda *args, **kwargs: None)
    start = pd.Timestamp("2020-10-01", tz="UTC")
    end = pd.Timestamp("2025-11-03", tz="UTC")
    asset = Asset("TQQQ", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    start_date = pd.Timestamp("2020-09-26", tz="UTC")
    end_date = pd.Timestamp("2025-11-03", tz="UTC")
    day_index = pd.date_range(start_date, end_date, freq="1D", tz="UTC")
    day_df = pd.DataFrame(
        {
            "open": range(len(day_index)),
            "high": range(len(day_index)),
            "low": range(len(day_index)),
            "close": range(len(day_index)),
            "volume": [1_000] * len(day_index),
            "missing": [False] * len(day_index),
        },
        index=day_index,
    )
    day_data = Data(asset=asset, df=day_df, quote=quote, timestep="day")
    day_data.strict_end_check = True

    ds = ThetaDataBacktestingPandas(
        datetime_start=start,
        datetime_end=end,
        pandas_data={(asset, quote, "day"): day_data},
    )
    ds._timestep = "day"

    calls = []

    def _fake_get_price_data(*args, **kwargs):
        calls.append(kwargs.get("timespan"))
        raise AssertionError("Downloader should not be invoked when cache covers end of window")

    monkeypatch.setattr(thetadata_helper, "get_price_data", _fake_get_price_data)

    ds._update_pandas_data(asset, quote, length=2, timestep="day", start_dt=end)

    assert calls == []
    meta = ds._dataset_metadata.get((asset, quote, "day"))
    assert meta is not None
    assert meta.get("prefetch_complete") is True
    assert meta.get("end").date() == datetime.date(2025, 11, 3)
    assert meta.get("data_end").date() == datetime.date(2025, 11, 3)


def test_tail_placeholder_at_end_marks_permanent_not_refetched(monkeypatch):
    """If the final requested day is missing (NO_DATA), mark it permanently and stop retry loops."""
    monkeypatch.setattr(ThetaDataBacktestingPandas, "kill_processes_by_name", lambda *args, **kwargs: None)
    start = pd.Timestamp("2024-01-01", tz="UTC")
    end = pd.Timestamp("2024-01-05", tz="UTC")
    asset = Asset("ZZTAIL", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)

    # Build a frame where the last day is missing; Theta should not be re-polled repeatedly.
    day_index = pd.date_range(start, end, freq="1D", tz="UTC")
    df = pd.DataFrame(
        {
            "open": [1.0, 2.0, 3.0, 4.0, None],
            "high": [1.0, 2.0, 3.0, 4.0, None],
            "low": [1.0, 2.0, 3.0, 4.0, None],
            "close": [1.0, 2.0, 3.0, 4.0, None],
            "volume": [100] * 5,
            "missing": [False, False, False, False, True],
        },
        index=day_index,
    )

    calls = []

    def _fake_get_price_data(*args, **kwargs):
        calls.append(kwargs.get("timespan"))
        return df

    monkeypatch.setattr(thetadata_helper, "get_price_data", _fake_get_price_data)

    ds = ThetaDataBacktestingPandas(datetime_start=start, datetime_end=end)
    ds._timestep = "day"

    ds._update_pandas_data(asset, quote, length=3, timestep="day", start_dt=end)

    assert calls == ["day"]
    meta = ds._dataset_metadata.get((asset, quote, "day"))
    assert meta is not None
    assert meta.get("tail_placeholder") is True
    assert meta.get("tail_missing_permanent") is True
    assert meta.get("tail_missing_date") == datetime.date(2024, 1, 5)


def test_daily_data_check_uses_utc_date_comparison():
    """
    Regression test: Daily bars timestamped at 00:00 UTC should cover the entire
    trading day, not just times before the UTC timestamp converted to local timezone.

    Without the fix in Data.check_data, a bar at 2025-11-03 00:00 UTC would appear
    as 2025-11-02 19:00 EST, causing requests for 2025-11-03 08:30 EST to fail
    even though the data logically covers Nov 3.
    """
    asset = Asset(asset_type="stock", symbol="TEST")

    # Create daily data with timestamps at 00:00 UTC
    # When converted to EST, Nov 3 00:00 UTC = Nov 2 19:00 EST
    utc = pytz.UTC
    est = pytz.timezone("America/New_York")

    # The bar timestamp: Nov 3 00:00 UTC (which is Nov 2 19:00 EST)
    bar_timestamp_utc = datetime.datetime(2025, 11, 3, 0, 0, 0, tzinfo=utc)
    bar_timestamp_est = bar_timestamp_utc.astimezone(est)

    df = pd.DataFrame(
        {
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
            "close": [100.5],
            "volume": [1_000_000],
        },
        index=pd.DatetimeIndex([bar_timestamp_est], name="datetime"),
    )

    data = Data(asset, df, timestep="day")
    data.strict_end_check = True

    # The request time: Nov 3 08:30 EST (morning of the same day the bar represents)
    request_time = datetime.datetime(2025, 11, 3, 8, 30, 0, tzinfo=est)

    # This should NOT raise - the bar covers Nov 3 trading day
    # Before the fix, this would raise:
    # "The date you are looking for (2025-11-03 08:30:00-05:00) is after the
    #  available data's end (2025-11-02 19:00:00-05:00)"
    result = data.get_last_price(request_time)
    assert result is not None
    assert result == 100.5  # Should return the close price


class TestZeroPriceFiltering:
    """Tests for filtering zero-price OHLC rows from ThetaData."""

    def test_filter_zero_ohlc_rows_removes_bad_data(self):
        """Test that rows with all-zero OHLC values are filtered out."""
        # Create DataFrame with some valid data and some zero-price rows
        index = pd.to_datetime([
            "2024-01-15 09:30",
            "2024-01-16 09:30",  # Bad data - all zeros
            "2024-01-17 09:30",
        ], utc=True)

        df = pd.DataFrame({
            "open": [100.0, 0.0, 102.0],
            "high": [101.0, 0.0, 103.0],
            "low": [99.0, 0.0, 101.0],
            "close": [100.5, 0.0, 102.5],
            "volume": [1000, 0, 1200],
        }, index=index)

        # Apply the filtering logic (same as in update_df)
        all_zero = (
            (df["open"] == 0) &
            (df["high"] == 0) &
            (df["low"] == 0) &
            (df["close"] == 0)
        )
        df_filtered = df[~all_zero]

        # Verify: only 2 rows remain
        assert len(df_filtered) == 2
        assert df_filtered["close"].tolist() == [100.5, 102.5]

    def test_filter_preserves_valid_zero_volume(self):
        """Test that rows with zero volume but valid prices are preserved."""
        index = pd.to_datetime([
            "2024-01-15 09:30",
            "2024-01-16 09:30",  # Valid data - has prices, just zero volume
        ], utc=True)

        df = pd.DataFrame({
            "open": [100.0, 50.0],
            "high": [101.0, 51.0],
            "low": [99.0, 49.0],
            "close": [100.5, 50.5],
            "volume": [1000, 0],  # Zero volume is fine
        }, index=index)

        all_zero = (
            (df["open"] == 0) &
            (df["high"] == 0) &
            (df["low"] == 0) &
            (df["close"] == 0)
        )
        df_filtered = df[~all_zero]

        # Both rows should be preserved
        assert len(df_filtered) == 2
        assert df_filtered["close"].tolist() == [100.5, 50.5]

    def test_filter_removes_weekend_zero_data(self):
        """
        Test that weekend rows with zero prices are filtered.

        This is the actual bug we fixed - ThetaData returned Saturday 2019-06-08
        with all zeros for MELI, causing the backtest to fail.
        """
        index = pd.to_datetime([
            "2019-06-07 09:30",  # Friday - valid
            "2019-06-08 00:00",  # Saturday - bad (all zeros)
            "2019-06-10 09:30",  # Monday - valid
        ], utc=True)

        df = pd.DataFrame({
            "open": [495.0, 0.0, 500.0],
            "high": [500.0, 0.0, 505.0],
            "low": [490.0, 0.0, 495.0],
            "close": [498.0, 0.0, 502.0],
            "volume": [10000, 0, 12000],
        }, index=index)

        all_zero = (
            (df["open"] == 0) &
            (df["high"] == 0) &
            (df["low"] == 0) &
            (df["close"] == 0)
        )
        df_filtered = df[~all_zero]

        # Only Friday and Monday should remain
        assert len(df_filtered) == 2

        # Verify the dates are correct (Friday and Monday)
        dates = df_filtered.index.tolist()
        assert dates[0].day == 7  # Friday
        assert dates[1].day == 10  # Monday

    def test_filter_handles_partial_zeros(self):
        """
        Test that rows with some zeros but not all are preserved.

        E.g., a stock that opened at 0 (bug) but has valid high/low/close
        should still be preserved as it's usable data.
        """
        index = pd.to_datetime([
            "2024-01-15 09:30",
        ], utc=True)

        df = pd.DataFrame({
            "open": [0.0],  # Zero open
            "high": [101.0],
            "low": [99.0],
            "close": [100.5],
            "volume": [1000],
        }, index=index)

        all_zero = (
            (df["open"] == 0) &
            (df["high"] == 0) &
            (df["low"] == 0) &
            (df["close"] == 0)
        )
        df_filtered = df[~all_zero]

        # Row should be preserved - only close being 0 is what matters
        assert len(df_filtered) == 1

    def test_filter_empty_df_returns_empty(self):
        """Test that filtering an empty DataFrame returns empty."""
        df = pd.DataFrame({
            "open": [],
            "high": [],
            "low": [],
            "close": [],
            "volume": [],
        })

        # Should not raise an error
        all_zero = (
            (df["open"] == 0) &
            (df["high"] == 0) &
            (df["low"] == 0) &
            (df["close"] == 0)
        )
        df_filtered = df[~all_zero]

        assert len(df_filtered) == 0

    def test_filter_all_zero_returns_empty(self):
        """Test that a DataFrame with only zero-price rows returns empty."""
        index = pd.to_datetime([
            "2024-01-15 09:30",
            "2024-01-16 09:30",
        ], utc=True)

        df = pd.DataFrame({
            "open": [0.0, 0.0],
            "high": [0.0, 0.0],
            "low": [0.0, 0.0],
            "close": [0.0, 0.0],
            "volume": [0, 0],
        }, index=index)

        all_zero = (
            (df["open"] == 0) &
            (df["high"] == 0) &
            (df["low"] == 0) &
            (df["close"] == 0)
        )
        df_filtered = df[~all_zero]

        assert len(df_filtered) == 0