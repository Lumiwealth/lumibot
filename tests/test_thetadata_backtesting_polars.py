import logging
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import polars as pl
import pytest

from lumibot.backtesting.thetadata_backtesting_polars import ThetaDataBacktestingPolars
from lumibot.entities import Asset
from lumibot.tools import thetadata_helper


def _ohlc_frame(start: datetime, rows: int = 8) -> pd.DataFrame:
    index = pd.date_range(start=start, periods=rows, freq="1min", tz="UTC")
    data = {
        "open": [200 + i for i in range(rows)],
        "high": [201 + i for i in range(rows)],
        "low": [199 + i for i in range(rows)],
        "close": [200.5 + i for i in range(rows)],
        "volume": [10_000 + i * 100 for i in range(rows)],
    }
    return pd.DataFrame(data, index=index)


def _quote_frame(start: datetime, rows: int = 8) -> pd.DataFrame:
    index = pd.date_range(start=start, periods=rows, freq="1min", tz="UTC")
    data = {
        "bid": [200.1 + i * 0.1 for i in range(rows)],
        "ask": [200.3 + i * 0.1 for i in range(rows)],
        "bid_size": [50 + i for i in range(rows)],
        "ask_size": [60 + i for i in range(rows)],
    }
    return pd.DataFrame(data, index=index)


def _ohlc_day_frame(start: datetime, rows: int = 140) -> pd.DataFrame:
    index = pd.date_range(start=start, periods=rows, freq="1D", tz="UTC")
    data = {
        "open": [120 + i for i in range(rows)],
        "high": [121 + i for i in range(rows)],
        "low": [119 + i for i in range(rows)],
        "close": [120.5 + i for i in range(rows)],
        "volume": [8_000 + i * 80 for i in range(rows)],
    }
    return pd.DataFrame(data, index=index)


def _ohlc_minute_with_trailing_nans(start: datetime) -> pd.DataFrame:
    index = pd.date_range(start=start, periods=5, freq="1min", tz="UTC")
    data = {
        "open": [420, 421, 422, 423, 424],
        "high": [421, 422, 423, 424, 425],
        "low": [419, 420, 421, 422, 423],
        "close": [420.5, 421.5, float("nan"), float("nan"), float("nan")],
        "volume": [70_000 + i * 120 for i in range(5)],
    }
    return pd.DataFrame(data, index=index)


def test_theta_polars_historical_prices(monkeypatch):
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(days=1)

    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_polars.subprocess.run",
        lambda *args, **kwargs: type("Result", (), {"stdout": ""})(),
    )

    def fake_get_price_data(username, password, asset, start_datetime, end_datetime, timespan, quote_asset, dt, datastyle, include_after_hours):
        if datastyle == "quote":
            return _quote_frame(start_datetime)
        return _ohlc_frame(start_datetime)

    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_polars.thetadata_helper.get_price_data",
        fake_get_price_data,
    )
    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_polars.thetadata_helper.get_chains_cached",
        lambda **kwargs: {"Multiplier": 100, "Chains": {}},
    )

    backtester = ThetaDataBacktestingPolars(
        datetime_start=start,
        datetime_end=end,
        username="demo",
        password="demo",
        show_progress_bar=False,
    )

    asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    bars = backtester.get_historical_prices(asset, length=4, timestep="minute", return_polars=True)

    assert bars is not None
    assert isinstance(bars.polars_df, pl.DataFrame)
    assert bars.polars_df.height == 4

    chains = backtester.get_chains(asset)
    assert "Multiplier" in chains


def test_theta_polars_minute_slice_no_forward_shift(monkeypatch):
    start = datetime(2025, 3, 1, 14, 30, tzinfo=timezone.utc)
    end = start + timedelta(days=1)

    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_polars.subprocess.run",
        lambda *args, **kwargs: type("Result", (), {"stdout": ""})(),
    )
    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_polars.thetadata_helper.get_chains_cached",
        lambda **kwargs: {"Multiplier": 100, "Chains": {}},
    )

    def fake_get_price_data(username, password, asset, start_datetime, end_datetime, timespan, quote_asset, dt, datastyle, include_after_hours):
        if datastyle == "quote":
            return _quote_frame(start_datetime, rows=10000)
        return _ohlc_frame(start_datetime, rows=10000)

    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_polars.thetadata_helper.get_price_data",
        fake_get_price_data,
    )

    backtester = ThetaDataBacktestingPolars(
        datetime_start=start,
        datetime_end=end,
        username="demo",
        password="demo",
        show_progress_bar=False,
    )

    asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    target_dt = start + timedelta(minutes=20)
    backtester._update_datetime(target_dt)

    bars = backtester.get_historical_prices(asset, length=5, timestep="minute", return_polars=True)
    assert bars is not None

    ordered = bars.polars_df.sort("datetime")["datetime"].to_list()
    assert len(ordered) == 5
    expected_last_dt = backtester.to_default_timezone(target_dt) - timedelta(minutes=1)
    assert ordered[-1] == expected_last_dt
    assert ordered[0] == expected_last_dt - timedelta(minutes=4)


def test_theta_missing_data_cached(monkeypatch, tmp_path):
    start = datetime(2025, 1, 2, tzinfo=timezone.utc)
    end = start + timedelta(days=1)

    monkeypatch.setattr(
        "lumibot.tools.thetadata_helper.LUMIBOT_CACHE_FOLDER",
        str(tmp_path),
    )
    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_polars.subprocess.run",
        lambda *args, **kwargs: type("Result", (), {"stdout": ""})(),
    )
    monkeypatch.setattr(
        "lumibot.tools.thetadata_helper.start_theta_data_client",
        lambda username, password: None,
    )
    monkeypatch.setattr(
        "lumibot.tools.thetadata_helper.is_process_alive",
        lambda: True,
    )
    monkeypatch.setattr(
        "lumibot.tools.thetadata_helper.check_connection",
        lambda username, password: (None, True),
    )

    fetch_calls = {"count": 0}

    def fake_get_historical_data(*args, **kwargs):
        fetch_calls["count"] += 1
        return None

    monkeypatch.setattr(
        "lumibot.tools.thetadata_helper.get_historical_data",
        fake_get_historical_data,
    )
    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_polars.thetadata_helper.get_price_data",
        thetadata_helper.get_price_data,
    )
    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_polars.thetadata_helper.get_chains_cached",
        lambda **kwargs: {"Multiplier": 100, "Chains": {}},
    )

    asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)

    # First request (OHLC) should hit ThetaData and store placeholders.
    first = thetadata_helper.get_price_data(
        username="demo",
        password="demo",
        asset=asset,
        start=start,
        end=end,
        timespan="minute",
        quote_asset=None,
        dt=None,
        datastyle="ohlc",
        include_after_hours=True,
    )
    assert fetch_calls["count"] == 1
    assert first is None or getattr(first, "empty", False)

    cache_file = thetadata_helper.build_cache_filename(asset, "minute", "ohlc")
    cache_df = thetadata_helper.load_cache(cache_file)
    assert "missing" in cache_df.columns, "missing flag column not persisted"
    assert cache_df["missing"].any(), "missing placeholder not stored"

    # Second request for the same range must be served from cache without new fetches.
    second = thetadata_helper.get_price_data(
        username="demo",
        password="demo",
        asset=asset,
        start=start,
        end=end,
        timespan="minute",
        quote_asset=None,
        dt=None,
        datastyle="ohlc",
        include_after_hours=True,
    )
    assert fetch_calls["count"] == 1, "ThetaData was re-queried for cached-miss OHLC data"
    assert second is None or getattr(second, "empty", False)

    # Repeat the same workflow for quote data to ensure placeholders work there too.
    third = thetadata_helper.get_price_data(
        username="demo",
        password="demo",
        asset=asset,
        start=start,
        end=end,
        timespan="minute",
        quote_asset=None,
        dt=None,
        datastyle="quote",
        include_after_hours=True,
    )
    assert fetch_calls["count"] == 2
    assert third is None or getattr(third, "empty", False)

    quote_cache_file = thetadata_helper.build_cache_filename(asset, "minute", "quote")
    quote_cache_df = thetadata_helper.load_cache(quote_cache_file)
    assert "missing" in quote_cache_df.columns, "missing flag missing for quote cache"
    assert quote_cache_df["missing"].any(), "quote cache placeholder not stored"

    fourth = thetadata_helper.get_price_data(
        username="demo",
        password="demo",
        asset=asset,
        start=start,
        end=end,
        timespan="minute",
        quote_asset=None,
        dt=None,
        datastyle="quote",
        include_after_hours=True,
    )
    assert fetch_calls["count"] == 2, "ThetaData was re-queried for cached-miss quote data"
    assert fourth is None or getattr(fourth, "empty", False)


def test_theta_polars_quote_failure_stores_ohlc(monkeypatch):
    start = datetime(2025, 3, 1, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    counts = {"ohlc": 0, "quote": 0}

    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_polars.subprocess.run",
        lambda *args, **kwargs: type("Result", (), {"stdout": ""})(),
    )
    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_polars.thetadata_helper.get_chains_cached",
        lambda **kwargs: {"Multiplier": 100, "Chains": {}},
    )

    def fake_get_price_data(username, password, asset, start_datetime, end_datetime, timespan, quote_asset, dt, datastyle, include_after_hours):
        if datastyle == "quote":
            counts["quote"] += 1
            raise ValueError("Cannot connect to Theta Data!")
        counts["ohlc"] += 1
        return _ohlc_frame(start_datetime, rows=16)

    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_polars.thetadata_helper.get_price_data",
        fake_get_price_data,
    )

    backtester = ThetaDataBacktestingPolars(
        datetime_start=start,
        datetime_end=end,
        username="demo",
        password="demo",
        show_progress_bar=False,
    )

    asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    with pytest.raises(ValueError, match="Cannot connect to Theta Data!"):
        backtester.get_historical_prices(asset, length=10, timestep="minute", return_polars=True)

    assert counts["ohlc"] == 1
    assert counts["quote"] == 1


def test_theta_polars_length_forwarded(monkeypatch):
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(days=365)

    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_polars.subprocess.run",
        lambda *args, **kwargs: type("Result", (), {"stdout": ""})(),
    )
    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_polars.thetadata_helper.get_chains_cached",
        lambda **kwargs: {"Multiplier": 100, "Chains": {}},
    )

    captured_lengths = []
    original_update = ThetaDataBacktestingPolars._update_data

    def tracking_update(self, asset, quote, length, timestep, start_dt=None):
        captured_lengths.append(length)
        return original_update(self, asset, quote, length, timestep, start_dt)

    monkeypatch.setattr(
        ThetaDataBacktestingPolars,
        "_update_data",
        tracking_update,
    )

    def fake_get_price_data(username, password, asset, start_datetime, end_datetime, timespan, quote_asset, dt, datastyle, include_after_hours):
        return _ohlc_day_frame(start_datetime)

    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_polars.thetadata_helper.get_price_data",
        fake_get_price_data,
    )

    backtester = ThetaDataBacktestingPolars(
        datetime_start=start,
        datetime_end=end,
        username="demo",
        password="demo",
        show_progress_bar=False,
    )

    asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    target_dt = start + timedelta(days=210)
    backtester._update_datetime(target_dt)

    bars = backtester.get_historical_prices(asset, length=63, timestep="day", return_polars=True)

    assert captured_lengths, "expected _update_data to be invoked"
    assert captured_lengths[0] >= 63
    assert bars is not None
    assert bars.polars_df.height == 63


def test_theta_polars_day_window_slice(monkeypatch):
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(days=365)

    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_polars.subprocess.run",
        lambda *args, **kwargs: type("Result", (), {"stdout": ""})(),
    )
    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_polars.thetadata_helper.get_chains_cached",
        lambda **kwargs: {"Multiplier": 100, "Chains": {}},
    )
    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_polars.thetadata_helper.get_price_data",
        lambda *args, **kwargs: _ohlc_day_frame(args[3]),
    )

    backtester = ThetaDataBacktestingPolars(
        datetime_start=start,
        datetime_end=end,
        username="demo",
        password="demo",
        show_progress_bar=False,
    )

    asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    target_dt = start + timedelta(days=200)
    backtester._update_datetime(target_dt)

    bars = backtester.get_historical_prices(asset, length=63, timestep="day", return_polars=True)

    assert bars.polars_df.height == 63
    ordered = bars.polars_df.sort("datetime")["datetime"].to_list()
    assert ordered, "expected timestamps"
    expected_last_dt = backtester.to_default_timezone(target_dt).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    assert ordered[-1] == expected_last_dt
    assert "missing" in bars.polars_df.columns
    assert bars.polars_df.sort("datetime")["missing"][-1] is True
    expected_first_dt = expected_last_dt - timedelta(days=62)
    assert ordered[0] == expected_first_dt


def test_theta_polars_quote_columns_present(monkeypatch, caplog):
    start = datetime(2025, 6, 1, tzinfo=timezone.utc)
    end = start + timedelta(days=1)

    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_polars.subprocess.run",
        lambda *args, **kwargs: type("Result", (), {"stdout": ""})(),
    )
    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_polars.thetadata_helper.get_chains_cached",
        lambda **kwargs: {"Multiplier": 100, "Chains": {}},
    )

    def fake_get_price_data(username, password, asset, start_datetime, end_datetime, timespan, quote_asset, dt, datastyle, include_after_hours):
        idx = pd.date_range(start=start_datetime, end=end_datetime, freq="1min", tz="UTC")
        if datastyle == "quote":
            return pd.DataFrame(
                {
                    "bid": [200.0 + i * 0.01 for i in range(len(idx))],
                    "ask": [200.2 + i * 0.01 for i in range(len(idx))],
                    "bid_size": [100 + i for i in range(len(idx))],
                    "ask_size": [120 + i for i in range(len(idx))],
                },
                index=idx,
            )
        return pd.DataFrame(
            {
                "open": [200 + i * 0.05 for i in range(len(idx))],
                "high": [200.1 + i * 0.05 for i in range(len(idx))],
                "low": [199.9 + i * 0.05 for i in range(len(idx))],
                "close": [200 + i * 0.05 for i in range(len(idx))],
                "volume": [10_000 + i for i in range(len(idx))],
            },
            index=idx,
        )

    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_polars.thetadata_helper.get_price_data",
        fake_get_price_data,
    )

    backtester = ThetaDataBacktestingPolars(
        datetime_start=start,
        datetime_end=end,
        username="demo",
        password="demo",
        show_progress_bar=False,
    )

    asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    caplog.set_level(logging.DEBUG, logger="lumibot.backtesting.thetadata_backtesting_polars")
    bars = backtester.get_historical_prices(asset, length=10, timestep="minute", return_polars=True)

    assert "bid" in bars.polars_df.columns
    assert "ask" in bars.polars_df.columns
    assert bars.polars_df["bid"].null_count() == 0
    assert bars.polars_df["ask"].null_count() == 0

    caplog.clear()
    backtester.get_historical_prices(asset, length=10, timestep="minute", return_polars=True)
    reuse_messages = [
        record.message
        for record in caplog.records
        if ("cache covers" in record.message
            or "Reusing cached data" in record.message
            or "[THETA-POLARS][CACHE-HIT]" in record.message)
    ]
    assert reuse_messages, "expected reuse debug log to be emitted"


def test_theta_polars_expired_option_reuses_cache(monkeypatch):
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(days=220)
    expiration = date(2025, 4, 18)

    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_polars.subprocess.run",
        lambda *args, **kwargs: type("Result", (), {"stdout": ""})(),
    )
    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_polars.thetadata_helper.get_chains_cached",
        lambda **kwargs: {"Multiplier": 100, "Chains": {}},
    )

    call_log = []

    def fake_get_price_data(username, password, asset, start_datetime, end_datetime, timespan, quote_asset, dt, datastyle, include_after_hours):
        call_log.append(datastyle)
        if datastyle == "quote":
            return _quote_frame(start_datetime, rows=180)
        return _ohlc_frame(start_datetime, rows=180)

    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_polars.thetadata_helper.get_price_data",
        fake_get_price_data,
    )

    backtester = ThetaDataBacktestingPolars(
        datetime_start=start,
        datetime_end=end,
        username="demo",
        password="demo",
        show_progress_bar=False,
    )

    option_asset = Asset(
        "PLTR",
        asset_type=Asset.AssetType.OPTION,
        expiration=expiration,
        strike=91.0,
        right=Asset.OptionRight.CALL,
    )
    quote_asset = Asset("USD", asset_type=Asset.AssetType.FOREX)
    backtester._update_datetime(datetime(2025, 5, 1, tzinfo=timezone.utc))

    backtester.get_historical_prices(option_asset, length=63, timestep="minute", quote=quote_asset, return_polars=True)
    assert call_log.count("ohlc") == 1
    # quote fetch only happens on initial load
    assert call_log.count("quote") == 1

    backtester.get_historical_prices(option_asset, length=63, timestep="minute", quote=quote_asset, return_polars=True)
    assert call_log.count("ohlc") == 1, "expected cached reuse without additional OHLC fetch"
    assert call_log.count("quote") == 1, "expected cached reuse without additional quote fetch"


def test_theta_polars_placeholder_reload_prevents_refetch(monkeypatch):
    start = datetime(2025, 7, 1, tzinfo=timezone.utc)
    end = start + timedelta(days=5)

    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_polars.subprocess.run",
        lambda *args, **kwargs: type("Result", (), {"stdout": ""})(),
    )
    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_polars.thetadata_helper.get_chains_cached",
        lambda **kwargs: {"Multiplier": 100, "Chains": {}},
    )

    call_params = {}
    call_log = {"ohlc": 0}
    load_calls = []

    def fake_get_price_data(username, password, asset, start_datetime, end_datetime, timespan, quote_asset, dt, datastyle, include_after_hours):
        if datastyle == "ohlc":
            call_log["ohlc"] += 1
            call_params.update({"start": start_datetime, "end": end_datetime, "timespan": timespan})
            return pd.DataFrame()
        return None

    def fake_load_cache(path):
        load_calls.append(path)
        if not call_params:
            return pd.DataFrame()
        start_dt = call_params["start"]
        timespan = call_params.get("timespan", "minute")
        if timespan == "minute":
            freq = "1min"
        elif timespan == "hour":
            freq = "1H"
        else:
            freq = "1D"
        end_dt = call_params.get("end", start_dt)
        if end_dt <= start_dt:
            end_dt = start_dt + timedelta(minutes=2)
        mid_dt = start_dt + (end_dt - start_dt) / 2
        index = pd.DatetimeIndex([start_dt, mid_dt, end_dt]).sort_values().unique()
        if len(index) < 3:
            index = pd.date_range(start=start_dt, periods=3, freq=freq, tz="UTC")
        df = pd.DataFrame(
            {
                "open": [0.0] * len(index),
                "high": [0.0] * len(index),
                "low": [0.0] * len(index),
                "close": [0.0] * len(index),
                "volume": [0] * len(index),
                "missing": [True] * len(index),
            },
            index=index,
        )
        return df

    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_polars.thetadata_helper.get_price_data",
        fake_get_price_data,
    )
    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_polars.thetadata_helper.load_cache",
        fake_load_cache,
    )

    backtester = ThetaDataBacktestingPolars(
        datetime_start=start,
        datetime_end=end,
        username="demo",
        password="demo",
        show_progress_bar=False,
    )

    asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    backtester._update_datetime(start + timedelta(minutes=30))

    backtester.get_historical_prices(asset, length=3, timestep="minute", return_polars=True)
    assert call_log["ohlc"] == 1
    assert load_calls, "expected placeholder reload via load_cache"

    backtester.get_historical_prices(asset, length=3, timestep="minute", return_polars=True)
    assert call_log["ohlc"] == 1, "expected cached reuse without additional fetch"


def test_theta_polars_last_price_trailing_nans(monkeypatch):
    start = datetime(2025, 4, 1, tzinfo=timezone.utc)
    end = start + timedelta(days=1)

    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_polars.subprocess.run",
        lambda *args, **kwargs: type("Result", (), {"stdout": ""})(),
    )

    def fake_get_price_data(username, password, asset, start_datetime, end_datetime, timespan, quote_asset, dt, datastyle, include_after_hours):
        if datastyle == "quote":
            return _quote_frame(start_datetime, rows=5)
        return _ohlc_minute_with_trailing_nans(start_datetime)

    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_polars.thetadata_helper.get_price_data",
        fake_get_price_data,
    )

    backtester = ThetaDataBacktestingPolars(
        datetime_start=start,
        datetime_end=end,
        username="demo",
        password="demo",
        show_progress_bar=False,
    )

    asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    backtester._update_datetime(start + timedelta(minutes=4))

    value = backtester.get_last_price(asset)
    assert value == pytest.approx(421.5)
