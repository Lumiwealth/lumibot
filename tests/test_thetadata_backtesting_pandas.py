from datetime import date, datetime, timezone, timedelta

import pandas as pd
import pytest

from lumibot.backtesting.thetadata_backtesting_pandas import ThetaDataBacktestingPandas
from lumibot.entities import Asset


def _ohlc_frame(start: datetime, rows: int = 12) -> pd.DataFrame:
    index = pd.date_range(start=start, periods=rows, freq="1min", tz="UTC")
    data = {
        "open": [300 + i for i in range(rows)],
        "high": [301 + i for i in range(rows)],
        "low": [299 + i for i in range(rows)],
        "close": [300.5 + i for i in range(rows)],
        "volume": [20_000 + i * 200 for i in range(rows)],
    }
    return pd.DataFrame(data, index=index)


def _ohlc_day_frame(start: datetime, rows: int = 120) -> pd.DataFrame:
    index = pd.date_range(start=start, periods=rows, freq="1D", tz="UTC")
    data = {
        "open": [150 + i for i in range(rows)],
        "high": [151 + i for i in range(rows)],
        "low": [149 + i for i in range(rows)],
        "close": [150.5 + i for i in range(rows)],
        "volume": [5_000 + i * 50 for i in range(rows)],
    }
    return pd.DataFrame(data, index=index)


def _quote_frame(start: datetime, rows: int = 12) -> pd.DataFrame:
    index = pd.date_range(start=start, periods=rows, freq="1min", tz="UTC")
    data = {
        "bid": [400 + i * 0.1 for i in range(rows)],
        "ask": [400.2 + i * 0.1 for i in range(rows)],
        "bid_size": [100 + i for i in range(rows)],
        "ask_size": [120 + i for i in range(rows)],
    }
    return pd.DataFrame(data, index=index)


def _ohlc_minute_with_trailing_nans(start: datetime) -> pd.DataFrame:
    index = pd.date_range(start=start, periods=5, freq="1min", tz="UTC")
    data = {
        "open": [310, 311, 312, 313, 314],
        "high": [311, 312, 313, 314, 315],
        "low": [309, 310, 311, 312, 313],
        "close": [310.5, 311.5, float("nan"), float("nan"), float("nan")],
        "volume": [50_000 + i * 100 for i in range(5)],
    }
    return pd.DataFrame(data, index=index)


@pytest.mark.parametrize("length", [8, 16])
def test_theta_pandas_quote_failure_stores_ohlc(monkeypatch, length):
    start = datetime(2025, 3, 1, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    counts = {"ohlc": 0, "quote": 0}

    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_pandas.subprocess.run",
        lambda *args, **kwargs: type("Result", (), {"stdout": ""})(),
    )

    def fake_get_price_data(username, password, asset, start_datetime, end_datetime, timespan, quote_asset, dt, datastyle, include_after_hours):
        if datastyle == "quote":
            counts["quote"] += 1
            raise ValueError("Cannot connect to Theta Data!")
        counts["ohlc"] += 1
        return _ohlc_frame(start_datetime, rows=32)

    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_pandas.thetadata_helper.get_price_data",
        fake_get_price_data,
    )

    backtester = ThetaDataBacktestingPandas(
        datetime_start=start,
        datetime_end=end,
        username="demo",
        password="demo",
        show_progress_bar=False,
    )

    asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    with pytest.raises(ValueError, match="Cannot connect to Theta Data!"):
        backtester.get_historical_prices(asset, length=length, timestep="minute")

    assert counts["ohlc"] == 1
    assert counts["quote"] == 1


def test_theta_pandas_length_forwarded(monkeypatch):
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(days=365)

    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_pandas.subprocess.run",
        lambda *args, **kwargs: type("Result", (), {"stdout": ""})(),
    )

    captured_calls = []

    def fake_get_price_data(username, password, asset, start_datetime, end_datetime, timespan, quote_asset, dt, datastyle, include_after_hours):
        captured_calls.append(
            {
                "datastyle": datastyle,
                "timespan": timespan,
                "start": start_datetime,
                "end": end_datetime,
            }
        )
        return _ohlc_day_frame(start_datetime, rows=140)

    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_pandas.thetadata_helper.get_price_data",
        fake_get_price_data,
    )

    lengths = []
    original_update = ThetaDataBacktestingPandas._update_pandas_data

    def tracking_update(self, asset, quote, length, timestep, start_dt=None):
        lengths.append(length)
        return original_update(self, asset, quote, length, timestep, start_dt)

    monkeypatch.setattr(
        ThetaDataBacktestingPandas,
        "_update_pandas_data",
        tracking_update,
    )

    backtester = ThetaDataBacktestingPandas(
        datetime_start=start,
        datetime_end=end,
        username="demo",
        password="demo",
        show_progress_bar=False,
    )

    asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    target_dt = start + timedelta(days=200)
    backtester._update_datetime(target_dt)

    bars = backtester.get_historical_prices(asset, length=63, timestep="day")

    assert lengths, "expected _update_pandas_data to be invoked"
    assert lengths[0] >= 63
    assert bars is not None
    assert len(bars.df) == 63
    assert any(call["datastyle"] == "ohlc" and call["timespan"] == "day" for call in captured_calls)


def test_theta_pandas_expired_option_reuses_cache(monkeypatch):
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(days=220)
    expiration = date(2025, 4, 18)

    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_pandas.subprocess.run",
        lambda *args, **kwargs: type("Result", (), {"stdout": ""})(),
    )

    call_log = []

    def fake_get_price_data(username, password, asset, start_datetime, end_datetime, timespan, quote_asset, dt, datastyle, include_after_hours):
        call_log.append(datastyle)
        if datastyle == "quote":
            return _quote_frame(start_datetime, rows=180)
        return _ohlc_frame(start_datetime, rows=180)

    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_pandas.thetadata_helper.get_price_data",
        fake_get_price_data,
    )

    backtester = ThetaDataBacktestingPandas(
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

    backtester.get_historical_prices(option_asset, length=63, timestep="minute", quote=quote_asset)
    assert call_log.count("ohlc") == 1
    assert call_log.count("quote") == 1

    backtester.get_historical_prices(option_asset, length=63, timestep="minute", quote=quote_asset)
    assert call_log.count("ohlc") == 1, "expected cached reuse without additional OHLC fetch"
    assert call_log.count("quote") == 1, "expected cached reuse without additional quote fetch"


def test_theta_pandas_placeholder_reload_prevents_refetch(monkeypatch):
    start = datetime(2025, 8, 1, tzinfo=timezone.utc)
    end = start + timedelta(days=3)

    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_pandas.subprocess.run",
        lambda *args, **kwargs: type("Result", (), {"stdout": ""})(),
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
        "lumibot.backtesting.thetadata_backtesting_pandas.thetadata_helper.get_price_data",
        fake_get_price_data,
    )
    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_pandas.thetadata_helper.load_cache",
        fake_load_cache,
    )

    backtester = ThetaDataBacktestingPandas(
        datetime_start=start,
        datetime_end=end,
        username="demo",
        password="demo",
        show_progress_bar=False,
    )

    asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    backtester._update_datetime(start + timedelta(minutes=30))

    backtester.get_historical_prices(asset, length=3, timestep="minute")
    assert call_log["ohlc"] == 1
    assert load_calls, "expected placeholder reload via load_cache"

    backtester.get_historical_prices(asset, length=3, timestep="minute")
    assert call_log["ohlc"] == 1, "expected cached reuse without additional fetch"


def test_theta_pandas_last_price_trailing_nans(monkeypatch):
    start = datetime(2025, 2, 1, tzinfo=timezone.utc)
    end = start + timedelta(days=1)

    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_pandas.subprocess.run",
        lambda *args, **kwargs: type("Result", (), {"stdout": ""})(),
    )

    def fake_get_price_data(username, password, asset, start_datetime, end_datetime, timespan, quote_asset, dt, datastyle, include_after_hours):
        if datastyle == "quote":
            return _quote_frame(start_datetime, rows=5)
        return _ohlc_minute_with_trailing_nans(start_datetime)

    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_pandas.thetadata_helper.get_price_data",
        fake_get_price_data,
    )

    backtester = ThetaDataBacktestingPandas(
        datetime_start=start,
        datetime_end=end,
        username="demo",
        password="demo",
        show_progress_bar=False,
    )

    asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    backtester._update_datetime(start + timedelta(minutes=4))

    value = backtester.get_last_price(asset)
    assert value == pytest.approx(311.5)


def test_theta_pandas_day_window_slice_includes_placeholder(monkeypatch):
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(days=365)

    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_pandas.subprocess.run",
        lambda *args, **kwargs: type("Result", (), {"stdout": ""})(),
    )

    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_pandas.thetadata_helper.get_chains_cached",
        lambda **kwargs: {"Multiplier": 100, "Chains": {}},
    )

    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_pandas.thetadata_helper.get_price_data",
        lambda *args, **kwargs: _ohlc_day_frame(args[3]),
    )

    backtester = ThetaDataBacktestingPandas(
        datetime_start=start,
        datetime_end=end,
        username="demo",
        password="demo",
        show_progress_bar=False,
    )

    asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)
    target_dt = start + timedelta(days=200)
    backtester._update_datetime(target_dt)

    bars = backtester.get_historical_prices(asset, length=63, timestep="day")
    df = bars.df

    expected_last_dt = backtester.to_default_timezone(target_dt).replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    assert df.index[-1] == expected_last_dt
    assert "missing" in df.columns
    assert df.iloc[-1]["missing"] is True
