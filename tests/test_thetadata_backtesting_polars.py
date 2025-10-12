from datetime import datetime, timezone, timedelta

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
