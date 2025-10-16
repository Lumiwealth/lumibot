"""
Test that ThetaData returns pandas DataFrames by default for backwards compatibility.

CRITICAL: This ensures all existing strategies using ThetaData continue to work.
"""

import pytest
import pandas as pd
import polars as pl
from datetime import datetime, timedelta, timezone

from lumibot.backtesting.thetadata_backtesting_polars import ThetaDataBacktestingPolars
from lumibot.entities import Asset


def _mock_ohlc_frame(start: datetime, rows: int = 8) -> pd.DataFrame:
    """Helper to create mock OHLC data."""
    index = pd.date_range(start=start, periods=rows, freq="1min", tz="UTC")
    data = {
        "open": [200 + i for i in range(rows)],
        "high": [201 + i for i in range(rows)],
        "low": [199 + i for i in range(rows)],
        "close": [200.5 + i for i in range(rows)],
        "volume": [10_000 + i * 100 for i in range(rows)],
    }
    return pd.DataFrame(data, index=index)


def _mock_quote_frame(start: datetime, rows: int = 8) -> pd.DataFrame:
    """Helper to create mock quote data."""
    index = pd.date_range(start=start, periods=rows, freq="1min", tz="UTC")
    data = {
        "bid": [200.1 + i * 0.1 for i in range(rows)],
        "ask": [200.3 + i * 0.1 for i in range(rows)],
        "bid_size": [50 + i for i in range(rows)],
        "ask_size": [60 + i for i in range(rows)],
    }
    return pd.DataFrame(data, index=index)


def test_thetadata_returns_pandas_by_default(monkeypatch):
    """
    Test that ThetaDataBacktestingPolars returns pandas DataFrames by default.

    CRITICAL: This is the backwards compatibility contract.
    - Default behavior (no return_polars param) MUST return pandas
    - Thousands of existing strategies depend on this
    """
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(days=1)

    # Mock subprocess and helper functions
    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_polars.subprocess.run",
        lambda *args, **kwargs: type("Result", (), {"stdout": ""})(),
    )

    def fake_get_price_data(username, password, asset, start_datetime, end_datetime,
                           timespan, quote_asset, dt, datastyle, include_after_hours, return_polars=False):
        df = _mock_quote_frame(start_datetime) if datastyle == "quote" else _mock_ohlc_frame(start_datetime)
        if return_polars:
            # Convert to polars
            df_reset = df.reset_index()
            df_reset['datetime'] = df.index
            return pl.from_pandas(df_reset)
        return df

    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_polars.thetadata_helper.get_price_data",
        fake_get_price_data,
    )
    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_polars.thetadata_helper.get_chains_cached",
        lambda **kwargs: {"Multiplier": 100, "Chains": {}},
    )

    # Create backtester
    backtester = ThetaDataBacktestingPolars(
        datetime_start=start,
        datetime_end=end,
        username="demo",
        password="demo",
        show_progress_bar=False,
    )

    asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)

    # Call WITHOUT return_polars parameter (default behavior)
    bars = backtester.get_historical_prices(asset, length=4, timestep="minute")

    # CRITICAL ASSERTIONS
    assert bars is not None, "get_historical_prices returned None"
    assert hasattr(bars, 'df'), "Bars object missing .df property"

    df = bars.df
    assert isinstance(df, pd.DataFrame), (
        f"BACKWARDS COMPATIBILITY BROKEN! "
        f"ThetaDataBacktestingPolars.get_historical_prices() returned {type(df)} "
        f"instead of pandas DataFrame when called without return_polars parameter. "
        f"This will break ALL existing strategies!"
    )
    assert not isinstance(df, pl.DataFrame), (
        "get_historical_prices() returned polars DataFrame by default! "
        "This breaks backwards compatibility."
    )

    # Verify it works like pandas
    assert hasattr(df, 'iloc'), "Result doesn't have pandas iloc"
    assert hasattr(df, 'loc'), "Result doesn't have pandas loc"
    assert len(df) == 4, f"Expected 4 rows, got {len(df)}"


def test_thetadata_returns_pandas_when_return_polars_false(monkeypatch):
    """
    Test that ThetaDataBacktestingPolars returns pandas when return_polars=False.
    """
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(days=1)

    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_polars.subprocess.run",
        lambda *args, **kwargs: type("Result", (), {"stdout": ""})(),
    )

    def fake_get_price_data(username, password, asset, start_datetime, end_datetime,
                           timespan, quote_asset, dt, datastyle, include_after_hours, return_polars=False):
        df = _mock_quote_frame(start_datetime) if datastyle == "quote" else _mock_ohlc_frame(start_datetime)
        if return_polars:
            # Convert to polars
            df_reset = df.reset_index()
            df_reset['datetime'] = df.index
            return pl.from_pandas(df_reset)
        return df

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

    # Call WITH return_polars=False (explicit pandas request)
    bars = backtester.get_historical_prices(asset, length=4, timestep="minute", return_polars=False)

    assert bars is not None
    df = bars.df
    assert isinstance(df, pd.DataFrame), f"Expected pandas DataFrame, got {type(df)}"
    assert not isinstance(df, pl.DataFrame), "Should NOT be polars DataFrame"


def test_thetadata_returns_polars_when_return_polars_true(monkeypatch):
    """
    Test that ThetaDataBacktestingPolars returns polars when return_polars=True.
    """
    start = datetime(2025, 1, 1, tzinfo=timezone.utc)
    end = start + timedelta(days=1)

    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_polars.subprocess.run",
        lambda *args, **kwargs: type("Result", (), {"stdout": ""})(),
    )

    def fake_get_price_data(username, password, asset, start_datetime, end_datetime,
                           timespan, quote_asset, dt, datastyle, include_after_hours, return_polars=False):
        df = _mock_quote_frame(start_datetime) if datastyle == "quote" else _mock_ohlc_frame(start_datetime)
        if return_polars:
            # Convert to polars
            df_reset = df.reset_index()
            df_reset['datetime'] = df.index
            return pl.from_pandas(df_reset)
        return df

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

    # Call WITH return_polars=True (opt-in to polars)
    bars = backtester.get_historical_prices(asset, length=4, timestep="minute", return_polars=True)

    assert bars is not None
    df = bars.df
    assert isinstance(df, pl.DataFrame), f"Expected polars DataFrame, got {type(df)}"
    assert not isinstance(df, pd.DataFrame), "Should NOT be pandas DataFrame"
