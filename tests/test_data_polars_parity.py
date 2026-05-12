"""
Regression test for Data vs DataPolars parity bug.

This test isolates the issue where DataPolars returns 234 rows when asked for 2 rows
with timeshift=-2 parameter.
"""

from datetime import UTC, datetime, timedelta

import pandas as pd
import polars as pl
import pytest

from lumibot.entities import Asset, Data, DataPolars


def _create_mock_ohlc_data(start: datetime, periods: int = 300) -> pd.DataFrame:
    """Create mock OHLC data for testing.

    Args:
        start: Starting datetime (must be timezone-aware)
        periods: Number of minute bars to generate

    Returns:
        DataFrame with OHLC data indexed by timestamp
    """
    index = pd.date_range(start=start, periods=periods, freq="1min", tz=UTC)
    data = {
        "open": [200 + i * 0.1 for i in range(periods)],
        "high": [201 + i * 0.1 for i in range(periods)],
        "low": [199 + i * 0.1 for i in range(periods)],
        "close": [200.5 + i * 0.1 for i in range(periods)],
        "volume": [10000 + i * 100 for i in range(periods)],
    }
    return pd.DataFrame(data, index=index)


def test_data_polars_row_count_parity():
    """
    Test that Data and DataPolars return the same number of rows for identical requests.

    This reproduces the bug where:
    - Data.get_bars(length=2, timeshift=-2) returns 2 rows
    - DataPolars.get_bars(length=2, timeshift=-2) returns 234 rows
    """
    # Create mock data starting at market open
    start = datetime(2024, 7, 18, 9, 30, tzinfo=UTC)
    mock_df = _create_mock_ohlc_data(start, periods=300)

    # Create asset
    asset = Asset("HIMS", asset_type=Asset.AssetType.STOCK)

    # Create Data instance (pandas mode)
    data_pandas = Data(
        asset=asset,
        df=mock_df.copy(),
        timestep="minute",
        quote=asset,
    )

    # Create DataPolars instance (polars mode)
    # Convert to polars format with datetime column
    mock_df_reset = mock_df.reset_index()
    mock_df_reset.columns = ["datetime", "open", "high", "low", "close", "volume"]
    mock_polars = pl.from_pandas(mock_df_reset)

    data_polars = DataPolars(
        asset=asset,
        df=mock_polars,
        timestep="minute",
        quote=asset,
    )

    # Test at a specific datetime (10:00 AM = 30 minutes after market open)
    test_dt = datetime(2024, 7, 18, 10, 0, tzinfo=UTC)

    # Request 2 bars with timeshift=-2
    # This should return bars at 09:58 and 09:59
    # get_bars() returns DataFrames directly
    df_pandas = data_pandas.get_bars(dt=test_dt, length=2, timestep="minute", timeshift=-2)

    df_polars = data_polars.get_bars(dt=test_dt, length=2, timestep="minute", timeshift=-2)

    # CRITICAL ASSERTIONS
    assert len(df_pandas) == 2, f"Pandas should return 2 rows, got {len(df_pandas)}"
    assert len(df_polars) == 2, f"Polars should return 2 rows, got {len(df_polars)}"
    assert len(df_pandas) == len(df_polars), (
        f"Row count mismatch! Pandas returned {len(df_pandas)} rows, Polars returned {len(df_polars)} rows"
    )


def test_data_polars_timeshift_timedelta():
    """
    Test timeshift parameter handling when passed as timedelta.

    Tests the conversion of timedelta(minutes=-2) to integer offset.
    """
    start = datetime(2024, 7, 18, 9, 30, tzinfo=UTC)
    mock_df = _create_mock_ohlc_data(start, periods=300)

    asset = Asset("HIMS", asset_type=Asset.AssetType.STOCK)

    # Create Data instance
    data_pandas = Data(
        asset=asset,
        df=mock_df.copy(),
        timestep="minute",
        quote=asset,
    )

    # Create DataPolars instance
    mock_df_reset = mock_df.reset_index()
    mock_df_reset.columns = ["datetime", "open", "high", "low", "close", "volume"]
    mock_polars = pl.from_pandas(mock_df_reset)

    data_polars = DataPolars(
        asset=asset,
        df=mock_polars,
        timestep="minute",
        quote=asset,
    )

    test_dt = datetime(2024, 7, 18, 10, 0, tzinfo=UTC)

    # Test with timedelta parameter (this is what the backtest engine uses)
    timeshift_td = timedelta(minutes=-2)

    # get_bars() returns DataFrames directly
    df_pandas = data_pandas.get_bars(dt=test_dt, length=2, timestep="minute", timeshift=timeshift_td)

    df_polars = data_polars.get_bars(dt=test_dt, length=2, timestep="minute", timeshift=timeshift_td)

    assert len(df_pandas) == 2, "Pandas should return 2 rows with timedelta timeshift"
    assert len(df_polars) == 2, "Polars should return 2 rows with timedelta timeshift"
    assert len(df_pandas) == len(df_polars), "Row count mismatch with timedelta timeshift"


def test_data_get_bars_fast_path_does_not_drop_on_nan_extra_columns():
    """
    Ensure the Data.get_bars() fast-path matches legacy resample semantics.

    Historically, get_bars() resampled/aggregated OHLCV and therefore ignored unrelated columns
    (e.g. bid/ask) when dropping NaNs. The fast-path must not drop rows just because an extra
    column is NaN.
    """
    start = datetime(2024, 7, 18, 9, 30, tzinfo=UTC)
    mock_df = _create_mock_ohlc_data(start, periods=300)

    # Add an "extra" column that is entirely NaN. The output should still include bars.
    mock_df["bid"] = None

    # Make volume NaN everywhere. The legacy resample path turns NaN volumes into 0 via `sum`,
    # so the fast-path must do the same.
    mock_df["volume"] = None

    asset = Asset("HIMS", asset_type=Asset.AssetType.STOCK)
    data_pandas = Data(
        asset=asset,
        df=mock_df.copy(),
        timestep="minute",
        quote=asset,
    )

    test_dt = datetime(2024, 7, 18, 10, 0, tzinfo=UTC)
    df_bars = data_pandas.get_bars(
        dt=test_dt,
        length=2,
        timestep="minute",
        timeshift=-2,
    )

    assert len(df_bars) == 2
    assert "bid" not in df_bars.columns, "Extra columns should not be returned by get_bars()"
    assert df_bars["volume"].isna().sum() == 0, "NaN volume should be normalized to 0 (resample parity)"
    assert all(float(v) == 0.0 for v in df_bars["volume"]), "Expected filled volume to be 0.0 for NaN inputs"


if __name__ == "__main__":
    # Run tests with verbose output
    pytest.main([__file__, "-v", "-s"])
