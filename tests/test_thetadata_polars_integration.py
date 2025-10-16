"""
Integration test for ThetaDataBacktestingPolars data parity.

This test reproduces the full backtesting chain to identify where the 234-row bug originates.
"""

from datetime import datetime, timedelta, timezone
import pandas as pd
import polars as pl
import pytest

from lumibot.backtesting.thetadata_backtesting_polars import ThetaDataBacktestingPolars
from lumibot.entities import Asset


def _mock_ohlc_frame(start: datetime, periods: int = 300) -> pd.DataFrame:
    """Create mock OHLC data for testing."""
    index = pd.date_range(start=start, periods=periods, freq="1min", tz="UTC")
    data = {
        "open": [200 + i * 0.1 for i in range(periods)],
        "high": [201 + i * 0.1 for i in range(periods)],
        "low": [199 + i * 0.1 for i in range(periods)],
        "close": [200.5 + i * 0.1 for i in range(periods)],
        "volume": [10000 + i * 100 for i in range(periods)],
    }
    return pd.DataFrame(data, index=index)


def test_thetadata_polars_full_chain_parity(monkeypatch):
    """
    Test the FULL backtesting chain to reproduce the 234-row bug.

    This mimics what happens during actual backtesting:
    1. Backtester creates Data/DataPolars objects
    2. Strategy calls get_historical_prices()
    3. Returns should be identical between pandas and polars modes
    """
    start = datetime(2024, 7, 18, 9, 30, tzinfo=timezone.utc)
    end = start + timedelta(hours=4)  # Trade through early morning

    # Mock subprocess and helper functions
    monkeypatch.setattr(
        "lumibot.backtesting.thetadata_backtesting_polars.subprocess.run",
        lambda *args, **kwargs: type("Result", (), {"stdout": ""})(),
    )

    def fake_get_price_data(username, password, asset, start_datetime, end_datetime,
                           timespan, quote_asset, dt, datastyle, include_after_hours, return_polars=False):
        # Return full dataset starting from requested start_datetime
        # Use end_datetime to calculate how many periods we need
        if end_datetime and start_datetime:
            time_diff = end_datetime - start_datetime
            periods = max(300, int(time_diff.total_seconds() / 60) + 10)  # Extra buffer
        else:
            periods = 300

        df = _mock_ohlc_frame(start_datetime, periods=periods)

        if return_polars:
            # Convert to polars format with datetime column
            df_reset = df.reset_index()
            df_reset.columns = ["datetime", "open", "high", "low", "close", "volume"]
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

    asset = Asset("HIMS", asset_type=Asset.AssetType.STOCK)

    # Simulate what happens during backtesting:
    # 1. First, the backtester advances to a specific datetime
    test_dt = datetime(2024, 7, 18, 10, 0, tzinfo=timezone.utc)

    # In the actual backtest, the datetime is set via the backtester's internal clock
    # We can't easily mock this, so we'll just call get_historical_prices directly

    # 2. Strategy calls get_historical_prices with a timeshift parameter
    timeshift_td = timedelta(minutes=-2)

    # Test with return_polars=False (pandas mode - backwards compat)
    bars_pandas_compat = backtester.get_historical_prices(
        asset,
        length=2,
        timestep="minute",
        timeshift=timeshift_td,
        return_polars=False
    )

    # Test with return_polars=True (polars mode)
    bars_polars = backtester.get_historical_prices(
        asset,
        length=2,
        timestep="minute",
        timeshift=timeshift_td,
        return_polars=True
    )

    # Get the DataFrames
    df_pandas_compat = bars_pandas_compat.df if bars_pandas_compat else None
    df_polars = bars_polars.df if bars_polars else None

    print(f"\n=== ThetaData Integration Test ===")
    print(f"Pandas compat mode rows: {len(df_pandas_compat) if df_pandas_compat is not None else 0}")
    print(f"Polars mode rows: {len(df_polars) if df_polars is not None else 0}")

    if df_pandas_compat is not None:
        print(f"Pandas compat timestamps: {df_pandas_compat.index.tolist() if hasattr(df_pandas_compat, 'index') else df_pandas_compat['datetime'].tolist()}")
    if df_polars is not None:
        print(f"Polars timestamps: {df_polars['datetime'].to_list() if isinstance(df_polars, pl.DataFrame) else df_polars.index.tolist()}")

    # CRITICAL ASSERTIONS
    assert bars_pandas_compat is not None, "Pandas compat mode returned None"
    assert bars_polars is not None, "Polars mode returned None"

    assert df_pandas_compat is not None, "Pandas compat mode has no df"
    assert df_polars is not None, "Polars mode has no df"

    # This is the key assertion - both should return 2 rows
    assert len(df_pandas_compat) == 2, (
        f"Pandas compat mode should return 2 rows, got {len(df_pandas_compat)}"
    )
    assert len(df_polars) == 2, (
        f"Polars mode should return 2 rows, got {len(df_polars)}. "
        f"This is the 234-row bug!"
    )

    # And they should have the same length
    assert len(df_pandas_compat) == len(df_polars), (
        f"Row count mismatch! Pandas compat returned {len(df_pandas_compat)} rows, "
        f"Polars returned {len(df_polars)} rows"
    )


if __name__ == "__main__":
    # Run tests with verbose output
    pytest.main([__file__, "-v", "-s"])
