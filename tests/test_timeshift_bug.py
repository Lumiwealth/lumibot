"""Unit test to isolate the timeshift=-2 conversion bug in polars.

This test demonstrates that polars incorrectly converts timeshift=-2 (integer)
to timedelta("-1 day, 23:58:00") somewhere in the call chain, causing it to
return 1 row instead of 2 rows like pandas does.
"""

# Add project root to path for local development
import sys
from pathlib import Path
project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

import pytest
from datetime import datetime
from lumibot.entities import Asset
from lumibot.backtesting.thetadata_backtesting_pandas import ThetaDataBacktestingPandas
from lumibot.backtesting.thetadata_backtesting_polars import ThetaDataBacktestingPolars


def test_timeshift_negative_integer_minute_bars():
    """Test that polars handles timeshift=-2 the same as pandas for minute bars.

    This test verifies the fix for the timedelta conversion bug where polars was
    incorrectly converting integer timeshift to timedelta(minutes=X), causing
    timeshift=-2 to become timedelta("-1 day, 23:58:00").

    The fix (in polars_data.py:671-683) ensures integer timeshift represents
    BAR offsets and calculates the correct datetime adjustment based on the
    actual timestep being requested.

    This test should PASS after the fix, returning 2 rows for both implementations.
    """
    # Use HIMS $22 CALL 2024-07-19 as test case (same as weekly_momentum strategy)
    asset = Asset(
        symbol="HIMS",
        asset_type=Asset.AssetType.OPTION,
        expiration=datetime(2024, 7, 19).date(),
        strike=22.0,
        right="call"
    )

    # July 17, 2024 10:00 - Date when first trade occurs (has future data available)
    test_dt = datetime(2024, 7, 17, 10, 0, 0)

    # Test parameters matching strategy.get_historical_prices call
    length = 2
    timeshift = -2  # INTEGER: 2 minutes INTO FUTURE (look ahead)
    timestep = "minute"

    # Pandas implementation (ground truth)
    pandas_backtester = ThetaDataBacktestingPandas(
        datetime_start=datetime(2024, 7, 15),   # Match profile script date range
        datetime_end=datetime(2024, 7, 26),
    )
    pandas_backtester.datetime = test_dt  # Set current datetime for the query

    pandas_bars = pandas_backtester.get_historical_prices(
        asset=asset,
        length=length,
        timestep=timestep,
        timeshift=timeshift,
    )

    # Polars implementation (has bug)
    polars_backtester = ThetaDataBacktestingPolars(
        datetime_start=datetime(2024, 7, 15),   # Same range as pandas
        datetime_end=datetime(2024, 7, 26),
    )
    polars_backtester.datetime = test_dt  # Set current datetime for the query

    polars_bars = polars_backtester.get_historical_prices(
        asset=asset,
        length=length,
        timestep=timestep,
        timeshift=timeshift,
    )

    # Compare results
    # VERIFICATION: If timeshift=-2 is incorrectly converted to timedelta("-1 day, 23:58:00"),
    # polars will return 1 row instead of 2. This test ensures both return 2 rows.
    # Check logs for "[TIMESHIFT_DEBUG][PRE_GET_BARS]" to verify timeshift_type=int (not timedelta)
    pandas_rows = len(pandas_bars.df) if pandas_bars and pandas_bars.df is not None else 0
    polars_rows = len(polars_bars.df) if polars_bars and polars_bars.df is not None else 0

    print(f"\nPandas rows: {pandas_rows}")
    print(f"Polars rows: {polars_rows}")

    if pandas_bars and pandas_bars.df is not None and len(pandas_bars.df) > 0:
        print(f"Pandas timestamps: {list(pandas_bars.df.index)}")
    if polars_bars and polars_bars.df is not None and len(polars_bars.df) > 0:
        print(f"Polars timestamps: {list(polars_bars.df.index)}")

    # This assertion MUST FAIL with current code (polars returns 1 row, pandas returns 2)
    assert polars_rows == pandas_rows, f"Parity failure: pandas returned {pandas_rows} rows, polars returned {polars_rows} rows"

    # If we get here, verify the data matches
    if pandas_rows > 0 and polars_rows > 0:
        # Compare timestamps
        pandas_ts = list(pandas_bars.df.index)
        polars_ts = list(polars_bars.df.index)
        assert pandas_ts == polars_ts, f"Timestamp mismatch: {pandas_ts} != {polars_ts}"

        # Compare OHLC values
        for col in ["open", "high", "low", "close"]:
            if col in pandas_bars.df.columns and col in polars_bars.df.columns:
                pandas_vals = list(pandas_bars.df[col])
                polars_vals = list(polars_bars.df[col])
                assert pandas_vals == polars_vals, f"{col} mismatch: {pandas_vals} != {polars_vals}"


if __name__ == "__main__":
    # Run the test to see it fail
    try:
        test_timeshift_negative_integer_minute_bars()
        print("\n✓ TEST PASSED - Bug is fixed!")
    except AssertionError as e:
        print(f"\n✗ TEST FAILED (expected) - Bug confirmed: {e}")
    except Exception as e:
        print(f"\n✗ TEST ERROR - Unexpected error: {e}")
        raise
