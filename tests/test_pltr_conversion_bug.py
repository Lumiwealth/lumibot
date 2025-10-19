"""Test to reproduce PLTR polars→pandas conversion corruption.

This test proves that calling bars.pandas_df on PLTR data corrupts the close/return columns.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from datetime import datetime

# Add the lumibot root directory to sys.path
current_file = Path(__file__).resolve()
lumibot_root = current_file.parent.parent
if str(lumibot_root) not in sys.path:
    sys.path.insert(0, str(lumibot_root))

import polars as pl
from lumibot.entities import Asset
from lumibot.entities.data_polars import DataPolars
from lumibot.entities.bars import Bars
from lumibot.tools import thetadata_helper


def test_pltr_conversion_corruption():
    """
    Prove that polars→pandas conversion corrupts PLTR data.

    This test:
    1. Loads PLTR data from ThetaData cache (2024-09-13)
    2. Creates DataPolars → Bars wrapper
    3. Accesses bars.pandas_df (triggers conversion)
    4. Asserts close/return columns are NOT all NULL

    Expected: This test should FAIL today, proving the bug exists.
    """
    print("\n" + "=" * 80)
    print("Phase 1: Proving PLTR Polars→Pandas Conversion Corruption")
    print("=" * 80)

    # Load PLTR data from cache - using the exact asset and date that fails
    asset = Asset("PLTR", Asset.AssetType.STOCK)
    date = datetime(2024, 9, 13)  # From logs: 2024-09-13 is when PLTR gets skipped

    print(f"\n1. Loading PLTR data for {date.date()}")

    # Get data using thetadata_helper (same as backtesting does)
    thetadata_helper.initialize_connection()
    df = thetadata_helper.get_historical_data_polars(
        asset=asset,
        start_date=datetime(2024, 9, 1),  # Get enough history
        end_date=datetime(2024, 9, 13),
        timestep="minute"
    )

    print(f"   Loaded polars DataFrame: shape={df.shape}")
    print(f"   Columns: {df.columns}")
    print(f"   Sample close values (first 5): {df['close'].head(5).to_list()}")
    print(f"   Close column has nulls: {df['close'].is_null().any()}")

    # Calculate returns (as strategy does)
    df = df.with_columns([
        pl.col("close").pct_change().alias("return")
    ])

    print(f"\n2. After adding return column:")
    print(f"   Shape: {df.shape}")
    print(f"   Close has nulls: {df['close'].is_null().any()}")
    print(f"   Return has nulls: {df['return'].is_null().any()}")
    print(f"   Non-null close count: {df['close'].is_not_null().sum()}")
    print(f"   Non-null return count: {df['return'].is_not_null().sum()}")

    # Create DataPolars wrapper
    data_polars = DataPolars(df)

    # Create Bars object
    bars = Bars(data_polars, asset, raw=df, source="thetadata")

    print(f"\n3. Created Bars object")
    print(f"   Bars._df type: {type(bars._df)}")
    print(f"   Bars._df.polars_df shape: {bars._df.polars_df.shape}")

    # THE CRITICAL MOMENT: Access pandas_df (triggers conversion in bars.py:186)
    print(f"\n4. TRIGGERING CONVERSION: Accessing bars.pandas_df...")
    pandas_df = bars.pandas_df

    print(f"   Converted pandas DataFrame: shape={pandas_df.shape}")
    print(f"   Columns: {list(pandas_df.columns)}")
    print(f"   Index type: {type(pandas_df.index)}")

    # Check for corruption
    if 'close' in pandas_df.columns:
        close_nulls = pandas_df['close'].isna().sum()
        close_total = len(pandas_df)
        print(f"   Close column nulls: {close_nulls}/{close_total}")
        print(f"   Sample close values (first 5): {pandas_df['close'].head(5).tolist()}")
    else:
        print(f"   ⚠️  WARNING: 'close' column missing!")

    if 'return' in pandas_df.columns:
        return_nulls = pandas_df['return'].isna().sum()
        return_total = len(pandas_df)
        print(f"   Return column nulls: {return_nulls}/{return_total}")
        print(f"   Sample return values (first 5): {pandas_df['return'].head(5).tolist()}")
    else:
        print(f"   ⚠️  WARNING: 'return' column missing!")

    print("\n" + "=" * 80)

    # ASSERTIONS: This should FAIL today if bug exists
    assert 'close' in pandas_df.columns, "close column missing after conversion!"
    assert 'return' in pandas_df.columns, "return column missing after conversion!"

    close_all_null = pandas_df['close'].isna().all()
    return_all_null = pandas_df['return'].isna().all()

    if close_all_null:
        print("❌ BUG CONFIRMED: close column is ALL NULL after conversion")
    if return_all_null:
        print("❌ BUG CONFIRMED: return column is ALL NULL after conversion")

    assert not close_all_null, "BUG CONFIRMED: close column became ALL NULL during polars→pandas conversion!"
    assert not return_all_null, "BUG CONFIRMED: return column became ALL NULL during polars→pandas conversion!"

    print("✅ TEST PASSED: Conversion preserved data correctly")
    print("=" * 80)


if __name__ == "__main__":
    test_pltr_conversion_corruption()
