#!/usr/bin/env python3
"""
Quick verification script to check if indices subscription is active.

Run this after ThetaData support activates the indices subscription.
If all tests pass, run the full test suite.
"""

import os
import datetime
from lumibot.entities import Asset
from lumibot.tools import thetadata_helper


def test_index_access():
    """Quick check if SPX is accessible."""
    print("="*80)
    print("THETADATA INDICES SUBSCRIPTION VERIFICATION")
    print("="*80)

    username = os.environ.get("THETADATA_USERNAME")
    password = os.environ.get("THETADATA_PASSWORD")

    if not username or not password:
        print("✗ ERROR: THETADATA_USERNAME or THETADATA_PASSWORD not set")
        return False

    print(f"\n1. Testing SPX index data access...")
    asset = Asset("SPX", asset_type="index")

    try:
        df = thetadata_helper.get_price_data(
            username=username,
            password=password,
            asset=asset,
            start=datetime.datetime(2024, 8, 1, 9, 30),
            end=datetime.datetime(2024, 8, 1, 9, 35),
            timespan="minute"
        )

        if df is not None and len(df) > 0:
            print(f"   ✓ SUCCESS: Got {len(df)} bars")
            print(f"   ✓ First bar: {df.index[0]}")
            print(f"   ✓ Price: ${df.iloc[0]['close']:.2f}")
            print(f"\n2. Verifying timestamp accuracy...")

            # Check first bar is at 9:30
            first_time = df.index[0]
            if first_time.hour == 9 and first_time.minute == 30:
                print(f"   ✓ First bar at correct time: {first_time}")
            else:
                print(f"   ✗ WARNING: First bar at {first_time}, expected 9:30")

            # Check bars are 60s apart
            all_correct = True
            for i in range(1, len(df)):
                time_diff = (df.index[i] - df.index[i-1]).total_seconds()
                if time_diff != 60:
                    print(f"   ✗ WARNING: Bar {i} is {time_diff}s after previous (expected 60s)")
                    all_correct = False

            if all_correct:
                print(f"   ✓ All bars are 60 seconds apart")

            print(f"\n{'='*80}")
            print(f"✓ INDICES SUBSCRIPTION IS ACTIVE AND WORKING")
            print(f"{'='*80}")
            print(f"\nNext steps:")
            print(f"1. Run full index tests:")
            print(f"   pytest tests/backtest/test_index_data_verification.py -v")
            print(f"\n2. Run comparison tests vs Polygon:")
            print(f"   pytest tests/backtest/test_thetadata_vs_polygon.py::TestThetaDataVsPolygonComparison::test_index_price_comparison -v")
            print(f"\n3. Clear cache before testing:")
            print(f"   rm -rf ~/Library/Caches/lumibot/1.0/thetadata/")
            return True
        else:
            print(f"   ✗ FAILED: No data returned")
            return False

    except Exception as e:
        print(f"   ✗ ERROR: {str(e)}")

        if "PERMISSION" in str(e).upper():
            print(f"\n{'='*80}")
            print(f"✗ INDICES SUBSCRIPTION NOT YET ACTIVE")
            print(f"{'='*80}")
            print(f"\nThe subscription upgrade hasn't been activated yet.")
            print(f"Wait for ThetaData support to resolve the ticket, then:")
            print(f"1. Restart ThetaTerminal: pkill -f ThetaTerminal.jar")
            print(f"2. Run this script again: python3 verify_index_access.py")

        return False


def test_vix_access():
    """Quick check if VIX is accessible."""
    print(f"\n3. Testing VIX index data access...")

    username = os.environ.get("THETADATA_USERNAME")
    password = os.environ.get("THETADATA_PASSWORD")

    asset = Asset("VIX", asset_type="index")

    try:
        df = thetadata_helper.get_price_data(
            username=username,
            password=password,
            asset=asset,
            start=datetime.datetime(2024, 8, 1, 9, 30),
            end=datetime.datetime(2024, 8, 1, 9, 35),
            timespan="minute"
        )

        if df is not None and len(df) > 0:
            print(f"   ✓ SUCCESS: Got {len(df)} bars")
            print(f"   ✓ First bar: {df.index[0]}")
            print(f"   ✓ Price: {df.iloc[0]['close']:.2f}")
            return True
        else:
            print(f"   ✗ FAILED: No data returned")
            return False

    except Exception as e:
        print(f"   ✗ ERROR: {str(e)}")
        return False


if __name__ == "__main__":
    spx_ok = test_index_access()

    if spx_ok:
        test_vix_access()
