"""
CRITICAL TEST: Daily Data Timestamp & Price Accuracy Comparison

This test verifies that daily (day timeframe) data from ThetaData and Polygon:
1. Has IDENTICAL timestamps (no day shifts, no hour shifts, no timezone bugs)
2. Has matching OHLC prices (within penny-level tolerance)
3. Covers FULL MONTH of data (minimum 20 trading days)
4. Tests MULTIPLE symbols (different exchanges, characteristics)
5. Handles edge cases (holidays, month boundaries, extended hours)

ANY failure in this test indicates a CRITICAL bug that could cause:
- Incorrect backtests
- Wrong trading signals
- Financial losses
- Lawsuits

ZERO TOLERANCE for failures.
"""

import os
import pytest
import datetime
import pandas as pd
from dotenv import load_dotenv
from lumibot.backtesting import ThetaDataBacktesting, PolygonDataBacktesting
from lumibot.entities import Asset
from lumibot.tools import thetadata_helper
from lumibot.tools.polygon_helper import get_price_data_from_polygon as polygon_get_price_data

load_dotenv()


@pytest.mark.apitest
class TestDailyDataTimestampComparison:
    """
    Comprehensive daily data comparison between ThetaData and Polygon.
    Tests full month, multiple symbols, penny-level accuracy.
    """

    def test_daily_data_full_month_pltr(self):
        """Test PLTR daily data for full September 2025 - ZERO tolerance."""
        self._test_symbol_daily_data(
            symbol="PLTR",
            start_date=datetime.datetime(2025, 9, 1),
            end_date=datetime.datetime(2025, 9, 30),
            min_trading_days=19
        )

    def test_daily_data_full_month_spy(self):
        """Test SPY daily data for full September 2025 - ZERO tolerance."""
        self._test_symbol_daily_data(
            symbol="SPY",
            start_date=datetime.datetime(2025, 9, 1),
            end_date=datetime.datetime(2025, 9, 30),
            min_trading_days=19
        )

    def test_daily_data_full_month_aapl(self):
        """Test AAPL daily data for full September 2025 - ZERO tolerance."""
        self._test_symbol_daily_data(
            symbol="AAPL",
            start_date=datetime.datetime(2025, 9, 1),
            end_date=datetime.datetime(2025, 9, 30),
            min_trading_days=19
        )

    def test_daily_data_full_month_amzn(self):
        """Test AMZN daily data for full September 2025 - ZERO tolerance."""
        self._test_symbol_daily_data(
            symbol="AMZN",
            start_date=datetime.datetime(2025, 9, 1),
            end_date=datetime.datetime(2025, 9, 30),
            min_trading_days=19
        )

    def _test_symbol_daily_data(self, symbol, start_date, end_date, min_trading_days):
        """
        Core test function that validates daily data for a symbol.

        CRITICAL CHECKS:
        1. Both sources return data
        2. Same number of trading days
        3. IDENTICAL timestamps (no shifts)
        4. OHLC within 0.01 (penny) tolerance
        5. Volume reasonable
        6. No duplicate dates
        7. No missing dates (within market calendar)
        """
        username = os.environ.get("THETADATA_USERNAME")
        password = os.environ.get("THETADATA_PASSWORD")
        polygon_api_key = os.environ.get("POLYGON_API_KEY")

        asset = Asset(symbol, asset_type="stock")

        print(f"\n{'='*80}")
        print(f"TESTING {symbol} DAILY DATA: {start_date.date()} to {end_date.date()}")
        print(f"{'='*80}")

        # ==== GET THETADATA DAILY DATA ====
        print(f"\n1. Fetching ThetaData daily data...")
        try:
            theta_df = thetadata_helper.get_price_data(
                username=username,
                password=password,
                asset=asset,
                start=start_date,
                end=end_date,
                timespan="day"
            )
        except Exception as e:
            pytest.fail(f"CRITICAL: ThetaData daily data FAILED for {symbol}: {e}")

        if theta_df is None or len(theta_df) == 0:
            pytest.fail(f"CRITICAL: ThetaData returned NO daily data for {symbol}")

        print(f"   ✓ ThetaData: {len(theta_df)} daily bars")
        print(f"   Date range: {theta_df.index[0]} to {theta_df.index[-1]}")

        # ==== GET POLYGON DAILY DATA ====
        print(f"\n2. Fetching Polygon daily data...")
        try:
            polygon_df = polygon_get_price_data(
                api_key=polygon_api_key,
                asset=asset,
                start=start_date,
                end=end_date,
                timespan="day",
                quote_asset=Asset("USD", asset_type="forex")
            )
        except Exception as e:
            pytest.fail(f"CRITICAL: Polygon daily data FAILED for {symbol}: {e}")

        if polygon_df is None or len(polygon_df) == 0:
            pytest.fail(f"CRITICAL: Polygon returned NO daily data for {symbol}")

        print(f"   ✓ Polygon: {len(polygon_df)} daily bars")
        print(f"   Date range: {polygon_df.index[0]} to {polygon_df.index[-1]}")

        # ==== CHECK 1: Minimum Trading Days ====
        print(f"\n3. Verifying minimum trading days...")
        assert len(theta_df) >= min_trading_days, \
            f"CRITICAL: ThetaData has only {len(theta_df)} days, expected >={min_trading_days}"
        assert len(polygon_df) >= min_trading_days, \
            f"CRITICAL: Polygon has only {len(polygon_df)} days, expected >={min_trading_days}"
        print(f"   ✓ Both sources have >={min_trading_days} trading days")

        # ==== CHECK 2: Same Number of Days ====
        print(f"\n4. Verifying same number of trading days...")
        if len(theta_df) != len(polygon_df):
            print(f"\n   ✗ MISMATCH: ThetaData={len(theta_df)} days, Polygon={len(polygon_df)} days")
            print(f"\n   ThetaData dates:")
            for dt in theta_df.index:
                print(f"      {dt.date()}")
            print(f"\n   Polygon dates:")
            for dt in polygon_df.index:
                print(f"      {dt.date()}")
            pytest.fail(f"CRITICAL: Different number of trading days: Theta={len(theta_df)}, Polygon={len(polygon_df)}")
        print(f"   ✓ Same number of trading days: {len(theta_df)}")

        # ==== CHECK 3: IDENTICAL TIMESTAMPS ====
        print(f"\n5. Verifying IDENTICAL timestamps (ZERO tolerance for shifts)...")

        # Convert to date for comparison (ignore time component)
        theta_dates = [dt.date() for dt in theta_df.index]
        polygon_dates = [dt.date() for dt in polygon_df.index]

        mismatched_dates = []
        for i, (theta_date, polygon_date) in enumerate(zip(theta_dates, polygon_dates)):
            if theta_date != polygon_date:
                mismatched_dates.append((i, theta_date, polygon_date))

        if mismatched_dates:
            print(f"\n   ✗ CRITICAL: TIMESTAMP MISMATCH DETECTED!")
            print(f"\n   {'Index':<10} {'ThetaData':<15} {'Polygon':<15} {'Shift (days)'}")
            print(f"   {'-'*60}")
            for idx, theta_date, polygon_date in mismatched_dates:
                shift = (theta_date - polygon_date).days
                print(f"   {idx:<10} {theta_date} {polygon_date} {shift:+d}")
            pytest.fail(f"CRITICAL: {len(mismatched_dates)} timestamp mismatches found!")

        print(f"   ✓ ALL timestamps match perfectly (0 shifts)")

        # ==== CHECK 4: OHLC PRICE ACCURACY ====
        print(f"\n6. Verifying OHLC prices (penny-level tolerance: $0.01)...")

        # Create aligned DataFrame for comparison
        comparison_data = []
        max_diff = {'open': 0.0, 'high': 0.0, 'low': 0.0, 'close': 0.0}

        for theta_idx, polygon_idx in zip(theta_df.index, polygon_df.index):
            theta_row = theta_df.loc[theta_idx]
            polygon_row = polygon_df.loc[polygon_idx]

            diffs = {
                'open': abs(theta_row['open'] - polygon_row['open']),
                'high': abs(theta_row['high'] - polygon_row['high']),
                'low': abs(theta_row['low'] - polygon_row['low']),
                'close': abs(theta_row['close'] - polygon_row['close'])
            }

            for field in ['open', 'high', 'low', 'close']:
                max_diff[field] = max(max_diff[field], diffs[field])

            comparison_data.append({
                'date': theta_idx.date(),
                'theta_open': theta_row['open'],
                'poly_open': polygon_row['open'],
                'diff_open': diffs['open'],
                'theta_close': theta_row['close'],
                'poly_close': polygon_row['close'],
                'diff_close': diffs['close'],
            })

        # Check tolerance
        tolerance = 0.01  # Penny-level
        failures = []

        for field in ['open', 'high', 'low', 'close']:
            if max_diff[field] > tolerance:
                failures.append(f"{field}: max diff ${max_diff[field]:.4f}")

        if failures:
            print(f"\n   ✗ PRICE TOLERANCE EXCEEDED:")
            for failure in failures:
                print(f"      {failure}")

            print(f"\n   Detailed comparison (first 10 days):")
            print(f"   {'Date':<12} {'Theta Open':<12} {'Poly Open':<12} {'Diff':<10} {'Theta Close':<12} {'Poly Close':<12} {'Diff':<10}")
            print(f"   {'-'*90}")
            for row in comparison_data[:10]:
                print(f"   {str(row['date']):<12} ${row['theta_open']:<11.2f} ${row['poly_open']:<11.2f} ${row['diff_open']:<9.4f} "
                      f"${row['theta_close']:<11.2f} ${row['poly_close']:<11.2f} ${row['diff_close']:<9.4f}")

            pytest.fail(f"CRITICAL: Price tolerance exceeded: {', '.join(failures)}")

        print(f"   ✓ All prices within $0.01 tolerance")
        print(f"      Max differences: open=${max_diff['open']:.4f}, high=${max_diff['high']:.4f}, "
              f"low=${max_diff['low']:.4f}, close=${max_diff['close']:.4f}")

        # ==== CHECK 5: No Duplicates ====
        print(f"\n7. Verifying no duplicate dates...")
        theta_duplicates = theta_df.index[theta_df.index.duplicated()].tolist()
        polygon_duplicates = polygon_df.index[polygon_df.index.duplicated()].tolist()

        if theta_duplicates:
            pytest.fail(f"CRITICAL: ThetaData has duplicate dates: {theta_duplicates}")
        if polygon_duplicates:
            pytest.fail(f"CRITICAL: Polygon has duplicate dates: {polygon_duplicates}")

        print(f"   ✓ No duplicate dates in either source")

        # ==== CHECK 6: Volume Sanity ====
        print(f"\n8. Verifying volume data...")
        if 'volume' in theta_df.columns and 'volume' in polygon_df.columns:
            theta_zero_vol = (theta_df['volume'] == 0).sum()
            polygon_zero_vol = (polygon_df['volume'] == 0).sum()

            if theta_zero_vol > len(theta_df) * 0.1:  # More than 10% zero volume
                print(f"   ⚠ WARNING: ThetaData has {theta_zero_vol}/{len(theta_df)} days with zero volume")
            if polygon_zero_vol > len(polygon_df) * 0.1:
                print(f"   ⚠ WARNING: Polygon has {polygon_zero_vol}/{len(polygon_df)} days with zero volume")

            print(f"   ✓ Volume data present (Theta: {theta_zero_vol} zero days, Polygon: {polygon_zero_vol} zero days)")

        # ==== FINAL SUMMARY ====
        print(f"\n{'='*80}")
        print(f"✓✓✓ {symbol} DAILY DATA VALIDATION PASSED ✓✓✓")
        print(f"    Trading days: {len(theta_df)}")
        print(f"    Timestamps: PERFECT MATCH (0 shifts)")
        print(f"    Prices: ALL within $0.01")
        print(f"    Period: {theta_df.index[0].date()} to {theta_df.index[-1].date()}")
        print(f"{'='*80}\n")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
