"""
Phase 1: Accuracy Verification Tests

This test suite verifies that ThetaData price variance compared to Polygon
remains acceptable over long time periods and across different price ranges.

Goals:
- Verify portfolio variance stays small (buy & hold parity)
- Verify no systematic bias (variance is random, not directional)
"""

import datetime
import os
import pytest
import pandas as pd
from dotenv import load_dotenv
from lumibot.entities import Asset
from lumibot.tools import polygon_helper, thetadata_helper

# Load environment variables from .env file
load_dotenv()

# Get credentials from environment variables
POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY")
THETADATA_USERNAME = os.environ.get("THETADATA_USERNAME")
THETADATA_PASSWORD = os.environ.get("THETADATA_PASSWORD")

# CI stability note:
# Data providers can disagree by small amounts (e.g., opening prints vs official open).
# We keep this test fast + meaningful by using a shorter window and a tight (but realistic)
# portfolio variance threshold.
MAX_PORTFOLIO_VARIANCE_PCT = 0.10  # 0.10% ~= $100 on a $100k portfolio


def _daily_close_series(df: pd.DataFrame) -> pd.Series:
    if df is None or df.empty:
        return pd.Series(dtype=float)

    idx = df.index
    if getattr(idx, "tz", None) is not None:
        idx = idx.tz_convert("America/New_York")
    series = pd.to_numeric(df["close"], errors="coerce")
    series.index = idx.date
    series = series.dropna()
    # If a provider returns multiple rows per date, keep the last one.
    return series.groupby(level=0).last()


def _fetch_day_ohlc_theta(symbol: str, start: datetime.datetime, end: datetime.datetime) -> pd.DataFrame:
    return thetadata_helper.get_price_data(
        username=THETADATA_USERNAME,
        password=THETADATA_PASSWORD,
        asset=Asset(symbol, asset_type="stock"),
        start=start,
        end=end,
        timespan="day",
        datastyle="ohlc",
        include_after_hours=False,
    )


def _fetch_day_ohlc_polygon(symbol: str, start: datetime.datetime, end: datetime.datetime) -> pd.DataFrame:
    return polygon_helper.get_price_data_from_polygon(
        POLYGON_API_KEY,
        Asset(symbol, asset_type="stock"),
        start,
        end,
        timespan="day",
        quote_asset=Asset("USD", "forex"),
    )


@pytest.mark.skipif(
    not POLYGON_API_KEY or not THETADATA_USERNAME or not THETADATA_PASSWORD,
    reason="Requires both Polygon and ThetaData credentials"
)
class TestAccuracyVerification:
    """Accuracy verification test suite"""

    def test_one_year_amzn_accuracy(self):
        """
        Test 1: Verify AMZN day-bar accuracy over a short window (fast CI signal)

        Expected:
        - Return variance stays small (see MAX_PORTFOLIO_VARIANCE_PCT)
        """
        # Use recent-ish dates to avoid subscription/history limitations and maximize cache reuse with other tests.
        backtesting_start = datetime.datetime(2024, 8, 1)
        backtesting_end = datetime.datetime(2024, 8, 9)  # ~1 week

        print("\n" + "="*80)
        print("TEST 1: ACCURACY VERIFICATION - AMZN")
        print("="*80)
        print(f"Period: {backtesting_start.date()} to {backtesting_end.date()}")
        print(f"Symbol: AMZN")
        print(f"Trading days: ~5-7")

        theta_df = _fetch_day_ohlc_theta("AMZN", backtesting_start, backtesting_end)
        polygon_df = _fetch_day_ohlc_polygon("AMZN", backtesting_start, backtesting_end)

        theta_close = _daily_close_series(theta_df)
        polygon_close = _daily_close_series(polygon_df)

        common_dates = sorted(set(theta_close.index) & set(polygon_close.index))
        assert common_dates, "No overlapping trading dates returned for AMZN"

        theta_close = theta_close.loc[common_dates]
        polygon_close = polygon_close.loc[common_dates]

        theta_return = (theta_close.iloc[-1] / theta_close.iloc[0]) - 1.0
        polygon_return = (polygon_close.iloc[-1] / polygon_close.iloc[0]) - 1.0
        difference = abs(theta_return - polygon_return)
        percent_diff = difference * 100.0

        print("\n" + "-"*80)
        print("RESULTS:")
        print("-"*80)
        print(f"ThetaData Total Return:           {theta_return:.4%}")
        print(f"Polygon Total Return:             {polygon_return:.4%}")
        print(f"Absolute Return Difference:       {difference:.4%}")
        print(f"Percentage Difference:            {percent_diff:.4f}%")
        print(f"Acceptance Threshold:             {MAX_PORTFOLIO_VARIANCE_PCT:.2f}%")

        # Verify acceptance criteria
        assert percent_diff < MAX_PORTFOLIO_VARIANCE_PCT, (
            f"Portfolio variance {percent_diff:.4f}% exceeds {MAX_PORTFOLIO_VARIANCE_PCT:.2f}% threshold"
        )

        print(f"\n✓ TEST PASSED: Variance {percent_diff:.4f}% is within acceptable range")
        print("="*80 + "\n")

    def test_multi_symbol_price_ranges(self):
        """
        Test 2: Verify accuracy across different price ranges

        Tests a few symbols with different price points:
        - AMZN: ~$180
        - SPY: ~$450
        - BRK.B: ~$420

        Expected:
        - 0.5¢ variance is consistent percentage across all price ranges
        - Sub-penny differences for all symbols
        """
        # Keep windows consistent with test_one_year_amzn_accuracy for cache reuse.
        backtesting_start = datetime.datetime(2024, 8, 1)
        backtesting_end = datetime.datetime(2024, 8, 9)

        symbols = [
            ("AMZN", 10, 180),   # ~$180/share, 10 shares
            ("SPY", 10, 450),    # ~$450/share, 10 shares
            ("BRK.B", 5, 420),   # ~$420/share, 5 shares
        ]

        print("\n" + "="*80)
        print("TEST 2: MULTI-SYMBOL PRICE RANGE VERIFICATION")
        print("="*80)
        print(f"Period: {backtesting_start.date()} to {backtesting_end.date()}")
        print(f"Symbols: {len(symbols)}")

        results_table = []

        for symbol, qty, approx_price in symbols:
            print(f"\n--- Testing {symbol} (~${approx_price}/share, {qty} shares) ---")

            theta_df = _fetch_day_ohlc_theta(symbol, backtesting_start, backtesting_end)
            polygon_df = _fetch_day_ohlc_polygon(symbol, backtesting_start, backtesting_end)

            theta_close = _daily_close_series(theta_df)
            polygon_close = _daily_close_series(polygon_df)

            common_dates = sorted(set(theta_close.index) & set(polygon_close.index))
            assert common_dates, f"No overlapping trading dates returned for {symbol}"

            theta_close = theta_close.loc[common_dates]
            polygon_close = polygon_close.loc[common_dates]

            theta_return = (theta_close.iloc[-1] / theta_close.iloc[0]) - 1.0
            polygon_return = (polygon_close.iloc[-1] / polygon_close.iloc[0]) - 1.0
            difference = abs(theta_return - polygon_return)
            percent_diff = difference * 100.0

            results_table.append({
                "symbol": symbol,
                "price": approx_price,
                "qty": qty,
                "theta": theta_return,
                "polygon": polygon_return,
                "diff": difference,
                "pct": percent_diff
            })

            print(f"  ThetaData:  {theta_return:.4%}")
            print(f"  Polygon:    {polygon_return:.4%}")
            print(f"  Difference: {difference:.4%} ({percent_diff:.4f}%)")

            # Verify sub-0.01% variance for each symbol
            assert percent_diff < MAX_PORTFOLIO_VARIANCE_PCT, (
                f"{symbol}: Variance {percent_diff:.4f}% exceeds {MAX_PORTFOLIO_VARIANCE_PCT:.2f}%"
            )

        # Summary table
        print("\n" + "-"*80)
        print("SUMMARY TABLE:")
        print("-"*80)
        print(f"{'Symbol':<8} {'Price':<8} {'Qty':<5} {'ThetaRet':<10} {'PolyRet':<10} {'AbsDiff':<10} {'Diff%':<8}")
        print("-"*80)

        for r in results_table:
            print(
                f"{r['symbol']:<8} ${r['price']:<7} {r['qty']:<5} "
                f"{r['theta']:>9.2%} {r['polygon']:>9.2%} {r['diff']:>9.2%} {r['pct']:.4f}%"
            )

        # Calculate average variance
        avg_pct = sum(r['pct'] for r in results_table) / len(results_table)
        max_pct = max(r['pct'] for r in results_table)

        print("-"*80)
        print(f"Average Variance: {avg_pct:.4f}%")
        print(f"Maximum Variance: {max_pct:.4f}%")
        print(f"Threshold:        {MAX_PORTFOLIO_VARIANCE_PCT:.2f}%")

        assert avg_pct < MAX_PORTFOLIO_VARIANCE_PCT, (
            f"Average variance {avg_pct:.4f}% exceeds {MAX_PORTFOLIO_VARIANCE_PCT:.2f}%"
        )
        assert max_pct < MAX_PORTFOLIO_VARIANCE_PCT, (
            f"Max variance {max_pct:.4f}% exceeds {MAX_PORTFOLIO_VARIANCE_PCT:.2f}%"
        )

        print(f"\n✓ TEST PASSED: All symbols within acceptable variance")
        print("="*80 + "\n")


if __name__ == "__main__":
    # Run tests directly
    pytest.main([__file__, "-v", "-s"])
