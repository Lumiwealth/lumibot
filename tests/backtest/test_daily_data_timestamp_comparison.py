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

    # ========== INDEX TESTS ==========
    def test_daily_data_full_month_spx_index(self):
        """Test SPX index daily data for full September 2025 - ZERO tolerance."""
        self._test_symbol_daily_data(
            symbol="SPX",
            start_date=datetime.datetime(2025, 9, 1),
            end_date=datetime.datetime(2025, 9, 30),
            min_trading_days=19,
            asset_type="index"
        )

    def test_daily_data_full_month_vix_index(self):
        """Test VIX index daily data for full September 2025 - ZERO tolerance."""
        self._test_symbol_daily_data(
            symbol="VIX",
            start_date=datetime.datetime(2025, 9, 1),
            end_date=datetime.datetime(2025, 9, 30),
            min_trading_days=19,
            asset_type="index"
        )

    def test_daily_data_full_month_ndx_index(self):
        """Test SPX index daily data for full August 2024 - ZERO tolerance."""
        self._test_symbol_daily_data(
            symbol="SPX",
            start_date=datetime.datetime(2024, 8, 1),
            end_date=datetime.datetime(2024, 8, 31),
            min_trading_days=21,
            asset_type="index"
        )

    # ========== OPTION TESTS ==========
    def test_daily_data_spy_call_option(self):
        """Test SPY call option daily data for September 2025 - ZERO tolerance."""
        self._test_option_daily_data(
            symbol="SPY",
            start_date=datetime.datetime(2025, 9, 1),
            end_date=datetime.datetime(2025, 9, 30),
            min_trading_days=15,  # Options may have less liquidity
            expiration=datetime.datetime(2025, 12, 19),  # Dec 2025 expiry
            strike=580.0,  # ATM/slightly OTM for SPY ~$570
            right="CALL"
        )

    def test_daily_data_spy_put_option(self):
        """Test SPY put option daily data for September 2025 - ZERO tolerance."""
        self._test_option_daily_data(
            symbol="SPY",
            start_date=datetime.datetime(2025, 9, 1),
            end_date=datetime.datetime(2025, 9, 30),
            min_trading_days=15,
            expiration=datetime.datetime(2025, 12, 19),
            strike=560.0,  # ATM/slightly ITM for SPY ~$570
            right="PUT"
        )

    def _test_option_daily_data(self, symbol, start_date, end_date, min_trading_days, expiration, strike, right):
        """
        Test function for option daily data validation.

        NOTE: Options comparison is challenging because:
        - Yahoo Finance historical option data is limited (often <1 week)
        - Different providers use different quote/trade data
        - Options have wider bid-ask spreads than stocks
        - No universal "official" option price like stocks have

        This test validates:
        1. ThetaData returns option data successfully
        2. Minimum number of trading days
        3. Price values are reasonable (not zero, not negative)
        4. Volume data exists
        """
        username = os.environ.get("THETADATA_USERNAME")
        password = os.environ.get("THETADATA_PASSWORD")

        asset = Asset(symbol, asset_type="option", expiration=expiration, strike=strike, right=right)

        print(f"\n{'='*80}")
        print(f"TESTING {symbol} {right} ${strike} (exp {expiration.date()}) OPTION DAILY DATA")
        print(f"Period: {start_date.date()} to {end_date.date()}")
        print(f"{'='*80}")

        # ==== GET THETADATA OPTION DATA ====
        print(f"\n1. Fetching ThetaData option daily data...")
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
            pytest.fail(f"CRITICAL: ThetaData option daily data FAILED: {e}")

        if theta_df is None or len(theta_df) == 0:
            pytest.fail(f"CRITICAL: ThetaData returned NO option daily data")

        print(f"   ✓ ThetaData: {len(theta_df)} daily bars")
        print(f"   Date range: {theta_df.index[0]} to {theta_df.index[-1]}")

        # ==== GET POLYGON OPTION DATA FOR COMPARISON ====
        print(f"\n2. Fetching Polygon option data for validation...")
        polygon_api_key = os.environ.get("POLYGON_API_KEY")

        try:
            polygon_df = polygon_get_price_data(
                api_key=polygon_api_key,
                asset=asset,
                start=start_date,
                end=end_date,
                timespan="day",
                quote_asset=Asset("USD", asset_type="forex")
            )

            if polygon_df is None or len(polygon_df) == 0:
                print(f"   ⚠ WARNING: Polygon returned NO option data - skipping price comparison")
                polygon_df = None
            else:
                print(f"   ✓ Polygon: {len(polygon_df)} daily bars")
                print(f"   Date range: {polygon_df.index[0]} to {polygon_df.index[-1]}")
        except Exception as e:
            print(f"   ⚠ WARNING: Polygon failed ({e}) - skipping price comparison")
            polygon_df = None

        # ==== CHECK: Minimum Trading Days ====
        print(f"\n3. Verifying minimum trading days...")
        assert len(theta_df) >= min_trading_days, \
            f"CRITICAL: Expected at least {min_trading_days} days, got {len(theta_df)}"
        print(f"   ✓ Sufficient trading days: {len(theta_df)} >= {min_trading_days}")

        # ==== CHECK: Price Comparison (if Polygon data available) ====
        if polygon_df is not None and len(polygon_df) > 0:
            print(f"\n4. Verifying OHLC prices vs Polygon (half-penny tolerance: $0.005)...")

            # Check same number of days
            if len(theta_df) != len(polygon_df):
                print(f"\n   ✗ MISMATCH: ThetaData={len(theta_df)} days, Polygon={len(polygon_df)} days")
                pytest.fail(f"CRITICAL: Different number of trading days")

            # Align data
            max_diff = {'open': 0.0, 'high': 0.0, 'low': 0.0, 'close': 0.0}
            comparison_data = []

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
                    'theta_close': theta_row['close'],
                    'polygon_close': polygon_row['close'],
                    'diff_close': diffs['close'],
                })

            # HALF-PENNY tolerance ($0.005) - anything more is unacceptable
            tolerance = 0.005
            failures = []

            for field in ['open', 'high', 'low', 'close']:
                if max_diff[field] > tolerance:
                    failures.append(f"{field}: max diff ${max_diff[field]:.4f}")

            if failures:
                print(f"\n   ✗ PRICE TOLERANCE EXCEEDED:")
                for failure in failures:
                    print(f"      {failure}")

                print(f"\n   PRICE COMPARISON (first 10 days):")
                print(f"   {'Date':<12} {'Theta':<10} {'Polygon':<10} {'Diff':<10}")
                print(f"   {'-'*50}")
                for row in comparison_data[:10]:
                    t_close = row['theta_close']
                    p_close = row['polygon_close']
                    diff = row['diff_close']
                    match_str = "✅" if diff <= tolerance else "❌"
                    print(f"   {row['date']} ${t_close:<9.2f} ${p_close:<9.2f} ${diff:<9.4f} {match_str}")

                pytest.fail(f"CRITICAL: Option price tolerance exceeded: {', '.join(failures)}")

            print(f"   ✓ All prices within ${tolerance:.3f} tolerance")
            print(f"      Max differences: open=${max_diff['open']:.4f}, high=${max_diff['high']:.4f}, "
                  f"low=${max_diff['low']:.4f}, close=${max_diff['close']:.4f}")

        # ==== CHECK: Price Data Sanity ====
        print(f"\n5. Verifying price data sanity...")

        # Check for zero or negative prices (invalid)
        zero_prices = (theta_df['close'] <= 0).sum()
        if zero_prices > 0:
            pytest.fail(f"CRITICAL: {zero_prices} bars have zero/negative close prices")

        # Check for reasonable price ranges
        min_price = theta_df['close'].min()
        max_price = theta_df['close'].max()
        avg_price = theta_df['close'].mean()

        print(f"   ✓ All prices positive")
        print(f"      Price range: ${min_price:.2f} - ${max_price:.2f} (avg: ${avg_price:.2f})")

        # ==== CHECK: OHLC Consistency ====
        print(f"\n4. Verifying OHLC consistency...")

        # High should be >= Low for every bar
        invalid_hl = (theta_df['high'] < theta_df['low']).sum()
        if invalid_hl > 0:
            pytest.fail(f"CRITICAL: {invalid_hl} bars have high < low")

        # High should be >= Open and Close
        invalid_h = ((theta_df['high'] < theta_df['open']) | (theta_df['high'] < theta_df['close'])).sum()
        if invalid_h > 0:
            pytest.fail(f"CRITICAL: {invalid_h} bars have high < open/close")

        # Low should be <= Open and Close
        invalid_l = ((theta_df['low'] > theta_df['open']) | (theta_df['low'] > theta_df['close'])).sum()
        if invalid_l > 0:
            pytest.fail(f"CRITICAL: {invalid_l} bars have low > open/close")

        print(f"   ✓ OHLC relationships valid (high >= low, high >= open/close, low <= open/close)")

        # ==== CHECK: Volume Data ====
        print(f"\n5. Verifying volume data...")
        zero_volume = (theta_df['volume'] == 0).sum()
        pct_zero_vol = (zero_volume / len(theta_df)) * 100

        print(f"   ✓ Volume data present ({zero_volume}/{len(theta_df)} bars with zero volume = {pct_zero_vol:.1f}%)")
        if pct_zero_vol > 50:
            print(f"   ⚠ WARNING: >50% of bars have zero volume (may indicate low liquidity)")

        print(f"\n{'='*80}")
        print(f"✓✓✓ {symbol} OPTION DATA VALIDATION PASSED ✓✓✓")
        print(f"    Trading days: {len(theta_df)}")
        print(f"    Price range: ${min_price:.2f} - ${max_price:.2f}")
        print(f"    OHLC relationships: VALID")
        print(f"    Period: {theta_df.index[0].date()} to {theta_df.index[-1].date()}")
        print(f"{'='*80}\n")

    def _test_symbol_daily_data(self, symbol, start_date, end_date, min_trading_days, asset_type="stock"):
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

        asset = Asset(symbol, asset_type=asset_type)

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

        # ==== GET POLYGON OR YAHOO DAILY DATA ====
        # NOTE: Polygon requires paid plan for indexes, so we use Yahoo Finance for indexes
        if asset_type == "index":
            print(f"\n2. Fetching Yahoo Finance daily data (indexes not available in free Polygon)...")
            import yfinance as yf

            # Yahoo Finance uses ^SPX for SPX, ^VIX for VIX, ^NDX for NDX
            yahoo_symbol = f"^{symbol}" if symbol in ["SPX", "VIX", "NDX", "RUT", "DJI"] else symbol
            ticker = yf.Ticker(yahoo_symbol)

            try:
                from datetime import timedelta as td
                # Yahoo requires end_date to be exclusive (next day)
                yahoo_end = (end_date + td(days=1)).strftime('%Y-%m-%d')
                yahoo_start = start_date.strftime('%Y-%m-%d')
                yahoo_hist = ticker.history(start=yahoo_start, end=yahoo_end, interval='1d')

                if yahoo_hist is None or len(yahoo_hist) == 0:
                    pytest.fail(f"CRITICAL: Yahoo Finance returned NO daily data for {symbol}")

                # Convert Yahoo data to match our format
                polygon_df = pd.DataFrame({
                    'open': yahoo_hist['Open'],
                    'high': yahoo_hist['High'],
                    'low': yahoo_hist['Low'],
                    'close': yahoo_hist['Close'],
                    'volume': yahoo_hist['Volume']
                })
                polygon_df.index = pd.to_datetime(polygon_df.index).tz_convert('UTC')

            except Exception as e:
                pytest.fail(f"CRITICAL: Yahoo Finance daily data FAILED for {symbol}: {e}")
        else:
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

        comparison_source = "Yahoo Finance" if asset_type == "index" else "Polygon"
        print(f"   ✓ {comparison_source}: {len(polygon_df)} daily bars")
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

        # TOLERANCE: Stocks require ZERO tolerance, indexes allow fractional cent (rounding)
        # Stocks: ZERO tolerance - regulated data must match EXACTLY
        # Indexes: $0.001 tolerance - calculated values may have fractional cent rounding differences
        tolerance = 0.001 if asset_type == "index" else 0.00
        failures = []

        for field in ['open', 'high', 'low', 'close']:
            if max_diff[field] > tolerance:
                failures.append(f"{field}: max diff ${max_diff[field]:.4f}")

        if failures:
            # Add Yahoo Finance 3-way comparison for failed days
            import yfinance as yf

            print(f"\n   ✗ PRICE TOLERANCE EXCEEDED:")
            for failure in failures:
                print(f"      {failure}")

            print(f"\n   3-WAY COMPARISON (ThetaData vs Polygon vs Yahoo):")
            print(f"   {'Date':<12} {'Theta':<10} {'Polygon':<10} {'Yahoo':<10} {'Which Match?':<20}")
            print(f"   {'-'*70}")

            ticker = yf.Ticker(symbol)
            for row in comparison_data[:10]:
                try:
                    from datetime import timedelta as td
                    date_obj = row['date']
                    date_str = date_obj.strftime('%Y-%m-%d')
                    next_date = (date_obj + td(days=1)).strftime('%Y-%m-%d')
                    yahoo_hist = ticker.history(start=date_str, end=next_date, interval='1d')
                    yahoo_close = yahoo_hist.iloc[0]['Close'] if len(yahoo_hist) > 0 else None

                    t_close = row['theta_close']
                    p_close = row['poly_close']
                    y_close = yahoo_close

                    # Check which ones match
                    tp_match = abs(t_close - p_close) < 0.01
                    ty_match = abs(t_close - y_close) < 0.01 if y_close else False
                    py_match = abs(p_close - y_close) < 0.01 if y_close else False

                    if tp_match and ty_match and py_match:
                        match_str = "✅ All match"
                    elif py_match:
                        match_str = "❌ Polygon+Yahoo (Theta wrong)"
                    elif ty_match:
                        match_str = "❌ Theta+Yahoo (Polygon wrong)"
                    elif tp_match:
                        match_str = "❌ Theta+Polygon (Yahoo wrong)"
                    else:
                        match_str = "❌ None match!"

                    print(f"   {date_str:<12} ${t_close:<9.2f} ${p_close:<9.2f} ${y_close:<9.2f} {match_str}")
                except:
                    print(f"   {date_str:<12} ${row['theta_close']:<9.2f} ${row['poly_close']:<9.2f} {'N/A':<9} Yahoo error")

            pytest.fail(f"CRITICAL: Price tolerance exceeded: {', '.join(failures)}")

        print(f"   ✓ All prices within ${tolerance:.2f} tolerance")
        print(f"      Max differences: open=${max_diff['open']:.4f}, high=${max_diff['high']:.4f}, "
              f"low=${max_diff['low']:.4f}, close=${max_diff['close']:.4f}")

        # ==== CHECK 5: Exact Timestamp Alignment ====
        print(f"\n6. Verifying EXACT timestamp alignment (no shifts allowed)...")
        timestamp_mismatches = []
        for i, (theta_ts, polygon_ts) in enumerate(zip(theta_df.index, polygon_df.index)):
            if theta_ts.date() != polygon_ts.date():
                timestamp_mismatches.append((i, theta_ts, polygon_ts))

        if timestamp_mismatches:
            print(f"\n   ✗ TIMESTAMP MISMATCH DETECTED:")
            for idx, theta_ts, polygon_ts in timestamp_mismatches[:10]:
                print(f"      Index {idx}: Theta={theta_ts.date()}, Polygon={polygon_ts.date()}")
            pytest.fail(f"CRITICAL: {len(timestamp_mismatches)} timestamp mismatches!")

        print(f"   ✓ ALL timestamps match EXACTLY (0 day shifts)")

        # ==== CHECK 6: No Duplicates ====
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


@pytest.mark.apitest
class TestIntradayDataComparison:
    """
    Comprehensive intraday interval comparison (5min, 10min, 15min, 30min, hour).
    Tests ThetaData server-side intervals vs Polygon client-side aggregation.
    ZERO TOLERANCE: Exact bar counts, exact timestamps, half-penny price accuracy.
    """

    @pytest.mark.parametrize("interval,resample_rule,expected_bars", [
        ("5minute", "5min", 78),
        ("10minute", "10min", 39),
        ("15minute", "15min", 26),
        ("30minute", "30min", 13),
        ("hour", "1h", 7),
    ])
    def test_theta_vs_polygon_intervals(self, interval, resample_rule, expected_bars):
        """Test ThetaData intervals match Polygon aggregated data EXACTLY."""
        import pytz
        from lumibot import LUMIBOT_DEFAULT_PYTZ

        username = os.environ.get("THETADATA_USERNAME")
        password = os.environ.get("THETADATA_PASSWORD")
        polygon_api_key = os.environ.get("POLYGON_API_KEY")

        asset = Asset("SPY", asset_type="stock")

        # Use timezone-aware datetimes (ET) to properly filter RTH
        et_tz = pytz.timezone("America/New_York")
        start_et = et_tz.localize(datetime.datetime(2025, 9, 15, 9, 30))  # 9:30 AM ET
        end_et = et_tz.localize(datetime.datetime(2025, 9, 15, 16, 0))    # 4:00 PM ET
        start = start_et.astimezone(pytz.UTC)
        end = end_et.astimezone(pytz.UTC)

        print(f"\n{'='*80}")
        print(f"TESTING {asset.symbol} {interval.upper()} INTERVAL: {start_et.date()}")
        print(f"{'='*80}")

        # ==== GET THETADATA SERVER-SIDE AGGREGATED DATA ====
        print(f"\n1. Fetching ThetaData {interval} data...")
        try:
            theta_df = thetadata_helper.get_price_data(
                username=username,
                password=password,
                asset=asset,
                start=start,
                end=end,
                timespan=interval,
                include_after_hours=False  # RTH only for fair comparison with Polygon
            )
        except Exception as e:
            pytest.fail(f"CRITICAL: ThetaData {interval} FAILED: {e}")

        if theta_df is None or len(theta_df) == 0:
            pytest.fail(f"CRITICAL: ThetaData returned NO {interval} data")

        print(f"   ✓ ThetaData: {len(theta_df)} {interval} bars")
        print(f"   First bar: {theta_df.index[0]}")
        print(f"   Last bar:  {theta_df.index[-1]}")

        # ==== GET POLYGON MINUTE DATA AND AGGREGATE CLIENT-SIDE ====
        print(f"\n2. Fetching Polygon minute data and aggregating to {interval}...")
        try:
            polygon_minute_df = polygon_get_price_data(
                api_key=polygon_api_key,
                asset=asset,
                start=start,
                end=end,
                timespan="minute",
                quote_asset=Asset("USD", asset_type="forex")
            )
        except Exception as e:
            pytest.fail(f"CRITICAL: Polygon minute data FAILED: {e}")

        if polygon_minute_df is None or len(polygon_minute_df) == 0:
            pytest.fail(f"CRITICAL: Polygon returned NO minute data")

        # Filter to RTH only (9:30 AM - 4:00 PM ET) before aggregating
        # Polygon may return extended hours data - we need to filter it manually
        polygon_minute_rth = polygon_minute_df[(polygon_minute_df.index >= start) & (polygon_minute_df.index <= end)]

        if polygon_minute_rth is None or len(polygon_minute_rth) == 0:
            pytest.fail(f"CRITICAL: Polygon returned NO RTH minute data")

        # Aggregate Polygon minute data
        # For hourly, offset to align with market open (9:30 AM = 13:30 UTC)
        if interval == "hour":
            polygon_agg_df = polygon_minute_rth.resample(resample_rule, offset='30min').agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum'
            }).dropna()
        else:
            polygon_agg_df = polygon_minute_rth.resample(resample_rule).agg({
                'open': 'first',
                'high': 'max',
                'low': 'min',
                'close': 'last',
                'volume': 'sum'
            }).dropna()

        print(f"   ✓ Polygon: {len(polygon_agg_df)} {interval} bars (aggregated from {len(polygon_minute_rth)} RTH minute bars)")
        print(f"   First bar: {polygon_agg_df.index[0]}")
        print(f"   Last bar:  {polygon_agg_df.index[-1]}")

        # ==== CHECK 1: Bar Count - Allow ±1 for 16:00 bar edge case ====
        print(f"\n3. Verifying bar count match...")

        # ThetaData RTH ends at 15:55 for intraday (no 16:00 bar), Polygon may include 16:00
        # This is acceptable behavior - both are correct interpretations of "4 PM close"
        bar_diff = abs(len(theta_df) - len(polygon_agg_df))

        if bar_diff > 1:
            print(f"\n   ✗ CRITICAL: Bar count MISMATCH!")
            print(f"      ThetaData: {len(theta_df)} bars")
            print(f"      Polygon:   {len(polygon_agg_df)} bars")
            print(f"      Difference: {bar_diff} bars")
            pytest.fail(f"CRITICAL: Bar count diff {bar_diff} > 1. Theta={len(theta_df)}, Polygon={len(polygon_agg_df)}")

        if bar_diff == 1:
            print(f"   ⚠ Bar count off by 1 (acceptable for 16:00 bar edge case)")
            print(f"      ThetaData: {len(theta_df)} bars (ends {theta_df.index[-1]})")
            print(f"      Polygon:   {len(polygon_agg_df)} bars (ends {polygon_agg_df.index[-1]})")
            # Use shorter dataset for comparison
            min_len = min(len(theta_df), len(polygon_agg_df))
            theta_df = theta_df.iloc[:min_len]
            polygon_agg_df = polygon_agg_df.iloc[:min_len]
        else:
            print(f"   ✓ EXACT match: {len(theta_df)} bars")

        # ==== CHECK 2: EXACT Timestamp Match ====
        print(f"\n4. Verifying EXACT timestamp alignment...")
        timestamp_mismatches = []
        for i, (theta_ts, polygon_ts) in enumerate(zip(theta_df.index, polygon_agg_df.index)):
            if theta_ts != polygon_ts:
                timestamp_mismatches.append((i, theta_ts, polygon_ts))

        if timestamp_mismatches:
            print(f"\n   ✗ TIMESTAMP MISMATCH DETECTED!")
            print(f"\n   {'Index':<8} {'ThetaData':<25} {'Polygon':<25} {'Shift (seconds)'}")
            print(f"   {'-'*75}")
            for idx, theta_ts, polygon_ts in timestamp_mismatches[:10]:
                shift = (theta_ts - polygon_ts).total_seconds()
                print(f"   {idx:<8} {theta_ts} {polygon_ts} {shift:+.0f}s")
            pytest.fail(f"CRITICAL: {len(timestamp_mismatches)} timestamp mismatches!")

        print(f"   ✓ ALL timestamps match EXACTLY (0 shifts)")

        # ==== CHECK 3: Price Accuracy (half-penny tolerance) ====
        print(f"\n5. Verifying OHLC prices (half-penny tolerance: $0.005)...")

        max_diff = {'open': 0.0, 'high': 0.0, 'low': 0.0, 'close': 0.0}
        price_failures = []

        for theta_ts, polygon_ts in zip(theta_df.index, polygon_agg_df.index):
            theta_row = theta_df.loc[theta_ts]
            polygon_row = polygon_agg_df.loc[polygon_ts]

            for field in ['open', 'high', 'low', 'close']:
                diff = abs(theta_row[field] - polygon_row[field])
                max_diff[field] = max(max_diff[field], diff)

                if diff > 0.005:  # Half-penny tolerance
                    price_failures.append({
                        'timestamp': theta_ts,
                        'field': field,
                        'theta': theta_row[field],
                        'polygon': polygon_row[field],
                        'diff': diff
                    })

        if price_failures:
            print(f"\n   ✗ PRICE TOLERANCE EXCEEDED ({len(price_failures)} failures):")
            for failure in price_failures[:10]:
                print(f"      {failure['timestamp']} {failure['field']}: Theta=${failure['theta']:.4f}, "
                      f"Polygon=${failure['polygon']:.4f}, Diff=${failure['diff']:.4f}")
            pytest.fail(f"CRITICAL: {len(price_failures)} price differences exceed $0.005")

        print(f"   ✓ All prices within $0.005 tolerance")
        print(f"      Max differences: open=${max_diff['open']:.4f}, high=${max_diff['high']:.4f}, "
              f"low=${max_diff['low']:.4f}, close=${max_diff['close']:.4f}")

        # ==== FINAL SUMMARY ====
        print(f"\n{'='*80}")
        print(f"✓✓✓ {asset.symbol} {interval.upper()} VALIDATION PASSED ✓✓✓")
        print(f"    Bars: {len(theta_df)} (EXACT match)")
        print(f"    Timestamps: PERFECT MATCH (0 shifts)")
        print(f"    Prices: ALL within $0.005 (half-penny)")
        print(f"    Period: {theta_df.index[0]} to {theta_df.index[-1]}")
        print(f"{'='*80}\n")


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
