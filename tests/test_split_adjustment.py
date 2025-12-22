"""
Comprehensive Split Adjustment Tests for ThetaData

This test file covers split handling across all ThetaData functions:
- _apply_corporate_actions_to_frame() - price/dividend adjustments
- _get_option_query_strike() - option strike conversion for API queries
- _normalize_split_events() - split event parsing
- _normalize_dividend_events() - dividend event parsing

Known splits used for testing:
- GOOG: 20:1 split on July 15, 2022
- AAPL: 4:1 split on August 31, 2020
- TSLA: 3:1 split on August 25, 2022

Created: 2025-12-11
Purpose: Prevent regression of split-related bugs
"""

import pytest
import pandas as pd
import numpy as np
from datetime import date, datetime
from unittest.mock import patch, MagicMock
import pytz

from lumibot.entities import Asset
from lumibot.tools import thetadata_helper


# =============================================================================
# TEST FIXTURES - Known Split Data
# =============================================================================

@pytest.fixture
def goog_split_data():
    """GOOG 20:1 split on July 15, 2022"""
    return {
        "symbol": "GOOG",
        "split_date": date(2022, 7, 15),
        "ratio": 20.0,  # 20-for-1 split
        "pre_split_price": 2240.0,  # Approximate price before split
        "post_split_price": 112.0,  # 2240 / 20
    }


@pytest.fixture
def aapl_split_data():
    """AAPL 4:1 split on August 31, 2020"""
    return {
        "symbol": "AAPL",
        "split_date": date(2020, 8, 31),
        "ratio": 4.0,  # 4-for-1 split
        "pre_split_price": 500.0,  # Approximate price before split
        "post_split_price": 125.0,  # 500 / 4
    }


@pytest.fixture
def tsla_split_data():
    """TSLA 3:1 split on August 25, 2022"""
    return {
        "symbol": "TSLA",
        "split_date": date(2022, 8, 25),
        "ratio": 3.0,  # 3-for-1 split
        "pre_split_price": 900.0,  # Approximate price before split
        "post_split_price": 300.0,  # 900 / 3
    }


@pytest.fixture
def mock_goog_splits():
    """Mock splits DataFrame for GOOG"""
    return pd.DataFrame({
        "event_date": pd.to_datetime(["2022-07-15"], utc=True),
        "ratio": [20.0],
        "symbol": ["GOOG"],
    })


@pytest.fixture
def mock_aapl_splits():
    """Mock splits DataFrame for AAPL"""
    return pd.DataFrame({
        "event_date": pd.to_datetime(["2020-08-31"], utc=True),
        "ratio": [4.0],
        "symbol": ["AAPL"],
    })


# =============================================================================
# TESTS FOR _get_option_query_strike()
# =============================================================================

class TestGetOptionQueryStrike:
    """Tests for converting split-adjusted strikes back to original strikes"""

    @patch("lumibot.tools.thetadata_helper._get_theta_splits")
    def test_goog_strike_adjustment_for_pre_split_option(self, mock_get_splits, goog_split_data, mock_goog_splits):
        """
        Test: A strategy holding a split-adjusted $66 strike should query ThetaData for $1320 strike.

        Scenario:
        - GOOG had a 20:1 split in July 2022
        - An option with original strike $1320 became $66 after adjustment
        - When querying historical data for this option, we need to use $1320
        """
        mock_get_splits.return_value = mock_goog_splits

        # Create an option with split-adjusted strike (as the strategy sees it)
        option_asset = Asset(
            symbol="GOOG",
            asset_type="option",
            expiration=date(2020, 4, 17),  # Pre-split option
            strike=66.0,  # Split-adjusted strike (1320 / 20)
            right="CALL",
        )

        # Get the original strike for API query
        original_strike = thetadata_helper._get_option_query_strike(
            option_asset
        )

        # Should return original pre-split strike
        assert original_strike == pytest.approx(1320.0, rel=0.01), \
            f"Expected strike 1320.0 but got {original_strike}"

    @patch("lumibot.tools.thetadata_helper._get_theta_splits")
    def test_no_split_returns_original_strike(self, mock_get_splits):
        """Test: If no splits occurred, return the original strike unchanged"""
        mock_get_splits.return_value = pd.DataFrame()  # No splits

        option_asset = Asset(
            symbol="SPY",
            asset_type="option",
            expiration=date(2024, 1, 19),
            strike=450.0,
            right="CALL",
        )

        original_strike = thetadata_helper._get_option_query_strike(
            option_asset
        )

        assert original_strike == 450.0

    @patch("lumibot.tools.thetadata_helper._get_theta_splits")
    def test_multiple_splits_cumulative_factor(self, mock_get_splits):
        """Test: Multiple splits should apply cumulative factor"""
        # Mock two splits: 2:1 and 3:1
        mock_get_splits.return_value = pd.DataFrame({
            "event_date": pd.to_datetime(["2021-01-15", "2022-06-15"], utc=True),
            "ratio": [2.0, 3.0],
            "symbol": ["TEST", "TEST"],
        })

        option_asset = Asset(
            symbol="TEST",
            asset_type="option",
            expiration=date(2020, 6, 19),  # Before both splits
            strike=10.0,  # Adjusted strike (60 / 6)
            right="CALL",
        )

        original_strike = thetadata_helper._get_option_query_strike(
            option_asset
        )

        # Should multiply by cumulative factor: 10.0 * 2.0 * 3.0 = 60.0
        assert original_strike == pytest.approx(60.0, rel=0.01)

    @patch("lumibot.tools.thetadata_helper._get_theta_splits")
    def test_timezone_aware_split_comparison(self, mock_get_splits):
        """
        Test: Timezone-aware event_date should work with timezone-naive as_of_date

        This tests the bug fix where comparing timezone-aware DataFrame columns
        with timezone-naive Timestamps caused TypeError.
        """
        # Split data with UTC timezone
        mock_get_splits.return_value = pd.DataFrame({
            "event_date": pd.to_datetime(["2022-07-15"], utc=True),
            "ratio": [20.0],
            "symbol": ["GOOG"],
        })

        option_asset = Asset(
            symbol="GOOG",
            asset_type="option",
            expiration=date(2020, 3, 20),  # Timezone-naive date
            strike=66.0,
            right="CALL",
        )

        # Should not raise TypeError
        original_strike = thetadata_helper._get_option_query_strike(
            option_asset
        )

        # Should still compute correct strike
        assert original_strike == pytest.approx(1320.0, rel=0.01)


# =============================================================================
# TESTS FOR _apply_corporate_actions_to_frame()
# =============================================================================

class TestApplyCorporateActionsToFrame:
    """Tests for price and dividend split adjustments"""

    @patch("lumibot.tools.thetadata_helper._get_theta_dividends")
    @patch("lumibot.tools.thetadata_helper._get_theta_splits")
    def test_goog_price_adjustment_for_historical_data(self, mock_splits, mock_dividends, goog_split_data, mock_goog_splits):
        """
        Test: March 2020 GOOG prices should be divided by 20 after July 2022 split

        This is the core test for the issue we've been debugging:
        - Fetching March 2020 data in 2025
        - Must apply July 2022 split to make prices comparable to current
        """
        mock_dividends.return_value = pd.DataFrame()
        mock_splits.return_value = mock_goog_splits

        asset = Asset(symbol="GOOG", asset_type="stock")

        # Create March 2020 price data (pre-split prices)
        index = pd.to_datetime(["2020-03-12", "2020-03-13", "2020-03-16"], utc=True)
        frame = pd.DataFrame({
            "open": [2400.0, 2300.0, 2100.0],
            "high": [2450.0, 2350.0, 2150.0],
            "low": [2350.0, 2250.0, 2050.0],
            "close": [2380.0, 2280.0, 2080.0],
            "volume": [1000, 1100, 1200],
        }, index=index)

        enriched = thetadata_helper._apply_corporate_actions_to_frame(
            asset,
            frame.copy(),
            date(2020, 3, 1),
            date(2020, 3, 31),
        )

        # Prices should be divided by 20
        assert enriched["close"].iloc[0] == pytest.approx(119.0, rel=0.01)  # 2380 / 20
        assert enriched["close"].iloc[1] == pytest.approx(114.0, rel=0.01)  # 2280 / 20
        assert enriched["close"].iloc[2] == pytest.approx(104.0, rel=0.01)  # 2080 / 20

        # Volume should be multiplied by 20
        assert enriched["volume"].iloc[0] == pytest.approx(20000.0, rel=0.01)  # 1000 * 20

    @patch("lumibot.tools.thetadata_helper._get_theta_dividends")
    @patch("lumibot.tools.thetadata_helper._get_theta_splits")
    def test_idempotency_prevents_double_adjustment(self, mock_splits, mock_dividends, mock_goog_splits):
        """
        Test: Calling _apply_corporate_actions_to_frame twice should NOT double-adjust

        This tests the idempotency fix that prevents cached data from being adjusted multiple times.
        """
        mock_dividends.return_value = pd.DataFrame()
        mock_splits.return_value = mock_goog_splits

        asset = Asset(symbol="GOOG", asset_type="stock")

        index = pd.to_datetime(["2020-03-12"], utc=True)
        frame = pd.DataFrame({
            "open": [2400.0],
            "high": [2450.0],
            "low": [2350.0],
            "close": [2380.0],
            "volume": [1000],
        }, index=index)

        # First adjustment
        enriched1 = thetadata_helper._apply_corporate_actions_to_frame(
            asset,
            frame.copy(),
            date(2020, 3, 1),
            date(2020, 3, 31),
        )

        # Second adjustment on already-adjusted data
        enriched2 = thetadata_helper._apply_corporate_actions_to_frame(
            asset,
            enriched1.copy(),
            date(2020, 3, 1),
            date(2020, 3, 31),
        )

        # Both should have the same adjusted price (not double-adjusted)
        assert enriched1["close"].iloc[0] == pytest.approx(enriched2["close"].iloc[0], rel=0.01)
        assert enriched1["close"].iloc[0] == pytest.approx(119.0, rel=0.01)  # 2380 / 20

    @patch("lumibot.tools.thetadata_helper._get_theta_dividends")
    @patch("lumibot.tools.thetadata_helper._get_theta_splits")
    def test_dividend_adjustment_with_splits(self, mock_splits, mock_dividends):
        """
        Test: Dividends before splits should be adjusted by split factor

        A $1.00 dividend paid before a 2:1 split should become $0.50 after adjustment.
        """
        mock_dividends.return_value = pd.DataFrame({
            "event_date": pd.to_datetime(["2020-01-15"], utc=True),
            "cash_amount": [1.00],
        })
        mock_splits.return_value = pd.DataFrame({
            "event_date": pd.to_datetime(["2020-06-15"], utc=True),
            "ratio": [2.0],
            "symbol": ["TEST"],
        })

        asset = Asset(symbol="TEST", asset_type="stock")

        index = pd.to_datetime(["2020-01-15"], utc=True)
        frame = pd.DataFrame({
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
            "close": [100.5],
            "volume": [1000],
        }, index=index)

        enriched = thetadata_helper._apply_corporate_actions_to_frame(
            asset,
            frame.copy(),
            date(2020, 1, 1),
            date(2020, 1, 31),
        )

        # Dividend should be adjusted: $1.00 / 2.0 = $0.50
        assert enriched["dividend"].iloc[0] == pytest.approx(0.50, rel=0.01)

    @patch("lumibot.tools.thetadata_helper._get_theta_dividends")
    @patch("lumibot.tools.thetadata_helper._get_theta_splits")
    def test_options_are_split_adjusted_like_underlying(self, mock_splits, mock_dividends):
        """
        Test: Option bars must be split-adjusted so option series remain continuous in post-split terms.

        In ThetaData backtests we normalize stock prices to today's share units (Yahoo-style split adjustment).
        To keep options consistent with split-adjusted underlyings and strikes, we also normalize option OHLC/NBBO
        by the same split factor for dates before the split.
        """
        mock_dividends.return_value = pd.DataFrame()
        mock_splits.return_value = pd.DataFrame({
            "event_date": pd.to_datetime(["2022-07-15"], utc=True),
            "ratio": [20.0],
            "symbol": ["GOOG"],
        })

        option_asset = Asset(
            symbol="GOOG",
            asset_type="option",
            expiration=date(2020, 4, 17),
            strike=1320.0,
            right="CALL",
        )

        index = pd.to_datetime(["2020-03-12"], utc=True)
        frame = pd.DataFrame({
            "open": [50.0],
            "high": [55.0],
            "low": [45.0],
            "close": [52.0],
            "volume": [100],
        }, index=index)

        enriched = thetadata_helper._apply_corporate_actions_to_frame(
            option_asset,
            frame.copy(),
            date(2020, 3, 1),
            date(2020, 3, 31),
        )

        # Option prices should be adjusted by the split factor (pre-split premiums divided by 20).
        assert enriched["close"].iloc[0] == pytest.approx(2.6, rel=0.01)


# =============================================================================
# TESTS FOR _normalize_split_events()
# =============================================================================

class TestNormalizeSplitEvents:
    """Tests for split event parsing and normalization"""

    def test_parses_numerator_denominator_format(self):
        """Test: Parse split_to/split_from format (e.g., 20:1)"""
        df = pd.DataFrame({
            "execution_date": ["2022-07-15"],
            "split_to": [20],
            "split_from": [1],
        })

        normalized = thetadata_helper._normalize_split_events(df, "GOOG")

        assert len(normalized) == 1
        assert normalized["ratio"].iloc[0] == 20.0

    def test_parses_ratio_string_format(self):
        """Test: Parse ratio string format (e.g., "20:1")"""
        df = pd.DataFrame({
            "execution_date": ["2022-07-15"],
            "ratio": ["20:1"],
        })

        normalized = thetadata_helper._normalize_split_events(df, "GOOG")

        assert len(normalized) == 1
        assert normalized["ratio"].iloc[0] == 20.0

    def test_filters_daily_data_to_actual_splits(self):
        """
        Test: ThetaData v2 returns a row for every trading day with "most recent" split info.
        We should filter to only actual split events where date == split_date.
        """
        # ThetaData v2 format: returns split info on every trading day
        df = pd.DataFrame({
            "date": ["20220714", "20220715", "20220718"],  # Thu, Fri (split day), Mon
            "split_date": ["20220715", "20220715", "20220715"],  # All reference same split
            "before_shares": [1, 1, 1],
            "after_shares": [20, 20, 20],
        })

        normalized = thetadata_helper._normalize_split_events(df, "GOOG")

        # Should only return 1 row for the actual split date
        assert len(normalized) == 1
        assert normalized["event_date"].iloc[0].date() == date(2022, 7, 15)

    def test_handles_empty_dataframe(self):
        """Test: Empty DataFrame returns empty DataFrame"""
        df = pd.DataFrame()
        normalized = thetadata_helper._normalize_split_events(df, "TEST")
        assert normalized.empty


# =============================================================================
# TESTS FOR _normalize_dividend_events()
# =============================================================================

class TestNormalizeDividendEvents:
    """Tests for dividend event parsing and normalization"""

    def test_deduplicates_by_ex_date(self):
        """
        Test: ThetaData sometimes returns multiple entries for the same ex_date.
        We should keep only the first occurrence.
        """
        df = pd.DataFrame({
            "ex_date": ["2020-03-20", "2020-03-20", "2020-03-20", "2020-06-15"],
            "amount": [0.50, 0.50, 0.50, 0.55],
        })

        normalized = thetadata_helper._normalize_dividend_events(df, "TEST")

        # Should deduplicate to 2 unique dates
        assert len(normalized) == 2

    def test_filters_special_distributions(self):
        """
        Test: Filter out special distributions where less_amount > 0
        These are return of capital or other special adjustments, not regular dividends.
        """
        df = pd.DataFrame({
            "ex_date": ["2020-03-20", "2020-07-02"],
            "amount": [0.50, 1.22],
            "less_amount": [0.0, 22.93],  # Second is a special distribution
        })

        normalized = thetadata_helper._normalize_dividend_events(df, "TEST")

        # Should filter out the special distribution
        assert len(normalized) == 1
        assert normalized["cash_amount"].iloc[0] == pytest.approx(0.50, rel=0.01)

    def test_handles_various_column_names(self):
        """Test: Handle different column naming conventions"""
        # ThetaData uses various column names for the same data
        df = pd.DataFrame({
            "ex_dividend_date": ["2020-03-20"],  # Alternative name
            "cash_amount": [0.50],  # Alternative name
        })

        normalized = thetadata_helper._normalize_dividend_events(df, "TEST")

        assert len(normalized) == 1


# =============================================================================
# EDGE CASE TESTS
# =============================================================================

class TestSplitEdgeCases:
    """Edge cases and regression tests"""

    @patch("lumibot.tools.thetadata_helper._get_theta_splits")
    def test_split_on_exact_option_expiration_date(self, mock_get_splits):
        """
        Test: Handle the edge case where a split occurs on the option's expiration date
        """
        mock_get_splits.return_value = pd.DataFrame({
            "event_date": pd.to_datetime(["2022-07-15"], utc=True),
            "ratio": [20.0],
            "symbol": ["GOOG"],
        })

        # Option expires ON the split date
        option_asset = Asset(
            symbol="GOOG",
            asset_type="option",
            expiration=date(2022, 7, 15),
            strike=66.0,
            right="CALL",
        )

        # Should NOT adjust - split is not "after" the expiration
        original_strike = thetadata_helper._get_option_query_strike(
            option_asset
        )

        # The split happens ON the expiration, so we need the adjusted strike
        # since the option would have been traded at pre-split levels before this date
        assert original_strike == 66.0  # No adjustment because split is not AFTER expiration

    @patch("lumibot.tools.thetadata_helper._get_theta_splits")
    def test_fractional_split_ratio(self, mock_get_splits):
        """Test: Handle fractional split ratios (e.g., 3:2 = 1.5)"""
        mock_get_splits.return_value = pd.DataFrame({
            "event_date": pd.to_datetime(["2022-01-15"], utc=True),
            "ratio": [1.5],  # 3:2 split
            "symbol": ["TEST"],
        })

        option_asset = Asset(
            symbol="TEST",
            asset_type="option",
            expiration=date(2021, 6, 18),
            strike=100.0,
            right="CALL",
        )

        original_strike = thetadata_helper._get_option_query_strike(
            option_asset
        )

        # 100 * 1.5 = 150
        assert original_strike == pytest.approx(150.0, rel=0.01)

    @patch("lumibot.tools.thetadata_helper._get_theta_splits")
    def test_reverse_split(self, mock_get_splits):
        """Test: Handle reverse splits (ratio < 1, e.g., 1:10)"""
        mock_get_splits.return_value = pd.DataFrame({
            "event_date": pd.to_datetime(["2022-01-15"], utc=True),
            "ratio": [0.1],  # 1:10 reverse split
            "symbol": ["TEST"],
        })

        option_asset = Asset(
            symbol="TEST",
            asset_type="option",
            expiration=date(2021, 6, 18),
            strike=50.0,
            right="CALL",
        )

        original_strike = thetadata_helper._get_option_query_strike(
            option_asset
        )

        # Reverse splits: ratio < 1 should still be applied. A 1:10 reverse split (ratio=0.1)
        # means today's strikes are 10x larger than pre-split strikes.
        assert original_strike == pytest.approx(5.0, rel=0.01)  # 50 * 0.1 = 5


# =============================================================================
# RUN TESTS
# =============================================================================

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
