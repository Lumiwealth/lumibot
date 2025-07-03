"""
Unit tests for the Asset auto-expiry functionality.

This module tests the automatic expiry calculation for futures contracts,
ensuring that the front_month and next_quarter options work correctly
and handle edge cases appropriately.
"""

import pytest
from datetime import date, datetime
from unittest.mock import patch

from lumibot.entities.asset import Asset


class TestAssetAutoExpiry:
    """Test suite for Asset auto-expiry functionality."""

    def test_asset_creation_with_auto_expiry_front_month(self):
        """Test that Asset correctly calculates front month expiry."""
        # Test with auto_expiry=Asset.AutoExpiry.FRONT_MONTH
        asset = Asset(
            symbol="MES",
            asset_type=Asset.AssetType.FUTURE,
            auto_expiry=Asset.AutoExpiry.FRONT_MONTH
        )
        
        # Should have an expiration date set
        assert asset.expiration is not None
        assert isinstance(asset.expiration, date)
        assert asset.auto_expiry == Asset.AutoExpiry.FRONT_MONTH
        
        # The expiration should be a quarterly month (Mar, Jun, Sep, Dec)
        assert asset.expiration.month in [3, 6, 9, 12]

    def test_asset_creation_with_auto_expiry_next_quarter(self):
        """Test that Asset correctly calculates next quarter expiry."""
        asset = Asset(
            symbol="ES",
            asset_type=Asset.AssetType.FUTURE,
            auto_expiry=Asset.AutoExpiry.NEXT_QUARTER
        )
        
        # Should have an expiration date set
        assert asset.expiration is not None
        assert isinstance(asset.expiration, date)
        assert asset.auto_expiry == Asset.AutoExpiry.NEXT_QUARTER
        
        # The expiration should be a quarterly month (Mar, Jun, Sep, Dec)
        assert asset.expiration.month in [3, 6, 9, 12]

    def test_asset_creation_with_auto_expiry_true(self):
        """Test that Asset correctly handles auto_expiry=True."""
        asset = Asset(
            symbol="NQ",
            asset_type=Asset.AssetType.FUTURE,
            auto_expiry=True
        )
        
        # Should have an expiration date set (should default to front_month behavior)
        assert asset.expiration is not None
        assert isinstance(asset.expiration, date)
        assert asset.auto_expiry is True

    def test_asset_creation_with_auto_expiry_auto(self):
        """Test that Asset correctly handles auto_expiry=Asset.AutoExpiry.AUTO."""
        asset = Asset(
            symbol="RTY",
            asset_type=Asset.AssetType.FUTURE,
            auto_expiry=Asset.AutoExpiry.AUTO
        )
        
        # Should have an expiration date set (should default to front_month behavior)
        assert asset.expiration is not None
        assert isinstance(asset.expiration, date)
        assert asset.auto_expiry == Asset.AutoExpiry.AUTO

    def test_manual_expiration_overrides_auto_expiry(self):
        """Test that manually provided expiration takes precedence over auto_expiry."""
        manual_expiry = date(2024, 6, 21)  # Specific date
        
        asset = Asset(
            symbol="ES",
            asset_type=Asset.AssetType.FUTURE,
            expiration=manual_expiry,
            auto_expiry=Asset.AutoExpiry.FRONT_MONTH
        )
        
        # Should use the manual expiration, not auto-calculated
        assert asset.expiration == manual_expiry
        assert asset.auto_expiry == Asset.AutoExpiry.FRONT_MONTH

    def test_auto_expiry_only_applies_to_futures(self):
        """Test that auto_expiry is only used for futures, not other asset types."""
        # Stock should ignore auto_expiry
        stock_asset = Asset(
            symbol="AAPL",
            asset_type=Asset.AssetType.STOCK,
            auto_expiry=Asset.AutoExpiry.FRONT_MONTH
        )
        assert stock_asset.expiration is None
        assert stock_asset.auto_expiry == Asset.AutoExpiry.FRONT_MONTH
        
        # Option without expiration should still be None (auto_expiry doesn't apply)
        option_asset = Asset(
            symbol="AAPL",
            asset_type=Asset.AssetType.OPTION,
            auto_expiry=Asset.AutoExpiry.FRONT_MONTH
        )
        assert option_asset.expiration is None
        assert option_asset.auto_expiry == Asset.AutoExpiry.FRONT_MONTH

    @patch('datetime.date')
    def test_front_month_calculation_march(self, mock_date):
        """Test front month calculation when current date is in March."""
        # Mock current date to March 1, 2024
        mock_date.today.return_value = date(2024, 3, 1)
        mock_date.side_effect = lambda *args, **kw: date(*args, **kw)
        
        asset = Asset(
            symbol="ES",
            asset_type=Asset.AssetType.FUTURE,
            auto_expiry="front_month"
        )
        
        # Should get March 2024 expiry (3rd Friday)
        expected_expiry = date(2024, 3, 15)  # 3rd Friday of March 2024
        assert asset.expiration == expected_expiry

    @patch('datetime.date')
    def test_front_month_calculation_after_expiry(self, mock_date):
        """Test front month calculation when current date is after expiry."""
        # Mock current date to March 16, 2024 (after 3rd Friday)
        mock_date.today.return_value = date(2024, 3, 16)
        mock_date.side_effect = lambda *args, **kw: date(*args, **kw)
        
        asset = Asset(
            symbol="ES",
            asset_type=Asset.AssetType.FUTURE,
            auto_expiry="front_month"
        )
        
        # Should get June 2024 expiry (next quarter)
        expected_expiry = date(2024, 6, 21)  # 3rd Friday of June 2024
        assert asset.expiration == expected_expiry

    @patch('datetime.date')
    def test_front_month_calculation_january(self, mock_date):
        """Test front month calculation when current date is in January."""
        # Mock current date to January 15, 2024
        mock_date.today.return_value = date(2024, 1, 15)
        mock_date.side_effect = lambda *args, **kw: date(*args, **kw)
        
        asset = Asset(
            symbol="ES",
            asset_type=Asset.AssetType.FUTURE,
            auto_expiry="front_month"
        )
        
        # Should get March 2024 expiry (next quarterly month)
        expected_expiry = date(2024, 3, 15)  # 3rd Friday of March 2024
        assert asset.expiration == expected_expiry

    @patch('datetime.date')
    def test_front_month_calculation_december(self, mock_date):
        """Test front month calculation when current date is in December."""
        # Mock current date to December 20, 2024 (after December expiry)
        mock_date.today.return_value = date(2024, 12, 20)
        mock_date.side_effect = lambda *args, **kw: date(*args, **kw)
        
        asset = Asset(
            symbol="ES",
            asset_type=Asset.AssetType.FUTURE,
            auto_expiry="front_month"
        )
        
        # Should get March 2025 expiry (next year)
        expected_expiry = date(2025, 3, 21)  # 3rd Friday of March 2025
        assert asset.expiration == expected_expiry

    def test_third_friday_calculation(self):
        """Test the _get_third_friday helper method."""
        asset = Asset("ES", asset_type=Asset.AssetType.FUTURE)
        
        # Test known third Fridays
        assert asset._get_third_friday(2024, 3) == date(2024, 3, 15)
        assert asset._get_third_friday(2024, 6) == date(2024, 6, 21)
        assert asset._get_third_friday(2024, 9) == date(2024, 9, 20)
        assert asset._get_third_friday(2024, 12) == date(2024, 12, 20)
        
        # Test edge case: February (short month)
        assert asset._get_third_friday(2024, 2) == date(2024, 2, 16)

    def test_invalid_auto_expiry_defaults_to_front_month(self):
        """Test that invalid auto_expiry values default to front_month behavior."""
        asset = Asset(
            symbol="ES",
            asset_type=Asset.AssetType.FUTURE,
            auto_expiry="invalid_option"
        )
        
        # Should still have an expiration (defaults to front_month)
        assert asset.expiration is not None
        assert isinstance(asset.expiration, date)
        assert asset.expiration.month in [3, 6, 9, 12]

    def test_asset_with_datetime_expiration_conversion(self):
        """Test that datetime expiration is correctly converted to date."""
        dt_expiry = datetime(2024, 6, 21, 15, 30, 0)
        
        asset = Asset(
            symbol="ES",
            asset_type=Asset.AssetType.FUTURE,
            expiration=dt_expiry
        )
        
        # Should convert datetime to date
        assert asset.expiration == date(2024, 6, 21)
        assert isinstance(asset.expiration, date)

    def test_asset_string_representation_with_auto_expiry(self):
        """Test that Asset string representation works with auto-expiry."""
        asset = Asset(
            symbol="MES",
            asset_type=Asset.AssetType.FUTURE,
            auto_expiry="front_month"
        )
        
        # Should be able to convert to string without errors
        asset_str = str(asset)
        assert "MES" in asset_str
        assert asset.expiration is not None

    def test_asset_equality_with_auto_expiry(self):
        """Test that assets with same calculated expiry are considered equal."""
        # Create two assets with the same auto_expiry
        asset1 = Asset("ES", asset_type=Asset.AssetType.FUTURE, auto_expiry="front_month")
        asset2 = Asset("ES", asset_type=Asset.AssetType.FUTURE, auto_expiry="front_month")
        
        # They should have the same calculated expiration
        assert asset1.expiration == asset2.expiration
        
        # Create asset with manual expiration matching the auto-calculated one
        asset3 = Asset("ES", asset_type=Asset.AssetType.FUTURE, expiration=asset1.expiration)
        
        # Should be equal since they have the same symbol, type, and expiration
        assert asset1.expiration == asset3.expiration

    def test_next_quarter_same_as_front_month(self):
        """Test that next_quarter and front_month produce the same result for quarterly futures."""
        asset_front = Asset("ES", asset_type=Asset.AssetType.FUTURE, auto_expiry="front_month")
        asset_quarter = Asset("ES", asset_type=Asset.AssetType.FUTURE, auto_expiry="next_quarter")
        
        # For quarterly futures, both should give the same result
        assert asset_front.expiration == asset_quarter.expiration

    def test_continuous_futures_creation(self):
        """Test that continuous futures assets are created correctly."""
        asset = Asset(
            symbol="MES",
            asset_type=Asset.AssetType.CONT_FUTURE
        )
        
        # Continuous futures don't need expiration dates
        assert asset.expiration is None
        assert asset.asset_type == Asset.AssetType.CONT_FUTURE
        assert asset.symbol == "MES"

    def test_continuous_futures_vs_auto_expiry(self):
        """Test that continuous futures are simpler than auto-expiry futures."""
        # Continuous future (recommended for backtesting)
        cont_asset = Asset("ES", asset_type=Asset.AssetType.CONT_FUTURE)
        
        # Auto-expiry future (more complex, for live trading)
        auto_asset = Asset("ES", asset_type=Asset.AssetType.FUTURE, auto_expiry=Asset.AutoExpiry.FRONT_MONTH)
        
        # Continuous future is simpler - no expiration to manage
        assert cont_asset.expiration is None
        assert auto_asset.expiration is not None
        
        # Both have the same symbol
        assert cont_asset.symbol == auto_asset.symbol == "ES"

    # ...existing code...


class TestAssetAutoExpiryIntegration:
    """Integration tests for auto-expiry with other Asset functionality."""

    def test_auto_expiry_with_symbol_parsing(self):
        """Test that auto-expiry works with symbol parsing."""
        # Test that symbol is properly uppercased
        asset = Asset(
            symbol="mes",  # lowercase
            asset_type=Asset.AssetType.FUTURE,
            auto_expiry="front_month"
        )
        
        assert asset.symbol == "MES"
        assert asset.expiration is not None

    def test_auto_expiry_with_multiplier(self):
        """Test that auto-expiry works with custom multipliers."""
        asset = Asset(
            symbol="ES",
            asset_type=Asset.AssetType.FUTURE,
            auto_expiry="front_month",
            multiplier=50
        )
        
        assert asset.multiplier == 50
        assert asset.expiration is not None

    def test_auto_expiry_preserved_in_asset_attributes(self):
        """Test that auto_expiry setting is preserved as an attribute."""
        asset = Asset(
            symbol="NQ",
            asset_type=Asset.AssetType.FUTURE,
            auto_expiry="next_quarter"
        )
        
        # The auto_expiry setting should be preserved
        assert asset.auto_expiry == "next_quarter"
        assert asset.expiration is not None

    def test_asset_validation_with_auto_expiry(self):
        """Test that asset validation still works with auto-expiry."""
        # Valid asset type should work
        asset = Asset(
            symbol="ES",
            asset_type=Asset.AssetType.FUTURE,
            auto_expiry="front_month"
        )
        assert asset.asset_type == Asset.AssetType.FUTURE
        
        # Invalid asset type should still raise error
        with pytest.raises(Exception):  # Should raise validation error
            Asset(
                symbol="ES",
                asset_type="invalid_type",
                auto_expiry="front_month"
            )


if __name__ == "__main__":
    # Run the tests
    pytest.main([__file__, "-v"])
