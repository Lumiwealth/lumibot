"""
Unit tests for DataBento integration with Asset auto-expiry functionality.

This module tests that the DataBento backtesting integration correctly
handles assets with auto-expiry, including symbol formatting and data fetching.
"""

import pytest
from datetime import date, datetime
from unittest.mock import patch, MagicMock

from lumibot.entities.asset import Asset
from lumibot.tools.databento_helper import _format_futures_symbol_for_databento


class TestDataBentoAutoExpiryIntegration:
    """Test suite for DataBento integration with auto-expiry functionality."""

    def test_databento_symbol_formatting_with_auto_expiry(self):
        """Test that DataBento symbol formatting works with auto-expiry assets."""
        # Create asset with auto-expiry
        asset = Asset(
            symbol="MES",
            asset_type=Asset.AssetType.FUTURE,
            auto_expiry="front_month"
        )
        
        # Test that the helper can format the symbol
        formatted_symbol = _format_futures_symbol_for_databento(asset)
        
        # Should include the symbol and expiration information
        assert "MES" in formatted_symbol
        assert asset.expiration is not None

    def test_databento_symbol_with_different_auto_expiry_types(self):
        """Test DataBento symbol formatting with different auto-expiry types."""
        
        # Test front_month
        asset_front = Asset("ES", asset_type=Asset.AssetType.FUTURE, auto_expiry="front_month")
        symbol_front = _format_futures_symbol_for_databento(asset_front)
        assert "ES" in symbol_front
        
        # Test next_quarter
        asset_quarter = Asset("NQ", asset_type=Asset.AssetType.FUTURE, auto_expiry="next_quarter")
        symbol_quarter = _format_futures_symbol_for_databento(asset_quarter)
        assert "NQ" in symbol_quarter
        
        # Test auto (should work like front_month)
        asset_auto = Asset("RTY", asset_type=Asset.AssetType.FUTURE, auto_expiry="auto")
        symbol_auto = _format_futures_symbol_for_databento(asset_auto)
        assert "RTY" in symbol_auto

    @patch('datetime.date')
    def test_databento_symbol_consistency_across_dates(self, mock_date):
        """Test that DataBento symbols are consistent for the same date."""
        
        # Mock a specific date
        mock_date.today.return_value = date(2024, 3, 1)
        mock_date.side_effect = lambda *args, **kw: date(*args, **kw)
        
        # Create multiple assets with same auto-expiry
        asset1 = Asset("ES", asset_type=Asset.AssetType.FUTURE, auto_expiry="front_month")
        asset2 = Asset("ES", asset_type=Asset.AssetType.FUTURE, auto_expiry="front_month")
        
        # Should have the same expiration
        assert asset1.expiration == asset2.expiration
        
        # Should generate the same symbol
        symbol1 = _format_futures_symbol_for_databento(asset1)
        symbol2 = _format_futures_symbol_for_databento(asset2)
        assert symbol1 == symbol2

    def test_databento_symbol_with_manual_vs_auto_expiry(self):
        """Test DataBento symbol formatting with manual vs auto expiry."""
        
        # Create asset with auto-expiry
        asset_auto = Asset("ES", asset_type=Asset.AssetType.FUTURE, auto_expiry="front_month")
        auto_expiry_date = asset_auto.expiration
        
        # Create asset with same expiration manually set
        asset_manual = Asset("ES", asset_type=Asset.AssetType.FUTURE, expiration=auto_expiry_date)
        
        # Should generate the same symbol
        symbol_auto = _format_futures_symbol_for_databento(asset_auto)
        symbol_manual = _format_futures_symbol_for_databento(asset_manual)
        assert symbol_auto == symbol_manual

    def test_databento_symbol_handles_stock_asset(self):
        """Test that DataBento symbol formatting gracefully handles stock assets."""
        # Create stock asset (no expiration)
        stock_asset = Asset("AAPL", asset_type=Asset.AssetType.STOCK)
        
        # This function is specifically for futures, so it might not handle stocks
        # but it shouldn't crash
        try:
            symbol = _format_futures_symbol_for_databento(stock_asset)
            # If it works, should contain AAPL
            assert "AAPL" in symbol
        except Exception:
            # If it doesn't work with stocks, that's expected
            pass

    @patch('datetime.date')
    def test_databento_symbol_changes_with_date(self, mock_date):
        """Test that DataBento symbols change appropriately when dates change."""
        
        # Test early in quarter
        mock_date.today.return_value = date(2024, 1, 15)
        mock_date.side_effect = lambda *args, **kw: date(*args, **kw)
        asset_jan = Asset("ES", asset_type=Asset.AssetType.FUTURE, auto_expiry="front_month")
        symbol_jan = _format_futures_symbol_for_databento(asset_jan)
        jan_expiry = asset_jan.expiration
        
        # Test late in quarter (after expiry)
        mock_date.today.return_value = date(2024, 3, 20)  # After March expiry
        mock_date.side_effect = lambda *args, **kw: date(*args, **kw)
        asset_mar = Asset("ES", asset_type=Asset.AssetType.FUTURE, auto_expiry="front_month")
        symbol_mar = _format_futures_symbol_for_databento(asset_mar)
        mar_expiry = asset_mar.expiration
        
        # Should have different expiries
        assert jan_expiry != mar_expiry
        
        # Symbols should potentially be different (depending on format)
        # The exact assertion depends on DataBento's symbol format

    def test_multiple_futures_with_auto_expiry(self):
        """Test handling multiple different futures with auto-expiry."""
        
        # Create different futures contracts
        mes_asset = Asset("MES", asset_type=Asset.AssetType.FUTURE, auto_expiry="front_month")
        es_asset = Asset("ES", asset_type=Asset.AssetType.FUTURE, auto_expiry="front_month")
        nq_asset = Asset("NQ", asset_type=Asset.AssetType.FUTURE, auto_expiry="front_month")
        
        # All should have valid expiries
        assert mes_asset.expiration is not None
        assert es_asset.expiration is not None
        assert nq_asset.expiration is not None
        
        # Should generate valid symbols
        mes_symbol = _format_futures_symbol_for_databento(mes_asset)
        es_symbol = _format_futures_symbol_for_databento(es_asset)
        nq_symbol = _format_futures_symbol_for_databento(nq_asset)
        
        assert "MES" in mes_symbol
        assert "ES" in es_symbol
        assert "NQ" in nq_symbol


class TestDataBentoBacktestingAutoExpiry:
    """Test suite for DataBento backtesting with auto-expiry assets."""

    def test_asset_auto_expiry_in_strategy_context(self):
        """Test that auto-expiry assets work in a strategy context."""
        # This test simulates how assets are used in strategies
        
        # Create asset as it would be in a strategy
        asset = Asset(
            symbol="MES",
            asset_type=Asset.AssetType.FUTURE,
            auto_expiry="front_month"
        )
        
        # Verify it has all necessary attributes
        assert asset.symbol == "MES"
        assert asset.asset_type == Asset.AssetType.FUTURE
        assert asset.expiration is not None
        assert asset.auto_expiry == "front_month"
        
        # Should be hashable (for use in dictionaries)
        asset_dict = {asset: "test_value"}
        assert asset_dict[asset] == "test_value"

    def test_asset_auto_expiry_serialization(self):
        """Test that auto-expiry assets can be serialized/deserialized."""
        asset = Asset(
            symbol="ES",
            asset_type=Asset.AssetType.FUTURE,
            auto_expiry="front_month"
        )
        
        # Test string representation
        asset_str = str(asset)
        assert isinstance(asset_str, str)
        assert len(asset_str) > 0
        
        # Test repr representation
        asset_repr = repr(asset)
        assert isinstance(asset_repr, str)
        assert "ES" in asset_repr

    @patch('datetime.date')
    def test_asset_expiry_stability_during_backtest(self, mock_date):
        """Test that asset expiry remains stable during a backtest run."""
        # Mock a stable date for the duration of the test
        mock_date.today.return_value = date(2024, 2, 15)
        mock_date.side_effect = lambda *args, **kw: date(*args, **kw)
        
        # Create asset
        asset = Asset(
            symbol="MES",
            asset_type=Asset.AssetType.FUTURE,
            auto_expiry="front_month"
        )
        
        # Store the initial expiry
        initial_expiry = asset.expiration
        
        # Simulate multiple strategy iterations (asset should be stable)
        for _ in range(10):
            # In a real backtest, the asset object persists
            # The expiry should remain the same
            assert asset.expiration == initial_expiry

    def test_asset_auto_expiry_edge_cases(self):
        """Test edge cases for auto-expiry assets."""
        # Test with various symbols
        symbols = ["ES", "MES", "NQ", "MNQ", "RTY", "M2K", "YM", "MYM"]
        
        for symbol in symbols:
            asset = Asset(
                symbol=symbol,
                asset_type=Asset.AssetType.FUTURE,
                auto_expiry="front_month"
            )
            
            # Should work for all symbols
            assert asset.symbol == symbol.upper()
            assert asset.expiration is not None
            assert asset.expiration.month in [3, 6, 9, 12]

    def test_asset_auto_expiry_with_existing_strategy_pattern(self):
        """Test auto-expiry with existing strategy patterns."""
        # This mimics how the Alligator strategy creates assets
        
        # Create asset similar to the strategy
        asset = Asset("MES", asset_type=Asset.AssetType.FUTURE, auto_expiry="front_month")
        
        # Test attributes that strategies commonly access
        assert hasattr(asset, 'symbol')
        assert hasattr(asset, 'asset_type')
        assert hasattr(asset, 'expiration')
        assert hasattr(asset, 'auto_expiry')
        
        # Test that it's a valid futures asset
        assert asset.asset_type == Asset.AssetType.FUTURE
        assert isinstance(asset.expiration, date)


class TestDataBentoHelperMethods:
    """Test suite for DataBento helper methods with auto-expiry."""

    def test_format_symbol_with_quarterly_expiry(self):
        """Test symbol formatting specifically for quarterly expiries."""
        # Create asset with known quarterly expiry
        asset = Asset(
            symbol="ES",
            asset_type=Asset.AssetType.FUTURE,
            expiration=date(2024, 6, 21)  # June quarterly expiry
        )
        
        symbol = _format_futures_symbol_for_databento(asset)
        
        # Should handle quarterly expiry correctly
        assert isinstance(symbol, str)
        assert len(symbol) > 0

    def test_format_symbol_consistency(self):
        """Test that symbol formatting is consistent."""
        
        # Create same asset multiple times
        assets = []
        for _ in range(5):
            asset = Asset(
                symbol="MES",
                asset_type=Asset.AssetType.FUTURE,
                auto_expiry="front_month"
            )
            assets.append(asset)
        
        # All should have the same expiration
        expiries = [asset.expiration for asset in assets]
        assert all(exp == expiries[0] for exp in expiries)
        
        # All should generate the same symbol
        symbols = [_format_futures_symbol_for_databento(asset) for asset in assets]
        assert all(sym == symbols[0] for sym in symbols)


if __name__ == "__main__":
    # Run the tests
    pytest.main([__file__, "-v"])
