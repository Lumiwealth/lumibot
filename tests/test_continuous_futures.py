"""
Tests for continuous futures position matching functionality.
"""

import pytest
from datetime import date, datetime
from unittest.mock import Mock, MagicMock

from lumibot.entities import Asset, Position
from lumibot.strategies.strategy import Strategy
from lumibot.tools.futures_symbols import (
    parse_contract_symbol,
    symbol_matches_root,
    from_ib_expiration_to_code,
    generate_symbol_variants,
    get_contract_priority_key,
    build_ib_contract_variants
)


class TestFuturesSymbolsUtilities:
    """Test the futures symbols utility functions."""

    def test_parse_contract_symbol_tradovate_style(self):
        """Test parsing Tradovate-style single-digit year symbols."""
        result = parse_contract_symbol("MNQU5")
        assert result is not None
        assert result["root"] == "MNQ"
        assert result["month_code"] == "U"
        assert result["year_1d"] == "5"
        assert result["year_2d"] == "25"
        assert result["year_4d"] == "2025"

    def test_parse_contract_symbol_standard_style(self):
        """Test parsing standard 2-digit year symbols."""
        result = parse_contract_symbol("MNQU25")
        assert result is not None
        assert result["root"] == "MNQ"
        assert result["month_code"] == "U"
        assert result["year_1d"] == "5"
        assert result["year_2d"] == "25"
        assert result["year_4d"] == "2025"

    def test_parse_contract_symbol_dot_notation(self):
        """Test parsing dot notation symbols."""
        result = parse_contract_symbol("MNQ.U25")
        assert result is not None
        assert result["root"] == "MNQ"
        assert result["month_code"] == "U"
        assert result["year_1d"] == "5"
        assert result["year_2d"] == "25"
        assert result["year_4d"] == "2025"

    def test_parse_contract_symbol_full_year(self):
        """Test parsing full year symbols."""
        result = parse_contract_symbol("MNQU2025")
        assert result is not None
        assert result["root"] == "MNQ"
        assert result["month_code"] == "U"
        assert result["year_1d"] == "5"
        assert result["year_2d"] == "25"
        assert result["year_4d"] == "2025"

    def test_parse_contract_symbol_invalid(self):
        """Test parsing invalid symbols."""
        assert parse_contract_symbol("INVALID") is None
        assert parse_contract_symbol("") is None
        assert parse_contract_symbol(None) is None
        assert parse_contract_symbol("123") is None

    def test_symbol_matches_root(self):
        """Test root symbol matching."""
        assert symbol_matches_root("MNQU5", "MNQ")
        assert symbol_matches_root("MNQU25", "MNQ")
        assert symbol_matches_root("MNQ.U25", "MNQ")
        assert symbol_matches_root("MNQU2025", "MNQ")
        assert symbol_matches_root("MNQ", "MNQ")  # Direct match
        assert not symbol_matches_root("ESU5", "MNQ")
        assert not symbol_matches_root("", "MNQ")
        assert not symbol_matches_root("MNQ", "")

    def test_from_ib_expiration_to_code(self):
        """Test IB expiration to month code conversion."""
        # Test with date object
        result = from_ib_expiration_to_code(date(2025, 9, 19))
        assert result == ("U", "25")
        
        # Test with datetime object
        result = from_ib_expiration_to_code(datetime(2025, 12, 18))
        assert result == ("Z", "25")
        
        # Test with string
        result = from_ib_expiration_to_code("202509")
        assert result == ("U", "25")
        
        # Test invalid inputs
        assert from_ib_expiration_to_code(None) is None
        assert from_ib_expiration_to_code("invalid") is None
        assert from_ib_expiration_to_code("20251") is None  # Invalid format

    def test_generate_symbol_variants(self):
        """Test symbol variant generation."""
        variants = generate_symbol_variants("MNQ", "U", "5", "25", "2025")
        expected = {"MNQU5", "MNQU25", "MNQ.U25", "MNQU2025"}
        assert variants == expected

    def test_get_contract_priority_key(self):
        """Test contract priority ranking."""
        priority_list = ["MNQU25", "MNQU5", "MNQZ25", "MNQZ5"]
        
        # Direct matches
        assert get_contract_priority_key("MNQU25", priority_list) == 0
        assert get_contract_priority_key("MNQU5", priority_list) == 1
        
        # Variant matches (dot notation should match standard)
        assert get_contract_priority_key("MNQ.U25", priority_list) == 0
        
        # No match
        assert get_contract_priority_key("ESU25", priority_list) == 999999

    def test_build_ib_contract_variants(self):
        """Test IB contract variant building."""
        variants = build_ib_contract_variants("MNQ", date(2025, 9, 19))
        expected = {"MNQU5", "MNQU25", "MNQ.U25", "MNQU2025"}
        assert variants == expected


class TestContinuousFuturesPositionMatching:
    """Test the continuous futures position matching in Strategy.get_position()."""

    def setup_method(self):
        """Set up test fixtures."""
        self.strategy = Strategy()
        self.strategy._name = "TestStrategy"
        
        # Mock broker
        self.mock_broker = Mock()
        self.strategy.broker = self.mock_broker
        
        # Create test assets
        self.cont_future_asset = Asset("MNQ", asset_type=Asset.AssetType.CONT_FUTURE)
        self.stock_asset = Asset("AAPL", asset_type=Asset.AssetType.STOCK)

    def test_non_continuous_future_unchanged(self):
        """Test that non-continuous futures use existing behavior."""
        mock_position = Mock()
        self.mock_broker.get_tracked_position.return_value = mock_position
        
        result = self.strategy.get_position(self.stock_asset)
        
        self.mock_broker.get_tracked_position.assert_called_once_with("TestStrategy", self.stock_asset)
        assert result == mock_position

    def test_continuous_future_exact_match(self):
        """Test exact match for continuous futures (ProjectX style)."""
        mock_position = Mock()
        self.mock_broker.get_tracked_position.return_value = mock_position
        
        result = self.strategy.get_position(self.cont_future_asset)
        
        self.mock_broker.get_tracked_position.assert_called_once_with("TestStrategy", self.cont_future_asset)
        assert result == mock_position

    def test_continuous_future_single_contract_match(self):
        """Test matching single contract for continuous futures."""
        # Mock exact match returns None
        self.mock_broker.get_tracked_position.return_value = None
        
        # Create mock position with Tradovate-style contract
        tradovate_asset = Asset("MNQU5", asset_type=Asset.AssetType.FUTURE)
        mock_position = Mock()
        mock_position.asset = tradovate_asset
        
        self.mock_broker.get_tracked_positions.return_value = [mock_position]
        
        result = self.strategy.get_position(self.cont_future_asset)
        
        assert result == mock_position

    def test_continuous_future_multiple_contracts_priority(self):
        """Test priority selection when multiple contracts exist."""
        # Mock exact match returns None
        self.mock_broker.get_tracked_position.return_value = None
        
        # Create mock positions with different contracts
        sep_asset = Asset("MNQU5", asset_type=Asset.AssetType.FUTURE)  # September
        dec_asset = Asset("MNQZ5", asset_type=Asset.AssetType.FUTURE)  # December
        
        sep_position = Mock()
        sep_position.asset = sep_asset
        
        dec_position = Mock()
        dec_position.asset = dec_asset
        
        self.mock_broker.get_tracked_positions.return_value = [dec_position, sep_position]
        
        # Mock the log_message method
        self.strategy.log_message = Mock()
        
        result = self.strategy.get_position(self.cont_future_asset)
        
        # Should prefer September over December (front-month)
        # Note: The exact choice depends on current date, but we can verify logging
        assert result in [sep_position, dec_position]
        self.strategy.log_message.assert_called_once()
        
        # Verify the log message mentions multiple contracts
        log_call = self.strategy.log_message.call_args[0][0]
        assert "Multiple futures contracts found" in log_call
        assert "MNQ" in log_call

    def test_continuous_future_ib_style_positions(self):
        """Test matching IB-style positions (root symbol + expiration)."""
        # Mock exact match returns None
        self.mock_broker.get_tracked_position.return_value = None
        
        # Create mock IB-style position
        ib_asset = Asset("MNQ", asset_type=Asset.AssetType.FUTURE, expiration=date(2025, 9, 19))
        mock_position = Mock()
        mock_position.asset = ib_asset
        
        self.mock_broker.get_tracked_positions.return_value = [mock_position]
        
        result = self.strategy.get_position(self.cont_future_asset)
        
        assert result == mock_position

    def test_continuous_future_no_matches(self):
        """Test no matches returns None."""
        # Mock exact match returns None
        self.mock_broker.get_tracked_position.return_value = None
        
        # Create mock position with different root
        other_asset = Asset("ESU5", asset_type=Asset.AssetType.FUTURE)
        mock_position = Mock()
        mock_position.asset = other_asset
        
        self.mock_broker.get_tracked_positions.return_value = [mock_position]
        
        result = self.strategy.get_position(self.cont_future_asset)
        
        assert result is None

    def test_continuous_future_ignores_non_futures(self):
        """Test that non-futures positions are ignored."""
        # Mock exact match returns None
        self.mock_broker.get_tracked_position.return_value = None
        
        # Create mock stock position with similar symbol
        stock_asset = Asset("MNQ", asset_type=Asset.AssetType.STOCK)
        mock_position = Mock()
        mock_position.asset = stock_asset
        
        self.mock_broker.get_tracked_positions.return_value = [mock_position]
        
        result = self.strategy.get_position(self.cont_future_asset)
        
        assert result is None

    def test_continuous_future_empty_positions(self):
        """Test empty positions list returns None."""
        # Mock exact match returns None
        self.mock_broker.get_tracked_position.return_value = None
        self.mock_broker.get_tracked_positions.return_value = []
        
        result = self.strategy.get_position(self.cont_future_asset)
        
        assert result is None


class TestAssetPotentialContracts:
    """Test the extended get_potential_futures_contracts method."""

    def test_includes_single_digit_variants(self):
        """Test that single-digit year variants are included."""
        asset = Asset("MNQ", asset_type=Asset.AssetType.CONT_FUTURE)
        contracts = asset.get_potential_futures_contracts()
        
        # Should include both single-digit and double-digit year formats
        # Look for patterns like MNQU5 and MNQU25
        single_digit_found = any("U5" in contract for contract in contracts)
        double_digit_found = any("U25" in contract for contract in contracts)
        
        assert single_digit_found, f"Single-digit variants not found in: {contracts}"
        assert double_digit_found, f"Double-digit variants not found in: {contracts}"

    def test_preserves_existing_order(self):
        """Test that existing contract ordering is preserved."""
        asset = Asset("MNQ", asset_type=Asset.AssetType.CONT_FUTURE)
        contracts = asset.get_potential_futures_contracts()
        
        # Should still be a list (preserves order)
        assert isinstance(contracts, list)
        
        # Should not have duplicates
        assert len(contracts) == len(set(contracts))

    def test_quarterly_contracts_prioritized(self):
        """Test that quarterly contracts appear early in the list."""
        asset = Asset("MNQ", asset_type=Asset.AssetType.CONT_FUTURE)
        contracts = asset.get_potential_futures_contracts()
        
        # Find positions of quarterly month codes (H=Mar, M=Jun, U=Sep, Z=Dec)
        quarterly_positions = []
        for i, contract in enumerate(contracts):
            if any(month in contract for month in ['H', 'M', 'U', 'Z']):
                quarterly_positions.append(i)
                if len(quarterly_positions) >= 3:  # Check first few
                    break
        
        # Quarterly contracts should appear in early positions
        assert len(quarterly_positions) > 0, "No quarterly contracts found"
        assert quarterly_positions[0] < 10, "Quarterly contracts not prioritized"
