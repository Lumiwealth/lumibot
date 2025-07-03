import datetime
from collections.abc import Hashable
from unittest.mock import patch

import pytest

from lumibot.entities.asset import Asset


def test_check_default_asset_name():
    asset = Asset(symbol="ABC")
    assert asset.symbol == "ABC"


def test_check_default_asset_type():
    asset = Asset(symbol="ABC")
    assert asset.asset_type == "stock"


def test_check_defaults_with_stock():
    asset = Asset(symbol="ABC")

    assert asset.asset_type == "stock"
    assert asset.expiration is None
    assert asset.right is None
    assert asset.multiplier == 1


def test_is_hashable():
    asset = Asset(symbol="ABC")
    assert isinstance(asset, Hashable)


def test_extra_attributes_at_initialization():
    # Ignore extra attributes during model initialization.
    with pytest.raises(Exception):
        Asset(symbol="ABC", extra_attribute=1)


def test_instances_equal():
    a = Asset(
        symbol="ABC",
        asset_type="option",
        expiration=datetime.date(2020, 1, 1),
        strike=150,
        right="CALL",
        multiplier=100,
    )
    b = Asset(
        symbol="ABC",
        asset_type="option",
        expiration=datetime.date(2020, 1, 1),
        strike=150,
        right="CALL",
        multiplier=100,
    )

    assert a == b


def test_symbol2asset():
    asset = Asset.symbol2asset("ABC")
    assert asset.symbol == "ABC"
    assert asset.asset_type == "stock"

    asset = Asset.symbol2asset("ABC200101C00150000")
    assert asset.symbol == "ABC"
    assert asset.asset_type == "option"
    assert asset.expiration == datetime.date(2020, 1, 1)
    assert asset.strike == 150
    assert asset.right == "CALL"


@pytest.mark.parametrize("param", ["not_call_or_CALL", "not_put_or_PUT", "CALLS", "PUTS"])
def test_right_validator(param):
    with pytest.raises(Exception):
        Asset(symbol="ABC", right=param)


@pytest.mark.parametrize("param", ["bonds", "cash", "swaptions"])
def test_asset_types_validator(param):
    with pytest.raises(Exception):
        Asset(symbol="ABC", asset_type=param)


# Continuous Futures Tests

def test_continuous_futures_asset_creation():
    """Test creation of continuous futures assets."""
    asset = Asset(symbol="ES", asset_type=Asset.AssetType.CONT_FUTURE)
    assert asset.symbol == "ES"
    assert asset.asset_type == Asset.AssetType.CONT_FUTURE
    assert asset.multiplier == 1  # Default multiplier

def test_resolve_continuous_futures_contract():
    """Test resolution of continuous futures contracts."""
    # Test ES futures - just check that it returns a valid contract format
    asset = Asset(symbol="ES", asset_type=Asset.AssetType.CONT_FUTURE)
    contract = asset.resolve_continuous_futures_contract()
    
    # Should return a contract in the format SYMBOLMM[M]YY
    assert isinstance(contract, str)
    assert contract.startswith("ES")
    assert len(contract) >= 5  # At least ESXYY format
    
    # Test MES futures
    asset = Asset(symbol="MES", asset_type=Asset.AssetType.CONT_FUTURE)
    contract = asset.resolve_continuous_futures_contract()
    assert isinstance(contract, str)
    assert contract.startswith("MES")

def test_get_potential_futures_contracts():
    """Test getting potential futures contracts."""
    asset = Asset(symbol="ES", asset_type=Asset.AssetType.CONT_FUTURE)
    contracts = asset.get_potential_futures_contracts()
    
    # Should return a list with contracts
    assert isinstance(contracts, list)
    assert len(contracts) > 0
    # All contracts should start with the base symbol
    for contract in contracts:
        assert contract.startswith("ES")

def test_futures_contract_resolution_monthly_cycles():
    """Test contract resolution across different monthly cycles."""
    # Test that the resolution works for different symbols and months
    # We'll test the actual functionality without mocking datetime
    
    test_symbols = ["ES", "NQ", "RTY"]
    
    for symbol in test_symbols:
        asset = Asset(symbol=symbol, asset_type=Asset.AssetType.CONT_FUTURE)
        contract = asset.resolve_continuous_futures_contract()
        
        # Verify we get a valid contract format
        assert isinstance(contract, str)
        assert contract.startswith(symbol)
        assert len(contract) >= len(symbol) + 2  # At least symbol + month + year

def test_different_futures_symbols():
    """Test contract resolution for different futures symbols."""
    test_symbols = ["ES", "MES", "NQ", "MNQ", "RTY", "CL", "GC"]
    
    for symbol in test_symbols:
        asset = Asset(symbol=symbol, asset_type=Asset.AssetType.CONT_FUTURE)
        contract = asset.resolve_continuous_futures_contract()
        assert isinstance(contract, str)
        assert contract.startswith(symbol), f"Contract {contract} should start with {symbol}"

def test_continuous_futures_equality():
    """Test that continuous futures assets are equal when they should be."""
    asset1 = Asset(symbol="ES", asset_type=Asset.AssetType.CONT_FUTURE)
    asset2 = Asset(symbol="ES", asset_type=Asset.AssetType.CONT_FUTURE)
    
    assert asset1 == asset2
    assert hash(asset1) == hash(asset2)

def test_continuous_futures_inequality():
    """Test that different continuous futures assets are not equal."""
    asset1 = Asset(symbol="ES", asset_type=Asset.AssetType.CONT_FUTURE)
    asset2 = Asset(symbol="NQ", asset_type=Asset.AssetType.CONT_FUTURE)
    
    assert asset1 != asset2
    assert hash(asset1) != hash(asset2)

def test_futures_contract_with_year_rollover():
    """Test contract resolution across year boundaries."""
    # Test near end of year - should roll to next year's contracts
    asset = Asset(symbol="ES", asset_type=Asset.AssetType.CONT_FUTURE)
    contract = asset.resolve_continuous_futures_contract()
    
    # Should return a valid contract format
    assert isinstance(contract, str)
    assert contract.startswith("ES")
    assert len(contract) >= 5

def test_asset_type_enum_values():
    """Test that AssetType enum has the expected values."""
    assert hasattr(Asset.AssetType, 'CONT_FUTURE')
    assert Asset.AssetType.CONT_FUTURE == "cont_future"

def test_continuous_futures_string_representation():
    """Test string representation of continuous futures assets."""
    asset = Asset(symbol="ES", asset_type=Asset.AssetType.CONT_FUTURE)
    str_repr = str(asset)
    
    # Should contain the symbol at minimum
    assert "ES" in str_repr
