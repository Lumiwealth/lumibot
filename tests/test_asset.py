import datetime
from collections.abc import Hashable

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
    assert asset.currency == "USD"

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

@pytest.mark.parametrize("param", ["not_call_or_CALL", "not_put_or_PUT", "CALLS", "PUTS"])
def test_right_validator(param):
    with pytest.raises(Exception):
        Asset(symbol="ABC", right=param)

@pytest.mark.parametrize("param", ["bonds", "cash", "swaptions"])
def test_asset_types_validator(param):
    with pytest.raises(Exception):
        Asset(symbol="ABC", asset_type=param)
