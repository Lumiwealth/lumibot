"""Provider-independent derivative quantities and settlement economics."""

from decimal import Decimal
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from lumibot.entities import Asset, ContractSpec
from lumibot.backtesting import BacktestingBroker
from lumibot.brokers.bitunix import Bitunix
from tests.test_ccxt_derivatives import derivatives
from tests.test_ccxt_unified_spot import make_broker


@pytest.mark.parametrize("inverse,size,unit,quantity", [
    (False, "0.001", "contracts", "20"),
    (True, "1", "contracts", "1000"),
    (False, "1", "base", "0.02"),
])
def test_same_notional_has_same_exposure_without_changing_native_units(inverse, size, unit, quantity):
    spec = ContractSpec("BTC", "USD", "BTC" if inverse else "USD", size, inverse, unit)
    qty = spec.quantity_for_notional(1000, 50000)
    assert qty == Decimal(quantity)
    assert spec.exposure(qty, 50000)["base"] == Decimal("0.02")
    assert spec.exposure(qty, 50000)["quote"] == Decimal("1000")


@pytest.mark.parametrize("inverse,quantity,expected", [(False, 20, "100"), (True, 1000, None)])
def test_contract_pnl_uses_linear_or_reciprocal_prices(inverse, quantity, expected):
    spec = ContractSpec("BTC", "USD", "BTC" if inverse else "USD", "1" if inverse else "0.001", inverse)
    expected = Decimal(expected) if expected else Decimal(1000) * (Decimal(1)/50000 - Decimal(1)/55000)
    assert spec.pnl(quantity, 50000, 55000) == expected
    assert spec.pnl(-quantity, 50000, 55000) == -expected


def test_contract_terms_roundtrip_without_changing_asset_identity():
    spec = ContractSpec("BTC", "USDC", "USDC", "0.001")
    asset = Asset("BTC/USDC:USDC", "crypto_future", contract_spec=spec)
    loaded = Asset.from_dict(asset.to_dict())
    assert loaded.contract_spec == spec
    assert loaded == Asset(asset.symbol, "crypto_future")
    assert hash(loaded) == hash(asset)


def test_ccxt_asset_carries_provider_contract_terms(derivatives):
    asset = derivatives.get_asset("BTC/USD:BTC")
    assert asset.contract_spec.inverse
    assert asset.contract_spec.settle == "BTC"
    assert asset.contract_spec.quantity_unit == "contracts"
    assert derivatives.get_quantity_for_notional(asset, 1000, price=50000) == 1000


def test_bitunix_terms_keep_existing_base_coin_quantities():
    broker = Bitunix.__new__(Bitunix)
    broker.api = SimpleNamespace(get_trading_pairs=Mock(return_value={"code": 0, "data": [
        {"symbol": "BTCUSDT", "base": "BTC", "quote": "USDT", "basePrecision": 4,
         "quotePrecision": 1, "minTradeVolume": "0.0001"}]}))
    asset = Asset("BTCUSDT", "crypto_future")
    spec = broker.get_contract_spec(asset)
    assert spec.quantity_unit == "base"
    assert broker.get_quantity_for_notional(asset, 1000, price=50000) == Decimal("0.02")


def test_backtesting_broker_uses_serialized_terms_without_provider_dependency():
    broker = BacktestingBroker.__new__(BacktestingBroker)
    asset = Asset("BTC/USD:BTC", "crypto_future", contract_spec=ContractSpec("BTC", "USD", "BTC", 1, True))
    assert broker.get_contract_exposure(asset, 1000, 50000)["base"] == Decimal("0.02")
