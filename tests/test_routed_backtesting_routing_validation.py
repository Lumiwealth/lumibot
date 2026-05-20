from __future__ import annotations

import pytest

from lumibot.backtesting.routed_backtesting import RoutedBacktestingPandas, _ProviderRegistry
from lumibot.entities import Asset


def test_routed_backtesting_accepts_polygon_provider_in_json_mapping():
    routing = RoutedBacktestingPandas._normalize_routing(
        {
            "default": "thetadata",
            "crypto": "polygon",
        }
    )
    rb = RoutedBacktestingPandas.__new__(RoutedBacktestingPandas)
    rb._routing = routing  # type: ignore[attr-defined]
    rb._registry = _ProviderRegistry(rb)  # type: ignore[attr-defined]

    asset = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
    assert rb._provider_spec_for_asset(asset).provider == "polygon"


def test_routed_backtesting_crypto_future_inherits_crypto_provider():
    routing = RoutedBacktestingPandas._normalize_routing(
        {
            "default": "thetadata",
            "crypto": "ibkr",
        }
    )
    rb = RoutedBacktestingPandas.__new__(RoutedBacktestingPandas)
    rb._routing = routing  # type: ignore[attr-defined]
    rb._registry = _ProviderRegistry(rb)  # type: ignore[attr-defined]

    asset = Asset("BTCUSDT", asset_type=Asset.AssetType.CRYPTO_FUTURE)
    assert rb._provider_spec_for_asset(asset).provider == "ibkr"


def test_routed_backtesting_rejects_unknown_provider():
    rb = RoutedBacktestingPandas.__new__(RoutedBacktestingPandas)
    rb._registry = _ProviderRegistry(rb)  # type: ignore[attr-defined]
    with pytest.raises(ValueError):
        rb._registry.resolve_provider_spec("nope")  # type: ignore[attr-defined]
