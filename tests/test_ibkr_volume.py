"""Volume units must come from provider metadata, never price/currency guesses."""
from copy import deepcopy
from decimal import Decimal
from enum import Enum

import pytest

from lumibot.tools.ibkr_volume import IbkrVolumeMetadataError, validated_volume_factor


@pytest.mark.parametrize("factor", [1, 100, 0.5, "100", Decimal("100")])
@pytest.mark.parametrize("asset_type", ["stock", "etf", "future"])
def test_explicit_factor_is_preserved(factor, asset_type):
    assert validated_volume_factor({"volumeFactor": factor}, asset_type, "Trades") == float(factor)


@pytest.mark.parametrize("factor", [None, True, False, 0, -100, float("nan"), float("inf"), "NaN", "Infinity", "", "shares", [], {}])
def test_invalid_factor_is_visible(factor):
    with pytest.raises(IbkrVolumeMetadataError, match="volumeFactor"):
        validated_volume_factor({"volumeFactor": factor}, "stock", "Trades")


@pytest.mark.parametrize("asset_type", ["stock", "etf"])
@pytest.mark.parametrize("source", ["Trades", "TRADES", "Last", "ADJUSTED_LAST"])
def test_missing_trade_factor_never_guesses_from_asset_or_route(asset_type, source):
    with pytest.raises(IbkrVolumeMetadataError, match="missing"):
        validated_volume_factor({"_botspot_meta": {"backend": "tws"}}, asset_type, source)


@pytest.mark.parametrize("source", ["MIDPOINT", "Bid_Ask", "BID", "ASK"])
def test_non_volume_sources_preserve_zero_without_inventing_volume(source):
    assert 0 * validated_volume_factor({}, "stock", source) == 0
    assert validated_volume_factor({}, "stock", source) == 1


@pytest.mark.parametrize("asset_type", ["future", "cont_future"])
def test_legacy_futures_are_contracts_not_equity_lots(asset_type):
    assert validated_volume_factor({}, asset_type, "Trades") == 1
    assert validated_volume_factor({"volumeFactor": 10}, asset_type, "Trades") == 10


def test_enum_asset_type_is_supported():
    class AssetType(str, Enum):
        FUTURE = "future"
    assert validated_volume_factor({}, AssetType.FUTURE, "Trades") == 1


def test_real_nvda_rest_factor_preserves_ohlc_and_payload():
    # Captured 2026-10-10, NVDA conid4815747, 2024-06-07 REST daily bar.
    payload = {"volumeFactor": 100, "data": [{"o": 119.79, "c": 120.89, "v": 2398561.2}]}
    original = deepcopy(payload)
    factor = validated_volume_factor(payload, "stock", "Trades")
    assert Decimal(str(payload["data"][0]["v"])) * Decimal(str(factor)) == Decimal("239856120")
    assert validated_volume_factor(payload, "stock", "Trades") == factor
    assert payload == original


def test_already_normalized_explicit_factor_one_does_not_rescale():
    payload = {"volumeFactor": 1, "data": [{"v": 239856120}]}
    assert payload["data"][0]["v"] * validated_volume_factor(payload, "stock", "Trades") == 239856120


@pytest.mark.parametrize("asset_type", ["index", "crypto", "option", "cont_future", "unknown", None])
def test_missing_nonstock_factor_preserves_legacy_units_and_sentinels(asset_type):
    factor = validated_volume_factor({}, asset_type, "Trades")
    assert factor == 1
    assert -1 * factor == -1
    assert 0 * factor == 0


@pytest.mark.parametrize("asset_type", ["index", "future", "crypto", "option"])
def test_explicit_invalid_factor_is_rejected_even_for_nonstock(asset_type):
    with pytest.raises(IbkrVolumeMetadataError):
        validated_volume_factor({"volumeFactor": False}, asset_type, "Trades")
