"""Generic prediction outcome identity and precision; neutral synthetic contracts."""

from decimal import Decimal

import pytest

from lumibot.entities import Asset, Order, Position


def test_yes_no_are_distinct_and_round_trip_through_entities():
    yes = Asset("SYNTHETIC-EVENT", asset_type="prediction_contract", outcome="yes")
    no = Asset("SYNTHETIC-EVENT", asset_type="prediction_contract", outcome="no")
    assert len({yes: "YES", no: "NO"}) == 2
    for asset in (yes, no):
        assert Asset.from_dict(asset.to_dict()) == asset
        assert Asset.from_dict(asset.to_minimal_dict()) == asset
        assert asset.to_minimal_dict()["outcome"] == asset.outcome
        order = Order("neutral", asset, Decimal("1.25"), "buy", limit_price=0.4321)
        assert Order.from_dict(order.to_dict()).asset == asset
        position = Position("neutral", asset, Decimal("1.25"))
        assert Position.from_dict(position.to_dict()).asset == asset
        assert asset.outcome.upper() in str(asset).upper()


def test_legacy_asset_identity_hash_and_wire_shape_are_unchanged():
    asset = Asset("TOKEN-ID", asset_type="prediction_contract")
    assert "outcome" not in asset.to_dict()
    assert "outcome" not in asset.to_minimal_dict()
    assert Asset.from_dict(asset.to_dict()) == asset
    assert hash(asset) == hash((asset.symbol, asset.asset_type, asset.expiration, asset.strike, asset.right, None))
    assert str(asset) == "TOKEN-ID"


def test_outcome_labels_are_generic_and_case_normalized():
    assert Asset("EVENT", asset_type="prediction_contract", outcome="YES") == Asset(
        "EVENT", asset_type="prediction_contract", outcome="yes"
    )
    named = Asset("EVENT", asset_type="prediction_contract", outcome="Candidate A")
    assert Asset.from_dict(named.to_dict()) == named


def test_prediction_partial_fill_average_uses_filled_quantity_and_subcent_precision():
    order = Order("neutral", Asset("EVENT", asset_type="prediction_contract"), 10, "buy", limit_price=0.5)
    order.add_transaction(Decimal("0.4321"), Decimal("1.25"))
    order.add_transaction(Decimal("0.4323"), Decimal("0.75"))
    assert order.get_fill_price() == pytest.approx(0.432175)


def test_prediction_zero_valuation_is_not_missing():
    position = Position("neutral", Asset("EVENT", asset_type="prediction_contract"), 1)
    position.current_price = position.market_value = position.pnl = position.pnl_percent = 0
    result = position.to_dict()
    for field in ("current_price", "market_value", "pnl", "pnl_percent"):
        assert result[field] == 0


def test_selected_outcomes_survive_existing_strategy_backup_codec():
    from lumibot.strategies._strategy import _Strategy

    assets = tuple(Asset("EVENT", "prediction_contract", outcome=side) for side in ("yes", "no"))
    restored = _Strategy._deserialize_variables_from_backup(
        _Strategy._serialize_variables_for_backup({"contracts": assets})
    )
    assert restored["contracts"] == assets
    assert len(set(restored["contracts"])) == 2
