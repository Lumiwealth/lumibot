"""Persistence must retain deferred futures selection, not just a lossy dict."""

import datetime
import json

import pytest

from lumibot.entities import Asset, Order, Position
from lumibot.strategies._strategy import _Strategy
from tests.test_strategy_asset_backup import restart as restart


@pytest.mark.parametrize("selector", [*Asset.AutoExpiry, True])
def test_asset_json_preserves_deferred_expiry_identity(selector):
    original = Asset("ES", "future", auto_expiry=selector)
    restored = Asset.from_dict(json.loads(json.dumps(original.to_dict())))
    assert restored.auto_expiry == selector, "Deferred futures selector lost during persistence"
    assert restored.expiration is None
    assert restored == original
    assert hash(restored) == hash(original)


def test_restart_future_reaches_ibkr_resolution(restart, monkeypatch):
    from lumibot.tools import ibkr_helper

    class ReachedResolution(BaseException):
        pass

    def reached_resolution(_symbol):
        raise ReachedResolution("Passed expiry preflight without provider access")

    monkeypatch.setattr(ibkr_helper, "_resolve_futures_exchange", reached_resolution)
    original = Asset("ES", "future", auto_expiry=Asset.AutoExpiry.NEXT_QUARTER)
    strategy = restart({"instruments": [{"pair": (original, Asset("USD", "forex"))}]})
    restored = strategy.vars.instruments[0]["pair"][0]
    arguments = {
        "quote": Asset("USD", "forex"),
        "timestep": "minute",
        "start_dt": datetime.datetime(2026, 9, 10, tzinfo=datetime.timezone.utc),
        "end_dt": datetime.datetime(2026, 9, 11, tzinfo=datetime.timezone.utc),
    }
    for instrument in (original, restored):
        with pytest.raises(ReachedResolution):
            ibkr_helper.get_price_data(asset=instrument, **arguments)
    assert restored == original
    assert restored.auto_expiry == original.auto_expiry
    again = restart(
        strategy.vars.all(), initialized={"instruments": [{"pair": (original, Asset("USD", "forex"))}]}
    )
    assert again.vars.instruments[0]["pair"][0] == original
    assert again._last_backup_state == strategy._last_backup_state


@pytest.mark.parametrize("missing", [(), ("auto_expiry",), ("leverage",), ("auto_expiry", "leverage")])
def test_legacy_optional_asset_fields_remain_supported(restart, missing):
    saved = Asset("SPY").to_dict()
    for key in missing:
        saved.pop(key, None)
    strategy = restart(
        {}, raw=json.dumps({"instrument": saved, "metadata": saved}), initialized={"instrument": Asset("SPY")}
    )
    assert strategy.vars.instrument == Asset("SPY")
    assert strategy.vars.instrument.auto_expiry is None
    assert strategy.vars.metadata == saved


def test_missing_selector_does_not_invent_initialized_roll_rule():
    initial = Asset("ES", "future", auto_expiry=Asset.AutoExpiry.NEXT_QUARTER)
    saved = initial.to_dict()
    saved.pop("auto_expiry", None)
    restored = _Strategy._deserialize_variables_from_backup(
        json.dumps({"instrument": saved}), {"instrument": initial}
    )
    assert restored["instrument"].auto_expiry is None
    assert restored["instrument"].expiration is None


def test_legacy_schema_still_rejects_unknown_or_missing_required_fields():
    for invalid in ({**Asset("SPY").to_dict(), "unexpected": 1}, {"symbol": "SPY"}):
        with pytest.raises(ValueError, match="does not match the Asset schema"):
            _Strategy._restore_legacy_assets(invalid, Asset("SPY"))


def test_explicit_expiry_and_nested_entity_consumers_keep_identity():
    expiry = datetime.date(2027, 3, 19)
    future = Asset("ES", "future", expiration=expiry, auto_expiry=Asset.AutoExpiry.NEXT_QUARTER)
    option = Asset("SPY", "option", expiration=expiry, strike=500, right="CALL", underlying_asset=future)
    for asset in (future, Asset("SPY"), option):
        restored = Asset.from_dict(json.loads(json.dumps(asset.to_dict())))
        assert restored == asset
        assert restored.expiration == asset.expiration
        assert restored.auto_expiry == asset.auto_expiry
        if asset.underlying_asset:
            assert restored.underlying_asset.auto_expiry == future.auto_expiry
    order = Order("persistence-test", future, 1, "buy")
    restored_order = Order.from_dict(json.loads(json.dumps(order.to_dict())))
    position = Position("persistence-test", future, 1, orders=[order], avg_fill_price=10)
    position_payload = json.loads(json.dumps(position.to_dict()))
    restored_position_asset = Asset.from_dict(position_payload["asset"])
    assert restored_order.asset.auto_expiry == future.auto_expiry
    assert restored_position_asset.auto_expiry == future.auto_expiry
    assert Asset.from_dict(position_payload["orders"][0]["asset"]).expiration == expiry
