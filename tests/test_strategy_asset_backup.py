"""Restart persistence must retain instruments accepted by strategy APIs."""

import datetime
import json
import logging
import os
from unittest.mock import Mock

import pytest
from sqlalchemy import create_engine, text

from lumibot.entities import Asset
from lumibot.strategies import Strategy
from lumibot.strategies._strategy import Vars, _Strategy


@pytest.fixture(params=["scheduled", "database"])
def restart(request, tmp_path, monkeypatch):
    engine = create_engine("sqlite:///:memory:")
    with engine.begin() as connection:
        connection.execute(
            text("CREATE TABLE variables (id TEXT, last_updated TEXT, variables TEXT, strategy_id TEXT)")
        )
    monkeypatch.setenv("LUMIBOT_SCHEDULED_EXECUTION", str(request.param == "scheduled").lower())
    monkeypatch.setenv("LUMIBOT_SCHEDULED_STATE_FILE", str(tmp_path / "state.json"))
    monkeypatch.setenv("LUMIBOT_SCHEDULED_STATE_BACKEND", "s3")

    def make_strategy():
        strategy = object.__new__(Strategy)
        strategy._name = "persistence-test"
        strategy.is_backtesting = False
        strategy.vars = Vars()
        strategy.logger = logging.getLogger("asset-backup-test")
        strategy._last_backup_state = None
        strategy.db_connection_str = "sqlite:///:memory:"
        strategy.db_engine = engine
        strategy.backup_table_name = "variables"
        strategy.should_backup_variables_to_database = True
        strategy._chart_ohlc_list = []
        strategy._refresh_live_positions = lambda **kwargs: None
        strategy.broker = Mock()
        strategy.broker.get_tracked_position.return_value = None
        return strategy

    def save_and_restart(variables, raw=None):
        original = make_strategy()
        for key, value in variables.items():
            original.vars.set(key, value)
        if raw is None:
            original.backup_variables_to_db()
        elif request.param == "scheduled":
            (tmp_path / "state.json").write_text(raw, encoding="utf-8")
        else:
            with engine.begin() as connection:
                connection.execute(text("DELETE FROM variables"))
                connection.execute(
                    text("INSERT INTO variables VALUES ('legacy', '2026-09-12', :payload, 'persistence-test')"),
                    {"payload": raw},
                )
        restored = make_strategy()
        if raw is not None:
            restored.vars.set("untouched", "initial")
        restored.load_variables_from_db()
        return restored

    yield save_and_restart
    engine.dispose()


@pytest.mark.parametrize(
    "asset",
    [
        Asset("SPY"),
        Asset("BTC", asset_type="crypto_future", leverage=5, precision="0.0001"),
        Asset(
            "SPY",
            asset_type="option",
            expiration=datetime.date(2027, 1, 15),
            strike=500,
            right="CALL",
            multiplier=100,
            underlying_asset=Asset("SPY"),
        ),
    ],
)
def test_restart_restores_asset_for_position_and_chart_apis(restart, asset):
    strategy = restart({"instrument": asset, "nested": [{"pair": (asset, Asset("USD", "forex"))}]})
    restored = strategy.vars.instrument
    assert isinstance(restored, Asset)
    assert restored.to_dict() == asset.to_dict()
    assert isinstance(strategy.vars.nested[0]["pair"][0], Asset)
    assert strategy.get_position(restored) is None
    strategy.broker.get_tracked_position.assert_called_once_with("persistence-test", restored)
    bar = strategy.add_ohlc(
        "price", 10, 12, 9, 11, asset=restored, dt=datetime.datetime(2026, 9, 12, tzinfo=datetime.timezone.utc)
    )
    assert bar["asset_symbol"] == asset.symbol
    # A second restart must preserve types and an unchanged backup fingerprint.
    again = restart(strategy.vars.all())
    assert again.vars.instrument.to_dict() == asset.to_dict()
    assert again._last_backup_state == strategy._last_backup_state


def test_asset_shaped_plain_dictionary_stays_dictionary(restart):
    value = Asset("SPY").to_dict()
    strategy = restart({"metadata": value, "empty": {}, "items": []})
    assert isinstance(strategy.vars.metadata, dict)
    assert strategy.vars.metadata == value
    assert strategy.vars.empty == {}
    assert strategy.vars.items == []


def test_legacy_untagged_asset_is_not_guessed():
    value = Asset("SPY").to_dict()
    assert _Strategy._deserialize_variables_from_backup(json.dumps({"asset": value})) == {"asset": value}


def test_variable_names_matching_envelope_are_preserved():
    variables = {"__lumibot_type__": "Asset", "value": Asset("SPY").to_dict()}
    encoded = _Strategy._serialize_variables_for_backup(variables)
    assert _Strategy._deserialize_variables_from_backup(encoded) == variables


def test_invalid_asset_tag_fails_before_partial_restore():
    with pytest.raises((KeyError, TypeError, ValueError)):
        _Strategy._deserialize_variables_from_backup('{"asset":{"__lumibot_type__":"Asset","value":{}}}')


def test_malformed_backup_keeps_initialized_state(restart):
    strategy = restart({}, raw='{"untouched":"changed","asset":{"__lumibot_type__":"Asset","value":{}}}')
    assert strategy.vars.all() == {"untouched": "initial"}
    assert strategy._last_backup_state is None


def test_legacy_date_strings_keep_backend_contract(restart):
    strategy = restart(
        {},
        raw=json.dumps(
            {"day": "2026-09-12", "nested": [{"day": "2026-09-12"}], "dates": ["2026-09-12"], "label": "2026-09-bad"}
        ),
    )
    expected = "2026-09-12" if os.environ["LUMIBOT_SCHEDULED_EXECUTION"] == "true" else datetime.date(2026, 9, 12)
    assert strategy.vars.day == expected
    assert strategy.vars.nested == [{"day": expected}]
    assert strategy.vars.dates == ["2026-09-12"]
    assert strategy.vars.label == "2026-09-bad"


@pytest.mark.parametrize(
    "value",
    [
        {"__lumibot_type__": "Asset", "value": Asset("SPY").to_dict()},
        {"__lumibot_type__": "dict", "value": {"label": "metadata"}},
        {"__lumibot_type__": "tuple", "value": [1, 2]},
    ],
)
def test_literal_type_envelope_remains_dictionary(restart, value):
    strategy = restart({"metadata": value, "nested": [value]})
    assert strategy.vars.metadata == value
    assert strategy.vars.nested == [value]
    again = restart(strategy.vars.all())
    assert again.vars.metadata == value


@pytest.mark.parametrize("bad", [object(), {1: "one", "two": 2}])
def test_unserializable_state_retains_previous_backup(restart, bad, caplog):
    restart({"instrument": Asset("SPY")})
    strategy = restart({"bad": bad})
    assert strategy.vars.instrument.to_dict() == Asset("SPY").to_dict()
    assert not hasattr(strategy.vars, "bad")
    assert "Error backing up variables" in caplog.text
