"""Regression tests for mode-neutral strategy parameter overrides.

These expectations intentionally supersede the backtest-only naming added in
March 2026: one saved parameter set must resolve the same way in simulated and
live execution.
"""

import importlib
import json

import pytest


@pytest.mark.parametrize("is_backtesting", ["true", "false"])
def test_generic_strategy_parameters_are_mode_neutral(monkeypatch, is_backtesting):
    values = {"fast": 12, "slow": 26, "risk_fraction": 0.02}
    monkeypatch.setenv("IS_BACKTESTING", is_backtesting)
    monkeypatch.setenv("LUMIBOT_STRATEGY_PARAMETERS", json.dumps(values))
    monkeypatch.delenv("BACKTESTING_PARAMETERS", raising=False)

    import lumibot.credentials

    credentials = importlib.reload(lumibot.credentials)
    assert credentials.STRATEGY_PARAMETERS == values


def test_generic_strategy_parameters_take_precedence_over_legacy_alias(monkeypatch):
    monkeypatch.setenv("LUMIBOT_STRATEGY_PARAMETERS", '{"fast": 20}')
    monkeypatch.setenv("BACKTESTING_PARAMETERS", '{"fast": 5}')

    import lumibot.credentials

    credentials = importlib.reload(lumibot.credentials)
    assert credentials.STRATEGY_PARAMETERS == {"fast": 20}


@pytest.mark.parametrize("payload", ["not-json", "[1, 2]", "null", "{}"])
def test_invalid_or_empty_generic_strategy_parameters_are_ignored(monkeypatch, payload):
    monkeypatch.setenv("LUMIBOT_STRATEGY_PARAMETERS", payload)
    monkeypatch.delenv("BACKTESTING_PARAMETERS", raising=False)

    import lumibot.credentials

    credentials = importlib.reload(lumibot.credentials)
    assert credentials.STRATEGY_PARAMETERS is None
