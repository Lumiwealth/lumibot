"""Historical research jobs receive only credentials for their chosen provider."""

import importlib.util
from pathlib import Path

import pytest


@pytest.mark.parametrize("source", ["yahoo", "polygon"])
def test_non_alpaca_research_job_does_not_inherit_alpaca_keys(source, monkeypatch):
    path = Path(__file__).resolve().parents[1] / "scripts/run_ai_strategy_flash_backtests.py"
    spec = importlib.util.spec_from_file_location("flash_runner", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    monkeypatch.setattr(module, "_load_openai_key", lambda: "fixture-key")
    monkeypatch.setattr(module, "_env_file_value", lambda filename, key: "fixture-value")
    env = module._child_env(source)
    assert not any(key.startswith("ALPACA_") for key in env)
