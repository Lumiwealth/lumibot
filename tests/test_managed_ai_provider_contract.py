"""Provider-neutral managed AI routing contracts added for universal workloads."""

import os

import pytest

from lumibot.components.agents.managed_gateway import managed_gateway_available_for


_PROVIDER_KEYS = (
    "GEMINI_API_KEY",
    "GOOGLE_API_KEY",
    "OPENAI_API_KEY",
    "ANTHROPIC_API_KEY",
    "XAI_API_KEY",
    "GROK_API_KEY",
)


@pytest.fixture(autouse=True)
def managed_gateway_without_byok(monkeypatch):
    # Universal managed inference is the default; provider credentials remain
    # an explicit owner-selected override rather than an inferred requirement.
    for name in _PROVIDER_KEYS:
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("LUMIBOT_AI_GATEWAY_URL", "https://gateway.example.test")
    monkeypatch.setenv("LUMIBOT_AI_GATEWAY_TOKEN", "deployment-bound-token")


@pytest.mark.parametrize(
    "model",
    [
        "gemini-3.1-flash-lite",
        "google/gemini-3.1-flash-lite",
        "gpt-5.6-luna",
        "openai/gpt-5.6-luna",
        "claude-sonnet-5",
        "anthropic/claude-sonnet-5",
        "grok-4.5",
        "xai/grok-4.5",
    ],
)
def test_prefixed_and_common_bare_models_use_managed_gateway(model):
    assert managed_gateway_available_for(model)


@pytest.mark.parametrize(
    ("model", "key_name"),
    [
        ("gemini-3.1-flash-lite", "GEMINI_API_KEY"),
        ("gpt-5.6-luna", "OPENAI_API_KEY"),
        ("claude-sonnet-5", "ANTHROPIC_API_KEY"),
        ("grok-4.5", "XAI_API_KEY"),
    ],
)
def test_explicit_byok_still_overrides_managed_gateway(monkeypatch, model, key_name):
    monkeypatch.setenv(key_name, "owner-selected-provider-key")

    assert not managed_gateway_available_for(model)
    assert os.environ[key_name] == "owner-selected-provider-key"
