import os

from lumibot.components.agents.runtime import _sync_gemini_api_key_alias, _sync_xai_api_key_alias


def test_grok_api_key_alias_populates_xai_api_key(monkeypatch):
    monkeypatch.delenv("XAI_API_KEY", raising=False)
    monkeypatch.setenv("GROK_API_KEY", "grok-test-key")

    _sync_xai_api_key_alias()

    assert os.environ["XAI_API_KEY"] == "grok-test-key"


def test_xai_api_key_wins_over_grok_alias(monkeypatch):
    monkeypatch.setenv("XAI_API_KEY", "xai-test-key")
    monkeypatch.setenv("GROK_API_KEY", "grok-test-key")

    _sync_xai_api_key_alias()

    assert os.environ["XAI_API_KEY"] == "xai-test-key"


def test_gemini_api_key_alias_populates_google_api_key(monkeypatch):
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)
    monkeypatch.setenv("GEMINI_API_KEY", "gemini-test-key")

    _sync_gemini_api_key_alias()

    assert os.environ["GOOGLE_API_KEY"] == "gemini-test-key"


def test_google_api_key_wins_over_gemini_alias(monkeypatch):
    monkeypatch.setenv("GOOGLE_API_KEY", "google-test-key")
    monkeypatch.setenv("GEMINI_API_KEY", "gemini-test-key")

    _sync_gemini_api_key_alias()

    assert os.environ["GOOGLE_API_KEY"] == "google-test-key"
