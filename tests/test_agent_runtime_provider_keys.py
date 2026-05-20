import os
import sys
import types

from lumibot.components.agents.runtime import (
    _aggregate_usage_metadata,
    _resolve_model_for_adk,
    _supports_explicit_temperature_for_adk_model,
    _sync_gemini_api_key_alias,
    _sync_together_api_key_alias,
    _sync_xai_api_key_alias,
)


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


def test_together_api_key_alias_populates_litellm_key(monkeypatch):
    monkeypatch.delenv("TOGETHERAI_API_KEY", raising=False)
    monkeypatch.setenv("TOGETHER_API_KEY", "together-test-key")

    _sync_together_api_key_alias()

    assert os.environ["TOGETHERAI_API_KEY"] == "together-test-key"


def test_together_litellm_key_populates_sdk_key(monkeypatch):
    monkeypatch.delenv("TOGETHER_API_KEY", raising=False)
    monkeypatch.setenv("TOGETHERAI_API_KEY", "togetherai-test-key")

    _sync_together_api_key_alias()

    assert os.environ["TOGETHER_API_KEY"] == "togetherai-test-key"


def test_together_litellm_key_wins_over_sdk_alias(monkeypatch):
    monkeypatch.setenv("TOGETHER_API_KEY", "together-test-key")
    monkeypatch.setenv("TOGETHERAI_API_KEY", "togetherai-test-key")

    _sync_together_api_key_alias()

    assert os.environ["TOGETHERAI_API_KEY"] == "togetherai-test-key"
    assert os.environ["TOGETHER_API_KEY"] == "together-test-key"


def test_aggregate_usage_metadata_sums_multiple_provider_events():
    usage = _aggregate_usage_metadata(
        [
            {"prompt_token_count": 5571, "candidates_token_count": 482, "total_token_count": 6053},
            {
                "prompt_token_count": 14145,
                "candidates_token_count": 237,
                "total_token_count": 14382,
                "cached_content_token_count": 5760,
            },
            {
                "prompt_token_count": 16323,
                "candidates_token_count": 93,
                "total_token_count": 16416,
                "cached_content_token_count": 13952,
            },
        ]
    )

    assert usage["prompt_token_count"] == 36039
    assert usage["candidates_token_count"] == 812
    assert usage["total_token_count"] == 36851
    assert usage["cached_content_token_count"] == 19712
    assert usage["aggregated_usage_event_count"] == 3


def test_openai_model_forwards_prompt_cache_key_and_24h_retention(monkeypatch):
    created: dict[str, object] = {}

    class FakeLiteLlm:
        def __init__(self, **kwargs):
            created.update(kwargs)

    fake_module = types.ModuleType("google.adk.models.lite_llm")
    fake_module.LiteLlm = FakeLiteLlm
    monkeypatch.setitem(sys.modules, "google.adk.models.lite_llm", fake_module)

    result = _resolve_model_for_adk("openai/gpt-5.4-mini", prompt_cache_key="stable-prefix-key")

    assert isinstance(result, FakeLiteLlm)
    assert created["model"] == "openai/gpt-5.4-mini"
    assert created["prompt_cache_key"] == "stable-prefix-key"
    assert created["prompt_cache_retention"] == "24h"


def test_xai_model_forwards_grok_conversation_cache_header(monkeypatch):
    created: dict[str, object] = {}

    class FakeLiteLlm:
        def __init__(self, **kwargs):
            created.update(kwargs)

    fake_module = types.ModuleType("google.adk.models.lite_llm")
    fake_module.LiteLlm = FakeLiteLlm
    monkeypatch.setitem(sys.modules, "google.adk.models.lite_llm", fake_module)

    result = _resolve_model_for_adk("xai/grok-4.20-0309-reasoning", prompt_cache_key="stable-prefix-key")

    assert isinstance(result, FakeLiteLlm)
    assert created["model"] == "xai/grok-4.20-0309-reasoning"
    assert created["headers"] == {"x-grok-conv-id": "stable-prefix-key"}


def test_new_provider_models_route_through_litellm(monkeypatch):
    created: list[dict[str, object]] = []

    class FakeLiteLlm:
        def __init__(self, **kwargs):
            self.kwargs = dict(kwargs)
            created.append(self.kwargs)

    fake_module = types.ModuleType("google.adk.models.lite_llm")
    fake_module.LiteLlm = FakeLiteLlm
    monkeypatch.setitem(sys.modules, "google.adk.models.lite_llm", fake_module)
    monkeypatch.setenv("TOGETHER_API_KEY", "together-test-key")
    monkeypatch.delenv("TOGETHERAI_API_KEY", raising=False)

    models = [
        "deepseek/deepseek-v4-flash",
        "deepseek/deepseek-v4-pro",
        "together_ai/deepseek-ai/DeepSeek-V4-Pro",
        "together_ai/moonshotai/Kimi-K2.6",
        "cerebras/gpt-oss-120b",
        "cerebras/zai-glm-4.7",
    ]

    results = [_resolve_model_for_adk(model, prompt_cache_key="stable-prefix-key") for model in models]

    assert all(isinstance(result, FakeLiteLlm) for result in results)
    assert [kwargs["model"] for kwargs in created] == models
    assert os.environ["TOGETHERAI_API_KEY"] == "together-test-key"
    assert all("prompt_cache_key" not in kwargs for kwargs in created)


def test_gemini_native_path_uses_plain_model_id_for_implicit_or_adk_context_cache():
    # Gemini stays on ADK's native path. Provider prompt-cache routing kwargs are
    # only for LiteLLM providers; Gemini implicit caching and ADK explicit
    # ContextCacheConfig are configured outside the LiteLLM wrapper.
    assert _resolve_model_for_adk("gemini-3.1-pro-preview", prompt_cache_key="stable-prefix-key") == "gemini-3.1-pro-preview"


def test_explicit_temperature_only_sent_to_gemini_native_models():
    assert _supports_explicit_temperature_for_adk_model("gemini-3.1-pro-preview") is True
    assert _supports_explicit_temperature_for_adk_model("models/gemini-3.1-pro-preview") is True

    # GPT-5/reasoning-class OpenAI models reject custom temperature values; the
    # provider default is the only accepted value.
    assert _supports_explicit_temperature_for_adk_model("openai/gpt-5.4") is False
    assert _supports_explicit_temperature_for_adk_model("openai/gpt-5.4-mini") is False
    assert _supports_explicit_temperature_for_adk_model("xai/grok-4.20-0309-reasoning") is False
    assert _supports_explicit_temperature_for_adk_model("anthropic/claude-opus-4-7") is False
    assert _supports_explicit_temperature_for_adk_model("deepseek/deepseek-v4-flash") is False
    assert _supports_explicit_temperature_for_adk_model("together_ai/moonshotai/Kimi-K2.6") is False
    assert _supports_explicit_temperature_for_adk_model("cerebras/gpt-oss-120b") is False
