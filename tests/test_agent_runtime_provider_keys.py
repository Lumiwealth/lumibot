import os
import sys
import types
import asyncio

import pytest

from lumibot.components.agents.runtime import (
    GoogleADKRuntime,
    RuntimeRequest,
    _aggregate_usage_metadata,
    _resolve_model_for_adk,
    _strip_thought_parts_from_litellm_request,
    _supports_explicit_temperature_for_adk_model,
    _sync_gemini_api_key_alias,
    _sync_together_api_key_alias,
    _sync_xai_api_key_alias,
    _wrap_tool_callable,
)
from lumibot.components.agents.schemas import BoundTool


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


def test_wrapped_tool_does_not_block_repeated_calls():
    calls = {"count": 0}

    def sample_tool():
        calls["count"] += 1
        return {"value": calls["count"]}

    tool = BoundTool(name="sample_tool", description="sample", function=sample_tool)
    wrapped = _wrap_tool_callable(tool)

    assert wrapped()["value"] == 1
    assert wrapped()["value"] == 2
    assert calls["count"] == 2


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


def test_cerebras_model_uses_reasoning_content_sanitizing_wrapper(monkeypatch):
    class FakeLiteLlm:
        def __init__(self, **kwargs):
            self.kwargs = dict(kwargs)

    fake_module = types.ModuleType("google.adk.models.lite_llm")
    fake_module.LiteLlm = FakeLiteLlm
    monkeypatch.setitem(sys.modules, "google.adk.models.lite_llm", fake_module)

    result = _resolve_model_for_adk("cerebras/gpt-oss-120b")

    assert isinstance(result, FakeLiteLlm)
    assert result.__class__ is not FakeLiteLlm
    assert result.kwargs["model"] == "cerebras/gpt-oss-120b"


def test_strip_thought_parts_from_litellm_request_preserves_normal_parts():
    class Part:
        def __init__(self, text: str, thought: bool = False):
            self.text = text
            self.thought = thought

    class Content:
        def __init__(self, role: str, parts: list[Part]):
            self.role = role
            self.parts = parts

        def model_copy(self, update):
            return Content(self.role, update["parts"])

    request = types.SimpleNamespace(
        contents=[
            Content("model", [Part("private reasoning", thought=True), Part("visible answer")]),
            Content("user", [Part("question")]),
        ]
    )

    _strip_thought_parts_from_litellm_request(request)

    assert [[part.text for part in content.parts] for content in request.contents] == [["visible answer"], ["question"]]


def test_runtime_prompt_only_names_available_tools():
    runtime = GoogleADKRuntime()
    bound_tools = [
        BoundTool(
            name="account_positions",
            description="Positions",
            function=lambda: {},
        ),
        BoundTool(
            name="get_indicator",
            description="Indicator",
            function=lambda: {},
        ),
    ]
    request = RuntimeRequest(
        agent_name="researcher",
        model="deepseek/deepseek-v4-flash",
        system_prompt="System prompt",
        task_prompt=None,
        context=None,
        runtime_context=None,
        memory_notes=[],
        bound_tools=bound_tools,
    )

    instruction = runtime._instruction_for(request)
    user_text = runtime._build_user_text(request)

    assert "account_positions, get_indicator" in instruction
    assert "Available Tools JSON" in user_text
    assert "list_fred_series" not in user_text
    assert "alpaca_news" not in user_text


def test_runtime_enforces_agent_run_timeout(monkeypatch):
    runtime = GoogleADKRuntime()

    async def never_finishes(_request):
        await asyncio.sleep(10)

    monkeypatch.setattr(runtime, "_run_async", never_finishes)
    monkeypatch.setenv("LUMIBOT_AGENT_RUN_TIMEOUT_SECONDS", "0.01")
    monkeypatch.setenv("LUMIBOT_AGENT_MAX_RUN_ATTEMPTS", "1")

    request = RuntimeRequest(
        agent_name="researcher",
        model="gemini-3.5-flash",
        system_prompt="System prompt",
        task_prompt="Do work",
        context=None,
        runtime_context={"mode": "backtesting"},
        memory_notes=[],
        bound_tools=[],
    )

    with pytest.raises(TimeoutError, match="Agent run exceeded 0.01s timeout"):
        runtime.run(request)


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
