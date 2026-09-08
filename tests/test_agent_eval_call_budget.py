"""No inference: exercise durable reservations and actual runtime callbacks."""

from concurrent.futures import ThreadPoolExecutor
from types import SimpleNamespace

import pytest

from lumibot.components.agents.runtime import GoogleADKRuntime, RuntimeRequest
from scripts.agent_eval_call_budget import EvalCallBudget

PRICES = {"gemini-fixture": {"input": 0.30, "cached_input": 0.03, "output": 2.50}}


def budget(path, cap=1):
    return EvalCallBudget(path, cap_usd=cap, prices=PRICES, max_input_tokens=1_000_000)


def test_reservation_survives_crash_and_resume(tmp_path):
    path = tmp_path / "calls.jsonl"
    first = budget(path, cap=0.4)
    first.reserve("gemini-fixture", 12000)
    resumed = budget(path, cap=0.4)
    assert resumed.committed_usd == pytest.approx(0.33)
    with pytest.raises(ValueError, match="spending limit"):
        resumed.reserve("gemini-fixture", 12000)


def test_actual_usage_releases_only_unused_reservation_and_is_idempotent(tmp_path):
    ledger = budget(tmp_path / "calls.jsonl")
    token = ledger.reserve("gemini-fixture", 12000)
    usage = {
        "prompt_token_count": 1000,
        "candidates_token_count": 20,
        "thoughts_token_count": 10,
        "cached_content_token_count": 500,
    }
    ledger.settle(token, usage)
    ledger.settle(token, usage)
    assert ledger.committed_usd == pytest.approx(0.00024)
    assert ledger.snapshot()["settled_calls"] == 1
    assert ledger.snapshot()["reserved_calls"] == 0


def test_unknown_usage_remains_reserved_and_corrupt_ledger_fails_closed(tmp_path):
    path = tmp_path / "calls.jsonl"
    ledger = budget(path)
    token = ledger.reserve("gemini-fixture", 12000)
    with pytest.raises(ValueError):
        ledger.settle(token, {})
    assert ledger.committed_usd == pytest.approx(0.33)
    with path.open("a") as stream:
        stream.write('{"incomplete":')
    with pytest.raises(ValueError, match="ledger"):
        budget(path)


def test_concurrent_instances_cannot_overreserve_shared_cap(tmp_path):
    path = tmp_path / "calls.jsonl"
    a, b = budget(path, cap=0.4), budget(path, cap=0.4)

    def attempt(item):
        try:
            item.reserve("gemini-fixture", 12000)
            return True
        except ValueError:
            return False

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(attempt, [a, b])) == [False, True]


def test_unknown_model_and_changed_cap_are_rejected(tmp_path):
    path = tmp_path / "calls.jsonl"
    ledger = budget(path)
    with pytest.raises(ValueError, match="pricing"):
        ledger.reserve("unknown", 100)
    with pytest.raises(ValueError, match="cap"):
        budget(path, cap=2)


def test_runtime_budget_reserves_every_continuation_and_disables_hidden_retries(tmp_path):
    from google.genai import types

    ledger = budget(tmp_path / "calls.jsonl", cap=0.4)
    request = RuntimeRequest(
        agent_name="budget_test",
        model="gemini-fixture",
        system_prompt="test",
        task_prompt="test",
        context=None,
        runtime_context=None,
        memory_state=None,
        memory_notes=[],
        bound_tools=[],
        max_output_tokens=12000,
        model_call_budget=ledger,
    )
    before, after = GoogleADKRuntime()._model_callbacks(request)
    before(llm_request=SimpleNamespace(contents=[]))
    after(
        llm_response=SimpleNamespace(
            partial=False,
            usage_metadata=types.GenerateContentResponseUsageMetadata(
                prompt_token_count=1000, candidates_token_count=20
            ),
        )
    )
    before(llm_request=SimpleNamespace(contents=[]))
    # Simulate provider failure with no final usage: its reservation is retained.
    with pytest.raises(ValueError, match="spending limit"):
        before(llm_request=SimpleNamespace(contents=[]))
    config = GoogleADKRuntime._generate_content_config_kwargs_for_request(request, types)
    assert config["http_options"].retry_options.attempts == 1
    assert ledger.snapshot()["settled_calls"] == 1
    assert ledger.snapshot()["reserved_calls"] == 1


def test_actual_adk_loop_budgets_two_native_calls_and_one_tool_result(tmp_path, monkeypatch):
    from google.adk.models.base_llm import BaseLlm
    from google.adk.models.llm_response import LlmResponse
    from google.genai import types

    from lumibot.components.agents import runtime
    from lumibot.components.agents.tools import BoundTool

    seen = []

    class ProviderFixture(BaseLlm):
        async def generate_content_async(self, llm_request, stream=False):
            assert llm_request.config.http_options.retry_options.attempts == 1
            seen.append(llm_request)
            if len(seen) == 1:
                part = types.Part(function_call=types.FunctionCall(name="account_portfolio", args={}, id="portfolio-1"))
            else:
                assert any(part.function_response for content in llm_request.contents for part in content.parts)
                part = types.Part(text="Observed portfolio; no trade requested.")
            yield LlmResponse(
                content=types.Content(role="model", parts=[part]),
                usage_metadata=types.GenerateContentResponseUsageMetadata(
                    prompt_token_count=100, candidates_token_count=10
                ),
            )

    monkeypatch.setattr(
        runtime, "_resolve_model_for_adk", lambda *args, **kwargs: ProviderFixture(model="gemini-fixture")
    )
    ledger = budget(tmp_path / "calls.jsonl")
    request = RuntimeRequest(
        agent_name="budget_loop",
        model="gemini-fixture",
        system_prompt="Read portfolio.",
        task_prompt="Read portfolio.",
        context=None,
        runtime_context=None,
        memory_state=None,
        memory_notes=[],
        include_builtin_skills=False,
        bound_tools=[
            BoundTool(name="account_portfolio", description="Read portfolio.", function=lambda: {"cash": 100})
        ],
        max_output_tokens=12000,
        model_call_budget=ledger,
    )
    result = GoogleADKRuntime().run(request)
    assert len(seen) == 2
    assert any(event.kind == "tool_result" for event in result.events)
    snapshot = ledger.snapshot()
    assert snapshot["committed_usd"] == 0.00011
    assert snapshot["settled_calls"] == 2
    assert snapshot["reserved_calls"] == 0
    assert snapshot["usage"]["input_tokens"] == 200
