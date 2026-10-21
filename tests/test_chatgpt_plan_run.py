"""Owned run credentials and admission contracts, never real-model evals."""
import asyncio
import json
import time
from concurrent.futures import ThreadPoolExecutor

import httpx
import pytest

from lumibot.components.agents import chatgpt_auth as auth


@pytest.fixture
def run_profile(tmp_path):
    path = tmp_path / "run.json"
    auth.save_private(path, {
        "credential_kind": "run", "run_id": "synthetic-run", "status": "connected",
        "scopes": [auth.PLAN_SCOPE], "access_token": "synthetic-run-token",
        "expires_at": time.time() + 600, "model": "gpt-6-luna",
        "limits": {"calls": 2, "input_tokens": 3_000_000, "output_tokens": 300_000},
    })
    return path


def test_run_access_is_nonrenewable_and_never_calls_oauth(run_profile, monkeypatch):
    monkeypatch.setattr(auth.httpx, "post", lambda *a, **k: pytest.fail("Run credentials must not refresh"))
    assert auth.access_token(run_profile) == "synthetic-run-token"
    data = auth.read_private(run_profile)
    data["expires_at"] = time.time() - 1
    auth.save_private(run_profile, data)
    with pytest.raises(RuntimeError, match="expired"):
        auth.access_token(run_profile)


def test_run_rejects_refresh_tokens_and_missing_plan_permission(run_profile):
    from lumibot.components.agents.chatgpt_run import RunBudget
    data = auth.read_private(run_profile)
    data["refresh_token"] = "must-not-be-exported"
    auth.save_private(run_profile, data)
    with pytest.raises(ValueError, match="refresh"):
        RunBudget(run_profile)
    data.pop("refresh_token")
    data["scopes"] = []
    auth.save_private(run_profile, data)
    with pytest.raises(ValueError, match="permission"):
        RunBudget(run_profile)


@pytest.mark.parametrize("scopes", [auth.PLAN_SCOPE, {auth.PLAN_SCOPE: True}, None])
def test_run_requires_an_explicit_scope_list(run_profile, scopes):
    data = auth.read_private(run_profile)
    data["scopes"] = scopes
    auth.save_private(run_profile, data)
    with pytest.raises(ValueError, match="permission"):
        auth.access_token(run_profile)


def test_expired_run_receipts_remain_readable_but_cannot_buy_another_request(run_profile, monkeypatch):
    from lumibot.components.agents import chatgpt_run
    budget = chatgpt_run.RunBudget(run_profile)
    ticket = budget.reserve("gpt-6-luna", input_bound=100, output_bound=50)
    expiry = auth.read_private(run_profile)["expires_at"]
    monkeypatch.setattr(chatgpt_run.time, "time", lambda: expiry + 1)
    resumed = chatgpt_run.RunBudget(run_profile)
    resumed.settle(ticket, {"input_tokens": 10, "output_tokens": 5})
    assert resumed.snapshot()["output_tokens"] == 5
    with pytest.raises(RuntimeError, match="expired"):
        resumed.reserve("gpt-6-luna", input_bound=100, output_bound=50)


@pytest.mark.parametrize("replacement", [0, -1, True, 1.5, None, float("inf")])
def test_invalid_run_caps_fail_before_creating_a_ledger(run_profile, replacement):
    from lumibot.components.agents.chatgpt_run import RunBudget
    data = auth.read_private(run_profile)
    data["limits"]["calls"] = replacement
    auth.save_private(run_profile, data)
    with pytest.raises(ValueError):
        RunBudget(run_profile)
    assert not run_profile.with_suffix(".usage.jsonl").exists()


def test_api_key_and_other_model_cannot_be_mistaken_for_plan_access(run_profile):
    from lumibot.components.agents.chatgpt_run import RunBudget
    data = auth.read_private(run_profile)
    data["access_token"] = "sk-synthetic-must-not-be-used"
    auth.save_private(run_profile, data)
    with pytest.raises(ValueError, match="API key"):
        auth.access_token(run_profile)
    data["access_token"] = "synthetic-run-token"
    auth.save_private(run_profile, data)
    budget = RunBudget(run_profile)
    with pytest.raises(ValueError, match="model"):
        budget.reserve("different-model", input_bound=10, output_bound=10)
    assert budget.snapshot()["calls"] == 0


def test_unknown_requests_hold_capacity_across_resume(run_profile):
    from lumibot.components.agents.chatgpt_run import RunBudget
    budget = RunBudget(run_profile)
    first = budget.reserve("gpt-6-luna", input_bound=100, output_bound=50)
    resumed = RunBudget(run_profile)
    resumed.reserve("gpt-6-luna", input_bound=100, output_bound=50)
    with pytest.raises(RuntimeError, match="call limit"):
        resumed.reserve("gpt-6-luna", input_bound=100, output_bound=50)
    assert resumed.snapshot()["reserved_input_tokens"] == 200
    resumed.settle(first, {"input_tokens": 20, "output_tokens": 10,
                          "input_tokens_details": {"cached_tokens": 5},
                          "output_tokens_details": {"reasoning_tokens": 7}})
    state = resumed.snapshot()
    assert state["input_tokens"] == 20
    assert state["output_tokens"] == 10  # Responses output already includes reasoning.
    assert state["cached_input_tokens"] == 5
    assert state["reserved_input_tokens"] == 100
    assert "synthetic-run-token" not in resumed.path.read_text()
    assert resumed.path.stat().st_mode & 0o077 == 0


@pytest.mark.parametrize("field", ["input_tokens", "output_tokens"])
def test_token_limits_are_checked_before_reserving(run_profile, field):
    from lumibot.components.agents.chatgpt_run import RunBudget
    data = auth.read_private(run_profile)
    data["limits"][field] = 9
    auth.save_private(run_profile, data)
    budget = RunBudget(run_profile)
    with pytest.raises(RuntimeError, match="token limit"):
        budget.reserve("gpt-6-luna", input_bound=10, output_bound=10)
    assert budget.snapshot()["calls"] == 0


def test_parallel_requests_share_one_cap_and_policy_cannot_reset(run_profile):
    from lumibot.components.agents.chatgpt_run import RunBudget
    def reserve(_):
        try:
            return RunBudget(run_profile).reserve("gpt-6-luna", input_bound=100, output_bound=50)
        except RuntimeError:
            return None
    with ThreadPoolExecutor(max_workers=4) as pool:
        assert sum(x is not None for x in pool.map(reserve, range(8))) == 2
    data = auth.read_private(run_profile)
    data["limits"]["calls"] = 3
    auth.save_private(run_profile, data)
    with pytest.raises(ValueError, match="contract"):
        RunBudget(run_profile)


@pytest.mark.parametrize("usage", [{}, {"input_tokens": -1, "output_tokens": 0},
    {"input_tokens": 101, "output_tokens": 1},
    {"input_tokens": 10, "output_tokens": 1, "input_tokens_details": {"cached_tokens": 11}}])
def test_invalid_usage_keeps_the_full_reservation(run_profile, usage):
    from lumibot.components.agents.chatgpt_run import RunBudget
    budget = RunBudget(run_profile)
    ticket = budget.reserve("gpt-6-luna", input_bound=100, output_bound=50)
    with pytest.raises(ValueError):
        budget.settle(ticket, usage)
    assert budget.snapshot()["reserved_input_tokens"] == 100


@pytest.mark.parametrize("completed", [True, False])
def test_actual_adapter_counts_http_requests_and_never_rotates_run_credentials(run_profile, monkeypatch, completed):
    from google.adk.models.llm_request import LlmRequest
    from google.genai import types
    from lumibot.components.agents import chatgpt_llm
    from lumibot.components.agents.chatgpt_run import RunBudget
    from lumibot.components.agents.runtime import _configure_litellm_quietly, _register_openai_gpt6_models
    _configure_litellm_quietly()
    _register_openai_gpt6_models()
    monkeypatch.setattr(chatgpt_llm, "next_connected_profile", lambda *a: pytest.fail("No cross-account fallback for a run"))
    calls = []
    def handle(request):
        body = json.loads(request.content)
        calls.append(body)
        assert request.headers["authorization"] == "Bearer synthetic-run-token"
        assert body["service_tier"] == "default"
        assert body["store"] is False and body["stream"] is True
        assert "max_output_tokens" not in body
        response = {"id": "resp_test", "object": "response", "model": "gpt-6-luna", "status": "completed",
            "output": [{"id": "msg_test", "type": "message", "role": "assistant", "status": "completed",
                "content": [{"type": "output_text", "text": "ok", "annotations": []}]}],
            "usage": {"input_tokens": 5, "output_tokens": 2, "total_tokens": 7}}
        events = [{"type": "response.output_text.delta", "delta": "ok", "item_id": "msg_test", "output_index": 0, "content_index": 0}]
        if completed:
            events.append({"type": "response.completed", "response": response})
        wire = "".join("data: " + json.dumps(event) + "\n\n" for event in events).encode()
        class Stream(httpx.AsyncByteStream):
            async def __aiter__(self):
                yield wire
        return httpx.Response(200, stream=Stream())
    model = chatgpt_llm.ChatGPTPlanLlm(model="openai/responses/gpt-6-luna", profile=run_profile,
        transport=httpx.MockTransport(handle))
    request = LlmRequest(contents=[types.Content(role="user", parts=[types.Part(text="say ok")])],
                         config=types.GenerateContentConfig(max_output_tokens=1))
    async def consume():
        return [r async for r in model.generate_content_async(request)]
    for _ in range(2):
        if completed:
            assert len(asyncio.run(consume())) == 1
        else:
            with pytest.raises(chatgpt_llm.SubscriptionModelResponseError):
                asyncio.run(consume())
    with pytest.raises(RuntimeError, match="call limit"):
        asyncio.run(consume())
    assert len(calls) == 2
    state = RunBudget(run_profile).snapshot()
    assert state["calls"] == 2
    assert state["output_tokens"] == (4 if completed else 0)
    assert state["reserved_output_tokens"] == (0 if completed else 256_000)


def test_run_quota_error_does_not_switch_accounts_or_use_api_keys(run_profile, monkeypatch):
    from google.adk.models.llm_request import LlmRequest
    from google.genai import types
    from lumibot.components.agents import chatgpt_llm
    from lumibot.components.agents.chatgpt_run import RunBudget
    from lumibot.components.agents.runtime import _configure_litellm_quietly, _register_openai_gpt6_models
    _configure_litellm_quietly()
    _register_openai_gpt6_models()
    monkeypatch.setenv("OPENAI_API_KEY", "sk-synthetic-never-used")
    monkeypatch.setattr(chatgpt_llm, "next_connected_profile", lambda *a: pytest.fail("No fallback"))
    monkeypatch.setattr(chatgpt_llm, "mark_exhausted", lambda *a: pytest.fail("Run must not mutate the shared account pool"))
    calls = []
    def handle(request):
        calls.append(request.url)
        assert request.headers["authorization"] == "Bearer synthetic-run-token"
        return httpx.Response(429, json={"error": {"code": "subscription_sharing_usage_limit_exceeded"}})
    model = chatgpt_llm.ChatGPTPlanLlm(model="openai/responses/gpt-6-luna", profile=run_profile,
        transport=httpx.MockTransport(handle))
    async def consume():
        return [r async for r in model.generate_content_async(LlmRequest(
            contents=[types.Content(role="user", parts=[types.Part(text="test")])]))]
    with pytest.raises(chatgpt_llm.SubscriptionModelResponseError):
        asyncio.run(consume())
    assert len(calls) == 1
    assert RunBudget(run_profile).snapshot()["reserved_output_tokens"] == 128_000


def test_missing_catalog_input_limit_cannot_use_an_output_limit_as_input(run_profile, monkeypatch):
    from google.adk.models.llm_request import LlmRequest
    from google.genai import types
    import litellm
    from lumibot.components.agents import chatgpt_llm
    from lumibot.components.agents.chatgpt_run import RunBudget
    from lumibot.components.agents.runtime import _configure_litellm_quietly, _register_openai_gpt6_models
    _configure_litellm_quietly()
    _register_openai_gpt6_models()
    original = litellm.get_model_info
    def model_info(model, *args, **kwargs):
        info = dict(original(model, *args, **kwargs))
        info.pop("max_input_tokens", None)
        return info
    monkeypatch.setattr(litellm, "get_model_info", model_info)
    calls = []
    def handle(request):
        calls.append(request.url)
        return httpx.Response(429, json={"error": {"code": "subscription_sharing_usage_limit_exceeded"}})
    model = chatgpt_llm.ChatGPTPlanLlm(model="openai/responses/gpt-6-luna", profile=run_profile,
        transport=httpx.MockTransport(handle))
    async def consume():
        return [r async for r in model.generate_content_async(LlmRequest(
            contents=[types.Content(role="user", parts=[types.Part(text="test")])]))]
    with pytest.raises(ValueError, match="known provider token bounds"):
        asyncio.run(consume())
    assert not calls
    assert RunBudget(run_profile).snapshot()["calls"] == 0
