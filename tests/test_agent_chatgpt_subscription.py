"""Subscription auth and transport contracts; no model or credential network calls."""
import json
import time

import pytest

from lumibot.components.agents import chatgpt_auth as auth


def test_subscription_model_limits_do_not_enter_litellm_device_auth(monkeypatch):
    from lumibot.components.agents import runtime
    import litellm

    requested = []
    def model_info(name, **kwargs):
        requested.append(name)
        return dict(runtime._OPENAI_GPT6_MODEL_INFO["gpt-6-luna"])

    monkeypatch.setattr(litellm, "get_model_info", model_info)
    monkeypatch.setattr(runtime, "_MODEL_OUTPUT_LIMIT_CACHE", {})
    expected = runtime._OPENAI_GPT6_MODEL_INFO["gpt-6-luna"]["max_output_tokens"]
    assert runtime._model_max_output_tokens("chatgpt/gpt-6-luna") == expected
    assert runtime._effective_max_output_tokens("chatgpt/gpt-6-luna", None) is None
    assert not requested, "metadata must not start LiteLLM's separate ChatGPT authentication"


@pytest.mark.parametrize("model,lookup", [("chatgpt/synthetic-model", "openai/synthetic-model"),
                                         ("openai/synthetic-model", "openai/synthetic-model"),
                                         ("anthropic/synthetic-model", "anthropic/synthetic-model")])
def test_only_subscription_metadata_namespace_is_normalized(monkeypatch, model, lookup):
    from lumibot.components.agents import runtime
    import litellm

    requested = []
    def model_info(name, **kwargs):
        requested.append(name)
        return dict(runtime._OPENAI_GPT6_MODEL_INFO["gpt-6-luna"])

    monkeypatch.setattr(litellm, "get_model_info", model_info)
    monkeypatch.setattr(runtime, "_MODEL_OUTPUT_LIMIT_CACHE", {})
    assert runtime._model_max_output_tokens(model) == runtime._OPENAI_GPT6_MODEL_INFO["gpt-6-luna"]["max_output_tokens"]
    assert requested == [lookup]


def test_callback_accepts_returning_client_and_rejects_account_switch():
    assert auth.validate_callback({"state": ["state"], "code": ["code"]}, "state", "issued") == ("code", "issued")
    with pytest.raises(ValueError, match="client"):
        auth.validate_callback({"state": ["state"], "code": ["code"], "client_id": ["other"]}, "state", "issued")
    with pytest.raises(ValueError, match="state"):
        auth.validate_callback({"state": ["bad"], "code": ["code"]}, "state", "issued")


def test_dynamic_callback_requires_issued_registration():
    for value in ({}, {"client_id": ["dynamic_agent_client"]}, {"client_id": ["a", "b"]}):
        with pytest.raises(ValueError):
            auth.validate_callback({"state": ["state"], "code": ["code"], **value}, "state")


def test_private_profile_requires_permission_without_api_fallback(tmp_path, monkeypatch):
    path = tmp_path / "profile.json"
    auth.save_private(path, {"status": "connected", "scopes": [], "access_token": "synthetic"})
    monkeypatch.setenv("OPENAI_API_KEY", "must-never-be-used")
    with pytest.raises(RuntimeError, match="permission"):
        auth.access_token(path)
    assert path.stat().st_mode & 0o077 == 0
    path.chmod(0o644)
    with pytest.raises(RuntimeError, match="owner-only"):
        auth.access_token(path)


def test_profile_pin_is_not_changed_by_active_selection(tmp_path, monkeypatch):
    from uuid import uuid4
    first, second = str(uuid4()), str(uuid4())
    monkeypatch.setenv("LUMIBOT_CHATGPT_HOME", str(tmp_path))
    monkeypatch.delenv("LUMIBOT_CHATGPT_PROFILE", raising=False)
    monkeypatch.delenv("LUMIBOT_CHATGPT_PROFILE_PATH", raising=False)
    auth.save_private(tmp_path / "active.json", {"profile_id": first})
    assert auth.profile_path().name == first + ".json"
    auth.save_private(tmp_path / "active.json", {"profile_id": second})
    assert auth.profile_path().name == first + ".json"


def test_model_catalog_uses_subscription_shape_and_visibility(tmp_path, monkeypatch):
    path = tmp_path / "profile.json"
    auth.save_private(path, {"status": "connected", "scopes": ["chatgpt.tokens.use.direct"],
                             "access_token": "synthetic", "saved_at": time.time(), "expires_in": 3600})
    import httpx
    def get(url, **kwargs):
        assert url == "https://api.openai.com/v1/models"
        assert kwargs["headers"]["Authorization"] == "Bearer synthetic"
        return httpx.Response(200, request=httpx.Request("GET", url), json={"models": [
            {"slug": "gpt-visible", "display_name": "Visible", "visibility": "list"},
            {"slug": "gpt-hidden", "visibility": "hidden"}]})
    monkeypatch.setattr(auth.httpx, "get", get)
    assert auth.available_models(path) == [{"slug": "gpt-visible", "display_name": "Visible", "visibility": "list"}]


def test_eval_preflight_rejects_paid_models_even_with_keys(monkeypatch):
    from scripts import run_agent_evals as evals
    monkeypatch.setenv("OPENAI_API_KEY", "must-never-be-used")
    with pytest.raises(RuntimeError, match="subscription"):
        evals.select_eval_credentials({"openai/gpt-6-luna"})
    assert evals.DEFAULT_ACTING_MODEL.startswith("chatgpt/")
    assert evals.DEFAULT_JUDGE_MODEL.startswith("chatgpt/")


def test_subscription_reporting_preserves_calls_across_resume(tmp_path):
    from scripts.agent_eval_subscription_budget import SubscriptionCallBudget
    path = tmp_path / "calls.jsonl"
    budget = SubscriptionCallBudget(path, max_calls=1)
    ticket = budget.for_scope("case", 1, "acting").reserve("chatgpt/gpt-6-luna", 100)
    budget.settle(ticket, {"input_tokens": 5, "output_tokens": 2})
    resumed = SubscriptionCallBudget(path, max_calls=1)
    resumed.reserve("chatgpt/gpt-6-luna", 100)
    assert resumed.snapshot()["committed_usd"] == 0
    assert resumed.snapshot()["usage"]["input_tokens"] == 5


def test_subscription_usage_windows_never_block_a_continuing_qualification(tmp_path):
    from scripts.agent_eval_subscription_budget import SubscriptionCallBudget

    path = tmp_path / "reporting.jsonl"
    budget = SubscriptionCallBudget(path, max_calls=1)
    original_header = path.read_text().splitlines()[0]
    for _ in range(10):
        budget.reserve("chatgpt/gpt-6-luna", 100)
    resumed = SubscriptionCallBudget(path, max_calls=1)
    snapshot = resumed.snapshot()
    assert snapshot["reserved_calls"] == 10
    assert snapshot["reserved_output_tokens"] == 1_280_000
    assert snapshot["accounting_mode"] == "reporting"
    assert snapshot["api_spend_usd"] == 0
    assert path.read_text().splitlines()[0] == original_header


def test_explicitly_disabled_request_deadline_reaches_subscription_transport():
    from lumibot.components.agents.runtime import _resolve_model_for_adk

    model = _resolve_model_for_adk("chatgpt/gpt-6-luna", model_request_timeout_seconds=0)
    assert model._kwargs.get("timeout", "missing") is None


def test_subscription_qualification_call_ceiling_is_shared_and_explicit(tmp_path):
    from scripts.agent_eval_subscription_budget import (
        MAX_SUBSCRIPTION_QUALIFICATION_CALLS,
        SubscriptionCallBudget,
    )

    # Historical window size is informational; no request is refused at this count.
    assert MAX_SUBSCRIPTION_QUALIFICATION_CALLS == 5000
    default = SubscriptionCallBudget(tmp_path / "default.jsonl")
    planned = SubscriptionCallBudget(tmp_path / "planned.jsonl", max_calls=5000)
    assert default.header["max_calls"] == 500
    assert planned.header["max_calls"] == 5000
    assert planned.snapshot()["funding"] == "chatgpt-plan"
    assert planned.snapshot()["api_spend_usd"] == 0


@pytest.mark.parametrize("invalid", [0, -1, True, False, 1.5, "2500", None, float("inf"), float("nan")])
def test_subscription_qualification_rejects_invalid_or_excessive_caps_before_writing(tmp_path, invalid):
    from scripts.agent_eval_subscription_budget import SubscriptionCallBudget

    path = tmp_path / "not-created" / "calls.jsonl"
    with pytest.raises(ValueError):
        SubscriptionCallBudget(path, max_calls=invalid)
    assert not path.parent.exists()


def test_subscription_new_attempt_cannot_resize_or_rewrite_an_existing_500_call_ledger(tmp_path):
    from scripts.agent_eval_subscription_budget import SubscriptionCallBudget

    path = tmp_path / "original.jsonl"
    original = SubscriptionCallBudget(path)
    settled = original.reserve("chatgpt/gpt-6-luna", 100)
    original.settle(settled, {"input_tokens": 5, "output_tokens": 2})
    original.reserve("chatgpt/gpt-6-luna", 100)
    before = path.read_bytes()
    counts = original.snapshot()
    SubscriptionCallBudget(path, max_calls=1500)
    assert path.read_bytes() == before
    resumed = SubscriptionCallBudget(path)
    assert resumed.snapshot() == counts
    assert counts["settled_calls"] == 1
    assert counts["reserved_calls"] == 1
    new_attempt = SubscriptionCallBudget(tmp_path / "new-attempt.jsonl", max_calls=1500)
    assert new_attempt.snapshot()["settled_calls"] == 0
    assert new_attempt.snapshot()["reserved_calls"] == 0
    assert path.read_bytes() == before


def test_subscription_remaining_call_guard_at_the_largest_explicit_cap(tmp_path):
    from scripts.agent_eval_subscription_budget import SubscriptionCallBudget, SubscriptionCallLimitError

    path = tmp_path / "planned.jsonl"
    budget = SubscriptionCallBudget(path, max_calls=1500)
    # Synthetic ledger rows exercise the machine boundary without model calls.
    reservations = [{"kind": "reserve", "id": f"synthetic-{index}",
                     "model": "chatgpt/gpt-6-luna", "max_output_tokens": 100,
                     "nano_usd": 0, "reserved_nano_usd": 0, "scope": None}
                    for index in range(1499)]
    completed = []
    for row in reservations:
        completed.extend([row, {"kind": "settle", "id": row["id"], "nano_usd": 0,
                               "usage": {"input_tokens": 1, "cached_input_tokens": 0,
                                         "output_tokens": 1, "thinking_tokens": 0}}])
    path.write_text("".join(json.dumps(row) + "\n" for row in [budget.header, *completed]), encoding="utf-8")
    budget.reserve("chatgpt/gpt-6-luna", 100)
    assert budget.snapshot()["settled_calls"] == 1499
    assert budget.snapshot()["reserved_calls"] == 1
    exhausted = path.read_bytes()
    SubscriptionCallBudget(path, max_calls=1500).reserve("chatgpt/gpt-6-luna", 100)
    assert path.read_bytes().startswith(exhausted)
    assert SubscriptionCallBudget(path).snapshot()["reserved_calls"] == 2


def test_subscription_reservations_are_atomic_across_concurrent_resumed_instances(tmp_path):
    from concurrent.futures import ThreadPoolExecutor

    from scripts.agent_eval_subscription_budget import SubscriptionCallBudget, SubscriptionCallLimitError

    path = tmp_path / "concurrent.jsonl"
    SubscriptionCallBudget(path, max_calls=3)

    def reserve():
        try:
            return SubscriptionCallBudget(path, max_calls=3).reserve("chatgpt/gpt-6-luna", 100)
        except SubscriptionCallLimitError:
            return None

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: reserve(), range(8)))
    tickets = [ticket for ticket in results if ticket is not None]
    assert len(tickets) == len(set(tickets)) == 8
    assert SubscriptionCallBudget(path, max_calls=3).snapshot()["reserved_calls"] == 8


def test_subscription_unknown_usage_and_unscoped_reservations_still_consume_the_allowance(tmp_path):
    from scripts.agent_eval_subscription_budget import SubscriptionCallBudget, SubscriptionCallLimitError

    path = tmp_path / "unknown.jsonl"
    budget = SubscriptionCallBudget(path, max_calls=2)
    first = budget.reserve("chatgpt/gpt-6-luna", 100)
    with pytest.raises(ValueError, match="authoritative usage"):
        budget.settle(first, {})
    budget.for_scope("case", 1, "acting").reserve("chatgpt/gpt-6-luna", 100)
    before = path.read_bytes()
    resumed = SubscriptionCallBudget(path, max_calls=2)
    assert resumed.snapshot()["reserved_calls"] == 2
    assert resumed.snapshot()["settled_calls"] == 0
    resumed.reserve("chatgpt/gpt-6-luna", 100)
    assert path.read_bytes().startswith(before)
    assert resumed.snapshot()["reserved_calls"] == 3


@pytest.mark.parametrize("model", ["openai/gpt-6-luna", "google/gemini-pro", "gpt-6-luna"])
def test_subscription_larger_call_plan_cannot_enable_api_routes(tmp_path, model):
    from scripts.agent_eval_subscription_budget import SubscriptionCallBudget

    budget = SubscriptionCallBudget(tmp_path / "funding.jsonl", max_calls=1500)
    before = budget.path.read_bytes()
    with pytest.raises(ValueError, match="API fallback is forbidden"):
        budget.reserve(model, 100)
    assert budget.path.read_bytes() == before
    assert budget.snapshot()["reserved_calls"] == 0


@pytest.mark.parametrize("event", ["error", "response.failed", "response.incomplete"])
def test_stream_rejects_provider_quota_without_logging_provider_body(event):
    import asyncio

    import httpx

    from lumibot.components.agents.chatgpt_llm import CompletionCheckedStream, SubscriptionModelResponseError
    from lumibot.components.agents.runtime import _classify_agent_error
    class Stream(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield ("data: " + json.dumps({"type": event, "error": {
                "code": "subscription_sharing_usage_limit_exceeded", "message": "private material"}}) + "\n\n").encode()
    async def consume():
        return [chunk async for chunk in CompletionCheckedStream(Stream())]
    with pytest.raises(SubscriptionModelResponseError) as captured:
        asyncio.run(consume())
    assert _classify_agent_error(captured.value) == "billing"
    assert "private material" not in str(captured.value)


@pytest.mark.parametrize("complete", [True, False])
def test_stream_requires_completed_terminal_event_across_chunk_boundaries(complete):
    import asyncio

    import httpx

    from lumibot.components.agents.chatgpt_llm import CompletionCheckedStream
    content = b'data: {"type":"response.output_text.delta","delta":"partial"}\n\n'
    if complete:
        content += b'data: {"type":"response.completed","response":{"status":"completed"}}\n\n'
    class Stream(httpx.AsyncByteStream):
        async def __aiter__(self):
            for offset in range(0, len(content), 7):
                yield content[offset:offset+7]
    async def consume():
        return b"".join([chunk async for chunk in CompletionCheckedStream(Stream())])
    if complete:
        assert asyncio.run(consume()) == content
    else:
        with pytest.raises(RuntimeError, match="subscription_stream_incomplete"):
            asyncio.run(consume())


def test_subscription_budget_config_does_not_require_api_pricing():
    from types import SimpleNamespace

    from google.genai import types

    from lumibot.components.agents.runtime import GoogleADKRuntime
    request = SimpleNamespace(model="chatgpt/gpt-6-luna", max_output_tokens=100,
                              model_request_timeout_seconds=30, model_call_budget=object())
    assert GoogleADKRuntime._generate_content_config_kwargs_for_request(request, types) == {"max_output_tokens": 100}


def test_refresh_serializes_and_persists_rotating_tokens(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor

    import httpx
    path = tmp_path / "profile.json"
    auth.save_private(path, {"status":"connected", "scopes":[auth.PLAN_SCOPE], "client_id":"issued",
        "access_token":"old", "refresh_token":"old-refresh", "saved_at":0, "expires_in":1})
    calls = []
    def post(url, **kwargs):
        calls.append(kwargs["data"])
        time.sleep(0.05)
        return httpx.Response(200, json={"access_token":"new", "refresh_token":"new-refresh", "expires_in":3600})
    monkeypatch.setattr(auth.httpx, "post", post)
    with ThreadPoolExecutor(max_workers=4) as pool:
        assert list(pool.map(lambda _: auth.access_token(path), range(4))) == ["new"] * 4
    assert len(calls) == 1
    assert json.loads(path.read_text())["refresh_token"] == "new-refresh"


@pytest.mark.parametrize('failure', ['exchange', 'identity'])
def test_failed_reconnect_preserves_an_existing_connected_profile(tmp_path, monkeypatch, failure):
    """Reconnect failure must not discard an already working renewable grant."""
    import webbrowser
    from threading import Thread
    from types import SimpleNamespace
    from urllib.parse import parse_qs, urlencode, urlsplit
    from urllib.request import urlopen
    from uuid import uuid4

    import httpx
    import jwt
    profile_id = str(uuid4())
    path = tmp_path/'profiles'/(profile_id+'.json')
    previous = {'status': 'connected', 'subject': 'original-subject', 'client_id': 'issued',
                'scopes': [auth.PLAN_SCOPE], 'access_token': 'working', 'refresh_token': 'working-refresh',
                'saved_at': time.time(), 'expires_in': 3600}
    auth.save_private(path, previous)
    def handle(request):
        if request.method == 'POST':
            if failure == 'exchange':
                return httpx.Response(500)
            return httpx.Response(200, json={'access_token': 'new', 'refresh_token': 'new-refresh',
                'expires_in': 3600, 'scope': auth.PLAN_SCOPE, 'id_token': 'synthetic'})
        return httpx.Response(200, json={'jwks_uri': auth.AUTH+'/jwks'})
    original_client = auth.httpx.Client
    monkeypatch.setattr(auth.httpx, 'Client', lambda **kwargs: original_client(transport=httpx.MockTransport(handle), **kwargs))
    monkeypatch.setattr(jwt, 'PyJWKClient', lambda _: SimpleNamespace(get_signing_key_from_jwt=lambda _: SimpleNamespace(key='synthetic')))
    pending, callback_threads = {}, []
    def browser(url):
        params = parse_qs(urlsplit(url).query)
        pending['nonce'] = params['nonce'][0]
        target = params['redirect_uri'][0] + '?' + urlencode({
            'code': 'synthetic', 'state': params['state'][0], 'client_id': 'issued'})
        def callback():
            with urlopen(target, timeout=5) as response:
                response.read()
        thread = Thread(target=callback)
        callback_threads.append(thread)
        thread.start()
        return True
    monkeypatch.setattr(webbrowser, 'open', browser)
    monkeypatch.setattr(jwt, 'decode', lambda *args, **kwargs: {'sub': 'different-subject', 'nonce': pending['nonce']})
    try:
        with pytest.raises((RuntimeError, ValueError)):
            auth.login(root=tmp_path, profile_id=profile_id, timeout_seconds=10)
    finally:
        for thread in callback_threads:
            thread.join(timeout=5)
            assert not thread.is_alive()
    assert auth.read_private(path) == previous
    assert auth.access_token(path) == 'working'


def test_logout_clears_tokens_even_when_revocation_cannot_be_confirmed(tmp_path, monkeypatch):
    import httpx
    path = tmp_path / "profile.json"
    auth.save_private(path, {"client_id":"issued", "subject":"subject", "status":"connected", "refresh_token":"secret"})
    def handle(request):
        if request.method == "GET":
            return httpx.Response(200, json={"revocation_endpoint":auth.AUTH+"/oauth/revoke"})
        return httpx.Response(503)
    original = auth.httpx.Client
    monkeypatch.setattr(auth.httpx, "Client", lambda **kwargs: original(transport=httpx.MockTransport(handle), **kwargs))
    assert auth.logout(path) is False
    record = json.loads(path.read_text())
    assert record == {"client_id":"issued", "subject":"subject", "status":"disconnected"}


def test_response_replay_rejects_conflicting_provider_call_ids_and_keeps_pruned_outputs():
    from lumibot.components.agents.chatgpt_llm import SubscriptionModelResponseError, _ResponsesReplay
    replay = _ResponsesReplay()
    original = [{'type': 'reasoning', 'id': 'rs_original', 'encrypted_content': 'opaque', 'summary': []},
                {'type': 'function_call', 'id': 'fc_original', 'call_id': 'call_unique', 'name': 'get_evidence', 'arguments': '{}'}]
    replay.remember(original)
    current = [{'type': 'message', 'role': 'user', 'content': 'question'}, original[1],
               {'type': 'function_call_output', 'call_id': 'call_unique', 'output': '[older payload pruned]'}]
    assert replay.restore(current) == [current[0], *original, current[2]]
    with pytest.raises(SubscriptionModelResponseError, match='subscription_history_conflict'):
        replay.remember([{**original[1], 'arguments': '{"different":true}'}])
    assert replay.restore(current) == [current[0], *original, current[2]]
    assert _ResponsesReplay().restore(current) == current
    assert replay.restore([current[0]]) == [current[0]]
    assert not replay.by_call


@pytest.mark.parametrize("completed", [True, False])
@pytest.mark.parametrize("content_type", ["text/event-stream", None])
def test_actual_adk_subscription_transport_owns_stream_validation(monkeypatch, tmp_path, completed, content_type):
    """Run the actual ADK/LiteLLM conversion against a recorded HTTP contract."""
    import asyncio

    import httpx
    from google.adk.models.llm_request import LlmRequest
    from google.genai import types

    from lumibot.components.agents import chatgpt_llm
    from lumibot.components.agents.runtime import _configure_litellm_quietly, _register_openai_gpt6_models
    _configure_litellm_quietly()
    _register_openai_gpt6_models()
    monkeypatch.setattr(chatgpt_llm, "access_token", lambda path: "synthetic-plan-token")
    requests = []
    def handle(request):
        requests.append(json.loads(request.content))
        assert str(request.url) == "https://api.openai.com/v1/responses"
        assert request.headers["authorization"] == "Bearer synthetic-plan-token"
        terminal = {"id":"resp_contract", "object":"response", "model":"gpt-6-luna", "status":"completed",
            "output":[{"id":"msg_contract", "type":"message", "role":"assistant", "status":"completed",
                "content":[{"type":"output_text", "text":"ok", "annotations":[]}]}],
            "usage":{"input_tokens":5, "output_tokens":2, "total_tokens":7}}
        events = [{"type":"response.output_text.delta", "delta":"ok", "item_id":"msg_contract", "output_index":0, "content_index":0}]
        if completed:
            events.append({"type":"response.completed", "response":terminal})
        data = "".join("data: "+json.dumps(event)+"\n\n" for event in events)
        class Stream(httpx.AsyncByteStream):
            async def __aiter__(self):
                yield data.encode()
        return httpx.Response(200, headers={"content-type": content_type} if content_type else {}, stream=Stream())
    original_send = httpx.AsyncClient.send
    async def synthetic_only(client, request, **kwargs):
        assert isinstance(client._transport, httpx.MockTransport), "The owned subscription HTTP handler was bypassed"
        return await original_send(client, request, **kwargs)
    monkeypatch.setattr(httpx.AsyncClient, "send", synthetic_only)
    model = chatgpt_llm.ChatGPTPlanLlm(model="openai/responses/gpt-6-luna", profile=tmp_path/"profile.json",
                                     transport=httpx.MockTransport(handle))
    request = LlmRequest(contents=[types.Content(role="user", parts=[types.Part(text="say ok")])],
                         config=types.GenerateContentConfig(temperature=0.2, max_output_tokens=100))
    async def consume():
        return [response async for response in model.generate_content_async(request)]
    if completed:
        responses = asyncio.run(consume())
        assert len(responses) == 1
        assert responses[0].usage_metadata.prompt_token_count == 5
    else:
        with pytest.raises(chatgpt_llm.SubscriptionModelResponseError, match="subscription_stream_incomplete"):
            asyncio.run(consume())
    assert len(requests) == 1
    assert requests[0]["stream"] is True
    assert requests[0]["store"] is False
    assert "temperature" not in requests[0]
    assert "max_output_tokens" not in requests[0]
    assert request.config.temperature == 0.2


@pytest.mark.parametrize('runner_mode', [False, True])
@pytest.mark.parametrize("content_type", ["text/event-stream", None])
@pytest.mark.parametrize('argument_mode', ['delta', 'done_only'])
def test_stateless_tool_continuation_replays_opaque_reasoning_and_function_items(monkeypatch, tmp_path, runner_mode, content_type, argument_mode):
    """ADK drops Responses reasoning metadata; assert the actual second wire body."""
    import asyncio

    import httpx
    from google.adk.models.llm_request import LlmRequest
    from google.genai import types

    from lumibot.components.agents import chatgpt_llm
    from lumibot.components.agents.runtime import _configure_litellm_quietly, _register_openai_gpt6_models
    _configure_litellm_quietly()
    _register_openai_gpt6_models()
    monkeypatch.setattr(chatgpt_llm, 'access_token', lambda path: 'synthetic-plan-token')
    reasoning = {'id': 'rs_contract', 'type': 'reasoning', 'summary': [], 'encrypted_content': 'opaque-contract'}
    call = {'id': 'fc_contract', 'type': 'function_call', 'call_id': 'call_contract',
            'name': 'get_evidence', 'arguments': '{"series":"CPI"}', 'status': 'completed'}
    commentary = {'id': 'msg_commentary', 'type': 'message', 'role': 'assistant', 'phase': 'commentary',
                  'status': 'completed', 'content': [{'type': 'output_text', 'text': 'Checking evidence.', 'annotations': []}]}
    parallel_call = {**call, 'id': 'fc_parallel', 'call_id': 'call_parallel', 'arguments': '{"series":"RATE"}'}
    original_output = [reasoning, commentary, call, parallel_call] if runner_mode else [reasoning, call]
    requests = []
    observed = []
    def handle(request):
        body = json.loads(request.content)
        requests.append(body)
        output = original_output if len(requests) == 1 else [
            {'id': 'msg_final', 'type': 'message', 'role': 'assistant', 'status': 'completed',
             'content': [{'type': 'output_text', 'text': 'done', 'annotations': []}]}]
        terminal = {'id': 'resp_contract', 'object': 'response', 'model': 'gpt-6-luna',
                    'status': 'completed', 'output': output,
                    'usage': {'input_tokens': 5, 'output_tokens': 2, 'total_tokens': 7,
                              'input_tokens_details': {'cached_tokens': 3},
                              'output_tokens_details': {'reasoning_tokens': 1}}}
        class Stream(httpx.AsyncByteStream):
            async def __aiter__(self):
                events = []
                if len(requests) == 1:
                    if runner_mode:
                        events.append({'type': 'response.output_text.delta', 'delta': 'Checking evidence.',
                                       'item_id': 'msg_commentary', 'output_index': 1, 'content_index': 0})
                    for index, tool_call in enumerate(original_output):
                        if tool_call['type'] != 'function_call':
                            continue
                        events.extend([
                            {'type': 'response.output_item.added', 'output_index': index,
                             'item': {**tool_call, 'arguments': '', 'status': 'in_progress'}},
                            {'type': 'response.function_call_arguments.' + ('delta' if argument_mode == 'delta' else 'done'),
                             'item_id': tool_call['id'], 'output_index': index,
                             ('delta' if argument_mode == 'delta' else 'arguments'): tool_call['arguments']},
                            {'type': 'response.function_call_arguments.done', 'item_id': tool_call['id'],
                             'output_index': index, 'arguments': tool_call['arguments']},
                            {'type': 'response.output_item.done', 'output_index': index, 'item': tool_call},
                        ])
                else:
                    events.append({'type': 'response.output_text.delta', 'delta': 'done',
                                   'item_id': 'msg_final', 'output_index': 0, 'content_index': 0})
                events.append({'type': 'response.completed', 'response': terminal})
                yield ''.join('data: ' + json.dumps(event) + '\n\n' for event in events).encode()
        return httpx.Response(200, headers={'content-type': content_type} if content_type else {}, stream=Stream())
    model = chatgpt_llm.ChatGPTPlanLlm(model='openai/responses/gpt-6-luna', profile=tmp_path/'profile.json',
                                     transport=httpx.MockTransport(handle))
    request = LlmRequest(contents=[types.Content(role='user', parts=[types.Part(text='get CPI evidence')])],
                         config=types.GenerateContentConfig())
    async def run_runner():
        from google.adk.agents import LlmAgent
        from google.adk.runners import InMemoryRunner
        def get_evidence(series: str) -> dict:
            """Return recorded economic evidence for the requested series."""
            observed.append(series)
            return {'value': 2.5 if series == 'CPI' else 4.0}
        runner = InMemoryRunner(agent=LlmAgent(name='researcher', model=model, tools=[get_evidence]), app_name='replay-contract')
        await runner.session_service.create_session(app_name=runner.app_name, user_id='synthetic', session_id='test')
        events = [event async for event in runner.run_async(user_id='synthetic', session_id='test', new_message=request.contents[0])]
        assert any(event.content and any(part.text == 'done' for part in event.content.parts) for event in events)
        assert 'opaque-contract' not in json.dumps([event.model_dump(mode='json') for event in events])
    async def run():
        first = [r async for r in model.generate_content_async(request)]
        assert first[-1].usage_metadata.cached_content_token_count == 3
        assert first[-1].usage_metadata.thoughts_token_count == 1
        function = next(p.function_call for p in first[-1].content.parts if p.function_call)
        assert function.args == {'series': 'CPI'}
        request.contents.extend([first[-1].content, types.Content(role='user', parts=[
            types.Part(function_response=types.FunctionResponse(id=function.id, name=function.name,
                                                                response={'value': 2.5}))])])
        return [r async for r in model.generate_content_async(request)]
    if runner_mode:
        asyncio.run(run_runner())
        assert observed == ['CPI', 'RATE']
    else:
        assert asyncio.run(run())[-1].content.parts[0].text == 'done'
    # The opaque provider item must not be exposed in ADK public response metadata.
    # Raw output ordering, identifiers and encrypted content must survive store=False.
    second_input = requests[1]['input']
    assert second_input[1:1 + len(original_output)] == original_output
    outputs = second_input[1 + len(original_output):]
    assert all(item['type'] == 'function_call_output' for item in outputs)
    assert {item['call_id'] for item in outputs} == ({'call_contract', 'call_parallel'} if runner_mode else {'call_contract'})
    assert 'opaque-contract' not in json.dumps(model.model_dump(mode='json'))
    assert requests[1]['store'] is False


def test_subscription_allowance_amendment_preserves_every_existing_call(tmp_path):
    from scripts.agent_eval_subscription_budget import SubscriptionCallBudget
    path = tmp_path / "calls.jsonl"
    first = SubscriptionCallBudget(path, max_calls=2)
    ticket = first.reserve("chatgpt/gpt-6-luna", 100)
    first.settle(ticket, {"input_tokens": 5, "output_tokens": 2})
    original = path.read_bytes()
    SubscriptionCallBudget.amend_limit(path, previous_limit=2, max_calls=3, reason="Reviewed additional qualification")
    assert path.read_bytes().startswith(original)
    resumed = SubscriptionCallBudget(path, max_calls=3)
    resumed.reserve("chatgpt/gpt-6-luna", 100)
    resumed.reserve("chatgpt/gpt-6-luna", 100)
    resumed.reserve("chatgpt/gpt-6-luna", 100)
    assert resumed.header["max_calls"] == 2
    assert resumed.snapshot()["max_calls"] == 5
    assert resumed.snapshot()["usage"]["input_tokens"] == 5


@pytest.mark.parametrize("limit,reason", [(1, "review"), (2, "review"), (True, "review"), (3, "")])
def test_subscription_allowance_rejects_invalid_amendments_without_writing(tmp_path, limit, reason):
    from scripts.agent_eval_subscription_budget import SubscriptionCallBudget
    path = tmp_path / "calls.jsonl"
    SubscriptionCallBudget(path, max_calls=2)
    original = path.read_bytes()
    with pytest.raises(ValueError):
        SubscriptionCallBudget.amend_limit(path, previous_limit=2, max_calls=limit, reason=reason)
    assert path.read_bytes() == original


def test_subscription_allowance_amendment_is_single_flight(tmp_path):
    from concurrent.futures import ThreadPoolExecutor

    from scripts.agent_eval_subscription_budget import SubscriptionCallBudget
    path = tmp_path / "calls.jsonl"
    SubscriptionCallBudget(path, max_calls=2)
    def amend(_):
        try:
            SubscriptionCallBudget.amend_limit(path, previous_limit=2, max_calls=3, reason="Scoped extension")
            return True
        except ValueError:
            return False
    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(amend, range(2))) == [False, True]
    assert SubscriptionCallBudget(path, max_calls=3).snapshot()["max_calls"] == 3


def test_subscription_amendment_cannot_create_a_new_ledger(tmp_path):
    from scripts.agent_eval_subscription_budget import SubscriptionCallBudget
    path = tmp_path / "missing.jsonl"
    with pytest.raises(ValueError, match="existing"):
        SubscriptionCallBudget.amend_limit(path, previous_limit=2, max_calls=3, reason="review")
    assert not path.exists()


def test_subscription_legacy_windows_preserve_all_recorded_usage(tmp_path):
    import json
    from uuid import uuid4

    from scripts.agent_eval_subscription_budget import SubscriptionCallBudget
    path = tmp_path / "calls.jsonl"
    budget = SubscriptionCallBudget(path, max_calls=1)
    budget.reserve("chatgpt/gpt-6-luna", 100)
    rows = [json.loads(line) for line in path.read_text().splitlines()]
    extra = {**rows[-1], "id": str(uuid4())}
    amendment = {"kind": "allowance_amendment", "previous_max_calls": 1, "max_calls": 3,
                 "reason": "Too late", "timestamp": extra["timestamp"]}
    with path.open("a") as stream:
        stream.write(json.dumps(extra) + "\n" + json.dumps(amendment) + "\n")
    resumed = SubscriptionCallBudget(path, max_calls=3)
    assert resumed.snapshot()["reserved_calls"] == 2


@pytest.mark.parametrize("model", ["chatgpt/gpt-6-sol", "chatgpt/gpt-6.1-sol", "chatgpt/gpt-6-astra"])
def test_nested_eval_budget_rejects_unapproved_model_before_reservation(tmp_path, model):
    from scripts.agent_eval_subscription_budget import SubscriptionCallBudget
    budget = SubscriptionCallBudget(tmp_path / "calls.jsonl", max_calls=2)
    scope = budget.for_scope("case", 1, "nested_actor")
    with pytest.raises(ValueError, match="Luna"):
        scope.reserve(model, 100)
    assert budget.snapshot()["reserved_calls"] == 0


def test_subscription_output_reservation_uses_provider_maximum_not_ignored_request_limit(tmp_path):
    from scripts.agent_eval_subscription_budget import SubscriptionCallBudget
    budget = SubscriptionCallBudget(tmp_path / "calls.jsonl", max_calls=20)
    for _ in range(7):
        budget.reserve("chatgpt/gpt-6-luna", 1)
    before = budget.path.read_bytes()
    budget.reserve("chatgpt/gpt-6-luna", 1)
    assert budget.path.read_bytes().startswith(before)
    status = SubscriptionCallBudget(budget.path, max_calls=20).snapshot()
    assert status["reserved_output_tokens"] == 1_024_000
    assert status["api_spend_usd"] == 0
    assert status["actual_subscription_credit_debit"] is None


def test_subscription_settlement_releases_only_unused_token_capacity(tmp_path):
    from scripts.agent_eval_subscription_budget import SubscriptionCallBudget
    budget = SubscriptionCallBudget(tmp_path / "calls.jsonl", max_calls=20)
    tickets = [budget.reserve("chatgpt/gpt-6-luna", 1) for _ in range(7)]
    budget.settle(tickets[0], {"input_tokens": 5, "cached_input_tokens": 3, "output_tokens": 2})
    resumed = SubscriptionCallBudget(budget.path, max_calls=20)
    resumed.reserve("chatgpt/gpt-6-luna", 1)
    resumed.reserve("chatgpt/gpt-6-luna", 1)
    s = resumed.snapshot()
    assert s["settled_calls"] == 1 and s["reserved_calls"] == 8
    assert s["usage"]["cached_input_tokens"] == 3
    assert s["equivalent_usd_upper_bound"] > 0
    assert s["equivalent_usd_allowance"] is None
    assert s["accounting_mode"] == "reporting"


def test_subscription_parallel_roles_share_reporting_without_a_token_cap(tmp_path):
    from concurrent.futures import ThreadPoolExecutor
    from scripts.agent_eval_subscription_budget import SubscriptionCallBudget
    path = tmp_path / "calls.jsonl"
    SubscriptionCallBudget(path, max_calls=20)
    def attempt(index):
        try:
            b = SubscriptionCallBudget(path, max_calls=20).for_scope("case", 1, str(index))
            return b.reserve("chatgpt/gpt-6-luna", 1)
        except ValueError:
            return None
    with ThreadPoolExecutor(max_workers=8) as pool:
        assert len([r for r in pool.map(attempt, range(12)) if r]) == 12


def test_subscription_adk_completion_reasoning_is_counted_once(tmp_path):
    from scripts.agent_eval_subscription_budget import SubscriptionCallBudget
    budget = SubscriptionCallBudget(tmp_path / "calls.jsonl", max_calls=2)
    ticket = budget.reserve("chatgpt/gpt-6-luna", 1)
    budget.settle(ticket, {"prompt_token_count": 10, "candidates_token_count": 100,
                          "thoughts_token_count": 80, "cached_content_token_count": 5})
    usage = budget.snapshot()["usage"]
    assert usage == {"input_tokens": 10, "cached_input_tokens": 5,
                     "output_tokens": 20, "thinking_tokens": 80}


def test_historical_mixed_model_cost_is_unknown_without_blocking_luna_reporting(tmp_path):
    from scripts.agent_eval_subscription_budget import SubscriptionCallBudget
    budget = SubscriptionCallBudget(tmp_path / "legacy.jsonl", max_calls=10)
    legacy = {key: value for key, value in budget.header.items() if key != "token_policy"}
    rows = [legacy,
            {"kind": "reserve", "id": "historical", "model": "chatgpt/gpt-6.1-sol",
             "max_output_tokens": 100, "nano_usd": 0, "reserved_nano_usd": 0},
            {"kind": "settle", "id": "historical", "nano_usd": 0,
             "usage": {"input_tokens": 100, "cached_input_tokens": 0,
                       "output_tokens": 10, "thinking_tokens": 20}}]
    budget.path.write_text("".join(json.dumps(row) + "\n" for row in rows))
    before = budget.path.read_bytes()
    resumed = SubscriptionCallBudget(budget.path, max_calls=10)
    assert resumed.snapshot()["equivalent_usd_upper_bound"] is None
    assert resumed.snapshot()["unpriced_historical_models"] == ["chatgpt/gpt-6.1-sol"]
    resumed.reserve("chatgpt/gpt-6-luna", 1)
    assert budget.path.read_bytes().startswith(before)


def _write_profile(home, profile_id, email):
    from lumibot.components.agents.chatgpt_auth import PLAN_SCOPE, save_private
    path = home / "profiles" / f"{profile_id}.json"
    save_private(path, {"status": "connected", "scopes": [PLAN_SCOPE], "email": email})
    return path


def test_usage_limit_fails_over_to_the_next_connected_account(monkeypatch, tmp_path):
    """Rob, 2026-10-17: one ChatGPT account running out must not stop local evals.

    The refused account cools down in a shared pool.json, the call retries on the
    next connected account, and later calls skip the cooling account entirely.
    """
    import asyncio

    import httpx
    from google.adk.models.llm_request import LlmRequest
    from google.genai import types

    from lumibot.components.agents import chatgpt_llm
    from lumibot.components.agents.runtime import _configure_litellm_quietly, _register_openai_gpt6_models
    _configure_litellm_quietly()
    _register_openai_gpt6_models()
    monkeypatch.setenv("LUMIBOT_CHATGPT_HOME", str(tmp_path))
    first = _write_profile(tmp_path, "11111111-1111-4111-8111-111111111111", "a@example.com")
    _write_profile(tmp_path, "22222222-2222-4222-8222-222222222222", "b@example.com")
    monkeypatch.setattr(chatgpt_llm, "access_token", lambda path: "token-" + json.loads(path.read_text())["email"])
    tokens = []

    def handle(request):
        token = request.headers["authorization"].removeprefix("Bearer ")
        tokens.append(token)
        if token == "token-a@example.com":
            events = [{"type": "response.failed", "response": {"error": {
                "code": "subscription_sharing_usage_limit_exceeded", "message": "limit"}}}]
        else:
            events = [
                {"type": "response.output_text.delta", "delta": "ok", "item_id": "m", "output_index": 0,
                 "content_index": 0},
                {"type": "response.completed", "response": {
                    "id": "r", "object": "response", "model": "gpt-6-luna", "status": "completed",
                    "output": [{"id": "m", "type": "message", "role": "assistant", "status": "completed",
                                "content": [{"type": "output_text", "text": "ok", "annotations": []}]}],
                    "usage": {"input_tokens": 1, "output_tokens": 1, "total_tokens": 2}}},
            ]
        data = "".join("data: " + json.dumps(event) + "\n\n" for event in events)

        class Stream(httpx.AsyncByteStream):
            async def __aiter__(self):
                yield data.encode()
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=Stream())

    def ask():
        model = chatgpt_llm.ChatGPTPlanLlm(model="openai/responses/gpt-6-luna", profile=first,
                                         transport=httpx.MockTransport(handle))
        request = LlmRequest(contents=[types.Content(role="user", parts=[types.Part(text="say ok")])],
                             config=types.GenerateContentConfig())

        async def consume():
            return [response async for response in model.generate_content_async(request)]
        return asyncio.run(consume())

    assert len(ask()) == 1
    assert tokens == ["token-a@example.com", "token-b@example.com"]
    pool = json.loads((tmp_path / "pool.json").read_text())
    assert pool["cooldowns"]["a@example.com"] > time.time()
    tokens.clear()
    assert len(ask()) == 1
    assert tokens == ["token-b@example.com"], "a cooling account is skipped, not retried"


def test_usage_limit_with_no_other_account_still_fails_visibly(monkeypatch, tmp_path):
    import asyncio

    import httpx
    from google.adk.models.llm_request import LlmRequest
    from google.genai import types

    from lumibot.components.agents import chatgpt_llm
    from lumibot.components.agents.runtime import _configure_litellm_quietly, _register_openai_gpt6_models
    _configure_litellm_quietly()
    _register_openai_gpt6_models()
    monkeypatch.setenv("LUMIBOT_CHATGPT_HOME", str(tmp_path))
    only = _write_profile(tmp_path, "11111111-1111-4111-8111-111111111111", "a@example.com")
    monkeypatch.setattr(chatgpt_llm, "access_token", lambda path: "token")
    calls = []

    def handle(request):
        calls.append(1)
        data = "data: " + json.dumps({"type": "response.failed", "response": {"error": {
            "code": "subscription_sharing_usage_limit_exceeded"}}}) + "\n\n"

        class Stream(httpx.AsyncByteStream):
            async def __aiter__(self):
                yield data.encode()
        return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=Stream())

    model = chatgpt_llm.ChatGPTPlanLlm(model="openai/responses/gpt-6-luna", profile=only,
                                     transport=httpx.MockTransport(handle))
    request = LlmRequest(contents=[types.Content(role="user", parts=[types.Part(text="x")])],
                         config=types.GenerateContentConfig())

    async def consume():
        return [response async for response in model.generate_content_async(request)]
    with pytest.raises(chatgpt_llm.SubscriptionModelResponseError, match="subscription_sharing_usage_limit_exceeded"):
        asyncio.run(consume())
    assert calls == [1]


def test_token_envelope_fits_a_full_release_catalog_inside_the_dollar_allowance():
    # 2026-10-17 4.6.20 release: 90 required repetitions over 18 cases used
    # about 250k input tokens each. A 10M input cap stopped the ledger after
    # 36 passes, so no full catalog could ever be qualified. The $20
    # equivalent allowance, not the token cap, must be the binding limit.
    from scripts import agent_eval_subscription_budget as budget

    full_catalog_inputs = 90 * 250_000 + 20 * budget.PROVIDER_INPUT_BOUND
    assert budget.MAX_SUBSCRIPTION_INPUT_TOKENS >= full_catalog_inputs
    assert budget.equivalent_upper_bound(
        budget.MAX_SUBSCRIPTION_INPUT_TOKENS, budget.MAX_SUBSCRIPTION_OUTPUT_TOKENS
    ) <= budget.SUBSCRIPTION_EQUIVALENT_ALLOWANCE_USD
