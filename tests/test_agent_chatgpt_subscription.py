"""Subscription auth and transport contracts; no model or credential network calls."""
import json
import os
import time

import pytest

from lumibot.components.agents import chatgpt_auth as auth


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


def test_subscription_call_budget_enforces_calls_across_resume(tmp_path):
    from scripts.agent_eval_subscription_budget import SubscriptionCallBudget
    path = tmp_path / "calls.jsonl"
    budget = SubscriptionCallBudget(path, max_calls=1)
    ticket = budget.for_scope("case", 1, "acting").reserve("chatgpt/gpt-6-luna", 100)
    budget.settle(ticket, {"input_tokens": 5, "output_tokens": 2})
    resumed = SubscriptionCallBudget(path, max_calls=1)
    with pytest.raises(ValueError, match="call limit"):
        resumed.reserve("chatgpt/gpt-6-luna", 100)
    assert resumed.snapshot()["committed_usd"] == 0
    assert resumed.snapshot()["usage"]["input_tokens"] == 5


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
    import httpx
    import jwt
    import webbrowser
    from types import SimpleNamespace
    from threading import Thread
    from urllib.parse import parse_qs, urlsplit, urlencode
    from urllib.request import urlopen
    from uuid import uuid4
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
    from lumibot.components.agents.chatgpt_llm import _ResponsesReplay, SubscriptionModelResponseError
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
def test_actual_adk_subscription_transport_owns_stream_validation(monkeypatch, tmp_path, completed):
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
        return httpx.Response(200, headers={"content-type":"text/event-stream"}, stream=Stream())
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
def test_stateless_tool_continuation_replays_opaque_reasoning_and_function_items(monkeypatch, tmp_path, runner_mode):
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
                            {'type': 'response.function_call_arguments.delta', 'item_id': tool_call['id'],
                             'output_index': index, 'delta': tool_call['arguments']},
                            {'type': 'response.output_item.done', 'output_index': index, 'item': tool_call},
                        ])
                else:
                    events.append({'type': 'response.output_text.delta', 'delta': 'done',
                                   'item_id': 'msg_final', 'output_index': 0, 'content_index': 0})
                events.append({'type': 'response.completed', 'response': terminal})
                yield ''.join('data: ' + json.dumps(event) + '\n\n' for event in events).encode()
        return httpx.Response(200, headers={'content-type': 'text/event-stream'}, stream=Stream())
    model = chatgpt_llm.ChatGPTPlanLlm(model='openai/responses/gpt-6-luna', profile=tmp_path/'profile.json',
                                     transport=httpx.MockTransport(handle))
    request = LlmRequest(contents=[types.Content(role='user', parts=[types.Part(text='get CPI evidence')])],
                         config=types.GenerateContentConfig())
    async def run_runner():
        from google.adk.agents import LlmAgent
        from google.adk.runners import InMemoryRunner
        def get_evidence(series: str) -> dict:
            """Return recorded economic evidence for the requested series."""
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
        request.contents.extend([first[-1].content, types.Content(role='user', parts=[
            types.Part(function_response=types.FunctionResponse(id=function.id, name=function.name,
                                                                response={'value': 2.5}))])])
        return [r async for r in model.generate_content_async(request)]
    if runner_mode:
        asyncio.run(run_runner())
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
