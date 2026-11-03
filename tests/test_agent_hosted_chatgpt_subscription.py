"""Hosted subscription wiring against recorded HTTP shapes; no provider calls."""
from pathlib import Path
import json
import pytest
from lumibot.components.agents import runtime, chatgpt_auth
from lumibot.components.agents.chatgpt_llm import ChatGPTPlanLlm
from lumibot.components.agents.managed_gateway import BotSpotManagedLlm


@pytest.fixture(autouse=True)
def native_routing_only(monkeypatch):
    monkeypatch.delenv('OPENAI_API_KEY', raising=False)
    monkeypatch.delenv('OPENAI_TOKEN', raising=False)


def test_hosted_explicit_chatgpt_route_does_not_require_a_local_login_file(monkeypatch):
    monkeypatch.setenv('LUMIBOT_AI_GATEWAY_URL', 'https://gateway.example.invalid/ai')
    monkeypatch.setenv('LUMIBOT_AI_GATEWAY_TOKEN', 'synthetic-workload-capability')
    def unavailable_local_profile():
        raise AssertionError('Hosted worker has no owner local-login file; use the existing plan-only gateway route')
    monkeypatch.setattr(chatgpt_auth, 'profile_path', unavailable_local_profile)
    model = runtime._resolve_model_for_adk('chatgpt/gpt-6-luna', reasoning_effort='high', model_request_timeout_seconds=0)
    assert isinstance(model, ChatGPTPlanLlm)
    assert model._profile is None
    assert model._gateway._gateway_url == 'https://gateway.example.invalid/ai'


def test_existing_local_explicit_chatgpt_route_stays_local(monkeypatch):
    monkeypatch.delenv('LUMIBOT_AI_GATEWAY_URL', raising=False)
    monkeypatch.delenv('LUMIBOT_AI_GATEWAY_TOKEN', raising=False)
    monkeypatch.setattr(chatgpt_auth, 'profile_path', lambda: Path('synthetic-local-profile'))
    model = runtime._resolve_model_for_adk('chatgpt/gpt-6-luna', reasoning_effort='high', model_request_timeout_seconds=0)
    assert isinstance(model, ChatGPTPlanLlm)
    assert model._profile == Path('synthetic-local-profile')


def test_existing_general_managed_openai_route_remains_supported(monkeypatch):
    monkeypatch.setenv('LUMIBOT_AI_GATEWAY_URL', 'https://gateway.example.invalid/ai')
    monkeypatch.setenv('LUMIBOT_AI_GATEWAY_TOKEN', 'synthetic-workload-capability')
    model = runtime._resolve_model_for_adk('openai/gpt-6-luna', reasoning_effort='high')
    assert isinstance(model, BotSpotManagedLlm)


def hosted_model(monkeypatch, handler, renewal=None):
    import httpx
    monkeypatch.setenv('LUMIBOT_AI_GATEWAY_URL', 'http://gateway.example.invalid/ai')
    monkeypatch.setenv('LUMIBOT_AI_GATEWAY_TOKEN', 'synthetic-workload-token')
    monkeypatch.setenv('OPENAI_API_KEY', 'must-never-fund-this-request')
    def no_local_profile():
        raise AssertionError('Hosted subscription must not read a local login file')
    monkeypatch.setattr(chatgpt_auth, 'profile_path', no_local_profile)
    model = runtime._resolve_model_for_adk('chatgpt/gpt-6-luna', reasoning_effort='high',
                                           model_request_timeout_seconds=0)
    model._transport = httpx.MockTransport(handler)
    if renewal is not None:
        model._gateway._post = renewal
    original_send = httpx.AsyncClient.send
    async def synthetic_only(client, request, **kwargs):
        assert isinstance(client._transport, httpx.MockTransport), 'No external HTTP is allowed in this native contract'
        return await original_send(client, request, **kwargs)
    monkeypatch.setattr(httpx.AsyncClient, 'send', synthetic_only)
    return model


def response_stream(*, failure=None, complete=True):
    import httpx
    # Same recorded Responses event/usage shape exercised by the local client.
    terminal = {'id': 'resp_contract', 'object': 'response', 'model': 'gpt-6-luna', 'status': 'completed',
        'output': [{'id': 'msg_contract', 'type': 'message', 'role': 'assistant', 'status': 'completed',
                    'content': [{'type': 'output_text', 'text': 'ok', 'annotations': []}]}],
        'usage': {'input_tokens': 5, 'output_tokens': 2, 'total_tokens': 7}}
    events = [{'type': 'response.output_text.delta', 'delta': 'ok', 'item_id': 'msg_contract',
               'output_index': 0, 'content_index': 0}]
    if failure:
        events.append({'type': 'error', 'error': {'code': failure}})
    elif complete:
        events.append({'type': 'response.completed', 'response': terminal})
    class Stream(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield ''.join('data: ' + json.dumps(e) + '\n\n' for e in events).encode()
    return httpx.Response(200, stream=Stream())


def consume(model, stream=False):
    import asyncio
    from google.adk.models.llm_request import LlmRequest
    from google.genai import types
    request = LlmRequest(contents=[types.Content(role='user', parts=[types.Part(text='say ok')])],
        config=types.GenerateContentConfig(max_output_tokens=100, temperature=0.2))
    async def run():
        return [result async for result in model.generate_content_async(request, stream=stream)]
    return asyncio.run(run())


@pytest.mark.parametrize('stream', [False, True])
def test_hosted_native_responses_use_plan_only_route_standard_and_original_usage(monkeypatch, stream):
    requests = []
    def handle(request):
        assert str(request.url) == 'http://gateway.example.invalid/ai/chatgpt/v1/responses'
        assert request.headers['authorization'] == 'Bearer synthetic-workload-token'
        requests.append(json.loads(request.content))
        return response_stream()
    model = hosted_model(monkeypatch, handle)
    result = consume(model, stream)
    assert result[-1].usage_metadata.prompt_token_count == 5
    assert len(requests) == 1
    assert requests[0]['model'] == 'gpt-6-luna'
    assert requests[0]['service_tier'] == 'default'
    assert requests[0]['stream'] is True and requests[0]['store'] is False
    assert requests[0]['reasoning']['effort'] == 'high'
    assert 'max_output_tokens' not in requests[0]


@pytest.mark.parametrize('always_rejected', [False, True])
def test_hosted_unauthorized_request_renews_once_through_existing_capability_owner(monkeypatch, always_rejected):
    import httpx
    from lumibot.components.agents.chatgpt_llm import SubscriptionModelResponseError
    tokens, renewals = [], []
    def handle(request):
        tokens.append(request.headers['authorization'])
        if len(tokens) == 1 or always_rejected:
            return httpx.Response(401, json={'error': {'code': 'workload_token_expired'}})
        return response_stream()
    def renew(url, token, body):
        renewals.append((url, token, body))
        return 200, {'accessToken': 'synthetic-renewed-workload-token'}
    model = hosted_model(monkeypatch, handle, renew)
    if always_rejected:
        with pytest.raises(SubscriptionModelResponseError) as error:
            consume(model)
        assert error.value.status_code == 401
    else:
        assert consume(model)[-1].content.parts[0].text == 'ok'
    assert tokens == ['Bearer synthetic-workload-token', 'Bearer synthetic-renewed-workload-token']
    assert renewals == [('http://gateway.example.invalid/ai/v1/grants/renew', 'synthetic-workload-token', {})]


@pytest.mark.parametrize('status,code', [(403, 'chatgpt_not_selected'), (429, 'usage_limit_reached'), (500, 'provider_failure')])
def test_hosted_failures_never_rotate_local_profiles_or_fall_back_to_paid_routes(monkeypatch, status, code):
    import httpx
    from lumibot.components.agents.chatgpt_llm import SubscriptionModelResponseError
    urls = []
    def handle(request):
        urls.append(str(request.url))
        return httpx.Response(status, json={'error': {'code': code}})
    model = hosted_model(monkeypatch, handle)
    with pytest.raises(SubscriptionModelResponseError) as error:
        consume(model)
    assert error.value.status_code == status
    assert error.value.code == code
    assert urls == ['http://gateway.example.invalid/ai/chatgpt/v1/responses']


@pytest.mark.parametrize('failure', ['usage_limit_reached', None])
def test_hosted_partial_or_failed_stream_is_not_retried_or_reported_complete(monkeypatch, failure):
    from lumibot.components.agents.chatgpt_llm import SubscriptionModelResponseError
    requests = []
    def handle(request):
        requests.append(str(request.url))
        return response_stream(failure=failure, complete=False)
    model = hosted_model(monkeypatch, handle)
    with pytest.raises(SubscriptionModelResponseError):
        consume(model)
    assert len(requests) == 1


def test_hosted_revoked_capability_cannot_renew_or_change_funding(monkeypatch):
    import httpx
    from lumibot.components.agents.managed_gateway import ManagedAiGatewayError
    requests, renewals = [], []
    def handle(request):
        requests.append(str(request.url))
        return httpx.Response(401, json={'error': {'code': 'workload_token_expired'}})
    def renew(url, token, body):
        renewals.append(url)
        return 403, {'error': 'renewal_denied', 'message': 'Workload is not authorized'}
    model = hosted_model(monkeypatch, handle, renew)
    with pytest.raises(ManagedAiGatewayError) as error:
        consume(model)
    assert error.value.status_code == 403
    assert error.value.code == 'renewal_denied'
    assert len(requests) == len(renewals) == 1
