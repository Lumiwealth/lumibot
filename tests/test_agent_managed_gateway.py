import asyncio
import base64
import os
import threading
import types as python_types

import pytest
from google.genai import types

from lumibot.components.agents import managed_gateway as managed_gateway_module
from lumibot.components.agents.managed_gateway import BotSpotManagedLlm
from lumibot.components.agents.managed_gateway import ManagedAiGatewayError


def _request():
    declaration = types.FunctionDeclaration(
        name="get_price",
        description="Get a price",
        parameters_json_schema={"type": "object", "properties": {"symbol": {"type": "string"}}},
    )
    return python_types.SimpleNamespace(
        config=types.GenerateContentConfig(
            system_instruction="Follow the rules",
            tools=[types.Tool(function_declarations=[declaration])],
            max_output_tokens=321,
            temperature=0.2,
        ),
        contents=[types.Content(role="user", parts=[types.Part(text="Analyze SPY")])],
    )


def test_default_gateway_transport_sanitizes_network_failures(monkeypatch):
    def fail(*_args, **_kwargs):
        raise managed_gateway_module.urllib.error.URLError("private-host.example.test")

    monkeypatch.setattr(managed_gateway_module.urllib.request, "urlopen", fail)

    with pytest.raises(ManagedAiGatewayError, match="temporarily unavailable") as exc_info:
        managed_gateway_module._post_json(
            "https://private-host.example.test/v1/inference",
            "secret-token",
            {"model": "openai/gpt-5.6-luna"},
        )

    assert "private-host" not in str(exc_info.value)


def test_managed_gateway_maps_adk_request_and_response():
    calls = []

    def post(url, token, payload):
        calls.append((url, token, payload))
        return 200, {
            "model": "gemini-3.1-flash-lite",
            "parts": [
                {"type": "text", "text": "Checking."},
                {
                    "type": "function_call",
                    "id": "call-1",
                    "name": "get_price",
                    "arguments": {"symbol": "SPY"},
                },
            ],
            "usage": {"inputTokens": 10, "cachedInputTokens": 2, "outputTokens": 3},
        }

    model = BotSpotManagedLlm(
        model="gemini-3.1-flash-lite",
        gateway_url="https://gateway.example.test/",
        access_token="bounded-token",
        post=post,
    )

    async def collect():
        return [item async for item in model.generate_content_async(_request())]

    responses = asyncio.run(collect())

    assert len(calls) == 1
    url, token, payload = calls[0]
    assert url == "https://gateway.example.test/v2/inference"
    assert token == "bounded-token"
    assert payload["provider"] == "google"
    assert payload["maxOutputTokens"] == 321
    assert payload["messages"] == [
        {"role": "system", "parts": [{"type": "text", "text": "Follow the rules"}]},
        {"role": "user", "parts": [{"type": "text", "text": "Analyze SPY"}]},
    ]
    assert payload["tools"][0]["name"] == "get_price"
    assert responses[0].content.parts[0].text == "Checking."
    assert responses[0].content.parts[1].function_call.name == "get_price"
    assert responses[0].usage_metadata.prompt_token_count == 10


def test_managed_gateway_preserves_structured_sequential_tool_history():
    calls = []
    signature = b"opaque-gemini-thought-signature"

    request = _request()
    request.contents.extend(
        [
            types.Content(
                role="model",
                parts=[
                    types.Part(
                        function_call=types.FunctionCall(
                            id="call-portfolio",
                            name="account_portfolio",
                            args={},
                        ),
                        thought_signature=signature,
                    )
                ],
            ),
            types.Content(
                role="user",
                parts=[
                    types.Part(
                        function_response=types.FunctionResponse(
                            id="call-portfolio",
                            name="account_portfolio",
                            response={"cash": 100_000, "portfolio_value": 100_000},
                        )
                    )
                ],
            ),
        ]
    )

    def post(url, token, payload):
        calls.append((url, token, payload))
        return 200, {
            "model": "gemini-3.5-flash-lite",
            "parts": [
                {
                    "type": "function_call",
                    "id": "call-positions",
                    "name": "account_positions",
                    "arguments": {},
                    "thoughtSignature": base64.b64encode(signature).decode("ascii"),
                }
            ],
            "usage": {"inputTokens": 10, "cachedInputTokens": 2, "outputTokens": 3},
            "finishReason": "STOP",
        }

    model = BotSpotManagedLlm(
        model="gemini-3.5-flash-lite",
        gateway_url="https://gateway.example.test/",
        access_token="bounded-token",
        post=post,
    )

    async def collect():
        return [item async for item in model.generate_content_async(request)]

    responses = asyncio.run(collect())

    assert calls[0][0] == "https://gateway.example.test/v2/inference"
    assert calls[0][2]["protocolVersion"] == 2
    assert calls[0][2]["messages"] == [
        {"role": "system", "parts": [{"type": "text", "text": "Follow the rules"}]},
        {"role": "user", "parts": [{"type": "text", "text": "Analyze SPY"}]},
        {
            "role": "assistant",
            "parts": [
                {
                    "type": "function_call",
                    "id": "call-portfolio",
                    "name": "account_portfolio",
                    "arguments": {},
                    "thoughtSignature": base64.b64encode(signature).decode("ascii"),
                }
            ],
        },
        {
            "role": "tool",
            "parts": [
                {
                    "type": "function_response",
                    "id": "call-portfolio",
                    "name": "account_portfolio",
                    "response": {"cash": 100_000, "portfolio_value": 100_000},
                }
            ],
        },
    ]
    function_call = responses[0].content.parts[0].function_call
    assert function_call.id == "call-positions"
    assert function_call.name == "account_positions"
    assert responses[0].content.parts[0].thought_signature == signature


def test_managed_gateway_preserves_parallel_call_order_and_linkage():
    request = _request()
    request.contents.extend(
        [
            types.Content(
                role="model",
                parts=[
                    types.Part(
                        function_call=types.FunctionCall(id="call-spy", name="get_price", args={"symbol": "SPY"})
                    ),
                    types.Part(
                        function_call=types.FunctionCall(id="call-qqq", name="get_price", args={"symbol": "QQQ"})
                    ),
                ],
            ),
            types.Content(
                role="user",
                parts=[
                    types.Part(
                        function_response=types.FunctionResponse(
                            id="call-spy",
                            name="get_price",
                            response={"price": 600},
                        )
                    ),
                    types.Part(
                        function_response=types.FunctionResponse(
                            id="call-qqq",
                            name="get_price",
                            response={"price": 500},
                        )
                    ),
                ],
            ),
        ]
    )
    payloads = []

    def post(_url, _token, payload):
        payloads.append(payload)
        return 200, {
            "model": "gemini-3.5-flash-lite",
            "parts": [{"type": "text", "text": "Done"}],
            "usage": {},
        }

    model = BotSpotManagedLlm(
        model="gemini-3.5-flash-lite",
        gateway_url="https://gateway.example.test/",
        access_token="bounded-token",
        post=post,
    )

    async def collect():
        return [item async for item in model.generate_content_async(request)]

    asyncio.run(collect())
    tool_history = payloads[0]["messages"][-2:]
    assert [part["id"] for part in tool_history[0]["parts"]] == ["call-spy", "call-qqq"]
    assert [part["id"] for part in tool_history[1]["parts"]] == ["call-spy", "call-qqq"]


@pytest.mark.parametrize(
    "parts",
    [
        [],
        [{"type": "function_response", "id": "call-1", "name": "get_price", "response": {}}],
        [{"type": "function_call", "name": "get_price", "arguments": []}],
        [{"type": "text", "text": "Done", "thoughtSignature": "not-base64"}],
    ],
)
def test_managed_gateway_rejects_lossy_or_malformed_provider_parts(parts):
    def post(_url, _token, _payload):
        return 200, {"model": "gemini-3.5-flash-lite", "parts": parts, "usage": {}}

    model = BotSpotManagedLlm(
        model="gemini-3.5-flash-lite",
        gateway_url="https://gateway.example.test/",
        access_token="bounded-token",
        post=post,
    )

    async def collect():
        return [item async for item in model.generate_content_async(_request())]

    with pytest.raises(ManagedAiGatewayError) as exc_info:
        asyncio.run(collect())
    assert exc_info.value.code == "protocol_integrity_error"


@pytest.mark.parametrize(
    ("model", "expected_provider"),
    [
        ("gemini-3.1-flash-lite", "google"),
        ("openai/gpt-5.6-luna", "openai"),
        ("anthropic/claude-sonnet-5", "anthropic"),
        ("xai/grok-4.5", "xai"),
    ],
)
def test_managed_gateway_routes_every_supported_provider(model, expected_provider):
    calls = []

    def post(url, token, payload):
        calls.append((url, token, payload))
        return 200, {
            "model": model,
            "parts": [{"type": "text", "text": "Done"}],
            "usage": {"inputTokens": 7, "cachedInputTokens": 2, "outputTokens": 1},
        }

    managed_model = BotSpotManagedLlm(
        model=model,
        gateway_url="https://gateway.example.test",
        access_token="bounded-token",
        post=post,
    )

    async def collect():
        # ADK is allowed to request streaming. The gateway currently returns one
        # final, fully accounted response rather than unmetered partial chunks.
        return [item async for item in managed_model.generate_content_async(_request(), stream=True)]

    responses = asyncio.run(collect())

    assert calls[0][2]["provider"] == expected_provider
    assert calls[0][2]["model"] == model
    assert responses[0].usage_metadata.prompt_token_count == 7
    assert responses[0].usage_metadata.cached_content_token_count == 2
    assert responses[0].usage_metadata.candidates_token_count == 1


def test_expired_capability_renews_and_retries_same_request(monkeypatch):
    monkeypatch.delenv("LUMIBOT_AI_GATEWAY_TOKEN", raising=False)
    calls = []

    def post(url, token, payload):
        calls.append((url, token, payload))
        if url.endswith("/v2/inference") and token == "expired-token":
            return 401, {"error": "unauthorized", "message": "Expired"}
        if url.endswith("/v1/grants/renew"):
            return 200, {"accessToken": "renewed-token", "expiresInSeconds": 600}
        return 200, {
            "model": "openai/gpt-5.6-luna",
            "parts": [{"type": "text", "text": "Done"}],
            "usage": {"inputTokens": 5, "cachedInputTokens": 0, "outputTokens": 1},
        }

    model = BotSpotManagedLlm(
        model="openai/gpt-5.6-luna",
        gateway_url="https://gateway.example.test",
        access_token="expired-token",
        post=post,
    )

    async def collect():
        return [item async for item in model.generate_content_async(_request())]

    responses = asyncio.run(collect())

    assert [call[0].rsplit("/", 2)[-2:] for call in calls] == [
        ["v2", "inference"],
        ["grants", "renew"],
        ["v2", "inference"],
    ]
    assert calls[-1][1] == "renewed-token"
    assert calls[0][2]["requestId"] == calls[-1][2]["requestId"]
    assert os.environ["LUMIBOT_AI_GATEWAY_TOKEN"] == "renewed-token"
    assert responses[0].content.parts[0].text == "Done"


def test_concurrent_expiration_rotates_the_capability_only_once(monkeypatch):
    monkeypatch.delenv("LUMIBOT_AI_GATEWAY_TOKEN", raising=False)
    calls = []
    both_expired = threading.Barrier(2)
    calls_lock = threading.Lock()

    def post(url, token, payload):
        with calls_lock:
            calls.append((url, token, payload))
        if url.endswith("/v2/inference") and token == "expired-token":
            both_expired.wait(timeout=2)
            return 401, {"error": "unauthorized", "message": "Expired"}
        if url.endswith("/v1/grants/renew"):
            return 200, {"accessToken": "renewed-token", "expiresInSeconds": 600}
        return 200, {
            "model": "openai/gpt-5.6-luna",
            "parts": [{"type": "text", "text": "Done"}],
            "usage": {"inputTokens": 5, "cachedInputTokens": 0, "outputTokens": 1},
        }

    model = BotSpotManagedLlm(
        model="openai/gpt-5.6-luna",
        gateway_url="https://gateway.example.test",
        access_token="expired-token",
        post=post,
    )

    async def collect():
        return await asyncio.gather(*[asyncio.create_task(_collect_one(model)) for _ in range(2)])

    async def run():
        return [item async for item in model.generate_content_async(_request())]

    async def _collect_one(model):
        return await run()

    responses = asyncio.run(collect())

    renewals = [call for call in calls if call[0].endswith("/v1/grants/renew")]
    successful_retries = [call for call in calls if call[0].endswith("/v2/inference") and call[1] == "renewed-token"]
    assert len(renewals) == 1
    assert len(successful_retries) == 2
    assert [response[0].content.parts[0].text for response in responses] == ["Done", "Done"]
