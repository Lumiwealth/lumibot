import asyncio
import base64
import os
import threading
import types as python_types

import pytest
from google.genai import types

from lumibot.components.agents import managed_gateway as managed_gateway_module
from lumibot.components.agents.managed_gateway import BotSpotManagedLlm, ManagedAiGatewayError


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
    assert payload["tools"][0]["inputSchema"] == {
        "type": "object",
        "properties": {"symbol": {"type": "string"}},
    }
    assert responses[0].content.parts[0].text == "Checking."
    assert responses[0].content.parts[1].function_call.name == "get_price"
    assert responses[0].usage_metadata.prompt_token_count == 10


def test_managed_gateway_forwards_explicit_reasoning_effort():
    payloads = []
    model = BotSpotManagedLlm(
        model="openai/gpt-5.6-luna",
        gateway_url="https://gateway.example.test/",
        access_token="bounded-token",
        reasoning_effort="high",
        post=lambda _url, _token, payload: (
            payloads.append(payload) or 200,
            {"model": "gpt-5.6-luna", "parts": [{"type": "text", "text": "Done"}], "usage": {}},
        ),
    )

    async def collect():
        return [item async for item in model.generate_content_async(_request())]

    asyncio.run(collect())
    assert payloads[0]["reasoningEffort"] == "high"


def test_managed_family_pins_exact_model_across_native_tool_continuations():
    calls = []

    def post(url, token, payload):
        calls.append(payload)
        return 200, {"resolvedModel": "gemini-3.5-flash-lite", "model": "vendor-version-001",
                     "parts": [{"type": "function_call", "name": "get_price", "id": "call-1",
                                "arguments": {"symbol": "SPY"}}]}

    model = BotSpotManagedLlm(model="google/gemini-flash-lite", gateway_url="https://gateway.example.test",
                             access_token="fixture-capability", post=post)

    async def run():
        request = _request()
        first = [item async for item in model.generate_content_async(request)]
        request.contents.extend([first[0].content, types.Content(role="user", parts=[types.Part(
            function_response=types.FunctionResponse(id="call-1", name="get_price", response={"price": 100}))])])
        return [item async for item in model.generate_content_async(request)]

    asyncio.run(run())
    assert [call["model"] for call in calls] == ["google/gemini-flash-lite", "gemini-3.5-flash-lite"]
    assert calls[1]["messages"][-1]["parts"][0]["id"] == "call-1"


@pytest.mark.parametrize("resolved", ["openai/gpt-5.6-luna", "gemini-3.8-flash", "google/gemini-pro", 123])
def test_managed_gateway_rejects_changed_or_invalid_exact_model_resolution(resolved):
    model = BotSpotManagedLlm(model="gemini-3.5-flash-lite", gateway_url="https://gateway.example.test",
                             access_token="fixture-capability", post=lambda *_: (200, {
                                 "resolvedModel": resolved, "parts": [{"type": "text", "text": "done"}]}))

    async def run():
        return [item async for item in model.generate_content_async(_request())]

    with pytest.raises(ManagedAiGatewayError, match="model resolution"):
        asyncio.run(run())


@pytest.mark.parametrize("second_resolution", [None, "gemini-3.8-flash"])
def test_managed_family_rejects_resolution_loss_or_change_mid_decision(second_resolution):
    resolutions = iter(["gemini-3.5-flash-lite", second_resolution])
    model = BotSpotManagedLlm(model="google/gemini-flash-lite", gateway_url="https://gateway.example.test",
                             access_token="fixture-capability", post=lambda *_: (200, {
                                 "resolvedModel": next(resolutions), "parts": [{"type": "text", "text": "done"}]}))

    async def run():
        for _ in range(2):
            _ = [item async for item in model.generate_content_async(_request())]

    with pytest.raises(ManagedAiGatewayError, match="model resolution"):
        asyncio.run(run())


def test_concurrent_family_requests_resolve_only_once():
    calls = []

    def post(url, token, payload):
        calls.append(payload["model"])
        return 200, {"resolvedModel": "gemini-3.5-flash-lite", "parts": [{"type": "text", "text": "done"}]}

    model = BotSpotManagedLlm(model="google/gemini-flash-lite", gateway_url="https://gateway.example.test",
                             access_token="fixture-capability", post=post)

    async def call():
        return [item async for item in model.generate_content_async(_request())]

    async def run():
        await asyncio.gather(call(), call(), call())

    asyncio.run(run())
    assert calls.count("google/gemini-flash-lite") == 1
    assert calls.count("gemini-3.5-flash-lite") == 2


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


def test_managed_gateway_continuation_is_request_scoped_not_model_scoped():
    payloads = []

    def post(_url, _token, payload):
        payloads.append(payload)
        return 200, {
            "model": "openai/gpt-5.6-luna",
            "continuationId": f"next-{payload.get('continuationId') or 'root'}",
            "parts": [{"type": "text", "text": "Done"}],
            "usage": {},
        }

    model = BotSpotManagedLlm(
        model="openai/gpt-5.6-luna",
        gateway_url="https://gateway.example.test/",
        access_token="bounded-token",
        post=post,
    )
    first = _request()
    first.previous_interaction_id = "conversation-a"
    second = _request()
    second.previous_interaction_id = "conversation-b"

    async def collect(request):
        return [item async for item in model.generate_content_async(request)]

    first_response = asyncio.run(collect(first))[0]
    second_response = asyncio.run(collect(second))[0]

    assert [payload.get("continuationId") for payload in payloads] == [
        "conversation-a",
        "conversation-b",
    ]
    assert first_response.interaction_id == "next-conversation-a"
    assert second_response.interaction_id == "next-conversation-b"


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
    ("model", "expected_provider", "resolved_model"),
    [
        ("gemini-3.1-flash-lite", "google", None),
        ("openai/gpt-5.6-luna", "openai", None),
        ("anthropic/claude-sonnet-5", "anthropic", None),
        ("anthropic/opus", "anthropic", "claude-opus-5"),
        ("anthropic/fable", "anthropic", "claude-fable-5-1"),
        ("xai/grok-4.5", "xai", None),
    ],
)
def test_managed_gateway_routes_every_supported_provider(model, expected_provider, resolved_model):
    calls = []

    def post(url, token, payload):
        calls.append((url, token, payload))
        return 200, {
            "model": resolved_model or model,
            **({"resolvedModel": resolved_model} if resolved_model else {}),
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
