import asyncio
import types as python_types

from google.genai import types

from lumibot.components.agents.managed_gateway import BotSpotManagedLlm


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


def test_managed_gateway_maps_adk_request_and_response():
    calls = []

    def post(url, token, payload):
        calls.append((url, token, payload))
        return 200, {
            "model": "gemini-3.1-flash-lite",
            "text": "Checking.",
            "toolCalls": [{"id": "call-1", "name": "get_price", "arguments": {"symbol": "SPY"}}],
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
    assert url == "https://gateway.example.test/v1/inference"
    assert token == "bounded-token"
    assert payload["provider"] == "google"
    assert payload["maxOutputTokens"] == 321
    assert payload["messages"] == [
        {"role": "system", "content": "Follow the rules"},
        {"role": "user", "content": "Analyze SPY"},
    ]
    assert payload["tools"][0]["name"] == "get_price"
    assert responses[0].content.parts[0].text == "Checking."
    assert responses[0].content.parts[1].function_call.name == "get_price"
    assert responses[0].usage_metadata.prompt_token_count == 10


def test_expired_capability_renews_and_retries_same_request():
    calls = []

    def post(url, token, payload):
        calls.append((url, token, payload))
        if url.endswith("/v1/inference") and token == "expired-token":
            return 401, {"error": "unauthorized", "message": "Expired"}
        if url.endswith("/v1/grants/renew"):
            return 200, {"accessToken": "renewed-token", "expiresInSeconds": 600}
        return 200, {
            "model": "openai/gpt-5.6-luna",
            "text": "Done",
            "toolCalls": [],
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
        ["v1", "inference"],
        ["grants", "renew"],
        ["v1", "inference"],
    ]
    assert calls[-1][1] == "renewed-token"
    assert calls[0][2]["requestId"] == calls[-1][2]["requestId"]
    assert responses[0].content.parts[0].text == "Done"
