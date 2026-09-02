"""Google ADK model adapter for BotSpot's managed AI gateway.

Provider credentials never enter the strategy process. Bot Manager supplies a
deployment-bound capability instead, and this adapter renews that capability
through Node when a long-running strategy outlives the short access-token TTL.
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import json
import os
import threading
import urllib.error
import urllib.request
import uuid
from typing import Any, Callable

from google.adk.models.base_llm import BaseLlm
from google.adk.models.llm_response import LlmResponse
from google.genai import types
from pydantic import PrivateAttr


class ManagedAiGatewayError(RuntimeError):
    """A sanitized managed gateway failure safe to surface to a strategy."""

    def __init__(self, message: str, *, status_code: int | None = None, code: str | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.code = code


def _provider_for_model(model: str) -> str | None:
    lower = model.strip().lower()
    if lower.startswith("gemini-") or lower.startswith("models/gemini") or lower.startswith("google/"):
        return "google"
    if lower.startswith("openai/") or lower.startswith(("gpt-", "chatgpt-", "o1", "o3", "o4")):
        return "openai"
    if lower.startswith("anthropic/") or lower.startswith("claude-"):
        return "anthropic"
    if lower.startswith("xai/") or lower.startswith("grok-"):
        return "xai"
    return None


def _has_provider_key(model: str) -> bool:
    provider = _provider_for_model(model)
    key_names = {
        "google": ("GEMINI_API_KEY", "GOOGLE_API_KEY"),
        "openai": ("OPENAI_API_KEY",),
        "anthropic": ("ANTHROPIC_API_KEY",),
        "xai": ("XAI_API_KEY", "GROK_API_KEY"),
    }
    return bool(provider and any(str(os.environ.get(name) or "").strip() for name in key_names[provider]))


def managed_gateway_available_for(model: str) -> bool:
    """Return true only when a supported model has no BYOK credential.

    A configured provider key always wins. If that key is rejected upstream,
    the native provider error is returned and LumiBot never spends managed
    credits as an implicit fallback.
    """

    if _provider_for_model(model) is None or _has_provider_key(model):
        return False
    return bool(
        str(os.environ.get("LUMIBOT_AI_GATEWAY_URL") or "").strip()
        and str(os.environ.get("LUMIBOT_AI_GATEWAY_TOKEN") or "").strip()
    )


def _encoded_signature(part: Any) -> str | None:
    signature = getattr(part, "thought_signature", None)
    if signature is None:
        return None
    if not isinstance(signature, bytes):
        raise ManagedAiGatewayError(
            "Managed AI encountered an unsupported thought-signature value.",
            code="protocol_integrity_error",
        )
    return base64.b64encode(signature).decode("ascii")


def _structured_part(part: Any) -> dict[str, Any]:
    text = getattr(part, "text", None)
    function_call = getattr(part, "function_call", None)
    function_response = getattr(part, "function_response", None)
    populated = sum((isinstance(text, str), function_call is not None, function_response is not None))
    if populated != 1:
        raise ManagedAiGatewayError(
            "Managed AI cannot preserve one or more provider content parts.",
            code="protocol_integrity_error",
        )
    signature = _encoded_signature(part)
    if isinstance(text, str):
        return {
            "type": "text",
            "text": text,
            **({"thought": bool(part.thought)} if getattr(part, "thought", None) is not None else {}),
            **({"thoughtSignature": signature} if signature else {}),
        }
    if function_call is not None:
        arguments = getattr(function_call, "args", None)
        if arguments is None:
            arguments = {}
        if not isinstance(arguments, dict):
            raise ManagedAiGatewayError(
                "Managed AI received invalid function-call arguments.",
                code="protocol_integrity_error",
            )
        call_id = getattr(function_call, "id", None)
        return {
            "type": "function_call",
            **({"id": str(call_id)} if call_id else {}),
            "name": str(getattr(function_call, "name", "") or ""),
            "arguments": arguments,
            **({"thoughtSignature": signature} if signature else {}),
        }
    response = getattr(function_response, "response", None)
    if response is None:
        response = {}
    if not isinstance(response, dict):
        raise ManagedAiGatewayError(
            "Managed AI received an invalid function response.",
            code="protocol_integrity_error",
        )
    response_id = getattr(function_response, "id", None)
    return {
        "type": "function_response",
        **({"id": str(response_id)} if response_id else {}),
        "name": str(getattr(function_response, "name", "") or ""),
        "response": response,
    }


def _messages(llm_request: Any) -> list[dict[str, Any]]:
    messages: list[dict[str, Any]] = []
    system_instruction = getattr(getattr(llm_request, "config", None), "system_instruction", None)
    if system_instruction:
        if isinstance(system_instruction, str):
            system_parts = [{"type": "text", "text": system_instruction}]
        else:
            system_parts = [_structured_part(part) for part in getattr(system_instruction, "parts", None) or []]
        if system_parts:
            messages.append({"role": "system", "parts": system_parts})

    for content in getattr(llm_request, "contents", None) or []:
        parts = [_structured_part(part) for part in getattr(content, "parts", None) or []]
        if not parts:
            raise ManagedAiGatewayError(
                "Managed AI cannot send an empty provider content block.",
                code="protocol_integrity_error",
            )
        if getattr(content, "role", None) == "model":
            role = "assistant"
        elif all(part["type"] == "function_response" for part in parts):
            role = "tool"
        else:
            role = "user"
        messages.append({"role": role, "parts": parts})
    if not messages:
        messages.append(
            {
                "role": "user",
                "parts": [{"type": "text", "text": "Continue according to the system instructions."}],
            }
        )
    return messages


def _decoded_signature(value: Any) -> bytes | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ManagedAiGatewayError(
            "Managed AI received an invalid provider thought signature.",
            code="protocol_integrity_error",
        )
    try:
        return base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError):
        raise ManagedAiGatewayError(
            "Managed AI received an invalid provider thought signature.",
            code="protocol_integrity_error",
        ) from None


def _response_part(value: Any) -> types.Part:
    if not isinstance(value, dict):
        raise ManagedAiGatewayError(
            "Managed AI received an invalid provider content part.",
            code="protocol_integrity_error",
        )
    part_type = value.get("type")
    if part_type == "text" and isinstance(value.get("text"), str):
        return types.Part(
            text=value["text"],
            thought=value.get("thought") if isinstance(value.get("thought"), bool) else None,
            thought_signature=_decoded_signature(value.get("thoughtSignature")),
        )
    if part_type == "function_call" and isinstance(value.get("name"), str):
        arguments = value.get("arguments")
        if arguments is None:
            arguments = {}
        if not isinstance(arguments, dict):
            raise ManagedAiGatewayError(
                "Managed AI received invalid provider function-call arguments.",
                code="protocol_integrity_error",
            )
        return types.Part(
            function_call=types.FunctionCall(
                id=str(value["id"]) if value.get("id") else None,
                name=value["name"],
                args=arguments,
            ),
            thought_signature=_decoded_signature(value.get("thoughtSignature")),
        )
    raise ManagedAiGatewayError(
        "Managed AI received an unsupported provider content part.",
        code="protocol_integrity_error",
    )


def _tools(llm_request: Any) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for tool in getattr(getattr(llm_request, "config", None), "tools", None) or []:
        for declaration in getattr(tool, "function_declarations", None) or []:
            schema = getattr(declaration, "parameters_json_schema", None)
            if schema is None:
                parameters = getattr(declaration, "parameters", None)
                schema = parameters.model_dump(exclude_none=True, by_alias=True) if parameters is not None else {}
            result.append(
                {
                    "name": str(getattr(declaration, "name", "")),
                    "description": str(getattr(declaration, "description", "") or ""),
                    "inputSchema": schema or {},
                }
            )
    return [tool for tool in result if tool["name"]]


def _post_json(url: str, token: str, payload: dict[str, Any]) -> tuple[int, dict[str, Any]]:
    request = urllib.request.Request(
        url,
        data=json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8"),
        headers={
            "authorization": f"Bearer {token}",
            "content-type": "application/json",
            "accept": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        try:
            body = json.loads(exc.read().decode("utf-8"))
        except Exception:
            body = {}
        return exc.code, body
    except (urllib.error.URLError, TimeoutError, json.JSONDecodeError):
        raise ManagedAiGatewayError("Managed AI gateway is temporarily unavailable.") from None


class BotSpotManagedLlm(BaseLlm):
    """ADK model implementation backed by a renewable BotSpot capability."""

    _gateway_url: str = PrivateAttr()
    _access_token: str = PrivateAttr()
    _post: Callable[[str, str, dict[str, Any]], tuple[int, dict[str, Any]]] = PrivateAttr()
    _renew_lock: threading.Lock = PrivateAttr()
    _continuation_id: str | None = PrivateAttr(default=None)

    def __init__(
        self,
        *,
        model: str,
        gateway_url: str,
        access_token: str,
        post: Callable[[str, str, dict[str, Any]], tuple[int, dict[str, Any]]] = _post_json,
    ) -> None:
        super().__init__(model=model)
        self._gateway_url = gateway_url.rstrip("/")
        self._access_token = access_token
        self._post = post
        self._renew_lock = threading.Lock()
        self._continuation_id = None

    def _renew(self) -> None:
        status, body = self._post(f"{self._gateway_url}/v1/grants/renew", self._access_token, {})
        token = str(body.get("accessToken") or "").strip()
        if status != 200 or not token:
            raise ManagedAiGatewayError(
                str(body.get("message") or "Managed AI authorization could not be renewed."),
                status_code=status,
                code=str(body.get("error") or "renewal_failed"),
            )
        self._access_token = token
        os.environ["LUMIBOT_AI_GATEWAY_TOKEN"] = token

    def _inference(self, payload: dict[str, Any]) -> dict[str, Any]:
        attempted_token = self._access_token
        inference_path = "/v2/inference" if payload.get("protocolVersion") == 2 else "/v1/inference"
        status, body = self._post(f"{self._gateway_url}{inference_path}", attempted_token, payload)
        if status == 401:
            with self._renew_lock:
                if self._access_token == attempted_token:
                    self._renew()
            status, body = self._post(f"{self._gateway_url}{inference_path}", self._access_token, payload)
        if status < 200 or status >= 300:
            raise ManagedAiGatewayError(
                str(body.get("message") or "Managed AI request failed."),
                status_code=status,
                code=str(body.get("error") or "gateway_error"),
            )
        return body

    async def generate_content_async(self, llm_request: Any, stream: bool = False):
        provider = _provider_for_model(self.model)
        if provider is None:
            raise ManagedAiGatewayError(f"Model '{self.model}' is not available through managed AI.")
        config = getattr(llm_request, "config", None)
        payload = {
            "protocolVersion": 2,
            "requestId": str(uuid.uuid4()),
            "provider": provider,
            "model": self.model,
            "messages": _messages(llm_request),
            "tools": _tools(llm_request),
            "maxOutputTokens": int(getattr(config, "max_output_tokens", None) or 16_384),
        }
        if self._continuation_id:
            payload["continuationId"] = self._continuation_id
        temperature = getattr(config, "temperature", None)
        if temperature is not None:
            payload["temperature"] = float(temperature)
        body = await asyncio.to_thread(self._inference, payload)

        raw_parts = body.get("parts")
        if not isinstance(raw_parts, list) or not raw_parts:
            raise ManagedAiGatewayError(
                "Managed AI returned no structured provider content.",
                code="protocol_integrity_error",
            )
        parts = [_response_part(part) for part in raw_parts]
        continuation_id = body.get("continuationId")
        self._continuation_id = str(continuation_id) if continuation_id else None
        usage = body.get("usage") or {}
        usage_metadata = types.GenerateContentResponseUsageMetadata(
            prompt_token_count=int(usage.get("inputTokens") or 0),
            cached_content_token_count=int(usage.get("cachedInputTokens") or 0),
            candidates_token_count=int(usage.get("outputTokens") or 0),
            total_token_count=int(usage.get("inputTokens") or 0) + int(usage.get("outputTokens") or 0),
        )
        yield LlmResponse(
            model_version=str(body.get("model") or self.model),
            content=types.Content(role="model", parts=parts),
            partial=False,
            turn_complete=True,
            usage_metadata=usage_metadata,
        )


def managed_gateway_model(model: str) -> BotSpotManagedLlm:
    return BotSpotManagedLlm(
        model=model,
        gateway_url=str(os.environ["LUMIBOT_AI_GATEWAY_URL"]),
        access_token=str(os.environ["LUMIBOT_AI_GATEWAY_TOKEN"]),
    )
