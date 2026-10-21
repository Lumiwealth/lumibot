"""ADK model adapter using the public subscription Responses endpoint.

Uses the same ADK/LiteLLM message and tool conversion as normal LumiBot inference.
HTTP streaming is mandatory for plan usage, including non-streaming ADK callers.
"""
from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Any

from google.adk.models.base_llm import BaseLlm
from google.adk.models.lite_llm import LiteLlm, LiteLLMClient
from pydantic import PrivateAttr
import httpx
from litellm.llms.custom_httpx.http_handler import AsyncHTTPHandler

from .chatgpt_auth import USAGE_LIMIT_CODES, access_token, is_cooling, mark_exhausted, next_connected_profile


class _ResponsesReplay:
    """Private protocol state for one ADK run, never public event metadata.

    ADK's chat projection loses opaque Responses reasoning and assistant phase.
    Anchor completed output groups to their stable function call ids. Keep the
    existing conversion for current user messages and tool outputs, including
    any tool-output pruning performed by the runtime.
    """
    def __init__(self):
        self.by_call = {}

    def remember(self, output):
        if not isinstance(output, list):
            return
        original = json.loads(json.dumps(output))
        call_ids = []
        for item in original:
            if item.get("type") != "function_call":
                continue
            call_id = item.get("call_id")
            if not isinstance(call_id, str) or not call_id:
                raise SubscriptionModelResponseError("subscription_call_id_missing")
            if call_id in call_ids or (call_id in self.by_call and self.by_call[call_id] != original):
                raise SubscriptionModelResponseError("subscription_history_conflict")
            call_ids.append(call_id)
        for item in original:
            if item.get("type") == "function_call" and isinstance(item.get("call_id"), str):
                self.by_call[item["call_id"]] = original

    @staticmethod
    def _text(item):
        content = item.get("content", [])
        if isinstance(content, str):
            return content
        return "".join(part.get("text", "") for part in content if isinstance(part, dict))

    def restore(self, items):
        calls = {item.get("call_id") for item in items if item.get("type") == "function_call"}
        self.by_call = {key: value for key, value in self.by_call.items() if key in calls}
        replacements, removed, seen = {}, set(), set()
        for index, item in enumerate(items):
            if item.get("type") != "function_call" or item.get("call_id") not in self.by_call:
                continue
            group = self.by_call[item["call_id"]]
            removed.add(index)
            identity = id(group)
            if identity in seen:
                continue
            seen.add(identity)
            start = index
            message_text = "".join(self._text(row) for row in group if row.get("type") == "message")
            # Only replace projected assistant text that actually belongs to
            # this completed response; preceding user/history text stays put.
            if index and message_text and items[index - 1].get("role") == "assistant" and self._text(items[index - 1]) == message_text:
                start = index - 1
                removed.add(start)
            replacements[start] = group
        result = []
        for index, item in enumerate(items):
            if index in replacements:
                result.extend(json.loads(json.dumps(replacements[index])))
            if index not in removed:
                result.append(item)
        return result


class SubscriptionModelResponseError(RuntimeError):
    """Keep the ADK machine reason without logging tool arguments or tokens."""

    def __init__(self, code: Any, status_code: int | None = None) -> None:
        raw = str(getattr(code, "value", code))
        self.code = raw if len(raw) <= 100 and all(c.isalnum() or c in "_.-" for c in raw) else "unknown"
        self.status_code = status_code
        super().__init__(f"Subscription model returned an unsuccessful response ({self.code})")


class CompletionCheckedStream(httpx.AsyncByteStream):
    """Require provider completion before ADK can finalize a truncated stream."""

    def __init__(self, delegate: httpx.AsyncByteStream, replay=None, on_completed=None) -> None:
        self.delegate = delegate
        self.failure = None
        self.replay = replay
        self.on_completed = on_completed

    async def __aiter__(self) -> AsyncGenerator[bytes, None]:
        pending = b""
        complete = False
        async for chunk in self.delegate:
            pending += chunk
            while b"\n" in pending:
                line, pending = pending.split(b"\n", 1)
                if line.startswith(b"data:"):
                    data = line[5:].strip()
                    if data and data != b"[DONE]":
                        event = json.loads(data)
                        if event.get("type") in {"error", "response.failed", "response.incomplete"}:
                            error = event.get("error") or event.get("response", {}).get("error") or {}
                            self.failure = SubscriptionModelResponseError(error.get("code") or event.get("code") or "unknown")
                            raise self.failure
                        if event.get("type") == "response.completed":
                            complete = event.get("response", {}).get("status") == "completed"
                            if complete and self.replay is not None:
                                self.replay.remember(event["response"].get("output"))
                            if complete and self.on_completed is not None:
                                try:
                                    await self.on_completed(event["response"])
                                except Exception as error:
                                    self.failure = error
                                    raise
            yield chunk
        if not complete:
            self.failure = SubscriptionModelResponseError("subscription_stream_incomplete")
            raise self.failure

    async def aclose(self) -> None:
        await self.delegate.aclose()


async def check_response_stream(response: httpx.Response, replay=None, on_completed=None) -> None:
    if not response.is_success:
        await response.aread()
        try:
            code = response.json().get("error", {}).get("code")
        except (ValueError, AttributeError):
            code = "unknown"
        raise SubscriptionModelResponseError(code, status_code=response.status_code)
    # This owned client always requests streaming Responses. Real plan replies
    # can omit Content-Type; the actual terminal event, rather than a MIME hint,
    # must attest completion and retain opaque tool-continuation history.
    if not isinstance(response.stream, httpx.AsyncByteStream):
        raise RuntimeError("Subscription requires an asynchronous response stream")
    response.stream = CompletionCheckedStream(response.stream, replay, on_completed)


class _PlanResponsesClient(LiteLLMClient):
    """Use the existing conversion bridge with an owned HTTP client.

    LiteLLM 1.105's async chat-to-Responses bridge drops its client argument.
    This narrow adapter supplies it to the same transformation. Remove when
    the upstream async bridge forwards clients and the wire contract passes.
    """
    def __init__(self, handler, token, replay):
        self.handler, self.token, self.replay = handler, token, replay

    async def acompletion(self, model, messages, tools, **kwargs):
        from datetime import datetime
        from uuid import uuid4
        from litellm import LiteLLMLoggingObj
        from litellm.types.utils import ModelResponse
        from litellm.completion_extras.litellm_responses_transformation.handler import ResponsesToCompletionBridgeHandler
        from litellm.completion_extras.litellm_responses_transformation.transformation import LiteLLMResponsesTransformationHandler

        handler = self.handler
        replay = self.replay
        class OwnedTransformation(LiteLLMResponsesTransformationHandler):
            def transform_request(self, **arguments):
                result = super().transform_request(**{**arguments, "client": handler})
                result["input"] = replay.restore(result["input"])
                return result

        slug = model.removeprefix("openai/responses/")
        bridge = ResponsesToCompletionBridgeHandler()
        bridge.transformation_handler = OwnedTransformation()
        optional = {key: kwargs[key] for key in (
            "reasoning_effort", "tool_choice", "response_format", "parallel_tool_calls", "stream_options"
        ) if kwargs.get(key) is not None}
        optional.update(tools=tools, stream=True, extra_body={"store": False}, service_tier="default")
        logging = LiteLLMLoggingObj(model=slug, messages=messages, stream=True,
            call_type="acompletion", start_time=datetime.now(), litellm_call_id=str(uuid4()), function_id=str(uuid4()))
        logging.stream_options = {"include_usage": True}
        return await bridge.acompletion(model=slug, messages=messages, optional_params=optional,
            litellm_params={"api_key": self.token, "api_base": "https://api.openai.com/v1",
                           "num_retries": 0, "max_retries": 0, "timeout": kwargs.get("timeout", 180)},
            headers={}, model_response=ModelResponse(), logging_obj=logging, custom_llm_provider="openai")


class ChatGPTPlanLlm(BaseLlm):
    _profile: Path = PrivateAttr()
    _kwargs: dict[str, Any] = PrivateAttr()
    _transport: Any = PrivateAttr(default=None)
    _replay: Any = PrivateAttr(default_factory=_ResponsesReplay)

    def __init__(self, *, model: str, profile: Path, transport: Any = None, **kwargs: Any) -> None:
        if not model.startswith("openai/responses/"):
            raise ValueError("ChatGPT plan requires an explicit OpenAI Responses model")
        if kwargs.get("service_tier") not in (None, "default"):
            raise ValueError("Subscription inference does not enable priority/Fast inference")
        super().__init__(model=model)
        self._profile = profile
        self._kwargs = kwargs
        self._transport = transport

    async def generate_content_async(self, llm_request: Any, stream: bool = False) -> AsyncGenerator[Any, None]:
        from .chatgpt_run import is_run_profile
        pinned_run = is_run_profile(self._profile)
        # A plan usage limit arrives before any output. Cool that account down
        # and retry on the next connected one; any other failure, or a limit
        # after output started, is raised unchanged.
        if not pinned_run and is_cooling(self._profile):
            self._profile = next_connected_profile(self._profile) or self._profile
        while True:
            produced = False
            try:
                async for response in self._generate_once(llm_request, stream):
                    produced = True
                    yield response
                return
            except SubscriptionModelResponseError as error:
                if pinned_run or produced or error.code not in USAGE_LIMIT_CODES:
                    raise
                mark_exhausted(self._profile)
                following = next_connected_profile(self._profile)
                if following is None:
                    raise
                self._profile = following

    async def _generate_once(self, llm_request: Any, stream: bool = False) -> AsyncGenerator[Any, None]:
        token = await asyncio.to_thread(access_token, self._profile)
        from .chatgpt_run import RunBudget, is_run_profile
        run_budget = RunBudget(self._profile) if is_run_profile(self._profile) else None
        kwargs = dict(self._kwargs)
        # LiteLLM drops top-level store=False on its completion bridge. The
        # existing Responses extra_body path preserves it (live-verified).
        kwargs["extra_body"] = {**kwargs.get("extra_body", {}), "store": False}
        kwargs.update(api_key=token, api_base="https://api.openai.com/v1", num_retries=0, service_tier="default")
        # Runtime tools own locks/clients and must retain their identity. Only
        # copy the configuration fields this transport changes.
        request = llm_request.model_copy()
        if request.config:
            request.config = request.config.model_copy()
            # Unsupported by the subscription endpoint. Do not modify prompts,
            # tools, history, requested model or requested reasoning effort.
            request.config.max_output_tokens = None
            request.config.temperature = None
            request.config.top_p = None
            request.config.top_k = None
        terminal = False
        checked_streams = []
        response_failure = None
        request_failure = None

        async def admit_request(request):
            nonlocal request_failure
            if run_budget is None:
                return
            try:
                if str(request.url) != "https://api.openai.com/v1/responses" or request.method != "POST":
                    raise ValueError("Run credential only permits subscription Responses requests")
                body = json.loads(request.content)
                if body.get("service_tier") != "default" or body.get("store") is not False or body.get("stream") is not True:
                    raise ValueError("Run request must use Standard, streaming and store=false")
                from litellm import get_model_info
                info = get_model_info("openai/" + body["model"])
                # Output limits are stripped on this endpoint. Reserve the actual
                # catalog envelope, not the smaller ADK caller's requested limit.
                ticket = await asyncio.to_thread(run_budget.reserve, body["model"],
                    input_bound=info.get("max_input_tokens"),
                    output_bound=info.get("max_output_tokens"))
                request.extensions["lumibot_plan_ticket"] = ticket
            except Exception as error:
                request_failure = error
                raise

        async def checked_response(response):
            nonlocal response_failure
            async def completed(payload):
                if run_budget is not None:
                    ticket = response.request.extensions.get("lumibot_plan_ticket")
                    if not ticket:
                        raise RuntimeError("Run response lacks a reserved request")
                    await asyncio.to_thread(run_budget.settle, ticket, payload.get("usage"))
            try:
                await check_response_stream(response, self._replay, completed if run_budget is not None else None)
            except SubscriptionModelResponseError as error:
                response_failure = error
                raise
            if isinstance(response.stream, CompletionCheckedStream):
                checked_streams.append(response.stream)

        # Responses routing uses LiteLLM's AsyncHTTPHandler, not an OpenAI
        # SDK client. Passing the wrong client type silently loses hooks.
        http_handler = AsyncHTTPHandler(timeout=self._kwargs.get("timeout", 180),
            event_hooks={"request": [admit_request], "response": [checked_response]}, transport=self._transport, follow_redirects=False)
        try:
            delegate = LiteLlm(model=self.model, llm_client=_PlanResponsesClient(http_handler, token, self._replay), **kwargs)
            async for response in delegate.generate_content_async(request, stream=True):
                if getattr(response, "error_code", None):
                    raise SubscriptionModelResponseError(response.error_code)
                if not getattr(response, "partial", False):
                    terminal = True
                if stream or not getattr(response, "partial", False):
                    yield response
        except Exception:
            # Preserve structured transport causes if LiteLLM wrapped them.
            failure = request_failure or response_failure or next((item.failure for item in checked_streams if item.failure), None)
            if failure:
                raise failure from None
            raise
        finally:
            await http_handler.close()
        if not terminal:
            raise RuntimeError("Subscription stream ended without a final ADK response")
