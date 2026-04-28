import asyncio
import contextlib
import hashlib
import importlib
import logging
import json
import inspect
import math
import os
import re
import sys
import time
import warnings
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any
from uuid import uuid4

import httpx
from anyio import run as anyio_run
from anyio.from_thread import start_blocking_portal
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.client.streamable_http import streamablehttp_client

from .schemas import AgentRunResult, AgentTraceEvent, BoundTool, MCPServer


_GOOGLE_SDK_NOISE_FILTERS_CONFIGURED = False


class _GoogleGenAITypesNoiseFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        return "there are non-text parts in the response" not in message


def _configure_google_sdk_noise_filters() -> None:
    global _GOOGLE_SDK_NOISE_FILTERS_CONFIGURED
    if _GOOGLE_SDK_NOISE_FILTERS_CONFIGURED:
        return
    warnings.filterwarnings(
        "ignore",
        message="deprecated",
        category=DeprecationWarning,
        module=r"google\.adk\.runners",
    )
    warnings.filterwarnings(
        "ignore",
        message=r"Inheritance class AiohttpClientSession from ClientSession is discouraged",
        category=DeprecationWarning,
        module=r"google\.genai\._api_client",
    )
    logging.getLogger("google.genai.types").addFilter(_GoogleGenAITypesNoiseFilter())
    logging.getLogger("google_genai.types").addFilter(_GoogleGenAITypesNoiseFilter())
    _GOOGLE_SDK_NOISE_FILTERS_CONFIGURED = True


def _tool_error_payload(tool_name: str, args: dict[str, Any], exc: Exception) -> dict[str, Any]:
    return {
        "ok": False,
        "tool_error": True,
        "tool_name": tool_name,
        "error": {
            "type": exc.__class__.__name__,
            "message": str(exc),
        },
        "arguments": _json_safe_value(dict(args or {})),
    }


def _utc_iso_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _tool_function_name(value: str) -> str:
    normalized = re.sub(r"[^0-9a-zA-Z_]+", "_", value).strip("_")
    if not normalized:
        normalized = "tool"
    if normalized[0].isdigit():
        normalized = f"tool_{normalized}"
    return normalized


def _wrap_tool_callable(tool: BoundTool):
    original = tool.function

    def wrapper(*args, **kwargs):
        try:
            return _json_safe_value(original(*args, **kwargs))
        except Exception as exc:
            return _tool_error_payload(tool.name, kwargs, exc)

    wrapper.__name__ = _tool_function_name(tool.name)
    wrapper.__qualname__ = wrapper.__name__
    wrapper.__doc__ = tool.description
    try:
        wrapper.__signature__ = inspect.signature(original)
    except (TypeError, ValueError):
        pass
    annotations = getattr(original, "__annotations__", None)
    if isinstance(annotations, dict):
        wrapper.__annotations__ = dict(annotations)
    return wrapper


def _json_safe_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (str, int)):
        return value
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, list):
        return [_json_safe_value(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe_value(item) for item in value]
    if isinstance(value, dict):
        return {str(k): _json_safe_value(v) for k, v in value.items()}
    model_dump = getattr(value, "model_dump", None)
    if callable(model_dump):
        return _json_safe_value(model_dump(mode="json"))
    serializable = _to_serializable_dict(value)
    if serializable is not None:
        return {str(k): _json_safe_value(v) for k, v in serializable.items()}
    float_value = getattr(value, "__float__", None)
    if callable(float_value):
        try:
            coerced = float(value)
        except Exception:
            pass
        else:
            return coerced if math.isfinite(coerced) else None
    return value


def _to_serializable_dict(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    if hasattr(value, "items"):
        try:
            return dict(value.items())
        except Exception:
            return None
    data = getattr(value, "__dict__", None)
    if isinstance(data, dict) and data:
        return data
    return None


def _extract_structured_content(result: Any) -> dict[str, Any]:
    if isinstance(result, dict):
        structured = result.get("structuredContent") or result.get("structured_content") or result.get("output") or result.get("result")
        if isinstance(structured, dict):
            return structured
        content = result.get("content")
        if isinstance(content, Sequence):
            for entry in content:
                if isinstance(entry, dict) and isinstance(entry.get("text"), str):
                    try:
                        parsed = json.loads(entry["text"])
                    except json.JSONDecodeError:
                        continue
                    if isinstance(parsed, dict):
                        return parsed
        return result
    structured_content = getattr(result, "structured_content", None)
    if isinstance(structured_content, dict):
        return structured_content
    return {"value": structured_content or result}


def _quiet_backtest_logs_enabled() -> bool:
    return (
        str(os.environ.get("IS_BACKTESTING", "")).strip().lower() == "true"
        and str(os.environ.get("BACKTESTING_QUIET_LOGS", "")).strip().lower() in {"1", "true", "yes", "on"}
    )


@contextlib.contextmanager
def _mcp_errlog_stream():
    if _quiet_backtest_logs_enabled():
        with open(os.devnull, "w", encoding="utf-8") as devnull:
            yield devnull
        return
    yield sys.stderr


def _extract_tool_text(response: Any) -> list[str]:
    if response is None:
        return []
    if isinstance(response, dict):
        content = response.get("content") or response.get("contents")
    else:
        content = getattr(response, "content", None) or getattr(response, "contents", None)
    if not content:
        return []
    chunks: list[str] = []
    for entry in content:
        text_value: str | None = None
        if isinstance(entry, str):
            text_value = entry
        elif isinstance(entry, dict):
            text_value = entry.get("text")
        else:
            text_value = getattr(entry, "text", None)
        if isinstance(text_value, str) and text_value.strip():
            chunks.append(text_value.strip())
    return chunks


def _coerce_usage_metadata(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    data = getattr(value, "__dict__", None)
    if isinstance(data, dict):
        return {str(k): v for k, v in data.items()}
    return None


def _normalize_event(event: Any) -> list[AgentTraceEvent]:
    normalized: list[AgentTraceEvent] = []
    parts = getattr(getattr(event, "content", None), "parts", None) or []
    for part in parts:
        if getattr(part, "thought", None) is True:
            thought_text = getattr(part, "text", None)
            if isinstance(thought_text, str) and thought_text.strip():
                normalized.append(
                    AgentTraceEvent(
                        kind="thinking",
                        text=thought_text.strip(),
                        payload={"source": "model_thought"},
                    )
                )
                continue

        text = getattr(part, "text", None)
        if isinstance(text, str) and text.strip():
            normalized.append(AgentTraceEvent(kind="text", text=text.strip()))

        function_call = getattr(part, "function_call", None)
        if function_call and getattr(function_call, "name", None):
            payload = _to_serializable_dict(getattr(function_call, "args", None)) or _to_serializable_dict(
                getattr(function_call, "arguments", None)
            )
            normalized.append(
                AgentTraceEvent(
                    kind="tool_call",
                    tool_name=str(function_call.name),
                    payload=payload,
                )
            )

        function_response = getattr(part, "function_response", None)
        if function_response and getattr(function_response, "name", None):
            tool_name = str(function_response.name)
            for chunk in _extract_tool_text(function_response.response):
                normalized.append(AgentTraceEvent(kind="text", text=chunk, tool_name=tool_name))
            normalized.append(
                AgentTraceEvent(
                    kind="tool_result",
                    tool_name=tool_name,
                    payload=_extract_structured_content(function_response.response or {}),
                )
            )

    usage_payload = _coerce_usage_metadata(getattr(event, "usage_metadata", None))
    if usage_payload:
        normalized.append(AgentTraceEvent(kind="usage", payload=usage_payload))
    return normalized


@dataclass
class RuntimeRequest:
    agent_name: str
    model: str
    system_prompt: str
    task_prompt: str | None
    context: dict[str, Any] | None
    runtime_context: dict[str, Any] | None
    memory_notes: list[dict[str, Any]]
    bound_tools: list[BoundTool]
    provider_prompt_cache_key: str | None = None


_LITELLM_CONFIGURED = False


# Error classification for AI agent calls.
#
# The taxonomy has five buckets. Scope: AI agent calls only. The rest of
# LumiBot's error handling (strategy_executor, brokers, data sources) is
# unchanged. See `AgentHandle.run()` and `docsrc/agents.rst` for how these
# buckets map to backtest-vs-live behavior.
#
#   "auth"      : missing/invalid API key, permission denied (401, 403)
#   "config"    : bad model id, malformed prompt, context-window exceeded,
#                 invalid payload (400, 404, 422)
#   "billing"   : out of credits, payment required, quota exhausted (402,
#                 429 + "insufficient_quota", 403 + billing/credits msg)
#   "transient" : 5xx, rate-limit bursts, timeouts, connection errors
#   "unknown"   : anything not matched above; treated as transient (safe default)

_ERROR_CLASS_AUTH = (
    "AuthenticationError",
    "PermissionDeniedError",
    "UnauthenticatedError",
    "NotAuthorized",
)
_ERROR_CLASS_CONFIG = (
    "BadRequestError",
    "NotFoundError",
    "UnprocessableEntityError",
    "ContextWindowExceededError",
    "ContentPolicyViolationError",
    "InvalidRequestError",
    "ImportError",
    "ModuleNotFoundError",
)
_ERROR_CLASS_BILLING = (
    "BillingError",
    "InsufficientQuotaError",
    "PaymentRequiredError",
)
_ERROR_CLASS_TRANSIENT = (
    "APIConnectionError",
    "APIResponseValidationError",
    "APITimeoutError",
    "InternalServerError",
    "ServerError",
    "ServiceUnavailableError",
    "Timeout",
    "TimeoutError",
    "OverloadedError",
    "RateLimitError",
    "ServerDisconnectedError",
    "ReadTimeout",
    "ConnectTimeout",
    "ConnectionError",
)

_BILLING_BODY_KEYWORDS = (
    "insufficient_quota",
    "insufficient funds",
    "no credits",
    "no credit",
    "available credits",
    "out of credits",
    "spending limit",
    "monthly spending limit",
    "billing",
    "payment",
    "purchase those",
    "team doesn't have any credits",
    "team does not have any credits",
    "quota exceeded",
    "exceeded your current quota",
)


def _classify_agent_error(exc: BaseException) -> str:
    """Map an exception raised by the AI agent stack to a bucket.

    See the module-level docstring for the taxonomy. Safe default is
    "unknown" so behavior matches "transient" (retry/skip) when we
    cannot tell — failing closed is the wrong choice for AI errors
    because most truly-unknown failures are transient provider issues.
    """
    exc_name = exc.__class__.__name__
    message = str(exc)
    message_lower = message.lower()

    # HTTP status code if the provider SDK attached one.
    status_code = None
    for attr in ("status_code", "http_status", "code"):
        value = getattr(exc, attr, None)
        if isinstance(value, int):
            status_code = value
            break

    # Body/keyword check for billing — takes precedence over auth because
    # providers often return 401/403 for "no credits" when the key itself
    # is valid (e.g. xAI's "team doesn't have any credits yet").
    if any(kw in message_lower for kw in _BILLING_BODY_KEYWORDS):
        return "billing"

    # Explicit class-name matches (litellm + google-genai + openai + anthropic).
    if exc_name in _ERROR_CLASS_AUTH:
        return "auth"
    if exc_name in _ERROR_CLASS_CONFIG:
        return "config"
    if exc_name in _ERROR_CLASS_BILLING:
        return "billing"
    if exc_name in _ERROR_CLASS_TRANSIENT:
        return "transient"

    # HTTP status code based classification as a fallback.
    if status_code is not None:
        if status_code == 402:
            return "billing"
        if status_code in (401, 403):
            # Already checked billing keywords above; if we got here it's auth.
            return "auth"
        if status_code == 404:
            return "config"
        if status_code in (400, 422):
            return "config"
        if status_code == 429:
            # Rate limits are transient; insufficient_quota already caught above.
            return "transient"
        if 500 <= status_code < 600:
            return "transient"

    # Message substring fallback for providers that don't use standard class names.
    lower_exc = exc_name.lower()
    if any(kw in lower_exc for kw in ("auth", "permission", "unauthorized", "apikey")):
        return "auth"
    if "context" in lower_exc and ("length" in lower_exc or "window" in lower_exc):
        return "config"
    if any(kw in message_lower for kw in ("api key", "apikey", "unauthenticated", "permission denied")):
        return "auth"
    if "invalid model" in message_lower or "model not found" in message_lower:
        return "config"
    if "context length" in message_lower or "context_length" in message_lower or "context window" in message_lower:
        return "config"

    return "unknown"


def _configure_litellm_quietly() -> None:
    # LiteLLM's provider-lookup path in get_llm_provider_logic.py prints a
    # red "Provider List: https://docs.litellm.ai/docs/providers" banner to
    # stderr on internal probes (cost/tokenizer lookups for models not in
    # litellm.model_cost). The banner is purely cosmetic: real failures
    # still raise BadRequestError. New model ids (e.g. gpt-5.4-*, grok-4.20)
    # routinely ship before LiteLLM's static registry catches up, so this
    # banner would fire on every agent call for current-generation models.
    # suppress_debug_info mutes the banner without suppressing exceptions.
    global _LITELLM_CONFIGURED
    if _LITELLM_CONFIGURED:
        return
    try:
        import litellm
    except ImportError:
        _LITELLM_CONFIGURED = True
        return
    try:
        litellm.suppress_debug_info = True
    except Exception:
        pass
    # Provider param compatibility: Google ADK's LiteLlm bridge emits
    # OpenAI-shaped params (e.g. max_completion_tokens). Some providers
    # (xAI, Anthropic, a few others) reject unknown params with
    # UnsupportedParamsError. drop_params makes LiteLLM silently drop
    # params the target provider does not accept instead of failing the
    # call. Affects only unknown kwargs; real errors still propagate.
    try:
        litellm.drop_params = True
    except Exception:
        pass
    # Transient-error retry: rate-limit (429), server errors (500/502/503/529),
    # and brief network blips all happen in normal operation. LiteLLM has
    # provider-aware retry logic (exponential backoff, 429 Retry-After
    # awareness). Enable it at the library level so every provider benefits.
    # Does NOT retry 4xx client errors (auth, invalid model, context length).
    try:
        litellm.num_retries = 3
    except Exception:
        pass
    _LITELLM_CONFIGURED = True


def _sync_xai_api_key_alias() -> None:
    """Allow Grok users to provide either the vendor key name or product name.

    LiteLLM's xAI provider reads XAI_API_KEY. LumiBot's older Grok helper also
    accepts GROK_API_KEY, so mirror GROK_API_KEY into XAI_API_KEY for xai/ models
    when the canonical xAI env var is absent.
    """
    if not os.environ.get("XAI_API_KEY") and os.environ.get("GROK_API_KEY"):
        os.environ["XAI_API_KEY"] = os.environ["GROK_API_KEY"]


def _sync_gemini_api_key_alias() -> None:
    """Allow product-facing GEMINI_API_KEY with Google SDK internals.

    Google examples and some SDK paths use GOOGLE_API_KEY. LumiBot's public
    docs use GEMINI_API_KEY, so mirror it when the Google name is absent.
    """
    if not os.environ.get("GOOGLE_API_KEY") and os.environ.get("GEMINI_API_KEY"):
        os.environ["GOOGLE_API_KEY"] = os.environ["GEMINI_API_KEY"]


def _provider_prompt_cache_key(request: RuntimeRequest) -> str:
    """Stable provider-routing key for server-side prompt caches.

    This is not LumiBot's replay cache key. It intentionally excludes the
    changing market context so providers can reuse the static prefix
    (system prompt + tool declarations) while still computing each new bar.
    """
    payload = {
        "agent": request.agent_name,
        "model": request.model,
        "system_prompt": request.system_prompt,
        "tools": [
            {
                "name": tool.name,
                "description": tool.description,
                "source": tool.source,
                "metadata": tool.metadata,
            }
            for tool in request.bound_tools
        ],
    }
    digest = hashlib.sha256(json.dumps(_json_safe_value(payload), sort_keys=True).encode("utf-8")).hexdigest()
    return f"lumibot:{request.agent_name}:{digest[:32]}"


def _resolve_model_for_adk(model: Any, *, prompt_cache_key: str | None = None) -> Any:
    # Native Gemini IDs take ADK's fast path as plain strings. Any other
    # provider prefix (e.g. "openai/...", "xai/...", "anthropic/...") is
    # routed through google.adk.models.lite_llm.LiteLlm which normalizes
    # tool-call shapes and auth across ~100 providers via LiteLLM.
    if not isinstance(model, str):
        return model
    lower = model.strip().lower()
    if lower.startswith("gemini-") or lower.startswith("models/gemini"):
        _sync_gemini_api_key_alias()
        return model
    if lower.startswith("xai/"):
        _sync_xai_api_key_alias()
    _configure_litellm_quietly()
    try:
        from google.adk.models.lite_llm import LiteLlm
    except ImportError as exc:
        raise ImportError(
            f"Agent model '{model}' requires the 'litellm' package. "
            "Install it with: pip install litellm"
        ) from exc
    kwargs: dict[str, Any] = {}
    if prompt_cache_key:
        if lower.startswith("openai/"):
            # OpenAI prompt caching is automatic for long shared prefixes. The
            # key improves routing stability and 24h is the documented maximum
            # extended retention value.
            kwargs["prompt_cache_key"] = prompt_cache_key
            kwargs["prompt_cache_retention"] = "24h"
        elif lower.startswith("xai/"):
            # xAI recommends x-grok-conv-id for Chat Completions cache routing.
            kwargs["headers"] = {"x-grok-conv-id": prompt_cache_key}
    return LiteLlm(model=model, **kwargs)


class GoogleADKRuntime:
    def __init__(self, mcp_servers: list[MCPServer] | None = None) -> None:
        self.mcp_servers = mcp_servers or []
        self._llm_agent_type: type[Any] | None = None
        self._runner_type: type[Any] | None = None
        self._genai_types: Any = None
        self._function_tool_type: Any = None
        self._google_genai_types: Any = None

    def _ensure_adk(self) -> tuple[type[Any], type[Any], Any, Any]:
        _configure_google_sdk_noise_filters()
        if (
            self._llm_agent_type is not None
            and self._runner_type is not None
            and self._genai_types is not None
            and self._function_tool_type is not None
        ):
            return self._llm_agent_type, self._runner_type, self._genai_types, self._function_tool_type

        llm_agent_module = importlib.import_module("google.adk.agents.llm_agent")
        runners_module = importlib.import_module("google.adk.runners")
        function_tool_module = importlib.import_module("google.adk.tools.function_tool")
        from google.genai import types as google_genai_types

        self._llm_agent_type = llm_agent_module.LlmAgent
        self._runner_type = runners_module.InMemoryRunner
        self._function_tool_type = getattr(function_tool_module, "FunctionTool")
        self._genai_types = google_genai_types
        self._google_genai_types = google_genai_types
        return self._llm_agent_type, self._runner_type, self._genai_types, self._function_tool_type

    @staticmethod
    def _maybe_build_gemini_thinking_planner(model: Any, genai_types: Any) -> Any | None:
        """Gemini 3 thought text only shows up when ADK thinking is enabled via
        BuiltInPlanner, not GenerateContentConfig.

        BotSpot already uses this path successfully. LumiBot originally tried to
        set `ThinkingConfig(include_thoughts=True)` inside
        `GenerateContentConfig`, which still yielded thought token counts but not
        explicit thought parts in normalized events. This helper mirrors the
        BotSpot pattern so real thought parts can flow through `_normalize_event`.
        """
        if not isinstance(model, str):
            return None
        lower_model = model.strip().lower()
        if not lower_model.startswith("gemini-3"):
            return None
        thinking_config_type = getattr(genai_types, "ThinkingConfig", None)
        if thinking_config_type is None:
            return None
        try:
            planners_module = importlib.import_module("google.adk.planners")
        except ImportError:
            return None
        planner_type = getattr(planners_module, "BuiltInPlanner", None)
        if planner_type is None:
            return None
        try:
            thinking_config = thinking_config_type(include_thoughts=True)
            return planner_type(thinking_config=thinking_config)
        except Exception:
            return None

    def _instruction_for(self, request: RuntimeRequest) -> str:
        lines = [request.system_prompt.strip()]
        lines.append("")
        lines.append("General rules:")
        lines.append("- Use tools for structured data and trading actions.")
        lines.append("- Use DuckDB for time-series analysis when historical tables are available.")
        lines.append("- Return a short final summary after you finish using tools.")
        if request.memory_notes:
            lines.append("")
            lines.append("Persistent memory from earlier runs:")
            for note in request.memory_notes[-5:]:
                timestamp = note.get("timestamp") or "unknown_time"
                summary = note.get("summary") or ""
                lines.append(f"- {timestamp}: {summary}")
        return "\n".join(lines).strip()

    def _build_user_text(self, request: RuntimeRequest) -> str:
        sections: list[str] = []
        if request.runtime_context:
            sections.append(
                f"Runtime Context JSON:\n{json.dumps(_json_safe_value(request.runtime_context), sort_keys=True, default=str)}"
            )
        if request.task_prompt:
            sections.append(f"Task:\n{request.task_prompt.strip()}")
        if request.context:
            sections.append(f"User Context JSON:\n{json.dumps(_json_safe_value(request.context), sort_keys=True, default=str)}")
        if not sections:
            sections.append("Task:\nDo your normal job for the current market state.")
        return "\n\n".join(sections)

    async def _run_async(self, request: RuntimeRequest) -> AgentRunResult:
        started_at = _utc_iso_timestamp()
        started_perf = time.perf_counter()
        first_event_at: str | None = None
        first_event_perf: float | None = None
        LlmAgentType, InMemoryRunnerType, genai_types, function_tool_type = self._ensure_adk()
        tool_name_map = {_tool_function_name(tool.name): tool.name for tool in request.bound_tools}
        tools = [function_tool_type(_wrap_tool_callable(tool)) for tool in request.bound_tools]
        config_kwargs: dict[str, Any] = {
            "temperature": 0.0,
            "max_output_tokens": 65535,
        }
        planner = self._maybe_build_gemini_thinking_planner(request.model, genai_types)
        agent = LlmAgentType(
            name=request.agent_name,
            model=_resolve_model_for_adk(
                request.model,
                prompt_cache_key=request.provider_prompt_cache_key or _provider_prompt_cache_key(request),
            ),
            instruction=self._instruction_for(request),
            tools=tools,
            generate_content_config=genai_types.GenerateContentConfig(**config_kwargs),
            planner=planner,
        )
        runner = InMemoryRunnerType(agent=agent, app_name="lumibot-agents")
        session_id = str(uuid4())
        user_id = "lumibot-user"
        await runner.session_service.create_session(
            app_name=runner.app_name,
            user_id=user_id,
            session_id=session_id,
        )
        content = genai_types.Content(
            role="user",
            parts=[genai_types.Part(text=self._build_user_text(request))],
        )
        events: list[AgentTraceEvent] = []
        async for event in runner.run_async(user_id=user_id, session_id=session_id, new_message=content):
            normalized_events = _normalize_event(event)
            if normalized_events and first_event_perf is None:
                first_event_perf = time.perf_counter()
                first_event_at = _utc_iso_timestamp()
            timestamp = _utc_iso_timestamp()
            for normalized_event in normalized_events:
                normalized_event.timestamp = timestamp
            events.extend(normalized_events)
        for event in events:
            if event.tool_name:
                event.tool_name = tool_name_map.get(event.tool_name, event.tool_name)
        summary = None
        text_chunks = [event.text for event in events if event.kind == "text" and event.text]
        if text_chunks:
            summary = text_chunks[-1]
        usage = None
        for event in reversed(events):
            if event.kind == "usage":
                usage = event.payload
                break
        ended_at = _utc_iso_timestamp()
        ended_perf = time.perf_counter()
        return AgentRunResult(
            summary=summary,
            model=request.model,
            events=events,
            usage=usage,
            started_at=started_at,
            first_event_at=first_event_at,
            ended_at=ended_at,
            latency_ms=max(int((ended_perf - started_perf) * 1000), 0),
            first_event_latency_ms=(
                max(int((first_event_perf - started_perf) * 1000), 0)
                if first_event_perf is not None
                else None
            ),
        )

    # Transient-error retry policy for the full agent call. Covers both the
    # Gemini-native path (google-genai exceptions) and the LiteLlm path
    # (network/timeouts below LiteLLM's own retry layer). LiteLLM already
    # retries individual HTTP calls 3x; this outer retry handles whole-run
    # failures like session setup errors, ADK runner glitches, and anything
    # else that bubbles up.
    #
    # Retry schedule: 10 attempts with per-step backoff capped at 60s so no
    # single wait exceeds one minute. Total budget ~= 5 minutes across all
    # attempts. Prefer more small tries over a few long ones — most cloud
    # provider 5xx storms clear within seconds or low-minutes, and a 5-min
    # budget covers the common case without leaving a live bot frozen for
    # 10 minutes on a single call. If the provider is still down after
    # this budget, the strategy-level safety net (in manager.py's
    # AgentHandle.run) catches the failure and skips this iteration so
    # the strategy stays alive and retries on the next bar.
    _MAX_RUN_ATTEMPTS = 10
    _RETRY_BACKOFF_SECONDS = (2.0, 3.0, 5.0, 10.0, 20.0, 30.0, 45.0, 60.0, 60.0, 60.0)

    @staticmethod
    def _is_non_retryable(exc: BaseException) -> bool:
        # Use the shared classifier: only transient and unknown errors retry.
        # auth / config / billing surface immediately so we don't waste ~5
        # minutes of retry budget on a wrong API key.
        return _classify_agent_error(exc) not in ("transient", "unknown")

    def run(self, request: RuntimeRequest) -> AgentRunResult:
        import time as _time

        last_exc: BaseException | None = None
        for attempt in range(1, self._MAX_RUN_ATTEMPTS + 1):
            try:
                return asyncio.run(self._run_async(request))
            except (KeyboardInterrupt, SystemExit):
                raise
            except BaseException as exc:  # noqa: BLE001 - intentional broad catch for retry
                last_exc = exc
                if self._is_non_retryable(exc):
                    raise
                if attempt >= self._MAX_RUN_ATTEMPTS:
                    break
                delay = self._RETRY_BACKOFF_SECONDS[min(attempt - 1, len(self._RETRY_BACKOFF_SECONDS) - 1)]
                try:
                    sys.stderr.write(
                        f"[lumibot.agents] transient error on attempt {attempt}/{self._MAX_RUN_ATTEMPTS} "
                        f"for model={request.model!r}: {exc.__class__.__name__}: {str(exc)[:240]}. "
                        f"Retrying in {delay:.0f}s...\n"
                    )
                    sys.stderr.flush()
                except Exception:
                    pass
                _time.sleep(delay)
        assert last_exc is not None
        raise last_exc


class StubAgentRuntime:
    def __init__(self, scripted_events: list[dict[str, Any]] | None = None) -> None:
        self.scripted_events = scripted_events or []

    def run(self, request: RuntimeRequest) -> AgentRunResult:
        if self.scripted_events:
            events = [
                AgentTraceEvent(
                    kind=event["kind"],
                    text=event.get("text"),
                    tool_name=event.get("tool_name"),
                    payload=event.get("payload"),
                    timestamp=event.get("timestamp") or _utc_iso_timestamp(),
                )
                for event in self.scripted_events
            ]
            summary = next((event.text for event in reversed(events) if event.kind == "text" and event.text), None)
            return AgentRunResult(summary=summary, model=request.model, events=events)

        events: list[AgentTraceEvent] = []
        if request.context is not None:
            events.append(
                AgentTraceEvent(
                    kind="thinking",
                    text="Stub runtime inspected the provided context.",
                    timestamp=_utc_iso_timestamp(),
                )
            )
        if request.bound_tools:
            first_tool = request.bound_tools[0]
            tool_result = first_tool.function() if callable(first_tool.function) else None
            events.append(
                AgentTraceEvent(
                    kind="tool_call",
                    tool_name=first_tool.name,
                    payload={},
                    timestamp=_utc_iso_timestamp(),
                )
            )
            payload = tool_result if isinstance(tool_result, dict) else {"value": tool_result}
            events.append(
                AgentTraceEvent(
                    kind="tool_result",
                    tool_name=first_tool.name,
                    payload=payload,
                    timestamp=_utc_iso_timestamp(),
                )
            )
        summary = "Stub agent completed run."
        events.append(
            AgentTraceEvent(
                kind="text",
                text=summary,
                timestamp=_utc_iso_timestamp(),
            )
        )
        return AgentRunResult(summary=summary, model=request.model, events=events)


def list_mcp_tools(server: MCPServer) -> list[dict[str, Any]]:
    return _run_mcp_sync(_list_mcp_tools_async, server)


def call_mcp_tool(server: MCPServer, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    return _run_mcp_sync(_call_mcp_tool_async, server, name, arguments)


def _mcp_headers(server: MCPServer) -> dict[str, str]:
    headers = {"Accept": "application/json, text/event-stream"}
    if server.headers:
        headers.update(server.headers)
    if server.auth_token_env:
        import os

        token = os.environ.get(server.auth_token_env)
        if token:
            headers["Authorization"] = f"Bearer {token}"
    return headers


def _jsonable(value: Any) -> Any:
    value = _json_safe_value(value)
    if value is None:
        return None
    if isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    return str(value)


async def _with_mcp_session(server: MCPServer, callback):
    transport = (server.transport or "http").lower().replace("-", "_")
    if transport == "stdio":
        parameters = StdioServerParameters(
            command=str(server.command),
            args=list(server.args or []),
            env=dict(server.env) if server.env else None,
            cwd=server.cwd,
        )
        with _mcp_errlog_stream() as errlog:
            async with stdio_client(parameters, errlog=errlog) as (read_stream, write_stream):
                async with ClientSession(read_stream, write_stream) as session:
                    await session.initialize()
                    return await callback(session)

    headers = _mcp_headers(server)
    timeout = server.timeout_seconds
    sse_timeout = server.sse_read_timeout_seconds
    async with streamablehttp_client(
        str(server.url),
        headers=headers,
        timeout=timeout,
        sse_read_timeout=sse_timeout,
        terminate_on_close=server.terminate_on_close,
    ) as (read_stream, write_stream, _get_session_id):
        async with ClientSession(read_stream, write_stream) as session:
            await session.initialize()
            return await callback(session)


def _run_mcp_sync(async_fn, *args):
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return anyio_run(async_fn, *args)
    with start_blocking_portal() as portal:
        return portal.call(async_fn, *args)


async def _list_mcp_tools_async(server: MCPServer) -> list[dict[str, Any]]:
    transport = (server.transport or "http").lower().replace("-", "_")
    async def callback(session: ClientSession) -> list[dict[str, Any]]:
        result = await session.list_tools()
        tools = getattr(result, "tools", None) or []
        normalized: list[dict[str, Any]] = []
        for tool in tools:
            dumped = _jsonable(tool)
            if isinstance(dumped, dict):
                normalized.append(dumped)
        return normalized

    if transport == "http":
        return await _legacy_http_list_tools(server)
    return await _with_mcp_session(server, callback)


async def _call_mcp_tool_async(server: MCPServer, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    transport = (server.transport or "http").lower().replace("-", "_")
    async def callback(session: ClientSession) -> dict[str, Any]:
        result = await session.call_tool(name, arguments or {})
        dumped = _jsonable(result)
        if not isinstance(dumped, dict):
            raise RuntimeError(f"{name} returned unexpected payload: {dumped!r}")
        if dumped.get("isError") is True:
            raise RuntimeError(f"{name} failed: {dumped}")
        return dumped

    if transport == "http":
        return await _legacy_http_call_tool(server, name, arguments)
    return await _with_mcp_session(server, callback)


async def _legacy_http_list_tools(server: MCPServer) -> list[dict[str, Any]]:
    payload = {
        "jsonrpc": "2.0",
        "id": "tools-list",
        "method": "tools/list",
        "params": {},
    }
    async with httpx.AsyncClient(timeout=server.timeout_seconds) as client:
        response = await client.post(str(server.url), json=payload, headers=_mcp_headers(server))
        response.raise_for_status()
        data = response.json()
    result = data.get("result") or {}
    tools = result.get("tools") or []
    return tools if isinstance(tools, list) else []


async def _legacy_http_call_tool(server: MCPServer, name: str, arguments: dict[str, Any]) -> dict[str, Any]:
    payload = {
        "jsonrpc": "2.0",
        "id": f"{name}-call",
        "method": "tools/call",
        "params": {"name": name, "arguments": arguments},
    }
    async with httpx.AsyncClient(timeout=server.timeout_seconds) as client:
        response = await client.post(str(server.url), json=payload, headers=_mcp_headers(server))
        response.raise_for_status()
        data = response.json()
    if "error" in data:
        raise RuntimeError(f"{name} failed: {data['error']}")
    result = data.get("result") or {}
    if not isinstance(result, dict):
        raise RuntimeError(f"{name} returned unexpected payload: {result!r}")
    return result
