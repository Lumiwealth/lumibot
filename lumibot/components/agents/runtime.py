from __future__ import annotations

import contextlib
import asyncio
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
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from .schemas import AgentRunResult, AgentTraceEvent, BoundTool, MCPServer
from .tool_context import agent_tool_context


_GOOGLE_SDK_NOISE_FILTERS_CONFIGURED = False
ClientSession = None
StdioServerParameters = None
stdio_client = None
streamablehttp_client = None
streamablehttp_client_uses_http_client = False


def _ensure_mcp_client_imports():
    global ClientSession, StdioServerParameters, stdio_client
    global streamablehttp_client, streamablehttp_client_uses_http_client
    if ClientSession is None or StdioServerParameters is None:
        from mcp import ClientSession as _ClientSession, StdioServerParameters as _StdioServerParameters

        ClientSession = _ClientSession
        StdioServerParameters = _StdioServerParameters
    if stdio_client is None:
        from mcp.client.stdio import stdio_client as _stdio_client

        stdio_client = _stdio_client
    if streamablehttp_client is None:
        try:
            from mcp.client.streamable_http import streamable_http_client as _streamablehttp_client
            streamablehttp_client_uses_http_client = True
        except ImportError:
            from mcp.client.streamable_http import streamablehttp_client as _streamablehttp_client

        streamablehttp_client = _streamablehttp_client


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


def _tool_name_space_aliases(canonical_name: str) -> list[str]:
    """Common LLM typos: a space after an underscore in the tool name.

    Example: options_find_expiration -> options_find_ expiration
    """
    parts = [part for part in str(canonical_name or "").split("_") if part != ""]
    if len(parts) < 2:
        return []
    aliases: list[str] = []
    for index in range(1, len(parts)):
        alias = "_".join(parts[:index]) + "_ " + "_".join(parts[index:])
        if alias and alias != canonical_name and alias not in aliases:
            aliases.append(alias)
    return aliases


def _clone_tool_callable(wrapper: Any, name: str) -> Any:
    def alias(*args, **kwargs):
        return wrapper(*args, **kwargs)

    alias.__name__ = name
    alias.__qualname__ = name
    alias.__doc__ = getattr(wrapper, "__doc__", None)
    signature = getattr(wrapper, "__signature__", None)
    if signature is not None:
        alias.__signature__ = signature
    annotations = getattr(wrapper, "__annotations__", None)
    if isinstance(annotations, dict):
        alias.__annotations__ = dict(annotations)
    return alias


def _wrap_tool_callable(tool: BoundTool, tool_context: dict[str, Any] | None = None):
    original = tool.function

    def wrapper(*args, **kwargs):
        result: Any
        try:
            with agent_tool_context(tool_context):
                result = _json_safe_value(original(*args, **kwargs))
        except Exception as exc:
            result = _tool_error_payload(tool.name, kwargs, exc)
        if isinstance(tool_context, dict):
            calls = tool_context.setdefault("tool_calls", [])
            if isinstance(calls, list):
                calls.append(
                    {
                        "tool_name": tool.name,
                        "arguments": _json_safe_value(dict(kwargs or {})),
                        "ok": not (isinstance(result, dict) and result.get("tool_error") is True),
                    }
                )
        return result

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


def _is_provider_safe_function_name(name: str) -> bool:
    """Gemini function_declarations reject spaces and most punctuation."""
    text = str(name or "")
    if not text or len(text) > 128:
        return False
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_.:-]*$", text):
        return False
    return True


def _normalize_tool_name_typo(name: str) -> str:
    """Collapse common LLM typos such as a space after an underscore."""
    text = str(name or "").strip()
    if not text:
        return text
    # options_find_ expiration -> options_find_expiration
    collapsed = re.sub(r"_ +", "_", text)
    collapsed = re.sub(r" +_", "_", collapsed)
    collapsed = re.sub(r"\s+", "", collapsed)
    return collapsed


def _function_tools_with_name_aliases(function_tool_type: Any, bound_tools: Sequence[BoundTool], tool_context: dict[str, Any] | None = None) -> list[Any]:
    """Register only provider-safe canonical tool names.

    Space-after-underscore typos are tolerated by normalizing inbound tool names
    (see ``_normalize_tool_name_typo``). They must not be registered as Gemini
    ``function_declarations`` because names with spaces are rejected with 400
    INVALID_ARGUMENT and abort the entire agent run.
    """
    tools: list[Any] = []
    seen_names: set[str] = set()
    for bound in bound_tools:
        wrapper = _wrap_tool_callable(bound, tool_context)
        canonical = wrapper.__name__
        if canonical in seen_names or not _is_provider_safe_function_name(canonical):
            continue
        tools.append(function_tool_type(wrapper))
        seen_names.add(canonical)
    return tools


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
    if isinstance(value, UUID):
        return str(value)
    if isinstance(value, Enum):
        return _json_safe_value(value.value)
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
    return str(value)


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


def _aggregate_usage_metadata(payloads: list[dict[str, Any]]) -> dict[str, Any] | None:
    if not payloads:
        return None
    if len(payloads) == 1:
        return payloads[0]

    aggregate: dict[str, Any] = dict(payloads[-1])
    additive_keys = {
        "cached_content_token_count",
        "cached_input_tokens",
        "cached_prompt_tokens",
        "cached_tokens",
        "cache_creation_input_tokens",
        "cache_read_input_tokens",
        "candidates_token_count",
        "completion_tokens",
        "prompt_token_count",
        "prompt_cache_hit_tokens",
        "prompt_cache_miss_tokens",
        "prompt_tokens",
        "reasoning_tokens",
        "thoughts_token_count",
        "tool_use_prompt_token_count",
        "total_token_count",
        "input_tokens",
        "output_tokens",
        "total_tokens",
    }
    for key in additive_keys:
        total = 0
        seen = False
        for payload in payloads:
            value = payload.get(key)
            if isinstance(value, bool):
                continue
            if isinstance(value, (int, float)):
                total += int(value)
                seen = True
        if seen:
            aggregate[key] = total
    aggregate["aggregated_usage_event_count"] = len(payloads)
    return aggregate


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
    memory_state: dict[str, Any] | None
    memory_notes: list[dict[str, Any]]
    bound_tools: list[BoundTool]
    include_builtin_skills: bool = True
    builtin_skill_fingerprint: str | None = None
    model_call_id: str | None = None
    provider_prompt_cache_key: str | None = None
    model_request_timeout_seconds: float | None = None
    run_timeout_seconds: float | None = None
    max_output_tokens: int | None = None


_LITELLM_CONFIGURED = False


def _coerce_positive_timeout_seconds(value: Any) -> float | None:
    if value is None:
        return None
    try:
        timeout_seconds = float(value)
    except Exception:
        return None
    return timeout_seconds if timeout_seconds > 0 else None


def _parse_timeout_seconds(value: Any) -> tuple[bool, float | None]:
    if value is None:
        return False, None
    try:
        timeout_seconds = float(value)
    except Exception:
        return False, None
    return True, timeout_seconds if timeout_seconds > 0 else None


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
    if "not json serializable" in message_lower or "not json-serializable" in message_lower:
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


def _sync_together_api_key_alias() -> None:
    """Allow either Together's SDK key name or LiteLLM's provider key name.

    Together's own examples commonly use TOGETHER_API_KEY, while LiteLLM's
    Together provider reads TOGETHERAI_API_KEY. Mirror in both directions so
    users can set either one.
    """
    if not os.environ.get("TOGETHERAI_API_KEY") and os.environ.get("TOGETHER_API_KEY"):
        os.environ["TOGETHERAI_API_KEY"] = os.environ["TOGETHER_API_KEY"]
    if not os.environ.get("TOGETHER_API_KEY") and os.environ.get("TOGETHERAI_API_KEY"):
        os.environ["TOGETHER_API_KEY"] = os.environ["TOGETHERAI_API_KEY"]


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
        "builtin_skill_fingerprint": request.builtin_skill_fingerprint,
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


DEFAULT_MODEL_CONTEXT_LIMIT_TOKENS = 1_000_000
DEFAULT_MODEL_CONTEXT_STRING_LIMIT_CHARS = 20_000

MODEL_CONTEXT_LIMIT_PREFIXES: tuple[tuple[str, int, int], ...] = (
    ("anthropic/claude-", 200_000, DEFAULT_MODEL_CONTEXT_STRING_LIMIT_CHARS),
    ("claude-", 200_000, DEFAULT_MODEL_CONTEXT_STRING_LIMIT_CHARS),
    ("deepseek/deepseek-v4-", 1_048_576, DEFAULT_MODEL_CONTEXT_STRING_LIMIT_CHARS),
    ("gemini-3.1", 1_048_576, DEFAULT_MODEL_CONTEXT_STRING_LIMIT_CHARS),
    ("gemini-2.5", 1_048_576, DEFAULT_MODEL_CONTEXT_STRING_LIMIT_CHARS),
    ("gemini-1.5", 1_048_576, DEFAULT_MODEL_CONTEXT_STRING_LIMIT_CHARS),
    ("openai/gpt-4.1", 1_047_576, DEFAULT_MODEL_CONTEXT_STRING_LIMIT_CHARS),
    ("gpt-4.1", 1_047_576, DEFAULT_MODEL_CONTEXT_STRING_LIMIT_CHARS),
    ("xai/grok-4.20", 2_000_000, DEFAULT_MODEL_CONTEXT_STRING_LIMIT_CHARS),
    ("grok-4.20", 2_000_000, DEFAULT_MODEL_CONTEXT_STRING_LIMIT_CHARS),
)


def _positive_int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        parsed = int(str(raw).strip())
    except (TypeError, ValueError):
        return default
    return parsed if parsed > 0 else default


def _model_context_limit_entry(model: Any) -> tuple[int, int] | None:
    if not isinstance(model, str):
        return None
    lower = model.strip().lower()
    if not lower:
        return None
    for prefix, token_limit, string_limit in MODEL_CONTEXT_LIMIT_PREFIXES:
        if lower.startswith(prefix):
            return token_limit, string_limit
    return (
        _positive_int_env("LUMIBOT_AGENT_DEFAULT_CONTEXT_LIMIT_TOKENS", DEFAULT_MODEL_CONTEXT_LIMIT_TOKENS),
        _positive_int_env("LUMIBOT_AGENT_DEFAULT_CONTEXT_STRING_LIMIT_CHARS", DEFAULT_MODEL_CONTEXT_STRING_LIMIT_CHARS),
    )


def _model_context_limit_tokens(model: Any) -> int | None:
    entry = _model_context_limit_entry(model)
    if entry is None:
        return None
    return entry[0]


def _model_context_string_limit_chars(model: Any) -> int | None:
    entry = _model_context_limit_entry(model)
    if entry is None:
        return None
    return entry[1]


def _truncate_preserving_edges(text: str, max_chars: int, *, label: str) -> str:
    if len(text) <= max_chars:
        return text
    head_chars = max(max_chars // 2, 0)
    tail_chars = max(max_chars - head_chars, 0)
    omitted = len(text) - head_chars - tail_chars
    notice = (
        f"\n\n[Lumibot input context pruned {omitted} characters from {label} "
        "to stay within the receiving model context window. Beginning and end "
        "are preserved.]\n\n"
    )
    if len(notice) >= max_chars:
        notice = f"\n[Pruned {omitted} chars from {label}.]\n"
    available = max(max_chars - len(notice), 0)
    head_chars = available // 2
    tail_chars = available - head_chars
    return f"{text[:head_chars]}{notice}{text[-tail_chars:] if tail_chars else ''}"


def _prune_large_context_strings(value: Any, *, max_string_chars: int, path: str = "context") -> tuple[Any, int]:
    if isinstance(value, str):
        if len(value) <= max_string_chars:
            return value, 0
        return _truncate_preserving_edges(value, max_string_chars, label=path), 1
    if isinstance(value, dict):
        pruned = 0
        output: dict[str, Any] = {}
        for key, item in value.items():
            child, child_pruned = _prune_large_context_strings(
                item,
                max_string_chars=max_string_chars,
                path=f"{path}.{key}",
            )
            output[str(key)] = child
            pruned += child_pruned
        return output, pruned
    if isinstance(value, list):
        pruned = 0
        output = []
        for idx, item in enumerate(value):
            child, child_pruned = _prune_large_context_strings(
                item,
                max_string_chars=max_string_chars,
                path=f"{path}[{idx}]",
            )
            output.append(child)
            pruned += child_pruned
        return output, pruned
    return value, 0


def _serialized_content_length(value: Any) -> int:
    try:
        if hasattr(value, "model_dump"):
            return len(json.dumps(_json_safe_value(value.model_dump(mode="json")), sort_keys=True, default=str))
        return len(json.dumps(_json_safe_value(value), sort_keys=True, default=str))
    except Exception:
        return len(repr(value))


def _request_contents_length(contents: list[Any]) -> int:
    return sum(_serialized_content_length(content) for content in contents)


def _part_has_function_response(part: Any) -> bool:
    return getattr(part, "function_response", None) is not None


def _function_response_payload_length(part: Any) -> int:
    function_response = getattr(part, "function_response", None)
    if function_response is None:
        return 0
    return _serialized_content_length(getattr(function_response, "response", None))


def _function_response_payload_is_pruned(part: Any) -> bool:
    function_response = getattr(part, "function_response", None)
    if function_response is None:
        return False
    response = getattr(function_response, "response", None)
    if isinstance(response, dict):
        return response.get("lumibot_context_pruned") is True
    return False


def _replace_function_response_payload(part: Any, message: str) -> bool:
    function_response = getattr(part, "function_response", None)
    if function_response is None:
        return False
    original_response = getattr(function_response, "response", None)
    original_chars = _serialized_content_length(original_response)
    replacement = {
        "lumibot_context_pruned": True,
        "message": message,
    }
    if _serialized_content_length(replacement) >= original_chars:
        return False
    try:
        function_response.response = replacement
        return True
    except Exception:
        return False


def _prune_tool_response_for_context_window(tool_response: Any, *, tool_name: str | None, max_chars: int = 4_000) -> Any | None:
    response_chars = _serialized_content_length(tool_response)
    if response_chars <= max_chars:
        return None
    serialized = json.dumps(_json_safe_value(tool_response), sort_keys=True, default=str)
    return {
        "lumibot_tool_result_pruned": True,
        "tool_name": tool_name,
        "original_chars": response_chars,
        "excerpt": _truncate_preserving_edges(serialized, max_chars, label=f"tool_response.{tool_name or 'unknown'}"),
        "message": (
            "Tool response was shortened by Lumibot before sending it back to this model "
            "because the provider context window would otherwise be exceeded. Call a targeted tool "
            "again if more detail is required."
        ),
    }


def _prune_request_contents_for_context_window(
    contents: list[Any],
    *,
    context_limit_tokens: int,
    reserve_ratio: float = 3.0,
    preserve_recent_tool_results: int = 4,
    always_prune_older_tool_results: bool = False,
) -> dict[str, Any] | None:
    """Trim oversized historical tool results before provider context failure.

    This is input-side context pruning. It leaves the Lumibot system prompt,
    task, tool declarations, function-call sequence, and most recent tool
    results intact. Only older tool-result payloads are replaced, and only when
    the serialized request is already larger than a conservative provider-window
    budget.
    """
    if not contents:
        return None

    # The registry stores provider limits in tokens while this guard only has a
    # cheap serialized-character estimate. Use a conservative character budget
    # instead of pruning at a tiny percentage of the true token window.
    max_chars = int(context_limit_tokens * reserve_ratio)
    before_chars = _request_contents_length(contents)

    tool_response_parts: list[Any] = []
    for content in contents:
        for part in getattr(content, "parts", None) or []:
            if _part_has_function_response(part):
                tool_response_parts.append(part)

    should_prune_for_size = before_chars > max_chars
    should_prune_for_history = always_prune_older_tool_results and len(tool_response_parts) > preserve_recent_tool_results
    if not should_prune_for_size and not should_prune_for_history:
        return None

    if len(tool_response_parts) <= preserve_recent_tool_results:
        return None

    pruned = 0
    replacement_message = (
        "Older tool result omitted by Lumibot before this model call because "
        "the provider context window would otherwise be exceeded. Use the most "
        "recent visible tool results or call a targeted tool again if this older "
        "detail is still required."
    )
    candidates = [
        part
        for part in tool_response_parts[: -preserve_recent_tool_results]
        if not _function_response_payload_is_pruned(part)
    ]
    candidates.sort(key=_function_response_payload_length, reverse=True)
    for part in candidates:
        if should_prune_for_size and _request_contents_length(contents) <= max_chars:
            break
        if _replace_function_response_payload(part, replacement_message):
            pruned += 1

    after_chars = _request_contents_length(contents)
    if pruned <= 0:
        return None
    return {
        "type": "provider_context_pruning",
        "pruned_tool_results": pruned,
        "before_chars": before_chars,
        "after_chars": after_chars,
        "max_chars": max_chars,
    }


def _copy_content_without_thought_parts(content: Any) -> tuple[Any, bool]:
    parts = getattr(content, "parts", None)
    if not parts:
        return content, False
    filtered_parts = [part for part in parts if not getattr(part, "thought", False)]
    if len(filtered_parts) == len(parts):
        return content, False
    if hasattr(content, "model_copy"):
        return content.model_copy(update={"parts": filtered_parts}), True
    try:
        return type(content)(role=getattr(content, "role", None), parts=filtered_parts), True
    except Exception:
        content.parts = filtered_parts
        return content, True


def _strip_thought_parts_from_litellm_request(llm_request: Any) -> None:
    contents = getattr(llm_request, "contents", None)
    if not contents:
        return
    updated = []
    changed = False
    for content in contents:
        clean_content, content_changed = _copy_content_without_thought_parts(content)
        updated.append(clean_content)
        changed = changed or content_changed
    if changed:
        llm_request.contents = updated


def _is_native_gemini_model(model: Any) -> bool:
    if not isinstance(model, str):
        return False
    lower = model.strip().lower()
    return lower.startswith("gemini-") or lower.startswith("models/gemini")


def _resolve_model_for_adk(
    model: Any,
    *,
    prompt_cache_key: str | None = None,
    model_request_timeout_seconds: float | None = None,
) -> Any:
    # Native Gemini IDs take ADK's fast path as plain strings. Any other
    # provider prefix (e.g. "openai/...", "xai/...", "anthropic/...") is
    # routed through google.adk.models.lite_llm.LiteLlm which normalizes
    # tool-call shapes and auth across ~100 providers via LiteLLM.
    if not isinstance(model, str):
        return model
    lower = model.strip().lower()
    if _is_native_gemini_model(model):
        return model
    if lower.startswith("xai/"):
        _sync_xai_api_key_alias()
    if lower.startswith("together_ai/"):
        _sync_together_api_key_alias()
    _configure_litellm_quietly()
    try:
        from google.adk.models.lite_llm import LiteLlm
    except ImportError as exc:
        raise ImportError(
            f"Agent model '{model}' requires the 'litellm' package. "
            "Install it with: pip install 'google-adk[extensions]' litellm"
        ) from exc

    class CerebrasLiteLlm(LiteLlm):
        async def generate_content_async(self, llm_request: Any, stream: bool = False):
            # ADK 2 preserves provider reasoning as Gemini thought parts and
            # LiteLLM serializes those back to `messages.*.reasoning_content`.
            # Cerebras rejects that nonstandard message field, so strip thought
            # parts before request conversion while preserving normal text and
            # tool-call history.
            _strip_thought_parts_from_litellm_request(llm_request)
            async for response in super().generate_content_async(llm_request, stream=stream):
                yield response

    kwargs: dict[str, Any] = {}
    resolved_timeout_seconds = _coerce_positive_timeout_seconds(model_request_timeout_seconds)
    if resolved_timeout_seconds is not None:
        # google.adk.models.lite_llm.LiteLlm forwards additional args to
        # LiteLLM's acompletion call. LiteLLM accepts timeout in seconds.
        kwargs["timeout"] = resolved_timeout_seconds
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
    model_type = CerebrasLiteLlm if lower.startswith("cerebras/") else LiteLlm
    return model_type(model=model, **kwargs)


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
        if request.bound_tools:
            tool_names = ", ".join(sorted(tool.name for tool in request.bound_tools))
            lines.append(f"- Available tool names for this run: {tool_names}.")
            lines.append("- Only call tool names that appear in the available tool list for this run.")
        lines.append("- Use DuckDB for time-series analysis when historical tables are available.")
        lines.append("- Return a short final summary after you finish using tools.")
        return "\n".join(lines).strip()

    def _before_model_context_pruning_callback(self, request: RuntimeRequest):
        context_limit = _model_context_limit_tokens(request.model)
        if not context_limit:
            return None

        def _callback(*args: Any, callback_context: Any = None, llm_request: Any = None, **_kwargs: Any) -> None:
            if llm_request is None and len(args) >= 2:
                llm_request = args[1]
            contents = getattr(llm_request, "contents", None)
            if not isinstance(contents, list):
                return None
            pruning = _prune_request_contents_for_context_window(
                contents,
                context_limit_tokens=context_limit,
            )
            if pruning:
                logging.getLogger(__name__).warning(
                    "Pruned %s older tool result payload(s) for model=%s before provider context window overflow "
                    "(request chars %s -> %s, budget %s).",
                    pruning["pruned_tool_results"],
                    request.model,
                    pruning["before_chars"],
                    pruning["after_chars"],
                    pruning["max_chars"],
                )
            return None

        return _callback

    def _after_tool_context_pruning_callback(self, request: RuntimeRequest):
        if _model_context_limit_tokens(request.model) is None:
            return None

        def _callback(
            *args: Any,
            tool: Any = None,
            tool_response: Any = None,
            **_kwargs: Any,
        ) -> Any | None:
            if tool is None and len(args) >= 1:
                tool = args[0]
            if tool_response is None and len(args) >= 4:
                tool_response = args[3]
            tool_name = str(getattr(tool, "name", None) or "")
            if tool_name in {"list_skills", "load_skill", "load_skill_resource"}:
                return None
            pruned = _prune_tool_response_for_context_window(tool_response, tool_name=tool_name)
            if pruned is not None:
                logging.getLogger(__name__).warning(
                    "Pruned oversized tool response for model=%s tool=%s original_chars=%s.",
                    request.model,
                    tool_name,
                    pruned["original_chars"],
                )
            return pruned

        return _callback

    def _build_user_text(self, request: RuntimeRequest) -> str:
        sections: list[str] = []
        tool_names = {tool.name for tool in request.bound_tools}
        if request.runtime_context:
            sections.append(
                f"Runtime Context JSON:\n{json.dumps(_json_safe_value(request.runtime_context), sort_keys=True, default=str)}"
            )
        if request.bound_tools:
            sections.append(
                "Available Tools JSON:\n"
                f"{json.dumps(sorted(tool_names), sort_keys=True, default=str)}"
            )
        if request.memory_state:
            sections.append(
                "Lumibot Memory State JSON:\n"
                f"{json.dumps(_json_safe_value(request.memory_state), sort_keys=True, default=str)}"
            )
        if request.memory_notes:
            memory_notes = _json_safe_value(request.memory_notes[-5:])
            context_string_limit = _model_context_string_limit_chars(request.model)
            if context_string_limit:
                memory_notes, memory_pruned = _prune_large_context_strings(
                    memory_notes,
                    max_string_chars=max(context_string_limit // 2, 1),
                    path="memory_notes",
                )
                if memory_pruned:
                    sections.append(f"Lumibot Context Notice:\nPruned {memory_pruned} oversized memory string(s).")
            sections.append(
                "Persistent Memory JSON:\n"
                f"{json.dumps(memory_notes, sort_keys=True, default=str)}"
            )
        if request.task_prompt:
            sections.append(f"Task:\n{request.task_prompt.strip()}")
        else:
            required_categories = [
                "account_positions or account_portfolio",
                "market_last_price or market_load_history_table",
                "duckdb_query after loading a price table",
                "get_indicator or get_indicators",
            ]
            if "alpaca_news" in tool_names:
                required_categories.append("alpaca_news")
            fred_tools = sorted(name for name in tool_names if name.startswith("get_fred_") or name == "list_fred_series")
            if fred_tools:
                required_categories.append(" or ".join(fred_tools))
            required_categories.extend(
                [
                    "get_income_statement, get_balance_sheet, get_cash_flow, or get_company_facts",
                    "get_filings, search_filing, or get_filing_document",
                ]
            )
            sections.append(
                "Task:\n"
                "Do your normal job for the current market state. Before making a trading decision, use the available "
                "tools to review account/portfolio state, current market prices, recent price history, technical "
                "indicators, relevant news when configured, macro/FRED data when configured, and SEC financial/filing "
                "evidence for relevant single-stock candidates. Specifically, include calls from these available "
                f"categories: {'; '.join(required_categories)}. "
                "In backtests, date-bound every external data request to the current simulated datetime and do not use "
                "future information."
            )
        if request.context:
            context_payload = _json_safe_value(request.context)
            context_string_limit = _model_context_string_limit_chars(request.model)
            if context_string_limit:
                context_payload, context_pruned = _prune_large_context_strings(
                    context_payload,
                    max_string_chars=context_string_limit,
                    path="context",
                )
                if context_pruned:
                    sections.append(f"Lumibot Context Notice:\nPruned {context_pruned} oversized context string(s).")
            sections.append(f"User Context JSON:\n{json.dumps(context_payload, sort_keys=True, default=str)}")
        return "\n\n".join(sections)

    async def _run_async(self, request: RuntimeRequest) -> AgentRunResult:
        started_at = _utc_iso_timestamp()
        started_perf = time.perf_counter()
        first_event_at: str | None = None
        first_event_perf: float | None = None
        LlmAgentType, InMemoryRunnerType, genai_types, function_tool_type = self._ensure_adk()
        run_config_module = importlib.import_module("google.adk.agents.run_config")
        tool_name_map = {_tool_function_name(tool.name): tool.name for tool in request.bound_tools}
        for canonical, original in list(tool_name_map.items()):
            for alias in _tool_name_space_aliases(canonical):
                tool_name_map.setdefault(alias, original)
                tool_name_map.setdefault(_normalize_tool_name_typo(alias), original)
        active_tool_context = {
            "agent_name": request.agent_name,
            "model_call_id": request.model_call_id,
            "enforce_order_readiness": True,
            "tool_calls": [],
        }
        tools = _function_tools_with_name_aliases(
            function_tool_type,
            request.bound_tools,
            active_tool_context,
        )
        if request.include_builtin_skills:
            from .skills import build_builtin_skill_toolset

            tools.append(build_builtin_skill_toolset())
        config_kwargs = self._generate_content_config_kwargs_for_request(request, genai_types)
        model_request_timeout_seconds = self._model_request_timeout_seconds_for_request(request)
        run_timeout_seconds = self._run_timeout_seconds_for_request(request)
        model_timeout_label = (
            f"{model_request_timeout_seconds:g}s" if model_request_timeout_seconds is not None else "disabled"
        )
        run_timeout_label = f"{run_timeout_seconds:g}s" if run_timeout_seconds is not None else "disabled"
        try:
            sys.stderr.write(
                f"[lumibot.agents] starting agent '{request.agent_name}' "
                f"(model={request.model!r}, model_request_timeout={model_timeout_label}, "
                f"run_timeout={run_timeout_label}).\n"
            )
            sys.stderr.flush()
        except Exception:
            pass
        planner = self._maybe_build_gemini_thinking_planner(request.model, genai_types)
        agent = LlmAgentType(
            name=request.agent_name,
            model=_resolve_model_for_adk(
                request.model,
                prompt_cache_key=request.provider_prompt_cache_key or _provider_prompt_cache_key(request),
                model_request_timeout_seconds=model_request_timeout_seconds,
            ),
            instruction=self._instruction_for(request),
            tools=tools,
            generate_content_config=genai_types.GenerateContentConfig(**config_kwargs),
            planner=planner,
            before_model_callback=self._before_model_context_pruning_callback(request),
            after_tool_callback=self._after_tool_context_pruning_callback(request),
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
        run_config = run_config_module.RunConfig(max_llm_calls=sys.maxsize - 1)
        async for event in runner.run_async(
            user_id=user_id,
            session_id=session_id,
            new_message=content,
            run_config=run_config,
        ):
            normalized_events = _normalize_event(event)
            if normalized_events and first_event_perf is None:
                first_event_perf = time.perf_counter()
                first_event_at = _utc_iso_timestamp()
                try:
                    sys.stderr.write(
                        f"[lumibot.agents] first ADK event for agent '{request.agent_name}' "
                        f"(model={request.model!r}) after "
                        f"{max(int((first_event_perf - started_perf) * 1000), 0)}ms.\n"
                    )
                    sys.stderr.flush()
                except Exception:
                    pass
            timestamp = _utc_iso_timestamp()
            for normalized_event in normalized_events:
                normalized_event.timestamp = timestamp
            events.extend(normalized_events)
        for event in events:
            if event.tool_name:
                mapped = tool_name_map.get(event.tool_name)
                if mapped is None:
                    mapped = tool_name_map.get(_normalize_tool_name_typo(event.tool_name), event.tool_name)
                event.tool_name = mapped
        summary = None
        text_chunks = [event.text for event in events if event.kind == "text" and event.text]
        if text_chunks:
            summary = text_chunks[-1]
        usage = _aggregate_usage_metadata(
            [event.payload for event in events if event.kind == "usage" and isinstance(event.payload, dict)]
        )
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
    _DEFAULT_RUN_TIMEOUT_SECONDS = 1800.0
    _DEFAULT_MODEL_REQUEST_TIMEOUT_SECONDS = 600.0
    _RETRY_BACKOFF_SECONDS = (2.0, 3.0, 5.0, 10.0, 20.0, 30.0, 45.0, 60.0, 60.0, 60.0)

    @staticmethod
    def _model_request_timeout_seconds_for_request(request: RuntimeRequest) -> float | None:
        if request.model_request_timeout_seconds is not None:
            return _coerce_positive_timeout_seconds(request.model_request_timeout_seconds)
        raw = os.environ.get("LUMIBOT_AGENT_MODEL_REQUEST_TIMEOUT_SECONDS")
        if raw is not None:
            parsed, timeout_seconds = _parse_timeout_seconds(raw)
            if parsed:
                return timeout_seconds
        # This is the provider/model HTTP request timeout, not the full agent
        # run timeout. Multi-tool agents can still run longer via the outer
        # run timeout; one wedged model request should not.
        return GoogleADKRuntime._DEFAULT_MODEL_REQUEST_TIMEOUT_SECONDS

    @staticmethod
    def _generate_content_config_kwargs_for_request(request: RuntimeRequest, genai_types: Any) -> dict[str, Any]:
        config_kwargs: dict[str, Any] = {
            "max_output_tokens": request.max_output_tokens or 65535,
        }
        request_timeout_seconds = GoogleADKRuntime._model_request_timeout_seconds_for_request(request)
        if _is_native_gemini_model(request.model) and request_timeout_seconds is not None:
            timeout_millis = max(int(request_timeout_seconds * 1000), 1)
            http_options_type = getattr(genai_types, "HttpOptions", None)
            if http_options_type is not None:
                config_kwargs["http_options"] = http_options_type(timeout=timeout_millis)
        return config_kwargs

    @staticmethod
    def _max_attempts_for_request(request: RuntimeRequest) -> int:
        raw = os.environ.get("LUMIBOT_AGENT_MAX_RUN_ATTEMPTS")
        if raw:
            try:
                return max(int(raw), 1)
            except Exception:
                pass
        mutating_order_tools = {"orders_submit_order", "orders_cancel_order", "orders_modify_order"}
        if any(tool.name in mutating_order_tools for tool in request.bound_tools):
            # Retrying the whole agent run after a broker-side effect can duplicate orders.
            # Research-only agents keep the larger retry budget; trading agents fail fast
            # and let the next scheduled/bar iteration re-evaluate from current broker state.
            return 1
        mode = ""
        if isinstance(request.runtime_context, dict):
            mode = str(request.runtime_context.get("mode") or "").strip().lower()
        # Backtests can multiply spend quickly because one strategy run may call
        # the model hundreds of times. Keep provider retries conservative unless
        # the user explicitly opts into a higher retry budget.
        if mode == "backtesting":
            return 2
        return GoogleADKRuntime._MAX_RUN_ATTEMPTS

    @staticmethod
    def _run_timeout_seconds_for_request(request: RuntimeRequest) -> float | None:
        if request.run_timeout_seconds is not None:
            return _coerce_positive_timeout_seconds(request.run_timeout_seconds)
        raw = os.environ.get("LUMIBOT_AGENT_RUN_TIMEOUT_SECONDS")
        if raw is not None:
            parsed, timeout_seconds = _parse_timeout_seconds(raw)
            if parsed:
                return timeout_seconds
        # Agentic trading/research runs can legitimately spend many minutes
        # across model calls and tool calls. Keep the default high enough for
        # realistic multi-tool agents while still preventing indefinite hangs.
        return GoogleADKRuntime._DEFAULT_RUN_TIMEOUT_SECONDS

    @staticmethod
    def _is_non_retryable(exc: BaseException) -> bool:
        # Use the shared classifier: only transient and unknown errors retry.
        # auth / config / billing surface immediately so we don't waste ~5
        # minutes of retry budget on a wrong API key.
        return _classify_agent_error(exc) not in ("transient", "unknown")

    async def _run_async_with_timeout(self, request: RuntimeRequest, timeout_seconds: float) -> AgentRunResult:
        try:
            return await asyncio.wait_for(self._run_async(request), timeout=timeout_seconds)
        except (TimeoutError, asyncio.TimeoutError) as exc:
            raise TimeoutError(
                f"Agent run exceeded {timeout_seconds:g}s timeout "
                f"(model={request.model!r}, agent={request.agent_name!r})."
            ) from exc

    def run(self, request: RuntimeRequest) -> AgentRunResult:
        import time as _time

        last_exc: BaseException | None = None
        max_attempts = self._max_attempts_for_request(request)
        timeout_seconds = self._run_timeout_seconds_for_request(request)
        for attempt in range(1, max_attempts + 1):
            try:
                if timeout_seconds is None:
                    return asyncio.run(self._run_async(request))
                return asyncio.run(self._run_async_with_timeout(request, timeout_seconds))
            except (KeyboardInterrupt, SystemExit):
                raise
            except BaseException as exc:  # noqa: BLE001 - intentional broad catch for retry
                last_exc = exc
                if self._is_non_retryable(exc):
                    raise
                if attempt >= max_attempts:
                    break
                delay = self._RETRY_BACKOFF_SECONDS[min(attempt - 1, len(self._RETRY_BACKOFF_SECONDS) - 1)]
                try:
                    sys.stderr.write(
                        f"[lumibot.agents] transient error on attempt {attempt}/{max_attempts} "
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
            tool_context = {
                "agent_name": request.agent_name,
                "model_call_id": request.model_call_id,
                "enforce_order_readiness": True,
                "tool_calls": [],
            }
            if callable(first_tool.function):
                tool_result = _wrap_tool_callable(first_tool, tool_context)()
            else:
                tool_result = None
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
    _ensure_mcp_client_imports()
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
    if streamablehttp_client_uses_http_client:
        import httpx
        from mcp.shared._httpx_utils import create_mcp_http_client

        http_timeout = httpx.Timeout(timeout, read=sse_timeout)
        async with create_mcp_http_client(headers=headers, timeout=http_timeout) as http_client:
            async with streamablehttp_client(
                str(server.url),
                http_client=http_client,
                terminate_on_close=server.terminate_on_close,
            ) as (read_stream, write_stream, _get_session_id):
                async with ClientSession(read_stream, write_stream) as session:
                    await session.initialize()
                    return await callback(session)
    else:
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
    import asyncio

    try:
        asyncio.get_running_loop()
    except RuntimeError:
        from anyio import run as anyio_run

        return anyio_run(async_fn, *args)
    from anyio.from_thread import start_blocking_portal

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
    import httpx

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
    import httpx

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
