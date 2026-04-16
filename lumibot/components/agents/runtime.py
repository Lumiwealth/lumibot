import asyncio
import contextlib
import importlib
import logging
import json
import inspect
import math
import os
import re
import sys
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
        LlmAgentType, InMemoryRunnerType, genai_types, function_tool_type = self._ensure_adk()
        tool_name_map = {_tool_function_name(tool.name): tool.name for tool in request.bound_tools}
        tools = [function_tool_type(_wrap_tool_callable(tool)) for tool in request.bound_tools]
        agent = LlmAgentType(
            name=request.agent_name,
            model=request.model,
            instruction=self._instruction_for(request),
            tools=tools,
            generate_content_config=genai_types.GenerateContentConfig(
                temperature=0.0,
                max_output_tokens=65535,
            ),
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
            events.extend(_normalize_event(event))
        timestamp = _utc_iso_timestamp()
        for event in events:
            if event.tool_name:
                event.tool_name = tool_name_map.get(event.tool_name, event.tool_name)
            event.timestamp = timestamp
        summary = None
        text_chunks = [event.text for event in events if event.kind == "text" and event.text]
        if text_chunks:
            summary = text_chunks[-1]
        usage = None
        for event in reversed(events):
            if event.kind == "usage":
                usage = event.payload
                break
        return AgentRunResult(
            summary=summary,
            model=request.model,
            events=events,
            usage=usage,
        )

    def run(self, request: RuntimeRequest) -> AgentRunResult:
        return asyncio.run(self._run_async(request))


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
