import hashlib
import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from lumibot import LUMIBOT_CACHE_FOLDER

from .schemas import AgentRunResult, AgentTraceEvent, BoundTool, MCPServer, ToolDefinition
from .tools import bind_callable_tool


_TIMESTAMP_HINT_RE = re.compile(
    r"(time|date|datetime|published|updated|created|accepted|released|release|as_of|realtime)",
    re.IGNORECASE,
)
_DEFAULT_MEMORY_NOTE_MAX_CHARS = 2000


class AgentModelCallLimitExceeded(RuntimeError):
    """Raised before a model call when the configured agent-call budget is exhausted."""


_PANDAS_MODULE = None
_REPLAY_IMPORTS = None
_DUCKDB_QUERY_LAYER = None
_RUNTIME_IMPORTS = None
_PARQUET_UTILS = None
_BUILTIN_SERIALIZERS = None


def _get_pandas():
    global _PANDAS_MODULE
    if _PANDAS_MODULE is None:
        import pandas as pd

        _PANDAS_MODULE = pd
    return _PANDAS_MODULE


def _get_replay_imports():
    global _REPLAY_IMPORTS
    if _REPLAY_IMPORTS is None:
        from .replay_cache import AgentReplayCache, _normalize_json as normalize_json

        _REPLAY_IMPORTS = (AgentReplayCache, normalize_json)
    return _REPLAY_IMPORTS


def _normalize_json(value: Any) -> Any:
    return _get_replay_imports()[1](value)


def _get_duckdb_query_layer_class():
    global _DUCKDB_QUERY_LAYER
    if _DUCKDB_QUERY_LAYER is None:
        from .duckdb_tools import DuckDBQueryLayer

        _DUCKDB_QUERY_LAYER = DuckDBQueryLayer
    return _DUCKDB_QUERY_LAYER


def _get_runtime_imports():
    global _RUNTIME_IMPORTS
    if _RUNTIME_IMPORTS is None:
        from .runtime import GoogleADKRuntime, RuntimeRequest, StubAgentRuntime, call_mcp_tool

        _RUNTIME_IMPORTS = (GoogleADKRuntime, RuntimeRequest, StubAgentRuntime, call_mcp_tool)
    return _RUNTIME_IMPORTS


def _get_parquet_utils():
    global _PARQUET_UTILS
    if _PARQUET_UTILS is None:
        from lumibot.tools.parquet_utils import (
            coerce_object_columns_to_json_strings,
            is_parquet_required,
            write_parquet_with_logging,
        )

        _PARQUET_UTILS = (
            coerce_object_columns_to_json_strings,
            is_parquet_required,
            write_parquet_with_logging,
        )
    return _PARQUET_UTILS


def _get_builtin_serializers():
    global _BUILTIN_SERIALIZERS
    if _BUILTIN_SERIALIZERS is None:
        from .builtins import _order_to_dict, _position_to_dict

        _BUILTIN_SERIALIZERS = (_order_to_dict, _position_to_dict)
    return _BUILTIN_SERIALIZERS


def _safe_call(func, default=None):
    try:
        return func()
    except Exception:
        return default


def _current_strategy_datetime(strategy: Any) -> Any:
    if hasattr(strategy, "get_datetime"):
        return _safe_call(strategy.get_datetime)
    return None


def _iso_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception:
            return None
    return str(value)


def _strategy_timezone_name(strategy: Any, current_dt: Any) -> str | None:
    tzinfo = getattr(current_dt, "tzinfo", None)
    if tzinfo is not None:
        zone = getattr(tzinfo, "zone", None)
        if isinstance(zone, str) and zone:
            return zone
        text = str(tzinfo)
        if text:
            return text
    timezone_value = getattr(strategy, "timezone", None)
    if timezone_value:
        return str(timezone_value)
    pytz_value = getattr(strategy, "pytz", None)
    if pytz_value:
        return str(pytz_value)
    return None


def _serialize_recent_trade_events(strategy: Any, limit: int = 10) -> list[dict[str, Any]]:
    """Serialize broker trade events; optional expand_trade_event_rows maps rows -> rows."""

    broker = getattr(strategy, "broker", None)
    rows = list(getattr(broker, "_trade_event_log_rows", []) or [])
    expand_rows = getattr(broker, "expand_trade_event_rows", None)
    if expand_rows is None:
        expand_rows = getattr(broker, "_expand_trade_event_rows", None)
    if callable(expand_rows):
        rows = expand_rows(rows)
    columns = list(getattr(broker, "_trade_event_log_columns", []) or [])
    serialized: list[dict[str, Any]] = []
    for row in rows[-limit:]:
        payload: dict[str, Any]
        if isinstance(row, dict):
            payload = dict(row)
        elif isinstance(row, (list, tuple)) and columns:
            payload = {columns[idx]: row[idx] if idx < len(row) else None for idx in range(len(columns))}
        else:
            continue
        normalized: dict[str, Any] = {}
        for key, value in payload.items():
            normalized[str(key)] = _iso_or_none(value) if hasattr(value, "isoformat") else value
        serialized.append(normalized)
    return serialized


def _parse_datetime_like(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        try:
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _truncate_text(value: Any, limit: int = 240) -> str:
    text = str(value)
    if len(text) <= limit:
        return text
    return text[: limit - 3] + "..."


def _agent_memory_note_max_chars() -> int:
    raw = os.environ.get("LUMIBOT_AGENT_MEMORY_NOTE_MAX_CHARS")
    if raw:
        try:
            return max(int(raw), 200)
        except Exception:
            pass
    return _DEFAULT_MEMORY_NOTE_MAX_CHARS


def _agent_model_call_limit(strategy: Any) -> int | None:
    params = getattr(strategy, "parameters", None)
    raw = None
    if isinstance(params, dict):
        raw = params.get("agent_max_model_calls")
    if raw is None:
        raw = os.environ.get("LUMIBOT_AGENT_MAX_MODEL_CALLS")
    if raw is None or str(raw).strip() == "":
        return None
    try:
        return max(int(raw), 0)
    except Exception:
        return None


def _compact_json(value: Any, limit: int = 240) -> str:
    try:
        text = json.dumps(_normalize_json(value), sort_keys=True)
    except Exception:
        text = repr(value)
    return _truncate_text(text, limit=limit)


def _coerce_usage_int(value: Any) -> int:
    if value is None:
        return 0
    try:
        return int(value)
    except Exception:
        return 0


def _usage_nested_value(payload: Any, *path: str) -> int:
    if not path:
        return _coerce_usage_int(payload)
    key = path[0]
    if isinstance(payload, dict):
        return _usage_nested_value(payload.get(key), *path[1:])
    if isinstance(payload, list):
        return sum(_usage_nested_value(item, *path) for item in payload)
    return 0


def _usage_value(payload: Any, *keys: str) -> int:
    if not isinstance(payload, dict):
        return 0
    for key in keys:
        if "." in key:
            value = _usage_nested_value(payload, *key.split("."))
        else:
            value = _usage_nested_value(payload, key)
        if value <= 0:
            continue
        return value
    return 0


def _zero_usage() -> dict[str, int]:
    return {
        "input_tokens": 0,
        "output_tokens": 0,
        "total_tokens": 0,
        "thinking_tokens": 0,
        "cached_input_tokens": 0,
        "cache_write_input_tokens": 0,
        "uncached_input_tokens": 0,
        "tool_use_input_tokens": 0,
    }


def _usage_breakdown(payload: Any, *, cache_hit: bool) -> dict[str, int]:
    if cache_hit or not isinstance(payload, dict):
        return _zero_usage()

    input_tokens = _usage_value(payload, "prompt_token_count", "prompt_tokens", "input_tokens")
    output_tokens = _usage_value(payload, "candidates_token_count", "completion_tokens", "output_tokens")
    total_tokens = _usage_value(payload, "total_token_count", "total_tokens")
    thinking_tokens = _usage_value(
        payload,
        "thoughts_token_count",
        "reasoning_tokens",
        "completion_tokens_details.reasoning_tokens",
        "output_tokens_details.reasoning_tokens",
    )
    cached_input_tokens = _usage_value(
        payload,
        "cached_content_token_count",
        "cached_input_tokens",
        "cache_read_input_tokens",
        "cached_prompt_tokens",
        "cached_tokens",
        "prompt_cache_hit_tokens",
        "prompt_tokens_details.cached_tokens",
        "input_tokens_details.cached_tokens",
        "cache_tokens_details.token_count",
    )
    cache_write_input_tokens = _usage_value(
        payload,
        "cache_creation_input_tokens",
        "cache_creation.ephemeral_5m_input_tokens",
        "cache_creation.ephemeral_1h_input_tokens",
    )
    uncached_input_tokens = _usage_value(payload, "prompt_cache_miss_tokens", "prompt_tokens_details.uncached_tokens")
    if uncached_input_tokens <= 0:
        uncached_input_tokens = max(input_tokens - cached_input_tokens, 0)
    tool_use_input_tokens = _usage_value(payload, "tool_use_prompt_token_count")

    if total_tokens <= 0:
        total_tokens = input_tokens + output_tokens

    return {
        "input_tokens": input_tokens,
        "output_tokens": output_tokens,
        "total_tokens": total_tokens,
        "thinking_tokens": thinking_tokens,
        "cached_input_tokens": cached_input_tokens,
        "cache_write_input_tokens": cache_write_input_tokens,
        "uncached_input_tokens": uncached_input_tokens,
        "tool_use_input_tokens": tool_use_input_tokens,
    }


_AGENT_DETAIL_COLUMNS = [
    "timestamp",
    "call_index",
    "event_index",
    "agent_name",
    "model",
    "mode",
    "cache_hit",
    "event_kind",
    "is_call_summary",
    "tool_name",
    "event_detail",
    "event_payload_json",
    "event_text",
    "summary",
    "final_text",
    "thinking_text",
    "thinking_captured",
    "tool_sequence",
    "tool_call_count",
    "event_count",
    "task_prompt",
    "user_system_prompt",
    "base_system_prompt",
    "effective_system_prompt",
    "context_text",
    "runtime_context_text",
    "warning_messages",
    "event_input_tokens",
    "event_output_tokens",
    "event_total_tokens",
    "event_thinking_tokens",
    "event_cached_input_tokens",
    "event_cache_write_input_tokens",
    "event_uncached_input_tokens",
    "event_tool_use_input_tokens",
    "call_input_tokens",
    "call_output_tokens",
    "call_total_tokens",
    "call_thinking_tokens",
    "call_cached_input_tokens",
    "call_cache_write_input_tokens",
    "call_uncached_input_tokens",
    "call_tool_use_input_tokens",
    "call_started_at",
    "call_first_event_at",
    "call_ended_at",
    "call_latency_ms",
    "call_first_event_latency_ms",
    "trace_path",
]


def _unwrap_tool_payload(payload: Any) -> Any:
    if isinstance(payload, dict) and set(payload.keys()) == {"payload"} and isinstance(payload.get("payload"), dict):
        return payload["payload"]
    return payload


def _sanitize_csv_text(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = text.replace("\n", " \\n ")
    text = text.replace("\t", " ")
    return re.sub(r"\s+", " ", text).strip()


def _payload_json_text(value: Any) -> str:
    if value is None:
        return ""
    try:
        return _sanitize_csv_text(json.dumps(_normalize_json(value), sort_keys=True, default=str))
    except Exception:
        return _sanitize_csv_text(repr(value))


def _flatten_csv_value(value: Any) -> str:
    value = _normalize_json(value)
    if value is None:
        return ""
    if isinstance(value, dict):
        parts = []
        for key, item in value.items():
            item_text = _flatten_csv_value(item)
            if item_text:
                parts.append(f"{key}={item_text}")
        return _sanitize_csv_text("; ".join(parts))
    if isinstance(value, (list, tuple)):
        parts = [_flatten_csv_value(item) for item in value]
        parts = [part for part in parts if part]
        return _sanitize_csv_text(" | ".join(parts))
    return _sanitize_csv_text(value)


def _summarize_tool_payload(tool_name: str | None, payload: Any) -> str:
    payload = _unwrap_tool_payload(payload)
    if not isinstance(payload, dict):
        return _sanitize_csv_text(_truncate_text(payload))

    if "articles" in payload and isinstance(payload["articles"], list):
        articles = payload["articles"]
        headline_parts: list[str] = []
        for article in articles[:3]:
            if not isinstance(article, dict):
                continue
            symbols = ",".join(article.get("symbols") or [])
            published_at = article.get("published_at") or "unknown_time"
            headline = article.get("headline") or "untitled"
            prefix = f"{symbols} " if symbols else ""
            headline_parts.append(f"{prefix}@ {published_at}: {headline}")
        return _sanitize_csv_text(_truncate_text(
            f"count={payload.get('count', len(articles))} "
            f"window=({payload.get('window_start')} -> {payload.get('window_end')}) "
            f"headlines={headline_parts}"
        ))

    if "row_count" in payload and "table_name" in payload:
        return _sanitize_csv_text(_truncate_text(
            f"table={payload.get('table_name')} symbol={payload.get('symbol')} "
            f"rows={payload.get('row_count')} timestep={payload.get('timestep')} "
            f"loaded_at={payload.get('loaded_at')}"
        ))

    if "rows" in payload and "row_count" in payload:
        rows = payload.get("rows") or []
        sample = rows[0] if rows else {}
        return _sanitize_csv_text(_truncate_text(
            f"rows={payload.get('row_count')} sample={_compact_json(sample, limit=140)}"
        ))

    if "positions" in payload and isinstance(payload["positions"], list):
        labels: list[str] = []
        for position in payload["positions"]:
            if not isinstance(position, dict):
                continue
            asset = position.get("asset") or {}
            symbol = asset.get("symbol") if isinstance(asset, dict) else asset
            labels.append(f"{symbol}:{position.get('quantity')}")
        return _sanitize_csv_text(_truncate_text(f"positions={labels}"))

    if "cash" in payload and "portfolio_value" in payload:
        return _sanitize_csv_text(_truncate_text(
            f"cash={payload.get('cash')} portfolio_value={payload.get('portfolio_value')} "
            f"datetime={payload.get('datetime')}"
        ))

    if "identifier" in payload or "status" in payload:
        return _sanitize_csv_text(_truncate_text(
            f"identifier={payload.get('identifier')} status={payload.get('status')} "
            f"symbol={payload.get('symbol')} side={payload.get('side')} quantity={payload.get('quantity')}"
        ))

    return _flatten_csv_value(payload) or _sanitize_csv_text(_compact_json(payload))


def _visible_model_texts(result: AgentRunResult) -> list[str]:
    summary = (result.summary or result.text or "").strip()
    return [
        _sanitize_csv_text(event.text)
        for event in result.events
        if event.kind == "text"
        and event.text
        and not event.tool_name
        and event.text.strip()
        and event.text.strip() != summary
    ]


def _thinking_texts(result: AgentRunResult) -> list[str]:
    return [
        _sanitize_csv_text(event.text)
        for event in result.events
        if event.kind == "thinking" and event.text and event.text.strip()
    ]


def _event_usage_breakdown(event: AgentTraceEvent) -> dict[str, int]:
    if event.kind != "usage":
        return _zero_usage()
    return _usage_breakdown(event.payload, cache_hit=False)


def _runtime_timing_payload(result: AgentRunResult) -> dict[str, Any]:
    return {
        "call_started_at": result.started_at or "",
        "call_first_event_at": result.first_event_at or "",
        "call_ended_at": result.ended_at or "",
        "call_latency_ms": result.latency_ms if result.latency_ms is not None else "",
        "call_first_event_latency_ms": (
            result.first_event_latency_ms if result.first_event_latency_ms is not None else ""
        ),
    }


def _iter_timestamp_candidates(value: Any, *, path: str = "payload", hinted: bool = False):
    if isinstance(value, dict):
        for key, item in value.items():
            key_text = str(key)
            next_hinted = hinted or bool(_TIMESTAMP_HINT_RE.search(key_text))
            yield from _iter_timestamp_candidates(item, path=f"{path}.{key_text}", hinted=next_hinted)
        return
    if isinstance(value, list):
        for idx, item in enumerate(value):
            yield from _iter_timestamp_candidates(item, path=f"{path}[{idx}]", hinted=hinted)
        return
    if hinted and isinstance(value, str):
        parsed = _parse_datetime_like(value)
        if parsed is not None:
            yield path, value, parsed


def _stable_tool_metadata_for_cache(tool: BoundTool) -> dict[str, Any]:
    metadata = dict(tool.metadata or {})
    if tool.source == "mcp":
        stable: dict[str, Any] = {
            "kind": metadata.get("kind"),
            "server": metadata.get("server"),
            "transport": metadata.get("transport"),
        }
        return {key: value for key, value in stable.items() if value is not None}
    stable = {
        "kind": metadata.get("kind"),
    }
    return {key: value for key, value in stable.items() if value is not None}


def _provider_prompt_cache_key(
    *,
    agent_name: str,
    model: str,
    effective_system_prompt: str,
    bound_tools: list[BoundTool],
) -> str:
    payload = {
        "agent": agent_name,
        "model": model,
        "effective_system_prompt": effective_system_prompt,
        "tool_surface": [
            {
                "name": tool.name,
                "description": tool.description,
                "source": tool.source,
                "metadata": _stable_tool_metadata_for_cache(tool),
            }
            for tool in bound_tools
        ],
    }
    digest = hashlib.sha256(json.dumps(_normalize_json(payload), sort_keys=True).encode("utf-8")).hexdigest()
    return f"lumibot:{agent_name}:{digest[:32]}"


class AgentHandle:
    def __init__(
        self,
        *,
        manager: "AgentManager",
        name: str,
        system_prompt: str,
        default_model: str,
        tools: list[Any] | None = None,
        mcp_servers: list[MCPServer] | None = None,
        runtime: Any | None = None,
        allow_trading: bool = True,
    ) -> None:
        self.manager = manager
        self.name = name
        self.system_prompt = system_prompt
        self.default_model = default_model
        self.allow_trading = bool(allow_trading)
        from .builtins import BuiltinTools
        builtin_tools = self._filter_tools_for_trading_permission(BuiltinTools.all())
        if tools is None:
            self._tool_inputs = builtin_tools
        else:
            # Always include built-in tools, plus any custom tools the user added
            self._tool_inputs = builtin_tools + self._filter_tools_for_trading_permission(list(tools))
        self._mcp_servers = mcp_servers or []
        google_runtime, _RuntimeRequest, _StubAgentRuntime, _call_mcp_tool = _get_runtime_imports()
        self._runtime = runtime or google_runtime(mcp_servers=self._mcp_servers)
        self._bound_tools: list[BoundTool] | None = None

    def _filter_tools_for_trading_permission(self, tools: list[Any]) -> list[Any]:
        if self.allow_trading:
            return list(tools)
        filtered: list[Any] = []
        for tool in tools:
            metadata = getattr(tool, "metadata", {}) or {}
            if bool(metadata.get("mutates_trading")):
                continue
            filtered.append(tool)
        return filtered

    def _state_bucket(self) -> dict[str, Any]:
        bucket = self.manager.strategy.vars.get("_agent_runtime_state", {})
        if not isinstance(bucket, dict):
            bucket = {}
        if self.name not in bucket or not isinstance(bucket[self.name], dict):
            bucket[self.name] = {"memory_notes": [], "runs": []}
        self.manager.strategy.vars.set("_agent_runtime_state", bucket)
        return bucket[self.name]

    def _memory_notes(self) -> list[dict[str, Any]]:
        state = self._state_bucket()
        notes = state.get("memory_notes", [])
        return notes if isinstance(notes, list) else []

    def _memory_prompt_notes(self) -> list[dict[str, Any]]:
        projected: list[dict[str, Any]] = []
        max_chars = _agent_memory_note_max_chars()
        for note in self._memory_notes():
            if not isinstance(note, dict):
                continue
            projected.append(
                {
                    "timestamp": note.get("timestamp"),
                    "summary": _truncate_text(note.get("summary") or "", limit=max_chars),
                    "warnings": list(note.get("warnings") or []),
                }
            )
        return projected

    def _event_timestamp(self) -> str:
        current_dt = _current_strategy_datetime(self.manager.strategy)
        if current_dt is not None and hasattr(current_dt, "isoformat"):
            return current_dt.isoformat()
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    def _serialize_positions(self) -> list[dict[str, Any]]:
        if not hasattr(self.manager.strategy, "get_positions"):
            return []
        positions = _safe_call(lambda: self.manager.strategy.get_positions(include_cash_positions=True), default=[]) or []
        _order_to_dict, _position_to_dict = _get_builtin_serializers()
        return [_position_to_dict(position) for position in positions]

    def _serialize_account_state(self) -> dict[str, Any]:
        return {
            "cash": _safe_call(self.manager.strategy.get_cash) if hasattr(self.manager.strategy, "get_cash") else None,
            "portfolio_value": _safe_call(self.manager.strategy.get_portfolio_value)
            if hasattr(self.manager.strategy, "get_portfolio_value")
            else None,
        }

    def _serialize_orders(self, limit: int = 10) -> list[dict[str, Any]]:
        if not hasattr(self.manager.strategy, "get_orders"):
            return []
        orders = _safe_call(self.manager.strategy.get_orders, default=[]) or []
        _order_to_dict, _position_to_dict = _get_builtin_serializers()
        return [_order_to_dict(order) for order in orders[-limit:]]

    def _runtime_mode(self) -> str:
        return "backtesting" if bool(getattr(self.manager.strategy, "is_backtesting", False)) else "live"

    def _runtime_context(self) -> dict[str, Any]:
        strategy = self.manager.strategy
        current_dt = _current_strategy_datetime(strategy)
        return {
            "agent_name": self.name,
            "mode": self._runtime_mode(),
            "current_datetime": _iso_or_none(current_dt),
            "timezone": _strategy_timezone_name(strategy, current_dt),
            "strategy_name": getattr(strategy, "name", None) or strategy.__class__.__name__,
            "market": getattr(strategy, "market", None),
            "positions": self._serialize_positions(),
            "account": self._serialize_account_state(),
            "recent_orders": self._serialize_orders(),
            "recent_trades": _serialize_recent_trade_events(strategy),
        }

    def _base_system_prompt(self, runtime_context: dict[str, Any]) -> str:
        mode = runtime_context.get("mode") or "live"
        lines = [
            "You are operating as a trading agent inside LumiBot.",
            "Use the provided runtime context and tool outputs as the ground truth for the current state of the strategy.",
            "Ground claims in tool results or runtime context instead of unsupported prior knowledge or vague market memory.",
            "If evidence is weak, conflicting, stale, or incomplete, prefer doing nothing and explain why.",
            "Execution mode, current datetime, current timezone, current positions, cash, equity/portfolio value, recent orders, and recent trades are provided in Runtime Context JSON.",
            "Review current exposure, available cash, and recent activity before proposing any new trade.",
            "",
            "DEFAULT INVESTOR POLICY - FOLLOW THIS UNLESS THE USER'S SYSTEM PROMPT CLEARLY ASKS FOR A DIFFERENT STYLE:",
            "Your job is to grow the account's value over time, not to maximize trade count.",
            "Do not trade for the sake of activity. Prefer no trade over a weak trade.",
            "Require a real thesis and real conviction before entering or rotating a position.",
            "Do not buy an asset just because it is tradable, mentioned in news, or recently active.",
            "Ask yourself why this should likely make money from here, why it is better than doing nothing, why it is better than what is already held, and what the downside is if you are wrong.",
            "Use capital intentionally. Avoid token positions that are too small to matter.",
            "Diversify when the strategy is broad and multiple opportunities compete for capital.",
            "Assume this strategy may be one component of a broader portfolio unless the user says otherwise.",
            "Do not resist intentional concentration when the user's strategy clearly calls for concentrated or single-asset exposure.",
            "If you are not deploying capital into risk assets, explain why a high-quality short-duration defensive parking choice is preferable right now.",
            "Avoid leaving raw cash idle unless there is a specific reason the defensive parking asset is unavailable or inappropriate.",
            "When rotating, compare the new idea against the current holdings or current defensive posture and only switch if the new opportunity is clearly better.",
            "Be aware that trading has costs. Commissions, spreads, and slippage add up, especially for thinly traded assets.",
            "Prefer limit orders over market orders when the asset is not highly liquid.",
            "Do not overtrade. Each round-trip has a cost, so the expected gain from a trade should clearly exceed the expected friction.",
            "",
            "RISK AND DRAWDOWN DISCIPLINE:",
            "Your objective is the best risk-adjusted return over time, not the highest raw return. A smoother equity curve with a lower max drawdown is more valuable than a jagged one with a slightly higher end value, because compounding is damaged by deep drawdowns and because real users abandon strategies that hurt too much.",
            "Remember the recovery math: a 20% drawdown requires a 25% gain to get back to even, a 50% drawdown requires a 100% gain, and an 80% drawdown requires a 400% gain. Small losses compound gently, large losses compound painfully. Limiting downside is almost always more valuable than squeezing out the last bit of upside.",
            "Protect the downside as seriously as you pursue the upside. Size positions relative to conviction and expected volatility, not just available cash. A high-conviction low-volatility idea can take a larger share than a speculative high-volatility one.",
            "Cut losing positions when the thesis is broken. Do not average down into a losing trade just to lower your cost basis. Reassess the thesis first, and exit if the evidence no longer supports the position.",
            "Do not chase returns after a drawdown by increasing size or taking more aggressive exposure. That is how small drawdowns become large ones.",
            "Think in terms of return per unit of volatility (Sharpe), return per unit of downside volatility (Sortino), and return relative to max drawdown (Calmar). The goal is compounding you can actually live with, not a headline number.",
            "",
            "POSITION SIZING AND ORDER EXECUTION:",
            "Do not buy token one-share positions. Use account cash, portfolio value, current position size, and last price to calculate a sensible whole-share quantity.",
            "Round down to whole shares when sizing positions.",
            "When switching from one asset to another, close or reduce the current position first to free up capital before buying the replacement.",
            "If the strategy holds a defensive parking asset (like SHV, BIL, or SGOV) and a better opportunity appears, sell the parking asset first to free the cash, then buy the new position. Do not assume parked capital is unavailable.",
            "",
            "TOOL USAGE:",
            "Use your available tools to gather evidence before making any trading decision. Do not guess when a tool can give you the answer.",
            "Before placing any trade, use tools to check current positions, available cash, and portfolio value.",
            "Load recent price history for any asset you are considering and inspect it before deciding.",
            "When available, use the built-in evidence stack before making a material equity decision: account/portfolio tools, current market prices, recent price history, DuckDB analysis, technical indicators, relevant news, macro/FRED data, SEC financial statements, SEC company facts, and SEC filings.",
            "Do not submit a material equity order until you have called account/portfolio tools, market price/history tools, at least one technical indicator tool, a relevant news tool when configured, a macro/FRED tool when configured, and SEC financial/filing tools for relevant single-stock candidates.",
            "For ETFs, indexes, or broad-market trades, use SEC financial/filing tools on the most relevant single-stock candidates, holdings, or alternatives you are considering; do not skip the category just because the final instrument is an ETF.",
            "If the user asks for an aggressive or concentrated strategy, let that user strategy prompt override the default investor style, but still ground the decision in tool evidence, position sizing, broker constraints, and backtesting look-ahead safety.",
            "When querying DuckDB tables, use datetime for timestamp columns and close for price columns unless the loaded sample rows clearly show different column names.",
            "When you have access to external MCP tools, explore what they offer and use them. You do not need to be told which specific tool to call.",
            "Finish every run with a short summary sentence starting with RESULT: that explains what you did and why.",
        ]
        if mode == "backtesting":
            lines.extend(
                [
                    "",
                    "BACKTESTING SAFETY RULES - READ THIS AS A HARD REQUIREMENT:",
                    "Look-ahead bias means using information that would not have been available at the current simulated datetime.",
                    "If you leak future information into a backtest, the backtest becomes invalid, misleading, and useless for decision-making.",
                    "Treat the current simulated datetime as a hard wall. Do not cross it. Do not infer across it. Do not hint across it.",
                    "Only use bars, news, macro data, filings, prices, positions, and events that were available at or before the current simulated datetime.",
                    "Correct example: if the current simulated time is 2026-03-10 10:15 ET, you may use a news article published at 09:30 ET that same day if it appears in tool output.",
                    "Incorrect example: using a headline published at 14:00 ET, a later macro revision, a later SEC filing, or knowledge of the close when the simulated time is still the morning.",
                    "Incorrect example: saying 'the market later sold off' or 'inflation kept rising after this' unless that fact is explicitly visible in current tool output at or before the simulated datetime.",
                    "Incorrect example: relying on what you remember happened historically when that information is not yet present in the runtime context or tool results.",
                    "CRITICAL: When calling ANY external tool, if the tool has ANY parameter that controls a time range, date filter, or temporal bound, you MUST set it so that no data after the current simulated datetime can be returned.",
                    "This applies regardless of what the parameter is named. Common names include: end, end_date, time_to, observation_end, before, until, to, date, timestamp, coed, realtime_end - but ANY parameter that limits the time range must be set.",
                    "If a tool has a start/end date range and you only set start without setting end, the tool will likely return data up to today, which is in the future. ALWAYS set the end bound.",
                    "Correct example: if the current simulated date is 2024-01-22 and a tool accepts end, end_date, time_to, or observation_end, pass 2024-01-22 (or the current simulated datetime) in that field.",
                    "Incorrect example: calling a news, macro, or data tool with only a start parameter and no end parameter, allowing it to return future data by default.",
                    "If a tool response seems to include future timestamps, treat that as suspicious. Do not rely on those records without calling out the risk in your reasoning.",
                    "If you are unsure whether information was available yet, say the evidence is insufficient and do nothing.",
                    "Backtesting accuracy is more important than being clever. A cautious no-trade is better than a future-biased trade.",
                ]
            )
        else:
            lines.extend(
                [
                    "",
                    "LIVE TRADING RULES:",
                    "Act on the current visible market state and runtime context.",
                    "Keep the strategy's positions, cash, and recent activity in mind before taking new actions.",
                ]
            )
        return "\n".join(lines).strip()

    def _compose_system_prompt(self, runtime_context: dict[str, Any]) -> str:
        return "\n\n".join(
            [
                self._base_system_prompt(runtime_context),
                "USER SYSTEM PROMPT:",
                "Treat this as the strategy-specific trading objective. It may override the default investor style, but not hard safety, broker, or look-ahead-bias rules.",
                self.system_prompt.strip(),
            ]
        ).strip()

    def _append_memory(self, result: AgentRunResult) -> None:
        state = self._state_bucket()
        notes = self._memory_notes()
        event_timestamp = self._event_timestamp()
        memory_summary = _truncate_text(result.summary or result.text or "", limit=_agent_memory_note_max_chars())
        notes.append(
            {
                "timestamp": event_timestamp,
                "summary": memory_summary,
                "tool_calls": [event.tool_name for event in result.tool_calls if event.tool_name],
                "warnings": result.warning_messages,
                "cache_hit": result.cache_hit,
            }
        )
        state["memory_notes"] = notes[-20:]
        runs = state.get("runs", [])
        if not isinstance(runs, list):
            runs = []
        runs.append(
            {
                "cache_key": result.cache_key,
                "cache_hit": result.cache_hit,
                "summary": result.summary,
                "model": result.model,
                "warnings": result.warning_messages,
                "timestamp": event_timestamp,
            }
        )
        state["runs"] = runs[-50:]
        self.manager.strategy.vars.set("_agent_runtime_state", self.manager.strategy.vars.get("_agent_runtime_state"))

    def _build_remote_tools(self) -> list[BoundTool]:
        remote_tools: list[BoundTool] = []
        for server in self._mcp_servers:
            for exposed_name in server.exposed_tools or []:
                description = f"Remote MCP tool {exposed_name} on server {server.name}."

                def make_remote_tool(_server: MCPServer, _tool_name: str):
                    def remote_tool(payload: dict[str, Any]) -> dict[str, Any]:
                        warning_key = (_server.name, _tool_name)
                        if bool(getattr(self.manager.strategy, "is_backtesting", False)) and warning_key not in self.manager._warned_backtest_mcp_tools:
                            log_message = getattr(self.manager.strategy, "log_message", None)
                            if callable(log_message):
                                log_message(
                                    f"[agents] external MCP tool {_server.name}:{_tool_name} is running during a backtest. "
                                    "LumiBot will trace it and warn on suspicious temporal behavior, but it will not block it.",
                                    color="yellow",
                                )
                            self.manager._warned_backtest_mcp_tools.add(warning_key)
                        _GoogleADKRuntime, _RuntimeRequest, _StubAgentRuntime, call_mcp_tool = _get_runtime_imports()
                        return call_mcp_tool(_server, _tool_name, payload)

                    return remote_tool

                remote_tool = make_remote_tool(server, exposed_name)
                remote_tools.append(
                    BoundTool(
                        name=exposed_name,
                        description=description,
                        function=remote_tool,
                        source="mcp",
                        metadata={
                            "kind": "mcp",
                            "server": server.name,
                            "transport": server.transport,
                            "url": server.url,
                            "command": server.command,
                        },
                    )
                )
        return remote_tools

    def _ensure_bound_tools(self) -> list[BoundTool]:
        if self._bound_tools is not None:
            return self._bound_tools
        bound: list[BoundTool] = []
        for entry in self._tool_inputs:
            if isinstance(entry, ToolDefinition):
                tool = entry.binder(self.manager.strategy, self.manager)
            else:
                tool = bind_callable_tool(entry).binder(self.manager.strategy, self.manager)
            if bool((tool.metadata or {}).get("disabled")):
                continue
            bound.append(tool)
        bound.extend(self._build_remote_tools())
        # Built-ins are auto-included, but strategies may also explicitly pass
        # a BuiltinTools entry. ADK rejects duplicate function declarations, so
        # keep one tool per name. Later entries win, which lets explicit
        # user-supplied tools override auto-included built-ins.
        deduped: dict[str, BoundTool] = {}
        for tool in bound:
            deduped[tool.name] = tool
        self._bound_tools = list(deduped.values())
        return self._bound_tools

    @staticmethod
    def _log_fatal_backtest_error(exc: BaseException, category: str, model: str) -> None:
        """Write a clean, loud error banner to stderr when we crash a backtest
        on an unrecoverable config/auth/billing error. Points the user at
        the likely fix (env var name, provider URL) so they can act
        without decoding a raw provider stack trace."""
        # Map provider prefix -> (env var, billing url).
        provider_hints = {
            "openai/": ("OPENAI_API_KEY", "https://platform.openai.com/api-keys", "https://platform.openai.com/account/billing"),
            "xai/": ("XAI_API_KEY or GROK_API_KEY", "https://console.x.ai/", "https://console.x.ai/team"),
            "anthropic/": ("ANTHROPIC_API_KEY", "https://console.anthropic.com/", "https://console.anthropic.com/settings/billing"),
            "deepseek/": ("DEEPSEEK_API_KEY", "https://platform.deepseek.com/api_keys", "https://platform.deepseek.com/usage"),
            "together_ai/": ("TOGETHER_API_KEY or TOGETHERAI_API_KEY", "https://api.together.ai/settings/api-keys", "https://api.together.ai/settings/billing"),
            "cerebras/": ("CEREBRAS_API_KEY", "https://cloud.cerebras.ai/platform/", "https://cloud.cerebras.ai/platform/billing"),
        }
        env_var, key_url, billing_url = ("GEMINI_API_KEY", "https://aistudio.google.com/apikey", "https://aistudio.google.com/")
        for prefix, (ev, ku, bu) in provider_hints.items():
            if isinstance(model, str) and model.startswith(prefix):
                env_var, key_url, billing_url = ev, ku, bu
                break

        lines = [
            "",
            "=" * 78,
            f"AI AGENT BACKTEST CRASHED ({category.upper()} error)",
            "=" * 78,
            f"Model:  {model}",
            f"Error:  {exc.__class__.__name__}: {str(exc)[:400]}",
            "",
        ]
        if category == "auth":
            lines.extend([
                f"Likely cause: {env_var} is missing or invalid.",
                f"  Get a key at: {key_url}",
                f"  Then:         export {env_var}='your-key-here'",
            ])
        elif category == "billing":
            lines.extend([
                f"Likely cause: provider billing issue (out of credits, quota exceeded).",
                f"  Check billing at: {billing_url}",
            ])
        elif category == "config":
            lines.extend([
                "Likely cause: bad model id, malformed request, or context-window exceeded.",
                f"  Current model:      {model}",
                "  Verify the model id is on your provider's /models list.",
                "  If context-window: reduce runtime context / memory / tool count.",
            ])
        lines.extend([
            "",
            "Backtest stopped intentionally so you can fix this and re-run.",
            "Note: live trading does NOT stop on this error category — it logs and",
            "skips the iteration so the bot stays alive for operator intervention.",
            "=" * 78,
            "",
        ])
        try:
            sys.stderr.write("\n".join(lines))
            sys.stderr.flush()
        except Exception:
            pass

    def _cache_payload(
        self,
        *,
        task_prompt: str | None,
        context: dict[str, Any] | None,
        model: str,
        runtime_context: dict[str, Any],
        effective_system_prompt: str,
        base_system_prompt: str,
    ) -> dict[str, Any]:
        bound_tools = self._ensure_bound_tools()
        return {
            "user_system_prompt": self.system_prompt,
            "base_system_prompt": base_system_prompt,
            "effective_system_prompt": effective_system_prompt,
            "task_prompt": task_prompt,
            "context": context or {},
            "runtime_context": runtime_context,
            "model": model,
            "tool_surface": [
                {
                    "name": tool.name,
                    "description": tool.description,
                    "source": tool.source,
                    "metadata": _stable_tool_metadata_for_cache(tool),
                }
                for tool in bound_tools
            ],
            "memory_notes": self._memory_prompt_notes(),
        }

    @staticmethod
    def _cache_root() -> Path:
        return Path(os.environ.get("LUMIBOT_CACHE_FOLDER") or LUMIBOT_CACHE_FOLDER)

    def _runtime_artifact_dir(self) -> Path:
        runtime_dir = self._cache_root() / "agent_runtime"
        runtime_dir.mkdir(parents=True, exist_ok=True)
        return runtime_dir

    def _trace_dir(self) -> Path:
        trace_dir = self._runtime_artifact_dir() / "traces" / self.name
        trace_dir.mkdir(parents=True, exist_ok=True)
        return trace_dir

    def _write_trace(self, result: AgentRunResult, trace_payload: dict[str, Any]) -> Path:
        trace_path = self._trace_dir() / f"{result.cache_key or 'live'}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S%f')}.json"
        trace_path.write_text(json.dumps(_normalize_json(trace_payload), indent=2, sort_keys=True), encoding="utf-8")
        return trace_path

    def _append_run_artifact_summary(self, result: AgentRunResult, runtime_context: dict[str, Any]) -> None:
        summary_path = self._runtime_artifact_dir() / "agent_run_summaries.jsonl"
        trace_path = ""
        if isinstance(result.payload, dict):
            trace_path = str(result.payload.get("trace_path") or "")
        cache_root = self._cache_root()
        trace_relative_path = trace_path
        if trace_path:
            try:
                trace_relative_path = Path(trace_path).resolve().relative_to(cache_root.resolve()).as_posix()
            except Exception:
                trace_relative_path = trace_path
        record = {
            "timestamp": self._event_timestamp(),
            "agent_name": self.name,
            "mode": runtime_context.get("mode"),
            "model": result.model,
            "summary": result.summary or result.text or "",
            "cache_hit": result.cache_hit,
            "cache_key": result.cache_key,
            "usage": _usage_breakdown(result.usage, cache_hit=bool(result.cache_hit)),
            "timing": _runtime_timing_payload(result),
            "tool_calls": [event.tool_name for event in result.tool_calls if event.tool_name],
            "warning_messages": result.warning_messages,
            "trace_path": trace_path,
            "trace_relative_path": trace_relative_path,
        }
        with summary_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(_normalize_json(record), sort_keys=True))
            handle.write("\n")

    def _result_from_cached(self, cached: dict[str, Any], cache_key: str) -> AgentRunResult:
        events = [
            AgentTraceEvent(
                kind=str(event.get("kind")),
                text=event.get("text"),
                tool_name=event.get("tool_name"),
                payload=event.get("payload"),
                timestamp=event.get("timestamp"),
            )
            for event in cached.get("events", [])
            if isinstance(event, dict)
        ]
        timing = cached.get("timing") if isinstance(cached.get("timing"), dict) else {}
        return AgentRunResult(
            summary=cached.get("summary"),
            model=cached.get("model") or self.default_model,
            events=events,
            cache_hit=True,
            cache_key=cache_key,
            usage=cached.get("usage"),
            payload=cached.get("payload"),
            warnings=list(cached.get("warnings") or []),
            started_at=timing.get("call_started_at"),
            first_event_at=timing.get("call_first_event_at"),
            ended_at=timing.get("call_ended_at"),
            latency_ms=_coerce_usage_int(timing.get("call_latency_ms")),
            first_event_latency_ms=_coerce_usage_int(timing.get("call_first_event_latency_ms")),
        )

    def _replay_cached_side_effects(self, result: AgentRunResult) -> None:
        bound_tools = {tool.name: tool for tool in self._ensure_bound_tools()}
        for event in result.tool_calls:
            tool_name = event.tool_name
            if not tool_name:
                continue
            tool = bound_tools.get(tool_name)
            if tool is None:
                continue
            if tool.source == "mcp":
                continue
            if not bool(tool.metadata.get("replay_on_cache")):
                continue
            payload = event.payload if isinstance(event.payload, dict) else {}
            tool.function(**payload)

    def _derive_warnings(self, result: AgentRunResult, runtime_context: dict[str, Any]) -> list[dict[str, Any]]:
        warnings: list[dict[str, Any]] = []
        current_dt = _parse_datetime_like(runtime_context.get("current_datetime"))
        mode = runtime_context.get("mode")
        if not result.tool_calls:
            warnings.append(
                {
                    "kind": "no_tool_calls",
                    "message": "Agent finished without calling any tools.",
                }
            )
        tool_names = [event.tool_name for event in result.tool_calls if event.tool_name]
        used_data_tool = any(
            name.startswith("market_")
            or name.startswith("duckdb_")
            or name.startswith("account_")
            or name in {
                "get_news",
                "alpaca_news",
                "list_fred_series",
                "get_fred_series",
                "get_fred_latest",
                "get_fred_snapshot",
            }
            for name in tool_names
        )
        used_order_tool = any(name.startswith("orders_") for name in tool_names)
        if used_order_tool and not used_data_tool:
            warnings.append(
                {
                    "kind": "order_without_data",
                    "message": "Agent used an order tool without prior visible non-order data/tool calls in the same run.",
                }
            )
        for event in result.tool_results:
            payload = event.payload if isinstance(event.payload, dict) else None
            if not payload:
                continue
            if payload.get("tool_error") is True:
                error = payload.get("error") or {}
                warnings.append(
                    {
                        "kind": "tool_error",
                        "tool_name": event.tool_name,
                        "message": (
                            f"Tool {event.tool_name} returned an error: "
                            f"{error.get('type') or 'Error'}: {error.get('message') or 'unknown error'}."
                        ),
                    }
                )
        if mode == "backtesting" and current_dt is not None:
            for event in result.tool_results:
                payload = event.payload if isinstance(event.payload, dict) else None
                if not payload:
                    continue
                for path, raw_value, parsed in _iter_timestamp_candidates(payload):
                    if parsed > current_dt:
                        warnings.append(
                            {
                                "kind": "future_timestamp",
                                "tool_name": event.tool_name,
                                "path": path,
                                "timestamp": raw_value,
                                "message": (
                                    f"Tool {event.tool_name} returned timestamp {raw_value} "
                                    f"after simulated time {current_dt.isoformat()}."
                                ),
                            }
                        )
                        if len(warnings) >= 10:
                            return warnings
        return warnings

    def _log_run_summary(self, result: AgentRunResult, runtime_context: dict[str, Any]) -> None:
        log_message = getattr(self.manager.strategy, "log_message", None)
        if not callable(log_message):
            return
        usage = _usage_breakdown(result.usage, cache_hit=bool(result.cache_hit))
        trace_path = ""
        if isinstance(result.payload, dict):
            trace_path = str(result.payload.get("trace_path") or "")
        summary = (result.summary or result.text or "").replace("\n", " ").strip()
        message = (
            f"[agents] name={self.name} mode={runtime_context.get('mode')} "
            f"model={result.model} cache_hit={result.cache_hit} "
            f"tokens_in={usage['input_tokens']} tokens_out={usage['output_tokens']} "
            f"tokens_cached_in={usage['cached_input_tokens']} "
            f"tokens_uncached_in={usage['uncached_input_tokens']} "
            f"tokens_thinking={usage['thinking_tokens']} tokens_total={usage['total_tokens']} "
            f"latency_ms={result.latency_ms if result.latency_ms is not None else 'unknown'} "
            f"first_event_latency_ms={result.first_event_latency_ms if result.first_event_latency_ms is not None else 'unknown'} "
            f"tool_calls={len(result.tool_calls)} observability_warnings={len(result.warnings)} "
            f"summary={summary!r} trace={trace_path}"
        )
        log_message(message, color="yellow")
        if result.tool_calls:
            tool_sequence = " -> ".join(
                event.tool_name or "unknown_tool" for event in result.tool_calls
            )
            log_message(f"[agents][tools] {tool_sequence}", color="yellow")
        for idx, event in enumerate(result.tool_calls, start=1):
            preview = _summarize_tool_payload(event.tool_name, event.payload)
            log_message(
                f"[agents][tool_call {idx}] {event.tool_name}: {preview}",
                color="yellow",
            )
        for idx, event in enumerate(result.tool_results, start=1):
            preview = _summarize_tool_payload(event.tool_name, event.payload)
            log_message(
                f"[agents][tool_result {idx}] {event.tool_name}: {preview}",
                color="yellow",
            )
        visible_model_texts = [
            event.text.strip()
            for event in result.events
            if event.kind == "text"
            and event.text
            and not event.tool_name
            and event.text.strip()
            and event.text.strip() != summary
        ]
        for idx, text in enumerate(visible_model_texts, start=1):
            preview = _truncate_text(text, limit=600)
            log_message(
                f"[agents][model_text {idx}] {preview}",
                color="yellow",
            )
        for warning in result.warning_messages:
            log_message(f"[agents][observability_warning] {warning}", color="yellow")

    @staticmethod
    def _finalize_runtime_timing(
        result: AgentRunResult,
        *,
        started_at: str,
        started_perf: float,
        ended_at: str,
        ended_perf: float,
    ) -> None:
        if result.started_at is None:
            result.started_at = started_at
        if result.ended_at is None:
            result.ended_at = ended_at
        if result.latency_ms is None:
            result.latency_ms = max(int((ended_perf - started_perf) * 1000), 0)
        if result.first_event_at is None:
            first_event = next((event for event in result.events if event.timestamp), None)
            if first_event is not None:
                result.first_event_at = first_event.timestamp
        if result.first_event_latency_ms is None and result.first_event_at:
            started_dt = _parse_datetime_like(result.started_at)
            first_dt = _parse_datetime_like(result.first_event_at)
            if started_dt is not None and first_dt is not None:
                result.first_event_latency_ms = max(int((first_dt - started_dt).total_seconds() * 1000), 0)

    def run(
        self,
        *,
        task_prompt: str | None = None,
        context: dict[str, Any] | None = None,
        model: str | None = None,
        **kwargs: Any,
    ) -> AgentRunResult:
        if "task" in kwargs and task_prompt is None:
            task_prompt = kwargs["task"]
        model_name = model or self.default_model
        runtime_context = self._runtime_context()
        base_system_prompt = self._base_system_prompt(runtime_context)
        effective_system_prompt = self._compose_system_prompt(runtime_context)
        cache_payload = self._cache_payload(
            task_prompt=task_prompt,
            context=context,
            model=model_name,
            runtime_context=runtime_context,
            effective_system_prompt=effective_system_prompt,
            base_system_prompt=base_system_prompt,
        )
        cache_key = self.manager.replay_cache.compute_key(cache_payload)
        strategy = self.manager.strategy
        should_replay = bool(getattr(strategy, "is_backtesting", False))
        if should_replay:
            cached = self.manager.replay_cache.load(cache_key)
            if cached is not None:
                result = self._result_from_cached(cached, cache_key)
                self._replay_cached_side_effects(result)
                self.manager._record_agent_observability(
                    handle=self,
                    result=result,
                    runtime_context=runtime_context,
                    cache_payload=cache_payload,
                )
                self._append_memory(result)
                self._append_run_artifact_summary(result, runtime_context)
                self._log_run_summary(result, runtime_context)
                return result

        _GoogleADKRuntime, runtime_request_class, _StubAgentRuntime, _call_mcp_tool = _get_runtime_imports()
        request = runtime_request_class(
            agent_name=self.name,
            model=model_name,
            system_prompt=effective_system_prompt,
            task_prompt=task_prompt,
            context=context,
            runtime_context=runtime_context,
            memory_notes=self._memory_prompt_notes(),
            bound_tools=self._ensure_bound_tools(),
            provider_prompt_cache_key=_provider_prompt_cache_key(
                agent_name=self.name,
                model=model_name,
                effective_system_prompt=effective_system_prompt,
                bound_tools=self._ensure_bound_tools(),
            ),
        )
        self.manager._reserve_model_call(agent_name=self.name, model=model_name)
        # Strategy-level safety net with live-vs-backtest branching.
        #
        # Scope: this behavior is ONLY for AI agent calls. The rest of
        # LumiBot's main loop error handling (strategy_executor catches,
        # _on_bot_crash, gracefully_exit, broker errors) is unchanged.
        #
        # Philosophy:
        #   - LIVE TRADING: never crash. A live bot must survive any AI
        #     provider error (outage, rate limit, auth, quota, bad model,
        #     context-window exceeded) without stopping the scheduler.
        #     Log the error and return a graceful "no decision this bar"
        #     result. Next iteration tries again.
        #   - BACKTEST: crash loud on config/auth/billing errors so the
        #     user can fix and re-run. Silent +0% tearsheets are worse
        #     than a clear error message. Transient errors (provider 5xx,
        #     rate limits) still skip silently since they're not bugs the
        #     user can act on.
        #
        # The _classify_agent_error helper (runtime.py) handles the taxonomy.
        started_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        started_perf = time.perf_counter()
        try:
            result = self._runtime.run(request)
        except (KeyboardInterrupt, SystemExit):
            raise
        except BaseException as exc:  # noqa: BLE001 - intentional broad catch
            import traceback as _tb
            from .runtime import _classify_agent_error
            from .schemas import AgentRunResult, AgentTraceEvent

            category = _classify_agent_error(exc)
            is_backtesting = bool(getattr(self.manager.strategy, "is_backtesting", False))

            # Backtest crashes loud on permanent config errors so user notices + fixes.
            # Live never crashes, regardless of category.
            if is_backtesting and category in ("auth", "config", "billing"):
                self._log_fatal_backtest_error(exc, category, model_name)
                raise

            error_detail = f"{exc.__class__.__name__}: {str(exc)[:400]}"
            try:
                sys.stderr.write(
                    f"[lumibot.agents] agent '{self.name}' (model={model_name!r}) call failed: "
                    f"category={category} mode={'backtest' if is_backtesting else 'live'}. "
                    f"Skipping this iteration (no trades placed). Error: {error_detail}\n"
                )
                sys.stderr.flush()
            except Exception:
                pass
            now_iso = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
            fallback_summary = (
                f"RESULT: Skipped this iteration. Agent call failed "
                f"(category={category}): {error_detail}. "
                f"Strategy continues with no-op decision; no trades placed."
            )
            error_event = AgentTraceEvent(
                kind="text",
                text=fallback_summary,
                timestamp=now_iso,
                payload={
                    "runtime_error": True,
                    "error_category": category,
                    "error_class": exc.__class__.__name__,
                    "error_message": str(exc)[:800],
                    "traceback": "".join(_tb.format_exception(type(exc), exc, exc.__traceback__))[-2000:],
                },
            )
            result = AgentRunResult(
                summary=fallback_summary,
                model=model_name,
                events=[error_event],
                usage=None,
            )
            # Mark it so downstream code and the user can easily filter/count
            # skipped iterations in the warnings stream.
            result.warnings.append(
                {
                    "kind": "agent_runtime_failure_skipped",
                    "category": category,
                    "message": f"agent_runtime_failure_skipped: {error_detail}",
                    "timestamp": now_iso,
                }
            )
            result.cache_key = None  # never cache a failure
            result.payload = {
                "trace_path": None,
                "runtime_error": True,
                "error_class": exc.__class__.__name__,
                "error_message": str(exc)[:800],
            }
            self._finalize_runtime_timing(
                result,
                started_at=started_at,
                started_perf=started_perf,
                ended_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                ended_perf=time.perf_counter(),
            )
            # Record this skipped run in the agent's memory so the model on
            # the next iteration knows the previous cycle was skipped.
            self.manager._record_agent_observability(
                handle=self,
                result=result,
                runtime_context=runtime_context,
                cache_payload=cache_payload,
            )
            self._append_memory(result)
            self._append_run_artifact_summary(result, runtime_context)
            self._log_run_summary(result, runtime_context)
            return result
        self._finalize_runtime_timing(
            result,
            started_at=started_at,
            started_perf=started_perf,
            ended_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            ended_perf=time.perf_counter(),
        )
        result.cache_key = cache_key
        result.warnings = self._derive_warnings(result, runtime_context)
        trace_payload = {
            "agent": self.name,
            "model": model_name,
            "request": cache_payload,
            "tool_calls": [
                {
                    "tool_name": event.tool_name,
                    "payload": event.payload,
                    "timestamp": event.timestamp,
                }
                for event in result.tool_calls
            ],
            "tool_results": [
                {
                    "tool_name": event.tool_name,
                    "payload": event.payload,
                    "timestamp": event.timestamp,
                }
                for event in result.tool_results
            ],
            "events": [
                {
                    "kind": event.kind,
                    "text": event.text,
                    "tool_name": event.tool_name,
                    "payload": event.payload,
                    "timestamp": event.timestamp,
                }
                for event in result.events
            ],
            "warnings": result.warnings,
            "summary": result.summary,
            "usage": result.usage,
            "timing": _runtime_timing_payload(result),
            "duckdb_metrics": self.manager.duckdb.get_metrics(),
        }
        trace_path = self._write_trace(result, trace_payload)
        result.payload = {
            "trace_path": trace_path.as_posix(),
            "warnings": result.warnings,
        }
        if should_replay:
            self.manager.replay_cache.save(
                cache_key,
                {
                    "summary": result.summary,
                    "model": model_name,
                    "events": trace_payload["events"],
                    "warnings": result.warnings,
                    "usage": result.usage,
                    "payload": result.payload,
                    "timing": _runtime_timing_payload(result),
                },
            )
        self.manager._record_agent_observability(
            handle=self,
            result=result,
            runtime_context=runtime_context,
            cache_payload=cache_payload,
        )
        self._append_memory(result)
        self._append_run_artifact_summary(result, runtime_context)
        self._log_run_summary(result, runtime_context)
        return result


class AgentManager:
    def __init__(self, strategy: Any) -> None:
        self.strategy = strategy
        self._agents: dict[str, AgentHandle] = {}
        self._warned_backtest_mcp_tools: set[tuple[str, str]] = set()
        self._model_call_count = 0
        agent_replay_cache_class, _ = _get_replay_imports()
        self.replay_cache = agent_replay_cache_class()
        self.duckdb = _get_duckdb_query_layer_class()(strategy)
        self._observability_totals: dict[str, dict[str, int]] = {}
        self._observability_call_index: dict[str, int] = {}
        self._observability_rows: dict[str, list[dict[str, Any]]] = {}
        self._observability_all_rows: list[dict[str, Any]] = []

    def __getitem__(self, item: str) -> AgentHandle:
        return self._agents[item]

    def _reserve_model_call(self, *, agent_name: str, model: str) -> None:
        limit = _agent_model_call_limit(self.strategy)
        params = getattr(self.strategy, "parameters", None)
        if limit is not None and self._model_call_count >= limit:
            raise AgentModelCallLimitExceeded(
                f"LUMIBOT_AGENT_MAX_MODEL_CALLS/agent_max_model_calls limit reached "
                f"before agent={agent_name!r} model={model!r}. "
                f"Configured limit={limit}, attempted_call={self._model_call_count + 1}."
            )
        self._model_call_count += 1
        if isinstance(params, dict):
            params["agent_model_calls"] = self._model_call_count
            if limit is not None:
                params["agent_max_model_calls"] = limit

    def _artifact_path(self, agent_name: str, suffix: str) -> Path:
        stats_file = getattr(self.strategy, "stats_file", None) or getattr(self.strategy, "_stats_file", None)
        if isinstance(stats_file, str) and stats_file.strip():
            stats_path = Path(stats_file)
            stats_suffix = "_stats.csv"
            if stats_path.name.endswith(stats_suffix):
                prefix = stats_path.name[: -len(stats_suffix)]
                return stats_path.with_name(f"{prefix}{suffix}")
            return stats_path.with_name(f"{stats_path.stem}{suffix}")
        fallback_dir = Path(os.environ.get("LUMIBOT_CACHE_FOLDER") or LUMIBOT_CACHE_FOLDER) / "agent_runtime"
        fallback_dir.mkdir(parents=True, exist_ok=True)
        return fallback_dir / f"{agent_name}{suffix}"

    def _write_detail_rows(
        self,
        *,
        handle: AgentHandle,
        result: AgentRunResult,
        runtime_context: dict[str, Any],
        cache_payload: dict[str, Any],
        usage: dict[str, int],
        call_index: int,
    ) -> Path:
        detail_path = self._artifact_path(handle.name, "_agent_detail.parquet")
        detail_path.parent.mkdir(parents=True, exist_ok=True)
        trace_path = ""
        if isinstance(result.payload, dict):
            trace_path = str(result.payload.get("trace_path") or "")
        warning_messages = " | ".join(_sanitize_csv_text(message) for message in result.warning_messages if message)
        normalized_events = result.events or [AgentTraceEvent(kind="text", text=result.summary or "")]
        thinking_texts = _thinking_texts(result)
        final_texts = _visible_model_texts(result)
        final_text = " || ".join(final_texts) if final_texts else _sanitize_csv_text(result.summary or result.text or "")
        thinking_text = " || ".join(thinking_texts)
        tool_sequence = " -> ".join(event.tool_name or "unknown_tool" for event in result.tool_calls)
        task_prompt = _sanitize_csv_text(cache_payload.get("task_prompt") or "")
        user_system_prompt = _sanitize_csv_text(cache_payload.get("user_system_prompt") or "")
        base_system_prompt = _sanitize_csv_text(cache_payload.get("base_system_prompt") or "")
        effective_system_prompt = _sanitize_csv_text(cache_payload.get("effective_system_prompt") or "")
        context_text = _flatten_csv_value(cache_payload.get("context") or {})
        runtime_context_text = _flatten_csv_value(cache_payload.get("runtime_context") or {})
        timing = _runtime_timing_payload(result)
        common: dict[str, Any] = {
            "timestamp": result.ended_at or handle._event_timestamp(),
            "call_index": call_index,
            "agent_name": handle.name,
            "model": result.model,
            "mode": runtime_context.get("mode"),
            "cache_hit": bool(result.cache_hit),
            "summary": _sanitize_csv_text(result.summary or result.text or ""),
            "final_text": final_text,
            "thinking_text": thinking_text,
            "thinking_captured": bool(thinking_texts),
            "tool_sequence": tool_sequence,
            "tool_call_count": len(result.tool_calls),
            "event_count": len(normalized_events),
            "task_prompt": task_prompt,
            "user_system_prompt": user_system_prompt,
            "base_system_prompt": base_system_prompt,
            "effective_system_prompt": effective_system_prompt,
            "context_text": context_text,
            "runtime_context_text": runtime_context_text,
            "warning_messages": warning_messages,
            **timing,
            "trace_path": trace_path,
        }
        rows: list[dict[str, Any]] = []
        rows.append(
            {
                **common,
                "timestamp": result.ended_at or handle._event_timestamp(),
                "event_index": 0,
                "event_kind": "call_summary",
                "is_call_summary": True,
                "tool_name": "",
                "event_detail": (
                    f"events={len(normalized_events)} tool_calls={len(result.tool_calls)} "
                    f"cache_hit={bool(result.cache_hit)}"
                ),
                "event_payload_json": _payload_json_text(
                    {
                        "usage": result.usage or {},
                        "warnings": result.warnings,
                        "timing": timing,
                    }
                ),
                "event_text": "",
                "event_input_tokens": 0,
                "event_output_tokens": 0,
                "event_total_tokens": 0,
                "event_thinking_tokens": 0,
                "event_cached_input_tokens": 0,
                "event_cache_write_input_tokens": 0,
                "event_uncached_input_tokens": 0,
                "event_tool_use_input_tokens": 0,
                "call_input_tokens": usage["input_tokens"],
                "call_output_tokens": usage["output_tokens"],
                "call_total_tokens": usage["total_tokens"],
                "call_thinking_tokens": usage["thinking_tokens"],
                "call_cached_input_tokens": usage["cached_input_tokens"],
                "call_cache_write_input_tokens": usage["cache_write_input_tokens"],
                "call_uncached_input_tokens": usage["uncached_input_tokens"],
                "call_tool_use_input_tokens": usage["tool_use_input_tokens"],
            }
        )
        for event_index, event in enumerate(normalized_events, start=1):
            event_usage = _event_usage_breakdown(event)
            rows.append(
                {
                    **common,
                    "timestamp": event.timestamp or handle._event_timestamp(),
                    "event_index": event_index,
                    "event_kind": event.kind,
                    "is_call_summary": False,
                    "tool_name": event.tool_name or "",
                    "event_detail": _summarize_tool_payload(event.tool_name, event.payload),
                    "event_payload_json": _payload_json_text(event.payload),
                    "event_text": _sanitize_csv_text(event.text or ""),
                    "event_input_tokens": event_usage["input_tokens"],
                    "event_output_tokens": event_usage["output_tokens"],
                    "event_total_tokens": event_usage["total_tokens"],
                    "event_thinking_tokens": event_usage["thinking_tokens"],
                    "event_cached_input_tokens": event_usage["cached_input_tokens"],
                    "event_cache_write_input_tokens": event_usage["cache_write_input_tokens"],
                    "event_uncached_input_tokens": event_usage["uncached_input_tokens"],
                    "event_tool_use_input_tokens": event_usage["tool_use_input_tokens"],
                    "call_input_tokens": 0,
                    "call_output_tokens": 0,
                    "call_total_tokens": 0,
                    "call_thinking_tokens": 0,
                    "call_cached_input_tokens": 0,
                    "call_cache_write_input_tokens": 0,
                    "call_uncached_input_tokens": 0,
                    "call_tool_use_input_tokens": 0,
                }
            )
        agent_rows = self._observability_rows.setdefault(handle.name, [])
        agent_rows.extend(rows)
        self._observability_all_rows.extend(rows)
        configured_stats_file = getattr(self.strategy, "stats_file", None) or getattr(self.strategy, "_stats_file", None)
        if isinstance(configured_stats_file, str) and configured_stats_file:
            detail_rows = self._observability_all_rows
        else:
            detail_rows = agent_rows
        detail_df = _get_pandas().DataFrame(detail_rows, columns=_AGENT_DETAIL_COLUMNS)
        (
            coerce_object_columns_to_json_strings,
            is_parquet_required,
            write_parquet_with_logging,
        ) = _get_parquet_utils()
        write_parquet_with_logging(
            df=detail_df,
            path=str(detail_path),
            artifact=f"agent_detail:{handle.name}",
            logger=getattr(self.strategy, "logger", None) or self,
            index=False,
            required=is_parquet_required(),
            compression="zstd",
            sanitizer=coerce_object_columns_to_json_strings,
        )
        return detail_path

    def _log_warning(self, message: str) -> None:
        try:
            logger = getattr(self.strategy, "logger", None)
            if logger is not None and hasattr(logger, "warning"):
                logger.warning(message)
                return
        except Exception:
            pass

    def info(self, message: str, *args: Any, **kwargs: Any) -> None:
        logger = getattr(self.strategy, "logger", None)
        if logger is not None and hasattr(logger, "info"):
            try:
                logger.info(message, *args, **kwargs)
                return
            except Exception:
                pass

    def warning(self, message: str, *args: Any, **kwargs: Any) -> None:
        if args:
            try:
                message = message % args
            except Exception:
                message = f"{message} {' '.join(str(arg) for arg in args)}"
        self._log_warning(message)

    def error(self, message: str, *args: Any, **kwargs: Any) -> None:
        if args:
            try:
                message = message % args
            except Exception:
                message = f"{message} {' '.join(str(arg) for arg in args)}"
        try:
            logger = getattr(self.strategy, "logger", None)
            if logger is not None and hasattr(logger, "error"):
                logger.error(message, **kwargs)
                return
        except Exception:
            pass
        self._log_warning(message)
        try:
            sys.stderr.write(f"{message}\n")
            sys.stderr.flush()
        except Exception:
            pass

    def _update_strategy_parameters_for_agent(
        self,
        *,
        agent_name: str,
        model: str,
        usage: dict[str, int],
        detail_parquet_path: Path,
        cache_hit: bool,
        tool_call_count: int,
        latency_ms: int | None,
        first_event_latency_ms: int | None,
    ) -> None:
        params = getattr(self.strategy, "parameters", None)
        if not isinstance(params, dict):
            return
        totals = self._observability_totals.setdefault(
            agent_name,
            {
                "calls": 0,
                "cache_hits": 0,
                "tool_calls": 0,
                "input_tokens": 0,
                "output_tokens": 0,
                "total_tokens": 0,
                "thinking_tokens": 0,
                "cached_input_tokens": 0,
                "cache_write_input_tokens": 0,
                "uncached_input_tokens": 0,
                "tool_use_input_tokens": 0,
                "latency_ms": 0,
                "first_event_latency_ms": 0,
            },
        )
        totals["calls"] += 1
        totals["tool_calls"] += int(tool_call_count)
        if cache_hit:
            totals["cache_hits"] += 1
        for key in (
            "input_tokens",
            "output_tokens",
            "total_tokens",
            "thinking_tokens",
            "cached_input_tokens",
            "cache_write_input_tokens",
            "uncached_input_tokens",
            "tool_use_input_tokens",
        ):
            totals[key] += int(usage.get(key, 0) or 0)
        if latency_ms is not None:
            totals["latency_ms"] += int(latency_ms)
        if first_event_latency_ms is not None:
            totals["first_event_latency_ms"] += int(first_event_latency_ms)

        prefix = f"agent_{agent_name}_"
        params[f"{prefix}model"] = model
        params[f"{prefix}calls"] = totals["calls"]
        params[f"{prefix}cache_hits"] = totals["cache_hits"]
        params[f"{prefix}tool_calls"] = totals["tool_calls"]
        params[f"{prefix}input_tokens"] = totals["input_tokens"]
        params[f"{prefix}output_tokens"] = totals["output_tokens"]
        params[f"{prefix}total_tokens"] = totals["total_tokens"]
        params[f"{prefix}thinking_tokens"] = totals["thinking_tokens"]
        params[f"{prefix}cached_input_tokens"] = totals["cached_input_tokens"]
        params[f"{prefix}cache_write_input_tokens"] = totals["cache_write_input_tokens"]
        params[f"{prefix}uncached_input_tokens"] = totals["uncached_input_tokens"]
        params[f"{prefix}tool_use_input_tokens"] = totals["tool_use_input_tokens"]
        params[f"{prefix}latency_ms_total"] = totals["latency_ms"]
        params[f"{prefix}latency_ms_avg"] = round(totals["latency_ms"] / totals["calls"], 2) if totals["calls"] else 0
        params[f"{prefix}first_event_latency_ms_avg"] = (
            round(totals["first_event_latency_ms"] / totals["calls"], 2) if totals["calls"] else 0
        )
        params[f"{prefix}detail_parquet"] = str(detail_parquet_path)

    def _record_agent_observability(
        self,
        *,
        handle: AgentHandle,
        result: AgentRunResult,
        runtime_context: dict[str, Any],
        cache_payload: dict[str, Any],
    ) -> None:
        usage = _usage_breakdown(result.usage, cache_hit=bool(result.cache_hit))
        call_index = self._observability_call_index.get(handle.name, 0) + 1
        self._observability_call_index[handle.name] = call_index
        detail_parquet_path = self._write_detail_rows(
            handle=handle,
            result=result,
            runtime_context=runtime_context,
            cache_payload=cache_payload,
            usage=usage,
            call_index=call_index,
        )
        self._update_strategy_parameters_for_agent(
            agent_name=handle.name,
            model=result.model,
            usage=usage,
            detail_parquet_path=detail_parquet_path,
            cache_hit=bool(result.cache_hit),
            tool_call_count=len(result.tool_calls),
            latency_ms=result.latency_ms,
            first_event_latency_ms=result.first_event_latency_ms,
        )

    def create(
        self,
        *,
        name: str,
        system_prompt: str | None = None,
        model: str | None = None,
        default_model: str | None = None,
        tools: list[Any] | None = None,
        mcp_servers: list[MCPServer] | None = None,
        prompt: str | None = None,
        cadence: str | None = None,
        allow_trading: bool | None = None,
        _runtime: Any | None = None,
    ) -> AgentHandle:
        if name in self._agents:
            raise ValueError(f"Agent with name {name!r} already exists.")
        resolved_system_prompt = system_prompt or prompt or "You are a LumiBot trading agent."
        if model is not None and default_model is not None and model != default_model:
            raise ValueError("Pass either model or default_model, not both with different values.")
        resolved_model = model or default_model or "gemini-3.1-flash-lite-preview"
        resolved_allow_trading = True if allow_trading is None else bool(allow_trading)
        handle = AgentHandle(
            manager=self,
            name=name,
            system_prompt=resolved_system_prompt,
            default_model=resolved_model,
            tools=tools,
            mcp_servers=mcp_servers,
            runtime=_runtime,
            allow_trading=resolved_allow_trading,
        )
        if cadence is not None:
            self.strategy.log_message(
                f"[agents] cadence={cadence!r} is informational only; scheduling stays in strategy lifecycle code.",
                color="yellow",
            )
        self._agents[name] = handle

        # Auto-populate strategy.parameters with this agent's model id so the
        # tearsheet's "Parameters Used" panel self-identifies which LLM the
        # strategy used. Works for multi-agent strategies (each gets its own
        # key). Non-fatal if strategy.parameters is unset or not a dict —
        # some test doubles or legacy strategies don't set it.
        try:
            params = getattr(self.strategy, "parameters", None)
            if isinstance(params, dict):
                params[f"agent_{name}_model"] = resolved_model
        except Exception:
            pass

        return handle
