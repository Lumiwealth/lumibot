import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from lumibot.constants import LUMIBOT_CACHE_FOLDER

from .builtins import _order_to_dict, _position_to_dict
from .duckdb_tools import DuckDBQueryLayer
from .replay_cache import AgentReplayCache, _normalize_json
from .runtime import GoogleADKRuntime, RuntimeRequest, StubAgentRuntime, call_mcp_tool
from .schemas import AgentRunResult, AgentTraceEvent, BoundTool, MCPServer, ToolDefinition
from .tools import bind_callable_tool


_TIMESTAMP_HINT_RE = re.compile(
    r"(time|date|datetime|published|updated|created|accepted|released|release|as_of|realtime)",
    re.IGNORECASE,
)


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
    broker = getattr(strategy, "broker", None)
    rows = list(getattr(broker, "_trade_event_log_rows", []) or [])
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


def _compact_json(value: Any, limit: int = 240) -> str:
    try:
        text = json.dumps(_normalize_json(value), sort_keys=True)
    except Exception:
        text = repr(value)
    return _truncate_text(text, limit=limit)


def _unwrap_tool_payload(payload: Any) -> Any:
    if isinstance(payload, dict) and set(payload.keys()) == {"payload"} and isinstance(payload.get("payload"), dict):
        return payload["payload"]
    return payload


def _summarize_tool_payload(tool_name: str | None, payload: Any) -> str:
    payload = _unwrap_tool_payload(payload)
    if not isinstance(payload, dict):
        return _truncate_text(payload)

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
        return _truncate_text(
            f"count={payload.get('count', len(articles))} "
            f"window=({payload.get('window_start')} -> {payload.get('window_end')}) "
            f"headlines={headline_parts}"
        )

    if "row_count" in payload and "table_name" in payload:
        return _truncate_text(
            f"table={payload.get('table_name')} symbol={payload.get('symbol')} "
            f"rows={payload.get('row_count')} timestep={payload.get('timestep')} "
            f"loaded_at={payload.get('loaded_at')}"
        )

    if "rows" in payload and "row_count" in payload:
        rows = payload.get("rows") or []
        sample = rows[0] if rows else {}
        return _truncate_text(
            f"rows={payload.get('row_count')} sample={_compact_json(sample, limit=140)}"
        )

    if "positions" in payload and isinstance(payload["positions"], list):
        labels: list[str] = []
        for position in payload["positions"]:
            if not isinstance(position, dict):
                continue
            asset = position.get("asset") or {}
            symbol = asset.get("symbol") if isinstance(asset, dict) else asset
            labels.append(f"{symbol}:{position.get('quantity')}")
        return _truncate_text(f"positions={labels}")

    if "cash" in payload and "portfolio_value" in payload:
        return _truncate_text(
            f"cash={payload.get('cash')} portfolio_value={payload.get('portfolio_value')} "
            f"datetime={payload.get('datetime')}"
        )

    if "identifier" in payload or "status" in payload:
        return _truncate_text(
            f"identifier={payload.get('identifier')} status={payload.get('status')} "
            f"symbol={payload.get('symbol')} side={payload.get('side')} quantity={payload.get('quantity')}"
        )

    return _compact_json(payload)


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
    ) -> None:
        self.manager = manager
        self.name = name
        self.system_prompt = system_prompt
        self.default_model = default_model
        from .builtins import BuiltinTools
        builtin_tools = BuiltinTools.all()
        if tools is None:
            self._tool_inputs = builtin_tools
        else:
            # Always include built-in tools, plus any custom tools the user added
            self._tool_inputs = builtin_tools + list(tools)
        self._mcp_servers = mcp_servers or []
        self._runtime = runtime or GoogleADKRuntime(mcp_servers=self._mcp_servers)
        self._bound_tools: list[BoundTool] | None = None

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
        for note in self._memory_notes():
            if not isinstance(note, dict):
                continue
            projected.append(
                {
                    "timestamp": note.get("timestamp"),
                    "summary": note.get("summary") or "",
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
        current_datetime = runtime_context.get("current_datetime") or "unknown"
        timezone_name = runtime_context.get("timezone") or "unknown"
        lines = [
            "You are operating as a trading agent inside LumiBot.",
            "Use the provided runtime context and tool outputs as the ground truth for the current state of the strategy.",
            "Ground claims in tool results or runtime context instead of unsupported prior knowledge or vague market memory.",
            "If evidence is weak, conflicting, stale, or incomplete, prefer doing nothing and explain why.",
            f"Execution mode: {mode}.",
            f"Current datetime: {current_datetime}.",
            f"Current timezone: {timezone_name}.",
            "Runtime context JSON will include current positions, cash, equity/portfolio value, recent orders, and recent trades.",
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
                self.system_prompt.strip(),
            ]
        ).strip()

    def _append_memory(self, result: AgentRunResult) -> None:
        state = self._state_bucket()
        notes = self._memory_notes()
        event_timestamp = self._event_timestamp()
        notes.append(
            {
                "timestamp": event_timestamp,
                "summary": result.summary or result.text or "",
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
                bound.append(entry.binder(self.manager.strategy, self.manager))
                continue
            bound.append(bind_callable_tool(entry).binder(self.manager.strategy, self.manager))
        bound.extend(self._build_remote_tools())
        self._bound_tools = bound
        return bound

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
        return AgentRunResult(
            summary=cached.get("summary"),
            model=cached.get("model") or self.default_model,
            events=events,
            cache_hit=True,
            cache_key=cache_key,
            usage=cached.get("usage"),
            payload=cached.get("payload"),
            warnings=list(cached.get("warnings") or []),
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
            name.startswith("market_") or name.startswith("duckdb_") or name.startswith("account_") or name in {"get_news", "fred_search", "fred_get_series"}
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
        trace_path = ""
        if isinstance(result.payload, dict):
            trace_path = str(result.payload.get("trace_path") or "")
        summary = (result.summary or result.text or "").replace("\n", " ").strip()
        message = (
            f"[agents] name={self.name} mode={runtime_context.get('mode')} "
            f"model={result.model} cache_hit={result.cache_hit} "
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
                self._append_memory(result)
                self._append_run_artifact_summary(result, runtime_context)
                self._log_run_summary(result, runtime_context)
                return result

        request = RuntimeRequest(
            agent_name=self.name,
            model=model_name,
            system_prompt=effective_system_prompt,
            task_prompt=task_prompt,
            context=context,
            runtime_context=runtime_context,
            memory_notes=self._memory_prompt_notes(),
            bound_tools=self._ensure_bound_tools(),
        )
        result = self._runtime.run(request)
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
                },
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
        self.replay_cache = AgentReplayCache()
        self.duckdb = DuckDBQueryLayer(strategy)

    def __getitem__(self, item: str) -> AgentHandle:
        return self._agents[item]

    def create(
        self,
        *,
        name: str,
        system_prompt: str | None = None,
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
        resolved_model = default_model or "gemini-3.1-flash-lite-preview"
        handle = AgentHandle(
            manager=self,
            name=name,
            system_prompt=resolved_system_prompt,
            default_model=resolved_model,
            tools=tools,
            mcp_servers=mcp_servers,
            runtime=_runtime,
        )
        if cadence is not None:
            self.strategy.log_message(
                f"[agents] cadence={cadence!r} is informational only; scheduling stays in strategy lifecycle code.",
                color="yellow",
            )
        if allow_trading is not None:
            self.strategy.log_message(
                f"[agents] allow_trading={allow_trading!r} is deprecated; tool exposure now controls mutations.",
                color="yellow",
            )
        self._agents[name] = handle
        return handle
