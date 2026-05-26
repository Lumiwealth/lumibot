from datetime import datetime, timezone

import pytest

from lumibot.components.agents import AgentManager, AgentRunResult, AgentTraceEvent
from lumibot.components.agents.manager import AgentModelCallLimitExceeded
from lumibot.components.agents.schemas import BoundTool, ToolDefinition


class _Vars(dict):
    def get(self, key, default=None):
        return super().get(key, default)

    def set(self, key, value):
        self[key] = value


class _Strategy:
    is_backtesting = True
    parameters = {}
    vars = _Vars()

    def get_datetime(self):
        return datetime(2026, 1, 2, tzinfo=timezone.utc)

    def log_message(self, *args, **kwargs):
        return None


class _Runtime:
    last_request = None

    def run(self, request):
        from lumibot.components.agents.runtime import _wrap_tool_callable

        type(self).last_request = request
        tool_map = {tool.name: _wrap_tool_callable(tool) for tool in request.bound_tools}
        assert "orders_submit_order" not in tool_map
        memory_result = tool_map["search_memory"](query="AAPL", limit=1)
        notify_result = tool_map["notify_user"](title="Test", message="Backtest dry run")
        events = [
            AgentTraceEvent(kind="tool_call", tool_name="search_memory", payload={"query": "AAPL"}),
            AgentTraceEvent(kind="tool_result", tool_name="search_memory", payload=memory_result),
            AgentTraceEvent(kind="tool_call", tool_name="notify_user", payload={"title": "Test"}),
            AgentTraceEvent(kind="tool_result", tool_name="notify_user", payload=notify_result),
            AgentTraceEvent(kind="text", text="Research completed without trading tools."),
        ]
        return AgentRunResult(summary="Research completed without trading tools.", model=request.model, events=events)


class _LongSummaryRuntime:
    requests = []

    def run(self, request):
        type(self).requests.append(request)
        summary = "x" * 5000
        return AgentRunResult(
            summary=summary,
            model=request.model,
            events=[AgentTraceEvent(kind="text", text=summary)],
        )


def test_agent_allow_trading_false_removes_only_mutating_order_tools(monkeypatch):
    monkeypatch.delenv("FRED_API_KEY", raising=False)
    strategy = _Strategy()
    manager = AgentManager(strategy)
    monkeypatch.setenv("FRED_API_KEY", "")

    agent = manager.create(name="researcher", model="openai/gpt-5.4-mini", allow_trading=False)
    tool_names = {tool.name for tool in agent._ensure_bound_tools()}

    assert "orders_submit_order" not in tool_names
    assert "orders_cancel_order" not in tool_names
    assert "orders_modify_order" not in tool_names
    assert "orders_open_orders" in tool_names
    assert "account_positions" in tool_names
    assert "get_income_statement" in tool_names
    assert "get_indicator" in tool_names
    assert "list_fred_series" not in tool_names
    assert "get_fred_series" not in tool_names
    assert "get_fred_latest" not in tool_names
    assert "get_fred_snapshot" not in tool_names
    assert agent.default_model == "openai/gpt-5.4-mini"


def test_agent_backtest_keeps_fred_tools_when_fred_api_key_is_set(monkeypatch):
    monkeypatch.setenv("FRED_API_KEY", "test-fred-key")
    strategy = _Strategy()
    manager = AgentManager(strategy)

    agent = manager.create(name="researcher_with_fred", model="openai/gpt-5.4-mini", allow_trading=False)
    tool_names = {tool.name for tool in agent._ensure_bound_tools()}

    assert "list_fred_series" in tool_names
    assert "get_fred_series" in tool_names
    assert "get_fred_latest" in tool_names
    assert "get_fred_snapshot" in tool_names


def test_read_only_tool_result_cache_is_shared_across_agent_handles():
    import inspect

    strategy = _Strategy()
    manager = AgentManager(strategy)
    calls = {"count": 0}

    def binder(strategy, manager):
        def cached_tool(symbol: str) -> dict:
            calls["count"] += 1
            return {"ok": True, "symbol": symbol, "count": calls["count"]}

        return BoundTool(
            name="cached_research_tool",
            description="Cached research tool.",
            function=cached_tool,
            source="builtin",
            metadata={"kind": "fundamentals", "cache_scope": "strategy_day"},
        )

    tool_definition = ToolDefinition(name="cached_research_tool", description="Cached research tool.", binder=binder)
    first = manager.create(name="first", tools=[tool_definition], allow_trading=False)
    second = manager.create(name="second", tools=[tool_definition], allow_trading=False)

    first_tool = next(tool for tool in first._ensure_bound_tools() if tool.name == "cached_research_tool")
    second_tool = next(tool for tool in second._ensure_bound_tools() if tool.name == "cached_research_tool")

    assert "symbol" in inspect.signature(first_tool.function).parameters
    assert first_tool.function(symbol="NVDA")["count"] == 1
    cached = second_tool.function(symbol="NVDA")
    assert cached["count"] == 1
    assert cached["_lumibot_tool_cache"]["hit"] is True
    assert calls["count"] == 1


def test_explicit_tool_scope_can_exclude_default_builtin_tools():
    strategy = _Strategy()
    manager = AgentManager(strategy)

    def binder(strategy, manager):
        def research_only_tool(symbol: str) -> dict:
            return {"symbol": symbol}

        return BoundTool(
            name="research_only_tool",
            description="Research-only test tool.",
            function=research_only_tool,
            source="custom",
        )

    tool_definition = ToolDefinition(name="research_only_tool", description="Research-only test tool.", binder=binder)
    agent = manager.create(
        name="scoped_researcher",
        tools=[tool_definition],
        allow_trading=False,
        include_builtin_tools=False,
    )

    tool_names = {tool.name for tool in agent._ensure_bound_tools()}

    assert tool_names == {"research_only_tool"}


def test_get_filings_future_report_date_does_not_trigger_lookahead_warning():
    strategy = _Strategy()
    manager = AgentManager(strategy)
    agent = manager.create(name="researcher", allow_trading=False)
    result = AgentRunResult(
        summary="RESULT: done",
        model="test",
        events=[
            AgentTraceEvent(
                kind="tool_call",
                tool_name="get_filings",
                payload={"symbol": "NVDA", "as_of": "2026-05-21"},
            ),
            AgentTraceEvent(
                kind="tool_result",
                tool_name="get_filings",
                payload={
                    "filings": [
                        {
                            "filing_date": "2026-05-12",
                            "acceptance_datetime": "2026-05-12T20:42:13.000Z",
                            "report_date": "2026-06-24",
                        }
                    ]
                },
            ),
        ],
    )

    warnings = agent._derive_warnings(
        result,
        {"mode": "backtesting", "current_datetime": "2026-05-21T13:30:00+00:00"},
    )

    assert not [warning for warning in warnings if warning["kind"] == "future_timestamp"]


def test_no_tool_warning_is_skipped_when_agent_has_no_tools():
    strategy = _Strategy()
    manager = AgentManager(strategy)
    agent = manager.create(name="debater", tools=[], include_builtin_tools=False, allow_trading=False)
    result = AgentRunResult(
        summary="RESULT: reasoned from context",
        model="test",
        events=[AgentTraceEvent(kind="text", text="RESULT: reasoned from context")],
    )

    warnings = agent._derive_warnings(
        result,
        {"mode": "backtesting", "current_datetime": "2026-05-21T13:30:00+00:00"},
    )

    assert not [warning for warning in warnings if warning["kind"] == "no_tool_calls"]


def test_order_tool_serialization_handles_uuid_identifiers():
    import json
    from uuid import uuid4

    from lumibot.components.agents.builtins import _order_to_dict

    class _Order:
        identifier = uuid4()
        status = "submitted"
        side = "buy"
        asset = None
        quantity = 1
        order_type = "market"
        time_in_force = "day"
        limit_price = None
        stop_price = None

    payload = _order_to_dict(_Order())

    json.dumps(payload)
    assert isinstance(payload["identifier"], str)


def test_builtin_indicator_schema_is_gemini_function_declaration_compatible():
    pytest.importorskip("google.adk.tools.function_tool")
    from google.adk.tools.function_tool import FunctionTool

    from lumibot.components.agents.runtime import _wrap_tool_callable

    strategy = _Strategy()
    manager = AgentManager(strategy)
    agent = manager.create(name="researcher", model="gemini-3.1-flash-lite-preview", allow_trading=False)
    indicator_tool = next(tool for tool in agent._ensure_bound_tools() if tool.name == "get_indicator")

    declaration = FunctionTool(_wrap_tool_callable(indicator_tool))._get_declaration().model_dump(exclude_none=True)
    schema_text = str(declaration)
    parameters = declaration.get("parameters") or declaration.get("parameters_json_schema") or {}

    assert "additional_properties" not in schema_text
    assert "parameters_json" in parameters["properties"]


def test_agent_allow_trading_true_keeps_mutating_order_tools():
    strategy = _Strategy()
    manager = AgentManager(strategy)

    agent = manager.create(name="trader", model="openai/gpt-5.5", allow_trading=True)
    tool_names = {tool.name for tool in agent._ensure_bound_tools()}

    assert "orders_submit_order" in tool_names
    assert "orders_cancel_order" in tool_names
    assert "orders_modify_order" in tool_names
    assert "orders_open_orders" in tool_names
    assert agent.default_model == "openai/gpt-5.5"


def test_read_only_agent_runtime_can_use_non_trading_tools(tmp_path):
    from lumibot.components.memory import MemoryStore
    from lumibot.components.notifications import NotificationManager

    strategy = _Strategy()
    strategy.is_backtesting = False
    strategy.memory = MemoryStore(strategy, root_dir=tmp_path)
    strategy.notifications = NotificationManager(strategy)
    strategy.notify = lambda title, message, **kwargs: strategy.notifications.notify(title, message, **kwargs)
    manager = AgentManager(strategy)
    runtime = _Runtime()

    agent = manager.create(
        name="researcher_runtime",
        model="openai/gpt-5.4-mini",
        allow_trading=False,
        _runtime=runtime,
    )
    result = agent.run(task_prompt="Research without trading.")

    assert result.summary == "Research completed without trading tools."
    assert _Runtime.last_request.model == "openai/gpt-5.4-mini"
    assert any(event.tool_name == "search_memory" for event in result.tool_calls)
    assert any(event.tool_name == "notify_user" for event in result.tool_calls)


def test_agent_runtime_memory_notes_are_compacted(monkeypatch, tmp_path):
    from lumibot.components.memory import MemoryStore
    from lumibot.components.notifications import NotificationManager

    monkeypatch.setenv("LUMIBOT_AGENT_MEMORY_NOTE_MAX_CHARS", "500")
    strategy = _Strategy()
    strategy.vars = _Vars()
    strategy.is_backtesting = False
    strategy.memory = MemoryStore(strategy, root_dir=tmp_path)
    strategy.notifications = NotificationManager(strategy)
    manager = AgentManager(strategy)
    runtime = _LongSummaryRuntime()
    _LongSummaryRuntime.requests = []

    agent = manager.create(
        name="compact_memory",
        model="openai/gpt-5.4-mini",
        allow_trading=False,
        _runtime=runtime,
    )
    agent.run(task_prompt="first long summary")
    agent.run(task_prompt="second should receive compact prior summary")

    assert len(_LongSummaryRuntime.requests) == 2
    prior_notes = _LongSummaryRuntime.requests[1].memory_notes
    assert len(prior_notes) == 1
    assert len(prior_notes[0]["summary"]) == 500
    assert prior_notes[0]["summary"].endswith("...")


def test_agent_model_call_limit_stops_before_runtime_call(monkeypatch):
    monkeypatch.setenv("LUMIBOT_AGENT_MAX_MODEL_CALLS", "1")
    strategy = _Strategy()
    strategy.vars = _Vars()
    strategy.is_backtesting = False
    manager = AgentManager(strategy)
    runtime = _LongSummaryRuntime()
    _LongSummaryRuntime.requests = []

    agent = manager.create(
        name="limited",
        model="openai/gpt-5.4-mini",
        allow_trading=False,
        _runtime=runtime,
    )

    agent.run(task_prompt="first call is allowed")
    with pytest.raises(AgentModelCallLimitExceeded):
        agent.run(task_prompt="second call is blocked before provider spend")

    assert len(_LongSummaryRuntime.requests) == 1
    assert strategy.parameters["agent_model_calls"] == 1
    assert strategy.parameters["agent_max_model_calls"] == 1
