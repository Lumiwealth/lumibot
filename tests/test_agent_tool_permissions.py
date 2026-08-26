import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from lumibot.components.agents import AgentManager, AgentRunResult, AgentTraceEvent, BuiltinTools
from lumibot.components.agents.manager import AgentModelCallLimitExceeded, _structured_operation_outcomes
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


class _OrderReadinessStrategy(_Strategy):
    def __init__(self):
        self.submitted_orders = []

    def get_positions(self, include_cash_positions=True):
        return []

    def get_cash(self):
        return 100000.0

    def get_portfolio_value(self):
        return 100000.0

    def get_last_price(self, asset, quote=None, exchange=None):
        return 100.0

    def create_order(self, asset, quantity, side, **kwargs):
        return SimpleNamespace(
            identifier="test-order",
            status="new",
            side=side,
            asset=asset,
            quantity=quantity,
            order_type=kwargs.get("order_type", "market"),
            time_in_force=kwargs.get("time_in_force", "day"),
            limit_price=kwargs.get("limit_price"),
            stop_price=kwargs.get("stop_price"),
        )

    def submit_order(self, order):
        self.submitted_orders.append(order)
        return order


def _wrap_builtin_tools(strategy, tool_definitions):
    from lumibot.components.agents.runtime import _wrap_tool_callable

    manager = AgentManager(strategy)
    tools = [definition.binder(strategy, manager) for definition in tool_definitions]
    tool_context = {
        "agent_name": "trader",
        "model_call_id": "test-model-call",
        "enforce_order_readiness": True,
        "tool_calls": [],
    }
    return {tool.name: _wrap_tool_callable(tool, tool_context) for tool in tools}


class _Runtime:
    last_request = None

    def run(self, request):
        from lumibot.components.agents.runtime import _wrap_tool_callable

        type(self).last_request = request
        tool_context = {"agent_name": request.agent_name, "model_call_id": request.model_call_id}
        tool_map = {tool.name: _wrap_tool_callable(tool, tool_context) for tool in request.bound_tools}
        assert "orders_submit_order" not in tool_map
        assert "remember_decision" not in tool_map
        assert "remember_proposal" in tool_map
        assert "remember_risk_note" in tool_map
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


class _CaptureRuntime:
    requests = []

    def run(self, request):
        type(self).requests.append(request)
        return AgentRunResult(
            summary="Captured runtime request.",
            model=request.model,
            events=[AgentTraceEvent(kind="text", text="Captured runtime request.")],
        )


class _MissingProviderCredentialRuntime:
    def run(self, request):
        raise ValueError("No API key was provided")


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
    assert "remember_decision" not in tool_names
    assert "remember_proposal" in tool_names
    assert "remember_risk_note" in tool_names
    assert "orders_open_orders" in tool_names
    assert "account_positions" in tool_names
    assert "get_income_statement" in tool_names
    assert "get_indicator" in tool_names
    assert "list_fred_series" not in tool_names
    assert "get_fred_series" not in tool_names
    assert "get_fred_latest" not in tool_names
    assert "get_fred_snapshot" not in tool_names
    assert agent.default_model == "openai/gpt-5.4-mini"


def test_live_agent_auth_failure_emits_structured_decision_outcome():
    strategy = _Strategy()
    strategy.is_backtesting = False
    manager = AgentManager(strategy)
    agent = manager.create(
        name="portfolio_manager",
        model="gemini/gemini-3.1-pro-preview",
        allow_trading=True,
        _runtime=_MissingProviderCredentialRuntime(),
    )

    result = agent.run(task_prompt="Make the scheduled investment decision.")

    assert result.payload["execution_outcome"] == {
        "operation": "managed_ai_inference",
        "requiredness": "decision_critical",
        "retryability": "non_retryable",
        "fallback_used": True,
        "decision_completed": False,
        "broker_state_certainty": "not_observed",
        "impact": "decision_blocked",
        "error_category": "auth",
    }


def test_optional_agent_auth_failure_does_not_mark_decision_blocked():
    strategy = _Strategy()
    strategy.is_backtesting = False
    manager = AgentManager(strategy)
    agent = manager.create(
        name="optional_researcher",
        model="anthropic/claude-sonnet-4-6",
        allow_trading=False,
        _runtime=_MissingProviderCredentialRuntime(),
    )

    result = agent.run(task_prompt="Enrich the completed decision with optional research.")

    assert result.payload["execution_outcome"] == {
        "operation": "managed_ai_inference",
        "requiredness": "optional",
        "retryability": "non_retryable",
        "fallback_used": True,
        "decision_completed": False,
        "broker_state_certainty": "not_observed",
        "impact": "optional_component_failed",
        "error_category": "auth",
    }


def test_agent_timeout_options_forward_to_runtime_request(monkeypatch):
    monkeypatch.delenv("FRED_API_KEY", raising=False)
    _CaptureRuntime.requests = []
    strategy = _Strategy()
    strategy.vars = _Vars()
    strategy.is_backtesting = False
    manager = AgentManager(strategy)
    manager.create(
        name="timed",
        system_prompt="Capture timeouts.",
        model="gemini-3.5-flash",
        tools=[],
        include_builtin_tools=False,
        _runtime=_CaptureRuntime(),
        model_request_timeout_seconds=123,
        run_timeout_seconds=456,
    )

    manager["timed"].run(task_prompt="Use defaults.")
    manager["timed"].run(
        task_prompt="Use overrides.",
        model_request_timeout_seconds=7,
        run_timeout_seconds=8,
    )

    assert _CaptureRuntime.requests[0].model_request_timeout_seconds == 123
    assert _CaptureRuntime.requests[0].run_timeout_seconds == 456
    assert _CaptureRuntime.requests[1].model_request_timeout_seconds == 7
    assert _CaptureRuntime.requests[1].run_timeout_seconds == 8


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


def test_order_submit_tool_records_memory_event(monkeypatch, tmp_path):
    import pandas as pd

    from lumibot.components.agents.builtins import _bind_submit_order
    from lumibot.components.agents.runtime import _wrap_tool_callable
    from lumibot.components.memory import MemoryStore

    class _Asset:
        symbol = "TQQQ"
        asset_type = "stock"

    class _Order:
        identifier = "order-123"
        status = "submitted"
        side = "buy"
        asset = _Asset()
        quantity = 10
        order_type = "market"
        time_in_force = "day"
        limit_price = None
        stop_price = None

    class _OrderStrategy(_Strategy):
        def create_order(self, *args, **kwargs):
            return _Order()

        def submit_order(self, order):
            return order

    strategy = _OrderStrategy()
    strategy.memory = MemoryStore(strategy, root_dir=tmp_path)
    decision = strategy.memory.remember_decision(
        "Buy TQQQ after the committee approved the risk-adjusted entry.",
        symbol="TQQQ",
        action="buy",
        agent_name="trader",
        model_call_id="call-order-1",
    )
    monkeypatch.setenv("BOTSPOT_DEPLOYMENT_ID", "deployment-123")
    monkeypatch.setenv("BOTSPOT_ARTIFACT_RUN_ID", "run-456")
    monkeypatch.setattr(
        "lumibot.components.agents.builtins.resolve_asset_and_quote",
        lambda *args, **kwargs: (_Asset(), None),
    )

    tool = _bind_submit_order(strategy, manager=None)
    wrapped = _wrap_tool_callable(tool, {"agent_name": "trader", "model_call_id": "call-order-1"})
    result = wrapped(symbol="TQQQ", quantity=10, side="buy")

    assert result["order"]["identifier"] == "order-123"
    assert result["order"]["decision_provenance"] == {
        "deployment_id": "deployment-123",
        "run_id": "run-456",
        "decision_id": decision["memory_id"],
        "model_call_id": "call-order-1",
    }
    events = pd.read_parquet(strategy.memory.export_artifacts(tmp_path, prefix="order_memory")["memory_events"])
    order_events = events[events["event_type"] == "order.submitted"]
    assert len(order_events) == 1
    assert order_events.iloc[0]["agent_name"] == "trader"
    assert order_events.iloc[0]["model_call_id"] == "call-order-1"
    event_metadata = json.loads(order_events.iloc[0]["metadata_json"])
    assert event_metadata["decision_provenance"] == result["order"]["decision_provenance"]


def test_order_submit_timeout_reports_unknown_broker_state(monkeypatch):
    from lumibot.components.agents.builtins import _bind_submit_order
    from lumibot.components.agents.runtime import _wrap_tool_callable

    class _Asset:
        symbol = "TQQQ"
        asset_type = "stock"

    class _Order:
        identifier = "order-timeout"
        asset = _Asset()
        quantity = 10
        side = "buy"
        order_type = "market"
        time_in_force = "day"
        limit_price = None
        stop_price = None

    class _TimeoutStrategy(_Strategy):
        def create_order(self, *args, **kwargs):
            return _Order()

        def submit_order(self, order):
            raise TimeoutError("broker response timed out")

    strategy = _TimeoutStrategy()
    monkeypatch.setattr(
        "lumibot.components.agents.builtins.resolve_asset_and_quote",
        lambda *args, **kwargs: (_Asset(), None),
    )

    result = _wrap_tool_callable(_bind_submit_order(strategy, manager=None))(
        symbol="TQQQ", quantity=10, side="buy"
    )

    assert result["tool_error"] is True
    assert result["execution_outcome"] == {
        "operation": "broker_order_submission",
        "requiredness": "decision_critical",
        "retryability": "retryable",
        "fallback_used": False,
        "decision_completed": True,
        "broker_state_certainty": "unknown",
        "impact": "operator_attention_required",
    }


def test_agent_summary_preserves_structured_tool_operation_outcomes(monkeypatch, tmp_path):
    ai_outcome = {
        "operation": "managed_ai_inference",
        "requiredness": "decision_critical",
        "decision_completed": True,
        "broker_state_certainty": "not_observed",
        "impact": "completed",
    }
    broker_outcome = {
        "operation": "broker_order_submission",
        "requiredness": "decision_critical",
        "retryability": "retryable",
        "fallback_used": False,
        "decision_completed": True,
        "broker_state_certainty": "unknown",
        "impact": "operator_attention_required",
    }
    result = AgentRunResult(
        summary="Submitted the rebalance.",
        model="openai/gpt-5.4-mini",
        payload={"execution_outcome": ai_outcome},
        events=[
            AgentTraceEvent(
                kind="tool_result",
                tool_name="orders_submit_order",
                payload={"payload": {"execution_outcome": broker_outcome}},
            )
        ],
    )

    assert _structured_operation_outcomes(result) == [ai_outcome, broker_outcome]

    monkeypatch.setenv("LUMIBOT_CACHE_FOLDER", str(tmp_path))
    monkeypatch.setenv("BOTSPOT_DEPLOYMENT_ID", "deployment-current")
    monkeypatch.setenv("BOTSPOT_RUN_ID", "run-current")
    handle = AgentManager(_Strategy()).create(
        name="trader",
        model="openai/gpt-5.4-mini",
        allow_trading=True,
    )
    handle._append_run_artifact_summary(result, {"mode": "live"})
    summary = json.loads((tmp_path / "agent_runtime" / "agent_run_summaries.jsonl").read_text())
    assert summary["deployment_id"] == "deployment-current"
    assert summary["run_id"] == "run-current"
    assert summary["operation_outcomes"] == [ai_outcome, broker_outcome]


def test_completed_agent_records_data_tool_error_as_optional_operation_failure():
    result = AgentRunResult(
        summary="Used the remaining data to complete the decision.",
        model="anthropic/claude-sonnet-4-6",
        payload={
            "execution_outcome": {
                "operation": "managed_ai_inference",
                "requiredness": "decision_critical",
                "decision_completed": True,
                "broker_state_certainty": "not_observed",
                "impact": "completed",
            }
        },
        events=[
            AgentTraceEvent(
                kind="tool_result",
                tool_name="account_positions",
                payload={
                    "tool_error": True,
                    "error": {"type": "ConnectionError", "message": "temporary outage"},
                },
            ),
            AgentTraceEvent(
                kind="tool_result",
                tool_name="account_positions",
                payload={"positions": []},
            ),
        ],
    )

    outcomes = _structured_operation_outcomes(result)

    assert outcomes[1] == {
        "operation": "tool:account_positions",
        "requiredness": "optional",
        "retryability": "unknown",
        "fallback_used": True,
        "decision_completed": True,
        "broker_state_certainty": "not_observed",
        "impact": "optional_component_failed",
        "error_category": "ConnectionError",
    }
    assert outcomes[2] == {
        "operation": "tool:account_positions",
        "requiredness": "optional",
        "retryability": "not_applicable",
        "fallback_used": False,
        "decision_completed": True,
        "broker_state_certainty": "not_observed",
        "impact": "completed",
        "error_category": None,
    }


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


def test_agent_order_tool_rejects_when_account_context_was_not_checked():
    strategy = _OrderReadinessStrategy()
    tool_map = _wrap_builtin_tools(
        strategy,
        [
            BuiltinTools.account.positions(),
            BuiltinTools.account.portfolio(),
            BuiltinTools.market.last_price(),
            BuiltinTools.orders.submit(),
        ],
    )

    result = tool_map["orders_submit_order"](
        symbol="SPY",
        quantity=1,
        side="buy",
        asset_type="stock",
        order_type="market",
    )

    assert result["tool_error"] is True
    assert result["error"]["type"] == "ValueError"
    assert "ORDER_READINESS_REQUIRED" in result["error"]["message"]
    assert "account_portfolio" in result["error"]["message"]
    assert "account_positions" in result["error"]["message"]
    assert "market_last_price" in result["error"]["message"]
    assert strategy.submitted_orders == []


def test_agent_order_tool_submits_after_account_context_was_checked():
    strategy = _OrderReadinessStrategy()
    tool_map = _wrap_builtin_tools(
        strategy,
        [
            BuiltinTools.account.positions(),
            BuiltinTools.account.portfolio(),
            BuiltinTools.market.last_price(),
            BuiltinTools.orders.submit(),
        ],
    )

    tool_map["account_portfolio"]()
    tool_map["account_positions"]()
    tool_map["market_last_price"](symbol="SPY", asset_type="stock")
    result = tool_map["orders_submit_order"](
        symbol="SPY",
        quantity=1,
        side="buy",
        asset_type="stock",
        order_type="market",
    )

    assert "tool_error" not in result
    assert result["order"]["asset"]["symbol"] == "SPY"
    assert len(strategy.submitted_orders) == 1


def test_agent_order_tool_requires_last_price_for_ordered_symbol():
    strategy = _OrderReadinessStrategy()
    tool_map = _wrap_builtin_tools(
        strategy,
        [
            BuiltinTools.account.positions(),
            BuiltinTools.account.portfolio(),
            BuiltinTools.market.last_price(),
            BuiltinTools.orders.submit(),
        ],
    )

    tool_map["account_portfolio"]()
    tool_map["account_positions"]()
    tool_map["market_last_price"](symbol="QQQ", asset_type="stock")
    result = tool_map["orders_submit_order"](
        symbol="SPY",
        quantity=1,
        side="buy",
        asset_type="stock",
        order_type="market",
    )

    assert result["tool_error"] is True
    assert "market_last_price(symbol='SPY')" in result["error"]["message"]
    assert strategy.submitted_orders == []


def test_market_last_price_rejects_comma_separated_symbols():
    strategy = _OrderReadinessStrategy()
    tool_map = _wrap_builtin_tools(
        strategy,
        [
            BuiltinTools.market.last_price(),
        ],
    )

    result = tool_map["market_last_price"](symbol="TQQQ,SQQQ", asset_type="stock")

    assert result["tool_error"] is True
    assert result["error"]["type"] == "ValueError"
    assert "one tradable symbol" in result["error"]["message"]


def test_order_submit_rejects_comma_separated_symbols():
    strategy = _OrderReadinessStrategy()
    tool_map = _wrap_builtin_tools(
        strategy,
        [
            BuiltinTools.account.positions(),
            BuiltinTools.account.portfolio(),
            BuiltinTools.market.last_price(),
            BuiltinTools.orders.submit(),
        ],
    )

    tool_map["account_portfolio"]()
    tool_map["account_positions"]()
    result = tool_map["orders_submit_order"](
        symbol="TQQQ,SQQQ",
        quantity=1,
        side="buy",
        asset_type="stock",
        order_type="market",
    )

    assert result["tool_error"] is True
    assert result["error"]["type"] == "ValueError"
    assert "one tradable symbol" in result["error"]["message"]
    assert strategy.submitted_orders == []


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
    retrievals = strategy.memory.export_artifacts(tmp_path, prefix="runtime_memory")["memory_retrievals"]
    import pandas as pd

    retrieval_rows = pd.read_parquet(retrievals)
    assert set(retrieval_rows["agent_name"]) == {"researcher_runtime"}
    assert _Runtime.last_request.model_call_id in set(retrieval_rows["model_call_id"])


def test_agent_runtime_memory_notes_are_compacted(monkeypatch, tmp_path):
    from lumibot.components.memory import MemoryStore
    from lumibot.components.notifications import NotificationManager

    monkeypatch.setenv("LUMIBOT_AGENT_MEMORY_NOTE_MAX_CHARS", "500")
    monkeypatch.setenv("LUMIBOT_CACHE_FOLDER", str(tmp_path))
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
    state = strategy.vars.get("_agent_runtime_state")
    state["compact_memory"]["runs"][0]["summary"] = "legacy unbounded duplicate"
    strategy.vars.set("_agent_runtime_state", state)
    agent.run(task_prompt="second should receive compact prior summary")

    assert len(_LongSummaryRuntime.requests) == 2
    prior_notes = _LongSummaryRuntime.requests[1].memory_notes
    assert len(prior_notes) == 1
    assert len(prior_notes[0]["summary"]) == 500
    assert prior_notes[0]["summary"].endswith("...")
    # Full summaries are already represented by bounded memory notes and must
    # not be duplicated in the scheduled self.vars runtime metadata.
    runs = strategy.vars.get("_agent_runtime_state")["compact_memory"]["runs"]
    assert all("summary" not in run for run in runs)
    artifact_path = tmp_path / "agent_runtime" / "agent_run_summaries.jsonl"
    artifact_rows = [json.loads(line) for line in artifact_path.read_text().splitlines()]
    migrated = [row for row in artifact_rows if row.get("migrated_from_runtime_state")]
    assert len(migrated) == 1
    assert migrated[0]["summary"] == "legacy unbounded duplicate"


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
