import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from lumibot.components.agents import AgentManager, AgentRunResult, AgentTraceEvent, BuiltinTools
from lumibot.components.agents.manager import AgentModelCallLimitExceeded, _structured_operation_outcomes
from lumibot.components.agents.schemas import BoundTool, ToolDefinition

# Order explanations are now required; retain every existing readiness/risk assertion.
ORDER_ACTION_REASON = "After checking the current quote, account cash, positions and pending orders, take the requested action within the strategy risk budget. The evidence is limited to this fixture timestamp. Reconcile the broker response and remaining exposure before taking any further action."


PRIVATE_TOOL_PURPOSE = (
    "Read the permitted account evidence to determine whether the existing position covers the action."
)


@pytest.mark.parametrize(
    "name", ["orders_submit_order", "orders_submit_orders", "orders_submit_multileg", "orders_cancel_order", "orders_modify_order"]
)
def test_public_order_descriptions_keep_account_relative_sizing_private(name):
    # Actual rc3 stock rep4 supplied a portfolio-relative cap in public_reason;
    # the privacy boundary correctly omitted that summary and its public hash.
    definition = next(tool for tool in BuiltinTools.all() if tool.name == name)
    assert "account-relative sizing percentages and formulas stay private" in definition.description
    assert "qualitative trade thesis and risk" in definition.description


def _agent_declared_callable(tool, context):
    from lumibot.components.agents.runtime import _function_tools_with_name_aliases

    # Exercise the production ADK registration boundary without invoking a model.
    return _function_tools_with_name_aliases(lambda function: function, [tool], context)[0]


def test_agent_bound_tool_purpose_is_required_only_in_actual_adk_and_managed_declaration():
    from google.adk.tools.function_tool import FunctionTool
    from google.genai import types

    from lumibot.components.agents.managed_gateway import _tools
    from lumibot.components.agents.runtime import _function_tools_with_name_aliases, _wrap_tool_callable

    def read(symbol: str) -> dict:
        return {"symbol": symbol}

    tool = BoundTool("read_evidence", "Read permitted evidence.", read)
    ordinary = FunctionTool(_wrap_tool_callable(tool))._get_declaration()
    declared = _function_tools_with_name_aliases(FunctionTool, [tool], {})[0]._get_declaration()
    request = SimpleNamespace(config=SimpleNamespace(tools=[types.Tool(function_declarations=[declared])]))
    schema = _tools(request)[0]["inputSchema"]
    assert "agent_purpose" in schema["required"]
    assert schema["properties"]["agent_purpose"]["type"].lower() == "string"
    assert "symbol" in schema["properties"]
    # Current ADK may use parameters_json_schema; exercise the same gateway
    # conversion for the ordinary declaration rather than assuming one form.
    ordinary_request = SimpleNamespace(config=SimpleNamespace(tools=[types.Tool(function_declarations=[ordinary])]))
    ordinary_schema = _tools(ordinary_request)[0]["inputSchema"]
    assert "agent_purpose" not in ordinary_schema["properties"]
    assert tool.function(symbol="SPY") == {"symbol": "SPY"}


@pytest.mark.parametrize("purpose", [None, "", " \t ", 42, False, {}, "x" * 501, "🧠" * 126, "a\0b", "a\nb", "\ud800"])
def test_agent_bound_tool_purpose_invalid_is_visible_and_does_not_invoke(purpose):
    invoked = []

    def read(**arguments) -> dict:
        invoked.append(arguments)
        return {"ok": True}

    context = {}
    wrapped = _agent_declared_callable(BoundTool("read_evidence", "Read evidence.", read), context)
    arguments = {"symbol": "SPY"}
    if purpose is not None:
        arguments["agent_purpose"] = purpose
    result = wrapped(**arguments)
    assert result["ok"] is False
    assert result["tool_error"] is True
    assert "agent_purpose" in result["error"]["message"]
    assert invoked == []
    assert context["tool_calls"][-1]["ok"] is False
    assert context["tool_calls"][-1]["status"] == "failed"


def test_agent_bound_tool_purpose_records_before_invocation_without_domain_or_public_projection():
    from lumibot.components.agents.tool_context import current_agent_tool_context

    context = {"agent_name": "researcher", "model_call_id": "call-1", "tool_calls": []}
    snapshots = []
    observed = []

    def read(symbol: str) -> dict:
        snapshots.append(dict(context["tool_calls"][-1]))
        observed.append((symbol, current_agent_tool_context()["tool_purpose"]))
        return {"ok": True, "symbol": symbol}

    wrapped = _agent_declared_callable(BoundTool("read_evidence", "Read evidence.", read), context)
    result = wrapped(symbol="SPY", agent_purpose=PRIVATE_TOOL_PURPOSE)
    assert result == {"ok": True, "symbol": "SPY"}
    assert observed == [("SPY", PRIVATE_TOOL_PURPOSE)]
    assert snapshots == [
        {
            "tool_name": "read_evidence",
            "arguments": {"symbol": "SPY"},
            "purpose": PRIVATE_TOOL_PURPOSE,
            "status": "started",
        }
    ]
    assert context["tool_calls"] == [
        {
            "tool_name": "read_evidence",
            "arguments": {"symbol": "SPY"},
            "purpose": PRIVATE_TOOL_PURPOSE,
            "status": "completed",
            "ok": True,
        }
    ]
    assert "tool_purpose" not in current_agent_tool_context()
    assert "public_reason" not in result


def test_agent_bound_tool_purpose_preserves_colliding_domain_arguments():
    from google.adk.tools.function_tool import FunctionTool

    def read(agent_purpose: str, purpose: str) -> dict:
        return {"agent_purpose": agent_purpose, "purpose": purpose}

    tool = BoundTool("read_evidence", "Read evidence.", read)
    wrapped = _agent_declared_callable(tool, {})
    declaration = FunctionTool(wrapped)._get_declaration()
    schema = declaration.parameters_json_schema or declaration.parameters.model_dump(exclude_none=True)
    assert {"agent_purpose", "purpose", "agent_purpose_2"}.issubset(schema["required"])
    assert wrapped(agent_purpose="domain selection", purpose="domain filter", agent_purpose_2=PRIVATE_TOOL_PURPOSE) == {
        "agent_purpose": "domain selection",
        "purpose": "domain filter",
    }


def test_agent_bound_tool_purpose_does_not_invoke_without_an_attempt_trace():
    invoked = []

    def read(symbol: str) -> dict:
        invoked.append(symbol)
        return {"ok": True}

    wrapped = _agent_declared_callable(BoundTool("read_evidence", "Read evidence.", read), {"tool_calls": "invalid"})
    result = wrapped(symbol="SPY", agent_purpose=PRIVATE_TOOL_PURPOSE)
    assert result["ok"] is False
    assert result["tool_error"] is True
    assert invoked == []


def test_agent_bound_tool_purpose_preserves_shared_cache_keys_without_purpose(monkeypatch, tmp_path):
    monkeypatch.setenv("LUMIBOT_CACHE_FOLDER", str(tmp_path / "cache"))
    manager = AgentManager(_Strategy())
    invoked = []

    def read(symbol: str) -> dict:
        invoked.append(symbol)
        return {"ok": True, "symbol": symbol}

    tool = manager._with_tool_result_cache(
        BoundTool("read_evidence", "Read evidence.", read, metadata={"cache_scope": "strategy_day"})
    )
    first_context, second_context = {}, {}
    first = _agent_declared_callable(tool, first_context)(symbol="SPY", agent_purpose=PRIVATE_TOOL_PURPOSE)
    second = _agent_declared_callable(tool, second_context)(
        symbol="SPY", agent_purpose="Verify the same permitted symbol for a separate decision."
    )
    assert first["ok"] is True
    assert second["ok"] is True
    assert second["_lumibot_tool_cache"]["hit"] is True
    assert invoked == ["SPY"]
    assert (
        first_context["tool_calls"][0]["arguments"] == second_context["tool_calls"][0]["arguments"] == {"symbol": "SPY"}
    )
    assert first_context["tool_calls"][0]["purpose"] != second_context["tool_calls"][0]["purpose"]


def test_agent_bound_tool_purpose_preserves_remote_failed_status_and_original_arguments():
    from lumibot.components.agents.runtime import RemoteMCPToolError

    observed = []
    failure = RemoteMCPToolError(
        "query_data",
        {
            "isError": True,
            "structuredContent": {
                "errorCode": "unsupported_capability",
                "message": "Original historical versions unavailable.",
            },
        },
    )

    def query(**arguments) -> dict:
        observed.append(arguments)
        raise failure

    context = {}
    wrapped = _agent_declared_callable(BoundTool("query_data", "Read recorded data.", query, source="mcp"), context)
    result = wrapped(datasetId="bls.public_series", agent_purpose=PRIVATE_TOOL_PURPOSE)
    assert observed == [{"datasetId": "bls.public_series"}]
    assert result["ok"] is False
    assert result["tool_error"] is True
    assert result["isError"] is True
    assert result["error"]["code"] == "unsupported_capability"
    assert result["arguments"] == {"datasetId": "bls.public_series"}
    assert context["tool_calls"][0]["purpose"] == PRIVATE_TOOL_PURPOSE
    assert context["tool_calls"][0]["status"] == "failed"
    assert context["tool_calls"][0]["ok"] is False


def test_explicit_execution_and_observed_action_rules_reach_assembled_adk_instruction(monkeypatch):
    import asyncio

    from google.adk.tools.function_tool import FunctionTool
    from google.genai import types

    from lumibot.components.agents import runtime as runtime_module

    captured = {}

    class ConstructionCaptured(RuntimeError):
        pass

    class CapturedAgent:
        def __init__(self, **arguments):
            captured.update(arguments)
            raise ConstructionCaptured("ADK constructor reached; no runner or model is started")

    runtime = runtime_module.GoogleADKRuntime()
    monkeypatch.setattr(runtime, "_ensure_adk", lambda: (CapturedAgent, object, types, FunctionTool))
    monkeypatch.setattr(runtime_module, "_resolve_model_for_adk", lambda model, **_kwargs: model)
    # This construction-only contract must not ask LiteLLM for ChatGPT model
    # metadata: that lookup can start interactive device authentication before
    # the captured constructor is reached. Model limits are not this test's job.
    monkeypatch.setattr(runtime_module, "_model_max_output_tokens", lambda _model: None)
    request = runtime_module.RuntimeRequest(
        agent_name="researcher",
        model="chatgpt/gpt-6-sol",
        system_prompt="Respect the authorized strategy rules.",
        task_prompt="Perform the requested authorized action when the stated conditions hold.",
        context=None,
        runtime_context={},
        memory_state=None,
        memory_notes=[],
        bound_tools=[],
        include_builtin_skills=False,
    )
    with pytest.raises(ConstructionCaptured):
        asyncio.run(runtime._run_async(request))
    instruction = captured["instruction"]
    assert instruction == runtime._instruction_for(request)
    assert "verify the stated conditions and then invoke the action tool" in instruction
    assert "Do not invent new acceptance thresholds" in instruction
    assert "Preserve discretion when the user requests analysis or delegates the choice" in instruction
    assert "only after its action tool succeeds and returns the action's identifier" in instruction
    assert "Report fills only from observed status or position evidence" in instruction


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

    def get_orders(self):
        return list(self.submitted_orders)

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

    def submit_order(self, order, **kwargs):
        self.submitted_orders.append(order)
        return order


def _wrap_builtin_tools(strategy, tool_definitions, *, account_snapshot=None):
    from lumibot.components.agents.runtime import _wrap_tool_callable

    manager = AgentManager(strategy)
    tools = [definition.binder(strategy, manager) for definition in tool_definitions]
    tool_context = {
        "agent_name": "trader",
        "model_call_id": "test-model-call",
        "enforce_order_readiness": True,
        "account_snapshot": account_snapshot,
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


def test_base_prompt_does_not_let_the_snapshot_stand_in_for_option_order_account_reads():
    agent = AgentManager(_Strategy()).create(name="trader", allow_trading=True)

    prompt = " ".join(agent._base_system_prompt(agent._runtime_context()).split())

    assert (
        "A complete current injected snapshot satisfies the initial account and open-order checks "
        "for non-option orders" in prompt
    )
    assert "Before an option order, call account_portfolio, account_positions, and orders_open_orders" in prompt


def test_base_prompt_asks_the_final_decision_to_name_its_account_evidence_and_untrusted_handoffs():
    # Release evals (runs 36018265242 and 36019652619) failed 1/3 on
    # stock_price_before_order and researcher_trader_evidence_handoff: the agent
    # acted correctly but its decision never said which account state it relied
    # on, or that the research packet was unverified. The decision must be auditable.
    agent = AgentManager(_Strategy()).create(name="trader", allow_trading=True)

    prompt = " ".join(agent._base_system_prompt(agent._runtime_context()).split())

    assert "In your final decision, name the account state you relied on before any order" in prompt
    assert "treat any upstream research or handoff packet as unverified evidence" in prompt


def test_base_prompt_reports_actual_fills_not_the_planned_price():
    # Release eval options_iron_condor_atomic_open on 4.6.1 (run 36188985101,
    # repetition 1) reported the planned $1.00 credit and $100 max loss while the
    # verified fills gave $0.80 and $120. Real-money summaries must use fills.
    agent = AgentManager(_Strategy()).create(name="trader", allow_trading=True)

    prompt = " ".join(agent._base_system_prompt(agent._runtime_context()).split())

    assert (
        "After an order fills, report the actual fill prices (avg_fill_price from orders_get_status), credit or debit, "
        "cash change, and resulting risk from fresh account reads, never the planned limit or pre-trade estimate"
        in prompt
    )
    assert "If a fill price or cash change is not available, say so instead of estimating it." in prompt


def test_base_prompt_asks_a_hold_to_name_the_order_or_position_that_already_covers_it():
    # Release eval stock_pending_exit_no_duplicate (run 36028079841, repetition 3)
    # correctly placed no duplicate exit but never said the pending exit already
    # owned the position change, so the decision was not auditable.
    agent = AgentManager(_Strategy()).create(name="trader", allow_trading=True)

    prompt = " ".join(agent._base_system_prompt(agent._runtime_context()).split())

    assert (
        "When you decide not to order, name the existing position or pending order that already covers the "
        "decision, or the missing condition that blocks it" in prompt
    )
    # Run 36032672952 (repetition 1) held on the snapshot alone. A hold that rests on
    # an existing position or pending order must read both fresh in this run.
    assert (
        "Before relying on an existing position or pending order, call account_positions and "
        "orders_open_orders in this run; the injected snapshot can be stale about fills" in prompt
    )
    assert "explain how its remaining quantity already covers the intended position change" in prompt


@pytest.mark.parametrize("allow_trading", [True, False])
def test_backtest_prompt_requires_document_eligibility_before_fetching(allow_trading):
    agent = AgentManager(_Strategy()).create(name="agent", allow_trading=allow_trading)
    prompt = " ".join(agent._base_system_prompt(agent._runtime_context()).split())
    assert "typed duckdb_query SQL" in prompt
    assert "first public availability and version time" in prompt
    assert (
        "Parse explicit timezone offsets as TIMESTAMPTZ and compare UTC instants before sorting or limiting" in prompt
    )
    assert "Never open a disclosure or revision known to have become public after the simulated datetime" in prompt


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
        "status": "runtime_error",
        "decision_completed": False,
        "broker_state_certainty": "not_observed",
        "impact": "decision_blocked",
        "error_category": "auth",
    }


def test_base_prompt_selects_relevant_evidence_instead_of_every_tool_category():
    manager = AgentManager(_Strategy())
    agent = manager.create(name="trader", allow_trading=True)

    prompt = agent._base_system_prompt(agent._runtime_context())

    assert "Do not call every available data category by default" in prompt
    assert "Other evidence categories are thesis-dependent" in prompt
    assert "availability alone is not a reason to call them" in prompt
    assert "Do not submit a material equity order until you have called" not in prompt
    assert "use SEC financial/filing tools on the most relevant single-stock" not in prompt


def test_backtest_prompt_treats_last_trade_option_prices_as_the_fill_basis():
    manager = AgentManager(_Strategy())
    agent = manager.create(name="interpreter", allow_trading=False)

    prompt = agent._base_system_prompt(agent._runtime_context())

    assert "price_basis='last_trade'" in prompt
    assert "missing bid/ask alone is not a reason" in prompt.lower()
    assert "recent trade bar" in prompt


def test_live_prompt_does_not_carry_backtest_last_trade_guidance():
    strategy = _Strategy()
    strategy.is_backtesting = False
    manager = AgentManager(strategy)
    agent = manager.create(name="interpreter", allow_trading=False)

    prompt = agent._base_system_prompt(agent._runtime_context())

    assert "price_basis='last_trade'" not in prompt


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
        "status": "runtime_error",
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
        reasoning_effort="high",
    )

    manager["timed"].run(task_prompt="Use defaults.")
    manager["timed"].run(
        task_prompt="Use overrides.",
        model_request_timeout_seconds=7,
        run_timeout_seconds=8,
    )

    assert _CaptureRuntime.requests[0].model_request_timeout_seconds == 123
    assert _CaptureRuntime.requests[0].run_timeout_seconds == 456
    assert _CaptureRuntime.requests[0].reasoning_effort == "high"
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

        def submit_order(self, order, **kwargs):
            # The Strategy execution boundary now supplies provenance.
            order.decision_provenance = self.memory.decision_provenance(
                agent_name="trader", model_call_id="call-order-1"
            )
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
    result = wrapped(symbol="TQQQ", quantity=10, side="buy", reason=ORDER_ACTION_REASON)

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

        def submit_order(self, order, **kwargs):
            raise TimeoutError("broker response timed out")

    strategy = _TimeoutStrategy()
    monkeypatch.setattr(
        "lumibot.components.agents.builtins.resolve_asset_and_quote",
        lambda *args, **kwargs: (_Asset(), None),
    )

    result = _wrap_tool_callable(_bind_submit_order(strategy, manager=None))(
        symbol="TQQQ", quantity=10, side="buy", reason=ORDER_ACTION_REASON
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


def test_actual_account_indicator_research_and_multileg_schemas_cross_managed_gateway():
    pytest.importorskip("google.adk.tools.function_tool")
    from google.adk.tools.function_tool import FunctionTool
    from google.genai import types

    from lumibot.components.agents.managed_gateway import _tools
    from lumibot.components.agents.runtime import _wrap_tool_callable

    agent = AgentManager(_Strategy()).create(
        name="trader",
        model="openai/gpt-5.6-luna",
        allow_trading=True,
    )
    selected = {
        tool.name: tool
        for tool in agent._ensure_bound_tools()
        if tool.name in {"account_positions", "get_indicators", "get_income_statement", "orders_submit_multileg"}
    }
    assert set(selected) == {
        "account_positions",
        "get_indicators",
        "get_income_statement",
        "orders_submit_multileg",
    }
    declarations = [FunctionTool(_wrap_tool_callable(tool))._get_declaration() for tool in selected.values()]
    request = SimpleNamespace(config=SimpleNamespace(tools=[types.Tool(function_declarations=declarations)]))

    gateway_tools = {tool["name"]: tool for tool in _tools(request)}
    assert gateway_tools.keys() == selected.keys()
    assert "symbol" in gateway_tools["account_positions"]["inputSchema"]["properties"]
    assert "requests_json" in gateway_tools["get_indicators"]["inputSchema"]["properties"]
    assert "symbol" in gateway_tools["get_income_statement"]["inputSchema"]["properties"]
    assert "legs_json" in gateway_tools["orders_submit_multileg"]["inputSchema"]["properties"]
    json.dumps(gateway_tools, allow_nan=False)


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
            BuiltinTools.orders.open_orders(),
            BuiltinTools.market.last_price(),
            BuiltinTools.orders.submit(),
        ],
    )

    result = tool_map["orders_submit_order"](
        reason=ORDER_ACTION_REASON,
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
            BuiltinTools.orders.open_orders(),
            BuiltinTools.market.last_price(),
            BuiltinTools.orders.submit(),
        ],
    )

    tool_map["account_portfolio"]()
    tool_map["account_positions"]()
    tool_map["orders_open_orders"]()
    tool_map["market_last_price"](symbol="SPY", asset_type="stock")
    result = tool_map["orders_submit_order"](
        reason=ORDER_ACTION_REASON,
        symbol="SPY",
        quantity=1,
        side="buy",
        asset_type="stock",
        order_type="market",
    )

    assert "tool_error" not in result
    assert result["order"]["asset"]["symbol"] == "SPY"
    assert len(strategy.submitted_orders) == 1


@pytest.mark.parametrize("injected_snapshot", [True, False])
def test_agent_audit_started_order_preserves_prior_readiness_and_completed_order_requires_refresh(injected_snapshot):
    strategy = _OrderReadinessStrategy()
    manager = AgentManager(strategy)
    context = {"enforce_order_readiness": True, "tool_calls": []}
    if injected_snapshot:
        context["account_snapshot"] = {
            "as_of": "2026-01-02T00:00:00+00:00",
            "account_complete": True,
            "positions_complete": True,
            "open_orders_complete": True,
        }
    definitions = [
        BuiltinTools.account.positions(),
        BuiltinTools.account.portfolio(),
        BuiltinTools.orders.open_orders(),
        BuiltinTools.market.last_price(),
        BuiltinTools.orders.submit(),
    ]
    tools = {}
    for definition in definitions:
        tool = definition.binder(strategy, manager)
        tools[tool.name] = _agent_declared_callable(tool, context)

    def refresh():
        for name in ["account_portfolio", "account_positions", "orders_open_orders"]:
            assert tools[name](agent_purpose=PRIVATE_TOOL_PURPOSE).get("tool_error") is not True

    if not injected_snapshot:
        refresh()
    tools["market_last_price"](symbol="SPY", asset_type="stock", agent_purpose=PRIVATE_TOOL_PURPOSE)
    arguments = dict(
        symbol="SPY",
        quantity=1,
        side="buy",
        asset_type="stock",
        order_type="market",
        reason=ORDER_ACTION_REASON,
        agent_purpose=PRIVATE_TOOL_PURPOSE,
    )
    first = tools["orders_submit_order"](**arguments)
    assert first.get("tool_error") is not True
    assert len(strategy.submitted_orders) == 1
    second = tools["orders_submit_order"](**arguments)
    assert second["tool_error"] is True
    assert "ORDER_READINESS_REQUIRED" in second["error"]["message"]
    assert len(strategy.submitted_orders) == 1
    refresh()
    third = tools["orders_submit_order"](**arguments)
    assert third.get("tool_error") is not True
    assert len(strategy.submitted_orders) == 2


def test_post_invocation_audit_failure_invalidates_account_evidence_before_another_order():
    strategy = _OrderReadinessStrategy()
    manager = AgentManager(strategy)
    fail_outcome = [True]

    def journal(record):
        if record["tool_name"] == "orders_submit_order" and record["status"] == "completed" and fail_outcome[0]:
            fail_outcome[0] = False
            raise OSError("synthetic outcome persistence failure")

    context = {
        "enforce_order_readiness": True,
        "tool_calls": [],
        "tool_journal_callback": journal,
        "account_snapshot": {
            "as_of": "2026-01-02T00:00:00+00:00",
            "account_complete": True,
            "positions_complete": True,
            "open_orders_complete": True,
        },
    }
    definitions = [
        BuiltinTools.account.positions(),
        BuiltinTools.account.portfolio(),
        BuiltinTools.orders.open_orders(),
        BuiltinTools.market.last_price(),
        BuiltinTools.orders.submit(),
    ]
    tools = {}
    for definition in definitions:
        tool = definition.binder(strategy, manager)
        tools[tool.name] = _agent_declared_callable(tool, context)
    tools["market_last_price"](symbol="SPY", agent_purpose=PRIVATE_TOOL_PURPOSE)
    arguments = dict(
        symbol="SPY", quantity=1, side="buy", reason=ORDER_ACTION_REASON, agent_purpose=PRIVATE_TOOL_PURPOSE
    )
    first = tools["orders_submit_order"](**arguments)
    assert first["error"]["code"] == "tool_journal_unavailable"
    assert len(strategy.submitted_orders) == 1
    second = tools["orders_submit_order"](**arguments)
    assert second["tool_error"] is True
    assert "ORDER_READINESS_REQUIRED" in second["error"]["message"]
    assert len(strategy.submitted_orders) == 1
    for name in ["account_portfolio", "account_positions", "orders_open_orders"]:
        tools[name](agent_purpose=PRIVATE_TOOL_PURPOSE)
    third = tools["orders_submit_order"](**arguments)
    assert third.get("tool_error") is not True
    assert len(strategy.submitted_orders) == 2


def test_agent_order_tool_accepts_complete_injected_account_snapshot_for_first_order():
    strategy = _OrderReadinessStrategy()
    tool_map = _wrap_builtin_tools(
        strategy,
        [
            BuiltinTools.market.last_price(),
            BuiltinTools.orders.submit(),
        ],
        account_snapshot={
            "as_of": "2026-01-02T00:00:00+00:00",
            "account_complete": True,
            "positions_complete": True,
            "open_orders_complete": True,
        },
    )

    tool_map["market_last_price"](symbol="SPY", asset_type="stock")
    result = tool_map["orders_submit_order"](
        reason=ORDER_ACTION_REASON,
        symbol="SPY",
        quantity=1,
        side="buy",
        asset_type="stock",
        order_type="market",
    )

    assert "tool_error" not in result
    assert len(strategy.submitted_orders) == 1


def test_successful_order_invalidates_injected_snapshot_until_account_is_refreshed():
    strategy = _OrderReadinessStrategy()
    tool_map = _wrap_builtin_tools(
        strategy,
        [
            BuiltinTools.account.positions(),
            BuiltinTools.account.portfolio(),
            BuiltinTools.orders.open_orders(),
            BuiltinTools.market.last_price(),
            BuiltinTools.orders.submit(),
        ],
        account_snapshot={
            "as_of": "2026-01-02T00:00:00+00:00",
            "account_complete": True,
            "positions_complete": True,
            "open_orders_complete": True,
        },
    )

    tool_map["market_last_price"](symbol="SPY", asset_type="stock")
    first = tool_map["orders_submit_order"](
        reason=ORDER_ACTION_REASON,
        symbol="SPY",
        quantity=1,
        side="buy",
        asset_type="stock",
        order_type="market",
    )
    second = tool_map["orders_submit_order"](
        reason=ORDER_ACTION_REASON,
        symbol="SPY",
        quantity=1,
        side="buy",
        asset_type="stock",
        order_type="market",
    )

    assert "tool_error" not in first
    assert second["tool_error"] is True
    assert "account_portfolio" in second["error"]["message"]
    assert "account_positions" in second["error"]["message"]

    tool_map["account_portfolio"]()
    tool_map["account_positions"]()
    tool_map["orders_open_orders"]()
    third = tool_map["orders_submit_order"](
        reason=ORDER_ACTION_REASON,
        symbol="SPY",
        quantity=1,
        side="buy",
        asset_type="stock",
        order_type="market",
    )

    assert "tool_error" not in third
    assert len(strategy.submitted_orders) == 2


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
        reason=ORDER_ACTION_REASON,
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
        reason=ORDER_ACTION_REASON,
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


def test_actual_adk_protocol_survives_manager_save_load_and_private_cache_replay(monkeypatch, tmp_path):
    from google.adk.models.base_llm import BaseLlm
    from google.adk.models.llm_response import LlmResponse
    from google.genai import types

    from lumibot.components.agents import runtime as runtime_module
    from lumibot.components.memory import MemoryStore

    provider_calls = []
    rebuilt = []

    class ProviderFixture(BaseLlm):
        async def generate_content_async(self, llm_request, stream=False):
            provider_calls.append(llm_request)
            if len(provider_calls) == 1:
                part = types.Part(
                    function_call=types.FunctionCall(
                        name="rebuild_context",
                        id="owned-replay-call",
                        args={"symbol": "SPY", "agent_purpose": PRIVATE_TOOL_PURPOSE},
                    )
                )
            else:
                part = types.Part(text="Permitted context inspected.")
            yield LlmResponse(content=types.Content(role="model", parts=[part]))

    def binder(_strategy, _manager):
        def rebuild_context(symbol: str):
            rebuilt.append(symbol)
            return {"symbol": symbol}

        return BoundTool(
            "rebuild_context", "Rebuild permitted context.", rebuild_context, metadata={"replay_on_cache": True}
        )

    monkeypatch.setenv("LUMIBOT_CACHE_FOLDER", str(tmp_path / "cache"))
    monkeypatch.setattr(
        runtime_module, "_resolve_model_for_adk", lambda *_args, **_kwargs: ProviderFixture(model="fixture")
    )
    strategy = _Strategy()
    strategy.vars = _Vars()
    strategy.parameters = {}
    strategy.memory = MemoryStore(strategy, root_dir=tmp_path / "memory")
    manager = AgentManager(strategy)
    agent = manager.create(
        name="private_replay",
        model="fixture",
        allow_trading=False,
        include_builtin_tools=False,
        include_builtin_skills=False,
        tools=[ToolDefinition("rebuild_context", "Rebuild permitted context.", binder)],
    )
    cold = agent.run(task_prompt="Inspect permitted context.")
    assert cold.payload["agent_tool_contract"] == runtime_module.GoogleADKRuntime.agent_tool_contract
    saved = manager.replay_cache.load(cold.cache_key)
    assert saved["payload"]["agent_tool_contract"] == cold.payload["agent_tool_contract"]
    # Recreate the same historical replay key without another provider call.
    monkeypatch.setattr(manager.replay_cache, "compute_key", lambda _payload: cold.cache_key)
    warm = agent.run(task_prompt="Inspect permitted context.")
    assert warm.cache_hit is True
    assert len(provider_calls) == 2
    assert rebuilt == ["SPY", "SPY"]
    with strategy.memory._connect() as connection:
        rows = connection.execute(
            "SELECT event_type FROM memory_events WHERE event_type LIKE 'agent.tool_%' ORDER BY sequence"
        ).fetchall()
    assert [row["event_type"] for row in rows] == ["agent.tool_started", "agent.tool_outcome"] * 2


@pytest.mark.parametrize("journal_available", [True, False])
def test_managed_agent_request_commits_private_intent_before_tool_action(monkeypatch, tmp_path, journal_available):
    from lumibot.components.agents.runtime import _wrap_agent_tool_callable
    from lumibot.components.memory import MemoryStore

    monkeypatch.setenv("LUMIBOT_CACHE_FOLDER", str(tmp_path / "cache"))
    strategy = _Strategy()
    strategy.vars = _Vars()
    strategy.parameters = {}
    strategy.is_backtesting = False
    strategy.memory = MemoryStore(strategy, root_dir=tmp_path / "memory")
    runtime = _LongSummaryRuntime()
    runtime.requests = []
    agent = AgentManager(strategy).create(
        name="private_audit",
        model="chatgpt/gpt-6-luna",
        allow_trading=False,
        include_builtin_tools=False,
        include_builtin_skills=False,
        _runtime=runtime,
    )
    result = agent.run(task_prompt="Inspect permitted evidence.")
    request = type(runtime).requests[-1]
    assert request.model_call_id == result.cache_key
    assert callable(request.tool_journal_callback)
    calls = []

    def read(account_reference: str):
        with strategy.memory._connect() as connection:
            intent = connection.execute(
                "SELECT event_type, agent_name, model_call_id, text FROM memory_events "
                "WHERE event_type LIKE 'agent.tool_%' ORDER BY sequence"
            ).fetchall()
        assert len(intent) == 1
        assert intent[0]["event_type"] == "agent.tool_started"
        assert intent[0]["agent_name"] == "private_audit"
        assert intent[0]["model_call_id"] == request.model_call_id
        assert account_reference not in intent[0]["text"]
        calls.append(account_reference)
        return {"ok": True}

    if not journal_available:

        def unavailable(**_arguments):
            raise RuntimeError("private database path and credentials must not reach the model")

        monkeypatch.setattr(strategy.memory, "record_agent_tool_action", unavailable)
        request.tool_journal_callback = agent._tool_journal_callback(request.model_call_id)

    wrapped = _wrap_agent_tool_callable(
        BoundTool("account_evidence", "Read permitted evidence.", read),
        {
            "agent_name": request.agent_name,
            "model_call_id": request.model_call_id,
            "tool_calls": [],
            "tool_journal_callback": request.tool_journal_callback,
        },
    )
    outcome = wrapped(account_reference="private-account-fixture", agent_purpose=PRIVATE_TOOL_PURPOSE)
    if journal_available:
        assert outcome == {"ok": True}
        assert calls == ["private-account-fixture"]
        with strategy.memory._connect() as connection:
            rows = connection.execute(
                "SELECT event_type, text FROM memory_events WHERE event_type LIKE 'agent.tool_%' ORDER BY sequence"
            ).fetchall()
        assert [row["event_type"] for row in rows] == ["agent.tool_started", "agent.tool_outcome"]
        assert all("private-account-fixture" not in row["text"] for row in rows)
    else:
        assert outcome["error"]["code"] == "tool_journal_unavailable"
        assert "credentials" not in str(outcome)
        assert calls == []


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


# Outbound network tools (HTTP, RSS, browser) are default-deny. Page text can
# carry prompt injection, and a network tool is the channel that could send
# agent context out. They also crowd the default trading toolset: the options
# iron-condor eval regressed from 3/3 to 1/3 when they joined every agent.
_NETWORK_TOOL_NAMES = {
    "http_request",
    "read_document",
    "rss_fetch",
    "web_search",
    "browser_session_open",
    "browser_session_close",
    "browser_session_recover",
    "browser_navigate",
    "browser_observe",
    "browser_act",
    "browser_tabs",
    "browser_extract",
    "browser_login",
    "browser_storage_state",
    "browser_screenshot",
}


def test_network_permission_set_covers_every_web_and_browser_builtin():
    from lumibot.components.agents.manager import NETWORK_TOOL_NAMES

    namespace_tools = set()
    for namespace in (BuiltinTools.web, BuiltinTools.browser):
        for attribute in dir(namespace):
            if not attribute.startswith("_"):
                namespace_tools.add(getattr(namespace, attribute)().name)

    assert namespace_tools == _NETWORK_TOOL_NAMES
    assert set(NETWORK_TOOL_NAMES) == _NETWORK_TOOL_NAMES
    assert _NETWORK_TOOL_NAMES.issubset({definition.name for definition in BuiltinTools.all()})


@pytest.mark.parametrize("allow_trading", [True, False])
def test_default_agent_gets_no_outbound_network_tools(allow_trading):
    agent = AgentManager(_Strategy()).create(name="trader", allow_trading=allow_trading)
    tool_names = {tool.name for tool in agent._ensure_bound_tools()}

    assert agent.allow_network is False
    assert not (tool_names & _NETWORK_TOOL_NAMES)
    assert "account_portfolio" in tool_names
    assert "options_get_chain" in tool_names


def test_allow_network_opts_the_agent_into_web_and_browser_tools():
    agent = AgentManager(_Strategy()).create(name="researcher", allow_trading=False, allow_network=True)
    tool_names = {tool.name for tool in agent._ensure_bound_tools()}

    assert agent.allow_network is True
    assert _NETWORK_TOOL_NAMES.issubset(tool_names)


def test_explicitly_listed_network_tool_is_an_opt_in_for_that_tool_only():
    agent = AgentManager(_Strategy()).create(
        name="researcher",
        allow_trading=False,
        tools=[BuiltinTools.web.http_request()],
    )
    tool_names = {tool.name for tool in agent._ensure_bound_tools()}

    assert "http_request" in tool_names
    assert not (tool_names & (_NETWORK_TOOL_NAMES - {"http_request"}))


def test_allow_network_false_removes_even_explicit_network_tools():
    agent = AgentManager(_Strategy()).create(
        name="researcher",
        allow_trading=False,
        allow_network=False,
        tools=[BuiltinTools.web.http_request(), BuiltinTools.browser.navigate()],
    )
    tool_names = {tool.name for tool in agent._ensure_bound_tools()}

    assert not (tool_names & _NETWORK_TOOL_NAMES)


def _example_network_agents():
    from lumibot.example_strategies.ai_fear_and_greed_trading_bot import FearAndGreedTradingBot
    from lumibot.example_strategies.ai_nancy_pelosi_copy_trading_bot import NancyPelosiCopyTradingBot
    from lumibot.example_strategies.ai_nancy_pelosi_trading_bot import NancyPelosiTradingBot

    return {
        "ai_nancy_pelosi_trading_bot.py": (NancyPelosiTradingBot, {"researcher"}),
        "ai_nancy_pelosi_copy_trading_bot.py": (NancyPelosiCopyTradingBot, {"researcher"}),
        "ai_fear_and_greed_trading_bot.py": (FearAndGreedTradingBot, {"researcher"}),
    }


def test_every_example_that_uses_network_tools_is_covered_by_the_opt_in_contract():
    import re
    from pathlib import Path

    examples = Path(__file__).resolve().parents[1] / "lumibot" / "example_strategies"
    # Plain-English prompts no longer name the tools, so the opt-in itself counts too.
    pattern = re.compile(
        r"\b(http_request|read_document|rss_fetch|browser_[a-z_]+|persistent browser|browser session|web browser)\b"
        r"|allow_network=True"
    )
    users = {path.name for path in examples.glob("*.py") if pattern.search(path.read_text(encoding="utf-8"))}

    assert users == set(_example_network_agents())


@pytest.mark.parametrize(
    "filename",
    ["ai_nancy_pelosi_trading_bot.py", "ai_nancy_pelosi_copy_trading_bot.py", "ai_fear_and_greed_trading_bot.py"],
)
def test_examples_that_use_the_web_opt_in_only_the_agents_that_fetch(filename):
    strategy_class, expected = _example_network_agents()[filename]
    created = []

    class _Agents(dict):
        def create(self, **kwargs):
            created.append(kwargs)

    context = SimpleNamespace(
        agents=_Agents(),
        parameters=dict(getattr(strategy_class, "parameters", {}) or {}),
        log_message=lambda *args, **kwargs: None,
    )
    strategy_class.initialize(context)

    opted_in = {item["name"] for item in created if item.get("allow_network") is True}
    assert opted_in == expected
    for item in created:
        if item["name"] not in expected:
            assert not item.get("allow_network"), item["name"]


class _SlowVars(dict):
    """A vars store that writes slowly, widening read-modify-write windows."""

    def get(self, key, default=None):
        return super().get(key, default)

    def set(self, key, value):
        import time

        time.sleep(0.01)
        self[key] = value


def test_run_together_keeps_model_calls_parallel_but_serializes_shared_state(monkeypatch, tmp_path):
    import threading
    import time

    monkeypatch.setenv("LUMIBOT_CACHE_FOLDER", str(tmp_path))
    monkeypatch.delenv("LUMIBOT_AGENT_MAX_MODEL_CALLS", raising=False)
    strategy = _Strategy()
    # Own parameters: other tests leave a model-call limit on the shared class dict.
    strategy.parameters = {}
    strategy.vars = _SlowVars()
    strategy.is_backtesting = False
    manager = AgentManager(strategy)

    agent_names = ["bull", "bear", "judge"]
    runs_per_agent = 3
    ledger = {"count": 0}
    both_models_running = threading.Barrier(len(agent_names), timeout=5)

    def record_tick(note: str) -> dict:
        """Append one tick to a shared ledger."""
        current = ledger["count"]
        time.sleep(0.01)
        ledger["count"] = current + 1
        return {"ok": True, "count": ledger["count"], "note": note}

    class _ParallelRuntime:
        def __init__(self):
            self.first_run = True

        def run(self, request):
            from lumibot.components.agents.runtime import _wrap_tool_callable

            if self.first_run:
                self.first_run = False
                # Every agent's model call must be in flight at the same time.
                both_models_running.wait()
            tool_context = {"agent_name": request.agent_name, "model_call_id": request.model_call_id}
            tool_map = {tool.name: _wrap_tool_callable(tool, tool_context) for tool in request.bound_tools}
            for index in range(4):
                tool_map["record_tick"](note=f"{request.agent_name}-{index}")
            summary = f"{request.agent_name} done"
            return AgentRunResult(
                summary=summary, model=request.model, events=[AgentTraceEvent(kind="text", text=summary)]
            )

    for name in agent_names:
        manager.create(
            name=name,
            model="openai/gpt-5.4-mini",
            allow_trading=False,
            include_builtin_tools=False,
            tools=[record_tick],
            _runtime=_ParallelRuntime(),
        )

    for _ in range(runs_per_agent):
        results = manager.run_together([(name, f"{name} task", None) for name in agent_names])
        assert set(results) == set(agent_names)

    total_runs = len(agent_names) * runs_per_agent
    assert ledger["count"] == total_runs * 4

    state = strategy.vars.get("_agent_runtime_state")
    assert set(state) == set(agent_names)
    for name in agent_names:
        assert len(state[name]["runs"]) == runs_per_agent
        assert len(state[name]["memory_notes"]) == runs_per_agent

    summary_rows = [
        json.loads(line) for line in (tmp_path / "agent_runtime" / "agent_run_summaries.jsonl").read_text().splitlines()
    ]
    assert len(summary_rows) == total_runs
    assert {row["agent_name"] for row in summary_rows} == set(agent_names)

    for name in agent_names:
        assert manager._observability_call_index[name] == runs_per_agent


def test_durable_private_tool_journal_precedes_call_and_links_actual_outcome():
    import hashlib
    from uuid import UUID

    records, observed = [], []
    context = {"tool_journal_callback": lambda record: records.append(dict(record))}

    def read(secret: str) -> dict:
        observed.append(list(records))
        return {"value": 42}

    result = _agent_declared_callable(BoundTool("read", "Read.", read), context)(
        secret="synthetic-private-value", agent_purpose=PRIVATE_TOOL_PURPOSE
    )
    assert result == {"value": 42}
    assert len(records) == 2
    started, completed = records
    assert observed == [[started]]
    UUID(started["action_id"])
    assert completed["action_id"] == started["action_id"]
    assert started["status"] == "started" and "ok" not in started
    assert completed["status"] == "completed" and completed["ok"] is True
    assert started["tool_name"] == "read" and started["purpose"] == PRIVATE_TOOL_PURPOSE
    expected = json.dumps(
        {"args": [], "kwargs": {"secret": "synthetic-private-value"}},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )
    assert started["arguments_sha256"] == hashlib.sha256(expected.encode("utf-8")).hexdigest()
    assert set(started) == {"action_id", "tool_name", "purpose", "status", "arguments_sha256"}
    assert "synthetic-private-value" not in json.dumps(records)
    assert context["tool_calls"][0]["action_id"] == started["action_id"]


@pytest.mark.parametrize("fail_status", ["started", "completed"])
def test_durable_private_tool_journal_failure_is_visible_without_false_completion(fail_status):
    records, invoked = [], []

    def journal(record):
        records.append(dict(record))
        if record["status"] == fail_status:
            raise RuntimeError("synthetic private credential detail must never enter the public error")

    context = {"tool_journal_callback": journal}
    wrapped = _agent_declared_callable(
        BoundTool("submit_order", "Submit.", lambda **kwargs: invoked.append(kwargs) or {"id": "order-1"}), context
    )
    result = wrapped(symbol="SPY", agent_purpose=PRIVATE_TOOL_PURPOSE)
    assert result["ok"] is False and result["tool_error"] is True
    assert result["error"]["code"] == "tool_journal_unavailable"
    assert "synthetic private credential" not in json.dumps(result)
    assert len(invoked) == (0 if fail_status == "started" else 1)
    assert context["tool_calls"][0]["status"] == "journal_failed"
    assert context["tool_calls"][0]["ok"] is False


def test_durable_private_tool_journal_records_remote_failure_code_without_payload():
    from lumibot.components.agents.runtime import RemoteMCPToolError

    records = []

    def read(**kwargs):
        raise RemoteMCPToolError(
            "query_data",
            {
                "isError": True,
                "structuredContent": {
                    "errorCode": "unsupported_capability",
                    "message": "Original archive unavailable.",
                    "private": "fixture-credential",
                },
            },
        )

    result = _agent_declared_callable(
        BoundTool("query_data", "Query.", read), {"tool_journal_callback": lambda record: records.append(dict(record))}
    )(datasetId="sec", agent_purpose=PRIVATE_TOOL_PURPOSE)
    assert result["error"]["code"] == "unsupported_capability"
    assert len(records) == 2 and records[1]["status"] == "failed" and records[1]["ok"] is False
    assert records[1]["error_code"] == "unsupported_capability"
    assert "fixture-credential" not in json.dumps(records)


def test_agent_purpose_field_helper_is_collision_safe_and_programmatic_sink_is_unchanged():
    from lumibot.components.agents.runtime import _wrap_tool_callable, agent_purpose_parameter_name

    assert agent_purpose_parameter_name({"symbol"}) == "agent_purpose"
    assert agent_purpose_parameter_name({"agent_purpose", "agent_purpose_2", "purpose"}) == "agent_purpose_3"
    records = []
    wrapped = _wrap_tool_callable(
        BoundTool("read", "Read.", lambda symbol: symbol), {"tool_journal_callback": records.append}
    )
    assert wrapped(symbol="SPY") == "SPY"
    assert records == []


def _native_skill_fixture():
    from google.adk.skills.models import Frontmatter, Resources, Skill
    from google.adk.tools.skill_toolset import SkillToolset

    skill = Skill(
        frontmatter=Frontmatter(name="audit-fixture", description="Synthetic local skill."),
        instructions="Use only permitted evidence.",
        resources=Resources(references={"source.md": "Synthetic official observation."}),
    )
    return SkillToolset(skills=[skill]), SimpleNamespace(
        state={}, agent_name="researcher", invocation_id="fixture-invocation"
    )


@pytest.mark.parametrize(
    "name,arguments",
    [
        ("list_skills", {}),
        ("load_skill", {"skill_name": "audit-fixture"}),
        ("load_skill_resource", {"skill_name": "audit-fixture", "file_path": "references/source.md"}),
    ],
)
def test_native_skill_tools_require_purpose_and_audit_before_actual_native_call(name, arguments, monkeypatch):
    import asyncio

    from google.adk.tools.base_tool import BaseTool
    from google.adk.tools.base_toolset import BaseToolset
    from google.genai import types

    from lumibot.components.agents.managed_gateway import _tools
    from lumibot.components.agents.runtime import _agent_toolset_with_purpose

    native, native_context = _native_skill_fixture()
    original = next(tool for tool in asyncio.run(native.get_tools()) if tool.name == name)
    snapshots, actual_args, records = [], [], []
    native_run = original.run_async

    async def observed_run(*, args, tool_context):
        assert tool_context is native_context
        snapshots.append(list(records))
        actual_args.append(dict(args))
        return await native_run(args=args, tool_context=tool_context)

    monkeypatch.setattr(original, "run_async", observed_run)
    context = {"tool_journal_callback": lambda record: records.append(dict(record))}
    adapted = _agent_toolset_with_purpose(native, context)
    assert isinstance(adapted, BaseToolset)
    tools = asyncio.run(adapted.get_tools_with_prefix())
    tool = next(tool for tool in tools if tool.name == name)
    assert isinstance(tool, BaseTool)
    declaration = tool._get_declaration()
    schema = _tools(SimpleNamespace(config=SimpleNamespace(tools=[types.Tool(function_declarations=[declaration])])))[
        0
    ]["inputSchema"]
    assert "agent_purpose" in schema["required"]
    raw_declaration = original._get_declaration()
    assert "agent_purpose" not in raw_declaration.parameters_json_schema["properties"]
    denied = asyncio.run(tool.run_async(args=arguments, tool_context=native_context))
    assert denied["tool_error"] is True and denied["ok"] is False
    assert actual_args == [] and records == []
    result = asyncio.run(
        tool.run_async(args={**arguments, "agent_purpose": PRIVATE_TOOL_PURPOSE}, tool_context=native_context)
    )
    assert actual_args == [arguments] and snapshots == [[records[0]]]
    assert [row["status"] for row in records] == ["started", "completed"]
    if name == "list_skills":
        assert "audit-fixture" in result
    elif name == "load_skill":
        assert result["instructions"] == "Use only permitted evidence."
        assert native.list_active_skills(native_context) == ["audit-fixture"]
    else:
        assert result["content"] == "Synthetic official observation."


def test_native_skill_request_processing_and_close_preserve_underlying_owner(monkeypatch):
    import asyncio

    from google.adk.models.llm_request import LlmRequest

    from lumibot.components.agents.runtime import _agent_toolset_with_purpose

    native, native_context = _native_skill_fixture()
    adapted = _agent_toolset_with_purpose(native, {})
    llm_request = LlmRequest(model="gemini-2.5-flash")
    asyncio.run(adapted.process_llm_request(tool_context=native_context, llm_request=llm_request))
    assert llm_request.config.system_instruction
    tools = asyncio.run(adapted.get_tools_with_prefix())
    for tool in tools:
        asyncio.run(tool.process_llm_request(tool_context=native_context, llm_request=llm_request))
    assert all(
        "agent_purpose" in declaration.parameters_json_schema["required"]
        for entry in llm_request.config.tools
        for declaration in entry.function_declarations or []
    )
    assert all(llm_request.tools_dict[tool.name] is tool for tool in tools)
    closed = []

    async def close():
        closed.append(True)

    monkeypatch.setattr(native, "close", close)
    asyncio.run(adapted.close())
    assert closed == [True]


def test_durable_journal_callback_and_native_skill_adapter_reach_runtime_assembly(monkeypatch):
    import asyncio

    from google.adk.tools.base_toolset import BaseToolset
    from google.adk.tools.function_tool import FunctionTool
    from google.genai import types

    from lumibot.components.agents import runtime as runtime_module

    captured, records = {}, []

    class Captured(Exception):
        pass

    class Agent:
        def __init__(self, **kwargs):
            captured.update(kwargs)
            raise Captured()

    runtime = runtime_module.GoogleADKRuntime()
    monkeypatch.setattr(runtime, "_ensure_adk", lambda: (Agent, object, types, FunctionTool))
    monkeypatch.setattr(runtime_module, "_resolve_model_for_adk", lambda model, **_kwargs: model)
    # As in the construction-only instruction contract above, model metadata
    # is fixture-owned so this assertion cannot start a login or model request.
    monkeypatch.setattr(runtime_module, "_model_max_output_tokens", lambda _model: None)
    request = runtime_module.RuntimeRequest(
        agent_name="researcher",
        model="chatgpt/gpt-6-sol",
        system_prompt="Read.",
        task_prompt="Read.",
        context=None,
        runtime_context={},
        memory_state=None,
        memory_notes=[],
        bound_tools=[BoundTool("read", "Read.", lambda: 42)],
        tool_journal_callback=records.append,
    )
    with pytest.raises(Captured):
        asyncio.run(runtime._run_async(request))
    assert captured["tools"][0].func(agent_purpose=PRIVATE_TOOL_PURPOSE) == 42
    assert len(records) == 2
    skillset = next(tool for tool in captured["tools"] if isinstance(tool, BaseToolset))
    declaration = asyncio.run(skillset.get_tools())[0]._get_declaration()
    assert "agent_purpose" in declaration.parameters_json_schema["required"]


@pytest.mark.parametrize("fail_status", ["started", "completed"])
def test_native_skill_durable_journal_failure_does_not_invoke_or_claim_completion(fail_status, monkeypatch):
    import asyncio

    from lumibot.components.agents.runtime import _agent_toolset_with_purpose

    native, native_context = _native_skill_fixture()
    original = asyncio.run(native.get_tools())[0]
    invoked = []
    native_run = original.run_async

    async def observed(*, args, tool_context):
        invoked.append(dict(args))
        return await native_run(args=args, tool_context=tool_context)

    monkeypatch.setattr(original, "run_async", observed)

    def journal(record):
        if record["status"] == fail_status:
            raise RuntimeError("synthetic private sink payload")

    context = {"tool_journal_callback": journal}
    adapted = asyncio.run(_agent_toolset_with_purpose(native, context).get_tools())[0]
    result = asyncio.run(adapted.run_async(args={"agent_purpose": PRIVATE_TOOL_PURPOSE}, tool_context=native_context))
    assert result["tool_error"] is True and result["ok"] is False
    assert result["error"]["code"] == "tool_journal_unavailable"
    assert "synthetic private sink" not in json.dumps(result)
    assert len(invoked) == (0 if fail_status == "started" else 1)
    assert context["tool_calls"][0]["status"] == "journal_failed"


def test_native_skill_actual_failed_response_and_prefix_remain_native(monkeypatch):
    import asyncio

    from lumibot.components.agents.runtime import _agent_toolset_with_purpose

    native, native_context = _native_skill_fixture()
    native.tool_name_prefix = "strategy"
    records, context = [], {}
    context["tool_journal_callback"] = lambda record: records.append(dict(record))
    adapted = _agent_toolset_with_purpose(native, context)
    tools = asyncio.run(adapted.get_tools_with_prefix())
    tool = next(tool for tool in tools if tool.name == "strategy_load_skill")
    assert tool._get_declaration().name == "strategy_load_skill"
    result = asyncio.run(
        tool.run_async(
            args={"skill_name": "missing-fixture", "agent_purpose": PRIVATE_TOOL_PURPOSE}, tool_context=native_context
        )
    )
    assert result["error_code"] == "SKILL_NOT_FOUND"
    assert records[-1]["status"] == "failed" and records[-1]["ok"] is False
    assert records[-1]["error_code"] == "SKILL_NOT_FOUND"
    assert native.list_active_skills(native_context) == []


def test_native_skill_resource_request_processing_preserves_binary_resource_injection():
    import asyncio

    from google.adk.models.llm_request import LlmRequest
    from google.genai import types

    from lumibot.components.agents.runtime import _agent_toolset_with_purpose

    native, native_context = _native_skill_fixture()
    native.skills[0].resources.assets["chart.png"] = b"\x89PNG\r\n\x1a\nsynthetic-local-fixture"
    adapted = _agent_toolset_with_purpose(native, {})
    resource = next(tool for tool in asyncio.run(adapted.get_tools()) if tool.name == "load_skill_resource")
    result = asyncio.run(
        resource.run_async(
            args={
                "skill_name": "audit-fixture",
                "file_path": "assets/chart.png",
                "agent_purpose": PRIVATE_TOOL_PURPOSE,
            },
            tool_context=native_context,
        )
    )
    request = LlmRequest(
        model="gemini-2.5-flash",
        contents=[
            types.Content(
                role="user",
                parts=[
                    types.Part(function_response=types.FunctionResponse(name="load_skill_resource", response=result))
                ],
            )
        ],
    )
    asyncio.run(resource.process_llm_request(tool_context=native_context, llm_request=request))
    assert any(
        part.inline_data and part.inline_data.data == native.skills[0].resources.assets["chart.png"]
        for content in request.contents
        for part in content.parts or []
    )
    assert request.tools_dict[resource.name] is resource


def test_native_additional_tool_collision_preserves_original_schema_arguments_and_confirmation():
    import asyncio

    from google.adk.tools.base_tool import BaseTool
    from google.genai import types

    from lumibot.components.agents.runtime import _agent_toolset_with_purpose

    native, native_context = _native_skill_fixture()
    invoked, confirmations = [], []

    class DomainTool(BaseTool):
        def __init__(self):
            super().__init__(name="domain_read", description="Synthetic dynamic tool.")

        def _get_declaration(self):
            return types.FunctionDeclaration(
                name=self.name,
                parameters=types.Schema(
                    type="OBJECT", properties={"agent_purpose": types.Schema(type="STRING")}, required=["agent_purpose"]
                ),
            )

        async def run_async(self, *, args, tool_context):
            invoked.append((dict(args), tool_context))
            return args

        async def check_require_confirmation(self, args, tool_context):
            confirmations.append(dict(args))
            return True

    native._tools.append(DomainTool())
    tool = next(
        tool for tool in asyncio.run(_agent_toolset_with_purpose(native, {}).get_tools()) if tool.name == "domain_read"
    )
    assert set(tool._get_declaration().parameters_json_schema["required"]) == {"agent_purpose", "agent_purpose_2"}
    args = {"agent_purpose": "domain-selection", "agent_purpose_2": PRIVATE_TOOL_PURPOSE}
    assert asyncio.run(tool.check_require_confirmation(args, native_context)) is True
    assert asyncio.run(tool.run_async(args=args, tool_context=native_context)) == {"agent_purpose": "domain-selection"}
    assert confirmations == [{"agent_purpose": "domain-selection"}]
    assert invoked == [({"agent_purpose": "domain-selection"}, native_context)]
