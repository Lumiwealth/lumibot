from types import SimpleNamespace

from google.genai import types

from lumibot.components.agents.manager import (
    _coalesce_trace_events,
    _managed_ai_error_status,
    _managed_ai_terminal_status,
)
from lumibot.components.agents.managed_gateway import ManagedAiGatewayError
from lumibot.components.agents.runtime import _normalize_event
from lumibot.components.agents.schemas import AgentRunResult, AgentTraceEvent


def _result(*events):
    return AgentRunResult(summary="Done", model="test-model", events=list(events))


def test_trading_agent_without_order_finishes_as_completed_no_action():
    result = _result(AgentTraceEvent(kind="text", text="No setup today."))
    assert _managed_ai_terminal_status(result, allow_trading=True) == "completed_no_action"


def test_successful_order_finishes_as_completed_decision():
    result = _result(
        AgentTraceEvent(kind="tool_call", tool_name="orders_submit_multileg", payload={}),
        AgentTraceEvent(
            kind="tool_result",
            tool_name="orders_submit_multileg",
            payload={"status": "submitted", "identifier": "order-1"},
        ),
    )
    assert _managed_ai_terminal_status(result, allow_trading=True) == "completed_decision"


def test_read_only_open_orders_check_does_not_claim_a_completed_decision():
    result = _result(
        AgentTraceEvent(kind="tool_call", tool_name="orders_open_orders", payload={}),
        AgentTraceEvent(
            kind="tool_result",
            tool_name="orders_open_orders",
            payload={"orders": [], "complete": True},
        ),
    )

    assert _managed_ai_terminal_status(result, allow_trading=True) == "completed_no_action"


def test_unrecovered_structural_tool_error_is_not_reported_as_completed():
    result = _result(
        AgentTraceEvent(kind="tool_call", tool_name="account_positions", payload={}),
        AgentTraceEvent(
            kind="tool_result",
            tool_name="account_positions",
            payload={"tool_error": True, "error": {"type": "BrokerUnavailable"}},
        ),
    )
    assert _managed_ai_terminal_status(result, allow_trading=True) == "tool_error"


def test_recovered_structural_tool_error_allows_completed_no_action():
    result = _result(
        AgentTraceEvent(
            kind="tool_result",
            tool_name="get_indicator",
            payload={"tool_error": True, "error": {"type": "ValueError"}},
        ),
        AgentTraceEvent(
            kind="tool_result",
            tool_name="get_indicator",
            payload={"value": 100000.0, "asset_type": "crypto", "quote_symbol": "USD"},
        ),
        AgentTraceEvent(kind="text", text="BTC/USD SMA is 100000. No trade requested."),
    )

    assert _managed_ai_terminal_status(result, allow_trading=True) == "completed_no_action"


def test_later_error_after_success_remains_unrecovered():
    result = _result(
        AgentTraceEvent(kind="tool_result", tool_name="get_indicator", payload={"value": 100000.0}),
        AgentTraceEvent(
            kind="tool_result",
            tool_name="get_indicator",
            payload={"tool_error": True, "error": {"type": "ConnectionError"}},
        ),
    )

    assert _managed_ai_terminal_status(result, allow_trading=True) == "tool_error"


def test_non_trading_research_agent_can_complete_without_an_order():
    result = _result(AgentTraceEvent(kind="text", text="Setup found."))
    assert _managed_ai_terminal_status(result, allow_trading=False) == "completed_decision"


def test_typed_gateway_errors_remain_distinguishable_from_runtime_errors():
    assert (
        _managed_ai_error_status(ManagedAiGatewayError("bad parts", code="protocol_integrity_error"))
        == "protocol_integrity_error"
    )
    assert (
        _managed_ai_error_status(ManagedAiGatewayError("provider rejected", code="provider_auth_failed"))
        == "provider_error"
    )
    assert _managed_ai_error_status(RuntimeError("unexpected")) == "runtime_error"


def test_normalized_tool_events_preserve_native_call_ids():
    event = SimpleNamespace(
        content=types.Content(
            role="model",
            parts=[
                types.Part(function_call=types.FunctionCall(id="call-portfolio", name="account_portfolio", args={})),
                types.Part(
                    function_response=types.FunctionResponse(
                        id="call-portfolio",
                        name="account_portfolio",
                        response={"cash": 100000},
                    )
                ),
            ],
        ),
        usage_metadata=None,
    )
    normalized = _normalize_event(event)
    assert [item.call_id for item in normalized] == ["call-portfolio", "call-portfolio"]


def test_adjacent_streamed_text_deltas_are_coalesced_without_crossing_tool_boundaries():
    events = [
        AgentTraceEvent(kind="text", text="Inspect"),
        AgentTraceEvent(kind="text", text=" account"),
        AgentTraceEvent(kind="tool_call", tool_name="account_portfolio", call_id="call-1"),
        AgentTraceEvent(kind="text", text="Then"),
        AgentTraceEvent(kind="text", text=" continue"),
    ]
    coalesced = _coalesce_trace_events(events)
    assert [(event.kind, event.text, event.call_id) for event in coalesced] == [
        ("text", "Inspect account", None),
        ("tool_call", None, "call-1"),
        ("text", "Then continue", None),
    ]


_HALLUCINATED = "orders_submit_multilevel_package_check_not_needed_use_orders_submit_multileg"


def test_unknown_tool_call_is_returned_to_the_model_instead_of_ending_the_run():
    """Release eval options_iron_condor_atomic_open (all12-final rep 1): the model
    called a tool name that does not exist; ADK raised ValueError and the whole
    run ended with no decision. The model now gets a structured tool error."""
    from lumibot.components.agents.runtime import GoogleADKRuntime

    callback = GoogleADKRuntime._unknown_tool_error_callback
    error = ValueError(f"Tool '{_HALLUCINATED}' not found.\nAvailable tools: account_positions, orders_submit_multileg")

    response = callback(tool=SimpleNamespace(name=_HALLUCINATED), args={}, tool_context=None, error=error)

    assert response["tool_error"] is True
    assert response["unknown_tool"] is True
    assert response["tool_name"] == _HALLUCINATED
    assert "does not exist" in response["error"]["message"]
    assert "orders_submit_multileg" in response["error"]["message"]
    other = callback(tool=SimpleNamespace(name="x"), args={}, tool_context=None, error=RuntimeError("boom"))
    assert other is None


def test_adk_agent_is_built_with_the_unknown_tool_callback(monkeypatch):
    import asyncio

    import pytest

    from lumibot.components.agents.runtime import GoogleADKRuntime, RuntimeRequest

    captured = {}

    class _Stop(Exception):
        pass

    class _FakeAgent:
        def __init__(self, **kwargs):
            captured.update(kwargs)
            raise _Stop()

    runtime = GoogleADKRuntime()
    real = runtime._ensure_adk
    monkeypatch.setattr(runtime, "_ensure_adk", lambda: (_FakeAgent, *real()[1:]))
    request = RuntimeRequest(
        agent_name="test",
        model="gemini-3.5-flash-lite",
        system_prompt="test",
        task_prompt="test",
        context=None,
        runtime_context=None,
        memory_state=None,
        memory_notes=[],
        bound_tools=[],
    )
    with pytest.raises(_Stop):
        asyncio.run(runtime._run_async(request))

    assert captured["on_tool_error_callback"] is GoogleADKRuntime._unknown_tool_error_callback


def test_unknown_tool_error_does_not_block_a_decision_the_agent_completed():
    result = _result(
        AgentTraceEvent(kind="tool_call", tool_name=_HALLUCINATED, payload={}),
        AgentTraceEvent(
            kind="tool_result",
            tool_name=_HALLUCINATED,
            payload={"ok": False, "tool_error": True, "unknown_tool": True, "tool_name": _HALLUCINATED},
        ),
        AgentTraceEvent(kind="tool_call", tool_name="orders_submit_multileg", payload={}),
        AgentTraceEvent(kind="tool_result", tool_name="orders_submit_multileg", payload={"identifier": "o-1"}),
    )
    assert _managed_ai_terminal_status(result, allow_trading=True) == "completed_decision"

    no_order = _result(
        AgentTraceEvent(kind="tool_call", tool_name=_HALLUCINATED, payload={}),
        AgentTraceEvent(
            kind="tool_result",
            tool_name=_HALLUCINATED,
            payload={"ok": False, "tool_error": True, "unknown_tool": True, "tool_name": _HALLUCINATED},
        ),
        AgentTraceEvent(kind="text", text="No setup."),
    )
    assert _managed_ai_terminal_status(no_order, allow_trading=True) == "completed_no_action"
