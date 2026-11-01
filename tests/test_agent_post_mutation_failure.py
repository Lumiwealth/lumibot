"""Provider failures must retain prior actions without permitting whole-run replay."""

from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from lumibot.components.agents import AgentManager
from lumibot.components.agents import runtime as runtime_module
from lumibot.components.agents.runtime import GoogleADKRuntime


class InternalServerError(Exception):
    pass


class Variables(dict):
    def set(self, key, value):
        self[key] = value


class Strategy:
    is_backtesting = True

    def __init__(self):
        self.parameters = {}
        self.vars = Variables()
        self.messages = []

    def get_datetime(self):
        return datetime(2026, 1, 2, tzinfo=timezone.utc)

    def log_message(self, message, **kwargs):
        self.messages.append(message)


@pytest.fixture(autouse=True)
def isolated_runtime(monkeypatch, tmp_path):
    monkeypatch.setenv("LUMIBOT_CACHE_FOLDER", str(tmp_path))
    monkeypatch.setenv("LUMIBOT_AGENT_RUN_TIMEOUT_SECONDS", "0")
    monkeypatch.setenv("LUMIBOT_AGENT_MAX_RUN_ATTEMPTS", "3")
    monkeypatch.setattr("time.sleep", lambda _: None)


MUTATIONS = ["orders_submit_order", "orders_submit_orders", "orders_submit_multileg",
             "orders_cancel_order", "orders_modify_order"]


@pytest.mark.parametrize("tool", MUTATIONS)
@pytest.mark.parametrize("status", ["started", "completed", "failed", "unknown", "journal_failed"])
def test_failure_after_mutation_never_claims_no_effect_or_retries(monkeypatch, capsys, tool, status):
    runtime = GoogleADKRuntime()
    attempts = []

    async def fails(request):
        attempts.append(request)
        record = {"tool_name": tool, "status": status, "ok": status == "completed"}
        if status == "unknown":
            record.update(status="completed", broker_state_certainty="unknown")
        request._attempt_tool_calls.append(record)
        raise InternalServerError("DNS transport failed")

    monkeypatch.setattr(runtime, "_run_async", fails)
    strategy = Strategy()
    handle = AgentManager(strategy).create(name="trader", allow_trading=True, _runtime=runtime)
    result = handle.run(task_prompt="Complete the authorized task.")
    assert len(attempts) == 1
    assert result.payload["broker_mutation_attempted"] is True
    outcome = result.payload["execution_outcome"]
    assert outcome["decision_completed"] is False
    assert outcome["broker_state_certainty"] == "unknown"
    assert outcome["retryability"] == "non_retryable"
    assert outcome["impact"] == "operator_attention_required"
    assert "reconcil" in result.summary.lower()
    assert "no trades placed" not in result.summary + capsys.readouterr().err
    assert "could not run" not in " ".join(strategy.messages)
    assert result.tool_calls == result.tool_results == []  # No invented observations.


@pytest.mark.parametrize("read_only", [False, True])
@pytest.mark.parametrize("allow_trading", [False, True])
def test_pre_mutation_failure_keeps_bounded_retries(monkeypatch, read_only, allow_trading):
    runtime = GoogleADKRuntime()
    attempts = []

    async def fails(request):
        attempts.append(request)
        if read_only:
            request._attempt_tool_calls.append({"tool_name": "market_quotes", "ok": True})
        raise InternalServerError("DNS transport failed")

    monkeypatch.setattr(runtime, "_run_async", fails)
    result = AgentManager(Strategy()).create(name="agent", allow_trading=allow_trading, _runtime=runtime).run(task_prompt="Inspect.")
    assert len(attempts) == 3
    assert result.payload["broker_mutation_attempted"] is False
    assert result.payload["execution_outcome"]["retryability"] == "retryable"
    assert result.payload["execution_outcome"]["broker_state_certainty"] == "not_observed"


def test_missing_runtime_mutation_trace_is_unknown():
    class AlternateRuntime:
        def run(self, request):
            raise InternalServerError("DNS transport failed")

    result = AgentManager(Strategy()).create(name="trader", allow_trading=True, _runtime=AlternateRuntime()).run(task_prompt="Inspect.")
    assert result.payload["broker_mutation_attempted"] is None
    assert result.payload["execution_outcome"]["broker_state_certainty"] == "unknown"
    assert result.payload["execution_outcome"]["retryability"] == "non_retryable"
    assert "no trades placed" not in result.summary


@pytest.mark.parametrize("observe_mutation_result", [False, True])
def test_real_fixture_fill_and_observed_events_survive_later_dns_failure(monkeypatch, observe_mutation_result):
    from google.genai import types
    from scripts.agent_eval_production_fixture import ProductionFixture
    from scripts.run_agent_evals import build_fixture

    production = ProductionFixture(build_fixture("flat_stock_account"))
    invocations = []
    observed = []

    class Runner:
        def __init__(self, *, agent, app_name):
            self.agent, self.app_name = agent, app_name
            self.session_service = self

        async def create_session(self, **kwargs):
            pass

        async def run_async(self, **kwargs):
            invocations.append(1)
            yield SimpleNamespace(usage_metadata={"prompt_token_count": 7, "candidates_token_count": 3, "total_token_count": 10})
            tools = {tool.__name__: tool for tool in self.agent.tools if callable(tool)}
            calls = [
                ("market_quote", {"symbol": "AAPL"}),
                ("orders_submit_order", {"symbol": "AAPL", "quantity": 2, "side": "buy", "order_type": "market",
                                         "reason": "Use the current quoted equity price and complete account evidence for the authorized two-share purchase. The small allocation fits available cash. Confirm the exact broker identifier and actual fills before reporting execution or taking another action."}),
            ]
            for name, args in calls:
                call_id = f"observed-{name}"
                yield SimpleNamespace(content=types.Content(parts=[types.Part(function_call=types.FunctionCall(name=name, args=args, id=call_id))]))
                response = tools[name](**args, agent_purpose="Execute this bounded fixture operation and inspect its actual result.")
                assert response.get("tool_error") is not True, response
                if name == "orders_submit_order" and not observe_mutation_result:
                    production.settle()
                    raise InternalServerError("Response lost after the actual order action")
                observed.append(response)
                response_name = "orders_submit_ order" if name == "orders_submit_order" else name
                yield SimpleNamespace(content=types.Content(parts=[types.Part(function_response=types.FunctionResponse(name=response_name, response=response, id=call_id))]))
            production.settle()
            raise InternalServerError("DNS transport failed after the order response")

    runtime = GoogleADKRuntime()
    monkeypatch.setattr(runtime, "_ensure_adk", lambda: (SimpleNamespace, Runner, types, lambda function: function))
    monkeypatch.setattr(runtime_module, "_resolve_model_for_adk", lambda *args, **kwargs: "offline-model")
    try:
        handle = production.manager.create(name="trader", allow_trading=True, _runtime=runtime, include_builtin_skills=False)
        result = handle.run(task_prompt="Execute the authorized fixture trade.")
        orders = production.capture(result)
        assert len(invocations) == len(orders) == 1
        assert orders[0]["quantity"] == 2
        assert orders[0]["status"] == "fill"
        assert [event.tool_name for event in result.tool_calls] == ["market_quote", "orders_submit_order"]
        assert [event.payload for event in result.tool_results] == observed
        assert result.usage["total_token_count"] == 10
        assert [event.tool_name for event in result.tool_results] == (["market_quote", "orders_submit_order"] if observe_mutation_result else ["market_quote"])
        assert all(event.timestamp and event.call_id for event in result.tool_calls + result.tool_results)
        assert result.payload["broker_mutation_attempted"] is True
        assert result.payload["execution_outcome"]["retryability"] == "non_retryable"
        assert "no trades placed" not in result.summary
        # Settlement is independent evidence, never a fabricated model-observed fill.
        assert not any(event.tool_name == "orders_get_status" for event in result.events)
    finally:
        production.close()
