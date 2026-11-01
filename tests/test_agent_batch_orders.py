"""Independent batches reuse Strategy.submit_orders and the native broker/journal."""
import json

import pytest

from lumibot.components.agents import BuiltinTools
from lumibot.components.agents.runtime import _wrap_tool_callable
from scripts.agent_eval_production_fixture import ProductionFixture, _capture_broker_order
from scripts.run_agent_evals import build_fixture, score_machine_contract


REASON = ("Establish the explicitly authorized equal-weight stock basket using the current executable "
          "asks and the initial account capital after its cash reserve. These quantities stay within "
          "each allocation and available cash; changed quotes or pending duplicates invalidate the plan.")


@pytest.fixture
def native_batch():
    production = ProductionFixture(build_fixture("portfolio_initial_25"))
    try:
        yield production
    finally:
        production.close()


def batch_tool(production):
    return BuiltinTools.orders.submit_orders().binder(production.strategy, production.manager).function


def rows(count):
    return [{"symbol": f"QZX{i:02d}", "quantity": 39, "side": "buy"} for i in range(1, count + 1)]


@pytest.mark.parametrize("count", [9, 25])
def test_batch_calls_existing_plural_method_once_and_fills_every_order(native_batch, monkeypatch, count):
    original = native_batch.strategy.submit_orders
    calls = []
    def observed(orders, **kwargs):
        calls.append((orders, kwargs))
        return original(orders, **kwargs)
    monkeypatch.setattr(native_batch.strategy, "submit_orders", observed)
    result = batch_tool(native_batch)(orders_json=json.dumps(rows(count)), reason=REASON)
    assert len(calls) == 1
    assert calls[0][1]["is_multileg"] is False
    assert len(result["orders"]) == count
    native_batch.settle()
    orders = native_batch.strategy.get_orders()
    assert len(orders) == count
    assert all(order.is_filled() for order in orders)
    assert all(order.quantity == 39 and order.avg_fill_price == 100 for order in orders)
    assert native_batch.strategy.get_cash() == 100000 - count * 3900
    assert all(order.decision_journal["action_id"] for order in orders)
    assert len({order.decision_journal["action_id"] for order in orders}) == 1
    # A batch is one tool call but count independent broker orders. Requiring
    # 25 submission calls would reject a correctly completed plural submission.
    transcript = {"fixture_calls": [], "submissions": [], "tool_calls": [],
                  "broker_orders": [_capture_broker_order(order) for order in orders]}
    assert score_machine_contract({"machineContract": {"exactBrokerOrderCount": count}}, transcript)["pass"]
    assert not score_machine_contract({"machineContract": {"exactBrokerOrderCount": count + 1}}, transcript)["pass"]


@pytest.mark.parametrize("invalid", [{"quantity": 0}, {"order_type": "limit"}, {"unexpected": True}])
def test_every_row_is_validated_before_any_broker_write(native_batch, invalid):
    requested = rows(2)
    requested[1].update(invalid)
    with pytest.raises((ValueError, TypeError)):
        batch_tool(native_batch)(orders_json=json.dumps(requested), reason=REASON)
    assert native_batch.strategy.get_orders() == []


def test_unknown_partial_submission_returns_all_requested_ids_without_retry(native_batch, monkeypatch):
    calls = []
    def interrupted(orders, **kwargs):
        calls.append(orders)
        native_batch.strategy.submit_order(orders[0], reason=REASON)
        raise ConnectionError("Recorded broker interruption after first acknowledgement")
    monkeypatch.setattr(native_batch.strategy, "submit_orders", interrupted)
    result = batch_tool(native_batch)(orders_json=json.dumps(rows(2)), reason=REASON)
    assert len(calls) == 1
    assert result["tool_error"] is True
    assert result["execution_outcome"]["broker_state_certainty"] == "unknown"
    assert len(result["orders"]) == 2
    assert all(row["identifier"] for row in result["orders"])
    assert len(native_batch.strategy.get_orders()) == 1


def test_batch_requires_quote_for_every_requested_symbol(native_batch):
    context = {"enforce_order_readiness": True, "tool_calls": []}
    definitions = [BuiltinTools.account.portfolio(), BuiltinTools.account.positions(),
                   BuiltinTools.orders.open_orders(), BuiltinTools.market.quotes(),
                   BuiltinTools.orders.submit_orders()]
    tools = {definition.name: _wrap_tool_callable(definition.binder(native_batch.strategy, native_batch.manager), context)
             for definition in definitions}
    tools["account_portfolio"]()
    tools["account_positions"]()
    tools["orders_open_orders"]()
    tools["market_quotes"](symbols_json='["QZX01"]')
    result = tools["orders_submit_orders"](orders_json=json.dumps(rows(2)), reason=REASON)
    assert result["tool_error"] is True
    assert "ORDER_READINESS_REQUIRED" in str(result)
    assert native_batch.strategy.get_orders() == []


def test_unknown_batch_invalidates_previous_account_evidence(native_batch, monkeypatch):
    context = {"enforce_order_readiness": True, "tool_calls": []}
    definitions = [BuiltinTools.account.portfolio(), BuiltinTools.account.positions(),
                   BuiltinTools.orders.open_orders(), BuiltinTools.market.quotes(),
                   BuiltinTools.orders.submit_orders()]
    tools = {d.name: _wrap_tool_callable(d.binder(native_batch.strategy, native_batch.manager), context)
             for d in definitions}
    tools["account_portfolio"]()
    tools["account_positions"]()
    tools["orders_open_orders"]()
    tools["market_quotes"](symbols_json='["QZX01","QZX02"]')
    calls = []
    def interrupted(orders, **kwargs):
        calls.append(orders)
        raise ConnectionError("Recorded uncertain submission")
    monkeypatch.setattr(native_batch.strategy, "submit_orders", interrupted)
    first = tools["orders_submit_orders"](orders_json=json.dumps(rows(2)), reason=REASON)
    second = tools["orders_submit_orders"](orders_json=json.dumps(rows(2)), reason=REASON)
    assert first["execution_outcome"]["broker_state_certainty"] == "unknown"
    assert "ORDER_READINESS_REQUIRED" in str(second)
    assert len(calls) == 1


def test_batch_preserves_exact_option_contracts_and_uses_independent_execution(native_batch, monkeypatch):
    requested = [{"symbol": "SPY", "asset_type": "option", "expiration": "2026-08-28",
                  "strike": strike, "right": "call", "quantity": 1, "side": "buy_to_open"}
                 for strike in (602, 604)]
    original = native_batch.strategy.submit_orders
    calls = []
    def observed(orders, **kwargs):
        calls.append((orders, kwargs))
        return original(orders, **kwargs)
    monkeypatch.setattr(native_batch.strategy, "submit_orders", observed)
    result = batch_tool(native_batch)(orders_json=json.dumps(requested), reason=REASON)
    assert result["ok"] is True
    assert calls[0][1]["is_multileg"] is False
    assert [order.asset.strike for order in calls[0][0]] == [602, 604]
    assert all(order.asset.multiplier == 100 for order in calls[0][0])
    assert [row["asset"]["strike"] for row in result["returned_orders"]] == [602, 604]


def test_batch_is_a_trading_mutation_and_part_of_normal_tools():
    definition = BuiltinTools.orders.submit_orders()
    assert definition.metadata["mutates_trading"] is True
    assert definition.name in {tool.name for tool in BuiltinTools.all()}


def test_batch_commits_each_explicit_reason_with_the_native_intent(native_batch):
    requested = rows(2)
    requested[1]["reason"] = REASON + " The second allocation is independently authorized."
    result = batch_tool(native_batch)(orders_json=json.dumps(requested), reason=REASON)
    with native_batch.strategy.memory._connect() as connection:
        stored = connection.execute(
            "SELECT text, metadata_json FROM memory_events WHERE event_type = 'order.intent'"
        ).fetchall()
    assert len(stored) == 1
    metadata = json.loads(stored[0]["metadata_json"])
    assert stored[0]["text"] == REASON
    assert [row["reason"] for row in metadata["evidence"]["orders"]] == [REASON, requested[1]["reason"]]
    assert [row["identifier"] for row in metadata["evidence"]["orders"]] == [row["identifier"] for row in result["orders"]]
