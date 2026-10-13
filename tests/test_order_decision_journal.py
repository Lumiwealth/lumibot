"""Pre-execution explanations must survive the actual order lifecycle."""

import hashlib
import json
import sqlite3
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from lumibot.components.agents.tool_context import agent_tool_context
from lumibot.components.memory import MemoryStore
from lumibot.entities import Asset, Order
from lumibot.strategies.strategy import Strategy


REASON = (
    "Increase the diversified equity allocation after confirming the current position, "
    "available cash and quote. Size this order below the strategy exposure limit. "
    "The thesis fails if trend strength reverses; leverage and a delayed fill remain risks."
)


def make_strategy(tmp_path):
    strategy = Strategy.__new__(Strategy)
    strategy._name = "journal-test"
    strategy.get_datetime = lambda: datetime(2026, 10, 9, tzinfo=timezone.utc)
    strategy._validate_order = lambda order: True
    strategy.memory = MemoryStore(strategy, root_dir=tmp_path)
    calls = []

    def broker_call(operation, orders):
        orders = orders if isinstance(orders, list) else [orders]
        with sqlite3.connect(strategy.memory.db_path) as conn:
            intents = conn.execute("SELECT COUNT(*) FROM memory_events WHERE event_type = 'order.intent'").fetchone()[0]
        calls.append((operation, intents, [getattr(o, "decision_journal", None) for o in orders]))
        return orders if len(orders) > 1 else orders[0]

    strategy.broker = SimpleNamespace(
        IS_BACKTESTING_BROKER=False,
        name="test",
        submit_order=lambda order: broker_call("submit", order),
        submit_orders=lambda orders, **kw: broker_call("submit", orders),
        modify_order=lambda order, **kw: broker_call("modify", order),
        cancel_order=lambda order: broker_call("cancel", order),
        cancel_orders=lambda orders: broker_call("cancel", orders),
    )
    return strategy, calls


def make_order():
    return Order("journal-test", Asset("SPY"), 1, "buy", identifier="test-order", status="new")


def test_intent_is_committed_before_broker_and_modifications_keep_history(tmp_path):
    strategy, calls = make_strategy(tmp_path)
    order = make_order()
    strategy.submit_order(order, reason=REASON, evidence={"quote_as_of": "2026-10-09T00:00:00Z"})
    first = dict(order.decision_journal)
    strategy.modify_order(order, limit_price=101, reason=REASON + " Update the price from a fresh quote.")
    second = dict(order.decision_journal)
    strategy.cancel_order(order, reason=REASON + " Cancel the remaining quantity because this quote expired.")

    assert [c[1] for c in calls] == [1, 2, 3]
    assert all(c[2][0]["action_id"] for c in calls)
    assert first["action_id"] != second["action_id"]
    assert second["previous_action_id"] == first["action_id"]
    assert second["decision_id"] == first["decision_id"]
    assert order.decision_journal["previous_action_id"] == second["action_id"]
    assert order.trade_decision_journal == second
    assert order.trade_decision_journal["operation"] == "modify"
    assert Order.from_dict(order.to_dict()).trade_decision_journal == second
    assert Order.from_dict(order.to_dict()).decision_journal == order.decision_journal
    with sqlite3.connect(strategy.memory.db_path) as conn:
        rows = conn.execute("SELECT text FROM memory_events WHERE event_type = 'order.intent' ORDER BY sequence").fetchall()
    assert rows[0][0] == REASON
    assert len(rows) == 3


@pytest.mark.parametrize("reason", [None, "too short", "x" * 100001], ids=["missing", "short", "oversize"])
def test_agent_cannot_execute_without_a_bounded_explanation(tmp_path, reason):
    strategy, calls = make_strategy(tmp_path)
    with agent_tool_context({"agent_name": "trader", "model_call_id": "call-1"}):
        with pytest.raises(ValueError, match="reason"):
            strategy.submit_order(make_order(), reason=reason)
    assert calls == []


def test_agent_direct_strategy_calls_have_provenance_and_multileg_one_decision(tmp_path):
    strategy, calls = make_strategy(tmp_path)
    orders = [make_order(), make_order()]
    with agent_tool_context({"agent_name": "trader", "model_call_id": "call-1"}):
        strategy.submit_order(orders, reason=REASON, is_multileg=False)
    assert calls[0][1] == 1
    assert orders[0].decision_journal["decision_id"] == orders[1].decision_journal["decision_id"]
    assert orders[0].decision_journal["model_call_id"] == "call-1"


def test_failed_durable_write_prevents_new_order_but_legacy_python_still_works(tmp_path, monkeypatch):
    strategy, calls = make_strategy(tmp_path)
    monkeypatch.setattr(strategy.memory, "record_order_action", lambda **kw: (_ for _ in ()).throw(OSError("disk full")))
    with pytest.raises(OSError, match="disk full"):
        strategy.submit_order(make_order(), reason=REASON)
    assert calls == []
    strategy.submit_order(make_order())
    assert len(calls) == 1


def test_unknown_broker_outcome_preserves_intent_and_does_not_retry(tmp_path):
    strategy, calls = make_strategy(tmp_path)
    def uncertain(order):
        calls.append("once")
        raise TimeoutError("acknowledgment unavailable")
    strategy.broker.submit_order = uncertain
    order = make_order()
    with pytest.raises(TimeoutError):
        strategy.submit_order(order, reason=REASON)
    assert calls == ["once"]
    assert order.decision_journal["action_id"]
    with sqlite3.connect(strategy.memory.db_path) as conn:
        rows = conn.execute("SELECT metadata_json FROM memory_events WHERE event_type = 'order.action_outcome'").fetchall()
    assert len(rows) == 1
    assert '"certainty": "unknown"' in rows[0][0]


def test_trade_event_exports_keep_compact_references_for_mixed_legacy_and_journal_orders(tmp_path):
    from tests.test_live_trade_event_log_bounded import _MockBroker, _MockDataSource
    strategy, _ = make_strategy(tmp_path)
    broker = _MockBroker(name="mock", connect_stream=False, data_source=_MockDataSource())
    broker._on_new_order = lambda *args, **kw: None
    broker._process_filled_order = lambda *args, **kw: None
    broker._process_trade_event(make_order(), broker.NEW_ORDER)
    order = make_order()
    strategy.submit_order(order, reason=REASON)
    order.status = "filled"
    broker._process_trade_event(order, broker.FILLED_ORDER, price=100, filled_quantity=1)
    events = broker._trade_event_log_df
    assert events.iloc[1]["decision.action_id"] == order.decision_journal["action_id"]
    assert events.iloc[1]["decision.reason_sha256"] == order.decision_journal["reason_sha256"]
    assert REASON not in events.to_json()


def test_builtin_mutations_require_a_reason_before_strategy_execution(tmp_path):
    from lumibot.components.agents.builtins import _bind_cancel_order, _bind_modify_order
    strategy, calls = make_strategy(tmp_path)
    order = make_order()
    strategy.get_order = lambda identifier: order
    for tool in [_bind_cancel_order(strategy, None), _bind_modify_order(strategy, None)]:
        with pytest.raises(TypeError):
            tool.function(identifier="test-order")
        with pytest.raises(ValueError, match="reason"):
            tool.function(identifier="test-order", reason="too short")
    assert calls == []


def test_batch_cancel_keeps_each_orders_original_decision(tmp_path):
    strategy, _ = make_strategy(tmp_path)
    first, second = make_order(), make_order()
    second.identifier = "second-order"
    strategy.submit_order(first, reason=REASON)
    strategy.submit_order(second, reason=REASON)
    originals = [dict(order.decision_journal) for order in (first, second)]
    strategy.cancel_orders([first, second], reason=REASON)
    for order, original in zip((first, second), originals):
        assert order.decision_journal["previous_action_id"] == original["action_id"]
        assert order.decision_journal["decision_id"] == original["decision_id"]
        assert order.to_minimal_dict()["decision_journal"] == order.decision_journal


def test_explicit_public_reason_survives_private_intent_order_serialization_and_lifecycle(tmp_path):
    strategy, calls = make_strategy(tmp_path)
    order = make_order()
    public_reason = "Buy $5,000 of SPY to increase diversified exposure; trend reversal remains a risk."
    evidence = {"public_reason": public_reason, "quote_as_of": "2026-10-09T00:00:00Z"}
    original_evidence = dict(evidence)
    strategy.submit_order(order, reason=REASON, evidence=evidence)
    first = dict(order.decision_journal)

    assert calls[0][1] == 1  # Durable intent exists before the actual broker call.
    assert first["public_reason_preview"] == public_reason
    assert first["public_reason_sha256"] == hashlib.sha256(public_reason.encode("utf-8")).hexdigest()
    assert first["reason_preview"] == REASON[:800]
    assert first["reason_sha256"] == hashlib.sha256(REASON.encode("utf-8")).hexdigest()
    assert evidence == original_evidence
    restored = Order.from_dict(order.to_dict())
    assert restored.decision_journal == first
    assert restored.trade_decision_journal == first
    with sqlite3.connect(strategy.memory.db_path) as conn:
        text, metadata = conn.execute(
            "SELECT text, metadata_json FROM memory_events WHERE event_type = 'order.intent'"
        ).fetchone()
    assert text == REASON
    assert json.loads(metadata)["reference"] == first
    assert json.loads(metadata)["evidence"] == original_evidence

    modified_reason = "Refresh the limit from a current quote; execution timing remains a risk."
    strategy.modify_order(order, limit_price=101, reason=REASON, evidence={"public_reason": modified_reason})
    modified = dict(order.decision_journal)
    assert modified["previous_action_id"] == first["action_id"]
    assert modified["decision_id"] == first["decision_id"]
    assert modified["public_reason_preview"] == modified_reason
    strategy.cancel_order(order, reason=REASON)
    assert order.decision_journal["previous_action_id"] == modified["action_id"]
    assert "public_reason_preview" not in order.decision_journal  # No carry-forward of an earlier action's consent.
    assert order.trade_decision_journal == modified
    assert Order.from_dict(order.to_dict()).trade_decision_journal == modified
    assert [call[0] for call in calls] == ["submit", "modify", "cancel"]


@pytest.mark.parametrize("evidence", [None, {}, {
    "public_reason_preview": "A caller cannot inject an already-projected public reason.",
    "public_reason_sha256": "0" * 64,
}])
def test_private_reason_is_never_a_public_fallback(tmp_path, evidence):
    strategy, calls = make_strategy(tmp_path)
    order = make_order()
    private_reason = REASON + " Contact private-owner@example.test; account balance is $90,000."
    strategy.submit_order(order, reason=private_reason, evidence=evidence)

    assert len(calls) == 1
    assert "public_reason_preview" not in order.decision_journal
    assert "public_reason_sha256" not in order.decision_journal
    assert order.decision_journal["reason_preview"] == private_reason[:800]
    with sqlite3.connect(strategy.memory.db_path) as conn:
        text = conn.execute("SELECT text FROM memory_events WHERE event_type = 'order.intent'").fetchone()[0]
    assert text == private_reason


@pytest.mark.parametrize("public_reason", [
    None, 42, {}, [], b"not a text value", "", "   ", " leading space", "trailing space ",
    "x" * 281, "é" * 141, "text\nnext line", "text\tprivate", "text\x00private", "text\x7fprivate",
    "text\x85private", "text\ud800private",
    "text\u202eprivate", "text\u200bprivate",
], ids=[
    "none", "number", "object", "array", "bytes", "empty", "whitespace", "leading", "trailing",
    "ascii-overflow", "unicode-byte-overflow", "newline", "tab", "nul", "del", "c1-control", "lone-surrogate",
    "bidi-format", "invisible-format",
])
def test_malformed_public_reason_omits_only_public_fields_and_preserves_private_trade(tmp_path, public_reason):
    strategy, calls = make_strategy(tmp_path)
    order = make_order()
    strategy.submit_order(order, reason=REASON, evidence={"public_reason": public_reason})

    assert len(calls) == 1
    assert order.decision_journal["reason_sha256"] == hashlib.sha256(REASON.encode("utf-8")).hexdigest()
    assert "public_reason_preview" not in order.decision_journal
    assert "public_reason_sha256" not in order.decision_journal
    with sqlite3.connect(strategy.memory.db_path) as conn:
        text = conn.execute("SELECT text FROM memory_events WHERE event_type = 'order.intent'").fetchone()[0]
    assert text == REASON


@pytest.mark.parametrize("public_reason", [
    "Email private-owner@example.test before buying SPY.",
    "SSN 123-45-6789 was checked.",
    "Account number: ABC-123456 belongs to this trade.",
    "Account: 123456789 funds this order.",
    "Bank account: 123456789 funds this order.",
    "Routing number 123456789 was confirmed.",
    "IBAN: GB82WEST12345698765432 was confirmed.",
    "API key: synthetic-secret-value was used.",
    "Authorization: Bearer synthetic-token-value was used.",
    "Use sk-fixture-secret-1234567890 for the order.",
    "The account cash is $20,000 after this trade.",
    "My balance is $20,000 after this trade.",
    "Available buying power is $20,000.",
    "I have $20,000 cash remaining.",
    "Cash: $20,000 remains available.",
    "Cash is $20,000 after this order.",
    "Balance: $20,000 after this order.",
    "I have $20,000 in cash after the order.",
    "Email ｐｒｉｖａｔｅ＠ｅｘａｍｐｌｅ．ｔｅｓｔ before trading.",
])
def test_private_identifiers_and_account_disclosures_are_omitted_from_public_fields_only(tmp_path, public_reason):
    strategy, calls = make_strategy(tmp_path)
    order = make_order()
    strategy.submit_order(order, reason=REASON, evidence={"public_reason": public_reason})

    assert len(calls) == 1
    assert "public_reason_preview" not in order.decision_journal
    assert "public_reason_sha256" not in order.decision_journal
    with sqlite3.connect(strategy.memory.db_path) as conn:
        text, metadata = conn.execute(
            "SELECT text, metadata_json FROM memory_events WHERE event_type = 'order.intent'"
        ).fetchone()
    assert text == REASON
    assert json.loads(metadata)["evidence"]["public_reason"] == public_reason  # Private audit evidence is unchanged.


@pytest.mark.parametrize("public_reason", [
    "Buy $5,000 of SPY to rebalance diversified exposure.",
    "The $5,000 trade reduces sector concentration; execution risk remains.",
    "é" * 140,  # Exactly 280 UTF-8 bytes, not 280 characters.
    "📈" * 70,
], ids=["trade-allocation", "trade-concentration", "utf8-latin-bound", "utf8-emoji-bound"])
def test_safe_public_trade_reason_uses_exact_utf8_bytes_without_truncation(tmp_path, public_reason):
    strategy, calls = make_strategy(tmp_path)
    order = make_order()
    strategy.submit_order(order, reason=REASON, evidence={"public_reason": public_reason})

    assert len(calls) == 1
    assert order.decision_journal["public_reason_preview"] == public_reason
    assert order.decision_journal["public_reason_sha256"] == hashlib.sha256(public_reason.encode("utf-8")).hexdigest()
