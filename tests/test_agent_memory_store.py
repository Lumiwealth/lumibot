from datetime import datetime

import pandas as pd

from lumibot.components.memory import MemoryStore


class FakeStrategy:
    name = "Memory Strategy"

    def get_datetime(self):
        return datetime(2026, 5, 25, 12, 30)


class FakeAsset:
    symbol = "TQQQ"
    asset_type = "stock"


class FakePosition:
    asset = FakeAsset()
    quantity = 12


class FakeStrategyWithPosition(FakeStrategy):
    def get_positions(self, include_cash_positions=False):
        return [FakePosition()]

    def get_last_price(self, asset):
        return 50.0


def test_memory_store_uses_sqlite_retrievals_and_parquet_exports(tmp_path):
    store = MemoryStore(FakeStrategy(), root_dir=tmp_path)

    thesis = store.open_thesis("Own TQQQ while momentum holds.", symbol="TQQQ", tags=["momentum"])
    store.remember_decision(
        "Bought TQQQ on trend strength.",
        symbol="TQQQ",
        action="buy",
        agent_name="trader",
        model_call_id="call-trader-1",
    )
    store.remember_proposal(
        "Researcher proposed TQQQ if momentum continues.",
        symbol="TQQQ",
        action="buy",
        agent_name="researcher",
        model_call_id="call-researcher-1",
    )
    store.remember_risk_note(
        "Bear case: leveraged decay if volatility stays elevated.",
        symbol="TQQQ",
        agent_name="bear",
        model_call_id="call-bear-1",
    )
    store.remember_lesson("Momentum exits need confirmation.", symbol="TQQQ", outcome={"validated": True})
    order_event = store.record_order_submitted(
        symbol="TQQQ",
        side="buy",
        quantity=10,
        order_type="market",
        asset_type="stock",
        agent_name="trader",
        model_call_id="call-trader-1",
        order_payload={"identifier": "order-1", "status": "submitted"},
    )

    result = store.search("momentum", symbol="TQQQ", limit=5)

    assert store.db_path.exists()
    assert result["retrieval_id"].startswith("retrieval_")
    assert any(item["id"] == thesis["id"] for item in result["results"])

    compact = store.compact_state(symbols=["TQQQ"])
    assert compact["held_symbols"] == ["TQQQ"]
    assert compact["current_position_rationales"][0]["symbol"] == "TQQQ"
    assert compact["validated_lessons"][0]["status"] == "validated"

    warning = store.record_warning("Agent skipped thesis retrieval.", symbol="TQQQ")
    assert warning["event_type"] == "agent_warning"

    exports = store.export_artifacts(tmp_path, prefix="sample_agent_memory")
    assert set(exports) == {"memory_events", "memory_retrievals", "memory_state"}
    events = pd.read_parquet(exports["memory_events"])
    retrievals = pd.read_parquet(exports["memory_retrievals"])
    state = pd.read_parquet(exports["memory_state"])

    assert "thesis.opened" in set(events["event_type"])
    assert "proposal.recorded" in set(events["event_type"])
    assert "risk_note.recorded" in set(events["event_type"])
    assert "order.submitted" in set(events["event_type"])
    assert order_event["agent_name"] == "trader"
    assert "call-trader-1" in set(events["model_call_id"])
    assert "researcher" in set(events["agent_name"])
    assert result["retrieval_id"] in set(retrievals["retrieval_id"])
    assert thesis["id"] in set(state["memory_id"])


def test_compact_state_records_open_thesis_outcome_once_per_day(tmp_path):
    store = MemoryStore(FakeStrategyWithPosition(), root_dir=tmp_path)
    thesis = store.open_thesis("Own TQQQ while momentum holds.", symbol="TQQQ")

    store.compact_state(symbols=["TQQQ"])
    store.compact_state(symbols=["TQQQ"])

    exports = store.export_artifacts(tmp_path, prefix="outcome_agent_memory")
    events = pd.read_parquet(exports["memory_events"])
    outcome_events = events[events["event_type"] == "thesis.outcome_observed"]

    assert len(outcome_events) == 1
    assert outcome_events.iloc[0]["subject_id"] == thesis["id"]
