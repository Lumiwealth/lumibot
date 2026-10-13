import hashlib
import json
import sqlite3
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from threading import Barrier

import pandas as pd
import pytest

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


def _tool_action(**overrides):
    return {
        "action_id": "12345678-1234-4234-8234-123456789abc",
        "tool_name": "community",
        "purpose": "Read the private research evidence before preparing a public summary.",
        "status": "started",
        "arguments_sha256": hashlib.sha256(b'{"action":"feed"}').hexdigest(),
        "agent_name": "Research agent",
        "model_call_id": "call-trader-1",
        **overrides,
    }


def _events(store):
    with sqlite3.connect(store.db_path) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(row) for row in conn.execute("SELECT * FROM memory_events ORDER BY sequence")]


def test_agent_tool_action_commits_private_intent_before_return(tmp_path, monkeypatch):
    store = MemoryStore(FakeStrategy(), root_dir=tmp_path)
    monkeypatch.setattr(store, "_export_artifacts_best_effort", lambda: pytest.fail("per-tool export"))
    monkeypatch.setattr(store, "export_artifacts", lambda *args, **kwargs: pytest.fail("per-tool export"))
    fields = _tool_action()
    started = store.record_agent_tool_action(**fields)

    # This separate connection must see the intent before a caller executes its tool.
    rows = _events(store)
    assert len(rows) == 1
    assert rows[0]["event_type"] == "agent.tool_started"
    assert rows[0]["subject_id"] == fields["action_id"]
    assert rows[0]["sequence"] == started["sequence"] == 1
    assert rows[0]["text"] == fields["purpose"]
    metadata = json.loads(rows[0]["metadata_json"])
    assert metadata["arguments_sha256"] == fields["arguments_sha256"]
    assert metadata["purpose"] == fields["purpose"]
    assert not {"arguments", "public_reason", "public_reason_preview"} & metadata.keys()
    assert not {"arguments", "public_reason", "public_reason_preview"} & json.loads(rows[0]["payload_json"]).keys()

    completed = store.record_agent_tool_action(**{**fields, "status": "completed"})
    rows = _events(store)
    assert [row["event_type"] for row in rows] == ["agent.tool_started", "agent.tool_outcome"]
    assert [row["sequence"] for row in rows] == [1, 2]
    assert completed["sequence"] == 2
    assert json.loads(rows[1]["metadata_json"])["status"] == "completed"
    with sqlite3.connect(store.db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM memory_index").fetchone()[0] == 0


def test_agent_tool_action_failed_outcome_is_durable(tmp_path):
    store = MemoryStore(FakeStrategy(), root_dir=tmp_path)
    fields = _tool_action()
    store.record_agent_tool_action(**fields)
    store.record_agent_tool_action(**{**fields, "status": "failed", "error_type": "TimeoutError"})
    rows = _events(store)
    assert rows[1]["event_type"] == "agent.tool_outcome"
    metadata = json.loads(rows[1]["metadata_json"])
    assert metadata["status"] == "failed"
    assert metadata["error_type"] == "TimeoutError"


def test_agent_tool_action_can_record_a_decision_during_its_execution(tmp_path):
    store = MemoryStore(FakeStrategy(), root_dir=tmp_path)
    fields = _tool_action(tool_name="remember_decision")
    store.record_agent_tool_action(**fields)
    decision = store.remember_decision(
        "The research supports holding the position.",
        agent_name=fields["agent_name"],
        model_call_id=fields["model_call_id"],
    )
    store.record_agent_tool_action(**{**fields, "status": "completed"})
    rows = _events(store)
    assert [row["event_type"] for row in rows] == ["agent.tool_started", "decision.recorded", "agent.tool_outcome"]
    assert json.loads(rows[0]["metadata_json"])["decision_id"] is None
    assert json.loads(rows[2]["metadata_json"])["decision_id"] == decision["id"]


@pytest.mark.parametrize("status", ["completed", "failed"])
def test_agent_tool_action_outcome_requires_started_intent(tmp_path, status):
    store = MemoryStore(FakeStrategy(), root_dir=tmp_path)
    with pytest.raises(ValueError):
        store.record_agent_tool_action(**_tool_action(status=status))
    assert _events(store) == []


@pytest.mark.parametrize(
    "changes",
    [
        {"status": "started"},
        {"tool_name": "query_data"},
        {"purpose": "A different purpose."},
        {"arguments_sha256": "b" * 64},
        {"agent_name": "Other agent"},
        {"model_call_id": "call-trader-2"},
    ],
)
def test_agent_tool_action_rejects_duplicate_or_mismatched_pair(tmp_path, changes):
    store = MemoryStore(FakeStrategy(), root_dir=tmp_path)
    fields = _tool_action()
    store.record_agent_tool_action(**fields)
    with pytest.raises(ValueError):
        store.record_agent_tool_action(**{**fields, "status": "completed", **changes})
    assert len(_events(store)) == 1


def test_agent_tool_action_rejects_cross_run_and_duplicate_outcomes(tmp_path, monkeypatch):
    store = MemoryStore(FakeStrategy(), root_dir=tmp_path)
    fields = _tool_action()
    monkeypatch.delenv("BOTSPOT_ARTIFACT_RUN_ID", raising=False)
    monkeypatch.setenv("BOTSPOT_RUN_ID", "synthetic-run-one")
    store.record_agent_tool_action(**fields)
    monkeypatch.setenv("BOTSPOT_RUN_ID", "synthetic-run-two")
    with pytest.raises(ValueError):
        store.record_agent_tool_action(**{**fields, "status": "completed"})
    monkeypatch.setenv("BOTSPOT_RUN_ID", "synthetic-run-one")
    store.record_agent_tool_action(**{**fields, "status": "completed"})
    with pytest.raises(ValueError):
        store.record_agent_tool_action(**{**fields, "status": "failed"})
    assert len(_events(store)) == 2


@pytest.mark.parametrize(
    "changes",
    [
        {"action_id": "not-a-uuid"},
        {"action_id": uuid.UUID("12345678-1234-4234-8234-123456789abc")},
        {"action_id": "12345678-1234-4234-8234-123456789ABC"},
        {"arguments_sha256": "a" * 63},
        {"arguments_sha256": "G" * 64},
        {"tool_name": ""},
        {"tool_name": " community"},
        {"tool_name": "x" * 129},
        {"agent_name": ""},
        {"agent_name": "Research\x00agent"},
        {"model_call_id": None},
        {"model_call_id": "x" * 241},
        {"purpose": ""},
        {"purpose": "x" * 501},
        {"purpose": "é" * 251},
        {"purpose": "\ud800"},
        {"status": "success"},
        {"error_type": "TimeoutError"},
    ],
)
def test_agent_tool_action_rejects_invalid_private_audit_fields(tmp_path, changes):
    store = MemoryStore(FakeStrategy(), root_dir=tmp_path)
    with pytest.raises(ValueError):
        store.record_agent_tool_action(**_tool_action(**changes))
    assert _events(store) == []


@pytest.mark.parametrize("purpose", ["x" * 500, "é" * 250])
def test_agent_tool_action_accepts_exact_utf8_purpose_boundary(tmp_path, purpose):
    store = MemoryStore(FakeStrategy(), root_dir=tmp_path)
    store.record_agent_tool_action(**_tool_action(purpose=purpose))
    assert _events(store)[0]["text"] == purpose


@pytest.mark.parametrize("status", ["started", "completed", "failed"])
def test_agent_tool_action_failed_sqlite_write_never_reports_success(tmp_path, status):
    store = MemoryStore(FakeStrategy(), root_dir=tmp_path)
    if status != "started":
        store.record_agent_tool_action(**_tool_action())
    event_type = "agent.tool_started" if status == "started" else "agent.tool_outcome"
    with sqlite3.connect(store.db_path) as conn:
        conn.execute(f"""CREATE TRIGGER refuse_tool_intent BEFORE INSERT ON memory_events
                        WHEN NEW.event_type = '{event_type}'
                        BEGIN SELECT RAISE(ABORT, 'synthetic audit persistence failure'); END""")
    with pytest.raises(sqlite3.IntegrityError, match="synthetic audit persistence failure"):
        store.record_agent_tool_action(**_tool_action(status=status))
    assert len(_events(store)) == (0 if status == "started" else 1)


def test_agent_tool_action_outcome_pairing_serializes_two_sqlite_writers(tmp_path):
    stores = [MemoryStore(FakeStrategy(), root_dir=tmp_path) for _ in range(2)]
    fields = _tool_action()
    stores[0].record_agent_tool_action(**fields)
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [
            executor.submit(store.record_agent_tool_action, **{**fields, "status": "completed"}) for store in stores
        ]
        successes = []
        failures = []
        for future in futures:
            try:
                successes.append(future.result(timeout=10))
            except ValueError as exc:
                failures.append(exc)
    assert len(successes) == len(failures) == 1
    assert [row["sequence"] for row in _events(stores[0])] == [1, 2]


def test_memory_event_sequence_serializes_two_actual_sqlite_writers(tmp_path, monkeypatch):
    stores = [MemoryStore(FakeStrategy(), root_dir=tmp_path) for _ in range(2)]
    assert stores[0].db_path == stores[1].db_path
    read_barrier = Barrier(2)

    class SavedRow:
        def __init__(self, row):
            self.row = row

        def fetchone(self):
            return self.row

    class ContendingConnection(sqlite3.Connection):
        def execute(self, sql, parameters=()):
            cursor = super().execute(sql, parameters)
            if sql.startswith("SELECT COALESCE(MAX(sequence)"):
                row = cursor.fetchone()
                cursor.close()
                # Force overlapping unlocked reads. A real write transaction
                # instead serializes writers before this read can overlap.
                if not self.in_transaction:
                    read_barrier.wait(timeout=5)
                return SavedRow(row)
            return cursor

    def connect():
        conn = sqlite3.connect(stores[0].db_path, factory=ContendingConnection)
        conn.row_factory = sqlite3.Row
        return conn

    for store in stores:
        monkeypatch.setattr(store, "_connect", connect)
    monkeypatch.setenv("LUMIBOT_MEMORY_EXPORT_PARQUET", "0")
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = [executor.submit(store.record_warning, f"Writer {index}") for index, store in enumerate(stores)]
        events = [future.result(timeout=10) for future in futures]
    assert sorted(event["sequence"] for event in events) == [1, 2]
    rows = _events(stores[0])
    assert [row["sequence"] for row in rows] == [1, 2]
    assert {row["text"] for row in rows} == {"Writer 0", "Writer 1"}
