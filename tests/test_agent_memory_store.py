from datetime import datetime

import pandas as pd

from lumibot.components.memory import MemoryStore


class FakeStrategy:
    name = "Memory Strategy"

    def get_datetime(self):
        return datetime(2026, 5, 25, 12, 30)


def test_memory_store_uses_sqlite_retrievals_and_parquet_exports(tmp_path):
    store = MemoryStore(FakeStrategy(), root_dir=tmp_path)

    thesis = store.open_thesis("Own TQQQ while momentum holds.", symbol="TQQQ", tags=["momentum"])
    store.remember_decision("Bought TQQQ on trend strength.", symbol="TQQQ", action="buy")
    store.remember_lesson("Momentum exits need confirmation.", symbol="TQQQ", outcome={"validated": True})

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
    assert result["retrieval_id"] in set(retrievals["retrieval_id"])
    assert thesis["id"] in set(state["memory_id"])
