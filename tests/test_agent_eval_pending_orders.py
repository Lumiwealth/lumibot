"""Exact broker-state contracts distinguish valid pending orders from fills."""
import copy

from scripts.run_agent_evals import score_machine_contract


def test_pending_order_contract_requires_zero_fills_and_preserves_fill_default():
    # Shape recorded from the native actor trial; no model trajectory is simulated.
    transcript = {
        "tool_calls": [],
        "fixture_calls": [{"name": "orders_submit_order"}],
        "submissions": [{"tool": "orders_submit_order"}],
        "final_positions": [],
        "broker_orders": [{"identifier": "bt_1", "status": "new"}],
        "execution_outcome": {"decision_completed": True},
    }
    case = {"machineContract": {"orderTool": "orders_submit_order", "exactOrderCount": 1,
                                "expectedFilledOrderCount": 0}}
    assert score_machine_contract(case, transcript)["pass"]
    filled = copy.deepcopy(transcript)
    filled["broker_orders"][0]["status"] = "fill"
    assert not score_machine_contract(case, filled)["pass"]
    del case["machineContract"]["expectedFilledOrderCount"]
    assert not score_machine_contract(case, transcript)["pass"]
    assert score_machine_contract(case, filled)["pass"]
    for invalid in (True, -1, "0"):
        case["machineContract"]["expectedFilledOrderCount"] = invalid
        assert not score_machine_contract(case, transcript)["pass"]


def test_parallel_native_fixtures_have_disjoint_agent_export_paths(monkeypatch, tmp_path):
    from scripts.agent_eval_production_fixture import ProductionFixture
    from scripts.run_agent_evals import build_fixture

    monkeypatch.setenv("LUMIBOT_CACHE_FOLDER", str(tmp_path / "shared_cache"))
    first = ProductionFixture(build_fixture("flat_stock_account"))
    second = ProductionFixture(build_fixture("flat_stock_account"))
    try:
        left = first.manager._artifact_path("trader", "_agent_detail.parquet")
        right = second.manager._artifact_path("trader", "_agent_detail.parquet")
        assert left != right
        assert left.is_relative_to(first.root)
        assert right.is_relative_to(second.root)
    finally:
        first.close()
        second.close()
