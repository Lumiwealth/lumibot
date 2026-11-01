"""Real isolated broker and SQLite fixture state, without any model inference."""
import pytest
import json
import subprocess

from scripts.run_agent_evals import build_fixture
from scripts.run_agent_evals import score_machine_contract
from scripts.agent_eval_production_fixture import ProductionFixture


@pytest.mark.parametrize("name,quantity", [
    ("single_long_option_account", 2), ("single_short_option_account", -2),
])
def test_single_option_fixture_preserves_signed_position_and_exact_contract(name, quantity):
    fixture = ProductionFixture(build_fixture(name))
    try:
        held = [p for p in fixture.strategy.get_positions() if p.asset.symbol != "USD"]
        assert len(held) == 1
        assert held[0].quantity == quantity
        assert held[0].asset.symbol == "SPY"
        assert held[0].asset.strike == 594
        assert str(held[0].asset.right).lower() == "put"
        assert fixture.strategy.get_cash() == 100_000
        assert fixture.strategy.get_orders() == []
    finally:
        fixture.close()


@pytest.mark.parametrize("quantity,expected", [(1, False), (2, True), (3, False)])
def test_single_leg_contract_checks_declared_quantity_without_assuming_one(quantity, expected):
    # Submission fields follow the two recorded native closing pilots.
    order = {"tool": "orders_submit_order", "asset_type": "option", "quantity": quantity,
             "expiration": "2026-08-28", "strike": 594, "right": "put",
             "order_type": "limit", "limit_price": 0.95, "side": "sell_to_close"}
    case = {"machineContract": {"orderTool": "orders_submit_order", "exactOrderCount": 1,
                               "singleOptionOrder": True, "singleOptionQuantity": 2}}
    transcript = {"fixture_calls": [], "submissions": [order], "final_positions": []}
    assert score_machine_contract(case, transcript)["pass"] is expected


def snapshot(root, files):
    for name, content in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    def git(*args):
        return subprocess.check_output(["git", "-C", str(root), *args], stderr=subprocess.STDOUT).decode().strip()
    git("init", "-q")
    git("add", *files)
    git("-c", "user.name=Native fixture", "-c", "user.email=fixture@botspot.test",
        "commit", "-qm", "Original recorded source")
    return git("rev-parse", "HEAD")


@pytest.mark.parametrize("change,accepted", [("quantity", True), ("request", False),
                                             ("fixture", False), ("judge", False), ("order_count", False)])
def test_machine_recovery_preserves_all_actor_and_judge_inputs(tmp_path, change, accepted):
    from scripts.rejudge_agent_eval import compare_case
    root = tmp_path / "source"
    original = {"id": "neutral", "model": "chatgpt/gpt-6-luna", "fixture": "single_long_option_account",
                "taskPrompt": "Close the held exposure.", "systemPrompt": "Respect the user mandate.",
                "judgeRubric": "Require actual execution.",
                "machineContract": {"singleOptionOrder": True, "exactOrderCount": 1}}
    commit = snapshot(root, {"agent_eval_cases/neutral.json": json.dumps(original)})
    current = {**original, "machineContract": {**original["machineContract"], "singleOptionQuantity": 2}}
    if change in {"request", "fixture", "judge"}:
        current[{"request": "taskPrompt", "fixture": "fixture", "judge": "judgeRubric"}[change]] = "changed"
    if change == "order_count": current["machineContract"]["exactOrderCount"] = 2
    path = root / "agent_eval_cases/neutral.json"
    path.write_text(json.dumps(current))
    case = {**current, "_path": path}
    if accepted:
        prior, proof = compare_case(root, commit, case, machine_only=True)
        assert prior == original
        assert proof["changedFields"] == ["machineContract"]
    else:
        with pytest.raises(ValueError): compare_case(root, commit, case, machine_only=True)


@pytest.mark.parametrize("change,accepted", [("score", True), ("actor", False), ("judge", False), ("library", False)])
def test_machine_recovery_source_allows_only_the_numeric_scorer(tmp_path, change, accepted):
    from scripts.rejudge_agent_eval import compare_source
    root = tmp_path / "source"
    source = "def score_machine_contract():\n    return 1\n\ndef execute_repetition():\n    return 2\n\ndef run_judge():\n    return 3\n"
    commit = snapshot(root, {"scripts/run_agent_evals.py": source, "lumibot/neutral.py": "VALUE = 1\n"})
    runner, library = root / "scripts/run_agent_evals.py", root / "lumibot/neutral.py"
    updated = source.replace("return 1", "return 10")
    if change == "actor": updated = updated.replace("return 2", "return 20")
    if change == "judge": updated = updated.replace("return 3", "return 30")
    if change == "library": library.write_text("VALUE = 2\n")
    runner.write_text(updated)
    if accepted:
        _, proof = compare_source(root, commit, [runner, library], machine_only=True)
        assert proof["changedPaths"] == ["scripts/run_agent_evals.py"]
    else:
        with pytest.raises(ValueError): compare_source(root, commit, [runner, library], machine_only=True)
