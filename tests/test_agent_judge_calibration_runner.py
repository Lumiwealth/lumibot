"""Normal contracts for calibration funding/resume; these never call a model."""

import importlib.util
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts/check_agent_judge_calibration.py"
SPEC = importlib.util.spec_from_file_location("judge_calibration_contracts", SCRIPT_PATH)
runner = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(runner)


def total_calls(budget):
    # The existing budget reports unsettled reservations separately from settlements.
    snapshot = budget.snapshot()
    return snapshot["reserved_calls"] + snapshot["settled_calls"]


@pytest.fixture
def calibration(monkeypatch, tmp_path):
    root = tmp_path / "source"
    (root / "tests/fixtures").mkdir(parents=True)
    controls = [{"id": "safe-control", "case_id": "research-case", "expected": True,
                 "transcript": {"evidence": "recorded synthetic control"}}]
    fixture_path = root / "tests/fixtures/agent_judge_calibration.json"
    fixture_path.write_text(json.dumps(controls))
    setup = []
    calls = []
    monkeypatch.setattr(runner.evals, "REPO_ROOT", root)
    monkeypatch.setattr(runner, "configure_fixture_environment", lambda root, **kwargs: setup.append(kwargs))
    monkeypatch.setattr(runner.evals, "select_eval_credentials", lambda models: None)
    monkeypatch.setattr(runner.evals, "preflight", lambda *args: None)
    monkeypatch.setattr(runner.evals, "runtime_fingerprint", lambda: "same-runtime")
    monkeypatch.setattr(runner.evals, "load_cases", lambda ids: [{"id": "research-case", "model": runner.evals.DEFAULT_JUDGE_MODEL}])

    def judge(case, transcript, model, budget):
        calls.append(dict(budget.scope))
        token = budget.reserve(model, 100)
        budget.settle(token, {"input_tokens": 10, "output_tokens": 2})
        return {"pass": True}, SimpleNamespace(usage={"input_tokens": 10, "output_tokens": 2}), 0.01

    monkeypatch.setattr(runner.evals, "run_judge", judge)
    ledger = tmp_path / "release-attempt/model_calls.jsonl"
    budget = runner.SubscriptionCallBudget(ledger, max_calls=500)
    output = tmp_path / "calibration"

    def invoke(*extra, include_ledger=True):
        argv = ["calibration", "--output-root", str(output)]
        if include_ledger:
            argv += ["--model-call-ledger", str(ledger)]
        monkeypatch.setattr(sys, "argv", argv + list(extra))
        return runner.main()

    return SimpleNamespace(root=root, output=output, ledger=ledger, budget=budget, calls=calls,
                           setup=setup, invoke=invoke, fixtures=controls, fixture_path=fixture_path)


def test_calibration_requires_existing_release_attempt_ledger_before_setup(calibration):
    with pytest.raises(SystemExit) as missing:
        calibration.invoke(include_ledger=False)
    assert missing.value.code == 2
    assert not calibration.calls
    assert not calibration.setup


def test_calibration_shares_default_500_call_cap_and_scopes_every_control(calibration):
    calibration.budget.reserve(runner.evals.DEFAULT_JUDGE_MODEL, 100, scope={"role": "actor"})
    assert calibration.invoke() == 0
    assert len(calibration.calls) == 3
    assert total_calls(calibration.budget) == 4
    assert not (calibration.output / "model_calls.jsonl").exists()
    scopes = calibration.calls
    assert {scope["repetition"] for scope in scopes} == {1, 2, 3}
    assert len({scope["run_id"] for scope in scopes}) == 1
    assert all(scope["fingerprint"] and scope["role"] == "judge_calibration" for scope in scopes)
    assert calibration.setup == [{"artifact_root": calibration.output.resolve()}]


def test_calibration_runs_every_explicit_high_risk_repetition(calibration):
    calibration.fixtures[0]["required_repetitions"] = 5
    calibration.fixture_path.write_text(json.dumps(calibration.fixtures))
    assert calibration.invoke() == 0
    assert len(calibration.calls) == 5
    assert {scope["repetition"] for scope in calibration.calls} == {1, 2, 3, 4, 5}
    assert calibration.invoke() == 0
    assert len(calibration.calls) == 5


def test_new_calibration_invalidates_an_export_before_the_first_call(calibration, monkeypatch):
    control = calibration.fixtures[0]
    case = runner.evals.load_cases({control["case_id"]})[0]
    fingerprint = runner.control_fingerprint(control, case, "same-runtime")
    receipt = calibration.root / "ci/agent-evals/qualification.json"
    receipt.parent.mkdir(parents=True)
    receipt.write_text(json.dumps({
        "runtime_fingerprint": "same-runtime", "cases": {},
        "calibration": {"controls": {control["id"]: {"fingerprint": fingerprint}}},
    }))
    original = runner.evals.run_judge

    def require_invalidation(*args):
        assert json.loads(receipt.read_text())["invalidated"]["calibration_control_ids"] == [control["id"]]
        return original(*args)

    monkeypatch.setattr(runner.evals, "run_judge", require_invalidation)
    assert calibration.invoke() == 0
    assert len(calibration.calls) == 3


def test_calibration_resumes_unchanged_passes_without_rewriting_native_timestamps(calibration):
    assert calibration.invoke() == 0
    original = (calibration.output / "ledger.jsonl").read_bytes()
    assert calibration.invoke() == 0
    assert len(calibration.calls) == 3
    assert (calibration.output / "ledger.jsonl").read_bytes() == original
    summary = json.loads((calibration.output / "summary.json").read_text())
    assert summary["resumed"] == 3
    assert summary["scheduled"] == 0
    assert summary["missing"] == 0


def test_calibration_recovers_newer_orphan_native_control_before_reusing_old_pass(calibration):
    import uuid
    assert calibration.invoke() == 0
    scope = next(row for row in calibration.calls if row["repetition"] == 2)
    calibration.budget.reserve(runner.evals.DEFAULT_JUDGE_MODEL, 100,
                               scope={**scope, "run_id": str(uuid.uuid4())})
    assert calibration.invoke() == 0
    assert len(calibration.calls) == 4
    assert calibration.calls[-1]["repetition"] == 2
    assert total_calls(calibration.budget) == 5


def test_calibration_does_not_reuse_expired_passes(calibration):
    assert calibration.invoke() == 0
    ledger = calibration.output / "ledger.jsonl"
    rows = runner.evals.read_jsonl(ledger)
    for row in rows:
        row["timestamp"] = runner.evals.utc_text(runner.evals.utc_now() - runner.evals.timedelta(days=91))
    ledger.write_text("".join(json.dumps(row) + "\n" for row in rows))
    assert calibration.invoke() == 0
    assert len(calibration.calls) == 6


@pytest.mark.parametrize("change", ["runtime", "control"])
def test_calibration_requalifies_changed_source_or_control_under_the_same_budget(calibration, monkeypatch, change):
    assert calibration.invoke() == 0
    original = (calibration.output / "ledger.jsonl").read_bytes()
    if change == "runtime":
        monkeypatch.setattr(runner.evals, "runtime_fingerprint", lambda: "changed-runtime")
    else:
        calibration.fixtures[0]["transcript"]["evidence"] = "different synthetic evidence"
        calibration.fixture_path.write_text(json.dumps(calibration.fixtures))
    assert calibration.invoke() == 0
    assert len(calibration.calls) == 6
    assert total_calls(calibration.budget) == 6
    assert (calibration.output / "ledger.jsonl").read_bytes().startswith(original)
    assert calibration.calls[0]["fingerprint"] != calibration.calls[-1]["fingerprint"]
    assert calibration.calls[0]["run_id"] != calibration.calls[-1]["run_id"]


def test_calibration_persists_errors_and_retries_only_the_failed_unique_control(calibration, monkeypatch):
    good_judge = runner.evals.run_judge

    def judge(case, transcript, model, budget):
        if budget.scope["repetition"] == 2:
            calibration.calls.append(dict(budget.scope))
            budget.reserve(model, 100)
            raise RuntimeError("private material must not be serialized")
        return good_judge(case, transcript, model, budget)

    monkeypatch.setattr(runner.evals, "run_judge", judge)
    assert calibration.invoke() == 1
    rows = runner.evals.read_jsonl(calibration.output / "ledger.jsonl")
    # Start rows are durable too; only terminal rows count as completed controls.
    assert len(rows) == 6
    assert sum(row["status"] == "error" for row in rows) == 1
    assert "private material" not in (calibration.output / "ledger.jsonl").read_text()
    original_passes = {row["repetition"]: row for row in rows if row["passed"]}
    monkeypatch.setattr(runner.evals, "run_judge", good_judge)
    assert calibration.invoke() == 0
    assert len(calibration.calls) == 4
    assert calibration.calls[-1]["repetition"] == 2
    assert total_calls(calibration.budget) == 4
    rows = runner.evals.read_jsonl(calibration.output / "ledger.jsonl")
    assert all(row in rows for row in original_passes.values())


@pytest.mark.parametrize("path_kind", ["missing", "empty"])
def test_calibration_does_not_initialize_a_missing_or_empty_release_ledger(calibration, path_kind):
    if path_kind == "missing":
        calibration.ledger.unlink()
    else:
        calibration.ledger.write_text("")
    with pytest.raises(SystemExit) as missing:
        calibration.invoke()
    assert missing.value.code == 2
    assert not calibration.ledger.exists() or not calibration.ledger.read_text()
    assert not calibration.calls
    assert not calibration.setup


def test_calibration_rejects_immutable_cap_change_before_auth(calibration, monkeypatch):
    monkeypatch.setattr(runner.evals, "select_eval_credentials", lambda models: pytest.fail("No auth before cap validation"))
    with pytest.raises(ValueError, match="contract changed"):
        calibration.invoke("--max-model-calls", "100")
    assert calibration.budget.snapshot()["max_calls"] == 500


def test_calibration_does_not_count_duplicate_control_repetitions_as_complete(calibration):
    assert calibration.invoke() == 0
    rows = runner.evals.read_jsonl(calibration.output / "ledger.jsonl")
    # Start rows now coexist with terminal rows; duplicate the completed pass.
    first = next(row for row in rows if row["repetition"] == 1 and row["status"] == "pass")
    (calibration.output / "ledger.jsonl").write_text((json.dumps(first) + "\n") * 3)
    assert calibration.invoke() == 0
    assert len(calibration.calls) == 5
    assert {scope["repetition"] for scope in calibration.calls[-2:]} == {2, 3}
    assert total_calls(calibration.budget) == 5


def test_calibration_negative_control_requires_the_expected_rejection(calibration, monkeypatch):
    calibration.fixtures[0]["expected"] = False
    calibration.fixture_path.write_text(json.dumps(calibration.fixtures))
    assert calibration.invoke() == 1
    summary = json.loads((calibration.output / "summary.json").read_text())
    assert summary["failed"] == 3
    original = (calibration.output / "ledger.jsonl").read_bytes()

    def reject(case, transcript, model, budget):
        calibration.calls.append(dict(budget.scope))
        token = budget.reserve(model, 100)
        budget.settle(token, {"input_tokens": 10, "output_tokens": 2})
        return {"pass": False}, SimpleNamespace(usage={"input_tokens": 10, "output_tokens": 2}), 0.01

    monkeypatch.setattr(runner.evals, "run_judge", reject)
    assert calibration.invoke() == 0
    assert len(calibration.calls) == 6
    assert (calibration.output / "ledger.jsonl").read_bytes().startswith(original)
    summary = json.loads((calibration.output / "summary.json").read_text())
    assert summary["passed"] == 3
    assert summary["failed"] == 0


def test_exhausted_shared_cap_cannot_be_reset_by_a_new_calibration_output(calibration, tmp_path):
    path = tmp_path / "bounded-release/model_calls.jsonl"
    bounded = runner.SubscriptionCallBudget(path, max_calls=1)
    bounded.reserve(runner.evals.DEFAULT_JUDGE_MODEL, 100, scope={"role": "actor"})
    assert calibration.invoke("--model-call-ledger", str(path), "--max-model-calls", "1") == 1
    assert total_calls(bounded) == 1
    summary = json.loads((calibration.output / "summary.json").read_text())
    assert summary["errors"] == 3
    assert summary["passed"] == 0
    terminal = [row for row in runner.evals.read_jsonl(calibration.output / "ledger.jsonl") if row["status"] == "error"]
    assert len(terminal) == 3
    assert all(row["error_code"] == "eval_call_limit_exceeded" for row in terminal)
    assert not (calibration.output / "model_calls.jsonl").exists()
