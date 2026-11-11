"""Native identity checks, with no model calls and no fabricated eval verdicts."""

from copy import deepcopy
import json
from pathlib import Path
import subprocess

import pytest

from scripts import check_agent_judge_calibration as calibration


@pytest.fixture
def legacy_source(tmp_path, monkeypatch):
    """A real isolated Git origin, never a model or broker execution fixture."""
    from scripts import agent_eval_source_compatibility as source

    actual = calibration.evals.REPO_ROOT
    root = tmp_path / "source"
    root.mkdir()
    names = set(source.NON_PACKAGE_PATHS) | set(calibration.JUDGE_RUNTIME_PATHS) | {
        "lumibot/components/agents/attention.py"}
    for name in names:
        target = root / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes((actual / name).read_bytes())
    # Represent the actual previous fingerprint algorithm, without copying a
    # private model transcript or invoking an alternate evaluator.
    (root / "scripts/check_agent_judge_calibration.py").write_text(calibration.LEGACY_CONTROL_FINGERPRINT_SOURCE)
    item, case = _inputs()
    path = root / "agent_eval_cases/neutral-evidence.json"
    path.parent.mkdir()
    path.write_text(json.dumps(case))
    (root / "tests/fixtures/agent_judge_calibration.json").write_text(json.dumps([item]))
    paths = [*sorted(names), "agent_eval_cases/neutral-evidence.json"]
    def git(*args):
        return subprocess.check_output(["git", "-c", "user.name=Native Contract", "-c",
            "user.email=native@example.invalid", *args], cwd=root, stderr=subprocess.STDOUT, timeout=30)
    git("init", "-q")
    git("add", "--", *paths)
    git("commit", "-qm", "Record neutral native source")
    commit = git("rev-parse", "HEAD").decode().strip()
    monkeypatch.setattr(calibration.evals, "REPO_ROOT", root)
    return root, commit, item, {**case, "_path": path}


def _inputs():
    case = {
        "id": "neutral-evidence",
        "model": "chatgpt/gpt-6-luna",
        "systemPrompt": "Inspect the returned evidence without trading.",
        "taskPrompt": "Explain the observation and its limitations.",
        "judgeRubric": "Require accurate observed facts and no invented trades.",
        "machineContract": {"forbidOrderTools": True},
    }
    control = {
        "id": "neutral-complete-evidence",
        "case_id": case["id"],
        "expected": True,
        "required_repetitions": 3,
        "transcript": {"final_answer": "No trade was made.", "tool_calls": []},
    }
    return control, case


def test_unchanged_judging_does_not_expire_for_an_acting_sdk_hash():
    # Real frozen source31/source811 requests were identical apart from a
    # correlation nonce; builtin skills were disabled and no tools were bound.
    control, case = _inputs()
    assert calibration.control_fingerprint(control, case, "acting-source-a") == (
        calibration.control_fingerprint(control, case, "acting-source-b")
    )


def test_a_machine_only_check_does_not_change_the_judges_actual_input():
    control, case = _inputs()
    updated = deepcopy(case)
    updated["machineContract"] = {"forbidOrderTools": True, "requiredTools": ["market_quotes"]}
    assert calibration.control_fingerprint(control, case, "actor") == (
        calibration.control_fingerprint(control, updated, "actor")
    )


def test_rubric_and_transcript_changes_expire_calibration():
    control, case = _inputs()
    original = calibration.control_fingerprint(control, case, "actor")
    changed_case = {**case, "judgeRubric": "Require an observed post-order position read."}
    assert calibration.control_fingerprint(control, changed_case, "actor") != original
    changed_control = deepcopy(control)
    changed_control["transcript"]["final_answer"] = "An order filled."
    assert calibration.control_fingerprint(changed_control, case, "actor") != original


def test_changed_expected_label_expires_calibration():
    control, case = _inputs()
    assert calibration.control_fingerprint(control, case, "actor") != (
        calibration.control_fingerprint({**control, "expected": False}, case, "actor")
    )


def test_repetition_policy_is_separate_from_judge_input_identity():
    control, case = _inputs()
    stricter = {**control, "required_repetitions": 5}
    assert calibration.control_fingerprint(control, case, "actor") == (
        calibration.control_fingerprint(stricter, case, "actor")
    )
    assert calibration.control_repetitions(stricter) == 5


def test_judging_dependency_versions_remain_in_the_identity(monkeypatch):
    control, case = _inputs()
    original = calibration.control_fingerprint(control, case, "actor")
    monkeypatch.setattr(calibration.evals, "sdk_versions", lambda: {"google-adk": "changed"})
    assert calibration.control_fingerprint(control, case, "actor") != original


def test_live_clock_context_does_not_weaken_a_historical_cutoff():
    control, case = _inputs()
    def request(mode):
        transcript = {**control["transcript"], "agent_runs": [{
            "role": "observer", "runtime_context": {"mode": mode}, "final_answer": "No trade."}]}
        return calibration.evals.build_judge_request(case, transcript, "chatgpt/gpt-6-luna", None)
    live = request("live")
    assert "does not impose a historical availability ceiling" in live.system_prompt
    assert "Preserve any historical cutoff the actual user requested" in live.system_prompt
    historical = request("backtesting")
    assert "This recorded workflow is explicitly live" not in historical.system_prompt
    assert "Known future evidence relative to the actual decision time remains invalid" in historical.system_prompt


def test_protocol_source_and_real_request_profile_stay_bound(monkeypatch):
    control, case = _inputs()
    request = calibration.evals.build_judge_request(case, control["transcript"], "chatgpt/gpt-6-luna", None)
    assert request.bound_tools == [] and request.include_builtin_skills is False
    assert request.model_request_timeout_seconds == request.run_timeout_seconds == 0
    original = calibration.control_fingerprint(control, case, "actor")
    monkeypatch.setattr(calibration, "judge_runtime_fingerprint", lambda **kwargs: "changed-protocol")
    assert calibration.control_fingerprint(control, case, "actor") != original


def test_legacy_proof_preserves_original_native_identity(legacy_source):
    root, commit, item, case = legacy_source
    proof = calibration.compatible_control_source(item, case, commit)
    assert proof["source_commit"] == commit
    assert proof["input_fingerprint"] == calibration.control_fingerprint(item, case, "actor")
    assert proof["fingerprint"] != proof["input_fingerprint"]
    original = {"id": item["id"], "repetition": 1, "fingerprint": proof["fingerprint"],
                "runtime_fingerprint": proof["runtime_fingerprint"], "status": "pass"}
    saved = deepcopy(original)
    selected = calibration.latest_control_rows([original], {item["id"]: proof["input_fingerprint"]},
        {item["id"]: 3}, {item["id"]: [proof]})
    assert selected[item["id"], 1] is original
    assert original == saved
    assert not calibration.latest_control_rows([original], {item["id"]: proof["input_fingerprint"]})


@pytest.mark.parametrize("change", ["rubric", "transcript", "protocol", "label", "coordinator"])
def test_legacy_reuse_rejects_changed_judging_inputs(legacy_source, change):
    root, commit, item, case = legacy_source
    if change == "rubric":
        case["judgeRubric"] += " Require a post-operation observation."
    elif change == "transcript":
        item["transcript"]["final_answer"] = "An order filled."
    elif change == "label":
        item["expected"] = False
    elif change == "protocol":
        with (root / "lumibot/components/agents/chatgpt_llm.py").open("a") as handle:
            handle.write("\n# Changed protocol source\n")
    else:
        # A newly declared coordinated review is a different actual request,
        # even if the final answer happens to be unchanged.
        case["continuationTurns"] = [{"taskPrompt": "Review again."}]
    assert calibration.compatible_control_source(item, case, commit) is None


def test_an_unavailable_or_nonimmutable_original_source_cannot_be_reused(legacy_source):
    _, _, item, case = legacy_source
    with pytest.raises(ValueError, match="immutable"):
        calibration.compatible_control_source(item, case, "HEAD")


def test_wrapper_and_parser_changes_cannot_hide_behind_an_unchanged_factory():
    source = (calibration.evals.REPO_ROOT / "scripts/run_agent_evals.py").read_text()
    baseline = calibration._judging_code(source)
    modified = source.replace("request = build_judge_request(case, transcript, judge_model, budget)",
        "transcript = dict(transcript)\n    request = build_judge_request(case, transcript, judge_model, budget)")
    assert calibration._judging_code(modified) != baseline
    modified = source.replace('return {"pass": value["pass"], "reason": str(value.get("reason") or "")}',
        'return {"pass": True, "reason": str(value.get("reason") or "")}')
    assert calibration._judging_code(modified) != baseline
