"""The release eval check verifies the author-written scope, never the whole catalog.

The release author lists the cases a change can affect, each with a reason, or states
why none can be affected. The check verifies that list and the pass evidence of every
listed case. It never infers, adds or refreshes cases (Rob, 2026-11-07).
Synthetic metadata here is never an agent eval.
"""

import json
import subprocess
import sys
from copy import deepcopy
from uuid import uuid4

import pytest

from scripts import qualify_agent_evals as qualification
from tests.test_agent_eval_qualification import (  # noqa: F401  (fixtures)
    maintained_contract_controls,
    make_receipt,
    native_metadata,
)


def _scope(cases, **extra):
    return {"version": 1, "package_version": "9.9.9", "cases": cases, **extra}


def _catalog(native_metadata):
    return native_metadata[2]


def test_empty_scope_with_a_reason_passes_without_any_receipt(native_metadata):
    scope = _scope([], no_cases_reason="Speed and logging changes only; no agent prompt, skill, tool or runtime code.")
    assert qualification.verify_release_scope(
        scope, _catalog(native_metadata), None, runtime_hash="source", version="9.9.9") == []


def test_empty_scope_without_a_reason_fails(native_metadata):
    with pytest.raises(ValueError, match="no_cases_reason"):
        qualification.verify_release_scope(
            _scope([]), _catalog(native_metadata), None, runtime_hash="source", version="9.9.9")


def test_scope_written_for_another_version_fails(native_metadata):
    scope = _scope([], no_cases_reason="none")
    with pytest.raises(ValueError, match="written for"):
        qualification.verify_release_scope(
            scope, _catalog(native_metadata), None, runtime_hash="source", version="9.9.10")


def test_listed_case_without_pass_evidence_fails(native_metadata):
    scope = _scope([{"id": "contract", "reason": "The change edits the tool this case exercises."}])
    with pytest.raises(ValueError, match="no pass evidence"):
        qualification.verify_release_scope(
            scope, _catalog(native_metadata), None, runtime_hash="source", version="9.9.9")
    with pytest.raises(ValueError, match="no pass evidence"):
        qualification.verify_release_scope(
            scope, _catalog(native_metadata), {"cases": {}}, runtime_hash="source", version="9.9.9")


def test_listed_case_with_current_pass_evidence_passes(native_metadata):
    receipt = make_receipt(deepcopy(native_metadata))
    scope = _scope([{"id": "contract", "reason": "The change edits the tool this case exercises."}])
    assert qualification.verify_release_scope(
        scope, _catalog(native_metadata), receipt, runtime_hash="source", version="9.9.9") == ["contract"]


def test_listed_case_with_evidence_from_other_source_fails(native_metadata):
    receipt = make_receipt(deepcopy(native_metadata))
    scope = _scope([{"id": "contract", "reason": "The change edits the tool this case exercises."}])
    with pytest.raises(ValueError, match="fingerprint"):
        qualification.verify_release_scope(
            scope, _catalog(native_metadata), receipt, runtime_hash="changed-source", version="9.9.9")


def test_listed_case_needs_a_reason_and_a_real_id(native_metadata):
    for entry in ({"id": "contract", "reason": ""}, {"id": "contract"}, {"id": "missing", "reason": "x"}):
        with pytest.raises(ValueError):
            qualification.verify_release_scope(
                _scope([entry]), _catalog(native_metadata), None, runtime_hash="source", version="9.9.9")


def test_the_committed_scope_verifies_from_the_command_line():
    """The command the release workflow runs uses the committed scope when it exists."""
    if not qualification.SCOPE_PATH.exists():
        pytest.skip("no release scope committed")
    result = subprocess.run([sys.executable, "scripts/qualify_agent_evals.py", "verify"],
                            cwd=qualification.evals.REPO_ROOT, capture_output=True, text=True, timeout=120)
    assert result.returncode == 0, result.stderr
    assert "author-written release eval scope" in result.stdout
    json.loads(qualification.SCOPE_PATH.read_text())


def test_scoped_create_needs_only_the_listed_cases_and_their_own_controls(native_metadata, monkeypatch, tmp_path):
    """With a release scope, the receipt covers the listed cases and their own calibration controls.

    Without this, creating the receipt demanded every catalog case's calibration controls (36
    judge runs) for a release that changed four cases: the whole-catalog gate the scope
    replaced (Rob, 2026-11-07).
    """
    from tests.test_agent_eval_qualification import calibration_metadata

    rows, calls, cases, versions, now = deepcopy(native_metadata)
    other = {"id": "other", "model": qualification.evals.DEFAULT_ACTING_MODEL, "judgeRubric": "Unrelated case"}
    controls = json.loads(qualification.CALIBRATION_CATALOG_PATH.read_text())
    own_path = tmp_path / "own-controls.json"
    own_path.write_text(json.dumps(controls))
    full = controls + [{"id": "other-accept", "case_id": "other", "expected": True, "required_repetitions": 3,
                        "transcript": {"private": "x"}},
                       {"id": "other-reject", "case_id": "other", "expected": False, "required_repetitions": 3,
                        "transcript": {"private": "x"}}]
    cal_rows, cal_calls, summary = calibration_metadata(cases, now)  # controls of "contract" only
    full_path = tmp_path / "full-controls.json"
    full_path.write_text(json.dumps(full))
    monkeypatch.setattr(qualification, "CALIBRATION_CATALOG_PATH", full_path)
    catalog = [*cases, other]
    receipt = qualification.build_receipt(rows, [*calls, *cal_calls], catalog, runtime_hash="source",
                                          versions=versions, now=now, calibration_rows=cal_rows,
                                          calibration_summary=summary, scope_case_ids=["contract"])
    assert set(receipt["cases"]) == {"contract"}
    assert set(receipt["calibration"]["controls"]) == {c["id"] for c in controls}
    assert receipt["scope"] == {"case_ids": ["contract"]}
    scope = _scope([{"id": "contract", "reason": "The change edits the tool this case exercises."}])
    assert qualification.verify_release_scope(scope, catalog, receipt, runtime_hash="source", version="9.9.9", versions=versions) == ["contract"]
    # Without a scope the whole catalog is still required.
    with pytest.raises(ValueError):
        qualification.build_receipt(rows, [*calls, *cal_calls], catalog, runtime_hash="source", versions=versions,
                                    now=now, calibration_rows=cal_rows, calibration_summary=summary)


def test_scoped_verification_preserves_verified_establishment(native_metadata, monkeypatch):
    from tests.test_agent_eval_qualification import calibration_metadata

    rows, calls, cases, versions, now = deepcopy(native_metadata)
    monkeypatch.setattr(qualification, "establishment_runtime", lambda ref, cases, versions: "source")
    monkeypatch.setattr(qualification, "_unchanged_establishment_cases", lambda ref, cases: cases)
    current = deepcopy(rows[:1])
    current[0]["run_id"] = str(uuid4())
    current[0]["fingerprint"] = qualification.evals.case_fingerprint(
        cases[0], judge_model=qualification.evals.DEFAULT_JUDGE_MODEL, runtime_hash="new-source")
    current_calls = deepcopy(calls[1:5])
    identities = {}
    for call in current_calls:
        identities.setdefault(call["id"], str(uuid4()))
        call["id"] = identities[call["id"]]
        if "scope" in call:
            call["scope"].update(run_id=current[0]["run_id"], fingerprint=current[0]["fingerprint"])
    cal_rows, cal_calls, summary = calibration_metadata(cases, now, "new-source")
    receipt = qualification.build_receipt(
        [*rows, *current], [*calls, *current_calls, *cal_calls], cases,
        runtime_hash="new-source", versions=versions, now=now, calibration_rows=cal_rows,
        calibration_summary=summary, established_source="a" * 40, scope_case_ids=["contract"])
    scope = _scope([{"id": "contract", "reason": "Current regression retains original stability proof."}])
    args = dict(runtime_hash="new-source", version="9.9.9", versions=versions, now=now)
    assert qualification.verify_release_scope(scope, cases, receipt, **args) == ["contract"]
    for section in ("establishment", "cases"):
        damaged = deepcopy(receipt)
        records = damaged[section]["cases"] if section == "establishment" else damaged[section]
        records["contract"]["repetitions"].pop()
        with pytest.raises(ValueError, match="required unique repetitions"):
            qualification.verify_release_scope(scope, cases, damaged, **args)
    with pytest.raises(ValueError, match="SDKs"):
        qualification.verify_release_scope(scope, cases, receipt, **{**args, "versions": {"changed": "sdk"}})

def test_scoped_verifier_reuses_the_existing_source_compatibility_check(monkeypatch, tmp_path):
    from scripts import agent_eval_source_compatibility as source

    scope_path = tmp_path / "ci/agent-evals/release-scope.json"
    scope_path.parent.mkdir(parents=True)
    scope_path.write_text(json.dumps(_scope([{"id": "contract", "reason": "unchanged stock case"}])))
    receipt_path = scope_path.with_name("qualification.json")
    receipt_path.write_text(json.dumps({"runtime_fingerprint": "original"}))
    scope_path.with_name("source-compatibility.json").write_text(json.dumps({"source_commit": "a" * 40}))
    monkeypatch.setattr(qualification, "SCOPE_PATH", scope_path)
    monkeypatch.setattr(qualification.evals, "REPO_ROOT", tmp_path)
    monkeypatch.setattr(qualification.evals, "load_cases", lambda: [])
    monkeypatch.setattr(qualification.evals, "runtime_fingerprint", lambda: "candidate")
    monkeypatch.setattr(qualification.evals, "sdk_versions", lambda: {})
    monkeypatch.setattr(qualification, "package_version", lambda: "9.9.9")
    observed = []
    def compatibility(receipt, raw, anchor):
        assert raw == receipt_path.read_bytes()
        observed.append(anchor["source_commit"])
        return "original"
    monkeypatch.setattr(source, "compatible_runtime", compatibility)
    def scoped(scope, cases, receipt, *, runtime_hash, version, versions=None):
        assert runtime_hash == "original"
        return ["contract"]
    monkeypatch.setattr(qualification, "verify_release_scope", scoped)
    monkeypatch.setattr(sys, "argv", ["qualify_agent_evals.py", "verify", "--receipt", str(receipt_path)])
    assert qualification.main() == 0
    assert observed == ["a" * 40]
