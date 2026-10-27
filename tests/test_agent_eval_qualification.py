"""Receipt integrity contracts. Synthetic metadata here is never an agent eval."""

import json
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from scripts import qualify_agent_evals as qualification
from scripts import run_agent_evals as evals


@pytest.fixture(autouse=True)
def maintained_contract_controls(monkeypatch, tmp_path):
    # Tiny labeled metadata stays in ordinary contracts, never the real eval catalog.
    controls = [{"id": "accept-control", "case_id": "contract", "expected": True,
                 "required_repetitions": 3, "transcript": {"private": "never exported calibration"}},
                {"id": "reject-control", "case_id": "contract", "expected": False,
                 "required_repetitions": 3, "transcript": {"private": "never exported calibration"}}]
    path = tmp_path / "control-catalog.json"
    path.write_text(json.dumps(controls))
    monkeypatch.setattr(qualification, "CALIBRATION_CATALOG_PATH", path)


def calibration_metadata(cases, now, runtime_hash="source"):
    controls = qualification.load_calibration_catalog(cases)
    case_map = {case["id"]: case for case in cases}
    rows, calls, attempt = [], [], str(uuid4())
    for control in controls:
        fingerprint = qualification.calibration.control_fingerprint(control, case_map[control["case_id"]], runtime_hash)
        for rep in range(1, control["required_repetitions"] + 1):
            started = now - timedelta(minutes=4 - rep / 10)
            row = {"id": control["id"], "case_id": control["case_id"], "repetition": rep,
                   "run_id": attempt, "fingerprint": fingerprint, "runtime_fingerprint": runtime_hash,
                   "started_at": started.isoformat(), "timestamp": (started + timedelta(seconds=20)).isoformat(),
                   "status": "pass", "passed": True, "expected": control["expected"],
                   "judge_model": evals.DEFAULT_JUDGE_MODEL, "judge": {"pass": control["expected"]},
                   "private": "never exported calibration"}
            rows.append(row)
            token = str(uuid4())
            calls.extend([
                {"kind": "reserve", "id": token, "model": evals.DEFAULT_JUDGE_MODEL, "nano_usd": 0,
                 "reserved_nano_usd": 0, "max_output_tokens": 100,
                 "timestamp": (started + timedelta(seconds=1)).isoformat(),
                 "scope": {"case_id": control["id"], "repetition": rep, "run_id": attempt,
                           "fingerprint": fingerprint, "role": "judge_calibration"}},
                {"kind": "settle", "id": token, "nano_usd": 0,
                 "settled_at": (started + timedelta(seconds=10)).isoformat(),
                 "usage": {"input_tokens": 12, "cached_input_tokens": 3, "output_tokens": 4, "thinking_tokens": 1}},
            ])
    summary = {"kind": "judge-calibration", "runtime_fingerprint": runtime_hash,
               "judge_model": evals.DEFAULT_JUDGE_MODEL, "passed": len(rows), "failed": 0,
               "errors": 0, "missing": 0, "actor_runs": 0, "external_writes": 0, "creates_case_freshness": False}
    return rows, calls, summary


def build_with_calibration(rows, calls, cases, *, runtime_hash, versions, now):
    calibration_rows, calibration_calls, summary = calibration_metadata(cases, now, runtime_hash)
    return qualification.build_receipt(rows, [*calls, *calibration_calls], cases, runtime_hash=runtime_hash,
                                       versions=versions, now=now, calibration_rows=calibration_rows,
                                       calibration_summary=summary)


@pytest.fixture
def native_metadata():
    now = datetime.now(timezone.utc)
    case = {"id": "contract", "model": evals.DEFAULT_ACTING_MODEL, "judgeRubric": "Contract fixture"}
    fingerprint = evals.case_fingerprint(case, judge_model=evals.DEFAULT_JUDGE_MODEL, runtime_hash="source")
    rows = []
    calls = [{"kind": "budget", "version": 2, "funding": "chatgpt-plan", "max_calls": 500,
              "max_input_tokens": evals.MAX_INPUT_TOKENS_PER_MODEL_CALL}]
    for rep in range(1, 4):
        started = now - timedelta(minutes=10 - rep)
        row = {"case_id": case["id"], "repetition": rep, "run_id": str(uuid4()), "fingerprint": fingerprint,
               "started_at": started.isoformat(), "timestamp": (started + timedelta(seconds=20)).isoformat(),
               "status": "pass", "acting_model": evals.DEFAULT_ACTING_MODEL, "judge_model": evals.DEFAULT_JUDGE_MODEL,
               "machine": {"pass": True}, "judge": {"pass": True}, "transcript": {"private": "never exported"}}
        rows.append(row)
        for role in ("acting", "judge"):
            token = str(uuid4())
            calls.extend([
                {"kind": "reserve", "id": token,
                 "model": evals.DEFAULT_ACTING_MODEL if role == "acting" else evals.DEFAULT_JUDGE_MODEL, "nano_usd": 0,
                 "reserved_nano_usd": 0, "max_output_tokens": 100,
                 "timestamp": (started + timedelta(seconds=1)).isoformat(),
                 "scope": {key: row[key] for key in ("case_id", "repetition", "run_id", "fingerprint") } | {"role": role}},
                {"kind": "settle", "id": token, "nano_usd": 0,
                 "settled_at": (started + timedelta(seconds=10)).isoformat(),
                 "usage": {"input_tokens": 12, "cached_input_tokens": 3, "output_tokens": 4, "thinking_tokens": 1}},
            ])
    return rows, calls, [case], {"sdk": "1"}, now


def make_receipt(metadata):
    rows, calls, cases, versions, now = metadata
    return build_with_calibration(rows, calls, cases, runtime_hash="source", versions=versions, now=now)


@pytest.mark.parametrize("cap", [1500, 2500, 4000, 5000])
def test_explicit_subscription_attempt_can_qualify_with_approved_call_limit(native_metadata, cap):
    metadata = deepcopy(native_metadata)
    metadata[1][0]["max_calls"] = cap
    receipt = make_receipt(metadata)
    assert receipt["call_allowance"] == {"max_calls": cap, "consumed_calls": 12}
    qualification.verify_receipt(receipt, metadata[2], runtime_hash="source",
                                 versions=metadata[3], now=metadata[4])


# The approved ceiling rises; malformed limits and limits above it stay invalid.
@pytest.mark.parametrize("cap", [5001, 0, -1, True, 1.5, "5000"])
def test_export_and_verification_reject_invalid_subscription_caps(native_metadata, cap):
    metadata = deepcopy(native_metadata)
    metadata[1][0]["max_calls"] = cap
    with pytest.raises(ValueError, match="allowance"):
        make_receipt(metadata)
    receipt = make_receipt(native_metadata)
    receipt["call_allowance"]["max_calls"] = cap
    with pytest.raises(ValueError, match="allowance"):
        qualification.verify_receipt(receipt, native_metadata[2], runtime_hash="source",
                                     versions=native_metadata[3], now=native_metadata[4])


def require_five_repetitions(metadata):
    rows, calls, cases, versions, now = deepcopy(metadata)
    cases[0]["requiredRepetitions"] = 5
    fingerprint = evals.case_fingerprint(cases[0], judge_model=evals.DEFAULT_JUDGE_MODEL,
                                         runtime_hash="source")
    for row in rows:
        row["fingerprint"] = fingerprint
    for call in calls:
        if "scope" in call:
            call["scope"]["fingerprint"] = fingerprint
    return rows, calls, cases, versions, now


def test_export_cannot_qualify_three_passes_for_a_five_repeat_case(native_metadata):
    with pytest.raises(ValueError, match="Unqualified or failed case"):
        make_receipt(require_five_repetitions(native_metadata))


def test_verifier_cannot_accept_a_three_repeat_receipt_for_a_five_repeat_case(native_metadata):
    receipt = make_receipt(native_metadata)
    metadata = require_five_repetitions(native_metadata)
    record = receipt["cases"]["contract"]
    record["fingerprint"] = metadata[0][0]["fingerprint"]
    for row in record["repetitions"]:
        row["fingerprint"] = record["fingerprint"]
        for call in row["calls"]:
            call["scope"]["fingerprint"] = record["fingerprint"]
    with pytest.raises(ValueError, match="required unique repetitions"):
        qualification.validate_record(record, metadata[2][0], runtime_hash="source", now=metadata[4])


@pytest.mark.parametrize("required", [True, 4, 6, "5", None])
def test_qualification_rejects_invalid_case_repetition_requirements(native_metadata, required):
    metadata = deepcopy(native_metadata)
    metadata[2][0]["requiredRepetitions"] = required
    fingerprint = evals.case_fingerprint(metadata[2][0], judge_model=evals.DEFAULT_JUDGE_MODEL,
                                         runtime_hash="source")
    for row in metadata[0]:
        row["fingerprint"] = fingerprint
    for call in metadata[1]:
        if "scope" in call:
            call["scope"]["fingerprint"] = fingerprint
    with pytest.raises((ValueError, RuntimeError)):
        make_receipt(metadata)


def test_export_and_verify_accept_five_distinct_qualified_repetitions(native_metadata):
    metadata = require_five_repetitions(native_metadata)
    rows, calls, cases, versions, now = metadata
    original_events = deepcopy(calls[-4:])
    for rep in (4, 5):
        row = deepcopy(rows[-1])
        row.update(repetition=rep, run_id=str(uuid4()))
        rows.append(row)
        ids = {}
        for event in deepcopy(original_events):
            ids.setdefault(event["id"], str(uuid4()))
            event["id"] = ids[event["id"]]
            if "scope" in event:
                event["scope"].update(repetition=rep, run_id=row["run_id"])
            calls.append(event)
    receipt = make_receipt(metadata)
    assert len(receipt["cases"]["contract"]["repetitions"]) == 5
    qualification.verify_receipt(receipt, cases, runtime_hash="source", versions=versions, now=now)


def test_actor_only_export_cannot_treat_real_judge_calibration_as_advisory(native_metadata):
    rows, calls, cases, versions, now = native_metadata
    with pytest.raises(ValueError):
        qualification.build_receipt(rows, calls, cases, runtime_hash="source", versions=versions, now=now)


def test_actor_only_receipt_cannot_satisfy_the_release_verifier(native_metadata):
    receipt = make_receipt(native_metadata)
    receipt.pop("calibration", None)
    with pytest.raises(ValueError):
        qualification.verify_receipt(receipt, native_metadata[2], runtime_hash="source",
                                     versions=native_metadata[3], now=native_metadata[4])


def test_export_requires_native_actor_and_judge_and_keeps_transcripts_private(native_metadata):
    receipt = make_receipt(native_metadata)
    # Both native actor/judge repetitions and independent calibration use one cap.
    assert receipt["call_allowance"] == {"max_calls": 500, "consumed_calls": 12}
    assert "never exported" not in evals.stable_json(receipt)
    qualification.verify_receipt(receipt, native_metadata[2], runtime_hash="source",
                                 versions=native_metadata[3], now=native_metadata[4])


@pytest.mark.parametrize("change", [
    "no_calls", "no_judge", "unsettled", "other_attempt", "other_source", "other_rep", "api_model",
    "no_usage", "empty_usage", "bad_cached_usage", "before_attempt", "after_attempt", "duplicate_call",
    "failed_machine", "failed_judge", "duplicate_repetition", "missing_repetition", "later_failure",
    "future", "naive", "stale", "exceeded_cap", "paid_header",
])
def test_export_rejects_unsafe_native_metadata(native_metadata, change):
    rows, calls, cases, versions, now = deepcopy(native_metadata)
    if change == "no_calls":
        calls = calls[:1]
    elif change == "no_judge":
        calls = [calls[0], *[event for i, event in enumerate(calls[1:]) if i % 4 < 2]]
    elif change == "unsettled":
        del calls[2]
    elif change == "other_attempt":
        calls[1]["scope"]["run_id"] = str(uuid4())
    elif change == "other_source":
        calls[1]["scope"]["fingerprint"] = "other"
    elif change == "other_rep":
        calls[1]["scope"]["repetition"] = 2
    elif change == "api_model":
        calls[1]["model"] = "openai/gpt-6-luna"
    elif change == "no_usage":
        del calls[2]["usage"]
    elif change == "empty_usage":
        calls[2]["usage"]["input_tokens"] = 0
    elif change == "bad_cached_usage":
        calls[2]["usage"]["cached_input_tokens"] = 100
    elif change == "before_attempt":
        calls[1]["timestamp"] = (now - timedelta(minutes=20)).isoformat()
    elif change == "after_attempt":
        calls[2]["settled_at"] = now.isoformat()
    elif change == "duplicate_call":
        calls.extend(deepcopy(calls[1:3]))
    elif change == "failed_machine":
        rows[0]["machine"]["pass"] = False
    elif change == "failed_judge":
        rows[0]["judge"]["pass"] = False
    elif change == "duplicate_repetition":
        rows[2]["repetition"] = 1
    elif change == "missing_repetition":
        del rows[1]
    elif change == "later_failure":
        rows.append({**rows[0], "status": "error"})
    elif change == "future":
        rows[0]["timestamp"] = (now + timedelta(days=1)).isoformat()
    elif change == "naive":
        rows[0]["timestamp"] = now.replace(tzinfo=None).isoformat()
    elif change == "stale":
        rows[0]["timestamp"] = (now - timedelta(days=91)).isoformat()
    elif change == "exceeded_cap":
        calls[0]["max_calls"] = 1
    elif change == "paid_header":
        calls[0]["funding"] = "api"
    with pytest.raises((ValueError, KeyError, TypeError)):
        build_with_calibration(rows, calls, cases, runtime_hash="source", versions=versions, now=now)


@pytest.mark.parametrize("change", ["missing_catalog", "wrong_sdk", "wrong_source", "unknown_field", "missing_calls"])
def test_verifier_blocks_incomplete_or_changed_release_candidates(native_metadata, change):
    receipt = make_receipt(native_metadata)
    cases, versions, runtime_hash = native_metadata[2], native_metadata[3], "source"
    if change == "missing_catalog":
        cases = [*cases, {"id": "new-case"}]
    elif change == "wrong_sdk":
        versions = {"sdk": "2"}
    elif change == "wrong_source":
        runtime_hash = "changed"
    elif change == "unknown_field":
        receipt["cases"]["contract"]["repetitions"][0]["transcript"] = {"private": "blocked"}
    elif change == "missing_calls":
        receipt["cases"]["contract"]["repetitions"][0]["calls"] = []
    with pytest.raises(ValueError):
        qualification.verify_receipt(receipt, cases, runtime_hash=runtime_hash, versions=versions, now=native_metadata[4])


def test_retired_freshness_archive_cannot_replace_native_release_evidence():
    root = evals.REPO_ROOT
    # Deliberate migration: the old successful-archive path accepted freshness with no native calls.
    assert not (root / "scripts/restore_agent_eval_freshness.py").exists()
    for name in ("agent-evals.yml", "release.yml"):
        workflow = (root / ".github/workflows" / name).read_text()
        assert "python scripts/qualify_agent_evals.py verify" in workflow
        assert "restore_agent_eval_freshness.py" not in workflow
        assert "LUMIBOT_SUBSCRIPTION_EVAL_RUNNER" not in workflow
        assert "OPENAI_API_KEY" not in workflow


def test_failed_export_invalidates_prior_green_receipt(native_metadata, tmp_path, monkeypatch):
    rows, calls, cases, versions, now = native_metadata
    receipt_path = tmp_path / "qualification.json"
    evals.write_json_atomic(receipt_path, make_receipt(native_metadata))
    rows.append({**rows[0], "status": "fail"})
    calibration_rows, calibration_calls, summary = calibration_metadata(cases, now)
    calibration_root = tmp_path / "calibration"
    for row in calibration_rows:
        evals.append_jsonl(calibration_root / "ledger.jsonl", row)
    evals.write_json_atomic(calibration_root / "summary.json", summary)
    for name, values in (("ledger.jsonl", rows), ("model_calls.jsonl", [*calls, *calibration_calls])):
        for value in values:
            evals.append_jsonl(tmp_path / name, value)
    monkeypatch.setattr(qualification.evals, "load_cases", lambda: cases)
    monkeypatch.setattr(qualification.evals, "runtime_fingerprint", lambda: "source")
    monkeypatch.setattr(qualification.evals, "sdk_versions", lambda: versions)
    monkeypatch.setattr(qualification.sys, "argv", ["qualify", "create", "--output-root", str(tmp_path),
                                                   "--calibration-output-root", str(calibration_root),
                                                   "--model-call-ledger", str(tmp_path / "model_calls.jsonl"),
                                                   "--receipt", str(receipt_path)])
    with pytest.raises(ValueError):
        qualification.main()
    import json
    with pytest.raises(ValueError):
        qualification.verify_receipt(json.loads(receipt_path.read_text()), cases,
                                     runtime_hash="source", versions=versions, now=now)


def test_pending_attempt_invalidates_only_matching_export(native_metadata, tmp_path):
    import json
    receipt = make_receipt(native_metadata)
    path = tmp_path / "qualification.json"
    evals.write_json_atomic(path, receipt)
    unchanged = path.read_bytes()
    fingerprint = receipt["cases"]["contract"]["fingerprint"]
    qualification.invalidate_receipt(path, {"contract": fingerprint}, runtime_hash="unrelated")
    qualification.invalidate_receipt(path, {"contract": "unrelated"}, runtime_hash="source")
    assert path.read_bytes() == unchanged
    qualification.invalidate_receipt(path, {"contract": fingerprint}, runtime_hash="source")
    with pytest.raises(ValueError):
        qualification.verify_receipt(json.loads(path.read_text()), native_metadata[2],
                                     runtime_hash="source", versions=native_metadata[3], now=native_metadata[4])


def test_later_orphan_native_call_cannot_revive_old_passes(native_metadata):
    rows, calls, cases, versions, now = deepcopy(native_metadata)
    calls.append({**calls[1], "id": str(uuid4()), "timestamp": (now - timedelta(seconds=1)).isoformat(),
                  "scope": {**calls[1]["scope"], "run_id": str(uuid4())}})
    with pytest.raises(ValueError):
        build_with_calibration(rows, calls, cases, runtime_hash="source", versions=versions, now=now)


def test_older_or_unrelated_orphan_still_consumes_allowance_without_poisoning_recovered_passes(native_metadata):
    rows, calls, cases, versions, now = deepcopy(native_metadata)
    calls.append({**calls[1], "id": str(uuid4()), "timestamp": (now - timedelta(minutes=20)).isoformat(),
                  "scope": {**calls[1]["scope"], "run_id": str(uuid4())}})
    calls.append({**calls[1], "id": str(uuid4()), "timestamp": (now - timedelta(seconds=1)).isoformat(),
                  "scope": {**calls[1]["scope"], "run_id": str(uuid4()), "fingerprint": "other-source"}})
    receipt = build_with_calibration(rows, calls, cases, runtime_hash="source", versions=versions, now=now)
    assert receipt["call_allowance"]["consumed_calls"] == 14


def test_resume_recovers_newer_native_orphan_as_incomplete_without_forgiving_calls(native_metadata):
    rows, calls, cases, versions, now = deepcopy(native_metadata)
    newer = {**calls[1], "id": str(uuid4()), "timestamp": (now - timedelta(seconds=1)).isoformat(),
             "scope": {**calls[1]["scope"], "run_id": str(uuid4())}}
    calls.append(newer)
    fingerprint = rows[0]["fingerprint"]
    recovered = evals.recover_native_incomplete_attempts(rows, calls, {"contract": fingerprint})
    assert len(recovered) == 1
    assert recovered[0]["run_id"] == newer["scope"]["run_id"]
    assert recovered[0]["repetition"] == 1
    assert recovered[0]["status"] == "incomplete"
    assert evals.consecutive_pass_count(rows + recovered, "contract", fingerprint) == 2
    assert not evals.recover_native_incomplete_attempts(rows + recovered, calls, {"contract": fingerprint})
    assert len(qualification.EvalCallBudget._calls(calls[1:])) == 7


def test_resume_does_not_recover_old_source_or_older_native_orphans(native_metadata):
    rows, calls, cases, versions, now = deepcopy(native_metadata)
    calls.append({**calls[1], "id": str(uuid4()), "timestamp": (now - timedelta(minutes=20)).isoformat(),
                  "scope": {**calls[1]["scope"], "run_id": str(uuid4())}})
    calls.append({**calls[1], "id": str(uuid4()), "timestamp": (now - timedelta(seconds=1)).isoformat(),
                  "scope": {**calls[1]["scope"], "run_id": str(uuid4()), "fingerprint": "other-source"}})
    assert not evals.recover_native_incomplete_attempts(rows, calls, {"contract": rows[0]["fingerprint"]})


@pytest.mark.parametrize("change", [
    "missing_control", "missing_repetition", "failed_latest", "incomplete_latest", "newer_orphan",
    "wrong_source", "wrong_runtime", "wrong_case", "wrong_label", "wrong_verdict", "missing_calls",
    "unsettled", "no_usage", "empty_usage", "api_model", "paid_cost", "actor_scope", "duplicate_call",
    "before_attempt", "future", "stale", "external_writes", "native_write", "actor_runs", "creates_freshness",
    "summary_errors", "summary_missing",
])
def test_calibration_export_requires_complete_independent_native_judgment(native_metadata, change):
    rows, calls, cases, versions, now = deepcopy(native_metadata)
    controls, judge_calls, summary = calibration_metadata(cases, now)
    if change == "missing_control":
        controls = [row for row in controls if row["id"] != "reject-control"]
    elif change == "missing_repetition":
        del controls[1]
    elif change in {"failed_latest", "incomplete_latest"}:
        controls.append({**controls[0], "status": "fail" if change == "failed_latest" else "incomplete", "passed": False})
    elif change == "newer_orphan":
        judge_calls.append({**judge_calls[0], "id": str(uuid4()), "timestamp": (now - timedelta(seconds=1)).isoformat(),
                            "scope": {**judge_calls[0]["scope"], "run_id": str(uuid4())}})
    elif change == "wrong_source":
        controls[0]["fingerprint"] = "other-source"
    elif change == "wrong_runtime":
        controls[0]["runtime_fingerprint"] = "other-runtime"
    elif change == "wrong_case":
        controls[0]["case_id"] = "other-case"
    elif change == "wrong_label":
        controls[0]["expected"] = False
    elif change == "wrong_verdict":
        controls[0]["judge"]["pass"] = False
    elif change == "missing_calls":
        judge_calls = []
    elif change == "unsettled":
        del judge_calls[1]
    elif change == "no_usage":
        del judge_calls[1]["usage"]
    elif change == "empty_usage":
        judge_calls[1]["usage"].update(output_tokens=0, thinking_tokens=0)
    elif change == "api_model":
        judge_calls[0]["model"] = "openai/gpt-6-luna"
    elif change == "paid_cost":
        judge_calls[1]["nano_usd"] = 1
    elif change == "actor_scope":
        judge_calls[0]["scope"]["role"] = "acting"
    elif change == "duplicate_call":
        judge_calls += deepcopy(judge_calls[:2])
    elif change == "before_attempt":
        judge_calls[0]["timestamp"] = (now - timedelta(minutes=20)).isoformat()
    elif change == "future":
        controls[0]["timestamp"] = (now + timedelta(days=1)).isoformat()
    elif change == "stale":
        controls[0]["timestamp"] = (now - timedelta(days=91)).isoformat()
    elif change == "native_write":
        controls[0]["external_writes"] = 1
    elif change in {"external_writes", "actor_runs"}:
        summary[change] = 1
    elif change == "creates_freshness":
        summary["creates_case_freshness"] = True
    elif change == "summary_errors":
        summary["errors"] = 1
    elif change == "summary_missing":
        summary["missing"] = 1
    with pytest.raises((ValueError, KeyError, TypeError)):
        qualification.build_receipt(rows, [*calls, *judge_calls], cases, runtime_hash="source", versions=versions,
                                    now=now, calibration_rows=controls, calibration_summary=summary)


@pytest.mark.parametrize("change", [
    "missing_control", "missing_repetition", "wrong_verdict", "wrong_label", "no_usage", "actor_scope",
    "source", "future", "stale", "private_field", "external_writes", "actor_reused",
])
def test_cloud_verifier_requires_the_same_calibration_contract(native_metadata, change):
    receipt = make_receipt(native_metadata)
    record = receipt["calibration"]["controls"]["accept-control"]
    row = record["repetitions"][0]
    if change == "missing_control":
        del receipt["calibration"]["controls"]["reject-control"]
    elif change == "missing_repetition":
        record["repetitions"].pop()
    elif change == "wrong_verdict":
        row["judge_pass"] = False
    elif change == "wrong_label":
        row["expected"] = False
    elif change == "no_usage":
        del row["calls"][0]["usage"]
    elif change == "actor_scope":
        row["calls"][0]["scope"]["role"] = "acting"
    elif change == "source":
        record["fingerprint"] = "other-source"
    elif change == "future":
        row["timestamp"] = (native_metadata[4] + timedelta(days=1)).isoformat()
    elif change == "stale":
        row["timestamp"] = (native_metadata[4] - timedelta(days=91)).isoformat()
    elif change == "private_field":
        row["transcript"] = {"private": "must stay private"}
    elif change == "external_writes":
        receipt["calibration"]["external_writes"] = 1
    elif change == "actor_reused":
        row["run_id"] = receipt["cases"]["contract"]["repetitions"][0]["run_id"]
        row["calls"][0]["scope"]["run_id"] = row["run_id"]
    with pytest.raises(ValueError):
        qualification.verify_receipt(receipt, native_metadata[2], runtime_hash="source",
                                     versions=native_metadata[3], now=native_metadata[4])


@pytest.mark.parametrize("change", ["negative_only", "positive_only", "no_repetition_policy", "duplicate_id", "new_control", "changed_control"])
def test_maintained_calibration_catalog_cannot_be_omitted_or_changed(native_metadata, change):
    receipt = make_receipt(native_metadata)
    controls = json.loads(qualification.CALIBRATION_CATALOG_PATH.read_text())
    if change == "negative_only":
        controls = [row for row in controls if not row["expected"]]
    elif change == "positive_only":
        controls = [row for row in controls if row["expected"]]
    elif change == "no_repetition_policy":
        del controls[0]["required_repetitions"]
    elif change == "duplicate_id":
        controls.append(deepcopy(controls[0]))
    elif change == "new_control":
        controls.append({**controls[0], "id": "new-control"})
    else:
        controls[0]["transcript"] = {"different": "source evidence"}
    qualification.CALIBRATION_CATALOG_PATH.write_text(json.dumps(controls))
    with pytest.raises(ValueError):
        qualification.verify_receipt(receipt, native_metadata[2], runtime_hash="source",
                                     versions=native_metadata[3], now=native_metadata[4])


def test_high_risk_control_requires_all_five_real_judge_repetitions(native_metadata):
    controls = json.loads(qualification.CALIBRATION_CATALOG_PATH.read_text())
    controls[1]["required_repetitions"] = 5
    qualification.CALIBRATION_CATALOG_PATH.write_text(json.dumps(controls))
    rows, calls, cases, versions, now = native_metadata
    calibration_rows, calibration_calls, summary = calibration_metadata(cases, now)
    receipt = qualification.build_receipt(rows, [*calls, *calibration_calls], cases, runtime_hash="source",
        versions=versions, now=now, calibration_rows=calibration_rows, calibration_summary=summary)
    assert len(receipt["calibration"]["controls"]["reject-control"]["repetitions"]) == 5
    incomplete = [row for row in calibration_rows if row["id"] != "reject-control" or row["repetition"] <= 3]
    with pytest.raises(ValueError):
        qualification.build_receipt(rows, [*calls, *calibration_calls], cases, runtime_hash="source",
            versions=versions, now=now, calibration_rows=incomplete, calibration_summary=summary)


def test_new_calibration_attempt_invalidates_an_older_publishable_receipt(native_metadata, tmp_path):
    receipt = make_receipt(native_metadata)
    path = tmp_path / "qualification.json"
    evals.write_json_atomic(path, receipt)
    fingerprint = receipt["calibration"]["controls"]["reject-control"]["fingerprint"]
    qualification.invalidate_receipt(path, {"reject-control": fingerprint}, runtime_hash="source")
    invalidated = json.loads(path.read_text())
    assert invalidated["invalidated"]["calibration_control_ids"] == ["reject-control"]
    with pytest.raises(ValueError):
        qualification.verify_receipt(invalidated, native_metadata[2], runtime_hash="source",
                                     versions=native_metadata[3], now=native_metadata[4])


def test_native_export_retains_original_header_and_explicit_allowance_amendment(native_metadata):
    metadata = deepcopy(native_metadata)
    metadata[1].append({"kind": "allowance_amendment", "previous_max_calls": 500, "max_calls": 1500,
                        "reason": "Additional source qualification", "timestamp": metadata[4].isoformat()})
    receipt = make_receipt(metadata)
    assert receipt["call_allowance"] == {"max_calls": 1500, "consumed_calls": 12}
    assert metadata[1][0]["max_calls"] == 500


def test_earlier_unsettled_judge_can_resume_without_replaying_settled_actor(native_metadata):
    rows, calls, cases, versions, now = deepcopy(native_metadata)
    row = rows[0]
    unknown = {**calls[1], "id": str(uuid4()),
               "scope": {**calls[1]["scope"], "role": "judge"},
               "timestamp": (datetime.fromisoformat(row["started_at"]) + timedelta(seconds=0.5)).isoformat()}
    calls.append(unknown)
    receipt = build_with_calibration(rows, calls, cases, runtime_hash="source", versions=versions, now=now)
    assert receipt["call_allowance"]["consumed_calls"] == 13
    assert all(call["id"] != unknown["id"] for item in receipt["cases"]["contract"]["repetitions"] for call in item["calls"])
    assert calls[-1] == unknown  # Unknown usage is retained, never settled or removed.


@pytest.mark.parametrize("role", ["acting", "judge"])
def test_unsettled_actor_or_newer_judge_still_blocks_resumed_export(native_metadata, role):
    rows, calls, cases, versions, now = deepcopy(native_metadata)
    row = rows[0]
    calls.append({**calls[1], "id": str(uuid4()), "scope": {**calls[1]["scope"], "role": role},
                  "timestamp": (datetime.fromisoformat(row["started_at"]) + timedelta(seconds=15)).isoformat()})
    with pytest.raises(ValueError):
        build_with_calibration(rows, calls, cases, runtime_hash="source", versions=versions, now=now)


@pytest.mark.parametrize("damage", ["model", "case_id", "fingerprint"])
def test_unknown_judge_exception_never_hides_foreign_scope_or_model(native_metadata, damage):
    rows, calls, cases, versions, now = deepcopy(native_metadata)
    old = {**calls[1], "id": str(uuid4()), "scope": {**calls[1]["scope"], "role": "judge"},
           "timestamp": (datetime.fromisoformat(rows[0]["started_at"]) + timedelta(seconds=0.5)).isoformat()}
    if damage == "model":
        old["model"] = "other-model"
    else:
        old["scope"][damage] = "other-identity"
    calls.append(old)
    with pytest.raises(ValueError):
        build_with_calibration(rows, calls, cases, runtime_hash="source", versions=versions, now=now)
