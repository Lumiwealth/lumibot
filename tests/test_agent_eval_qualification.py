"""Receipt integrity contracts. Synthetic metadata here is never an agent eval."""

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest

from scripts import qualify_agent_evals as qualification
from scripts import run_agent_evals as evals


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
                {"kind": "reserve", "id": token, "model": evals.DEFAULT_ACTING_MODEL, "nano_usd": 0,
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
    return qualification.build_receipt(rows, calls, cases, runtime_hash="source", versions=versions, now=now)


def test_export_requires_native_actor_and_judge_and_keeps_transcripts_private(native_metadata):
    receipt = make_receipt(native_metadata)
    assert receipt["call_allowance"] == {"max_calls": 500, "consumed_calls": 6}
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
        qualification.build_receipt(rows, calls, cases, runtime_hash="source", versions=versions, now=now)


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
    for name, values in (("ledger.jsonl", rows), ("model_calls.jsonl", calls)):
        for value in values:
            evals.append_jsonl(tmp_path / name, value)
    monkeypatch.setattr(qualification.evals, "load_cases", lambda: cases)
    monkeypatch.setattr(qualification.evals, "runtime_fingerprint", lambda: "source")
    monkeypatch.setattr(qualification.evals, "sdk_versions", lambda: versions)
    monkeypatch.setattr(qualification.sys, "argv", ["qualify", "create", "--output-root", str(tmp_path),
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
        qualification.build_receipt(rows, calls, cases, runtime_hash="source", versions=versions, now=now)


def test_older_or_unrelated_orphan_still_consumes_allowance_without_poisoning_recovered_passes(native_metadata):
    rows, calls, cases, versions, now = deepcopy(native_metadata)
    calls.append({**calls[1], "id": str(uuid4()), "timestamp": (now - timedelta(minutes=20)).isoformat(),
                  "scope": {**calls[1]["scope"], "run_id": str(uuid4())}})
    calls.append({**calls[1], "id": str(uuid4()), "timestamp": (now - timedelta(seconds=1)).isoformat(),
                  "scope": {**calls[1]["scope"], "run_id": str(uuid4()), "fingerprint": "other-source"}})
    receipt = qualification.build_receipt(rows, calls, cases, runtime_hash="source", versions=versions, now=now)
    assert receipt["call_allowance"]["consumed_calls"] == 8


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
