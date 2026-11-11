#!/usr/bin/env python3
"""Export and verify source-bound evidence of authenticated real actor/judge runs.

The release owner reviews native private artifacts before committing this compact,
public-safe receipt. Digests bind evidence; they are not proof of inference or a
substitute for trusted review. Hosted checks never copy a developer's rotating
ChatGPT login into a hosted worker.

Release scope (2026-11-11): when ``ci/agent-evals/release-scope.json`` exists, ``verify``
checks only the author-written scope for this package version: the case ids the author
listed, each with a reason, and current pass evidence for every listed case. An empty
list is valid when the author states why no case can be affected. This check never
infers, adds or refreshes cases; the author chooses them. Without the scope file the
original whole-catalog receipt check runs unchanged.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path
from uuid import UUID

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import check_agent_judge_calibration as calibration
from scripts import run_agent_evals as evals
from scripts.agent_eval_call_budget import EvalCallBudget as EvalCallBudget
from scripts.agent_eval_subscription_budget import subscription_call_state

KIND = "lumibot-real-model-qualification"
VERSION = 2
DEFAULT_PATH = evals.REPO_ROOT / "ci/agent-evals/qualification.json"
CALIBRATION_CATALOG_PATH = evals.REPO_ROOT / "tests/fixtures/agent_judge_calibration.json"
SCOPE_PATH = evals.REPO_ROOT / "ci/agent-evals/release-scope.json"
SCOPE_VERSION = 1


def digest(value):
    return hashlib.sha256(evals.stable_json(value).encode()).hexdigest()


def timestamp(value, *, now, require_fresh=True):
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except (AttributeError, TypeError, ValueError) as exc:
        raise ValueError("Invalid qualification timestamp") from exc
    if (result.tzinfo is None or result > now
            or require_fresh and result < now - timedelta(days=evals.DEFAULT_FRESHNESS_DAYS)):
        raise ValueError("Qualification is future-dated, stale, or lacks a timezone")
    return result


def identity(value):
    try:
        if str(UUID(value)) != value:
            raise ValueError()
    except (AttributeError, TypeError, ValueError) as exc:
        raise ValueError("Invalid native attempt/call identity") from exc


def validate_call(call, *, row, role, model, now):
    identity(call.get("id"))
    expected_scope = {key: row[key] for key in ("case_id", "repetition", "run_id", "fingerprint")}
    expected_scope["role"] = role
    if call.get("scope") != expected_scope or call.get("model") != model or call.get("kind") != "settle":
        raise ValueError("Missing or mismatched native actor/judge settlement")
    if call.get("nano_usd") != 0 or call.get("reserved_nano_usd") != 0:
        raise ValueError("API-funded evidence cannot qualify a subscription release")
    usage = call.get("usage")
    fields = {"input_tokens", "cached_input_tokens", "output_tokens", "thinking_tokens"}
    if not isinstance(usage, dict) or set(usage) != fields or any(type(n) is not int or n < 0 for n in usage.values()):
        raise ValueError("Missing authoritative model usage")
    if usage["input_tokens"] <= 0 or usage["output_tokens"] + usage["thinking_tokens"] <= 0:
        raise ValueError("Empty native model usage")
    if usage["cached_input_tokens"] > usage["input_tokens"]:
        raise ValueError("Invalid cached model usage")
    started, ended = (timestamp(row[key], now=now) for key in ("started_at", "timestamp"))
    reserved, settled = (timestamp(call[key], now=now) for key in ("timestamp", "settled_at"))
    if not started <= reserved <= settled <= ended:
        raise ValueError("Native model usage falls outside its attempt")


def validate_record(record, case, *, runtime_hash, now, required=None):
    acting_model = str(case.get("model") or evals.DEFAULT_ACTING_MODEL)
    judge_model = evals.DEFAULT_JUDGE_MODEL
    fingerprint = evals.case_fingerprint(case, judge_model=judge_model, runtime_hash=runtime_hash)
    if not acting_model.startswith("chatgpt/") or record.get("fingerprint") != fingerprint:
        raise ValueError("Qualification source/model fingerprint mismatch")
    rows = record.get("repetitions")
    if not isinstance(rows, list) or len(rows) < (evals.case_required_passes(case) if required is None else required):
        raise ValueError("Qualification lacks required unique repetitions")
    repetitions, attempts, call_ids = set(), set(), set()
    for row in rows:
        required_keys = {"case_id", "repetition", "run_id", "fingerprint", "started_at", "timestamp", "status",
                         "acting_model", "judge_model", "machine_pass", "judge_pass", "native_evidence_digest", "calls"}
        if not isinstance(row, dict) or set(row) != required_keys:
            raise ValueError("Unexpected qualification fields; native transcripts must stay private")
        rep = row["repetition"]
        identity(row["run_id"])
        if type(rep) is not int or rep < 1 or rep in repetitions or row["run_id"] in attempts:
            raise ValueError("Duplicate or invalid native repetition")
        repetitions.add(rep)
        attempts.add(row["run_id"])
        if (row["case_id"] != case["id"] or row["fingerprint"] != fingerprint or row["status"] != "pass"
                or row["machine_pass"] is not True or row["judge_pass"] is not True
                or row["acting_model"] != acting_model or row["judge_model"] != judge_model):
            raise ValueError("Qualification has a failed or mismatched actor/judge result")
        source_digest = row["native_evidence_digest"]
        if not isinstance(source_digest, str) or len(source_digest) != 64 or any(c not in "0123456789abcdef" for c in source_digest):
            raise ValueError("Invalid native evidence digest")
        timestamp(row["timestamp"], now=now)
        calls = row["calls"]
        if not isinstance(calls, list) or not calls:
            raise ValueError("Qualification is missing native model calls")
        roles = set()
        allowed_call_keys = {"id", "kind", "scope", "model", "nano_usd", "reserved_nano_usd", "usage", "timestamp", "settled_at"}
        for call in calls:
            if not isinstance(call, dict) or set(call) != allowed_call_keys or not isinstance(call.get("scope"), dict):
                raise ValueError("Unexpected native model-call fields")
            role = call["scope"].get("role")
            if role not in {"acting", "judge"} or call["id"] in call_ids:
                raise ValueError("Duplicated or unsupported model call")
            validate_call(call, row=row, role=role, model=acting_model if role == "acting" else judge_model, now=now)
            roles.add(role)
            call_ids.add(call["id"])
        if roles != {"acting", "judge"}:
            raise ValueError("Both real actor and judge inference are required")
    if max(repetitions) - min(repetitions) + 1 != len(repetitions):
        raise ValueError("Qualification repetitions are not consecutive")
    return attempts, call_ids


def load_calibration_catalog(cases, *, scoped=False):
    """The maintained controls; with ``scoped``, only the controls of ``cases``.

    A release scope lists the cases a release can affect. Its receipt calibrates the
    judge on those cases' own controls only (possibly none); the whole-catalog checks
    (every control linked, both labels present) apply to the unscoped receipt.
    """
    controls = json.loads(CALIBRATION_CATALOG_PATH.read_text())
    case_ids = {case["id"] for case in cases}
    if not isinstance(controls, list) or not controls:
        raise ValueError("Maintained judge calibration catalog is required")
    if scoped:
        controls = [control for control in controls if isinstance(control, dict) and control.get("case_id") in case_ids]
    seen, labels = set(), set()
    for control in controls:
        if (not isinstance(control, dict) or not isinstance(control.get("id"), str) or not control["id"]
                or control["id"] in seen or control.get("case_id") not in case_ids
                or type(control.get("expected")) is not bool or not isinstance(control.get("transcript"), dict)
                or type(control.get("required_repetitions")) is not int
                or control["required_repetitions"] not in {3, 5}):
            raise ValueError("Calibration catalog requires unique linked controls and explicit 3/5 repetitions")
        seen.add(control["id"])
        labels.add(control["expected"])
    if not scoped and labels != {True, False}:
        raise ValueError("Calibration requires both positive and negative maintained controls")
    return controls


def validate_calibration_record(record, control, case, *, runtime_hash, now):
    fingerprint = calibration.control_fingerprint(control, case, runtime_hash)
    if not isinstance(record, dict) or set(record) != {"fingerprint", "repetitions"} or record["fingerprint"] != fingerprint:
        raise ValueError("Calibration control/source fingerprint mismatch")
    rows = record["repetitions"]
    if not isinstance(rows, list) or len(rows) != control["required_repetitions"]:
        raise ValueError("Missing required calibration repetitions")
    repetitions, attempts, call_ids = set(), set(), set()
    keys = {"id", "case_id", "repetition", "run_id", "fingerprint", "started_at", "timestamp", "status",
            "expected", "passed", "judge_pass", "judge_model", "native_evidence_digest", "calls"}
    call_keys = {"id", "kind", "scope", "model", "nano_usd", "reserved_nano_usd", "usage", "timestamp", "settled_at"}
    for row in rows:
        if not isinstance(row, dict) or set(row) != keys:
            raise ValueError("Unexpected calibration fields; private controls must not be exported")
        identity(row["run_id"])
        rep = row["repetition"]
        if type(rep) is not int or rep in repetitions:
            raise ValueError("Duplicate or invalid calibration repetition")
        repetitions.add(rep)
        attempts.add(row["run_id"])
        if (row["id"] != control["id"] or row["case_id"] != case["id"] or row["fingerprint"] != fingerprint
                or row["expected"] is not control["expected"] or row["passed"] is not True
                or row["judge_pass"] is not control["expected"] or row["status"] != "pass"
                or row["judge_model"] != evals.DEFAULT_JUDGE_MODEL):
            raise ValueError("Calibration did not correctly classify its maintained control")
        h = row["native_evidence_digest"]
        if not isinstance(h, str) or len(h) != 64 or any(c not in "0123456789abcdef" for c in h):
            raise ValueError("Missing native calibration evidence digest")
        calls = row["calls"]
        if not isinstance(calls, list) or not calls:
            raise ValueError("Calibration requires real judge inference")
        for call in calls:
            if not isinstance(call, dict) or set(call) != call_keys or call["id"] in call_ids:
                raise ValueError("Duplicated or unsupported calibration call")
            validate_call(call, row={**row, "case_id": control["id"]}, role="judge_calibration",
                          model=evals.DEFAULT_JUDGE_MODEL, now=now)
            call_ids.add(call["id"])
    if repetitions != set(range(1, control["required_repetitions"] + 1)):
        raise ValueError("Calibration does not cover every required unique repetition")
    return attempts, call_ids


def build_calibration(rows, summary, calls, cases, *, runtime_hash, now, scoped=False):
    controls = load_calibration_catalog(cases, scoped=scoped)
    total = sum(item["required_repetitions"] for item in controls)
    if scoped and not controls:
        return {"catalog_digest": digest(controls), "actor_runs": 0, "external_writes": 0,
                "creates_case_freshness": False, "controls": {}}
    if (not isinstance(rows, list) or not isinstance(summary, dict) or summary.get("kind") != "judge-calibration"
            or summary.get("runtime_fingerprint") != runtime_hash or summary.get("judge_model") != evals.DEFAULT_JUDGE_MODEL
            or type(summary.get("passed")) is not int or summary["passed"] != total
            or any(type(summary.get(key)) is not int or summary[key] != 0 for key in
                   ("failed", "errors", "missing", "actor_runs", "external_writes"))
            or summary.get("creates_case_freshness") is not False):
        raise ValueError("Complete judge-only calibration with no external writes is required")
    by_case = {case["id"]: case for case in cases}
    records = {}
    for control in controls:
        fingerprint = calibration.control_fingerprint(control, by_case[control["case_id"]], runtime_hash)
        latest = evals.latest_repetition_rows(
            [{**row, "case_id": row.get("id"), "_native": row} for row in rows], control["id"], fingerprint
        )
        if set(latest) != set(range(1, control["required_repetitions"] + 1)) or any(
            row.get("status") != "pass" or row.get("passed") is not True for row in latest.values()
        ):
            raise ValueError("Calibration is missing, failed, or superseded by an incomplete attempt")
        projected = []
        for rep, normalized in sorted(latest.items()):
            # The calibration runner's native case_id links the control to its
            # actor case; its model-call scope instead identifies the control.
            row = normalized["_native"]
            if (row.get("case_id") != control["case_id"] or row.get("runtime_fingerprint") != runtime_hash
                    or row.get("actor_runs", 0) != 0 or row.get("external_writes", 0) != 0):
                raise ValueError("Calibration native case/runtime link or write boundary mismatch")
            started = timestamp(row.get("started_at"), now=now)
            for call in calls.values():
                scope = call.get("scope") or {}
                if (scope.get("case_id") == control["id"] and scope.get("repetition") == rep
                        and scope.get("fingerprint") == fingerprint and scope.get("run_id") != row.get("run_id")
                        and timestamp(call.get("timestamp"), now=now, require_fresh=False) >= started):
                    raise ValueError("A newer incomplete calibration attempt supersedes the prior pass")
                if scope.get("run_id") == row.get("run_id") and scope.get("role") != "judge_calibration":
                    raise ValueError("Calibration attempted actor inference or another unsupported model role")
            bound = [call for call in calls.values() if (call.get("scope") or {}).get("run_id") == row.get("run_id")
                     and (call.get("scope") or {}).get("case_id") == control["id"]
                     and (call.get("scope") or {}).get("repetition") == rep]
            value = {key: row.get(key) for key in ("id", "case_id", "repetition", "run_id", "fingerprint", "started_at",
                                                  "timestamp", "status", "expected", "passed", "judge_model")}
            value.update(judge_pass=(row.get("judge") or {}).get("pass"), native_evidence_digest=digest(row),
                         calls=[{key: call.get(key) for key in ("id", "kind", "scope", "model", "nano_usd",
                                "reserved_nano_usd", "usage", "timestamp", "settled_at")} for call in bound])
            projected.append(value)
        records[control["id"]] = {"fingerprint": fingerprint, "repetitions": projected}
    return {"catalog_digest": digest(controls), "actor_runs": 0, "external_writes": 0,
            "creates_case_freshness": False, "controls": records}


def establishment_runtime(source_ref, cases, versions):
    """Rebuild the original runtime and unchanged case definitions from Git."""
    import re
    from scripts import agent_eval_source_compatibility as source
    if not isinstance(source_ref, str) or not re.fullmatch("[0-9a-f]{40}", source_ref):
        raise ValueError("Establishment requires an immutable full Git commit")
    source._git("merge-base", "--is-ancestor", source_ref, "HEAD", root=evals.REPO_ROOT)
    original = source._blobs(source_ref, source.runtime_paths(root=evals.REPO_ROOT, source_ref=source_ref), root=evals.REPO_ROOT)
    for case in cases:
        path = Path(case["_path"]).relative_to(evals.REPO_ROOT).as_posix()
        before = json.loads(source._git("show", f"{source_ref}:{path}", root=evals.REPO_ROOT))
        if before != {key: value for key, value in case.items() if not key.startswith("_")}:
            raise ValueError("A new or changed case requires its complete targeted repetitions")
    return source._fingerprint(original, versions)


def build_establishment(rows, call_rows, cases, *, source_ref, versions, now=None):
    now = now or evals.utc_now()
    # A catalog can contain unchanged established cases beside changed/new ones.
    # Verify the immutable original runtime first; only exact original case
    # definitions can receive its once-per-regression shortcut.
    runtime_hash = establishment_runtime(source_ref, [], versions)
    unchanged = unchanged_establishment_cases(source_ref, cases)
    if rows and not call_rows:
        raise ValueError("Native subscription call ledger is required for native rows")
    calls = subscription_call_state(call_rows)[1] if call_rows else {}
    established = []
    original_attempts_present = False
    for case in unchanged:
        fingerprint = evals.case_fingerprint(case, judge_model=evals.DEFAULT_JUDGE_MODEL, runtime_hash=runtime_hash)
        latest = evals.latest_repetition_rows(rows, case["id"], fingerprint)
        original_attempts_present |= bool(latest)
        if (len(latest) >= evals.case_required_passes(case)
                and all(row.get("status") == "pass" for row in latest.values())):
            established.append(case)
    records = project_case_records(rows, calls, established, runtime_hash=runtime_hash, now=now)
    if not records and not original_attempts_present:
        # Published receipts already retain the original source, native call
        # identities, usage, times and evidence digests. Reuse that verified
        # establishment after private raw artifacts expire; never manufacture
        # replacement actor rows or relabel their historical source.
        from scripts import agent_eval_source_compatibility as source
        path = "ci/agent-evals/qualification.json"
        if source._git("ls-tree", "--name-only", source_ref, "--", path, root=evals.REPO_ROOT).strip():
            published = json.loads(source._git("show", f"{source_ref}:{path}", root=evals.REPO_ROOT))
            if (published.get("sdk_versions") != versions or published.get("funding") != "chatgpt-plan"
                    or published.get("api_spend_usd") != 0 or published.get("invalidated")):
                raise ValueError("Published establishment SDK, funding or validity mismatch")
            # A first published qualification has its original full repetitions
            # in cases, before any later receipt adds an establishment section.
            # Preserve those verified native records without recreating journals.
            if published.get("runtime_fingerprint") == runtime_hash:
                native = {case["id"]: published.get("cases", {}).get(case["id"]) for case in unchanged}
                native = {key: value for key, value in native.items() if isinstance(value, dict)
                          and isinstance(value.get("repetitions"), list)
                          and len(value["repetitions"]) >= evals.case_required_passes(
                              next(case for case in unchanged if case["id"] == key))}
                if native:
                    proof = {"source_commit": source_ref, "runtime_fingerprint": runtime_hash, "cases": native}
                    validate_establishment(proof, cases, versions=versions, now=now)
                    return proof
            prior = published.get("establishment")
            if prior:
                if published.get("sdk_versions") != versions or published.get("funding") != "chatgpt-plan":
                    raise ValueError("Published establishment SDK or funding mismatch")
                compatible = {case["id"] for case in unchanged_establishment_cases(prior["source_commit"], cases)}
                proof = {**prior, "cases": {key: value for key, value in prior["cases"].items()
                                            if key in compatible}}
                validate_establishment(proof, cases, versions=versions, now=now)
                return proof
    proof = {"source_commit": source_ref, "runtime_fingerprint": runtime_hash, "cases": records}
    validate_establishment(proof, cases, versions=versions, now=now)
    return proof


def unchanged_establishment_cases(source_ref, cases):
    from scripts import agent_eval_source_compatibility as source

    paths = {Path(case["_path"]).relative_to(evals.REPO_ROOT).as_posix(): case for case in cases}
    tracked = set(source._git("ls-tree", "-r", "--name-only", "-z", source_ref,
                              root=evals.REPO_ROOT).decode().split("\0"))
    originals = source._blobs(source_ref, sorted(set(paths) & tracked), root=evals.REPO_ROOT)
    return [case for path, case in paths.items() if path in originals
            and json.loads(originals[path]) == {key: value for key, value in case.items() if not key.startswith("_")}]


def validate_establishment(proof, cases, *, versions, now):
    if not isinstance(proof, dict) or set(proof) != {"source_commit", "runtime_fingerprint", "cases"}:
        raise ValueError("Invalid original establishment evidence")
    if not isinstance(proof["cases"], dict):
        raise ValueError("Original establishment source/SDK fingerprint mismatch")
    by_id = {case["id"]: case for case in cases}
    if set(proof["cases"]) - set(by_id):
        raise ValueError("Unknown established case")
    runtime_hash = establishment_runtime(proof["source_commit"],
        [by_id[case_id] for case_id in proof["cases"]], versions)
    if proof["runtime_fingerprint"] != runtime_hash:
        raise ValueError("Original establishment source/SDK fingerprint mismatch")
    for case_id, record in proof["cases"].items():
        validate_record(record, by_id[case_id], runtime_hash=runtime_hash, now=now)


def regression_required(case, establishment, rows, runtime_hash):
    """A genuine current failure requires stability; unchanged established cases run once."""
    fingerprint = evals.case_fingerprint(case, judge_model=evals.DEFAULT_JUDGE_MODEL, runtime_hash=runtime_hash)
    failed = any(row.get("case_id") == case["id"] and row.get("fingerprint") == fingerprint
                 and row.get("status") == "fail" for row in rows)
    if establishment and case["id"] in establishment["cases"] and not failed:
        return 1
    return evals.case_required_passes(case)


def project_case_records(rows, calls, cases, *, runtime_hash, now, required_counts=None):
    required_counts = required_counts or {}
    records = {}
    for case in cases:
        fingerprint = evals.case_fingerprint(case, judge_model=evals.DEFAULT_JUDGE_MODEL, runtime_hash=runtime_hash)
        latest = evals.latest_repetition_rows(rows, case["id"], fingerprint)
        if len(latest) < required_counts.get(case["id"], evals.case_required_passes(case)) or any(row.get("status") != "pass" for row in latest.values()):
            raise ValueError(f"Unqualified or failed case: {case['id']}")
        repetitions = []
        for rep, row in sorted(latest.items()):
            attempt = row.get("run_id")
            started = timestamp(row.get("started_at"), now=now)
            for call in calls.values():
                scope = call.get("scope") or {}
                if (scope.get("case_id") == case["id"] and scope.get("repetition") == rep
                        and scope.get("fingerprint") == fingerprint and scope.get("run_id") != attempt
                        and timestamp(call.get("timestamp"), now=now, require_fresh=False) >= started):
                    raise ValueError("A newer incomplete native attempt supersedes the prior pass")
            bound_calls = [call for call in calls.values() if (call.get("scope") or {}).get("run_id") == attempt]
            # A transport-failed judge may have unknown usage. Retain its native
            # reservation in the full cumulative journal, while qualifying only
            # a later completed judge over this same completed actor attempt.
            settled_judges = [call for call in bound_calls if call.get("kind") == "settle"
                              and (call.get("scope") or {}).get("role") == "judge"]
            expected_judge_scope = {key: row[key] for key in ("case_id", "repetition", "run_id", "fingerprint")} | {"role": "judge"}
            earlier_unknown_judges = {
                call["id"] for call in bound_calls if call.get("kind") == "reserve"
                and call.get("scope") == expected_judge_scope
                and call.get("model") == evals.DEFAULT_JUDGE_MODEL
                and any(timestamp(call["timestamp"], now=now) < timestamp(judge["timestamp"], now=now)
                        for judge in settled_judges)
            }
            bound_calls = [call for call in bound_calls if call["id"] not in earlier_unknown_judges]
            projected = {key: row.get(key) for key in ("case_id", "repetition", "run_id", "fingerprint", "started_at",
                                                       "timestamp", "status", "acting_model", "judge_model")}
            projected.update(machine_pass=(row.get("machine") or {}).get("pass"),
                             judge_pass=(row.get("judge") or {}).get("pass"), native_evidence_digest=digest(row))
            projected["calls"] = [{key: call.get(key) for key in (
                "id", "kind", "scope", "model", "nano_usd", "reserved_nano_usd", "usage", "timestamp", "settled_at"
            )} for call in bound_calls]
            repetitions.append(projected)
        records[case["id"]] = {"fingerprint": fingerprint, "repetitions": repetitions}
    return records


def build_receipt(rows, call_rows, cases, *, runtime_hash, versions, calibration_rows=None,
                  calibration_summary=None, now=None, established_source=None, scope_case_ids=None):
    """Receipt for the whole catalog, or with ``scope_case_ids`` for the release scope's cases only."""
    now = now or evals.utc_now()
    scoped = scope_case_ids is not None
    if scoped:
        wanted = list(scope_case_ids)
        cases = [case for case in cases if case["id"] in set(wanted)]
        if {case["id"] for case in cases} != set(wanted):
            raise ValueError("A scoped case is not in the case catalog")
        if not load_calibration_catalog(cases, scoped=True) and (calibration_rows is None or calibration_summary is None):
            calibration_rows, calibration_summary = [], {}
    if calibration_rows is None or calibration_summary is None:
        raise ValueError("Native judge calibration is a required release qualification")
    if not call_rows or call_rows[0].get("funding") != "chatgpt-plan" or call_rows[0].get("version") != 2:
        raise ValueError("Native subscription call ledger is required")
    cap, calls = subscription_call_state(call_rows)
    if type(cap) is not int or cap <= 0 or len(calls) > cap:
        raise ValueError("Native model-call allowance is invalid or exceeded")
    establishment = build_establishment(rows, call_rows, cases, source_ref=established_source,
                                        versions=versions, now=now) if established_source else None
    if establishment:
        # A current genuine failure revokes the once-per-regression shortcut.
        # Keep the original rows in the native journal; require current stability.
        establishment["cases"] = {case["id"]: establishment["cases"][case["id"]]
            for case in cases if case["id"] in establishment["cases"]
            and regression_required(case, establishment, rows, runtime_hash) == 1}
    required_counts = {case["id"]: regression_required(case, establishment, rows, runtime_hash) for case in cases}
    records = project_case_records(rows, calls, cases, runtime_hash=runtime_hash, now=now,
                                   required_counts=required_counts)
    receipt = {"kind": KIND, "version": VERSION, "funding": "chatgpt-plan", "api_spend_usd": 0,
               "runtime_fingerprint": runtime_hash, "sdk_versions": versions, "cases": records,
               "calibration": build_calibration(calibration_rows, calibration_summary, calls, cases,
                                                runtime_hash=runtime_hash, now=now, scoped=scoped),
               "call_allowance": {"max_calls": cap, "consumed_calls": len(calls)},
               "native_ledger_digests": {"repetitions": digest(rows), "model_calls": digest(call_rows),
                                         "calibration": digest(calibration_rows), "calibration_summary": digest(calibration_summary)}}
    if establishment:
        receipt["establishment"] = establishment
    if scoped:
        receipt["scope"] = {"case_ids": sorted(scope_case_ids)}
    verify_receipt(receipt, cases, runtime_hash=runtime_hash, versions=versions, now=now)
    return receipt


def verify_receipt(receipt, cases, *, runtime_hash, versions, now=None):
    now = now or evals.utc_now()
    expected_keys = {"kind", "version", "funding", "api_spend_usd", "runtime_fingerprint", "sdk_versions", "cases",
                     "call_allowance", "native_ledger_digests", "calibration"}
    if not isinstance(receipt, dict) or set(receipt) - {"establishment", "scope"} != expected_keys:
        raise ValueError("Missing or unsupported native qualification receipt")
    # A scoped receipt (release scope) covers exactly its listed cases and their own controls.
    scoped = "scope" in receipt
    if scoped and (not isinstance(receipt["scope"], dict) or set(receipt["scope"]) != {"case_ids"}
                   or receipt["scope"]["case_ids"] != sorted(case["id"] for case in cases)):
        raise ValueError("Scoped qualification does not match the listed cases")
    if (receipt["kind"] != KIND or receipt["version"] != VERSION or receipt["funding"] != "chatgpt-plan"
            or receipt["api_spend_usd"] != 0 or receipt["runtime_fingerprint"] != runtime_hash
            or receipt["sdk_versions"] != versions or "missing" in versions.values()):
        raise ValueError("Qualification does not match the current source, SDKs, or subscription funding")
    records = receipt["cases"]
    if not isinstance(records, dict) or set(records) != {case["id"] for case in cases}:
        raise ValueError("Qualification does not cover the entire current case catalog")
    allowance = receipt["call_allowance"]
    if (not isinstance(allowance, dict) or set(allowance) != {"max_calls", "consumed_calls"}
            or any(type(n) is not int for n in allowance.values())
            or not 0 < allowance["consumed_calls"] <= allowance["max_calls"]):
        raise ValueError("Invalid cumulative subscription call allowance")
    hashes = receipt["native_ledger_digests"]
    if not isinstance(hashes, dict) or set(hashes) != {"repetitions", "model_calls", "calibration", "calibration_summary"} or any(
        not isinstance(h, str) or len(h) != 64 or any(c not in "0123456789abcdef" for c in h) for h in hashes.values()
    ):
        raise ValueError("Missing native ledger digests")
    seen_attempts, seen_calls = set(), set()
    establishment = receipt.get("establishment")
    if establishment:
        validate_establishment(establishment, cases, versions=versions, now=now)
        for case in cases:
            prior = establishment["cases"].get(case["id"])
            if prior:
                attempts, calls = validate_record(prior, case, runtime_hash=establishment["runtime_fingerprint"], now=now)
                if seen_attempts & attempts or seen_calls & calls:
                    raise ValueError("Establishment reused a native attempt or call")
                seen_attempts.update(attempts)
                seen_calls.update(calls)
    historical_calls = set(seen_calls)
    for case in cases:
        record = records[case["id"]]
        if not isinstance(record, dict) or set(record) != {"fingerprint", "repetitions"}:
            raise ValueError("Invalid per-case qualification record")
        attempts, calls = validate_record(record, case, runtime_hash=runtime_hash, now=now,
            required=1 if establishment and case["id"] in establishment["cases"] else None)
        if seen_attempts & attempts or seen_calls & calls:
            raise ValueError("Another case reused a native attempt or model call")
        seen_attempts.update(attempts)
        seen_calls.update(calls)
    controls = load_calibration_catalog(cases, scoped=scoped)
    proof = receipt["calibration"]
    if (not isinstance(proof, dict) or set(proof) != {"catalog_digest", "actor_runs", "external_writes", "creates_case_freshness", "controls"}
            or proof["catalog_digest"] != digest(controls) or type(proof["actor_runs"]) is not int or proof["actor_runs"] != 0
            or type(proof["external_writes"]) is not int or proof["external_writes"] != 0
            or proof["creates_case_freshness"] is not False or not isinstance(proof["controls"], dict)
            or set(proof["controls"]) != {control["id"] for control in controls}):
        raise ValueError("Qualification does not cover the complete maintained judge calibration catalog")
    by_case = {case["id"]: case for case in cases}
    for control in controls:
        attempts, calls = validate_calibration_record(proof["controls"][control["id"]], control,
                            by_case[control["case_id"]], runtime_hash=runtime_hash, now=now)
        if seen_attempts & attempts or seen_calls & calls:
            raise ValueError("Calibration reused an actor attempt or another model call")
        # One calibration batch legitimately shares its attempt id across
        # controls/repetitions, while every call is independently scoped.
        seen_calls.update(calls)
    # Establishment carries independently verified original call identities.
    # The current journal need not recreate those expired historical rows.
    # Retain global duplicate detection above, and bind this journal's usage
    # to current actor/calibration calls only.
    if len(seen_calls - historical_calls) > allowance["consumed_calls"]:
        raise ValueError("Qualified calls exceed the cumulative native ledger")


def package_version(root=None):
    import re

    text = ((root or evals.REPO_ROOT) / "setup.py").read_text()
    match = re.search(r"""version=["']([^"']+)["']""", text)
    if not match:
        raise ValueError("setup.py has no version")
    return match.group(1)


def verify_release_scope(scope, cases, receipt, *, runtime_hash, version, now=None):
    """Verify the author-written eval scope; return the verified case ids.

    The scope names the package version it was written for, the cases the author judged
    could be affected (each with a reason), and, when that list is empty, why. Every
    listed case needs current passing actor/judge evidence in the receipt. Nothing here
    selects, adds or drops cases.
    """
    now = now or evals.utc_now()
    allowed = {"version", "package_version", "cases", "no_cases_reason"}
    if not isinstance(scope, dict) or not {"version", "package_version", "cases"} <= set(scope) <= allowed:
        raise ValueError("Invalid release eval scope; expected version, package_version, cases and optional no_cases_reason")
    if scope["version"] != SCOPE_VERSION:
        raise ValueError("Unsupported release eval scope version")
    if scope["package_version"] != version:
        raise ValueError(f"Release eval scope was written for {scope['package_version']!r}, not {version!r}; "
                         "the release author must write the scope for this version")
    listed = scope["cases"]
    if not isinstance(listed, list):
        raise ValueError("Release eval scope cases must be a list")
    if not listed:
        reason = scope.get("no_cases_reason")
        if not isinstance(reason, str) or not reason.strip():
            raise ValueError("An empty release eval scope needs no_cases_reason")
        return []
    by_id = {case["id"]: case for case in cases}
    ids = []
    for entry in listed:
        if (not isinstance(entry, dict) or set(entry) != {"id", "reason"}
                or not isinstance(entry["reason"], str) or not entry["reason"].strip()):
            raise ValueError("Each scoped case needs an id and a reason")
        if entry["id"] not in by_id:
            raise ValueError(f"Scoped case {entry['id']!r} is not in the case catalog")
        if entry["id"] in ids:
            raise ValueError(f"Scoped case {entry['id']!r} is listed twice")
        ids.append(entry["id"])
    records = receipt.get("cases") if isinstance(receipt, dict) else None
    establishment = receipt.get("establishment") if isinstance(receipt, dict) else None
    if establishment is not None:
        validate_establishment(establishment, cases, versions=evals.sdk_versions(), now=now)
    for case_id in ids:
        record = records.get(case_id) if isinstance(records, dict) else None
        if not isinstance(record, dict):
            raise ValueError(f"Scoped case {case_id!r} has no pass evidence in the receipt")
        required = 1 if establishment and case_id in establishment["cases"] else None
        validate_record(record, by_id[case_id], runtime_hash=runtime_hash, now=now, required=required)
    return ids


def read_ledger(path):
    rows = [json.loads(line) for line in path.read_text().splitlines() if line.strip()]
    if any(not isinstance(row, dict) for row in rows):
        raise ValueError("Invalid native ledger row")
    return rows


def invalidate_receipt(path, fingerprints, *, runtime_hash):
    """A new matching attempt revokes the prior export before any inference.

    The old evidence remains in Git/native ledgers. Interrupted or failed attempts
    must not leave that file publishable; only a complete reviewed export clears it.
    """
    if not path.exists():
        return
    receipt = json.loads(path.read_text())
    if not isinstance(receipt, dict) or receipt.get("runtime_fingerprint") != runtime_hash:
        return
    records = receipt.get("cases", {})
    affected = sorted(case_id for case_id, fingerprint in fingerprints.items()
                      if isinstance(records.get(case_id), dict) and records[case_id].get("fingerprint") == fingerprint)
    controls = (receipt.get("calibration") or {}).get("controls", {})
    affected_controls = sorted(key for key, fingerprint in fingerprints.items()
                               if isinstance(controls.get(key), dict) and controls[key].get("fingerprint") == fingerprint)
    if affected or affected_controls:
        receipt["invalidated"] = {"case_ids": affected, "calibration_control_ids": affected_controls, "timestamp": evals.utc_text()}
        evals.write_json_atomic(path, receipt)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("create", "verify"))
    parser.add_argument("--output-root", type=Path)
    parser.add_argument("--calibration-output-root", type=Path)
    parser.add_argument("--model-call-ledger", type=Path)
    parser.add_argument("--receipt", type=Path, default=DEFAULT_PATH)
    parser.add_argument("--established-source", help="Immutable earlier source with original established native passes; every case still needs current regression proof")
    args = parser.parse_args()
    cases, runtime_hash, versions = evals.load_cases(), evals.runtime_fingerprint(), evals.sdk_versions()
    if args.mode == "create":
        scope_ids = None
        if SCOPE_PATH.exists():
            scope = json.loads(SCOPE_PATH.read_text())
            if scope.get("package_version") == package_version() and scope.get("cases"):
                scope_ids = [entry["id"] for entry in scope["cases"]]
        if args.output_root is None or args.model_call_ledger is None or (
                args.calibration_output_root is None and scope_ids is None):
            parser.error("create requires actor/calibration output roots and the shared --model-call-ledger")
        invalidate_receipt(args.receipt, {
            case["id"]: evals.case_fingerprint(case, judge_model=evals.DEFAULT_JUDGE_MODEL, runtime_hash=runtime_hash)
            for case in cases
        }, runtime_hash=runtime_hash)
        receipt = build_receipt(read_ledger(args.output_root / "ledger.jsonl"),
                                read_ledger(args.model_call_ledger), cases,
                                runtime_hash=runtime_hash, versions=versions,
                                calibration_rows=(read_ledger(args.calibration_output_root / "ledger.jsonl")
                                                  if args.calibration_output_root else None),
                                calibration_summary=(json.loads((args.calibration_output_root / "summary.json").read_text())
                                                     if args.calibration_output_root else None),
                                established_source=args.established_source, scope_case_ids=scope_ids)
        evals.write_json_atomic(args.receipt, receipt)
    elif SCOPE_PATH.exists():
        receipt = json.loads(args.receipt.read_text()) if args.receipt.exists() else None
        ids = verify_release_scope(json.loads(SCOPE_PATH.read_text()), cases, receipt,
                                   runtime_hash=runtime_hash, version=package_version())
        print(f"Verified the author-written release eval scope: {len(ids)} listed case(s) with current pass evidence"
              + ("" if ids else " (none listed; reason recorded in release-scope.json)") + ".")
        return 0
    else:
        receipt_bytes = args.receipt.read_bytes()
        receipt = json.loads(receipt_bytes)
        evidence_hash = runtime_hash
        anchor = evals.REPO_ROOT / "ci/agent-evals/source-compatibility.json"
        if receipt.get("runtime_fingerprint") != runtime_hash and anchor.exists():
            from scripts.agent_eval_source_compatibility import compatible_runtime

            evidence_hash = compatible_runtime(receipt, receipt_bytes, json.loads(anchor.read_text()))
            print(f"Preserving native inference at original source {evidence_hash}; candidate {runtime_hash} is source-compatible. No inference timestamps or usage were rewritten.")
        verify_receipt(receipt, cases, runtime_hash=evidence_hash, versions=versions)
    print(f"Verified {len(cases)} real-model actor/judge qualifications and complete judge calibration for the candidate and SDKs.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, KeyError, TypeError, RuntimeError) as exc:
        print(f"Real-model qualification blocked: {exc}. Run the authenticated local evals, review evidence, then create the receipt.", file=sys.stderr)
        raise SystemExit(1) from exc
