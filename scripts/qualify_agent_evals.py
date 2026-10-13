#!/usr/bin/env python3
"""Export and verify source-bound evidence of authenticated real actor/judge runs.

The release owner reviews native private artifacts before committing this compact,
public-safe receipt. Digests bind evidence; they are not proof of inference or a
substitute for trusted review. CI requires the entire current catalog and SDK tuple,
without copying a developer's rotating ChatGPT login into a hosted worker.
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
from scripts import run_agent_evals as evals
from scripts.agent_eval_call_budget import EvalCallBudget

KIND = "lumibot-real-model-qualification"
VERSION = 1
DEFAULT_PATH = evals.REPO_ROOT / "ci/agent-evals/qualification.json"


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


def validate_record(record, case, *, runtime_hash, now):
    acting_model = str(case.get("model") or evals.DEFAULT_ACTING_MODEL)
    judge_model = evals.DEFAULT_JUDGE_MODEL
    fingerprint = evals.case_fingerprint(case, judge_model=judge_model, runtime_hash=runtime_hash)
    if not acting_model.startswith("chatgpt/") or record.get("fingerprint") != fingerprint:
        raise ValueError("Qualification source/model fingerprint mismatch")
    rows = record.get("repetitions")
    if not isinstance(rows, list) or len(rows) < evals.NEW_CASE_REQUIRED_PASSES:
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


def build_receipt(rows, call_rows, cases, *, runtime_hash, versions, now=None):
    now = now or evals.utc_now()
    if not call_rows or call_rows[0].get("funding") != "chatgpt-plan" or call_rows[0].get("version") != 2:
        raise ValueError("Native subscription call ledger is required")
    header = call_rows[0]
    cap = header.get("max_calls")
    calls = EvalCallBudget._calls(call_rows[1:])
    if type(cap) is not int or not 0 < cap <= 500 or len(calls) > cap:
        raise ValueError("Native model-call allowance is invalid or exceeded")
    records = {}
    for case in cases:
        fingerprint = evals.case_fingerprint(case, judge_model=evals.DEFAULT_JUDGE_MODEL, runtime_hash=runtime_hash)
        latest = evals.latest_repetition_rows(rows, case["id"], fingerprint)
        if len(latest) < evals.NEW_CASE_REQUIRED_PASSES or any(row.get("status") != "pass" for row in latest.values()):
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
            projected = {key: row.get(key) for key in ("case_id", "repetition", "run_id", "fingerprint", "started_at",
                                                       "timestamp", "status", "acting_model", "judge_model")}
            projected.update(machine_pass=(row.get("machine") or {}).get("pass"),
                             judge_pass=(row.get("judge") or {}).get("pass"), native_evidence_digest=digest(row))
            projected["calls"] = [{key: call.get(key) for key in (
                "id", "kind", "scope", "model", "nano_usd", "reserved_nano_usd", "usage", "timestamp", "settled_at"
            )} for call in bound_calls]
            repetitions.append(projected)
        records[case["id"]] = {"fingerprint": fingerprint, "repetitions": repetitions}
    receipt = {"kind": KIND, "version": VERSION, "funding": "chatgpt-plan", "api_spend_usd": 0,
               "runtime_fingerprint": runtime_hash, "sdk_versions": versions, "cases": records,
               "call_allowance": {"max_calls": cap, "consumed_calls": len(calls)},
               "native_ledger_digests": {"repetitions": digest(rows), "model_calls": digest(call_rows)}}
    verify_receipt(receipt, cases, runtime_hash=runtime_hash, versions=versions, now=now)
    return receipt


def verify_receipt(receipt, cases, *, runtime_hash, versions, now=None):
    now = now or evals.utc_now()
    expected_keys = {"kind", "version", "funding", "api_spend_usd", "runtime_fingerprint", "sdk_versions", "cases",
                     "call_allowance", "native_ledger_digests"}
    if not isinstance(receipt, dict) or set(receipt) != expected_keys:
        raise ValueError("Missing or unsupported native qualification receipt")
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
            or not 0 < allowance["consumed_calls"] <= allowance["max_calls"] <= 500):
        raise ValueError("Invalid cumulative subscription call allowance")
    hashes = receipt["native_ledger_digests"]
    if not isinstance(hashes, dict) or set(hashes) != {"repetitions", "model_calls"} or any(
        not isinstance(h, str) or len(h) != 64 or any(c not in "0123456789abcdef" for c in h) for h in hashes.values()
    ):
        raise ValueError("Missing native ledger digests")
    seen_attempts, seen_calls = set(), set()
    for case in cases:
        record = records[case["id"]]
        if not isinstance(record, dict) or set(record) != {"fingerprint", "repetitions"}:
            raise ValueError("Invalid per-case qualification record")
        attempts, calls = validate_record(record, case, runtime_hash=runtime_hash, now=now)
        if seen_attempts & attempts or seen_calls & calls:
            raise ValueError("Another case reused a native attempt or model call")
        seen_attempts.update(attempts)
        seen_calls.update(calls)
    if len(seen_calls) > allowance["consumed_calls"]:
        raise ValueError("Qualified calls exceed the cumulative native ledger")


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
    if affected:
        receipt["invalidated"] = {"case_ids": affected, "timestamp": evals.utc_text()}
        evals.write_json_atomic(path, receipt)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("create", "verify"))
    parser.add_argument("--output-root", type=Path)
    parser.add_argument("--receipt", type=Path, default=DEFAULT_PATH)
    args = parser.parse_args()
    cases, runtime_hash, versions = evals.load_cases(), evals.runtime_fingerprint(), evals.sdk_versions()
    if args.mode == "create":
        if args.output_root is None:
            parser.error("create requires --output-root with the native ledgers")
        invalidate_receipt(args.receipt, {
            case["id"]: evals.case_fingerprint(case, judge_model=evals.DEFAULT_JUDGE_MODEL, runtime_hash=runtime_hash)
            for case in cases
        }, runtime_hash=runtime_hash)
        receipt = build_receipt(read_ledger(args.output_root / "ledger.jsonl"),
                                read_ledger(args.output_root / "model_calls.jsonl"), cases,
                                runtime_hash=runtime_hash, versions=versions)
        evals.write_json_atomic(args.receipt, receipt)
    else:
        verify_receipt(json.loads(args.receipt.read_text()), cases, runtime_hash=runtime_hash, versions=versions)
    print(f"Verified {len(cases)} real-model actor/judge qualifications for the current source and SDKs.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, ValueError, KeyError, TypeError) as exc:
        print(f"Real-model qualification blocked: {exc}. Run the authenticated local evals, review evidence, then create the receipt.", file=sys.stderr)
        raise SystemExit(1) from exc
