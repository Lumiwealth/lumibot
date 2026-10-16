"""Qualify rubric changes with real judges and labeled, non-agent-run controls.

These are judge calibration fixtures, not fabricated agent evals. They never
create case freshness or replace the real actor repetitions in the release gate.
"""

import argparse
import concurrent.futures
import hashlib
import json
import sys
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import run_agent_evals as evals
from scripts.agent_eval_isolation import configure_fixture_environment
from scripts.agent_eval_subscription_budget import SubscriptionCallBudget

REQUIRED_REPETITIONS = 3


def control_fingerprint(item, case, runtime_hash):
    contract = {
        "control": item,
        "case": evals.case_fingerprint(case, judge_model=evals.DEFAULT_JUDGE_MODEL, runtime_hash=runtime_hash),
        "runner": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    }
    return hashlib.sha256(evals.stable_json(contract).encode()).hexdigest()


def control_repetitions(item):
    repetitions = item.get("required_repetitions", REQUIRED_REPETITIONS)
    if type(repetitions) is not int or repetitions not in {3, 5}:
        raise ValueError("Calibration repetitions must be 3 or 5")
    return repetitions


def latest_control_rows(rows, fingerprints, required_repetitions=None):
    latest = {}
    required_repetitions = required_repetitions or {}
    for row in rows:
        control_id, repetition = row.get("id"), row.get("repetition")
        if (control_id in fingerprints and row.get("fingerprint") == fingerprints[control_id]
                and type(repetition) is int
                and 1 <= repetition <= required_repetitions.get(control_id, REQUIRED_REPETITIONS)):
            latest[(control_id, repetition)] = row
    return latest


def control_passed(row):
    if row.get("status") != "pass" or row.get("passed") is not True:
        return False
    try:
        timestamp = evals.datetime.fromisoformat(str(row["timestamp"]).replace("Z", "+00:00"))
        uuid.UUID(row["run_id"])
    except (KeyError, TypeError, ValueError, AttributeError):
        return False
    now = evals.utc_now()
    return timestamp.tzinfo is not None and now - evals.timedelta(days=evals.DEFAULT_FRESHNESS_DAYS) <= timestamp <= now


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-cost-usd", type=float, default=0)
    parser.add_argument("--max-model-calls", type=int, default=500)
    parser.add_argument("--model-call-ledger", type=Path, required=True,
                        help="Existing shared release-attempt subscription ledger; never a fresh per-output cap")
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--qualification-receipt", type=Path,
                        help="Existing release export to invalidate before a new control attempt")
    parser.add_argument("--case-id", action="append", default=[])
    args = parser.parse_args()
    if args.max_cost_usd != 0:
        parser.error("Calibration API spend must be zero")
    call_ledger = args.model_call_ledger.resolve()
    if not call_ledger.is_file() or not call_ledger.stat().st_size:
        parser.error("--model-call-ledger must select the existing release-attempt ledger")
    budget = SubscriptionCallBudget(call_ledger, max_calls=args.max_model_calls)
    output_root = args.output_root.resolve()
    configure_fixture_environment(evals.REPO_ROOT, artifact_root=output_root)
    fixtures = json.loads((evals.REPO_ROOT / "tests/fixtures/agent_judge_calibration.json").read_text())
    if (not fixtures or len({item["id"] for item in fixtures}) != len(fixtures)
            or any(type(item["expected"]) is not bool for item in fixtures)):
        parser.error("Calibration requires unique labeled control ids")
    if args.case_id:
        known = {item["case_id"] for item in fixtures}
        if set(args.case_id) - known:
            parser.error("Unknown calibration case")
        fixtures = [item for item in fixtures if item["case_id"] in args.case_id]
    cases = {case["id"]: case for case in evals.load_cases({item["case_id"] for item in fixtures})}
    runtime_hash = evals.runtime_fingerprint()
    fingerprints = {item["id"]: control_fingerprint(item, cases[item["case_id"]], runtime_hash) for item in fixtures}
    output_root.mkdir(parents=True, exist_ok=True)
    ledger_path = output_root / "ledger.jsonl"
    existing = evals.read_jsonl(ledger_path)
    with budget._ledger() as (_, native_rows):
        recovered = evals.recover_native_incomplete_attempts(
            [{**row, "case_id": row.get("id")} for row in existing], native_rows, fingerprints
        )
    controls = {item["id"]: item for item in fixtures}
    for row in recovered:
        control_id = row["case_id"]
        row.update(id=control_id, case_id=controls[control_id]["case_id"], passed=False,
                   expected=controls[control_id]["expected"], runtime_fingerprint=runtime_hash,
                   judge_model=evals.DEFAULT_JUDGE_MODEL)
        evals.append_jsonl(ledger_path, row)
    repetitions = {item["id"]: control_repetitions(item) for item in fixtures}
    latest = latest_control_rows(existing + recovered, fingerprints, repetitions)
    required = [(item["id"], repetition) for item in fixtures for repetition in range(1, repetitions[item["id"]] + 1)]
    work = [(item, repetition) for item in fixtures for repetition in range(1, repetitions[item["id"]] + 1)
            if not control_passed(latest.get((item["id"], repetition), {}))]
    resumed = len(required) - len(work)
    run_id = str(uuid.uuid4())
    if work:
        from scripts.qualify_agent_evals import invalidate_receipt

        receipt = args.qualification_receipt or evals.REPO_ROOT / "ci/agent-evals/qualification.json"
        invalidate_receipt(receipt, {item["id"]: fingerprints[item["id"]] for item, _ in work},
                           runtime_hash=runtime_hash)
        evals.select_eval_credentials({evals.DEFAULT_JUDGE_MODEL})
        evals.preflight(list(cases.values()), evals.DEFAULT_JUDGE_MODEL, args.max_cost_usd)

    def check(item, repetition):
        fingerprint = fingerprints[item["id"]]
        row = {"id": item["id"], "case_id": item["case_id"], "repetition": repetition,
               "run_id": run_id, "fingerprint": fingerprint, "runtime_fingerprint": runtime_hash,
               "expected": item["expected"], "judge_model": evals.DEFAULT_JUDGE_MODEL}
        row.update(started_at=evals.utc_text(), timestamp=evals.utc_text(), status="incomplete", passed=False)
        evals.append_jsonl(ledger_path, row)
        try:
            judge, result, seconds = evals.run_judge(
                cases[item["case_id"]], item["transcript"], evals.DEFAULT_JUDGE_MODEL,
                budget.for_scope(item["id"], repetition, "judge_calibration",
                                 run_id=run_id, fingerprint=fingerprint),
            )
            passed = judge["pass"] is item["expected"]
            row.update(status="pass" if passed else "fail", passed=passed, judge=judge,
                       seconds=seconds, usage=result.usage)
        except Exception as exc:
            failure = evals.harness_error_row(item["id"], repetition, fingerprint, exc)
            row.update(status="error", passed=False, **{key: failure[key] for key in
                       ("error", "error_code", "http_status", "error_location")})
        row["timestamp"] = evals.utc_text()
        # Persist each completed result/error in its worker, even if collecting
        # another future fails or the parent is interrupted between results.
        evals.append_jsonl(ledger_path, row)
        return row

    rows = []

    def write_summary():
        selected = [latest.get(key, {}) for key in required]
        summary = {"kind": "judge-calibration", "run_id": run_id,
                   "runtime_fingerprint": runtime_hash, "judge_model": evals.DEFAULT_JUDGE_MODEL,
                   "passed": sum(control_passed(row) for row in selected),
                   "failed": sum(row.get("status") == "fail" for row in selected),
                   "errors": sum(row.get("status") == "error" for row in selected),
                   "missing": sum(not row or row.get("status") not in {"pass", "fail", "error"}
                                  or row.get("status") == "pass" and not control_passed(row) for row in selected),
                   "resumed": resumed, "scheduled": len(work), "completed": len(rows),
                   "budget": budget.snapshot(), "model_call_ledger_path": str(call_ledger),
                   "actor_runs": 0, "external_writes": 0, "creates_case_freshness": False}
        evals.write_json_atomic(output_root / "summary.json", summary)
        return summary

    summary = write_summary()
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        futures = [pool.submit(check, item, repetition) for item, repetition in work]
        for future in concurrent.futures.as_completed(futures):
            row = future.result()
            rows.append(row)
            latest[(row["id"], row["repetition"])] = row
            summary = write_summary()
    print(json.dumps(summary))
    return int(summary["passed"] != len(required))


if __name__ == "__main__":
    raise SystemExit(main())
