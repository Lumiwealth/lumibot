"""Qualify rubric changes with real judges and labeled, non-agent-run controls.

These are judge calibration fixtures, not fabricated agent evals. They never
create case freshness or replace the real actor repetitions in the release gate.
"""

import argparse
import concurrent.futures
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import run_agent_evals as evals
from scripts.agent_eval_call_budget import EvalCallBudget
from scripts.agent_eval_isolation import configure_fixture_environment


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-cost-usd", type=float, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--case-id", action="append", default=[])
    args = parser.parse_args()
    configure_fixture_environment(evals.REPO_ROOT)
    fixtures = json.loads((evals.REPO_ROOT / "tests/fixtures/agent_judge_calibration.json").read_text())
    if args.case_id:
        known = {item["case_id"] for item in fixtures}
        if set(args.case_id) - known:
            parser.error("Unknown calibration case")
        fixtures = [item for item in fixtures if item["case_id"] in args.case_id]
    cases = {case["id"]: case for case in evals.load_cases({item["case_id"] for item in fixtures})}
    evals.select_eval_credentials({evals.DEFAULT_JUDGE_MODEL})
    evals.preflight(list(cases.values()), evals.DEFAULT_JUDGE_MODEL, args.max_cost_usd)
    args.output_root.mkdir(parents=True, exist_ok=True)
    budget = EvalCallBudget(
        args.output_root / "model_calls.jsonl", cap_usd=args.max_cost_usd,
        prices=evals.MODEL_PRICES_PER_MILLION, max_input_tokens=evals.MAX_INPUT_TOKENS_PER_MODEL_CALL,
    )

    def check(item, repetition):
        judge, result, seconds = evals.run_judge(
            cases[item["case_id"]], item["transcript"], evals.DEFAULT_JUDGE_MODEL,
            budget.for_scope(item["id"], repetition, "judge_calibration"),
        )
        return {"id": item["id"], "repetition": repetition, "expected": item["expected"],
                "passed": judge["pass"] is item["expected"], "judge": judge,
                "seconds": seconds, "usage": result.usage}

    rows = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
        futures = [pool.submit(check, item, rep) for item in fixtures for rep in range(1, 4)]
        for future in concurrent.futures.as_completed(futures):
            row = future.result()
            evals.append_jsonl(args.output_root / "ledger.jsonl", row)
            rows.append(row)
    summary = {"kind": "judge-calibration", "passed": sum(row["passed"] for row in rows),
               "failed": sum(not row["passed"] for row in rows), "budget": budget.snapshot(),
               "actor_runs": 0, "external_writes": 0, "creates_case_freshness": False}
    evals.write_json_atomic(args.output_root / "summary.json", summary)
    print(json.dumps(summary))
    return int(summary["failed"] != 0)


if __name__ == "__main__":
    raise SystemExit(main())
