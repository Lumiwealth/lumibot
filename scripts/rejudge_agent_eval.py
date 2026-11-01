"""Recover judging with original actors after a proved judge-only source repair.

Uses the canonical runner, ledger, model profile and freshness owner. It never
executes an actor or changes a case's user request, policies or machine contract.
"""
from __future__ import annotations

import argparse
import ast
import concurrent.futures
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path
from types import FunctionType

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import run_agent_evals as evals
from scripts.agent_eval_isolation import configure_fixture_environment
from scripts.agent_eval_subscription_budget import SubscriptionCallBudget

RUNNER = "scripts/run_agent_evals.py"
JUDGE_CONTROLS = "tests/fixtures/agent_judge_calibration.json"


def _git(root: Path, *arguments: str) -> bytes:
    return subprocess.check_output(["git", "-C", str(root), *arguments])


def compare_source(root: Path, commit: str, paths: list[Path]) -> tuple[dict[str, bytes], dict]:
    if re.fullmatch(r"[0-9a-f]{40}", commit) is None:
        raise ValueError("Original source must be an immutable full Git commit")
    root = root.resolve()
    names = sorted(path.resolve().relative_to(root).as_posix() for path in paths)
    original_names = _git(root, "ls-tree", "-r", "--name-only", commit).decode().splitlines()
    dynamic = lambda name: name.startswith("lumibot/") and (name.endswith(".py") or
        name.startswith("lumibot/components/agents/skills/"))
    if {name for name in original_names if dynamic(name)} != {name for name in names if dynamic(name)}:
        raise ValueError("Acting source membership changed")
    originals, hashes, changed = {}, {}, []
    for name in names:
        prior = _git(root, "show", f"{commit}:{name}")
        current = (root / name).read_bytes()
        originals[name] = prior
        hashes[name] = {"original": hashlib.sha256(prior).hexdigest(),
                        "current": hashlib.sha256(current).hexdigest()}
        if prior == current:
            continue
        changed.append(name)
        if name == RUNNER:
            def acting_ast(content):
                module = ast.parse(content)
                module.body = [node for node in module.body if not (
                    isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == "run_judge")]
                return ast.dump(module, include_attributes=False)
            if acting_ast(prior) != acting_ast(current):
                raise ValueError("Acting runner or profile changed outside run_judge")
        elif name != JUDGE_CONTROLS:
            raise ValueError(f"Acting input changed: {name}")
    proof = {"kind": "judge-only-source-compatibility", "originalSourceCommit": commit,
             "changedPaths": changed, "fileHashes": hashes}
    return originals, proof



def compare_case(root: Path, commit: str, case: dict) -> tuple[dict, dict]:
    """Permit a rubric correction only; preserve the exact original actor mandate."""
    if re.fullmatch(r"[0-9a-f]{40}", commit) is None:
        raise ValueError("Original source must be an immutable full Git commit")
    root = root.resolve()
    path = Path(case["_path"]).resolve()
    name = path.relative_to(root).as_posix()
    if path.parent != root / "agent_eval_cases" or path.suffix != ".json":
        raise ValueError("Acting case must use the canonical case owner")
    prior_bytes = _git(root, "show", f"{commit}:{name}")
    original = json.loads(prior_bytes)
    current = {key: value for key, value in case.items() if not key.startswith("_")}
    if json.loads(path.read_bytes()) != current:
        raise ValueError("Acting case no longer matches its saved source")
    changed = sorted(key for key in original.keys() | current.keys()
                     if original.get(key) != current.get(key))
    if set(changed) - {"judgeRubric"}:
        raise ValueError("Acting case changed outside its judge rubric")
    return original, {"path": name, "changedFields": changed,
        "originalSourceSha256": hashlib.sha256(prior_bytes).hexdigest(),
        "currentSourceSha256": hashlib.sha256(path.read_bytes()).hexdigest(),
        "originalCaseDigest": evals.actor_case_digest(original),
        "currentCaseDigest": evals.actor_case_digest(current)}


def source_compatibility(commit: str) -> dict:
    captured = {}
    def old_source_hash(paths):
        originals, proof = compare_source(evals.REPO_ROOT, commit, paths)
        captured.update(proof)
        digest = hashlib.sha256()
        for path in sorted(paths):
            name = path.relative_to(evals.REPO_ROOT).as_posix()
            digest.update(name.encode()); digest.update(b"\0")
            digest.update(originals[name]); digest.update(b"\0")
        return digest.hexdigest()
    original_fingerprint = FunctionType(evals.runtime_fingerprint.__code__,
        {**evals.runtime_fingerprint.__globals__, "sha256_files": old_source_hash})()
    return {**captured, "originalRuntimeFingerprint": original_fingerprint,
            "currentRuntimeFingerprint": evals.runtime_fingerprint(),
            "sdkVersions": evals.sdk_versions()}


def recover(cases, *, source_commit: str, output_root: Path, repetitions: list[int], workers: int):
    output_root = output_root.resolve()
    configure_fixture_environment(evals.REPO_ROOT, artifact_root=output_root)
    evals.preflight(cases, evals.DEFAULT_JUDGE_MODEL, 0)
    proof = source_compatibility(source_commit)
    original_cases = {case["id"]: compare_case(evals.REPO_ROOT, source_commit, case) for case in cases}
    proof["caseDefinitions"] = {identity: pair[1] for identity, pair in original_cases.items()}
    proof["recoveryToolSha256"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    proof_bytes = (json.dumps(proof, sort_keys=True, indent=2) + "\n").encode()
    proof_hash = hashlib.sha256(proof_bytes).hexdigest()
    proof_path = output_root / f"judge-recovery-source-{proof_hash}.json"
    proof_path.write_bytes(proof_bytes)
    ledger_path = output_root / "ledger.jsonl"
    rows = evals.read_jsonl(ledger_path)
    native = evals.read_jsonl(output_root / "model_calls.jsonl")
    settled = {row["id"] for row in native if row.get("kind") == "settle"}
    reservations = [row for row in native if row.get("kind") == "reserve" and row.get("id") in settled]
    pending, retained = [], 0
    for case in cases:
        original_case, case_proof = original_cases[case["id"]]
        prior_fp = evals.case_fingerprint(original_case, judge_model=evals.DEFAULT_JUDGE_MODEL,
                                         runtime_hash=proof["originalRuntimeFingerprint"])
        current_fp = evals.case_fingerprint(case, judge_model=evals.DEFAULT_JUDGE_MODEL,
                                           runtime_hash=proof["currentRuntimeFingerprint"])
        latest = evals.latest_repetition_rows(rows, case["id"], current_fp)
        for repetition in repetitions or list(range(1, evals.case_required_passes(case) + 1)):
            existing = latest.get(repetition)
            if existing and existing.get("status") == "pass":
                retained += 1
                continue
            if existing and existing.get("status") == "fail":
                raise ValueError("Current-source judge already failed; diagnose before another attempt")
            actor = next((row for row in reversed(rows) if row.get("case_id") == case["id"]
                and row.get("fingerprint") == prior_fp and row.get("repetition") == repetition
                and row.get("status") == "actor_completed"), None)
            if (not actor or actor.get("actor_case_digest") != evals.actor_case_digest(original_case)
                    or actor.get("acting_model") != case["model"]
                    or actor.get("judge_model") != evals.DEFAULT_JUDGE_MODEL
                    or actor.get("machine") != evals.score_machine_contract(case, actor["transcript"])
                    or not actor["machine"]["pass"]):
                raise ValueError("Missing unchanged completed actor with passing original machine evidence")
            if not any(row.get("scope", {}).get("run_id") == actor["run_id"]
                       and row["scope"].get("fingerprint") == prior_fp
                       and row["scope"].get("role") == "acting"
                       for row in reservations):
                raise ValueError("Original actor has no settled native model-call evidence")
            checkpoint = {**actor, "fingerprint": current_fp, "actor_reused": True,
                "actor_case_digest": case_proof["currentCaseDigest"],
                "actor_original_case_digest": case_proof["originalCaseDigest"],
                "actor_original_fingerprint": prior_fp, "actor_original_completed_at": actor["timestamp"],
                "judge_recovery_started_at": evals.utc_text(),
                "source_compatibility": {"sha256": proof_hash, "path": str(proof_path),
                                         "originalSourceCommit": source_commit}}
            pending.append((case, checkpoint))
    budget = SubscriptionCallBudget(output_root / "model_calls.jsonl", max_calls=1500)
    def run(item):
        case, checkpoint = item
        evals.append_jsonl(ledger_path, checkpoint)
        result = evals.resume_judge(case, checkpoint, judge_model=evals.DEFAULT_JUDGE_MODEL, budget=budget)
        evals.append_jsonl(ledger_path, result)
        return result
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
        completed = list(executor.map(run, pending))
    summary = {"kind": "judge-only-recovery", "actorRuns": 0, "retained": retained,
        "passed": sum(row["status"] == "pass" for row in completed),
        "failed": sum(row["status"] == "fail" for row in completed),
        "errors": sum(row["status"] == "error" for row in completed),
        "sourceCompatibility": str(proof_path)}
    evals.write_json_atomic(output_root / "judge-recovery-summary.json", summary)
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--case-id", action="append", required=True)
    parser.add_argument("--original-source-commit", required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--repetition", type=int, action="append", default=[])
    parser.add_argument("--max-workers", type=int, default=20)
    args = parser.parse_args()
    if args.max_workers < 1 or any(value < 1 for value in args.repetition):
        parser.error("Workers and repetition identities must be positive")
    result = recover(evals.load_cases(set(args.case_id)), source_commit=args.original_source_commit,
                     output_root=args.output_root, repetitions=args.repetition, workers=args.max_workers)
    print(json.dumps(result))
    raise SystemExit(0 if not result["failed"] and not result["errors"] else 1)


if __name__ == "__main__":
    main()
