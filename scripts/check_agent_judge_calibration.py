"""Qualify rubric changes with real judges and labeled, non-agent-run controls.

These are judge calibration fixtures, not fabricated agent evals. They never
create case freshness or replace the real actor repetitions in the release gate.
"""

import argparse
import ast
import concurrent.futures
import dataclasses
import functools
import hashlib
import json
import sys
import uuid
from copy import deepcopy
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import run_agent_evals as evals
from scripts.agent_eval_isolation import configure_fixture_environment
from scripts.agent_eval_subscription_budget import SubscriptionCallBudget

REQUIRED_REPETITIONS = 3

# The real pre-migration producer, retained only to validate still-fresh native
# records made by that version. This is not an alternate evaluation path.
LEGACY_CONTROL_FINGERPRINT_SOURCE = '''def control_fingerprint(item, case, runtime_hash):
    contract = {
        "control": item,
        "case": evals.case_fingerprint(case, judge_model=evals.DEFAULT_JUDGE_MODEL, runtime_hash=runtime_hash),
        "runner": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
    }
    return hashlib.sha256(evals.stable_json(contract).encode()).hexdigest()
'''

# These are the direct, tool-free judge's local protocol/runtime dependencies.
# Acting managers, broker adapters and builtin skills are not used by its
# RuntimeRequest. Actor proof continues to use the complete SDK fingerprint.
JUDGE_RUNTIME_PATHS = (
    "lumibot/__init__.py", "lumibot/components/agents/__init__.py",
    "lumibot/components/agents/runtime.py", "lumibot/components/agents/schemas.py",
    "lumibot/components/agents/tool_context.py", "lumibot/components/agents/chatgpt_auth.py",
    "lumibot/components/agents/chatgpt_llm.py", "lumibot/components/agents/chatgpt_run.py",
    "lumibot/components/agents/managed_gateway.py", "scripts/agent_eval_call_budget.py",
    "scripts/agent_eval_subscription_budget.py", "scripts/agent_eval_rate_pacing.py",
    "scripts/agent_eval_isolation.py",
)


@functools.lru_cache(maxsize=8)
def _judging_code(source):
    module = ast.parse(source)
    functions = {node.name: node for node in module.body if isinstance(node, ast.FunctionDef)}
    execution = functions["run_judge"]
    # Bind execution and parsing independently of the request producer. The
    # complete produced request is fingerprinted below. This also recognizes
    # the original inline producer without relabeling any old model call.
    assignments = [index for index, node in enumerate(execution.body)
                   if isinstance(node, ast.Assign) and any(
                       isinstance(target, ast.Name) and target.id == "request" for target in node.targets)]
    if len(assignments) != 1:
        raise ValueError("Judge execution must have one explicit RuntimeRequest")
    index = assignments[0]
    value = execution.body[index].value
    imports = [node for node in execution.body[:index] if isinstance(node, ast.ImportFrom)
               and node.module == "lumibot.components.agents.runtime"
               and any(name.name == "GoogleADKRuntime" and name.asname is None for name in node.names)]
    if len(imports) != 1 or not isinstance(value, ast.Call) or not isinstance(value.func, ast.Name):
        raise ValueError("Judge execution must use the canonical runtime and request producer")
    if value.func.id == "RuntimeRequest":
        # The immutable inline producer's output is captured separately.
        wrapper = []
        request_call = ast.parse("build_judge_request(case, transcript, judge_model, budget)", mode="eval").body
    elif value.func.id == "build_judge_request":
        wrapper = [node for node in execution.body[:index]
                   if not isinstance(node, (ast.Import, ast.ImportFrom))]
        request_call = value
    else:
        raise ValueError("Unknown judge request producer")
    tail = ast.Module(body=execution.body[index + 1:], type_ignores=[])
    return {"execution": ast.dump(tail, include_attributes=False),
            "wrapper": ast.dump(ast.Module(body=wrapper, type_ignores=[]), include_attributes=False),
            "request_call": ast.dump(request_call, include_attributes=False),
            "parser": ast.dump(functions["parse_judge_json"], include_attributes=False)}


@functools.lru_cache(maxsize=16)
def _git_sources(root, source_ref):
    from scripts import agent_eval_source_compatibility as source

    import re
    if not isinstance(source_ref, str) or re.fullmatch(r"[0-9a-f]{40}", source_ref) is None:
        raise ValueError("Judge reuse requires an immutable full source commit")
    source._git("merge-base", "--is-ancestor", source_ref, "HEAD", root=root)
    names = [*JUDGE_RUNTIME_PATHS, "scripts/run_agent_evals.py",
             "scripts/check_agent_judge_calibration.py", "tests/fixtures/agent_judge_calibration.json",
             "lumibot/components/agents/attention.py"]
    return source._blobs(source_ref, names, root=root)


def _original_sources(source_ref):
    return _git_sources(evals.REPO_ROOT.resolve(), source_ref)


@functools.lru_cache(maxsize=256)
def original_case_bytes(root, source_ref, path):
    from scripts import agent_eval_source_compatibility as source
    import re

    if re.fullmatch(r"[0-9a-f]{40}", source_ref) is None:
        raise ValueError("Original cases require an immutable full source commit")
    return source._git("show", f"{source_ref}:{path}", root=root)


@functools.lru_cache(maxsize=32)
def original_case_paths(root, source_ref):
    from scripts import agent_eval_source_compatibility as source
    import re

    if re.fullmatch(r"[0-9a-f]{40}", source_ref) is None:
        raise ValueError("Original cases require an immutable full source commit")
    return frozenset(source._git("ls-tree", "-r", "--name-only", source_ref, "--", "agent_eval_cases",
                                 root=root).decode().splitlines())


def judge_runtime_fingerprint(*, source_ref=None):
    blobs = (_original_sources(source_ref) if source_ref else {
        name: (evals.REPO_ROOT / name).read_bytes()
        for name in (*JUDGE_RUNTIME_PATHS, "scripts/run_agent_evals.py")})
    profile = {"sources": {name: hashlib.sha256(blobs[name]).hexdigest() for name in JUDGE_RUNTIME_PATHS},
               "code": _judging_code(blobs["scripts/run_agent_evals.py"]),
               "sdk_versions": evals.sdk_versions(), "service_tier": "default"}
    return hashlib.sha256(evals.stable_json(profile).encode()).hexdigest()


def _request_value(request):
    if request.bound_tools or request.include_builtin_skills:
        raise ValueError("Tool-bearing judges require a separately qualified input identity")
    value = dataclasses.asdict(request)
    value.pop("model_call_id")  # Transport correlation nonce, not model input.
    value.pop("model_call_budget")  # The real native usage scopes are verified separately.
    return value


def _input_fingerprint(item, request, protocol):
    value = {"version": 2, "id": item["id"], "case_id": item["case_id"],
             "expected": item["expected"], "request": _request_value(request), "protocol": protocol}
    return hashlib.sha256(evals.stable_json(value).encode()).hexdigest()


def control_fingerprint(item, case, runtime_hash):
    # Keep the third argument for existing runner/receipt callers. Acting SDK
    # changes expire actor evidence, not an unchanged tool-free judge request.
    request = evals.build_judge_request(case, item["transcript"], evals.DEFAULT_JUDGE_MODEL, None)
    return _input_fingerprint(item, request, judge_runtime_fingerprint())


def _original_request(blobs, case, transcript):
    """Capture the immutable producer's request without running a model.

    The old inline producer is cut immediately after RuntimeRequest creation.
    Only that function and its literal judge-profile constants are compiled;
    no old CLI, actor, runtime.run, credential or broker operation is executed.
    Unknown future helper dependencies fail visibly rather than using current
    functions to manufacture an original input.
    """
    module = ast.parse(blobs["scripts/run_agent_evals.py"])
    functions = {node.name: node for node in module.body if isinstance(node, ast.FunctionDef)}
    node = deepcopy(functions.get("build_judge_request") or functions["run_judge"])
    if node.name == "run_judge":
        index = next(index for index, entry in enumerate(node.body) if isinstance(entry, ast.Assign)
                     and any(isinstance(target, ast.Name) and target.id == "request" for target in entry.targets))
        node.body = node.body[:index + 1] + [ast.Return(value=ast.Name(id="request", ctx=ast.Load()))]
    attention_text = blobs["lumibot/components/agents/attention.py"].decode()
    attention_tree = ast.parse(attention_text)
    owner = next(n for n in attention_tree.body if isinstance(n, ast.ClassDef) and n.name == "AttentionMonitor")
    dispatch = next(n for n in owner.body if isinstance(n, ast.FunctionDef) and n.name == "dispatch_team")
    coordinator = "".join(attention_text.splitlines(keepends=True)[dispatch.lineno - 1:dispatch.end_lineno])

    class OriginalCoordinator(ast.NodeTransformer):
        def visit_Call(self, call):
            if isinstance(call.func, ast.Attribute) and isinstance(call.func.value, ast.Name) and (
                    call.func.value.id, call.func.attr) == ("inspect", "getsource"):
                if len(call.args) != 1 or ast.unparse(call.args[0]) != "AttentionMonitor.dispatch_team":
                    raise ValueError("Unknown original judge source projection")
                return ast.copy_location(ast.Constant(coordinator), call)
            return self.generic_visit(call)

    node = OriginalCoordinator().visit(node)
    namespace = {"json": json, "uuid": uuid, "Any": object, "__name__": __name__}
    for entry in module.body:
        if isinstance(entry, ast.Assign):
            for target in entry.targets:
                if isinstance(target, ast.Name) and target.id in {
                        "DEFAULT_JUDGE_MODEL", "JUDGE_MAX_OUTPUT_TOKENS", "JUDGE_REASONING_EFFORT"}:
                    namespace[target.id] = ast.literal_eval(entry.value)
    captured = ast.fix_missing_locations(ast.Module(body=[node], type_ignores=[]))
    exec(compile(captured, "<immutable-judge-request>", "exec"), namespace)
    return namespace[node.name](case, transcript, namespace["DEFAULT_JUDGE_MODEL"], None)


@functools.lru_cache(maxsize=16)
def _original_runtime(root, source_ref, versions_json):
    from scripts import agent_eval_source_compatibility as source

    return source._fingerprint(source._blobs(source_ref,
        source.runtime_paths(root=root, source_ref=source_ref), root=root), json.loads(versions_json))


def compatible_control_source(item, case, source_ref):
    """Prove a legacy call's exact input/protocol; never rewrite its identity."""
    from scripts import agent_eval_source_compatibility as source

    blobs = _original_sources(source_ref)
    controls = json.loads(blobs["tests/fixtures/agent_judge_calibration.json"])
    original = next((row for row in controls if row["id"] == item["id"]), None)
    if original is None or original["case_id"] != item["case_id"] or original["expected"] is not item["expected"]:
        return None
    path = Path(case["_path"]).relative_to(evals.REPO_ROOT).as_posix()
    prior_case = json.loads(original_case_bytes(evals.REPO_ROOT.resolve(), source_ref, path))
    protocol = judge_runtime_fingerprint(source_ref=source_ref)
    if protocol != judge_runtime_fingerprint():
        return None
    prior_request = _original_request(blobs, prior_case, original["transcript"])
    current = control_fingerprint(item, case, "unused-acting-source")
    if _input_fingerprint(original, prior_request, protocol) != current:
        return None
    versions = evals.sdk_versions()
    runtime = _original_runtime(evals.REPO_ROOT.resolve(), source_ref, evals.stable_json(versions))
    legacy_module = ast.parse(blobs["scripts/check_agent_judge_calibration.py"])
    functions = {node.name: node for node in legacy_module.body if isinstance(node, ast.FunctionDef)}
    if "_input_fingerprint" in functions:
        current_functions = {node.name: node for node in ast.parse(Path(__file__).read_bytes()).body
                             if isinstance(node, ast.FunctionDef)}
        if any(ast.dump(functions[name], include_attributes=False) !=
               ast.dump(current_functions[name], include_attributes=False)
               for name in ("control_fingerprint", "_input_fingerprint")):
            raise ValueError("Unsupported original calibration identity algorithm")
        fingerprint = _input_fingerprint(original, prior_request, protocol)
    else:
        # Compatibility for native records produced by the pre-input-identity
        # runner. Retire this branch after those fresh records are no longer
        # admissible; their original fingerprints/scopes remain unchanged.
        expected = ast.parse(LEGACY_CONTROL_FINGERPRINT_SOURCE).body[0]
        if ("control_fingerprint" not in functions or
                ast.dump(functions["control_fingerprint"], include_attributes=False) != ast.dump(expected, include_attributes=False)):
            raise ValueError("Unsupported original calibration fingerprint algorithm")
        contract = {"control": original,
                    "case": evals.case_fingerprint(prior_case, judge_model=evals.DEFAULT_JUDGE_MODEL,
                                                   runtime_hash=runtime),
                    "runner": hashlib.sha256(blobs["scripts/check_agent_judge_calibration.py"]).hexdigest()}
        fingerprint = hashlib.sha256(evals.stable_json(contract).encode()).hexdigest()
    return {"source_commit": source_ref, "runtime_fingerprint": runtime, "fingerprint": fingerprint,
            "input_fingerprint": current, "sdk_versions": versions}


def control_sources(fixtures, cases, source_refs):
    return {item["id"]: [proof for ref in source_refs
            if (proof := compatible_control_source(item, cases[item["case_id"]], ref)) is not None]
            for item in fixtures}


def control_repetitions(item):
    repetitions = item.get("required_repetitions", REQUIRED_REPETITIONS)
    if type(repetitions) is not int or repetitions not in {3, 5}:
        raise ValueError("Calibration repetitions must be 3 or 5")
    return repetitions


def latest_control_rows(rows, fingerprints, required_repetitions=None, compatible_sources=None):
    latest = {}
    required_repetitions = required_repetitions or {}
    compatible_sources = compatible_sources or {}
    for row in rows:
        control_id, repetition = row.get("id"), row.get("repetition")
        compatible = next((proof for proof in compatible_sources.get(control_id, [])
                           if row.get("fingerprint") == proof["fingerprint"]
                           and row.get("runtime_fingerprint") == proof["runtime_fingerprint"]), None)
        if (control_id in fingerprints and (row.get("fingerprint") == fingerprints[control_id] or compatible)
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


def select_controls(fixtures, *, case_ids=(), control_ids=()):
    cases, controls = set(case_ids), set(control_ids)
    if cases - {item["case_id"] for item in fixtures}:
        raise ValueError("Unknown calibration case")
    if controls - {item["id"] for item in fixtures}:
        raise ValueError("Unknown calibration control")
    selected = [item for item in fixtures
                if (not cases or item["case_id"] in cases) and (not controls or item["id"] in controls)]
    if not selected:
        raise ValueError("Calibration selection contains no controls")
    return selected


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
    parser.add_argument("--control-id", action="append", default=[],
                        help="Run named controls first; the complete qualification still requires the full catalog")
    parser.add_argument("--reuse-source", action="append", default=[],
                        help="Verify unchanged legacy judging inputs against an immutable original Git source")
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
    try:
        fixtures = select_controls(fixtures, case_ids=args.case_id, control_ids=args.control_id)
    except ValueError as exc:
        parser.error(str(exc))
    cases = {case["id"]: case for case in evals.load_cases({item["case_id"] for item in fixtures})}
    runtime_hash = evals.runtime_fingerprint()
    judge_protocol = judge_runtime_fingerprint()
    fingerprints = {item["id"]: control_fingerprint(item, cases[item["case_id"]], runtime_hash) for item in fixtures}
    output_root.mkdir(parents=True, exist_ok=True)
    ledger_path = output_root / "ledger.jsonl"
    existing = evals.read_jsonl(ledger_path)
    summary_path = output_root / "summary.json"
    prior_summary = json.loads(summary_path.read_text()) if summary_path.exists() else {}
    source_refs = sorted(set(args.reuse_source or prior_summary.get("compatible_source_commits", [])))
    compatible = control_sources(fixtures, cases, source_refs)
    evals.write_json_atomic(output_root / "source-compatibility.json", {
        "source_commits": source_refs, "controls": compatible,
        "policy": "Original source, fingerprint, verdict, timestamps and native call scopes are unchanged."})
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
    latest = latest_control_rows(existing + recovered, fingerprints, repetitions, compatible)
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
               "judge_runtime_fingerprint": judge_protocol,
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
                   "compatible_source_commits": source_refs,
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
