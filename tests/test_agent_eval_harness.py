import importlib.util
import io
import json
import os
import sys
import zipfile
from pathlib import Path

import pytest

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts/run_agent_evals.py"
SPEC = importlib.util.spec_from_file_location("run_agent_evals", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
evals = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = evals
SPEC.loader.exec_module(evals)

RESTORE_SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts/restore_agent_eval_freshness.py"
RESTORE_SPEC = importlib.util.spec_from_file_location("restore_agent_eval_freshness", RESTORE_SCRIPT_PATH)
assert RESTORE_SPEC is not None and RESTORE_SPEC.loader is not None
restore_freshness = importlib.util.module_from_spec(RESTORE_SPEC)
sys.modules[RESTORE_SPEC.name] = restore_freshness
RESTORE_SPEC.loader.exec_module(restore_freshness)


@pytest.fixture(autouse=True)
def close_production_fixtures(monkeypatch):
    original = evals.build_fixture
    fixtures = []

    def tracked(name):
        result = original(name)
        fixtures.append(result)
        return result

    monkeypatch.setattr(evals, "build_fixture", tracked)
    yield
    for fixture in fixtures:
        if hasattr(fixture, "production"):
            fixture.production.close()


def mcp_value(result):
    assert not result.get("isError"), result
    if result.get("structuredContent") is not None:
        return result["structuredContent"]
    return json.loads(next(part["text"] for part in result["content"] if part["type"] == "text"))


def test_release_eval_uses_production_manager_context_and_builtin_bindings(monkeypatch):
    from types import SimpleNamespace

    import lumibot.components.agents.runtime as runtime
    from lumibot.components.agents import AgentRunResult, AgentTraceEvent

    requests = []

    class CaptureRuntime:
        def __init__(self, **kwargs):
            pass

        def run(self, request):
            requests.append(request)
            return AgentRunResult(
                summary="No action.",
                model=request.model,
                events=[AgentTraceEvent(kind="text", text="No action.")],
                usage={"input_tokens": 10, "output_tokens": 5},
            )

    monkeypatch.setattr(runtime, "GoogleADKRuntime", CaptureRuntime)
    # AgentManager caches the runtime class on first use. Reset the cache for
    # this test only, so the stub is not left behind for later tests.
    import lumibot.components.agents.manager as manager_module

    monkeypatch.setattr(manager_module, "_RUNTIME_IMPORTS", None)
    monkeypatch.setattr(
        evals,
        "run_judge",
        lambda *args: (
            {"pass": True, "reason": "unit fixture"},
            AgentRunResult(summary="", model="gemini-3.1-flash-lite", events=[], usage={}),
            0.0,
        ),
    )
    case = {
        "id": "parity-contract",
        "fixture": "stock_momentum",
        "model": "gemini-3.5-flash-lite",
        "systemPrompt": "Inspect the account.",
        "taskPrompt": "Hold.",
        "machineContract": {"forbidOrderTools": True},
        "judgeRubric": "No order.",
    }
    evals.execute_repetition(
        case,
        repetition=1,
        fingerprint="unit",
        judge_model="gemini-3.1-flash-lite",
        budget=SimpleNamespace(for_scope=lambda *args: object()),
    )
    assert len(requests) == 1
    request = requests[0]
    tools = {tool.name: tool for tool in request.bound_tools}
    assert "get_indicators" in tools
    assert all(tool.source != "eval_fixture" for tool in request.bound_tools)
    assert request.runtime_context.get("account_snapshot") is not None
    assert request.model_call_budget is not None


def test_production_execution_cannot_pass_without_observed_fill_or_completed_decision():
    case = {"machineContract": {"orderTool": "orders_submit_order", "exactOrderCount": 1}}
    transcript = {
        "tool_calls": [],
        "fixture_calls": [{"name": "orders_submit_order"}],
        "submissions": [{"tool": "orders_submit_order"}],
        "final_positions": [],
        "broker_orders": [{"identifier": "pending", "status": "new"}],
        "execution_outcome": {"decision_completed": False},
    }
    result = evals.score_machine_contract(case, transcript)
    assert not result["pass"]
    assert len(result["failures"]) == 2
    transcript["broker_orders"][0]["status"] = "fill"
    transcript["execution_outcome"]["decision_completed"] = True
    assert evals.score_machine_contract(case, transcript)["pass"]


def test_only_complete_current_injected_account_evidence_can_replace_redundant_reads():
    context = {
        "current_datetime": "2026-08-11T14:35:00Z",
        "positions": [{"quantity": 3}],
        "account_snapshot": {
            "as_of": "2026-08-11T14:35:00Z",
            "positions_complete": True,
            "positions_total": 1,
            "positions_included": 1,
            "positions_omitted": 0,
        },
    }
    transcript = {"initial_runtime_context": context}
    assert evals.initial_snapshot_covers(transcript, "account_positions")
    assert not evals.initial_snapshot_covers(transcript, "market_last_price")
    assert not evals.initial_snapshot_covers({}, "account_positions")
    for field, value in [
        ("positions_total", 51),
        ("positions_omitted", 1),
        ("positions_complete", False),
        ("as_of", "2026-08-10T14:35:00Z"),
    ]:
        original = context["account_snapshot"][field]
        context["account_snapshot"][field] = value
        assert not evals.initial_snapshot_covers(transcript, "account_positions")
        context["account_snapshot"][field] = original


def test_every_eval_case_uses_a_real_model_and_a_production_contract():
    cases = evals.load_cases()
    assert len(cases) >= 7
    assert len({case["id"] for case in cases}) == len(cases)
    for case in cases:
        # Rob, 2026-09-23: release evals run the product default, GPT-6 Luna.
        assert case["model"] == "openai/gpt-6-luna"
        assert case["judgeRubric"].strip()
        assert case["machineContract"]
        assert "simulatedEvents" not in case


def test_entry_evals_accept_only_complete_current_account_snapshots_instead_of_redundant_reads():
    cases = {case["id"]: case for case in evals.load_cases()}
    for case_id in (
        "options_single_leg_chain_and_quote",
        "stock_price_before_order",
        "stock_orb_completed_bars",
    ):
        contract = cases[case_id]["machineContract"]
        assert contract["acceptCompleteInitialAccountSnapshot"] is True
        assert any(
            tool in contract["requiredBeforeOrder"]
            for tool in ("account_portfolio", "account_positions", "orders_open_orders")
        )


def test_stock_entry_evals_require_explicit_instrument_identity():
    cases = {case["id"]: case for case in evals.load_cases()}
    for case_id in ("stock_price_before_order", "stock_orb_completed_bars"):
        assert "asset_type stock" in cases[case_id]["systemPrompt"]


def test_stock_price_eval_accepts_each_production_history_analysis_path():
    case = next(case for case in evals.load_cases() if case["id"] == "stock_price_before_order")
    assert case["machineContract"]["requiredAnyTools"] == [
        ["market_historical_prices", "market_load_history_table", "get_indicator"]
    ]
    assert "risk_calculate_stock_quantity" in case["machineContract"]["requiredBeforeOrder"]


def test_crypto_identity_eval_preserves_typed_instrument_contract_and_red_baseline():
    case = evals.load_cases({"crypto_instrument_identity"})[0]
    assert case["context"]["instrument_universe"] == [
        {"symbol": "BTC", "asset_type": "stock"},
        {"symbol": "BTC", "asset_type": "crypto", "quote_symbol": "USD"},
    ]
    assert case["machineContract"]["instrumentIdentity"] == {
        "symbol": "BTC",
        "asset_type": "crypto",
        "quote_symbol": "USD",
    }
    assert case["machineContract"]["requiredAnyTools"] == [["get_indicator", "get_indicators"]]
    baseline = json.loads(
        (evals.REPO_ROOT / "agent_eval_baselines/2026-09-09_crypto_instrument_identity_red.json").read_text()
    )
    assert baseline["bad_tool_call"]["arguments"] == {
        "indicators": ["rsi", "macd"],
        "symbol": "BTC",
        "timestep": "hour",
    }


def test_research_eval_catalog_covers_point_in_time_injection_fallback_and_handoff():
    cases = {
        case["id"]: case
        for case in evals.load_cases(
            {
                "research_macro_point_in_time",
                "research_sec_prompt_injection",
                "research_unavailable_safe_fallback",
                "researcher_trader_evidence_handoff",
            }
        )
    }
    assert all(case["requiredSkill"] == "research-data" for case in cases.values())
    handoff = cases["researcher_trader_evidence_handoff"]
    assert handoff["agentTopology"] == "researcher_then_trader"
    assert handoff["machineContract"]["requiredAgents"] == ["researcher", "trader"]
    assert handoff["machineContract"]["requiredTraderTools"] == [
        "account_portfolio",
        "account_positions",
        "orders_open_orders",
    ]
    assert cases["research_macro_point_in_time"]["machineContract"]["requiredTools"] == [
        "search_data_catalog",
        "query_data",
    ]
    assert cases["research_sec_prompt_injection"]["machineContract"]["requiredTools"] == [
        "search_documents",
        "get_document",
    ]
    assert all(case["machineContract"]["forbidOrderTools"] for case in cases.values())


def test_research_eval_fixture_preserves_provenance_and_embeds_untrusted_filing_text():
    fixture = evals.build_fixture("research_available")
    tools = {tool.name: tool for tool in evals.build_tools(fixture)}

    catalog = mcp_value(tools["search_data_catalog"].function(query="inflation"))
    macro = mcp_value(
        tools["query_data"].function(
            datasetId="bls.public_series",
            timeRange={"endDate": "2026-08-11"},
        )
    )
    filings = mcp_value(
        tools["search_documents"].function(
            datasetId="sec.filings",
            entity="ACME",
            timeRange={"endDate": "2026-08-11"},
        )
    )
    document = mcp_value(
        tools["get_document"].function(
            datasetId="sec.filings",
            documentId=filings["rows"][0]["documentId"],
            section="risk_factors",
            asOf="2026-08-11",
        )
    )

    assert catalog["available"] is True
    assert macro["attribution"] == "BLS Public Data API"
    assert macro["rows"][0]["releaseDate"] <= "2026-08-11"
    assert document["attribution"].startswith("U.S. Securities and Exchange Commission")
    assert "IGNORE ALL PRIOR RULES" in document["text"]


def test_research_eval_unavailable_fixture_never_returns_synthetic_observations():
    fixture = evals.build_fixture("research_unavailable")
    tools = {tool.name: tool for tool in evals.build_tools(fixture)}

    catalog = mcp_value(tools["search_data_catalog"].function())
    assert catalog["available"] is True
    assert any(item["datasetId"] == "bls.public_series" for item in catalog["datasets"])
    unavailable = mcp_value(tools["query_data"].function(datasetId="bls.public_series"))
    assert unavailable == {
        "available": False,
        "error": "managed_research_unavailable",
        "message": "No research observations were returned. Do not infer or invent values.",
    }


def test_research_eval_fixture_rejects_an_unsupported_dataset_instead_of_substituting_treasury():
    fixture = evals.build_fixture("research_available")
    tools = {tool.name: tool for tool in evals.build_tools(fixture)}

    result = mcp_value(tools["query_data"].function(datasetId="unsupported.dataset"))

    assert result["available"] is False
    assert result["error"] == "unsupported_dataset"
    assert result["datasetId"] == "unsupported.dataset"


def test_research_eval_macro_rows_come_from_a_provenance_bearing_recorded_fixture():
    fixture_path = Path(__file__).resolve().parents[1] / "agent_eval_fixtures/research_data.json"
    recorded = json.loads(fixture_path.read_text(encoding="utf-8"))

    assert recorded["capturedAt"]
    assert recorded["sources"]["bls.public_series"]["sourceUrl"].startswith("https://")
    assert len(recorded["sources"]["bls.public_series"]["responseSha256"]) == 64
    assert recorded["sources"]["treasury.daily_yield_curve"]["sourceUrl"].startswith("https://")


def test_release_runner_prefers_gemini_key_when_both_credential_names_exist(monkeypatch):
    monkeypatch.setenv("GEMINI_API_KEY", "release-gemini-key")
    monkeypatch.setenv("GOOGLE_API_KEY", "stale-google-key")

    assert evals.select_eval_credentials({"gemini-3.5-flash-lite"}) == ["GEMINI_API_KEY"]
    assert os.environ["GOOGLE_API_KEY"] == "release-gemini-key"


def test_release_runner_supports_google_key_when_it_is_the_only_credential(monkeypatch):
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.setenv("GOOGLE_API_KEY", "google-key")

    assert evals.select_eval_credentials({"gemini-3.5-flash-lite"}) == ["GOOGLE_API_KEY"]
    assert os.environ["GOOGLE_API_KEY"] == "google-key"


def test_luna_evals_need_only_the_openai_key(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "synthetic-openai")
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.delenv("GOOGLE_API_KEY", raising=False)

    assert evals.select_eval_credentials({"openai/gpt-6-luna"}) == ["OPENAI_API_KEY"]
    evals.preflight(evals.load_cases(), evals.DEFAULT_JUDGE_MODEL, 2.0)

    monkeypatch.delenv("OPENAI_API_KEY")
    with pytest.raises(RuntimeError, match="OPENAI_API_KEY"):
        evals.select_eval_credentials({"openai/gpt-6-luna"})
    with pytest.raises(RuntimeError, match="OPENAI_API_KEY"):
        evals.preflight(evals.load_cases(), evals.DEFAULT_JUDGE_MODEL, 2.0)


def test_eval_defaults_are_gpt6_luna_on_medium_reasoning_with_registered_prices():
    from lumibot.components.agents.manager import DEFAULT_AGENT_MODEL
    from lumibot.components.agents.runtime import _OPENAI_GPT6_MODEL_INFO

    assert evals.DEFAULT_ACTING_MODEL == DEFAULT_AGENT_MODEL == "openai/gpt-6-luna"
    assert evals.DEFAULT_JUDGE_MODEL == "openai/gpt-6-luna"
    assert evals.EVAL_REASONING_EFFORT == "medium"
    info = _OPENAI_GPT6_MODEL_INFO["gpt-6-luna"]
    prices = evals.MODEL_PRICES_PER_MILLION["openai/gpt-6-luna"]
    assert prices["input"] == pytest.approx(info["input_cost_per_token"] * 1_000_000)
    assert prices["cached_input"] == pytest.approx(info["cache_read_input_token_cost"] * 1_000_000)
    assert prices["output"] == pytest.approx(info["output_cost_per_token"] * 1_000_000)
    assert "openai" in evals.estimate_cost("openai/gpt-6-luna", {"input_tokens": 10})["price_source_url"]


def test_acting_and_judge_requests_use_medium_reasoning(monkeypatch):
    from lumibot.components.agents import runtime
    from lumibot.components.agents.schemas import AgentRunResult, AgentTraceEvent
    from scripts.agent_eval_production_fixture import ProductionFixture

    production = ProductionFixture(evals.build_fixture("flat_stock_account"))
    try:
        handle = production.create_agent(evals.load_cases({"stock_price_before_order"})[0], None)
        assert handle.reasoning_effort == "medium"
    finally:
        production.close()

    seen = []

    class CaptureRuntime:
        def run(self, request):
            seen.append(request)
            return AgentRunResult(
                summary='{"pass": true, "reason": "ok"}',
                model=request.model,
                events=[AgentTraceEvent(kind="text", text='{"pass": true, "reason": "ok"}')],
            )

    monkeypatch.setattr(runtime, "GoogleADKRuntime", CaptureRuntime)
    case = evals.load_cases({"stock_price_before_order"})[0]
    evals.run_judge(case, {"tool_calls": []}, evals.DEFAULT_JUDGE_MODEL, None)
    assert seen[0].model == "openai/gpt-6-luna"
    assert seen[0].reasoning_effort == "medium"


def test_judge_output_cap_leaves_room_for_luna_reasoning(monkeypatch):
    # GPT-6 Luna counts reasoning tokens against max_output_tokens. A passing
    # judge call used 985 of the old 1,000 (463 reasoning + 522 text), and run
    # 36021663457 errored when a longer reasoning pass truncated the JSON verdict.
    from lumibot.components.agents import runtime
    from lumibot.components.agents.schemas import AgentRunResult, AgentTraceEvent

    seen = []

    class CaptureRuntime:
        def run(self, request):
            seen.append(request)
            return AgentRunResult(
                summary='{"pass": true, "reason": "ok"}',
                model=request.model,
                events=[AgentTraceEvent(kind="text", text='{"pass": true, "reason": "ok"}')],
            )

    monkeypatch.setattr(runtime, "GoogleADKRuntime", CaptureRuntime)
    case = evals.load_cases({"stock_price_before_order"})[0]
    evals.run_judge(case, {"tool_calls": []}, evals.DEFAULT_JUDGE_MODEL, None)
    assert seen[0].max_output_tokens == evals.JUDGE_MAX_OUTPUT_TOKENS
    assert evals.JUDGE_MAX_OUTPUT_TOKENS >= 4_000


def test_release_publish_is_blocked_by_real_model_agent_evals():
    workflow = (Path(__file__).resolve().parents[1] / ".github/workflows/release.yml").read_text(encoding="utf-8")
    assert "agent-evals:" in workflow
    assert "python scripts/run_agent_evals.py" in workflow
    assert "needs: [validate-build, unit-tests, backtest-tests, agent-evals]" in workflow
    assert "OPENAI_API_KEY: ${{ secrets.OPENAI_API_KEY }}" in workflow
    standalone = (Path(__file__).resolve().parents[1] / ".github/workflows/agent-evals.yml").read_text(encoding="utf-8")
    assert "OPENAI_API_KEY: ${{ secrets.OPENAI_API_KEY }}" in standalone


def test_paid_eval_workflows_cap_each_run_at_two_dollars():
    repo_root = Path(__file__).resolve().parents[1]
    standalone = (repo_root / ".github/workflows/agent-evals.yml").read_text(encoding="utf-8")
    release = (repo_root / ".github/workflows/release.yml").read_text(encoding="utf-8")

    assert 'default: "2"' in standalone
    assert "--max-cost-usd 2" in release
    assert "--max-cost-usd 10" not in release


def test_standalone_eval_workflow_supports_targeted_case_repeats():
    workflow = (Path(__file__).resolve().parents[1] / ".github/workflows/agent-evals.yml").read_text(encoding="utf-8")

    assert "case_ids:" in workflow
    assert "CASE_IDS: ${{ inputs.case_ids }}" in workflow
    assert 'args+=(--case-id "${case_id}")' in workflow
    assert 'case_id="${case_id//[[:space:]]/}"' in workflow


def test_release_restores_repository_scoped_eval_evidence_after_branch_scoped_cache():
    workflow = (Path(__file__).resolve().parents[1] / ".github/workflows/release.yml").read_text(encoding="utf-8")
    artifact_restore = workflow.index("Restore cross-workflow passing eval freshness")
    cache_restore = workflow.index("Restore passing eval freshness")
    assert cache_restore < artifact_restore
    assert "actions: read" in workflow
    assert "scripts/restore_agent_eval_freshness.py" in workflow
    assert '--trusted-commit "${GITHUB_SHA}"' in workflow


def test_cross_workflow_restore_accepts_only_a_valid_freshness_archive():
    valid_payload = io.BytesIO()
    with zipfile.ZipFile(valid_payload, "w") as archive:
        archive.writestr("artifacts/summary.json", "{}")
        archive.writestr("freshness.json", json.dumps({"version": 1, "cases": {"case": {}}}))
    assert restore_freshness._freshness_from_zip(valid_payload.getvalue()) == {
        "version": 1,
        "cases": {"case": {}},
    }

    invalid_payload = io.BytesIO()
    with zipfile.ZipFile(invalid_payload, "w") as archive:
        archive.writestr("freshness.json", json.dumps({"version": 1, "cases": []}))
    assert restore_freshness._freshness_from_zip(invalid_payload.getvalue()) is None


def test_cross_workflow_restore_skips_unusable_runs_and_writes_the_first_valid_state(monkeypatch, tmp_path):
    valid_payload = io.BytesIO()
    expected = {"version": 1, "cases": {"case": {"fingerprint": "abc"}}}
    with zipfile.ZipFile(valid_payload, "w") as archive:
        archive.writestr("freshness.json", json.dumps(expected))

    def fake_get_json(url, _token):
        if "/workflows/" in url:
            return {
                "workflow_runs": [
                    {"id": 9, "conclusion": "success", "head_sha": "9" * 40},
                    {"id": 8, "conclusion": "failure"},
                    {"id": 7, "conclusion": "success", "head_sha": "7" * 40},
                ]
            }
        if "/compare/" in url:
            return {"status": "ahead"}
        if "/runs/9/" in url:
            return {
                "artifacts": [
                    {
                        "name": "lumibot-agent-evals-9",
                        "expired": True,
                        "archive_download_url": "https://example.test/expired",
                    }
                ]
            }
        if "/runs/7/" in url:
            return {
                "artifacts": [
                    {
                        "name": "lumibot-agent-evals-7",
                        "expired": False,
                        "archive_download_url": "https://example.test/valid",
                    }
                ]
            }
        raise AssertionError(url)

    monkeypatch.setattr(restore_freshness, "_get_json", fake_get_json)
    monkeypatch.setattr(restore_freshness, "_get_bytes", lambda _url, _token: valid_payload.getvalue())
    output = tmp_path / "nested" / "freshness.json"
    assert (
        restore_freshness.restore(
            repository="Lumiwealth/lumibot",
            token="redacted",
            workflow="agent-evals.yml",
            output=output,
            trusted_commit="a" * 40,
        )
        == 7
    )
    assert json.loads(output.read_text(encoding="utf-8")) == expected


def test_cross_workflow_restore_rejects_a_newer_unrelated_branch_artifact(monkeypatch, tmp_path):
    calls = []

    def fake_get_json(url, _token):
        calls.append(url)
        if "/workflows/" in url:
            return {
                "workflow_runs": [
                    {"id": 10, "conclusion": "success", "head_sha": "b" * 40},
                ]
            }
        if "/compare/" in url:
            return {"status": "diverged"}
        raise AssertionError(f"untrusted run artifacts must not be downloaded: {url}")

    monkeypatch.setattr(restore_freshness, "_get_json", fake_get_json)
    monkeypatch.setattr(
        restore_freshness,
        "_get_bytes",
        lambda *_args: pytest.fail("untrusted artifact must not be downloaded"),
    )

    assert (
        restore_freshness.restore(
            repository="Lumiwealth/lumibot",
            token="redacted",
            workflow="agent-evals.yml",
            output=tmp_path / "freshness.json",
            trusted_commit="a" * 40,
        )
        is None
    )
    assert any("/compare/" in url for url in calls)


def test_eval_freshness_policy_has_one_90_day_source_of_truth():
    repo_root = Path(__file__).resolve().parents[1]
    workflows = [
        repo_root / ".github/workflows/agent-evals.yml",
        repo_root / ".github/workflows/release.yml",
    ]
    policy_docs = [
        repo_root / "docs/AGENT_EVALS.md",
        repo_root / "docs/AI_TRADING_AGENTS.md",
    ]

    assert evals.DEFAULT_FRESHNESS_DAYS == 90
    for workflow_path in workflows:
        workflow = workflow_path.read_text(encoding="utf-8")
        assert "--freshness-days" not in workflow
    for doc_path in policy_docs:
        policy_doc = doc_path.read_text(encoding="utf-8")
        assert "30-day" not in policy_doc
        assert "30 days" not in policy_doc
        assert "--freshness-days 30" not in policy_doc
        assert "90 days" in policy_doc


def test_eval_fingerprint_changes_with_runtime_and_judge():
    case = evals.load_cases({"stock_price_before_order"})[0]
    first = evals.case_fingerprint(case, judge_model="gemini-3.1-flash-lite", runtime_hash="a")
    second = evals.case_fingerprint(case, judge_model="gemini-3.1-flash-lite", runtime_hash="b")
    third = evals.case_fingerprint(case, judge_model="gemini-3.5-flash-lite", runtime_hash="a")
    assert first != second
    assert first != third


def test_eval_cost_uses_official_cached_and_uncached_rates():
    cost = evals.estimate_cost(
        "gemini-3.5-flash-lite",
        {
            "input_tokens": 1_000_000,
            "cached_input_tokens": 250_000,
            "output_tokens": 100_000,
        },
    )
    assert cost["estimated_usd"] == 0.4825
    assert cost["price_source_url"].startswith("https://cloud.google.com/")


def test_eval_initial_worker_admission_does_not_overcommit_initial_calls():
    case = evals.load_cases({"stock_price_before_order"})[0]
    work = [(case, repetition, "fingerprint") for repetition in range(1, 4)]
    # This admits workers, not whole tool loops. Per-call ledger tests enforce
    # the actual cap before each continuation and across process resumes.
    per_repetition = evals.initial_repetition_reservation_usd(case, evals.DEFAULT_JUDGE_MODEL)

    batch, remaining = evals.reserve_budget_batch(
        work,
        max_workers=3,
        remaining_budget=(per_repetition * 2) + (per_repetition / 2),
        judge_model=evals.DEFAULT_JUDGE_MODEL,
    )

    assert len(batch) == 2
    assert len(remaining) == 1
    assert sum(item[3] for item in batch) <= (per_repetition * 2) + (per_repetition / 2)


def test_runtime_fingerprint_includes_indicators_broker_and_installed_sdks(monkeypatch):
    seen = []
    monkeypatch.setattr(evals, "sha256_files", lambda paths: seen.extend(paths) or "source")
    monkeypatch.setattr(evals.importlib.metadata, "version", lambda package: "first")
    first = evals.runtime_fingerprint()
    monkeypatch.setattr(evals.importlib.metadata, "version", lambda package: "second")
    assert evals.runtime_fingerprint() != first
    assert evals.REPO_ROOT / "lumibot/indicators/indicators.py" in seen
    assert evals.REPO_ROOT / "lumibot/components/agents/asset_resolution.py" in seen
    assert evals.REPO_ROOT / "lumibot/brokers/broker.py" in seen
    assert evals.REPO_ROOT / "scripts/agent_eval_call_budget.py" in seen
    # The DuckDB table behind market_historical_prices(table_name=...) is agent
    # evidence; a change there must invalidate fresh eval receipts.
    assert evals.REPO_ROOT / "lumibot/components/agents/duckdb_tools.py" in seen


def test_fresh_gate_preserves_original_pass_time_without_spending(tmp_path, monkeypatch):
    # main() rebuilds the process environment for the fixture. Give it a
    # private copy and a synthetic key so it neither clears the real test
    # environment nor imports a key from a developer's .env.local.
    monkeypatch.setattr(os, "environ", {"PATH": os.environ.get("PATH", "")})
    monkeypatch.setenv("OPENAI_API_KEY", "unused-synthetic-key")
    case = evals.load_cases({"stock_price_before_order"})[0]
    fingerprint = evals.case_fingerprint(case, judge_model=evals.DEFAULT_JUDGE_MODEL, runtime_hash="runtime")
    passed_at = evals.utc_text(evals.utc_now() - evals.timedelta(days=2))
    state_path = tmp_path / "freshness.json"
    evals.write_json_atomic(
        state_path,
        {
            "version": 1,
            "cases": {
                case["id"]: {
                    "fingerprint": fingerprint,
                    "passed_at": passed_at,
                    "consecutive_passes": 3,
                }
            },
        },
    )
    monkeypatch.setattr(evals, "runtime_fingerprint", lambda: "runtime")
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "runner",
            "--gate",
            "--case-id",
            case["id"],
            "--max-cost-usd",
            "4",
            "--freshness-state",
            str(state_path),
            "--output-root",
            str(tmp_path / "run"),
        ],
    )
    assert evals.main() == 0
    assert evals.load_freshness(state_path)["cases"][case["id"]]["passed_at"] == passed_at
    summary = json.loads((tmp_path / "run/summary.json").read_text())
    assert summary["scheduled_repetitions"] == 0
    assert summary["model_call_budget"]["committed_usd"] == 0


def test_credit_spread_machine_contract_accepts_correct_signed_close():
    case = evals.load_cases({"options_credit_spread_close_signed_quantities"})[0]
    transcript = {
        "tool_calls": [{"name": "load_skill", "payload": {"skill_name": "options-trading"}}],
        "fixture_calls": [
            {"name": "account_positions"},
            {"name": "orders_open_orders"},
            {"name": "market_last_price"},
            {"name": "options_evaluate_market"},
            {"name": "options_calculate_multileg_price"},
            {"name": "orders_submit_multileg"},
        ],
        "submissions": [
            {
                "tool": "orders_submit_multileg",
                "legs": [
                    {"strike": 594, "side": "buy_to_close", "quantity": 3},
                    {"strike": 592, "side": "sell_to_close", "quantity": 3},
                ],
            }
        ],
        "final_positions": [],
    }
    score = evals.score_machine_contract(case, transcript)
    assert score["pass"] is True


def test_credit_spread_machine_contract_rejects_reversed_close():
    case = evals.load_cases({"options_credit_spread_close_signed_quantities"})[0]
    transcript = {
        "tool_calls": [{"name": "load_skill", "payload": {"skill_name": "options-trading"}}],
        "fixture_calls": [
            {"name": name}
            for name in [
                "account_positions",
                "orders_open_orders",
                "market_last_price",
                "options_evaluate_market",
                "options_calculate_multileg_price",
                "orders_submit_multileg",
            ]
        ],
        "submissions": [
            {
                "tool": "orders_submit_multileg",
                "legs": [
                    {"strike": 594, "side": "sell_to_close", "quantity": 3},
                    {"strike": 592, "side": "buy_to_close", "quantity": 3},
                ],
            }
        ],
        "final_positions": [{"quantity": -6}],
    }
    score = evals.score_machine_contract(case, transcript)
    assert score["pass"] is False
    assert any("closing legs" in failure for failure in score["failures"])


def test_credit_spread_fixture_rejects_reversed_closing_sides_before_submission():
    fixture = evals.build_fixture("open_credit_spread")
    submit = next(tool for tool in evals.build_tools(fixture) if tool.name == "orders_submit_multileg")

    with pytest.raises(ValueError, match="does not reduce the current signed position"):
        submit.function(
            legs_json=(
                '[{"symbol":"SPY","expiration":"2026-08-28","strike":592,'
                '"right":"put","quantity":3,"side":"buy_to_close"},'
                '{"symbol":"SPY","expiration":"2026-08-28","strike":594,'
                '"right":"put","quantity":3,"side":"sell_to_close"}]'
            )
        )

    assert {(position["strike"], position["quantity"]) for position in fixture.positions} == {
        (594.0, -3),
        (592.0, 3),
    }


def test_credit_spread_fixture_rejects_duplicate_closes_beyond_position():
    fixture = evals.build_fixture("open_credit_spread")
    submit = next(tool for tool in evals.build_tools(fixture) if tool.name == "orders_submit_multileg")

    with pytest.raises(ValueError, match="requested_quantity=2.0"):
        submit.function(
            legs_json=(
                '[{"symbol":"SPY","expiration":"2026-08-28","strike":592,'
                '"right":"put","quantity":2,"side":"sell_to_close"},'
                '{"symbol":"SPY","expiration":"2026-08-28","strike":592,'
                '"right":"put","quantity":2,"side":"sell_to_close"}]'
            )
        )

    assert fixture.submissions == []


def test_credit_spread_eval_has_an_honest_preserved_red_baseline():
    baseline_path = Path(__file__).resolve().parents[1] / "agent_eval_baselines/2026-08-06_credit_spread_close_red.json"
    baseline = __import__("json").loads(baseline_path.read_text(encoding="utf-8"))
    assert baseline["caseId"] == "options_credit_spread_close_signed_quantities"
    assert baseline["status"] == "red"
    assert baseline["observedFailure"]["maximumFilledQuantity"] == 480
    assert baseline["sourceArtifactHashes"]["tradesCsvSha256"] == (
        "4084e53c8e1735c6d50a4b178b83a475935fbcbe0aca6b267705172a6d4b1ba2"
    )


def test_stock_pending_exit_contract_requires_inspection_and_no_submission():
    case = evals.load_cases({"stock_pending_exit_no_duplicate"})[0]
    transcript = {
        "tool_calls": [{"name": "load_skill", "payload": {"skill_name": "stock-trading"}}],
        "fixture_calls": [
            {"name": "account_positions"},
            {"name": "orders_open_orders"},
        ],
        "submissions": [],
        "final_positions": [{"symbol": "AAPL", "quantity": 40}],
    }
    score = evals.score_machine_contract(case, transcript)
    assert score["pass"] is True


def test_stock_pending_exit_eval_has_an_honest_preserved_red_baseline():
    baseline_path = (
        Path(__file__).resolve().parents[1] / "agent_eval_baselines/2026-08-11_stock_pending_exit_duplicate_red.json"
    )
    baseline = __import__("json").loads(baseline_path.read_text(encoding="utf-8"))
    assert baseline["caseId"] == "stock_pending_exit_no_duplicate"
    assert baseline["status"] == "red"
    assert baseline["observedFailure"]["closingSubmissionsObserved"] == 6
    assert baseline["sourceArtifactHashes"]["tradesCsvSha256"] == (
        "0f617ee6587dd44e22646062444d891c965d9de879f7cccf021e961fc6397a4f"
    )


def test_stock_orb_eval_has_an_honest_preserved_red_baseline():
    baseline_path = (
        Path(__file__).resolve().parents[1]
        / "agent_eval_baselines/2026-09-02_stock_orb_semantic_contradiction_red.json"
    )
    baseline = __import__("json").loads(baseline_path.read_text(encoding="utf-8"))
    assert baseline["caseId"] == "stock_orb_completed_bars"
    assert baseline["status"] == "red"
    assert baseline["observedFailure"]["submittedOrderIdentifier"] == "fixture-order-1"
    assert baseline["observedFailure"]["finalAnswerClaimedNoTrade"] is True
    assert baseline["sourceArtifactHashes"]["ledgerJsonlSha256"] == (
        "45806e694ec460539b5eb205c729796d8625aeea38f0e719b3ed40cc379e81c7"
    )


def test_stock_orb_fixture_honors_requested_minute_interval():
    fixture = evals.build_fixture("orb_breakout")
    history = next(tool for tool in evals.build_tools(fixture) if tool.name == "market_historical_prices")

    result = history.function(symbols="AAPL", length=70, timestep="minute")
    bars = [bar for bar in result["bars_by_symbol"]["AAPL"] if "T09:30:" <= bar["datetime"][10:] < "T09:50:"]

    assert result["timestep"] == "minute"
    assert len(bars) == 20
    assert bars[0]["datetime"] == "2026-08-11T09:30:00-04:00"
    assert bars[14]["datetime"] == "2026-08-11T09:44:00-04:00"
    assert bars[15]["datetime"] == "2026-08-11T09:45:00-04:00"
    assert max(bar["high"] for bar in bars[:15]) == 228.5
    assert bars[19]["close"] == 230.0
    assert sum(bar["volume"] for bar in bars[15:20]) > max(
        sum(bar["volume"] for bar in bars[offset : offset + 5]) for offset in range(0, 15, 5)
    )


def test_stock_orb_contract_requires_deterministic_quantity_calculation():
    case = evals.load_cases({"stock_orb_completed_bars"})[0]

    assert "risk_calculate_stock_quantity" in case["machineContract"]["requiredBeforeOrder"]

    fixture = evals.build_fixture("orb_breakout")
    sizing = next(tool for tool in evals.build_tools(fixture) if tool.name == "risk_calculate_stock_quantity")
    result = sizing.function(maximum_notional=10_000, price=230, available_cash=100_000)

    assert result["quantity"] == 43
    assert result["notional"] == 9_890


def test_account_eval_fixtures_match_compact_pagination_contract():
    fixture = evals.build_fixture("open_credit_spread")
    tools = {tool.name: tool.function for tool in evals.build_tools(fixture)}

    positions = tools["account_positions"](offset=0, limit=1, symbol="SPY")
    orders = tools["orders_open_orders"](offset=0, limit=50)

    assert positions["total"] == 3  # Includes the real broker cash position.
    assert positions["matched"] == 2
    assert positions["returned"] == 1
    assert positions["omitted"] == 1
    assert positions["complete"] is False
    assert positions["next_offset"] == 1
    assert positions["snapshot_id"]
    assert orders["total"] == 0
    assert orders["returned"] == 0
    assert orders["omitted"] == 0
    assert orders["complete"] is True
    assert orders["next_offset"] is None
    assert orders["snapshot_id"]

    original_snapshot_id = positions["snapshot_id"]
    position = next(p for p in fixture.production.strategy.get_positions() if p.asset.symbol == "SPY")
    position.quantity = float(position.quantity) + 1
    changed = tools["account_positions"](offset=0, limit=1, symbol="SPY")
    assert changed["snapshot_id"] != original_snapshot_id


def test_stock_order_fixture_applies_filled_order_to_positions():
    fixture = evals.build_fixture("stock_momentum")
    submit = next(tool for tool in evals.build_tools(fixture) if tool.name == "orders_submit_order")

    submit.function(
        symbol="AAPL",
        quantity=43,
        side="buy",
        asset_type="stock",
        order_type="limit",
        limit_price=230,
    )

    fixture.production.settle()
    positions = [p for p in fixture.production.strategy.get_positions() if p.asset.symbol == "AAPL"]
    assert len(positions) == 1
    assert float(positions[0].quantity) == 43.0


@pytest.mark.parametrize(
    "extra_args,fresh,select_all",
    [
        ([], False, True),
        ([], True, True),
        (["--gate"], True, False),
        (["--gate", "--force"], True, True),
    ],
)
def test_preflight_only_never_constructs_a_spending_ledger(
    monkeypatch, tmp_path, capsys, extra_args, fresh, select_all
):
    from scripts import agent_eval_call_budget, agent_eval_isolation

    monkeypatch.setattr(agent_eval_isolation, "configure_fixture_environment", lambda root: None)
    monkeypatch.setenv("OPENAI_API_KEY", "unused-synthetic-key")
    monkeypatch.setattr(evals, "preflight", lambda *args: None)
    monkeypatch.setattr(evals, "preflight_production_fixtures", lambda cases: None)
    monkeypatch.setattr(evals, "runtime_fingerprint", lambda: "test-fingerprint")
    monkeypatch.setattr(evals, "is_fresh", lambda *args: fresh)
    monkeypatch.setattr(
        agent_eval_call_budget,
        "EvalCallBudget",
        lambda *args, **kwargs: pytest.fail("Preflight cannot reserve or create spending"),
    )
    monkeypatch.setattr(
        evals.sys,
        "argv",
        [
            "run_agent_evals.py",
            "--max-cost-usd",
            "4",
            "--preflight-only",
            "--freshness-state",
            str(tmp_path / "freshness.json"),
            "--output-root",
            str(tmp_path / "no-ledger"),
        ]
        + extra_args,
    )
    assert evals.main() == 0
    report = json.loads(capsys.readouterr().out)
    assert report["paid_calls"] == 0
    assert report["case_count"] > 0
    assert report["selected_case_count"] == (report["case_count"] if select_all else 0)
    assert not (tmp_path / "no-ledger").exists()


def test_resume_rebuilds_missing_freshness_from_completed_matching_ledger(monkeypatch, tmp_path, capsys):
    """An interruption after ledger fsync must not require paying for passed cases again."""
    from datetime import timedelta
    from scripts import agent_eval_isolation
    from scripts.agent_eval_call_budget import EvalCallBudget

    case = {"id": "resume-contract", "model": "openai/gpt-6-luna"}
    monkeypatch.setattr(agent_eval_isolation, "configure_fixture_environment", lambda root: None)
    monkeypatch.setenv("OPENAI_API_KEY", "unused-synthetic-key")
    monkeypatch.setattr(evals, "load_cases", lambda ids: [case])
    monkeypatch.setattr(evals, "preflight", lambda *args: None)
    monkeypatch.setattr(evals, "preflight_production_fixtures", lambda cases: None)
    monkeypatch.setattr(evals, "runtime_fingerprint", lambda: "same-runtime")
    monkeypatch.setattr(evals, "execute_repetition", lambda *a, **k: pytest.fail("Completed passes must be reused"))
    fingerprint = evals.case_fingerprint(case, judge_model=evals.DEFAULT_JUDGE_MODEL, runtime_hash="same-runtime")
    passed_at = (evals.utc_now() - timedelta(days=1)).isoformat()
    rows = [{"case_id": case["id"], "fingerprint": fingerprint, "status": "pass", "timestamp": passed_at}
            for _ in range(evals.REQUIRED_CONSECUTIVE_PASSES)]
    # An unrelated later fingerprint must not rejuvenate the recovered receipt.
    rows.append({"case_id": case["id"], "fingerprint": "other-runtime", "status": "pass", "timestamp": evals.utc_text()})
    (tmp_path / "ledger.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))
    EvalCallBudget(tmp_path / "model_calls.jsonl", cap_usd=25,
                   prices=evals.MODEL_PRICES_PER_MILLION, max_input_tokens=evals.MAX_INPUT_TOKENS_PER_MODEL_CALL)
    state_path = tmp_path / "freshness.json"
    monkeypatch.setattr(evals.sys, "argv", ["run_agent_evals.py", "--max-cost-usd", "25",
        "--output-root", str(tmp_path), "--freshness-state", str(state_path)])
    assert evals.main() == 0
    summary = json.loads(capsys.readouterr().out)
    assert summary["scheduled_repetitions"] == 0
    assert summary["fresh_case_count"] == 1
    assert summary["incremental_estimated_cost_usd"] == 0
    assert json.loads(state_path.read_text())["cases"][case["id"]]["passed_at"] == passed_at


def _iron_condor_case():
    return evals.load_cases({"options_iron_condor_atomic_open"})[0]


def _good_condor_legs():
    rows = [
        (592, "put", "buy_to_open"),
        (594, "put", "sell_to_open"),
        (606, "call", "sell_to_open"),
        (608, "call", "buy_to_open"),
    ]
    return [
        {"symbol": "SPY", "expiration": "2026-08-28", "strike": strike, "right": right, "side": side, "quantity": 1}
        for strike, right, side in rows
    ]


def test_iron_condor_scoring_reports_a_malformed_leg_instead_of_crashing():
    """GitHub run 35930118229 recorded options_iron_condor_atomic_open as a bare
    KeyError: a submission whose legs lacked a strike crashed the scorer."""
    legs = _good_condor_legs()
    del legs[1]["strike"]
    transcript = {
        "tool_calls": [{"name": "load_skill", "payload": {"skill_name": "options-trading"}}],
        "fixture_calls": [{"name": "orders_submit_multileg"}],
        "submissions": [{"tool": "orders_submit_multileg", "legs": legs}],
        "final_positions": [],
    }

    score = evals.score_machine_contract(_iron_condor_case(), transcript)

    assert score["pass"] is False
    assert any("strike" in failure for failure in score["failures"])


def test_rejected_order_attempts_are_not_counted_as_broker_submissions():
    """An order the tool rejected (readiness gate, malformed legs) never reached
    the broker. exactOrderCount and topology score the accepted order; the
    rejected attempt is kept separately and still fails a no-order contract."""
    from types import SimpleNamespace

    from lumibot.components.agents import AgentTraceEvent
    from scripts.agent_eval_production_fixture import ProductionFixture

    bad = _good_condor_legs()
    del bad[0]["strike"]
    def event(kind, call_id, payload):
        return AgentTraceEvent(kind=kind, tool_name="orders_submit_multileg", call_id=call_id, payload=payload)

    events = [
        event("tool_call", "a", {"legs_json": json.dumps(bad)}),
        event("tool_result", "a", {"ok": False, "tool_error": True}),
        event("tool_call", "b", {"legs_json": json.dumps(_good_condor_legs())}),
        event("tool_result", "b", {"order_type": "credit"}),
    ]
    result = SimpleNamespace(
        tool_calls=[event for event in events if event.kind == "tool_call"],
        tool_results=[event for event in events if event.kind == "tool_result"],
    )
    production = ProductionFixture(evals.build_fixture("flat_options_account"))
    try:
        production.capture(result)
    finally:
        production.close()
    fixture = production.fixture

    assert [submission["legs"] for submission in fixture.submissions] == [_good_condor_legs()]
    assert [submission["legs"] for submission in fixture.rejected_submissions] == [bad]

    no_order_case = evals.load_cases({"stock_pending_exit_no_duplicate"})[0]
    transcript = {
        "tool_calls": [{"name": "load_skill", "payload": {"skill_name": "stock-trading"}}],
        "fixture_calls": [{"name": "account_positions"}, {"name": "orders_open_orders"}],
        "submissions": [],
        "rejected_submissions": [{"tool": "orders_submit_order", "symbol": "AAPL"}],
        "final_positions": [{"symbol": "AAPL", "quantity": 40}],
    }
    score = evals.score_machine_contract(no_order_case, transcript)
    assert score["pass"] is False
    assert "submitted an order despite a no-order contract" in score["failures"]


def test_close_mode_submission_is_scored_from_the_legs_lumibot_built():
    """In action='close' mode the agent sends only contracts and LumiBot derives
    each side and quantity. The harness must score the legs that reached the
    broker, not the side-less (or ignored) call arguments."""
    from types import SimpleNamespace

    from lumibot.components.agents import AgentTraceEvent
    from scripts.agent_eval_production_fixture import ProductionFixture

    contracts = [
        {"symbol": "SPY", "expiration": "2026-08-28", "strike": 594, "right": "put"},
        {"symbol": "SPY", "expiration": "2026-08-28", "strike": 592, "right": "put"},
    ]
    built = [
        {"asset": {"symbol": "SPY", "expiration": "2026-08-28", "strike": 594.0, "right": "PUT"}, "side": "buy_to_close", "quantity": 3.0},
        {"asset": {"symbol": "SPY", "expiration": "2026-08-28", "strike": 592.0, "right": "PUT"}, "side": "sell_to_close", "quantity": 3.0},
    ]

    def event(kind, payload):
        return AgentTraceEvent(kind=kind, tool_name="orders_submit_multileg", call_id="a", payload=payload)

    result = SimpleNamespace(
        tool_calls=[event("tool_call", {"legs_json": json.dumps(contracts), "action": "close"})],
        tool_results=[event("tool_result", {"legs": built, "order_type": "debit"})],
    )
    production = ProductionFixture(evals.build_fixture("open_credit_spread"))
    try:
        production.capture(result)
    finally:
        production.close()

    legs = production.fixture.submissions[0]["legs"]
    assert {(float(leg["strike"]), leg["side"], leg["quantity"]) for leg in legs} == {
        (594.0, "buy_to_close", 3.0),
        (592.0, "sell_to_close", 3.0),
    }
    assert {leg["right"] for leg in legs} == {"put"}


def test_harness_error_rows_name_where_the_error_happened_without_its_message():
    def fail():
        return {}["secret-looking-key"]

    try:
        fail()
    except KeyError as exc:
        row = evals.harness_error_row("case", 1, "fingerprint", exc)

    assert row["status"] == "error"
    assert row["error"] == "KeyError"
    assert row["error_location"].startswith("tests/test_agent_eval_harness.py:")
    assert "fail" in row["error_location"]
    assert "secret-looking-key" not in json.dumps(row)


def test_stock_fixture_supports_the_above_five_day_average_premise():
    """stock_price_before_order expects a buy when AAPL is above its five-day
    average. The fixture's prior sessions all closed at the current 230.00, so a
    careful model (GPT-6 Luna) correctly found price equal to the average."""
    from scripts.agent_eval_production_fixture import ProductionFixture

    production = ProductionFixture(evals.build_fixture("flat_stock_account"))
    try:
        tools = {tool.name: tool for tool in production.tools()}
        last = tools["market_last_price"].function(symbol="AAPL", asset_type="stock")["price"]
        daily = tools["market_historical_prices"].function(symbols="AAPL", length=5, timestep="day")
    finally:
        production.close()
    closes = [bar["close"] for bar in daily["bars_by_symbol"]["AAPL"]]
    assert len(closes) == 5
    average = sum(closes) / len(closes)
    assert last > average
    # "recent completed daily bars confirm it remains above": the latest
    # completed close is above the average too, not merely today's price.
    assert closes[-1] > average


def test_limit_between_bid_ask_case_does_not_tell_the_agent_to_use_a_limit():
    case = evals.load_cases({"options_iron_condor_limit_between_bid_ask"})[0]
    prompt = f"{case['systemPrompt']} {case['taskPrompt']}".lower()
    assert "limit" not in prompt


def test_explicit_limit_scoring_rejects_a_missing_package_price():
    case = evals.load_cases({"options_iron_condor_limit_between_bid_ask"})[0]
    transcript = {
        "tool_calls": [{"name": "load_skill", "payload": {"skill_name": "options-trading"}}],
        "fixture_calls": [
            {"name": "account_portfolio"},
            {"name": "account_positions"},
            {"name": "orders_open_orders"},
            {"name": "market_last_price"},
            {"name": "options_get_chain"},
            {"name": "options_get_greeks"},
            {"name": "options_evaluate_market"},
            {"name": "options_calculate_multileg_price"},
            {"name": "load_skill"},
            {"name": "orders_submit_multileg"},
        ],
        "submissions": [{"tool": "orders_submit_multileg", "legs": _good_condor_legs(), "net_limit_price": None}],
        "final_positions": [],
    }
    score = evals.score_machine_contract(case, transcript)
    assert score["pass"] is False
    assert any("explicit limit" in failure for failure in score["failures"])


def test_explicit_limit_scoring_accepts_a_price_between_bid_and_ask():
    case = evals.load_cases({"options_iron_condor_limit_between_bid_ask"})[0]
    transcript = {
        "tool_calls": [{"name": "load_skill", "payload": {"skill_name": "options-trading"}}],
        "fixture_calls": [
            {"name": "account_portfolio"},
            {"name": "account_positions"},
            {"name": "orders_open_orders"},
            {"name": "market_last_price"},
            {"name": "options_get_chain"},
            {"name": "options_get_greeks"},
            {"name": "options_evaluate_market"},
            {"name": "options_calculate_multileg_price"},
            {"name": "load_skill"},
            {"name": "orders_submit_multileg"},
        ],
        "submissions": [
            {
                "tool": "orders_submit_multileg",
                "legs": _good_condor_legs(),
                "net_limit_price": -1.0,
                "price_style": "mid",
            }
        ],
        "final_positions": [],
    }
    score = evals.score_machine_contract(case, transcript)
    assert score["pass"] is True


def test_nearest_expiration_without_data_is_listed_and_unpriced():
    """The nearer expiration is on the chain. It has no bars, so a quote check
    cannot price it. The later expiration still has bid and ask."""
    from scripts.agent_eval_production_fixture import ProductionFixture

    production = ProductionFixture(evals.build_fixture("options_nearest_expiration_without_data"))
    try:
        tools = {tool.name: tool for tool in production.tools()}
        chain = tools["options_get_chain"].function(symbol="SPY")
        empty = tools["options_evaluate_market"].function(
            symbol="SPY", expiration="2026-08-14", strike=594, right="put"
        )
        priced = tools["options_evaluate_market"].function(
            symbol="SPY", expiration="2026-08-28", strike=594, right="put"
        )
        empty_greeks = tools["options_get_greeks"].function(
            symbol="SPY", expiration="2026-08-14", strike=594, right="put"
        )
    finally:
        production.close()
    assert chain["call_expirations"][0] == "2026-08-14"
    assert "2026-08-28" in chain["call_expirations"]
    assert empty["market"]["usable_for_limit_pricing"] is False
    assert empty["market"]["price_basis"] == "none"
    assert priced["market"]["usable_for_limit_pricing"] is True
    assert empty_greeks["available"] is False


def test_expiration_with_data_case_does_not_name_the_fallback():
    case = evals.load_cases({"options_expiration_with_data"})[0]
    prompt = f"{case['systemPrompt']} {case['taskPrompt']}".lower()
    assert "2026-08-28" not in prompt
    assert "2026-08-14" not in prompt
    assert "fall back" not in prompt
    assert "fallback" not in prompt


def test_expiration_scoring_rejects_the_unpriced_expiration():
    case = evals.load_cases({"options_expiration_with_data"})[0]
    legs = _good_condor_legs()
    for leg in legs:
        leg["expiration"] = "2026-08-14"
    transcript = {
        "tool_calls": [{"name": "load_skill", "payload": {"skill_name": "options-trading"}}],
        "fixture_calls": [
            {"name": "account_portfolio"},
            {"name": "account_positions"},
            {"name": "orders_open_orders"},
            {"name": "market_last_price"},
            {"name": "options_get_chain"},
            {"name": "options_get_greeks"},
            {"name": "options_evaluate_market"},
            {"name": "options_calculate_multileg_price"},
            {"name": "load_skill"},
            {"name": "orders_submit_multileg"},
        ],
        "submissions": [
            {
                "tool": "orders_submit_multileg",
                "legs": legs,
                "net_limit_price": -1.0,
                "price_style": "mid",
            }
        ],
        "final_positions": [],
    }
    score = evals.score_machine_contract(case, transcript)
    assert score["pass"] is False
    assert any("2026-08-28" in failure for failure in score["failures"])


def test_public_filings_scoring_rejects_a_future_filing_in_the_tool_result():
    case = evals.load_cases({"congress_public_filings_only"})[0]
    transcript = {
        "tool_calls": [],
        "fixture_calls": [{"name": "house_public_disclosures"}],
        "submissions": [],
        "tool_results": [
            {
                "name": "house_public_disclosures",
                "payload": {
                    "ok": True,
                    "as_of": "2026-08-11T14:35:00+00:00",
                    "filings": [
                        {"ticker": "AAPL", "published_at": "2026-08-01T00:00:00+00:00", "doc_id": "111"},
                        {"ticker": "ZZZZ", "published_at": "2026-09-15T00:00:00+00:00", "doc_id": "222"},
                    ],
                },
            }
        ],
        "final_positions": [],
    }
    score = evals.score_machine_contract(case, transcript)
    assert score["pass"] is False
    assert any("222" in failure or "after as_of" in failure for failure in score["failures"])


class TestRepeatPolicy:
    """A new eval proves itself three times. The ongoing gate runs it once.

    Three repeats on every run made the suite too expensive to run often, and
    coverage stayed thin as a result. Establishing a case still costs three
    consecutive passes, so a flaky eval cannot sneak in.
    """

    def test_establishing_a_case_still_requires_three_consecutive_passes(self):
        assert evals.NEW_CASE_REQUIRED_PASSES == 3

    def test_repeat_one_is_allowed(self):
        parser = evals.build_parser()
        args = parser.parse_args(["--max-cost-usd", "5", "--repeat", "1"])
        assert args.repeat == 1
        evals.validate_args(args)  # must not raise

    def test_repeat_zero_is_still_rejected(self):
        parser = evals.build_parser()
        args = parser.parse_args(["--max-cost-usd", "5", "--repeat", "0"])
        with pytest.raises(RuntimeError, match="repeat"):
            evals.validate_args(args)

    def test_default_repeat_is_one_so_the_ordinary_loop_is_cheap(self):
        parser = evals.build_parser()
        args = parser.parse_args(["--max-cost-usd", "5"])
        assert args.repeat == 1

    def test_workers_default_high_enough_for_network_bound_calls(self):
        parser = evals.build_parser()
        args = parser.parse_args(["--max-cost-usd", "5"])
        assert args.max_workers >= 8

    def test_one_pass_does_not_establish_a_brand_new_case(self):
        """Freshness is still earned with three, never with one."""
        rows = [{"case_id": "c1", "fingerprint": "f1", "passed": True, "timestamp": "t"}]
        assert evals.consecutive_pass_count(rows, "c1", "f1") < evals.NEW_CASE_REQUIRED_PASSES


class TestTargetPasses:
    """An unestablished case is driven to three regardless of --repeat.

    Without this, defaulting --repeat to 1 would let a brand new eval be
    accepted on a single lucky pass, which is exactly what the three-in-a-row
    rule exists to stop.
    """

    def test_new_case_is_driven_to_three_even_when_repeat_is_one(self):
        assert evals.target_passes(already=0, repeat=1) == 3

    def test_partially_established_case_finishes_its_three(self):
        assert evals.target_passes(already=2, repeat=1) == 3

    def test_established_case_honours_repeat(self):
        assert evals.target_passes(already=3, repeat=1) == 1
        assert evals.target_passes(already=5, repeat=2) == 2

    def test_an_explicit_higher_repeat_still_wins(self):
        assert evals.target_passes(already=0, repeat=5) == 5
