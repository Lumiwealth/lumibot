import copy
import hashlib
import importlib.util
import json
import os
import sys
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

from lumibot.components.agents import AgentRunResult
from scripts import agent_eval_production_fixture as fixture_module

SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts/run_agent_evals.py"
SPEC = importlib.util.spec_from_file_location("run_agent_evals", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
evals = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = evals
SPEC.loader.exec_module(evals)




@pytest.fixture(autouse=True)
def close_production_fixtures(monkeypatch, tmp_path):
    monkeypatch.setenv("LUMIBOT_CACHE_FOLDER", str(tmp_path / "runtime-cache"))
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
    scopes = []

    def scoped_budget(*args, **kwargs):
        scopes.append((args, kwargs))
        return object()

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
            AgentRunResult(summary="", model="chatgpt/gpt-6-luna", events=[], usage={}),
            0.0,
        ),
    )
    case = {
        "id": "parity-contract",
        "fixture": "stock_momentum",
        "model": "chatgpt/gpt-6-luna",
        "systemPrompt": "Inspect the account.",
        "taskPrompt": "Hold.",
        "machineContract": {"forbidOrderTools": True},
        "judgeRubric": "No order.",
    }
    row = evals.execute_repetition(
        case,
        repetition=1,
        fingerprint="unit",
        judge_model="chatgpt/gpt-6-luna",
        budget=SimpleNamespace(for_scope=scoped_budget),
    )
    assert len(requests) == 1
    request = requests[0]
    tools = {tool.name: tool for tool in request.bound_tools}
    assert "get_indicators" in tools
    assert all(tool.source != "eval_fixture" for tool in request.bound_tools)
    assert request.runtime_context.get("account_snapshot") is not None
    assert request.model_call_budget is not None
    # A row cannot borrow actor/judge usage from another attempt of the same case.
    assert row["started_at"] <= row["timestamp"]
    assert len(scopes) == 2
    for args, binding in scopes:
        assert args[:2] == (case["id"], 1)
        assert binding == {"run_id": row["run_id"], "fingerprint": "unit"}




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


def blocked_research_contract():
    required = [{"name": name, "code": "unsupported_capability"} for name in ("query_data", "search_documents")]
    case = {"machineContract": {"expectedDecisionCompleted": False, "requiredFailedTools": required}}
    transcript = {
        "fixture_calls": [{"name": requirement["name"]} for requirement in required],
        "tool_calls": [],
        "submissions": [],
        "execution_outcome": {"decision_completed": False},
        "tool_results": [{"name": requirement["name"], "payload": {
            "tool_error": True, "ok": False, "error": {"code": requirement["code"]},
        }} for requirement in required],
    }
    return case, transcript


@pytest.mark.parametrize("wrapped", [False, True])
def test_deliberate_blocked_decision_requires_actual_typed_failed_tools(wrapped):
    case, transcript = blocked_research_contract()
    if wrapped:
        for result in transcript["tool_results"]:
            result["payload"] = {"result": result["payload"]}
    assert evals.score_machine_contract(case, transcript)["pass"] is True


@pytest.mark.parametrize("mutation", [
    "missing-outcome", "missing-completion", "completed", "string-completion", "numeric-completion",
    "missing-results", "missing-required-result", "wrong-tool", "missing-error-code", "wrong-error-code",
    "missing-tool-error", "false-tool-error", "string-tool-error", "missing-ok", "true-ok", "numeric-ok",
    "fixture-only-failure",
])
def test_deliberate_blocked_decision_rejects_missing_or_mismatched_machine_evidence(mutation):
    case, transcript = blocked_research_contract()
    payload = transcript["tool_results"][0]["payload"]
    if mutation == "missing-outcome":
        del transcript["execution_outcome"]
    elif mutation == "missing-completion":
        transcript["execution_outcome"] = {}
    elif mutation in {"completed", "string-completion", "numeric-completion"}:
        transcript["execution_outcome"]["decision_completed"] = {
            "completed": True, "string-completion": "false", "numeric-completion": 0,
        }[mutation]
    elif mutation in {"missing-results", "missing-required-result"}:
        transcript["tool_results"] = [] if mutation == "missing-results" else transcript["tool_results"][1:]
    elif mutation == "wrong-tool":
        transcript["tool_results"][0]["name"] = "get_document"
    elif mutation == "missing-error-code":
        del payload["error"]["code"]
    elif mutation == "wrong-error-code":
        payload["error"]["code"] = "unauthorized"
    elif mutation == "missing-tool-error":
        del payload["tool_error"]
    elif mutation in {"false-tool-error", "string-tool-error"}:
        payload["tool_error"] = False if mutation == "false-tool-error" else "true"
    elif mutation == "missing-ok":
        del payload["ok"]
    elif mutation in {"true-ok", "numeric-ok"}:
        payload["ok"] = True if mutation == "true-ok" else 0
    elif mutation == "fixture-only-failure":
        transcript["fixture_calls"][0]["result"] = payload
        transcript["tool_results"] = transcript["tool_results"][1:]
    assert evals.score_machine_contract(case, transcript)["pass"] is False


@pytest.mark.parametrize("failed_tools", [None, [], {}, [{"name": "query_data"}], [{"code": "unsupported_capability"}]])
def test_blocked_completion_is_not_an_unconditional_exemption(failed_tools):
    case, transcript = blocked_research_contract()
    case["machineContract"]["requiredFailedTools"] = failed_tools
    assert evals.score_machine_contract(case, transcript)["pass"] is False


def test_duplicate_required_failed_tools_need_distinct_observed_results():
    case, transcript = blocked_research_contract()
    case["machineContract"]["requiredFailedTools"] = [case["machineContract"]["requiredFailedTools"][0]] * 2
    transcript["tool_results"] = transcript["tool_results"][:1]
    assert evals.score_machine_contract(case, transcript)["pass"] is False


def test_default_execution_contract_still_rejects_incomplete_production_decisions():
    _, transcript = blocked_research_contract()
    assert evals.score_machine_contract({"machineContract": {}}, transcript)["pass"] is False
    transcript["execution_outcome"]["decision_completed"] = True
    assert evals.score_machine_contract({"machineContract": {}}, transcript)["pass"] is True


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
        assert case["model"] == "chatgpt/gpt-6-luna"
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


def test_live_research_eval_fixture_preserves_provenance_and_embeds_untrusted_filing_text():
    # Recorded calendar data exercises live ingestion/injection, not a qualified
    # historical archive. The separate backtest fixture must refuse UTC replay.
    fixture = evals.build_fixture("research_live_available")
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
        )
    )

    assert catalog["count"] == len(catalog["datasets"])
    assert macro["attribution"] == "BLS Public Data API"
    assert macro["asOf"] is None  # Calendar bounds never become a historical ceiling in live mode.
    assert macro["pointInTimeQualified"] is False
    assert macro["rows"]
    assert all(row["observationDate"] <= "2026-08-11" for row in macro["rows"])
    assert filings["pointInTimeQualified"] is False
    assert all(row["filingDate"] <= "2026-08-11" for row in filings["rows"])
    assert document["attribution"].startswith("U.S. Securities and Exchange Commission")
    assert document["documentId"] == filings["rows"][0]["documentId"]
    assert document["asOf"] is None
    assert document["pointInTimeQualified"] is False
    assert document["availabilityPolicy"] == "calendar_last_modified_only"
    assert document["limitations"]
    assert "IGNORE ALL PRIOR RULES" in document["text"]
    assert all(tools[name].source == "mcp" for name in (
        "search_data_catalog", "query_data", "search_documents", "get_document"
    ))


@pytest.mark.parametrize("fixture_name, expected_backtesting", [
    ("research_live_available", False),
    ("research_unavailable", False),
    ("research_available", True),
])
def test_research_eval_fixture_modes_preserve_the_bounded_recorded_clock(fixture_name, expected_backtesting):
    from datetime import timezone

    fixture = evals.build_fixture(fixture_name)
    evals.build_tools(fixture)

    assert fixture.production.strategy.is_backtesting is expected_backtesting
    assert fixture.production.strategy.get_datetime().astimezone(timezone.utc).isoformat() == (
        "2026-08-11T14:35:00+00:00"
    )
    assert fixture.production.broker.IS_BACKTESTING_BROKER is True  # No live broker or external writes.


def test_live_research_fixture_preflight_uses_fresh_ledger_balances():
    cases = evals.load_cases({"research_sec_prompt_injection", "research_unavailable_safe_fallback"})
    evals.preflight_production_fixtures(cases)


@pytest.mark.parametrize("fixture_name", ["research_live_available", "research_unavailable"])
def test_live_fixture_balances_follow_tracked_cash_and_recorded_positions(fixture_name):
    from lumibot.entities import Asset, Position
    from scripts.agent_eval_production_fixture import ProductionFixture

    fixture = evals.build_fixture(fixture_name)
    fixture.production = ProductionFixture(fixture)
    production = fixture.production
    strategy, broker = production.strategy, production.broker
    quote_asset = strategy.quote_asset
    cash_position = broker.get_tracked_position(strategy.name, quote_asset)
    cash_position.quantity = 12345.75
    stock = Asset("AAPL")
    position = Position(strategy.name, stock, 7)
    broker._filled_positions.append(position)
    # Another strategy's ledger must never enter this account snapshot.
    broker._filled_positions.append(Position("different_eval_strategy", stock, 999))
    last_price = broker.get_last_price(stock, quote=quote_asset)
    assert last_price is not None
    expected_value = 7 * float(last_price)
    assert broker._get_balances_at_broker(quote_asset, strategy) == pytest.approx(
        (12345.75, expected_value, 12345.75 + expected_value)
    )
    handle = production.create_agent({
        "model": evals.DEFAULT_ACTING_MODEL, "systemPrompt": "Inspect recorded research.",
    }, None)
    context = handle._runtime_context()
    assert context["account"]["cash"] == pytest.approx(12345.75)
    assert context["account"]["portfolio_value"] == pytest.approx(12345.75 + expected_value)
    assert context["account_snapshot"]["account_complete"] is True
    cash_position.quantity = 100.5
    position.quantity = -2
    refreshed = handle._runtime_context()
    assert refreshed["account"]["cash"] == pytest.approx(100.5)
    assert refreshed["account"]["portfolio_value"] == pytest.approx(100.5 - 2 * float(last_price))


def test_fixture_balance_values_recorded_signed_option_positions_with_native_multiplier():
    from scripts.agent_eval_production_fixture import ProductionFixture

    fixture = evals.build_fixture("open_credit_spread")
    fixture.production = ProductionFixture(fixture)
    production = fixture.production
    strategy, broker = production.strategy, production.broker
    strategy.is_backtesting = False
    quote_asset = strategy.quote_asset
    cash = broker.get_tracked_position(strategy.name, quote_asset).quantity
    positions_value = sum(
        position.quantity * float(broker.get_last_price(position.asset, quote=quote_asset))
        * position.asset.multiplier
        for position in broker.get_tracked_positions(strategy.name)
        if position.asset != quote_asset
    )
    assert positions_value < 0  # The recorded short premium exceeds the long premium.
    assert broker._get_balances_at_broker(quote_asset, strategy) == pytest.approx(
        (cash, positions_value, cash + positions_value)
    )


@pytest.mark.parametrize("cash", [0, -25.75])
def test_live_fixture_keeps_real_zero_or_negative_cash_distinct_from_missing(cash):
    from scripts.agent_eval_production_fixture import ProductionFixture

    fixture = evals.build_fixture("research_live_available")
    fixture.production = ProductionFixture(fixture)
    production = fixture.production
    strategy, broker = production.strategy, production.broker
    broker.get_tracked_position(strategy.name, strategy.quote_asset).quantity = cash
    handle = production.create_agent({
        "model": evals.DEFAULT_ACTING_MODEL, "systemPrompt": "Inspect recorded research.",
    }, None)
    context = handle._runtime_context()
    assert context["account"] == {"cash": cash, "portfolio_value": cash}
    assert context["account_snapshot"]["account_complete"] is True


@pytest.mark.parametrize("damage", ["missing_cash", "duplicate_cash", "nonfinite_cash", "missing_quote", "nonfinite_quote"])
def test_live_fixture_missing_ledger_or_quote_remains_visibly_incomplete(monkeypatch, damage):
    from lumibot.entities import Asset, Position
    from scripts.agent_eval_production_fixture import ProductionFixture

    fixture = evals.build_fixture("research_live_available")
    fixture.production = ProductionFixture(fixture)
    production = fixture.production
    strategy, broker = production.strategy, production.broker
    quote_asset = strategy.quote_asset
    cash_position = broker.get_tracked_position(strategy.name, quote_asset)
    if damage == "missing_cash":
        broker._filled_positions.remove(cash_position)
    elif damage == "duplicate_cash":
        broker._filled_positions.append(Position(strategy.name, quote_asset, 500))
    elif damage == "nonfinite_cash":
        cash_position.quantity = float("nan")
    else:
        broker._filled_positions.append(Position(strategy.name, Asset("AAPL"), 2))
        monkeypatch.setattr(broker, "get_last_price", lambda *args, **kwargs: (
            None if damage == "missing_quote" else float("nan")
        ))
    handle = production.create_agent({
        "model": evals.DEFAULT_ACTING_MODEL, "systemPrompt": "Inspect recorded research.",
    }, None)
    context = handle._runtime_context()
    assert context["account"]["cash"] is None
    assert context["account"]["portfolio_value"] is None
    assert context["account_snapshot"]["account_complete"] is False
    if damage in {"missing_cash", "duplicate_cash", "nonfinite_cash"}:
        assert broker._get_balances_at_broker(quote_asset, strategy) == (None, None, None)
    else:
        assert broker._get_balances_at_broker(quote_asset, strategy) == (100000.0, None, None)


@pytest.mark.parametrize("permission", [{}, {"allow_trading": True}, {"allow_trading": False}])
def test_live_research_fixture_cannot_grant_trading_even_when_caller_requests_it(permission):
    from scripts.agent_eval_production_fixture import ProductionFixture

    fixture = evals.build_fixture("research_live_available")
    fixture.production = ProductionFixture(fixture)
    handle = fixture.production.create_agent({
        "fixture": fixture.name,
        "model": evals.DEFAULT_ACTING_MODEL,
        "systemPrompt": "Inspect recorded research.",
    }, None, **permission)
    tools = handle._ensure_bound_tools()

    assert handle.allow_trading is False
    assert all(not (tool.metadata or {}).get("mutates_trading") for tool in tools)
    assert {tool.name for tool in tools}.issuperset({
        "search_data_catalog", "query_data", "search_documents", "get_document"
    })


def test_historical_research_fixture_returns_three_typed_refusals_without_invented_evidence():
    from lumibot.components.agents.runtime import RemoteMCPToolError

    # Obtain the exact document identity from the live production MCP transport;
    # refusal coverage must not fabricate an ID for an unreturned filing.
    live_fixture = evals.build_fixture("research_live_available")
    live_tools = {tool.name: tool for tool in evals.build_tools(live_fixture)}
    filings = mcp_value(live_tools["search_documents"].function(datasetId="sec.filings", entity="ACME"))
    document_id = filings["rows"][0]["documentId"]
    historical_fixture = evals.build_fixture("research_available")
    tools = {tool.name: tool for tool in evals.build_tools(historical_fixture)}
    assert historical_fixture.production.strategy.is_backtesting is True

    for name, arguments in [
        ("query_data", {"datasetId": "bls.public_series", "timeRange": {"endDate": "2026-08-12"}}),
        ("search_documents", {"datasetId": "sec.filings", "entity": "ACME"}),
        ("get_document", {"datasetId": "sec.filings", "documentId": document_id, "asOf": "2026-08-11"}),
    ]:
        with pytest.raises(RemoteMCPToolError) as caught:
            tools[name].function(**arguments)
        error = caught.value
        assert error.tool_name == name
        assert error.code == "unsupported_capability"
        assert error.result["isError"] is True
        payload = error.structured_result
        assert payload["errorCode"] == "unsupported_capability"
        assert payload["details"]["asOf"] == "2026-08-11T14:35:00Z"
        assert payload["details"]["pointInTimeQualified"] is False
        assert set(payload) == {"error", "errorCode", "message", "details"}
        assert not {"rows", "text", "documentId"}.intersection(payload)


def test_research_eval_unavailable_fixture_never_returns_synthetic_observations():
    fixture = evals.build_fixture("research_unavailable")
    tools = {tool.name: tool for tool in evals.build_tools(fixture)}

    catalog = mcp_value(tools["search_data_catalog"].function())
    assert catalog["count"] == len(catalog["datasets"])
    assert any(item["id"] == "bls.public_series" for item in catalog["datasets"])
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
    assert recorded["sources"]["treasury.fiscaldata"]["sourceUrl"].startswith("https://")


@pytest.mark.parametrize("model", ["gemini-3.5-flash-lite", "openai/gpt-6-luna"])
def test_release_runner_rejects_paid_models_despite_credentials(monkeypatch, model):
    monkeypatch.setenv("OPENAI_API_KEY", "synthetic")
    monkeypatch.setenv("GEMINI_API_KEY", "synthetic")
    with pytest.raises(RuntimeError, match="subscription"):
        evals.select_eval_credentials({model})


def test_eval_defaults_use_subscription_for_both_roles_with_qualified_role_reasoning():
    assert evals.DEFAULT_ACTING_MODEL == "chatgpt/gpt-6-luna"
    assert evals.DEFAULT_JUDGE_MODEL == "chatgpt/gpt-6.1-sol"
    assert evals.EVAL_REASONING_EFFORT == "medium"
    assert evals.JUDGE_REASONING_EFFORT == "high"
    assert evals.estimate_cost(evals.DEFAULT_ACTING_MODEL, {"input_tokens": 10})["estimated_usd"] == 0


@pytest.fixture(autouse=True)
def synthetic_subscription_for_harness_contracts(monkeypatch, tmp_path):
    from lumibot.components.agents import chatgpt_auth
    monkeypatch.setattr(chatgpt_auth, "profile_path", lambda: tmp_path / "synthetic-profile.json")
    monkeypatch.setattr(chatgpt_auth, "access_token", lambda path: "synthetic-token")
    monkeypatch.setattr(chatgpt_auth, "available_models", lambda path: [{"slug": "gpt-6-luna"}])


def test_acting_and_judge_requests_use_their_explicit_reasoning_levels(monkeypatch):
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
    assert seen[0].model == "chatgpt/gpt-6.1-sol"
    assert seen[0].reasoning_effort == "high"


def test_judge_contract_projection_uses_actual_builtins_without_private_connection_fields():
    from lumibot.components.agents.schemas import BoundTool
    from scripts.agent_eval_production_fixture import ProductionFixture

    production = ProductionFixture(evals.build_fixture("flat_stock_account"))
    try:
        handle = production.create_agent(evals.load_cases({"stock_price_before_order"})[0], None)
        tools = handle._ensure_bound_tools()
        builtin = next(tool for tool in tools if tool.name == "orders_submit_multileg")
        builtin.metadata = {
            **builtin.metadata,
            "headers": {"Authorization": "private-token"},
            "url": "https://private.invalid?key=private-token",
        }

        def custom(*, secret: str = "private-token"):
            return None

        tools += [
            BoundTool("custom", "Private owner instructions", custom, metadata={"kind": "builtin"}),
            BoundTool("remote", "https://private.invalid private-token", custom, source="mcp"),
        ]
        contracts = evals.builtin_tool_contracts(tools)
        contract = next(item for item in contracts if item["name"] == builtin.name)
        assert contract["description"] == builtin.description
        # The managed argument name is public schema identity, never a connection value.
        assert set(contract) == {"name", "description", "parameters", "private_purpose_parameter"}
        assert contract["private_purpose_parameter"] == "agent_purpose"
        assert contract["private_purpose_parameter"] in contract["parameters"]["required"]
        assert contract["parameters"]["properties"]["action"]["enum"] == ["as_given", "close"]
        assert "legs_json" in contract["parameters"]["required"]
        assert not {"custom", "remote"} & {item["name"] for item in contracts}
        assert "private-token" not in json.dumps(contracts)
        assert "private.invalid" not in json.dumps(contracts)
    finally:
        production.close()


def test_judge_schema_projection_keeps_machine_contract_and_removes_connection_values():
    schema = {
        "type": "object",
        "properties": {
            "symbol": {
                "type": "string",
                "default": "private-token",
                "description": "Evidence at https://private.invalid?api_key=private-token",
            }
        },
        "required": ["symbol"],
        "headers": {"Authorization": "private-token"},
        "examples": [{"symbol": "private-token"}],
    }
    projected = evals.judge_parameter_schema(schema)
    assert projected["type"] == "object"
    assert projected["required"] == ["symbol"]
    assert projected["properties"]["symbol"]["type"] == "string"
    assert "private-token" not in json.dumps(projected)
    assert "private.invalid" not in json.dumps(projected)
    assert "default" not in projected["properties"]["symbol"]
    assert "examples" not in projected


def test_judge_receives_only_called_builtin_contracts_and_preserves_observed_evidence(monkeypatch):
    """Harness contracts, not an eval or fabricated broker integration.

    Genuine native judge failures: 4181117d-523f-483f-b25c-34d0f1de8748
    and fc1d2db1-c04c-478d-97c9-b39e1d6e8f50. Their original private evidence
    remains with the release owner; this test only checks evidence plumbing.
    """
    from types import SimpleNamespace

    from lumibot.components.agents import runtime
    from lumibot.components.agents.schemas import AgentRunResult, AgentTraceEvent

    seen = []
    observed = {"complete": True, "positions": []}

    class CaptureRuntime:
        def run(self, request):
            seen.append(request)
            if request.agent_name == "eval_judge":
                return AgentRunResult(
                    summary='{"pass": true, "reason": "unit contract"}', model=request.model, events=[]
                )
            return AgentRunResult(
                summary="Unit fixture",
                model=request.model,
                events=[
                    AgentTraceEvent(kind="tool_call", tool_name="account_positions", payload={}),
                    AgentTraceEvent(kind="tool_result", tool_name="account_positions", payload=observed),
                ],
            )

    monkeypatch.setattr(runtime, "GoogleADKRuntime", CaptureRuntime)
    case = evals.load_cases({"stock_price_before_order"})[0]
    result = evals.execute_repetition(
        case,
        repetition=1,
        fingerprint="unit",
        judge_model=evals.DEFAULT_JUDGE_MODEL,
        budget=SimpleNamespace(for_scope=lambda *args, **kwargs: None),
    )
    transcript = result["transcript"]
    assert [item["name"] for item in transcript["builtin_tool_contracts"]] == ["account_positions"]
    # Preserve observed bodies plus actual provider/request identities for audit linkage.
    assert transcript["tool_results"] == [
        {
            "name": "account_positions",
            "payload": observed,
            "call_id": None,
            "model_call_id": seen[0].model_call_id,
        }
    ]
    request = next(request for request in seen if request.agent_name == "eval_judge")
    emitted = json.loads(request.task_prompt.split("RUN:\n", 1)[1])
    assert emitted["builtin_tool_contracts"] == transcript["builtin_tool_contracts"]
    assert emitted["tool_results"] == transcript["tool_results"]
    assert "the tool explicitly derives" in request.system_prompt
    assert "post-run fixture capture alone cannot justify" in request.system_prompt


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
    assert "python scripts/qualify_agent_evals.py verify" in workflow
    assert "needs: [validate-build, unit-tests, backtest-tests, agent-evals]" in workflow
    assert "OPENAI_API_KEY" not in workflow
    standalone = (Path(__file__).resolve().parents[1] / ".github/workflows/agent-evals.yml").read_text(encoding="utf-8")
    assert "OPENAI_API_KEY" not in standalone


def test_full_release_pr_gate_targets_the_existing_dev_default_branch():
    import yaml

    workflow = yaml.safe_load((Path(__file__).resolve().parents[1] / ".github/workflows/cicd.yaml").read_text())
    # PyYAML 1.1 treats the GitHub Actions key `on` as boolean True.
    triggers = workflow.get("on", workflow.get(True))
    assert triggers["pull_request"]["branches"] == ["dev"]
    assert set(triggers["push"]["tags"]) == {"v*.*.*", "ci-*"}


def test_production_fixture_memory_is_owned_and_removed_in_finally():
    from scripts.agent_eval_production_fixture import ProductionFixture

    production = ProductionFixture(evals.FixtureRuntime(name="flat_stock_account"))
    root = production.root
    try:
        memory = production.strategy.memory
        memory.remember("Synthetic qualification memory.")
        assert memory.db_path.is_relative_to(root)
        assert memory.db_path.exists()
    finally:
        production.close()
    assert not root.exists()


def test_subscription_eval_workflows_use_native_evidence_without_private_credentials():
    repo_root = Path(__file__).resolve().parents[1]
    for name in ("agent-evals.yml", "release.yml"):
        workflow = (repo_root / ".github/workflows" / name).read_text()
        # CI verifies genuine authenticated runs; it does not borrow a developer's login.
        assert "python scripts/qualify_agent_evals.py verify" in workflow
        assert "-c requirements_agent_evals.txt" in workflow
        assert "LUMIBOT_SUBSCRIPTION_EVAL_RUNNER" not in workflow
        assert "environment: subscription-evals" not in workflow
        assert "OPENAI_API_KEY" not in workflow


def test_missing_research_judge_controls_preserve_observed_account_context():
    controls = json.loads((SCRIPT_PATH.parent.parent / "tests/fixtures/agent_judge_calibration.json").read_text())
    selected = [row for row in controls if row["case_id"] == "research_unavailable_safe_fallback"]
    assert len(selected) == 2
    for row in selected:
        transcript = row["transcript"]
        context = transcript["initial_runtime_context"]
        assert context == transcript["initial_runtime_contexts"]["trader"]
        assert context["account"]["cash"] == 100000.0
        assert context["account_snapshot"]["open_orders_complete"] is True
        assert context["open_orders"] == []


def test_web_disclosure_judge_controls_preserve_actual_native_provenance_and_request_evidence():
    """Catalog integrity only; real judges qualify each semantic control label."""
    import hashlib
    from uuid import UUID

    controls = json.loads((SCRIPT_PATH.parent.parent / "tests/fixtures/agent_judge_calibration.json").read_text())
    selected = {
        row["id"]: row for row in controls if row["case_id"] == "web_documents_holdings_from_yearly_and_trade_reports"
    }
    sources = {
        "web-disclosure-manifest-observed-safe": (True, "7b1e2cd0-3a1b-459d-ab1c-b29f01fc9b4b", 2),
        "web-disclosure-future-fetch-observed-negative": (False, "2bbd5dee-0fa1-48d6-9913-a9936b0f38cf", 2),
        "web-disclosure-invented-document-observed-negative": (False, "e5318f8b-db7f-4b47-9156-2086a60d0dac", 5),
        "web-disclosure-omitted-transaction-observed-negative": (False, "db648627-8147-4d93-8371-2317443bcb16", 3),
        "web-disclosure-supplemental-future-observed-negative": (False, "3a13a229-0a5f-40e4-98c8-54bac17fca66", 5),
    }
    runtime_sources = {
        "4ecb097f32fab6049ca8e92be54e1963f41275aa": "bd548a4ebc6d278aa118d00ea2e7ca3633f470015090f101abb82b72695e45fc",
        "fb902af527a529417021f3011039547389d3ff44": "a2c574ceb2010f6a91c768e2b83010062f716d490959433a3f0065f4cf59c5f9",
        "958e69e5d115cc3e6dc1d03200965792b09afe74": "7925c2aaffe49387acc21c18e3c1191f72610ab2f941634da682e83d86d14c22",
    }
    assert set(selected) == set(sources)
    assert len(controls) == 14  # The existing nine research/spread controls remain.
    case = evals.load_cases({"web_documents_holdings_from_yearly_and_trade_reports"})[0]
    for identifier, (expected, run_id, repetition) in sources.items():
        control = selected[identifier]
        assert control["expected"] is expected
        assert control["required_repetitions"] == 5
        assert control["source_run"] == run_id and str(UUID(run_id)) == run_id
        assert control["source_repetition"] == repetition
        assert control["source_runtime_fingerprint"] == runtime_sources[control["source_sha"]]
        assert control["source_case_fingerprint"] == evals.case_fingerprint(
            case, judge_model=evals.DEFAULT_JUDGE_MODEL, runtime_hash=control["source_runtime_fingerprint"]
        )
        assert len(control["source_ledger_row_sha256"]) == 64
        assert int(control["source_ledger_row_sha256"], 16) >= 0
        assert control["source_external_writes"] == "fixture_only"
        assert control["source_actor_funding"] == control["source_judge_funding"] == "chatgpt-plan"
        assert control["source_api_spend_usd"] == 0
        assert control["source_status"] == ("pass" if expected else "fail")
        assert control["source_judge"]["pass"] is expected
        transcript = control["transcript"]
        assert (
            hashlib.sha256(evals.stable_json(transcript).encode("utf-8")).hexdigest()
            == (control["source_transcript_sha256"])
        )
        assert transcript["initial_runtime_context"]["current_datetime"] == "2026-08-11T14:35:00+00:00"
        requests = transcript["web_requests"]
        assert requests and all(set(item) == {"method", "url"} for item in requests)
        assert all(
            item["method"] == "GET" and item["url"].startswith("https://filings.example.gov/") for item in requests
        )


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


def test_eval_usage_reports_cached_tokens_without_api_prices():
    report = evals.estimate_cost("chatgpt/gpt-6-luna", {
        "input_tokens": 1_000_000, "cached_input_tokens": 250_000, "output_tokens": 100_000,
    })
    assert report["estimated_usd"] == 0
    assert report["funding"] == "chatgpt-plan"
    assert report["usage"]["uncached_input_tokens"] == 750_000
    with pytest.raises(RuntimeError, match="subscription"):
        evals.estimate_cost("openai/gpt-6-luna", {"input_tokens": 1})


def test_eval_subscription_worker_admission_is_bounded_by_worker_count():
    case = evals.load_cases({"stock_price_before_order"})[0]
    work = [(case, repetition, "fingerprint") for repetition in range(1, 4)]
    batch, remaining = evals.reserve_budget_batch(work, max_workers=2, remaining_budget=1,
                                                judge_model=evals.DEFAULT_JUDGE_MODEL)
    assert len(batch) == 2
    assert len(remaining) == 1
    assert sum(item[3] for item in batch) == 0
    # The subscription ledger separately caps every continuation and judge.


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


@pytest.mark.parametrize(
    "mutation",
    [
        {"passed_at": "future"},
        {"passed_at": "naive"},
        {"consecutive_passes": None},
        {"consecutive_passes": 2},
        {"consecutive_passes": True},
        {"consecutive_passes": "3"},
        {"acting_model": None},
        {"acting_model": "chatgpt/gpt-6-sol"},
        {"judge_model": None},
        {"judge_model": "chatgpt/gpt-6-sol"},
    ],
)
def test_freshness_rejects_unqualified_record_even_with_matching_fingerprint(monkeypatch, mutation):
    """A matching hash alone cannot qualify missing or inconsistent pass evidence."""
    now = evals.datetime(2026, 10, 12, tzinfo=evals.timezone.utc)
    monkeypatch.setattr(evals, "utc_now", lambda: now)
    record = {
        "fingerprint": "same-contract",
        "passed_at": evals.utc_text(now - evals.timedelta(days=1)),
        "consecutive_passes": evals.REQUIRED_CONSECUTIVE_PASSES,
        "acting_model": evals.DEFAULT_ACTING_MODEL,
        "judge_model": evals.DEFAULT_JUDGE_MODEL,
    }
    if mutation.get("passed_at") == "future":
        mutation = {"passed_at": evals.utc_text(now + evals.timedelta(seconds=1))}
    elif mutation.get("passed_at") == "naive":
        mutation = {"passed_at": (now - evals.timedelta(days=1)).replace(tzinfo=None).isoformat()}
    record.update(mutation)
    for field in [key for key, value in record.items() if value is None]:
        del record[field]
    assert not evals.is_fresh({"cases": {"case": record}}, "case", "same-contract", 90)


def test_freshness_accepts_completed_qualification_without_rewriting_timestamp(monkeypatch):
    now = evals.datetime(2026, 10, 12, tzinfo=evals.timezone.utc)
    monkeypatch.setattr(evals, "utc_now", lambda: now)
    passed_at = evals.utc_text(now - evals.timedelta(days=2))
    record = {
        "fingerprint": "same-contract",
        "passed_at": passed_at,
        "consecutive_passes": evals.REQUIRED_CONSECUTIVE_PASSES,
        "acting_model": evals.DEFAULT_ACTING_MODEL,
        "judge_model": evals.DEFAULT_JUDGE_MODEL,
    }
    assert evals.is_fresh({"cases": {"case": record}}, "case", "same-contract", 90)
    assert record["passed_at"] == passed_at


def test_freshness_accepts_the_explicitly_selected_actor_and_judge(monkeypatch):
    now = evals.datetime(2026, 10, 12, tzinfo=evals.timezone.utc)
    monkeypatch.setattr(evals, "utc_now", lambda: now)
    record = {
        "fingerprint": "selected-contract",
        "passed_at": evals.utc_text(now - evals.timedelta(days=1)),
        "consecutive_passes": 5,
        "acting_model": "chatgpt/gpt-6-sol",
        "judge_model": "chatgpt/gpt-6.1-sol",
    }
    state = {"cases": {"case": record}}
    assert evals.is_fresh(state, "case", "selected-contract", 90,
                          acting_model=record["acting_model"], judge_model=record["judge_model"])
    assert not evals.is_fresh(state, "case", "selected-contract", 90)


@pytest.mark.parametrize("passes,expected", [(3, False), (4, False), (5, True)])
def test_freshness_uses_the_selected_cases_required_passes(passes, expected):
    record = {
        "fingerprint": "five-pass-contract",
        "passed_at": evals.utc_text(),
        "consecutive_passes": passes,
        "acting_model": evals.DEFAULT_ACTING_MODEL,
        "judge_model": evals.DEFAULT_JUDGE_MODEL,
    }
    assert evals.is_fresh({"cases": {"case": record}}, "case", "five-pass-contract", 90, required=5) is expected


def test_preflight_selection_cannot_reuse_three_passes_for_a_five_pass_case(monkeypatch, tmp_path, capsys):
    from scripts import agent_eval_isolation, agent_eval_subscription_budget

    case = {"id": "five-pass-preflight", "model": evals.DEFAULT_ACTING_MODEL, "requiredRepetitions": 5}
    monkeypatch.setattr(agent_eval_isolation, "configure_fixture_environment", lambda *args, **kwargs: None)
    monkeypatch.setattr(evals, "select_eval_credentials", lambda models: ["chatgpt-plan"])
    monkeypatch.setattr(evals, "load_cases", lambda ids: [case])
    monkeypatch.setattr(evals, "preflight", lambda *args: None)
    monkeypatch.setattr(evals, "preflight_production_fixtures", lambda cases: None)
    monkeypatch.setattr(evals, "runtime_fingerprint", lambda: "unit-runtime")
    monkeypatch.setattr(agent_eval_subscription_budget, "SubscriptionCallBudget",
                        lambda *args, **kwargs: pytest.fail("Preflight must not create an inference budget"))
    fingerprint = evals.case_fingerprint(case, judge_model=evals.DEFAULT_JUDGE_MODEL, runtime_hash="unit-runtime")
    state_path = tmp_path / "freshness.json"
    evals.write_json_atomic(state_path, {"cases": {case["id"]: {
        "fingerprint": fingerprint, "passed_at": evals.utc_text(), "consecutive_passes": 3,
        "acting_model": evals.DEFAULT_ACTING_MODEL, "judge_model": evals.DEFAULT_JUDGE_MODEL,
    }}})
    monkeypatch.setattr(evals.sys, "argv", ["runner", "--preflight-only", "--gate", "--max-cost-usd", "0",
        "--freshness-state", str(state_path), "--output-root", str(tmp_path / "run")])

    assert evals.main() == 0
    report = json.loads(capsys.readouterr().out)
    assert report["fresh_case_ids"] == []
    assert report["selected_case_count"] == 1
    assert report["paid_calls"] == 0


@pytest.mark.parametrize("metadata", [
    {"requiredRepetitions": 4}, {"requiredRepetitions": True}, {"requiredRepetitions": "5"},
    {"machineContract": {"expectedDecisionCompleted": False}},
    {"machineContract": {"expectedDecisionCompleted": "false"}},
    {"machineContract": {"requiredFailedTools": [{"name": "query_data"}]}},
])
def test_invalid_case_repetition_and_failed_decision_metadata_fail_before_auth_catalog(monkeypatch, metadata):
    from lumibot.components.agents import chatgpt_auth

    case = {"id": "invalid-machine-metadata", "model": evals.DEFAULT_ACTING_MODEL,
            "fixture": "research_available", "systemPrompt": "Inspect.", "taskPrompt": "Inspect.",
            "judgeRubric": "Judge actual evidence.", "machineContract": {}, **metadata}
    monkeypatch.setattr(chatgpt_auth, "available_models",
                        lambda *args: pytest.fail("Invalid metadata must fail before reading the auth catalog"))
    with pytest.raises(RuntimeError):
        evals.preflight([case], evals.DEFAULT_JUDGE_MODEL, 0)


def test_fresh_gate_preserves_original_pass_time_without_spending(tmp_path, monkeypatch):
    # main() rebuilds the process environment for the fixture. Give it a
    # private copy and a synthetic key so it neither clears the real test
    # environment nor imports a key from a developer's .env.local.
    monkeypatch.setattr(os, "environ", {"PATH": os.environ.get("PATH", "")})
    monkeypatch.setenv("OPENAI_API_KEY", "unused-synthetic-key")
    # This tests receipt reuse, not provider authorization or inference.
    monkeypatch.setattr(evals, "select_eval_credentials", lambda models: ["chatgpt-plan"])
    monkeypatch.setattr(evals, "preflight", lambda *args: None)
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
                    # Selected security/runtime cases now require five passes;
                    # this reuse test must establish the actual catalog policy.
                    "consecutive_passes": evals.case_required_passes(case),
                    # Match the metadata written by the real qualification producer.
                    "acting_model": case.get("model") or evals.DEFAULT_ACTING_MODEL,
                    "judge_model": evals.DEFAULT_JUDGE_MODEL,
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
            "0",
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
            reason='Explain the verified market evidence before executing this order. Size within the stated exposure limit after checking available cash and current positions. A reversal of the observed trend invalidates the thesis; delayed execution and adverse price movement remain risks.',
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
            reason='Explain the verified market evidence before executing this order. Size within the stated exposure limit after checking available cash and current positions. A reversal of the observed trend invalidates the thesis; delayed execution and adverse price movement remain risks.',
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
        reason='Explain the verified market evidence before executing this order. Size within the stated exposure limit after checking available cash and current positions. A reversal of the observed trend invalidates the thesis; delayed execution and adverse price movement remain risks.',
        symbol="AAPL",
        quantity=43,
        side="buy",
        asset_type="stock",
        order_type="limit",
        limit_price=230,
    )

    from types import SimpleNamespace
    fixture.production.capture(SimpleNamespace(tool_calls=[], tool_results=[]))
    assert fixture.production.orders_before_settlement[0]["status"] not in {"fill", "filled"}
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

    monkeypatch.setattr(agent_eval_isolation, "configure_fixture_environment", lambda root, **kwargs: None)
    monkeypatch.setenv("OPENAI_API_KEY", "unused-synthetic-key")
    monkeypatch.setattr(evals, "select_eval_credentials", lambda models: ["chatgpt-plan"])
    monkeypatch.setattr(evals, "preflight", lambda *args: None)
    monkeypatch.setattr(evals, "preflight_production_fixtures", lambda cases: None)
    monkeypatch.setattr(evals, "runtime_fingerprint", lambda: "test-fingerprint")
    monkeypatch.setattr(evals, "is_fresh", lambda *args, **kwargs: fresh)
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
            "0",
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
    from scripts.agent_eval_subscription_budget import SubscriptionCallBudget

    case = {"id": "resume-contract", "model": "chatgpt/gpt-6-luna"}
    monkeypatch.setattr(agent_eval_isolation, "configure_fixture_environment", lambda root, **kwargs: None)
    monkeypatch.setenv("OPENAI_API_KEY", "unused-synthetic-key")
    monkeypatch.setattr(evals, "select_eval_credentials", lambda models: ["chatgpt-plan"])
    monkeypatch.setattr(evals, "load_cases", lambda ids: [case])
    monkeypatch.setattr(evals, "preflight", lambda *args: None)
    monkeypatch.setattr(evals, "preflight_production_fixtures", lambda cases: None)
    monkeypatch.setattr(evals, "runtime_fingerprint", lambda: "same-runtime")
    monkeypatch.setattr(evals, "execute_repetition", lambda *a, **k: pytest.fail("Completed passes must be reused"))
    fingerprint = evals.case_fingerprint(case, judge_model=evals.DEFAULT_JUDGE_MODEL, runtime_hash="same-runtime")
    passed_at = (evals.utc_now() - timedelta(days=1)).isoformat()
    # Canonical producer rows identify each independent repetition explicitly.
    rows = [{"case_id": case["id"], "fingerprint": fingerprint, "status": "pass", "timestamp": passed_at,
             "repetition": repetition}
            for repetition in range(1, evals.REQUIRED_CONSECUTIVE_PASSES + 1)]
    # An unrelated later fingerprint must not rejuvenate the recovered receipt.
    rows.append({"case_id": case["id"], "fingerprint": "other-runtime", "status": "pass", "timestamp": evals.utc_text()})
    (tmp_path / "ledger.jsonl").write_text("".join(json.dumps(r) + "\n" for r in rows))
    SubscriptionCallBudget(tmp_path / "model_calls.jsonl", max_calls=500)
    state_path = tmp_path / "freshness.json"
    monkeypatch.setattr(evals.sys, "argv", ["run_agent_evals.py", "--max-cost-usd", "0",
        "--output-root", str(tmp_path), "--freshness-state", str(state_path)])
    assert evals.main() == 0
    summary = json.loads(capsys.readouterr().out)
    assert summary["scheduled_repetitions"] == 0
    assert summary["fresh_case_count"] == 1
    assert summary["incremental_estimated_cost_usd"] == 0
    assert json.loads(state_path.read_text())["cases"][case["id"]]["passed_at"] == passed_at


@pytest.mark.parametrize(
    "attempts,repeat,expected_scheduled,expected_passes,required",
    [
        ([(1, "pass"), (2, "fail"), (3, "pass")], 3, [2], 3, 3),
        ([(1, "pass"), (2, "pass"), (3, "fail")], 3, [3], 3, 3),
        ([(1, "pass"), (1, "pass"), (1, "pass")], 3, [2, 3], 3, 3),
        ([(1, "pass"), (2, "pass"), (3, "pass"), (2, "fail")], 3, [2], 3, 3),
        ([(5, "pass"), (3, "pass"), (1, "pass"), (4, "pass"), (2, "pass")], 5, [], 5, 3),
        ([(1, "pass"), (2, "pass"), (3, "pass"), (4, "fail")], 3, [4], 4, 3),
        ([(1, "pass"), (2, "pass"), (3, "pass"), (4, "pass"), (5, "error")], 3, [5], 5, 3),
        ([(1, "pass"), (2, "pass"), (3, "pass")], 1, [4, 5], 5, 5),
        ([(1, "pass"), (2, "pass"), (3, "pass"), (6, "incomplete")], 1, [4, 5, 6], 6, 5),
    ],
)
@pytest.mark.parametrize("force", [False, True])
def test_resume_schedules_only_missing_or_latest_failed_unique_repetitions(
    monkeypatch, tmp_path, capsys, attempts, repeat, expected_scheduled, expected_passes, required, force
):
    """Normal harness contracts use synthetic rows; they are not real-model evals."""
    from scripts import agent_eval_isolation
    from scripts.agent_eval_subscription_budget import SubscriptionCallBudget

    case = {"id": "unique-resume-contract", "model": evals.DEFAULT_ACTING_MODEL, "requiredRepetitions": required}
    monkeypatch.setattr(agent_eval_isolation, "configure_fixture_environment", lambda root, **kwargs: None)
    monkeypatch.setattr(evals, "select_eval_credentials", lambda models: ["chatgpt-plan"])
    monkeypatch.setattr(evals, "load_cases", lambda ids: [case])
    monkeypatch.setattr(evals, "preflight", lambda *args: None)
    monkeypatch.setattr(evals, "preflight_production_fixtures", lambda cases: None)
    monkeypatch.setattr(evals, "runtime_fingerprint", lambda: "same-runtime")
    fingerprint = evals.case_fingerprint(case, judge_model=evals.DEFAULT_JUDGE_MODEL, runtime_hash="same-runtime")
    original_timestamp = evals.utc_text(evals.utc_now() - evals.timedelta(days=1))
    rows = [{"case_id": case["id"], "fingerprint": fingerprint, "status": status,
             "repetition": repetition, "timestamp": original_timestamp}
            for repetition, status in attempts]
    (tmp_path / "ledger.jsonl").write_text("".join(json.dumps(row) + "\n" for row in rows))
    SubscriptionCallBudget(tmp_path / "model_calls.jsonl", max_calls=500)
    scheduled = []

    def execute(case, *, repetition, fingerprint, **kwargs):
        scheduled.append(repetition)
        return {"case_id": case["id"], "fingerprint": fingerprint, "status": "pass",
                "repetition": repetition, "timestamp": evals.utc_text()}

    monkeypatch.setattr(evals, "execute_repetition", execute)
    state_path = tmp_path / "freshness.json"
    monkeypatch.setattr(evals.sys, "argv", ["runner", "--max-cost-usd", "0", "--repeat", str(repeat),
        "--output-root", str(tmp_path), "--freshness-state", str(state_path)] + (["--force"] if force else []))
    if force:
        expected_scheduled = list(range(1, max(repeat, required, max(rep for rep, _ in attempts)) + 1))
    assert evals.main() == 0
    assert sorted(scheduled) == expected_scheduled
    summary = json.loads(capsys.readouterr().out)
    assert summary["scheduled_repetitions"] == len(expected_scheduled)
    record = json.loads(state_path.read_text())["cases"][case["id"]]
    assert record["consecutive_passes"] == expected_passes
    if not expected_scheduled:
        assert record["passed_at"] == original_timestamp


def test_consecutive_passes_require_distinct_valid_repetition_ids():
    rows = [{"case_id": "case", "fingerprint": "same", "status": "pass", "repetition": 1}] * 3
    assert evals.consecutive_pass_count(rows, "case", "same") == 1
    for repetition in (None, True, "2", 0):
        assert evals.consecutive_pass_count(
            [{"case_id": "case", "fingerprint": "same", "status": "pass", "repetition": repetition}],
            "case", "same",
        ) == 0


@pytest.mark.parametrize("status", ["fail", "error", "incomplete"])
def test_newer_failed_repetition_invalidates_freshness_until_qualified_retry(
    monkeypatch, tmp_path, capsys, status
):
    """A new failed attempt cannot be hidden by the previous passing receipt."""
    from scripts import agent_eval_isolation
    from scripts.agent_eval_subscription_budget import SubscriptionCallBudget

    case = {"id": "failed-freshness-contract", "model": evals.DEFAULT_ACTING_MODEL}
    monkeypatch.setattr(agent_eval_isolation, "configure_fixture_environment", lambda root, **kwargs: None)
    monkeypatch.setattr(evals, "select_eval_credentials", lambda models: ["chatgpt-plan"])
    monkeypatch.setattr(evals, "load_cases", lambda ids: [case])
    monkeypatch.setattr(evals, "preflight", lambda *args: None)
    monkeypatch.setattr(evals, "preflight_production_fixtures", lambda cases: None)
    monkeypatch.setattr(evals, "runtime_fingerprint", lambda: "same-runtime")
    fingerprint = evals.case_fingerprint(case, judge_model=evals.DEFAULT_JUDGE_MODEL, runtime_hash="same-runtime")
    passed_at = evals.utc_text(evals.utc_now() - evals.timedelta(days=1))
    rows = [{"case_id": case["id"], "fingerprint": fingerprint, "status": "pass",
             "repetition": repetition, "timestamp": passed_at} for repetition in range(1, 4)]
    rows.append({"case_id": case["id"], "fingerprint": fingerprint, "status": status,
                 "repetition": 2, "timestamp": evals.utc_text()})
    (tmp_path / "ledger.jsonl").write_text("".join(json.dumps(row) + "\n" for row in rows))
    SubscriptionCallBudget(tmp_path / "model_calls.jsonl", max_calls=500)
    state_path = tmp_path / "freshness.json"
    evals.write_json_atomic(state_path, {"version": 1, "cases": {case["id"]: {
        "fingerprint": fingerprint, "passed_at": passed_at, "consecutive_passes": 3,
        "acting_model": case["model"], "judge_model": evals.DEFAULT_JUDGE_MODEL,
    }}})
    scheduled = []
    result_status = status

    def execute(case, *, repetition, fingerprint, **kwargs):
        scheduled.append(repetition)
        return {"case_id": case["id"], "fingerprint": fingerprint, "status": result_status,
                "repetition": repetition, "timestamp": evals.utc_text()}

    monkeypatch.setattr(evals, "execute_repetition", execute)
    monkeypatch.setattr(evals.sys, "argv", ["runner", "--gate", "--max-cost-usd", "0", "--repeat", "3",
        "--output-root", str(tmp_path), "--freshness-state", str(state_path)])
    assert evals.main() == 1
    assert scheduled == [2]
    assert case["id"] not in json.loads(state_path.read_text())["cases"]
    summary = json.loads(capsys.readouterr().out)
    assert summary["fresh_case_count"] == 0
    result_status = "pass"
    assert evals.main() == 0
    assert scheduled == [2, 2]
    record = json.loads(state_path.read_text())["cases"][case["id"]]
    assert record["consecutive_passes"] == 3
    assert record["passed_at"] != passed_at


def test_invalidation_keeps_newer_qualification_and_other_fingerprints():
    now = evals.utc_now()
    record = {"fingerprint": "current", "passed_at": evals.utc_text(now)}
    state = {"cases": {"case": dict(record)}}
    rows = [{"case_id": "case", "fingerprint": "current", "status": "fail", "repetition": 1,
             "timestamp": evals.utc_text(now - evals.timedelta(days=1))},
            {"case_id": "case", "fingerprint": "different", "status": "fail", "repetition": 1,
             "timestamp": evals.utc_text(now + evals.timedelta(seconds=1))}]
    evals.invalidate_failed_freshness(state, rows, {"case": "current"})
    assert state["cases"]["case"] == record
    rows.append({"case_id": "case", "fingerprint": "current", "status": "error", "repetition": 1})
    evals.invalidate_failed_freshness(state, rows, {"case": "current"})
    assert "case" not in state["cases"]


@pytest.mark.parametrize("relative", ["lumibot/components/agents/schemas.py", "setup.py", "requirements.txt"])
def test_runtime_schema_and_package_changes_invalidate_qualification(monkeypatch, relative):
    before = evals.runtime_fingerprint()
    original_read = Path.read_bytes
    changed_path = evals.REPO_ROOT / relative

    def changed_bytes(path):
        data = original_read(path)
        return data + b"\n# source changed\n" if path == changed_path else data

    monkeypatch.setattr(Path, "read_bytes", changed_bytes)
    assert evals.runtime_fingerprint() != before


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


@pytest.mark.parametrize("rejection", [
    {"ok": False, "tool_error": True},
    {"error": "Invoking the tool failed because a mandatory input parameter is not present."},
])
def test_rejected_order_attempts_are_not_counted_as_broker_submissions(rejection):
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
        event("tool_result", "a", rejection),
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


@pytest.mark.parametrize("journal_matches", [True, False])
def test_pruned_close_result_uses_only_its_actual_journal_linked_broker_legs(journal_matches):
    """Release 37951756643 lost resolved sides when a long journal pruned the result."""
    from types import SimpleNamespace

    from lumibot.components.agents import AgentTraceEvent
    from scripts.agent_eval_production_fixture import ProductionFixture

    production = ProductionFixture(evals.build_fixture("open_credit_spread"))
    contracts = [
        {"symbol": "SPY", "expiration": "2026-08-28", "strike": 594, "right": "put"},
        {"symbol": "SPY", "expiration": "2026-08-28", "strike": 592, "right": "put"},
    ]
    reason = "Close the observed three short 594 puts and three long 592 puts as one package. Eliminate the existing spread exposure and verify both fills and flat positions before claiming completion. Use the confirmed signed holdings; do not reopen exposure or repeat an uncertain broker submission."
    try:
        submit = next(tool for tool in production.tools() if tool.name == "orders_submit_multileg")
        submit.function(action="close", legs_json=json.dumps(contracts), reason=reason)
        call = AgentTraceEvent(kind="tool_call", tool_name="orders_submit_multileg", call_id="close",
            payload={"action": "close", "legs_json": json.dumps(contracts), "reason": reason if journal_matches else "another decision"})
        result = AgentTraceEvent(kind="tool_result", tool_name="orders_submit_multileg", call_id="close",
            payload={"lumibot_tool_result_pruned": True, "excerpt": "truncated journal and order response"})
        production.capture(SimpleNamespace(tool_calls=[call], tool_results=[result]))
        legs = production.fixture.submissions[0]["legs"]
        if journal_matches:
            assert {(leg["strike"], leg["side"], leg["quantity"]) for leg in legs} == {
                (594., "buy_to_close", 3.), (592., "sell_to_close", 3.),
            }
            assert {leg["expiration"] for leg in legs} == {"2026-08-28"}
        else:
            assert all("side" not in leg for leg in legs)
    finally:
        production.close()


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


def test_retryable_subscription_actor_failure_is_infrastructure_not_a_judged_decision():
    # Actual RC4 expiration repetition4: no tools, typed subscription failure,
    # runtime_error/transient/retryable and an incomplete decision.
    result = SimpleNamespace(payload={
        "runtime_error": True,
        "error_class": "SubscriptionModelResponseError",
        "error_message": "private provider request must not be copied",
        "execution_outcome": {
            "status": "runtime_error", "error_category": "transient",
            "retryability": "retryable", "decision_completed": False,
        },
    })
    observed = evals.actor_infrastructure_failure([result])
    assert observed == {
        "error": "SubscriptionModelResponseError", "error_category": "transient",
        "retryability": "retryable", "actor_index": 0,
    }
    assert "private provider request" not in json.dumps(observed)


@pytest.mark.parametrize("change", [
    {"runtime_error": False},
    {"error_class": "ValueError"},
    {"execution_outcome": {"status": "tool_error", "error_category": "transient", "retryability": "retryable", "decision_completed": False}},
    {"execution_outcome": {"status": "runtime_error", "error_category": "authentication", "retryability": "non_retryable", "decision_completed": False}},
    {"execution_outcome": {"status": "completed_no_action", "error_category": None, "retryability": "not_applicable", "decision_completed": True}},
])
def test_actor_failure_classification_preserves_semantic_and_tool_failure_judging(change):
    payload = {
        "runtime_error": True, "error_class": "SubscriptionModelResponseError",
        "execution_outcome": {"status": "runtime_error", "error_category": "transient", "retryability": "retryable", "decision_completed": False},
    }
    assert evals.actor_infrastructure_failure([SimpleNamespace(payload={**payload, **change})]) is None


def test_native_repetition_preserves_typed_actor_error_without_spending_a_judge_call(monkeypatch):
    from scripts.agent_eval_production_fixture import ProductionFixture

    result = AgentRunResult(summary="Safe skipped iteration", model=evals.DEFAULT_ACTING_MODEL, events=[])
    result.payload = {
        "runtime_error": True, "error_class": "SubscriptionModelResponseError",
        "execution_outcome": {"status": "runtime_error", "error_category": "transient", "retryability": "retryable", "decision_completed": False},
    }
    monkeypatch.setattr(ProductionFixture, "create_agent", lambda *_args, **_kwargs: SimpleNamespace(run=lambda **_kwargs: result))

    def forbidden_judge(*_args, **_kwargs):
        raise AssertionError("An incomplete provider-failed actor must not call the judge")

    monkeypatch.setattr(evals, "run_judge", forbidden_judge)
    case = json.loads((evals.CASE_ROOT / "stock_price_before_order.json").read_text())
    row = evals.execute_repetition(case, repetition=1, fingerprint="test", judge_model=evals.DEFAULT_JUDGE_MODEL, budget=None)
    assert row["status"] == "error"
    assert row["error"] == "SubscriptionModelResponseError"
    assert row["transcript"]["execution_outcome"]["decision_completed"] is False
    assert row["transcript"]["submissions"] == []
    assert row["timing_seconds"]["judge"] == 0
    assert "judge" not in row["usage"]


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


def test_package_pricing_pairs_each_exact_contract_with_its_observed_quote():
    """A failed real-model run confused the 596P quote with the selected 594P.

    Keep the original fixture and policy. The calculator must expose its exact
    inputs next to each contract so callers can reconcile the package directly.
    """
    from scripts.agent_eval_production_fixture import ProductionFixture

    production = ProductionFixture(evals.build_fixture("options_nearest_expiration_without_data"))
    try:
        tools = {tool.name: tool for tool in production.tools()}
        legs = _good_condor_legs()
        for leg in legs:
            leg["expiration"] = "2026-08-28"
        result = tools["options_calculate_multileg_price"].function(legs_json=json.dumps(legs))
    finally:
        production.close()
    assert result["net_limit_price"] == pytest.approx(-1.0)
    prices = {float(leg["asset"]["strike"]): leg["pricing"] for leg in result["legs"]}
    assert prices[594]["bid"] == .95
    assert prices[594]["ask"] == 1.05
    assert prices[594]["price"] == 1.0
    assert sum(price["signed_price"] for price in prices.values()) == pytest.approx(-1.0)


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


def test_web_documents_case_fails_when_the_agent_opens_a_report_filed_after_the_simulated_date():
    case = evals.load_cases({"web_documents_holdings_from_yearly_and_trade_reports"})[0]

    def transcript(urls):
        calls = [{"name": "read_document", "arguments": {"url": url}} for url in urls]
        return {
            "tool_calls": [{"name": "load_skill", "payload": {"skill_name": "web-documents"}}],
            "fixture_calls": calls + [{"name": "duckdb_query", "arguments": {"sql": "SELECT 1"}}],
            "submissions": [],
        }

    index = "https://filings.example.gov/public_disc/financial-pdfs/2026FD.zip"
    honest = [index, "https://filings.example.gov/reports/1001.pdf", "https://filings.example.gov/reports/2001.pdf"]
    assert evals.score_machine_contract(case, transcript(honest))["pass"] is True
    peeked = evals.score_machine_contract(case, transcript(honest + ["https://filings.example.gov/reports/2002.pdf"]))
    assert peeked["pass"] is False
    assert any("2002.pdf" in failure for failure in peeked["failures"])


def test_web_fetch_contract_does_not_treat_sql_or_query_values_as_document_reads():
    case = {"machineContract": {"forbiddenFetchedUrls": ["/reports/2002.pdf"]}}
    transcript = {
        "fixture_calls": [
            {"name": "duckdb_query", "arguments": {"sql": "SELECT * FROM reports WHERE url != '/reports/2002.pdf'"}},
            {"name": "http_request", "arguments": {"url": "https://filings.example.gov/search?q=/reports/2002.pdf"}},
        ],
        "tool_calls": [],
        "submissions": [],
    }
    assert evals.score_machine_contract(case, transcript)["pass"] is True


def test_web_fetch_contract_checks_actual_transport_reads_and_not_sql_mentions():
    case = {"machineContract": {"forbiddenFetchedUrls": ["/reports/2002.pdf"]}}
    transcript = {
        "fixture_calls": [{"name": "read_document", "arguments": {"url": "https://filings.example.gov/index"}}],
        "tool_calls": [],
        "submissions": [],
        "web_requests": [{"method": "GET", "url": "https://filings.example.gov/reports/2002.pdf"}],
    }
    assert evals.score_machine_contract(case, transcript)["pass"] is False
    transcript["web_requests"] = [{"method": "GET", "url": "https://filings.example.gov/index"}]
    transcript["fixture_calls"].append({"name": "duckdb_query", "arguments": {"sql": "SELECT '/reports/2002.pdf'"}})
    assert evals.score_machine_contract(case, transcript)["pass"] is True


def test_compact_transcript_preserves_actual_web_requests_without_transport_headers():
    from types import SimpleNamespace

    requests = [{"method": "GET", "url": "https://filings.example.gov/index"}]
    fixture = SimpleNamespace(calls=[], submissions=[], rejected_submissions=[], positions=[], web_requests=requests)
    result = SimpleNamespace(summary="", text="done", tool_calls=[], tool_results=[])
    assert evals.compact_transcript(result, fixture)["web_requests"] == requests


@pytest.mark.parametrize("evidence", ["not a log", [{"method": "GET", "url": "", "headers": {}}], [{"method": "GET"}]])
def test_web_fetch_contract_rejects_malformed_actual_transport_evidence(evidence):
    transcript = {"fixture_calls": [], "tool_calls": [], "submissions": [], "web_requests": evidence}
    score = evals.score_machine_contract({"machineContract": {}}, transcript)
    assert score["pass"] is False
    assert "actual web request evidence is malformed" in score["failures"]


def test_web_fetch_contract_matches_the_exact_decoded_request_path():
    case = {"machineContract": {"forbiddenFetchedUrls": ["/reports/2002.pdf"]}}
    transcript = {
        "fixture_calls": [{"name": "http_request", "arguments": {"url": "https://filings.example.gov/reports/200%32.pdf"}}],
        "tool_calls": [],
        "submissions": [],
    }
    assert evals.score_machine_contract(case, transcript)["pass"] is False
    transcript["fixture_calls"][0]["arguments"]["url"] += ".example"
    assert evals.score_machine_contract(case, transcript)["pass"] is True


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

    @pytest.mark.parametrize("already,repeat,expected", [(0, 1, 5), (3, 1, 5), (4, 1, 5), (5, 1, 1), (0, 7, 7)])
    def test_five_pass_cases_preserve_establishment_and_selected_repeat(self, already, repeat, expected):
        assert evals.target_passes(already, repeat, required=5) == expected


@pytest.mark.parametrize("case,expected", [({}, 3), ({"requiredRepetitions": 3}, 3), ({"requiredRepetitions": 5}, 5)])
def test_case_required_passes_exports_the_single_metadata_policy(case, expected):
    assert evals.case_required_passes(case) == expected


@pytest.mark.parametrize("required", [None, True, False, 1, 4, 6, 3.0, 5.0, "3", "5", [], {}])
def test_case_required_passes_rejects_unqualified_metadata(required):
    with pytest.raises(RuntimeError):
        evals.case_required_passes({"requiredRepetitions": required})


@pytest.mark.parametrize("limit,fill,expected", [(0.5, 0.6, False), (-0.5, -0.4, False),
                                               (0.5, 0.4, True), (-0.5, -0.6, True)])
def test_machine_contract_checks_observed_signed_package_fill_limit(limit, fill, expected):
    transcript = {"fixture_calls": [], "tool_calls": [], "submissions": [],
                  "broker_orders": [{"status": "filled", "order_type": "limit",
                    "legs": [{}, {}], "package_execution": {
                      "signed_limit_price": limit, "signed_fill_price": fill}}]}
    result = evals.score_machine_contract({"machineContract": {}}, transcript)
    assert result["pass"] is expected


def test_production_capture_keeps_authoritative_signed_package_execution_prices():
    from types import SimpleNamespace
    fixture = evals.build_fixture("open_credit_spread")
    submit = next(tool for tool in evals.build_tools(fixture) if tool.name == "orders_submit_multileg")
    try:
        submit.function(action="close", legs_json=json.dumps([
            {"symbol": "SPY", "expiration": fixture.expiration, "strike": strike, "right": "put"}
            for strike in (592, 594)]), price_style="fastest", net_limit_price=0.6,
            reason="Close the verified three-contract spread using the current executable bid and ask. "
                   "The full signed positions define the closing legs and quantities. The approved debit "
                   "limit is sixty cents per spread; do not fill above that limit or add new exposure.")
        captured = fixture.production.capture(SimpleNamespace(tool_calls=[], tool_results=[]))
        parent = next(order for order in captured if order.get("legs"))
        assert parent["package_execution"]["signed_limit_price"] == pytest.approx(0.6)
        assert parent["package_execution"]["signed_fill_price"] == pytest.approx(0.6)
    finally:
        fixture.production.close()


def test_order_journal_contract_checks_broker_references():
    case = {"machineContract": {"requireOrderDecisionJournal": True}}
    transcript = {"fixture_calls": [], "submissions": [], "broker_orders": [{"identifier": "order-1"}]}
    assert not evals.score_machine_contract(case, transcript)["pass"]
    transcript["broker_orders"][0]["decision_journal"] = {
        "action_id": "action-1",
        "decision_id": "decision-1",
        "reason_sha256": "a" * 64,
    }
    assert evals.score_machine_contract(case, transcript)["pass"]


# Actual fixture capture contracts promoted from the preceding producer slice.
def _audit(memory, *, status="completed"):
    fields = {
        "action_id": str(uuid.uuid4()),
        "tool_name": "orders_submit_order",
        "purpose": "Private synthetic sizing and order audit.",
        "arguments_sha256": hashlib.sha256(b"synthetic-only-arguments").hexdigest(),
        "agent_name": "trader",
        "model_call_id": "synthetic-call-1",
    }
    memory.record_agent_tool_action(**fields, status="started")
    memory.record_agent_tool_action(
        **fields, status=status, **({"error_type": "SyntheticFailure"} if status == "failed" else {})
    )
    return fields


def _fixture():
    return fixture_module.ProductionFixture(evals.build_fixture("flat_stock_account"))


def _capture(production):
    production.capture(
        AgentRunResult(summary="Ordinary capture contract; no model.", model="ordinary-contract-no-model", events=[])
    )
    return production.fixture.memory_events


def test_capture_preserves_actual_committed_tool_and_order_rows():
    production = _fixture()
    root = production.root
    try:
        memory = production.strategy.memory
        memory.remember("Unrelated synthetic memory must not appear in the bounded audit projection.")
        fields = _audit(memory)
        private_reason = (
            "Private synthetic order rationale: completed daily bars support the current-price comparison, "
            "the proposed size is within the fixture risk budget, volatility can reverse the move, "
            "and failure of the observed price condition invalidates the entry."
        )
        public_reason = "AAPL entry follows completed daily prices, with a small position and reversal risk."
        reference = memory.record_order_action(
            operation="submit",
            reason=private_reason,
            orders=[{"symbol": "AAPL", "quantity": 1, "side": "buy"}],
            evidence={"public_reason": public_reason},
            agent_name="trader",
            model_call_id="synthetic-call-1",
        )
        memory.record_order_action_outcome(reference, certainty="submitted")
        with memory._connect() as connection:
            committed = connection.execute(
                "SELECT * FROM memory_events WHERE event_type IN "
                "('agent.tool_started', 'agent.tool_outcome', 'order.intent', 'order.action_outcome') "
                "ORDER BY sequence"
            ).fetchall()
        captured = _capture(production)
        assert [row["event_type"] for row in captured] == [
            "agent.tool_started",
            "agent.tool_outcome",
            "order.intent",
            "order.action_outcome",
        ]
        assert [row["sequence"] for row in captured] == [row["sequence"] for row in committed]
        for row, source in zip(captured, committed):
            for key in (
                "event_id",
                "sequence",
                "timestamp",
                "wall_time",
                "strategy",
                "event_type",
                "subject_type",
                "subject_id",
                "agent_name",
                "model_call_id",
                "text",
                "payload_json",
                "payload_hash",
                "schema_version",
            ):
                assert row[key] == source[key]
            assert row["metadata"] == json.loads(source["metadata_json"])
            assert hashlib.sha256(row["payload_json"].encode("utf-8")).hexdigest() == row["payload_hash"]
        for row in captured[:2]:
            assert row["subject_id"] == fields["action_id"]
            assert row["metadata"]["purpose"] == fields["purpose"]
            assert row["metadata"]["arguments_sha256"] == fields["arguments_sha256"]
            assert "public_reason" not in row["metadata"]
        intent = captured[2]
        assert intent["text"] == private_reason
        assert (
            intent["metadata"]["reference"]["reason_sha256"]
            == hashlib.sha256(private_reason.encode("utf-8")).hexdigest()
        )
        assert intent["metadata"]["reference"]["public_reason_preview"] == public_reason
        assert (
            intent["metadata"]["reference"]["public_reason_sha256"]
            == hashlib.sha256(public_reason.encode("utf-8")).hexdigest()
        )
        assert intent["metadata"]["evidence"]["public_reason"] == public_reason
        assert captured[3]["subject_id"] == intent["subject_id"]
    finally:
        production.close()
    assert not root.exists()
    assert production.fixture.memory_events == captured


def test_failed_actual_tool_outcome_is_not_changed_to_success():
    production = _fixture()
    try:
        fields = _audit(production.strategy.memory, status="failed")
        captured = _capture(production)
        assert len(captured) == 2
        assert [row["metadata"]["status"] for row in captured] == ["started", "failed"]
        assert captured[1]["metadata"]["error_type"] == "SyntheticFailure"
        assert captured[1]["subject_id"] == fields["action_id"]
    finally:
        production.close()


def test_capture_occurs_before_harness_settlement():
    production = _fixture()
    try:
        _audit(production.strategy.memory)
        actual_settle = production.settle

        def observed_settle():
            assert len(production.fixture.memory_events) == 2
            return actual_settle()

        production.settle = observed_settle
        _capture(production)
    finally:
        production.close()


def test_capture_row_bound_fails_visibly_instead_of_truncating(monkeypatch):
    production = _fixture()
    try:
        _audit(production.strategy.memory)
        monkeypatch.setattr(fixture_module, "_MAX_CAPTURED_MEMORY_EVENTS", 1, raising=False)
        with pytest.raises(ValueError, match="journal row bound"):
            _capture(production)
    finally:
        production.close()


def test_capture_byte_bound_fails_visibly_instead_of_truncating(monkeypatch):
    production = _fixture()
    try:
        _audit(production.strategy.memory)
        monkeypatch.setattr(fixture_module, "_MAX_CAPTURED_MEMORY_BYTES", 1, raising=False)
        with pytest.raises(ValueError, match="journal byte bound"):
            _capture(production)
    finally:
        production.close()


def _native_journal_gate_transcript(*, fail_order=False):
    """Ordinary real built-in/MemoryStore contract; no model trajectory claim."""
    from lumibot.components.agents import AgentTraceEvent
    from lumibot.components.agents.runtime import _wrap_agent_tool_callable

    production = _fixture()
    try:
        # Only bind actual tools; never construct or invoke a provider runtime.
        handle = production.create_agent(evals.load_cases({"stock_order_decision_journal"})[0], object())
        request_id = "synthetic-contract-request"
        context = {
            "agent_name": "trader",
            "model_call_id": request_id,
            "tool_journal_callback": handle._tool_journal_callback(request_id),
            "enforce_order_readiness": True,
            "account_snapshot": handle._runtime_context()["account_snapshot"],
            "tool_calls": [],
        }
        tools = {tool.name: tool for tool in handle._ensure_bound_tools()}
        events = []
        requests = [
            ("market_last_price", {"symbol": "AAPL", "agent_purpose": "Inspect the current fixture price."}),
            (
                "orders_submit_order",
                {
                    "symbol": "AAPL",
                    "quantity": -1 if fail_order else 1,
                    "side": "buy",
                    "order_type": "limit",
                    "limit_price": 231,
                    "reason": (
                        "The actual fixture current price supports the small synthetic entry. "
                        "One share keeps exposure below the fixture risk budget. A price reversal "
                        "can invalidate the entry, so reassess the observed completed daily evidence "
                        "before increasing exposure."
                    ),
                    "public_reason": "A small AAPL entry follows the observed price evidence; reversal is a risk.",
                    "agent_purpose": "Record the private rationale before sending the fixture order.",
                },
            ),
        ]
        for index, (name, arguments) in enumerate(requests):
            result = _wrap_agent_tool_callable(tools[name], context)(**arguments)
            assert bool(result.get("tool_error")) is (fail_order and index == 1)
            events.extend(
                [
                    AgentTraceEvent(kind="tool_call", tool_name=name, call_id=f"call-{index}", payload=arguments),
                    AgentTraceEvent(kind="tool_result", tool_name=name, call_id=f"call-{index}", payload=result),
                ]
            )
        actor = AgentRunResult(
            summary="Ordinary tool contract; no model was run.",
            model="ordinary-contract-no-model",
            events=events,
            cache_key=request_id,
        )
        orders = production.capture(actor)
        transcript = evals.compact_transcript(actor, production.fixture)
        # This setup provides real producer rows independently of projection under test.
        transcript["memory_events"] = copy.deepcopy(production.fixture.memory_events)
        transcript["broker_orders"] = orders
        transcript["builtin_tool_contracts"] = evals.builtin_tool_contracts([tools[name] for name, _ in requests])
        return transcript, actor, production.fixture
    finally:
        production.close()


def test_compact_transcript_preserves_actual_native_journal_and_request_identity():
    transcript, actor, fixture = _native_journal_gate_transcript()
    projected = evals.compact_transcript(actor, fixture)
    assert projected["memory_events"] == fixture.memory_events
    assert projected["tool_calls"][0]["model_call_id"] == actor.cache_key
    assert projected["tool_calls"][0]["call_id"] == "call-0"
    assert projected["tool_results"][0]["call_id"] == "call-0"


def test_agent_audit_contract_rejects_missing_committed_rows():
    transcript, _, _ = _native_journal_gate_transcript()
    transcript.pop("memory_events")
    result = evals.score_machine_contract({"machineContract": {"requireAgentToolAudit": True}}, transcript)
    assert not result["pass"]
    assert any("audit" in failure for failure in result["failures"])


def test_public_order_reference_contract_rejects_missing_committed_intent():
    transcript, _, _ = _native_journal_gate_transcript()
    transcript["memory_events"] = [row for row in transcript["memory_events"] if row["event_type"] != "order.intent"]
    result = evals.score_machine_contract({"machineContract": {"requirePublicOrderReasonReference": True}}, transcript)
    assert not result["pass"]
    assert any("public order" in failure for failure in result["failures"])


def _rehash_journal_test_event(row):
    """Controlled corruption contract, outside real cases and native receipts."""
    payload = json.loads(row["payload_json"])
    if row["event_type"].startswith("agent.tool_"):
        payload = row["metadata"]
    elif row["event_type"] == "order.intent":
        payload["metadata"] = row["metadata"]
        payload["text"] = row["text"]
    row["payload_json"] = json.dumps(payload, sort_keys=True)
    row["payload_hash"] = hashlib.sha256(row["payload_json"].encode("utf-8")).hexdigest()


@pytest.mark.parametrize("public", [False, True])
def test_native_journal_gate_accepts_actual_committed_builtin_actions(public):
    transcript, _, _ = _native_journal_gate_transcript()
    result = evals.score_machine_contract(
        {
            "machineContract": {
                "requireAgentToolAudit": True,
                "requirePublicOrderReasonReference": public,
            }
        },
        transcript,
    )
    assert result["pass"], result["failures"]


def test_native_journal_gate_preserves_actual_failed_invocation_status():
    transcript, _, _ = _native_journal_gate_transcript(fail_order=True)
    result = evals.score_machine_contract({"machineContract": {"requireAgentToolAudit": True}}, transcript)
    assert result["pass"], result["failures"]
    assert transcript["memory_events"][-1]["metadata"]["status"] == "failed"
    assert not transcript["broker_orders"]


@pytest.mark.parametrize(
    "mutation",
    [
        "missing_start",
        "missing_outcome",
        "duplicate_event",
        "invalid_uuid",
        "wrong_call_request",
        "wrong_result_id",
        "wrong_status",
        "arguments_hash",
        "payload_hash",
        "sequence",
        "schema",
        "purpose",
        "purpose_controls",
        "mixed_store",
        "metadata_request",
        "extra_result",
        "missing_result",
    ],
)
def test_native_journal_gate_rejects_exact_corrupted_audit_contract(mutation):
    transcript, _, _ = _native_journal_gate_transcript()
    rows = transcript["memory_events"]
    first_pair = rows[:2]
    if mutation == "missing_start":
        rows.pop(0)
    elif mutation == "missing_outcome":
        rows.pop(1)
    elif mutation == "duplicate_event":
        duplicate = copy.deepcopy(rows[-1])
        duplicate["sequence"] += 1
        rows.append(duplicate)
    elif mutation == "invalid_uuid":
        for row in first_pair:
            row["metadata"]["action_id"] = row["subject_id"] = "invalid-action-id"
            _rehash_journal_test_event(row)
    elif mutation == "wrong_call_request":
        transcript["tool_calls"][0]["model_call_id"] = "another-request"
    elif mutation == "wrong_result_id":
        transcript["tool_results"][0]["call_id"] = "another-provider-call"
    elif mutation == "wrong_status":
        first_pair[1]["metadata"]["status"] = "failed"
        _rehash_journal_test_event(first_pair[1])
    elif mutation == "arguments_hash":
        for row in first_pair:
            row["metadata"]["arguments_sha256"] = "0" * 64
            _rehash_journal_test_event(row)
    elif mutation == "payload_hash":
        first_pair[0]["payload_hash"] = "0" * 64
    elif mutation == "sequence":
        first_pair[0]["sequence"] = True
    elif mutation == "schema":
        first_pair[0]["schema_version"] = 2
    elif mutation == "purpose":
        for row in first_pair:
            row["metadata"]["purpose"] = row["text"] = "Different private purpose."
            _rehash_journal_test_event(row)
    elif mutation == "purpose_controls":
        for row in first_pair:
            row["metadata"]["purpose"] = row["text"] = "Private purpose\nwith control."
            _rehash_journal_test_event(row)
        transcript["tool_calls"][0]["payload"]["agent_purpose"] = "Private purpose\nwith control."
    elif mutation == "mixed_store":
        first_pair[0]["strategy"] = "different-fixture-store"
    elif mutation == "metadata_request":
        first_pair[0]["metadata"]["model_call_id"] = "different-request"
        _rehash_journal_test_event(first_pair[0])
    elif mutation == "extra_result":
        transcript["tool_results"].append(copy.deepcopy(transcript["tool_results"][0]))
    elif mutation == "missing_result":
        transcript["tool_results"].pop(0)
    result = evals.score_machine_contract({"machineContract": {"requireAgentToolAudit": True}}, transcript)
    assert not result["pass"], mutation


@pytest.mark.parametrize(
    "mutation",
    [
        "missing_public",
        "public_reference_hash",
        "private_reference_hash",
        "public_content",
        "missing_order_outcome",
        "late_intent",
        "unknown_outcome",
        "missing_broker_reference",
        "duplicate_intent",
    ],
)
def test_native_journal_gate_rejects_exact_corrupted_public_order_contract(mutation):
    transcript, _, _ = _native_journal_gate_transcript()
    rows = transcript["memory_events"]
    intent = next(row for row in rows if row["event_type"] == "order.intent")
    outcome = next(row for row in rows if row["event_type"] == "order.action_outcome")
    if mutation == "missing_public":
        transcript["tool_calls"][-1]["payload"].pop("public_reason")
    elif mutation == "public_reference_hash":
        transcript["broker_orders"][0]["decision_journal"]["public_reason_sha256"] = "0" * 64
    elif mutation == "private_reference_hash":
        intent["metadata"]["reference"]["reason_sha256"] = "0" * 64
        _rehash_journal_test_event(intent)
    elif mutation == "public_content":
        intent["metadata"]["evidence"]["public_reason"] = "Different summary."
        _rehash_journal_test_event(intent)
    elif mutation == "missing_order_outcome":
        rows.remove(outcome)
    elif mutation == "late_intent":
        intent["sequence"] = rows[-1]["sequence"] + 1
        rows.sort(key=lambda row: row["sequence"])
    elif mutation == "unknown_outcome":
        outcome["metadata"]["certainty"] = "unknown"
    elif mutation == "missing_broker_reference":
        transcript["broker_orders"][0].pop("decision_journal")
    elif mutation == "duplicate_intent":
        duplicate = copy.deepcopy(intent)
        duplicate["event_id"] = f"event_{uuid.uuid4().hex}"
        duplicate["sequence"] = rows[-1]["sequence"] + 1
        rows.append(duplicate)
    result = evals.score_machine_contract(
        {
            "machineContract": {
                "requireAgentToolAudit": True,
                "requirePublicOrderReasonReference": True,
            }
        },
        transcript,
    )
    assert not result["pass"], mutation


def test_native_journal_gate_accepts_pruned_results_using_committed_status():
    transcript, _, _ = _native_journal_gate_transcript()
    transcript["tool_results"][-1]["payload"] = {"lumibot_tool_result_pruned": True, "excerpt": "bounded"}
    result = evals.score_machine_contract(
        {
            "machineContract": {
                "requireAgentToolAudit": True,
                "requirePublicOrderReasonReference": True,
            }
        },
        transcript,
    )
    assert result["pass"], result["failures"]


@pytest.mark.parametrize("flag", ["requireAgentToolAudit", "requirePublicOrderReasonReference"])
def test_native_journal_machine_flags_require_exact_booleans(flag):
    with pytest.raises(RuntimeError, match="must be a boolean"):
        evals._decision_completion_contract({flag: 1})


@pytest.mark.parametrize("value", [2, float("nan")])
def test_native_journal_gate_preserves_domain_purpose_and_unique_repeated_calls(value):
    from google.adk.tools import FunctionTool

    from lumibot.components.agents import AgentTraceEvent
    from lumibot.components.agents.runtime import _function_tools_with_name_aliases, _wrap_agent_tool_callable
    from lumibot.components.agents.schemas import BoundTool

    production = _fixture()
    try:
        handle = production.create_agent(evals.load_cases({"stock_order_decision_journal"})[0], object())
        request_id = "synthetic-domain-purpose-request"
        context = {"tool_calls": [], "tool_journal_callback": handle._tool_journal_callback(request_id)}

        def domain_tool(*, agent_purpose: str, value: float):
            return {"domain_purpose": agent_purpose, "value": value}

        tool = BoundTool(name="domain_tool", description="Ordinary domain argument contract.", function=domain_tool)
        arguments = {"agent_purpose": "domain-owned value", "value": value, "agent_purpose_2": "Private audit purpose."}
        events = []
        for _ in range(2):
            response = _wrap_agent_tool_callable(tool, context)(**arguments)
            assert response["domain_purpose"] == "domain-owned value"
            events.extend(
                [
                    AgentTraceEvent(kind="tool_call", tool_name=tool.name, call_id=None, payload=dict(arguments)),
                    AgentTraceEvent(kind="tool_result", tool_name=tool.name, call_id=None, payload=response),
                ]
            )
        actor = AgentRunResult(
            summary="Ordinary contract; no model.",
            model="ordinary-contract-no-model",
            events=events,
            cache_key=request_id,
        )
        production.capture(actor)
        transcript = evals.compact_transcript(actor, production.fixture)
        managed = _function_tools_with_name_aliases(FunctionTool, [tool])[0]
        declaration = managed._get_declaration()
        parameters = declaration.parameters_json_schema or declaration.parameters
        if hasattr(parameters, "model_dump"):
            parameters = parameters.model_dump(mode="json", exclude_none=True)
        transcript["builtin_tool_contracts"] = [
            {
                "name": tool.name,
                "parameters": evals.judge_parameter_schema(parameters),
                "private_purpose_parameter": evals._managed_private_purpose_parameter(tool, managed),
            }
        ]
        assert transcript["builtin_tool_contracts"][0]["private_purpose_parameter"] == "agent_purpose_2"
        outcome = evals.score_machine_contract({"machineContract": {"requireAgentToolAudit": True}}, transcript)
        assert outcome["pass"], outcome["failures"]
        assert len({row["subject_id"] for row in transcript["memory_events"]}) == 2
        # Reusing one pair for two otherwise identical calls must never pass.
        transcript["memory_events"] = transcript["memory_events"][:2]
        assert not evals.score_machine_contract({"machineContract": {"requireAgentToolAudit": True}}, transcript)[
            "pass"
        ]
    finally:
        production.close()


def test_actual_native_stock_audit_failure_uses_managed_declaration_identity():
    """Actual failure regression, ordinary offline scoring; not a new model pass."""
    transcript = copy.deepcopy(_NATIVE_STOCK_AUDIT_FAILURE)
    for row in transcript["memory_events"]:
        payload = json.loads(row["payload_json"])
        if row["event_type"].startswith("agent.tool_"):
            row["metadata"], row["text"] = payload, payload["purpose"]
        elif row["event_type"] == "order.intent":
            row["metadata"], row["text"] = payload["metadata"], payload["text"]
    # Freeze the original synthetic source fields, not a requalified pass.
    assert hashlib.sha256(json.dumps(transcript, sort_keys=True, separators=(",", ":")).encode()).hexdigest() == (
        "1a19df09ed3d83d8f7d94acfe33b8030d528cf67fd1f0aab7542d7964cfe70d0"
    )
    transcript["fixture_calls"] = [
        {"name": call["name"], "arguments": call["payload"]} for call in transcript["tool_calls"]
    ]
    transcript["submissions"] = []
    production = _fixture()
    try:
        handle = production.create_agent(evals.load_cases({"stock_order_decision_journal"})[0], object())
        called = {call["name"] for call in transcript["tool_calls"]}
        transcript["builtin_tool_contracts"] = evals.builtin_tool_contracts(
            [tool for tool in handle._ensure_bound_tools() if tool.name in called]
        )
        # Native declarations already expose the actual private argument.
        assert all("agent_purpose" in item["parameters"]["properties"] for item in transcript["builtin_tool_contracts"])
        result = evals.score_machine_contract(
            {
                "machineContract": {
                    "requireAgentToolAudit": True,
                    "requirePublicOrderReasonReference": True,
                }
            },
            transcript,
        )
        assert result["pass"], result["failures"]
    finally:
        production.close()


@pytest.mark.parametrize("identity", [None, "symbol", "agent_purpose_2"])
def test_audit_scorer_requires_the_actual_managed_private_identity(identity):
    transcript, _, _ = _native_journal_gate_transcript()
    declaration = next(item for item in transcript["builtin_tool_contracts"] if item["name"] == "market_last_price")
    if identity is None:
        declaration.pop("private_purpose_parameter")
    else:
        declaration["private_purpose_parameter"] = identity
    assert not evals.score_machine_contract({"machineContract": {"requireAgentToolAudit": True}}, transcript)["pass"]


def test_managed_private_identity_preserves_dense_domain_parameter_names():
    from google.adk.tools import FunctionTool

    from lumibot.components.agents.runtime import _function_tools_with_name_aliases
    from lumibot.components.agents.schemas import BoundTool

    def domain_tool(*, agent_purpose: str, agent_purpose_2: str, agent_purpose_4: str):
        return {"values": [agent_purpose, agent_purpose_2, agent_purpose_4]}

    tool = BoundTool(name="domain_tool", description="Exact domain signature contract.", function=domain_tool)
    managed = _function_tools_with_name_aliases(FunctionTool, [tool])[0]
    assert evals._managed_private_purpose_parameter(tool, managed) == "agent_purpose_3"
    with pytest.raises(RuntimeError, match="exactly one"):
        evals._managed_private_purpose_parameter(tool, FunctionTool(domain_tool))


# Lossless native synthetic nine-call excerpt; never actor/judge freshness evidence.
# Source ac313ad535f0d0f4233c9c0846141ede56514d3a, original machine FAIL.
# Run 4db57f7a-9ff6-47d3-a0f7-ff9b710fd57b, repetition 2.
# Case fingerprint 16bc4d95f2e60e5d8a73f61f118f8ec6d0b44db3775337cf48436f113118f6e6.
# Original completed ledger line SHA256 a8427ea4e1e7586099dbc7a7344fa6d41e61498951d2cab26528eb619e444f43.
_NATIVE_STOCK_AUDIT_FAILURE = {
    "broker_orders": [
        {
            "asset": {"symbol": "AAPL", "type": "stock"},
            "avg_fill_price": 230.0,
            "decision_journal": {
                "action_id": "action_11d5873ecdce45c8af023835174044ec",
                "agent_name": "trader",
                "decision_id": "decision_e29f1d15384f4f0fa9ae6e870194f3ca",
                "deployment_id": None,
                "model_call_id": "142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e",
                "operation": "submit",
                "previous_action_id": None,
                "public_reason_preview": "AAPL is above the mean of its five "
                "latest completed daily closes, meeting "
                "the entry rule. A 43-share limit order "
                "stays within the defined portfolio "
                "exposure cap; price declines and "
                "non-execution remain possible.",
                "public_reason_sha256": "90845318d387f8be6ed3c105d98604798ec8c9cc556f8617996ab129039b7338",
                "reason_preview": "The conditional entry rule is satisfied: the "
                "current AAPL price at 2026-08-11 14:35 UTC is "
                "$230, strictly above the arithmetic mean of "
                "$227 across the five latest completed daily "
                "closes: $225 on 2026-08-06, $226 on "
                "2026-08-07, $227 on 2026-08-08, $228 on "
                "2026-08-09, and $229 on 2026-08-10. Account "
                "tools showed $100,000 portfolio value, "
                "$100,000 cash, a USD cash position and no AAPL "
                "holding or open orders. The 10% portfolio "
                "notional cap is $10,000; the sizing tool "
                "returned 43 shares at the current price, "
                "$9,890 notional, below both the cap and cash, "
                "leaving room for price movement. This single "
                "day limit buy at $230 risks loss if AAPL "
                "falls; the rule's entry condition is "
                "invalidated if current price is no longer "
                "strictly above the five-close mean, and "
                "execution is not guaranteed at this limi",
                "reason_sha256": "dc4cf49812a22b1e253b3d0c9545d7fe737a652081a4bf5f90b4a419fb740e82",
                "recorded_at": "2026-08-11T14:35:00+00:00",
                "run_id": None,
                "strategy_revision_id": None,
            },
            "decision_provenance": {
                "decision_id": "decision_e29f1d15384f4f0fa9ae6e870194f3ca",
                "model_call_id": "142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e",
            },
            "identifier": "bt_1",
            "limit_price": 230.0,
            "order_type": "limit",
            "quantity": 43.0,
            "quote": {"symbol": "USD", "type": "forex"},
            "side": "buy",
            "status": "fill",
            "time_in_force": "day",
        }
    ],
    "memory_events": [
        {
            "agent_name": "trader",
            "event_id": "event_2fe1e5c21cf84d7cb9400dd793989d56",
            "event_type": "agent.tool_started",
            "model_call_id": "142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e",
            "payload_hash": "d55f37b08ea2d0b1a34961b71160f9094b73a159b2921cb30af076b4cd158163",
            "payload_json": '{"action_id": "574ac3dd-4730-4a75-ac1d-f17c29c68e72", "agent_name": '
            '"trader", "arguments_sha256": '
            '"abfbf177d94bada96858af8930469c9b6f8a41f237da7c287bb1a42509be89b2", '
            '"decision_id": null, "deployment_id": null, "error_type": null, '
            '"model_call_id": '
            '"142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e", '
            '"purpose": "Load required stock/ETF procedures before evaluating the '
            'conditional AAPL equity trade.", "run_id": null, "status": "started", '
            '"strategy_revision_id": null, "tool_name": "load_skill"}',
            "schema_version": 1,
            "sequence": 1,
            "strategy": "eval_3770cd03ca43423dac37d0c4204dec0e",
            "subject_id": "574ac3dd-4730-4a75-ac1d-f17c29c68e72",
            "subject_type": "agent_tool_action",
            "timestamp": "2026-08-11T14:35:00+00:00",
            "wall_time": "2026-10-13T17:26:05.948126Z",
        },
        {
            "agent_name": "trader",
            "event_id": "event_35edfb7c87cc4d688cccca861d7dcaf8",
            "event_type": "agent.tool_outcome",
            "model_call_id": "142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e",
            "payload_hash": "05b2af91c291c0c537779b210140103e6df2c472c02445c1d668c24accfb3d0a",
            "payload_json": '{"action_id": "574ac3dd-4730-4a75-ac1d-f17c29c68e72", "agent_name": '
            '"trader", "arguments_sha256": '
            '"abfbf177d94bada96858af8930469c9b6f8a41f237da7c287bb1a42509be89b2", '
            '"decision_id": null, "deployment_id": null, "error_type": null, '
            '"model_call_id": '
            '"142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e", '
            '"purpose": "Load required stock/ETF procedures before evaluating the '
            'conditional AAPL equity trade.", "run_id": null, "status": '
            '"completed", "strategy_revision_id": null, "tool_name": "load_skill"}',
            "schema_version": 1,
            "sequence": 2,
            "strategy": "eval_3770cd03ca43423dac37d0c4204dec0e",
            "subject_id": "574ac3dd-4730-4a75-ac1d-f17c29c68e72",
            "subject_type": "agent_tool_action",
            "timestamp": "2026-08-11T14:35:00+00:00",
            "wall_time": "2026-10-13T17:26:05.949673Z",
        },
        {
            "agent_name": "trader",
            "event_id": "event_38f6ed0fab464be9ab6b4b0ddacd28a1",
            "event_type": "agent.tool_started",
            "model_call_id": "142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e",
            "payload_hash": "19e852ba7c267e9f2e0d30eeb908d2990728070846fdf6753bf7dc9132d258e6",
            "payload_json": '{"action_id": "2de0bc50-0c13-4ef9-b9dd-741bbb38a532", "agent_name": '
            '"trader", "arguments_sha256": '
            '"4a3d438d999142aedeefe480c14fb96d01e0a1430963c4c55b6b2e1a2b90bef3", '
            '"decision_id": null, "deployment_id": null, "error_type": null, '
            '"model_call_id": '
            '"142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e", '
            '"purpose": "Obtain same-run current AAPL price for strict entry test '
            'and buy-limit reference.", "run_id": null, "status": "started", '
            '"strategy_revision_id": null, "tool_name": "market_last_price"}',
            "schema_version": 1,
            "sequence": 3,
            "strategy": "eval_3770cd03ca43423dac37d0c4204dec0e",
            "subject_id": "2de0bc50-0c13-4ef9-b9dd-741bbb38a532",
            "subject_type": "agent_tool_action",
            "timestamp": "2026-08-11T14:35:00+00:00",
            "wall_time": "2026-10-13T17:26:14.450655Z",
        },
        {
            "agent_name": "trader",
            "event_id": "event_0b06acaff7d2409c85c34f53c8a7ade8",
            "event_type": "agent.tool_outcome",
            "model_call_id": "142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e",
            "payload_hash": "41865e30a4a25ca45df57630b64111480eeb55d520db26241cca10da3b0b6b79",
            "payload_json": '{"action_id": "2de0bc50-0c13-4ef9-b9dd-741bbb38a532", "agent_name": '
            '"trader", "arguments_sha256": '
            '"4a3d438d999142aedeefe480c14fb96d01e0a1430963c4c55b6b2e1a2b90bef3", '
            '"decision_id": null, "deployment_id": null, "error_type": null, '
            '"model_call_id": '
            '"142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e", '
            '"purpose": "Obtain same-run current AAPL price for strict entry test '
            'and buy-limit reference.", "run_id": null, "status": "completed", '
            '"strategy_revision_id": null, "tool_name": "market_last_price"}',
            "schema_version": 1,
            "sequence": 4,
            "strategy": "eval_3770cd03ca43423dac37d0c4204dec0e",
            "subject_id": "2de0bc50-0c13-4ef9-b9dd-741bbb38a532",
            "subject_type": "agent_tool_action",
            "timestamp": "2026-08-11T14:35:00+00:00",
            "wall_time": "2026-10-13T17:26:14.453735Z",
        },
        {
            "agent_name": "trader",
            "event_id": "event_f7c76db64af348d4b85d7bba06403be2",
            "event_type": "agent.tool_started",
            "model_call_id": "142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e",
            "payload_hash": "93b5f45b2065aff36c3c5ad9788261ffa83b9c1718d77ffcbd14edb17c78ce1a",
            "payload_json": '{"action_id": "3dc80c78-e01d-428a-8530-c492972d50d0", "agent_name": '
            '"trader", "arguments_sha256": '
            '"08b3a683a4f465032bbdb194c6119d93f2a04af6bbd01df564afcd2644d4e72d", '
            '"decision_id": null, "deployment_id": null, "error_type": null, '
            '"model_call_id": '
            '"142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e", '
            '"purpose": "Retrieve recent dated daily bars to select the five '
            'latest completed closes for the user\'s threshold.", "run_id": null, '
            '"status": "started", "strategy_revision_id": null, "tool_name": '
            '"market_historical_prices"}',
            "schema_version": 1,
            "sequence": 5,
            "strategy": "eval_3770cd03ca43423dac37d0c4204dec0e",
            "subject_id": "3dc80c78-e01d-428a-8530-c492972d50d0",
            "subject_type": "agent_tool_action",
            "timestamp": "2026-08-11T14:35:00+00:00",
            "wall_time": "2026-10-13T17:26:14.455448Z",
        },
        {
            "agent_name": "trader",
            "event_id": "event_34bdcc74237d489cb2da1f05981824a9",
            "event_type": "agent.tool_outcome",
            "model_call_id": "142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e",
            "payload_hash": "8fc8edeb27ac21266eeb3bb15f73d4af0a30c8d63422e25c888d0f26e3b4f75e",
            "payload_json": '{"action_id": "3dc80c78-e01d-428a-8530-c492972d50d0", "agent_name": '
            '"trader", "arguments_sha256": '
            '"08b3a683a4f465032bbdb194c6119d93f2a04af6bbd01df564afcd2644d4e72d", '
            '"decision_id": null, "deployment_id": null, "error_type": null, '
            '"model_call_id": '
            '"142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e", '
            '"purpose": "Retrieve recent dated daily bars to select the five '
            'latest completed closes for the user\'s threshold.", "run_id": null, '
            '"status": "completed", "strategy_revision_id": null, "tool_name": '
            '"market_historical_prices"}',
            "schema_version": 1,
            "sequence": 6,
            "strategy": "eval_3770cd03ca43423dac37d0c4204dec0e",
            "subject_id": "3dc80c78-e01d-428a-8530-c492972d50d0",
            "subject_type": "agent_tool_action",
            "timestamp": "2026-08-11T14:35:00+00:00",
            "wall_time": "2026-10-13T17:26:14.462564Z",
        },
        {
            "agent_name": "trader",
            "event_id": "event_4d4d6097fa0c42c6becb1b6214de0e36",
            "event_type": "agent.tool_started",
            "model_call_id": "142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e",
            "payload_hash": "be6640db0fb924f75cec945a32b3d300f5bb1f5d1ced1a711101449fa5624ee6",
            "payload_json": '{"action_id": "f589a5f0-8609-48d1-950f-7b4ddf8288f2", "agent_name": '
            '"trader", "arguments_sha256": '
            '"f73d6a4a0737a94472154591b5b488acbe69403b2330ff68e25d4597ddec595d", '
            '"decision_id": null, "deployment_id": null, "error_type": null, '
            '"model_call_id": '
            '"142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e", '
            '"purpose": "Compute the exact mean of the five latest completed AAPL '
            'daily closes and inspect dated inputs.", "run_id": null, "status": '
            '"started", "strategy_revision_id": null, "tool_name": "duckdb_query"}',
            "schema_version": 1,
            "sequence": 7,
            "strategy": "eval_3770cd03ca43423dac37d0c4204dec0e",
            "subject_id": "f589a5f0-8609-48d1-950f-7b4ddf8288f2",
            "subject_type": "agent_tool_action",
            "timestamp": "2026-08-11T14:35:00+00:00",
            "wall_time": "2026-10-13T17:26:20.197809Z",
        },
        {
            "agent_name": "trader",
            "event_id": "event_a67e5e1066be475a867486288eb543f7",
            "event_type": "agent.tool_outcome",
            "model_call_id": "142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e",
            "payload_hash": "04a1750eb52b03985d1ee0d9cd9c18e56a7be33db8ecc0e14fcb2e5098a9cf96",
            "payload_json": '{"action_id": "f589a5f0-8609-48d1-950f-7b4ddf8288f2", "agent_name": '
            '"trader", "arguments_sha256": '
            '"f73d6a4a0737a94472154591b5b488acbe69403b2330ff68e25d4597ddec595d", '
            '"decision_id": null, "deployment_id": null, "error_type": null, '
            '"model_call_id": '
            '"142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e", '
            '"purpose": "Compute the exact mean of the five latest completed AAPL '
            'daily closes and inspect dated inputs.", "run_id": null, "status": '
            '"completed", "strategy_revision_id": null, "tool_name": '
            '"duckdb_query"}',
            "schema_version": 1,
            "sequence": 8,
            "strategy": "eval_3770cd03ca43423dac37d0c4204dec0e",
            "subject_id": "f589a5f0-8609-48d1-950f-7b4ddf8288f2",
            "subject_type": "agent_tool_action",
            "timestamp": "2026-08-11T14:35:00+00:00",
            "wall_time": "2026-10-13T17:26:20.202194Z",
        },
        {
            "agent_name": "trader",
            "event_id": "event_99b4649becad44ada089284daa4d2d9e",
            "event_type": "agent.tool_started",
            "model_call_id": "142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e",
            "payload_hash": "488e45b241ad03ec8e1598f508fc8dfc1cc1c24fcf18596b72d3dcd1e0d314d4",
            "payload_json": '{"action_id": "32a6b6fa-0727-4de5-8f94-e8b144bf6f2b", "agent_name": '
            '"trader", "arguments_sha256": '
            '"97fcdad9fe31227a9ec60e519ff7635ac71941c55b77e585c908db1fa6406030", '
            '"decision_id": null, "deployment_id": null, "error_type": null, '
            '"model_call_id": '
            '"142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e", '
            '"purpose": "Refresh cash and portfolio value for 10% AAPL position '
            'sizing.", "run_id": null, "status": "started", '
            '"strategy_revision_id": null, "tool_name": "account_portfolio"}',
            "schema_version": 1,
            "sequence": 9,
            "strategy": "eval_3770cd03ca43423dac37d0c4204dec0e",
            "subject_id": "32a6b6fa-0727-4de5-8f94-e8b144bf6f2b",
            "subject_type": "agent_tool_action",
            "timestamp": "2026-08-11T14:35:00+00:00",
            "wall_time": "2026-10-13T17:26:27.566253Z",
        },
        {
            "agent_name": "trader",
            "event_id": "event_d6871395fa9049fcb1b6896c1e9a3154",
            "event_type": "agent.tool_outcome",
            "model_call_id": "142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e",
            "payload_hash": "eae713d8931c82ad9b2874d60ce41e8a04532ca7d2d6cf72a4cd00f53afab7e6",
            "payload_json": '{"action_id": "32a6b6fa-0727-4de5-8f94-e8b144bf6f2b", "agent_name": '
            '"trader", "arguments_sha256": '
            '"97fcdad9fe31227a9ec60e519ff7635ac71941c55b77e585c908db1fa6406030", '
            '"decision_id": null, "deployment_id": null, "error_type": null, '
            '"model_call_id": '
            '"142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e", '
            '"purpose": "Refresh cash and portfolio value for 10% AAPL position '
            'sizing.", "run_id": null, "status": "completed", '
            '"strategy_revision_id": null, "tool_name": "account_portfolio"}',
            "schema_version": 1,
            "sequence": 10,
            "strategy": "eval_3770cd03ca43423dac37d0c4204dec0e",
            "subject_id": "32a6b6fa-0727-4de5-8f94-e8b144bf6f2b",
            "subject_type": "agent_tool_action",
            "timestamp": "2026-08-11T14:35:00+00:00",
            "wall_time": "2026-10-13T17:26:27.569562Z",
        },
        {
            "agent_name": "trader",
            "event_id": "event_0b4fbeaa592b455b8b20bed3912e3daa",
            "event_type": "agent.tool_started",
            "model_call_id": "142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e",
            "payload_hash": "172329d2a23b16d715f3d765f338c036f9b69a9cbc28c00a40db51abc9921a83",
            "payload_json": '{"action_id": "a92381e1-3048-4d30-a912-ef9cd923fc72", "agent_name": '
            '"trader", "arguments_sha256": '
            '"97fcdad9fe31227a9ec60e519ff7635ac71941c55b77e585c908db1fa6406030", '
            '"decision_id": null, "deployment_id": null, "error_type": null, '
            '"model_call_id": '
            '"142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e", '
            '"purpose": "Verify exact current holdings before opening AAPL '
            'exposure.", "run_id": null, "status": "started", '
            '"strategy_revision_id": null, "tool_name": "account_positions"}',
            "schema_version": 1,
            "sequence": 11,
            "strategy": "eval_3770cd03ca43423dac37d0c4204dec0e",
            "subject_id": "a92381e1-3048-4d30-a912-ef9cd923fc72",
            "subject_type": "agent_tool_action",
            "timestamp": "2026-08-11T14:35:00+00:00",
            "wall_time": "2026-10-13T17:26:27.571933Z",
        },
        {
            "agent_name": "trader",
            "event_id": "event_427e685991e241558c4fb8d684d07ace",
            "event_type": "agent.tool_outcome",
            "model_call_id": "142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e",
            "payload_hash": "65cc750861adbc2964e58aadfc754766473846cb3b86e453e100d826a964c593",
            "payload_json": '{"action_id": "a92381e1-3048-4d30-a912-ef9cd923fc72", "agent_name": '
            '"trader", "arguments_sha256": '
            '"97fcdad9fe31227a9ec60e519ff7635ac71941c55b77e585c908db1fa6406030", '
            '"decision_id": null, "deployment_id": null, "error_type": null, '
            '"model_call_id": '
            '"142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e", '
            '"purpose": "Verify exact current holdings before opening AAPL '
            'exposure.", "run_id": null, "status": "completed", '
            '"strategy_revision_id": null, "tool_name": "account_positions"}',
            "schema_version": 1,
            "sequence": 12,
            "strategy": "eval_3770cd03ca43423dac37d0c4204dec0e",
            "subject_id": "a92381e1-3048-4d30-a912-ef9cd923fc72",
            "subject_type": "agent_tool_action",
            "timestamp": "2026-08-11T14:35:00+00:00",
            "wall_time": "2026-10-13T17:26:27.575778Z",
        },
        {
            "agent_name": "trader",
            "event_id": "event_4ef70ccdf5194752959b85e735e1446e",
            "event_type": "agent.tool_started",
            "model_call_id": "142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e",
            "payload_hash": "af6372c71ff65c9a5e944d943647af9dfa003d6113c5f099fc3d23dd22db37d2",
            "payload_json": '{"action_id": "59e49f24-cf84-46bd-b0b1-f297713849fe", "agent_name": '
            '"trader", "arguments_sha256": '
            '"97fcdad9fe31227a9ec60e519ff7635ac71941c55b77e585c908db1fa6406030", '
            '"decision_id": null, "deployment_id": null, "error_type": null, '
            '"model_call_id": '
            '"142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e", '
            '"purpose": "Verify no pending order already covers the intended AAPL '
            'purchase.", "run_id": null, "status": "started", '
            '"strategy_revision_id": null, "tool_name": "orders_open_orders"}',
            "schema_version": 1,
            "sequence": 13,
            "strategy": "eval_3770cd03ca43423dac37d0c4204dec0e",
            "subject_id": "59e49f24-cf84-46bd-b0b1-f297713849fe",
            "subject_type": "agent_tool_action",
            "timestamp": "2026-08-11T14:35:00+00:00",
            "wall_time": "2026-10-13T17:26:27.579524Z",
        },
        {
            "agent_name": "trader",
            "event_id": "event_99268f134d7c4664969e6568f45907d9",
            "event_type": "agent.tool_outcome",
            "model_call_id": "142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e",
            "payload_hash": "bb1da552ef3d07642d9d5d79ea676c5b2063a4ca28bb588ba8b0be22c0384918",
            "payload_json": '{"action_id": "59e49f24-cf84-46bd-b0b1-f297713849fe", "agent_name": '
            '"trader", "arguments_sha256": '
            '"97fcdad9fe31227a9ec60e519ff7635ac71941c55b77e585c908db1fa6406030", '
            '"decision_id": null, "deployment_id": null, "error_type": null, '
            '"model_call_id": '
            '"142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e", '
            '"purpose": "Verify no pending order already covers the intended AAPL '
            'purchase.", "run_id": null, "status": "completed", '
            '"strategy_revision_id": null, "tool_name": "orders_open_orders"}',
            "schema_version": 1,
            "sequence": 14,
            "strategy": "eval_3770cd03ca43423dac37d0c4204dec0e",
            "subject_id": "59e49f24-cf84-46bd-b0b1-f297713849fe",
            "subject_type": "agent_tool_action",
            "timestamp": "2026-08-11T14:35:00+00:00",
            "wall_time": "2026-10-13T17:26:27.582806Z",
        },
        {
            "agent_name": "trader",
            "event_id": "event_2cf00e46407b447ea4b177a105caf2d3",
            "event_type": "agent.tool_started",
            "model_call_id": "142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e",
            "payload_hash": "6b6a0359cc70f2e128c8a3e48fb91ce8bc753097123ca44c1eee5c0156557679",
            "payload_json": '{"action_id": "698fcc43-aa6c-49b0-9f7a-0f20080c525a", "agent_name": '
            '"trader", "arguments_sha256": '
            '"853a7e4bb7ff23482a2c012ea069cf267e4e51c18a9c1c1fb91be5a3d272a946", '
            '"decision_id": null, "deployment_id": null, "error_type": null, '
            '"model_call_id": '
            '"142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e", '
            '"purpose": "Calculate whole-share quantity within the user\'s 10% '
            'portfolio risk cap.", "run_id": null, "status": "started", '
            '"strategy_revision_id": null, "tool_name": '
            '"risk_calculate_stock_quantity"}',
            "schema_version": 1,
            "sequence": 15,
            "strategy": "eval_3770cd03ca43423dac37d0c4204dec0e",
            "subject_id": "698fcc43-aa6c-49b0-9f7a-0f20080c525a",
            "subject_type": "agent_tool_action",
            "timestamp": "2026-08-11T14:35:00+00:00",
            "wall_time": "2026-10-13T17:26:27.585418Z",
        },
        {
            "agent_name": "trader",
            "event_id": "event_8005d72214b148b696b48fa92397aebe",
            "event_type": "agent.tool_outcome",
            "model_call_id": "142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e",
            "payload_hash": "738b49f11301326e7e22bb8743864ee5f19486a11a30960921fc3efb8379181e",
            "payload_json": '{"action_id": "698fcc43-aa6c-49b0-9f7a-0f20080c525a", "agent_name": '
            '"trader", "arguments_sha256": '
            '"853a7e4bb7ff23482a2c012ea069cf267e4e51c18a9c1c1fb91be5a3d272a946", '
            '"decision_id": null, "deployment_id": null, "error_type": null, '
            '"model_call_id": '
            '"142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e", '
            '"purpose": "Calculate whole-share quantity within the user\'s 10% '
            'portfolio risk cap.", "run_id": null, "status": "completed", '
            '"strategy_revision_id": null, "tool_name": '
            '"risk_calculate_stock_quantity"}',
            "schema_version": 1,
            "sequence": 16,
            "strategy": "eval_3770cd03ca43423dac37d0c4204dec0e",
            "subject_id": "698fcc43-aa6c-49b0-9f7a-0f20080c525a",
            "subject_type": "agent_tool_action",
            "timestamp": "2026-08-11T14:35:00+00:00",
            "wall_time": "2026-10-13T17:26:27.588265Z",
        },
        {
            "agent_name": "trader",
            "event_id": "event_c6cf0b123b5048fab39f21f6fd373767",
            "event_type": "agent.tool_started",
            "model_call_id": "142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e",
            "payload_hash": "9269133ba4a63cdda8e537b397e97295b0d06cbec648b791539e759c587bca47",
            "payload_json": '{"action_id": "95a52df9-aad1-481a-be67-c032c5e6856a", "agent_name": '
            '"trader", "arguments_sha256": '
            '"cc2cb3113e48335004059e723f33765f0f476891ad8e49b75383819dc860f777", '
            '"decision_id": null, "deployment_id": null, "error_type": null, '
            '"model_call_id": '
            '"142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e", '
            '"purpose": "Submit one AAPL limit buy because current price exceeds '
            "the exact five-completed-close average; retain dated evidence and "
            'capped sizing in private audit.", "run_id": null, "status": '
            '"started", "strategy_revision_id": null, "tool_name": '
            '"orders_submit_order"}',
            "schema_version": 1,
            "sequence": 17,
            "strategy": "eval_3770cd03ca43423dac37d0c4204dec0e",
            "subject_id": "95a52df9-aad1-481a-be67-c032c5e6856a",
            "subject_type": "agent_tool_action",
            "timestamp": "2026-08-11T14:35:00+00:00",
            "wall_time": "2026-10-13T17:26:37.168218Z",
        },
        {
            "agent_name": "trader",
            "event_id": "event_0e1aed09e5de4f4fa86da4b86443e7fc",
            "event_type": "order.intent",
            "model_call_id": "142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e",
            "payload_hash": "48c1833f5dd032f8a0b78c7db0b5914623994da7903616786142bf6bff17ac72",
            "payload_json": '{"kind": "order_action", "metadata": {"evidence": {"public_reason": '
            '"AAPL is above the mean of its five latest completed daily closes, '
            "meeting the entry rule. A 43-share limit order stays within the "
            "defined portfolio exposure cap; price declines and non-execution "
            'remain possible."}, "orders": [{"changes": {}, "identifier": "bt_1", '
            '"limit_price": 230.0, "order_type": "limit", "previous_reference": '
            'null, "quantity": "43.0", "side": "buy", "stop_price": null, '
            '"symbol": "AAPL"}], "reference": {"action_id": '
            '"action_11d5873ecdce45c8af023835174044ec", "agent_name": "trader", '
            '"decision_id": "decision_e29f1d15384f4f0fa9ae6e870194f3ca", '
            '"deployment_id": null, "model_call_id": '
            '"142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e", '
            '"operation": "submit", "previous_action_id": null, '
            '"public_reason_preview": "AAPL is above the mean of its five latest '
            "completed daily closes, meeting the entry rule. A 43-share limit "
            "order stays within the defined portfolio exposure cap; price declines "
            'and non-execution remain possible.", "public_reason_sha256": '
            '"90845318d387f8be6ed3c105d98604798ec8c9cc556f8617996ab129039b7338", '
            '"reason_preview": "The conditional entry rule is satisfied: the '
            "current AAPL price at 2026-08-11 14:35 UTC is $230, strictly above "
            "the arithmetic mean of $227 across the five latest completed daily "
            "closes: $225 on 2026-08-06, $226 on 2026-08-07, $227 on 2026-08-08, "
            "$228 on 2026-08-09, and $229 on 2026-08-10. Account tools showed "
            "$100,000 portfolio value, $100,000 cash, a USD cash position and no "
            "AAPL holding or open orders. The 10% portfolio notional cap is "
            "$10,000; the sizing tool returned 43 shares at the current price, "
            "$9,890 notional, below both the cap and cash, leaving room for price "
            "movement. This single day limit buy at $230 risks loss if AAPL falls; "
            "the rule's entry condition is invalidated if current price is no "
            "longer strictly above the five-close mean, and execution is not "
            'guaranteed at this limi", "reason_sha256": '
            '"dc4cf49812a22b1e253b3d0c9545d7fe737a652081a4bf5f90b4a419fb740e82", '
            '"recorded_at": "2026-08-11T14:35:00+00:00", "run_id": null, '
            '"strategy_revision_id": null}}, "status": "intent", "symbol": null, '
            '"tags": [], "text": "The conditional entry rule is satisfied: the '
            "current AAPL price at 2026-08-11 14:35 UTC is $230, strictly above "
            "the arithmetic mean of $227 across the five latest completed daily "
            "closes: $225 on 2026-08-06, $226 on 2026-08-07, $227 on 2026-08-08, "
            "$228 on 2026-08-09, and $229 on 2026-08-10. Account tools showed "
            "$100,000 portfolio value, $100,000 cash, a USD cash position and no "
            "AAPL holding or open orders. The 10% portfolio notional cap is "
            "$10,000; the sizing tool returned 43 shares at the current price, "
            "$9,890 notional, below both the cap and cash, leaving room for price "
            "movement. This single day limit buy at $230 risks loss if AAPL falls; "
            "the rule's entry condition is invalidated if current price is no "
            "longer strictly above the five-close mean, and execution is not "
            'guaranteed at this limit."}',
            "schema_version": 1,
            "sequence": 18,
            "strategy": "eval_3770cd03ca43423dac37d0c4204dec0e",
            "subject_id": "action_11d5873ecdce45c8af023835174044ec",
            "subject_type": "order_action",
            "timestamp": "2026-08-11T14:35:00+00:00",
            "wall_time": "2026-10-13T17:26:37.169980Z",
        },
        {
            "agent_name": "trader",
            "event_id": "event_9e0ae0ba25f9401496a6caedcfdba61b",
            "event_type": "order.action_outcome",
            "metadata": {
                "certainty": "returned",
                "error_type": None,
                "reference": {
                    "action_id": "action_11d5873ecdce45c8af023835174044ec",
                    "agent_name": "trader",
                    "decision_id": "decision_e29f1d15384f4f0fa9ae6e870194f3ca",
                    "deployment_id": None,
                    "model_call_id": "142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e",
                    "operation": "submit",
                    "previous_action_id": None,
                    "public_reason_preview": "AAPL is above the mean of its "
                    "five latest completed daily "
                    "closes, meeting the entry rule. A "
                    "43-share limit order stays within "
                    "the defined portfolio exposure "
                    "cap; price declines and "
                    "non-execution remain possible.",
                    "public_reason_sha256": "90845318d387f8be6ed3c105d98604798ec8c9cc556f8617996ab129039b7338",
                    "reason_preview": "The conditional entry rule is satisfied: "
                    "the current AAPL price at 2026-08-11 "
                    "14:35 UTC is $230, strictly above the "
                    "arithmetic mean of $227 across the five "
                    "latest completed daily closes: $225 on "
                    "2026-08-06, $226 on 2026-08-07, $227 on "
                    "2026-08-08, $228 on 2026-08-09, and $229 "
                    "on 2026-08-10. Account tools showed "
                    "$100,000 portfolio value, $100,000 cash, "
                    "a USD cash position and no AAPL holding "
                    "or open orders. The 10% portfolio "
                    "notional cap is $10,000; the sizing tool "
                    "returned 43 shares at the current price, "
                    "$9,890 notional, below both the cap and "
                    "cash, leaving room for price movement. "
                    "This single day limit buy at $230 risks "
                    "loss if AAPL falls; the rule's entry "
                    "condition is invalidated if current "
                    "price is no longer strictly above the "
                    "five-close mean, and execution is not "
                    "guaranteed at this limi",
                    "reason_sha256": "dc4cf49812a22b1e253b3d0c9545d7fe737a652081a4bf5f90b4a419fb740e82",
                    "recorded_at": "2026-08-11T14:35:00+00:00",
                    "run_id": None,
                    "strategy_revision_id": None,
                },
            },
            "model_call_id": "142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e",
            "payload_hash": "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a",
            "payload_json": "{}",
            "schema_version": 1,
            "sequence": 19,
            "strategy": "eval_3770cd03ca43423dac37d0c4204dec0e",
            "subject_id": "action_11d5873ecdce45c8af023835174044ec",
            "subject_type": "order_action",
            "text": "Order action returned",
            "timestamp": "2026-08-11T14:35:00+00:00",
            "wall_time": "2026-10-13T17:26:37.180390Z",
        },
        {
            "agent_name": "trader",
            "event_id": "event_ed25394fd4ea4db5ad6dfbc866b7dffb",
            "event_type": "agent.tool_outcome",
            "model_call_id": "142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e",
            "payload_hash": "c31b4e1b25c677d2b6e9e441dfe8ae083d4b092367f102384bacb66c461d1f1d",
            "payload_json": '{"action_id": "95a52df9-aad1-481a-be67-c032c5e6856a", "agent_name": '
            '"trader", "arguments_sha256": '
            '"cc2cb3113e48335004059e723f33765f0f476891ad8e49b75383819dc860f777", '
            '"decision_id": null, "deployment_id": null, "error_type": null, '
            '"model_call_id": '
            '"142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e", '
            '"purpose": "Submit one AAPL limit buy because current price exceeds '
            "the exact five-completed-close average; retain dated evidence and "
            'capped sizing in private audit.", "run_id": null, "status": '
            '"completed", "strategy_revision_id": null, "tool_name": '
            '"orders_submit_order"}',
            "schema_version": 1,
            "sequence": 21,
            "strategy": "eval_3770cd03ca43423dac37d0c4204dec0e",
            "subject_id": "95a52df9-aad1-481a-be67-c032c5e6856a",
            "subject_type": "agent_tool_action",
            "timestamp": "2026-08-11T14:35:00+00:00",
            "wall_time": "2026-10-13T17:26:37.194592Z",
        },
    ],
    "tool_calls": [
        {
            "call_id": "call_wAZq22MK3xLWzV5uNr41qu4r",
            "model_call_id": "142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e",
            "name": "load_skill",
            "payload": {
                "agent_purpose": "Load required stock/ETF procedures before evaluating the "
                "conditional AAPL equity trade.",
                "skill_name": "stock-trading",
            },
        },
        {
            "call_id": "call_PBlu7nwIr4q20041YAEXQEWM",
            "model_call_id": "142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e",
            "name": "market_last_price",
            "payload": {
                "agent_purpose": "Obtain same-run current AAPL price for strict entry test and buy-limit reference.",
                "asset_type": "stock",
                "symbol": "AAPL",
            },
        },
        {
            "call_id": "call_bANgnOcRM8g2vl8hTkwlwSuh",
            "model_call_id": "142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e",
            "name": "market_historical_prices",
            "payload": {
                "agent_purpose": "Retrieve recent dated daily bars to select the five latest "
                "completed closes for the user's threshold.",
                "asset_type": "stock",
                "length": 8,
                "symbols_json": '["AAPL"]',
                "table_name": "aapl_rule_bars",
                "timestep": "day",
            },
        },
        {
            "call_id": "call_0X9OOtHeTl2LKATPPXmbSd9y",
            "model_call_id": "142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e",
            "name": "duckdb_query",
            "payload": {
                "agent_purpose": "Compute the exact mean of the five latest completed AAPL "
                "daily closes and inspect dated inputs.",
                "sql": "SELECT datetime, close, AVG(close) OVER () AS mean_five FROM (SELECT "
                "datetime, close FROM aapl_rule_bars WHERE CAST(datetime AS DATE) < "
                "DATE '2026-08-11' ORDER BY datetime DESC LIMIT 5) s ORDER BY "
                "datetime",
            },
        },
        {
            "call_id": "call_KgVnEe99tPnW88vI6p7q4u1o",
            "model_call_id": "142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e",
            "name": "account_portfolio",
            "payload": {"agent_purpose": "Refresh cash and portfolio value for 10% AAPL position sizing."},
        },
        {
            "call_id": "call_v9oxQSFoPF4avCM7aBKLA7Mu",
            "model_call_id": "142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e",
            "name": "account_positions",
            "payload": {"agent_purpose": "Verify exact current holdings before opening AAPL exposure."},
        },
        {
            "call_id": "call_IGsVbprxJePxIufG1VMvVrKM",
            "model_call_id": "142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e",
            "name": "orders_open_orders",
            "payload": {"agent_purpose": "Verify no pending order already covers the intended AAPL purchase."},
        },
        {
            "call_id": "call_Jg8Y6o6rUuQs8Sp2OqtyhrZE",
            "model_call_id": "142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e",
            "name": "risk_calculate_stock_quantity",
            "payload": {
                "agent_purpose": "Calculate whole-share quantity within the user's 10% portfolio risk cap.",
                "available_cash": 100000,
                "maximum_notional": 10000,
                "price": 230,
            },
        },
        {
            "call_id": "call_Mzavogvt2LfxhAJl3eq4vB9y",
            "model_call_id": "142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e",
            "name": "orders_submit_order",
            "payload": {
                "agent_purpose": "Submit one AAPL limit buy because current price exceeds the "
                "exact five-completed-close average; retain dated evidence "
                "and capped sizing in private audit.",
                "asset_type": "stock",
                "limit_price": 230.0,
                "order_type": "limit",
                "public_reason": "AAPL is above the mean of its five latest completed daily "
                "closes, meeting the entry rule. A 43-share limit order "
                "stays within the defined portfolio exposure cap; price "
                "declines and non-execution remain possible.",
                "quantity": 43,
                "reason": "The conditional entry rule is satisfied: the current AAPL price at "
                "2026-08-11 14:35 UTC is $230, strictly above the arithmetic mean "
                "of $227 across the five latest completed daily closes: $225 on "
                "2026-08-06, $226 on 2026-08-07, $227 on 2026-08-08, $228 on "
                "2026-08-09, and $229 on 2026-08-10. Account tools showed $100,000 "
                "portfolio value, $100,000 cash, a USD cash position and no AAPL "
                "holding or open orders. The 10% portfolio notional cap is $10,000; "
                "the sizing tool returned 43 shares at the current price, $9,890 "
                "notional, below both the cap and cash, leaving room for price "
                "movement. This single day limit buy at $230 risks loss if AAPL "
                "falls; the rule's entry condition is invalidated if current price "
                "is no longer strictly above the five-close mean, and execution is "
                "not guaranteed at this limit.",
                "side": "buy",
                "symbol": "AAPL",
                "time_in_force": "day",
            },
        },
    ],
    "tool_results": [
        {
            "call_id": "call_wAZq22MK3xLWzV5uNr41qu4r",
            "model_call_id": "142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e",
            "name": "load_skill",
            "payload": {
                "frontmatter": {
                    "allowed_tools": None,
                    "compatibility": None,
                    "description": "Use before researching, selecting, opening, "
                    "modifying, or closing a stock or ETF "
                    "position, including discretionary "
                    "investing, rotation, breakout, momentum, "
                    "mean-reversion, opening-range breakout, and "
                    "VWAP trading. Also use when a broad mandate "
                    "leads you to consider stocks or ETFs even "
                    "if the user did not name an asset class "
                    "initially.",
                    "license": None,
                    "metadata": {},
                    "name": "stock-trading",
                },
                "instructions": "# Stock Trading\n"
                "\n"
                "Load this skill before using stocks or ETFs as part of a "
                "trading decision. If a\n"
                "broad mandate leads you to a stock idea, load it before "
                "submitting an order.\n"
                "\n"
                "## Core workflow\n"
                "\n"
                "1. Read portfolio value, cash, current positions, and open "
                "orders.\n"
                "2. Retrieve the current price and recent price history for "
                "every serious candidate.\n"
                "   Read that history with `market_historical_prices`, also "
                "for a single symbol;\n"
                "   pass `table_name` to query it with `duckdb_query`.\n"
                "   `market_load_history_table` does not replace it before "
                "a stock order.\n"
                "   Compute averages and indicators with a tool "
                "(`get_indicator`, `get_indicators`,\n"
                "   or `duckdb_query` over loaded bars), never by mental "
                "arithmetic, and quote\n"
                "   the tool's value. A query returning individual closes "
                "has not computed their\n"
                "   average. Request the aggregate or indicator and inspect "
                "its returned value\n"
                "   before using it in a decision or describing it as "
                "tool-calculated.\n"
                "   For lookback rules, select the requested count of the "
                "latest completed bars\n"
                "   and match the reported indicator and date window to "
                "that tool result. Do not\n"
                "   discard an already-completed bar again or substitute an "
                "earlier indicator row.\n"
                "3. Use batch tools for a universe. Do not loop one symbol "
                "at a time when a batch\n"
                "   price or history tool can return the same evidence.\n"
                "4. Evaluate the user's entry, exit, sizing, and frequency "
                "rules against current\n"
                "   evidence. Write down the decisive condition and whether "
                "it is true before\n"
                "   submitting an order. Do not invent missing signals. "
                "Keep the explanation's\n"
                "   comparisons identical to the evaluated rule: a latest "
                "price exceeding an\n"
                "   average does not establish that every constituent bar "
                "exceeded it. If a\n"
                "   claim concerns all bars, their count, or a date range, "
                "compute and inspect\n"
                "   that exact predicate from the returned data before "
                "asserting it.\n"
                "5. Size from current portfolio value, available cash, "
                "current price, volatility or\n"
                "   stop distance, and the user's risk rules. For a "
                "notional cap, calculate\n"
                "   the maximum notional and call "
                "`risk_calculate_stock_quantity`; use its returned\n"
                "   whole-share quantity unchanged. Verify its notional is "
                "at or below both the cap\n"
                "   and available cash before submission. Submit only when "
                "the returned quantity is\n"
                "   greater than zero; otherwise make a no-trade decision.\n"
                "6. Submit the selected order once. Price a limit order "
                "from the current\n"
                "   `market_last_price` result, never from a historical "
                "bar's close: a buy limit\n"
                "   below the current price usually does not fill. Follow "
                "the order tool's\n"
                "   guidance when the order must fill this session. This is "
                "for a new order; it\n"
                "   is never a reason to modify an order that is already "
                "pending (see step 8).\n"
                "7. Capture the returned identifier, inspect that exact "
                "order, and reread positions\n"
                "   and open orders. In backtests, a short bounded "
                "`orders_wait_for_terminal` is\n"
                "   appropriate immediately after your own market-order "
                "submission because it\n"
                "   lets the simulator process the pending fill. Do not use "
                "an unbounded wait.\n"
                "8. If a related order is already open, call "
                "`orders_get_status` with its returned\n"
                "   identifier to inspect that exact order; listing open "
                "orders alone does not\n"
                "   replace this status read. Do not submit another order "
                "for the same intended\n"
                "   position change. A pending exit already\n"
                "   owns the exit: leave it in place and report that it "
                "owns the position change.\n"
                "   Modify or cancel and replace it only when the user's "
                "rules explicitly require\n"
                "   changing an existing order. A due exit condition or a "
                "nonmarketable limit\n"
                "   alone does not authorize changing an already-pending "
                "exit.\n"
                "9. Reconcile the final summary with the mutation tools and "
                "the final account\n"
                "   reads. If an order tool returned a submitted "
                "identifier, never say that no\n"
                "   order was entered. Report the exact observed status "
                "instead.\n"
                "\n"
                "## Research depth\n"
                "\n"
                "Match research to the strategy. A broad discretionary "
                "investment decision should\n"
                "use relevant technical, news, macro, and company evidence "
                "when available. A\n"
                "mechanical intraday strategy should prioritize the exact "
                "price, bar, volume, and\n"
                "indicator evidence required by its rules. Do not force "
                "irrelevant research merely\n"
                "to increase tool use.\n"
                "\n"
                "## References\n"
                "\n"
                "Load only the smallest relevant reference:\n"
                "\n"
                "- `references/research-sizing-and-orders.md`: evidence, "
                "sizing, entries, exits,\n"
                "  rotation, and order verification.\n"
                "- `references/intraday-setups.md`: opening-range breakout "
                "and VWAP mechanics.\n"
                "\n"
                "The user's active strategy rules decide whether a trade "
                "should happen. This skill\n"
                "provides reusable stock-trading mechanics and does not "
                "invent a strategy.\n"
                "\n"
                "## Explain consequential order actions\n"
                "\n"
                "Supply an explicit `reason` before every submission, "
                "modification or cancellation.\n"
                "Use at least 200 characters; aim for 600–2,500 when the "
                "decision needs it. Explain\n"
                "the thesis, dated evidence, sizing, material risks and "
                "what would invalidate it.\n"
                "State missing evidence honestly. This is a decision "
                "summary, not hidden internal\n"
                "reasoning, and must contain no credentials or account "
                "identifiers. The order\n"
                "service commits it before execution. Report only observed "
                "order status and fills;\n"
                "reconcile an uncertain outcome before trying again.",
                "skill_name": "stock-trading",
            },
        },
        {
            "call_id": "call_PBlu7nwIr4q20041YAEXQEWM",
            "model_call_id": "142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e",
            "name": "market_last_price",
            "payload": {
                "asset_type": "stock",
                "datetime": "2026-08-11T14:35:00+00:00",
                "price": 230.0,
                "symbol": "AAPL",
            },
        },
        {
            "call_id": "call_bANgnOcRM8g2vl8hTkwlwSuh",
            "model_call_id": "142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e",
            "name": "market_historical_prices",
            "payload": {
                "asset_type": "stock",
                "columns": ["symbol", "datetime", "open", "high", "low", "close", "volume"],
                "count_available": 1,
                "count_requested": 1,
                "datetime": "2026-08-11T14:35:00+00:00",
                "datetime_timezone": "America/New_York",
                "first_datetime": "2026-08-04T00:00:00",
                "include_after_hours": True,
                "last_datetime": "2026-08-10T00:00:00",
                "length": 8,
                "row_count": 7,
                "rows_by_symbol": {"AAPL": 7},
                "symbols_available": ["AAPL"],
                "symbols_interval_mismatch": [],
                "symbols_missing": [],
                "symbols_requested": ["AAPL"],
                "table_name": "aapl_rule_bars",
                "timestep": "day",
            },
        },
        {
            "call_id": "call_0X9OOtHeTl2LKATPPXmbSd9y",
            "model_call_id": "142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e",
            "name": "duckdb_query",
            "payload": {
                "columns": ["datetime", "close", "mean_five"],
                "query_ms": 1.471,
                "row_count": 5,
                "rows": [
                    {"close": 225.0, "datetime": "2026-08-06T00:00:00", "mean_five": 227.0},
                    {"close": 226.0, "datetime": "2026-08-07T00:00:00", "mean_five": 227.0},
                    {"close": 227.0, "datetime": "2026-08-08T00:00:00", "mean_five": 227.0},
                    {"close": 228.0, "datetime": "2026-08-09T00:00:00", "mean_five": 227.0},
                    {"close": 229.0, "datetime": "2026-08-10T00:00:00", "mean_five": 227.0},
                ],
                "truncated": False,
            },
        },
        {
            "call_id": "call_KgVnEe99tPnW88vI6p7q4u1o",
            "model_call_id": "142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e",
            "name": "account_portfolio",
            "payload": {"cash": 100000.0, "datetime": "2026-08-11T14:35:00+00:00", "portfolio_value": 100000.0},
        },
        {
            "call_id": "call_v9oxQSFoPF4avCM7aBKLA7Mu",
            "model_call_id": "142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e",
            "name": "account_positions",
            "payload": {
                "as_of": "2026-08-11T14:35:00+00:00",
                "complete": True,
                "filters": {},
                "limit": 50,
                "matched": 1,
                "next_offset": None,
                "offset": 0,
                "omitted": 0,
                "positions": [
                    {
                        "asset": {"symbol": "USD", "type": "forex"},
                        "closing_quantity": 100000.0,
                        "position_side": "long",
                        "quantity": 100000.0,
                    }
                ],
                "returned": 1,
                "snapshot_id": "79f25008dbe1aa8d020a1025bcceb64a1c0e904e920ae44bb21de69f12b6ebb2",
                "total": 1,
            },
        },
        {
            "call_id": "call_IGsVbprxJePxIufG1VMvVrKM",
            "model_call_id": "142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e",
            "name": "orders_open_orders",
            "payload": {
                "as_of": "2026-08-11T14:35:00+00:00",
                "complete": True,
                "filters": {},
                "limit": 50,
                "matched": 0,
                "next_offset": None,
                "offset": 0,
                "omitted": 0,
                "orders": [],
                "returned": 0,
                "snapshot_id": "4f53cda18c2baa0c0354bb5f9a3ecbe5ed12ab4d8e11ba873c2f11161202b945",
                "total": 0,
            },
        },
        {
            "call_id": "call_Jg8Y6o6rUuQs8Sp2OqtyhrZE",
            "model_call_id": "142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e",
            "name": "risk_calculate_stock_quantity",
            "payload": {
                "available_cash": 100000.0,
                "maximum_notional": 10000.0,
                "notional": 9890.0,
                "price": 230.0,
                "quantity": 43,
                "remaining_notional": 110.0,
                "spendable_notional": 10000.0,
                "within_available_cash": True,
                "within_maximum_notional": True,
            },
        },
        {
            "call_id": "call_Mzavogvt2LfxhAJl3eq4vB9y",
            "model_call_id": "142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e",
            "name": "orders_submit_order",
            "payload": {
                "execution_outcome": {
                    "broker_state_certainty": "confirmed_submitted",
                    "decision_completed": True,
                    "fallback_used": False,
                    "impact": "completed",
                    "operation": "broker_order_submission",
                    "requiredness": "decision_critical",
                    "retryability": "not_applicable",
                },
                "order": {
                    "asset": {"symbol": "AAPL", "type": "stock"},
                    "decision_journal": {
                        "action_id": "action_11d5873ecdce45c8af023835174044ec",
                        "agent_name": "trader",
                        "decision_id": "decision_e29f1d15384f4f0fa9ae6e870194f3ca",
                        "deployment_id": None,
                        "model_call_id": "142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e",
                        "operation": "submit",
                        "previous_action_id": None,
                        "public_reason_preview": "AAPL is above the "
                        "mean of its five "
                        "latest completed "
                        "daily closes, "
                        "meeting the entry "
                        "rule. A 43-share "
                        "limit order stays "
                        "within the defined "
                        "portfolio exposure "
                        "cap; price "
                        "declines and "
                        "non-execution "
                        "remain possible.",
                        "public_reason_sha256": "90845318d387f8be6ed3c105d98604798ec8c9cc556f8617996ab129039b7338",
                        "reason_preview": "The conditional entry "
                        "rule is satisfied: the "
                        "current AAPL price at "
                        "2026-08-11 14:35 UTC is "
                        "$230, strictly above the "
                        "arithmetic mean of $227 "
                        "across the five latest "
                        "completed daily closes: "
                        "$225 on 2026-08-06, $226 "
                        "on 2026-08-07, $227 on "
                        "2026-08-08, $228 on "
                        "2026-08-09, and $229 on "
                        "2026-08-10. Account tools "
                        "showed $100,000 portfolio "
                        "value, $100,000 cash, a "
                        "USD cash position and no "
                        "AAPL holding or open "
                        "orders. The 10% portfolio "
                        "notional cap is $10,000; "
                        "the sizing tool returned "
                        "43 shares at the current "
                        "price, $9,890 notional, "
                        "below both the cap and "
                        "cash, leaving room for "
                        "price movement. This "
                        "single day limit buy at "
                        "$230 risks loss if AAPL "
                        "falls; the rule's entry "
                        "condition is invalidated "
                        "if current price is no "
                        "longer strictly above the "
                        "five-close mean, and "
                        "execution is not "
                        "guaranteed at this limi",
                        "reason_sha256": "dc4cf49812a22b1e253b3d0c9545d7fe737a652081a4bf5f90b4a419fb740e82",
                        "recorded_at": "2026-08-11T14:35:00+00:00",
                        "run_id": None,
                        "strategy_revision_id": None,
                    },
                    "decision_provenance": {
                        "decision_id": "decision_e29f1d15384f4f0fa9ae6e870194f3ca",
                        "model_call_id": "142db474b414c8e1a237bf952294f6369b8a8178f71bb803eac7a7fbf78c9e9e",
                    },
                    "identifier": "bt_1",
                    "limit_price": 230.0,
                    "order_type": "limit",
                    "quantity": 43.0,
                    "quote": {"symbol": "USD", "type": "forex"},
                    "side": "buy",
                    "status": "new",
                    "time_in_force": "day",
                },
            },
        },
    ],
}
