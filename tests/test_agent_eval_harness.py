import importlib.util
import sys
from pathlib import Path

import pytest


SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts/run_agent_evals.py"
SPEC = importlib.util.spec_from_file_location("run_agent_evals", SCRIPT_PATH)
assert SPEC is not None and SPEC.loader is not None
evals = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = evals
SPEC.loader.exec_module(evals)


def test_every_eval_case_uses_a_real_model_and_a_production_contract():
    cases = evals.load_cases()
    assert len(cases) >= 7
    assert len({case["id"] for case in cases}) == len(cases)
    for case in cases:
        assert case["model"] == "gemini-3.5-flash-lite"
        assert case["judgeRubric"].strip()
        assert case["machineContract"]
        assert "simulatedEvents" not in case


def test_release_publish_is_blocked_by_real_model_agent_evals():
    workflow = (Path(__file__).resolve().parents[1] / ".github/workflows/release.yml").read_text(
        encoding="utf-8"
    )
    assert "agent-evals:" in workflow
    assert "python scripts/run_agent_evals.py" in workflow
    assert "needs: [validate-build, unit-tests, backtest-tests, agent-evals]" in workflow
    assert "GEMINI_API_KEY: ${{ secrets.GEMINI_API_KEY }}" in workflow


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


def test_eval_batch_reservations_never_exceed_the_hard_spend_budget():
    case = evals.load_cases({"stock_price_before_order"})[0]
    work = [(case, repetition, "fingerprint") for repetition in range(1, 4)]
    per_repetition = evals.maximum_repetition_cost_usd(case, evals.DEFAULT_JUDGE_MODEL)

    batch, remaining = evals.reserve_budget_batch(
        work,
        max_workers=3,
        remaining_budget=(per_repetition * 2) + (per_repetition / 2),
        judge_model=evals.DEFAULT_JUDGE_MODEL,
    )

    assert len(batch) == 2
    assert len(remaining) == 1
    assert sum(item[3] for item in batch) <= (per_repetition * 2) + (per_repetition / 2)


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
    submit = next(
        tool for tool in evals.build_tools(fixture) if tool.name == "orders_submit_multileg"
    )

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
    assert fixture.submissions == []


def test_credit_spread_eval_has_an_honest_preserved_red_baseline():
    baseline_path = (
        Path(__file__).resolve().parents[1]
        / "agent_eval_baselines/2026-08-06_credit_spread_close_red.json"
    )
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
        Path(__file__).resolve().parents[1]
        / "agent_eval_baselines/2026-08-11_stock_pending_exit_duplicate_red.json"
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

    result = history.function(symbols="AAPL", length=30, timestep="minute")
    bars = result["bars"]["AAPL"]

    assert result["timestep"] == "minute"
    assert len(bars) == 20
    assert bars[0]["datetime"] == "2026-08-11T13:30:00Z"
    assert bars[14]["datetime"] == "2026-08-11T13:44:00Z"
    assert bars[15]["datetime"] == "2026-08-11T13:45:00Z"
    assert max(bar["high"] for bar in bars[:15]) == 228.5
    assert bars[19]["close"] == 230.0
    assert sum(bar["volume"] for bar in bars[15:20]) > max(
        sum(bar["volume"] for bar in bars[offset : offset + 5])
        for offset in range(0, 15, 5)
    )


def test_stock_orb_contract_requires_deterministic_quantity_calculation():
    case = evals.load_cases({"stock_orb_completed_bars"})[0]

    assert "risk_calculate_stock_quantity" in case["machineContract"]["requiredBeforeOrder"]

    fixture = evals.build_fixture("orb_breakout")
    sizing = next(
        tool for tool in evals.build_tools(fixture) if tool.name == "risk_calculate_stock_quantity"
    )
    result = sizing.function(maximum_notional=10_000, price=230, available_cash=100_000)

    assert result["quantity"] == 43
    assert result["notional"] == 9_890


def test_account_eval_fixtures_match_compact_pagination_contract():
    fixture = evals.build_fixture("open_credit_spread")
    tools = {tool.name: tool.function for tool in evals.build_tools(fixture)}

    positions = tools["account_positions"](offset=0, limit=1)
    orders = tools["orders_open_orders"](offset=0, limit=50)

    assert positions["total"] == 2
    assert positions["returned"] == 1
    assert positions["omitted"] == 1
    assert positions["complete"] is False
    assert positions["next_offset"] == 1
    assert positions["snapshot_id"] == "fixture-positions-2"
    assert orders["total"] == 0
    assert orders["returned"] == 0
    assert orders["omitted"] == 0
    assert orders["complete"] is True
    assert orders["next_offset"] is None
    assert orders["snapshot_id"] == "fixture-open-orders-0"


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

    assert fixture.positions == [
        {"symbol": "AAPL", "asset_type": "stock", "quantity": 43.0}
    ]
