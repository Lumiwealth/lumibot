import importlib.util
import sys
from pathlib import Path


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


def test_credit_spread_fixture_does_not_flatten_reversed_closing_sides():
    fixture = evals.build_fixture("open_credit_spread")
    submit = next(
        tool for tool in evals.build_tools(fixture) if tool.name == "orders_submit_multileg"
    )

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
