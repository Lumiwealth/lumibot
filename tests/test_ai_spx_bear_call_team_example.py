from pathlib import Path

from lumibot.example_strategies.ai_spx_zero_dte_bear_call_team import (
    AISpxZeroDteBearCallTeamStrategy,
    build_trader_prompt,
)


EXAMPLE = (
    Path(__file__).resolve().parents[1]
    / "lumibot"
    / "example_strategies"
    / "ai_spx_zero_dte_bear_call_team.py"
)


def test_spx_experiment_is_a_two_agent_research_then_trade_flow():
    source = EXAMPLE.read_text(encoding="utf-8")

    assert 'name="researcher"' in source
    assert 'allow_trading=False' in source
    assert 'name="trader"' in source
    assert 'allow_trading=True' in source
    assert '"research": research.summary' in source
    assert "orders_submit_multileg" in source
    trader_prompt = build_trader_prompt(AISpxZeroDteBearCallTeamStrategy.parameters)
    assert "verify the\nsubmitted order" in trader_prompt
    assert "deterministic executor" not in source.lower()


def test_spx_experiment_requires_one_atomic_five_point_package():
    source = EXAMPLE.read_text(encoding="utf-8")

    assert "SPX" in source
    assert "0 DTE" in source
    assert "bear call" in source.lower()
    trader_prompt = build_trader_prompt(AISpxZeroDteBearCallTeamStrategy.parameters)
    assert "exactly\n5 points" in trader_prompt
    assert "one atomic\nmulti-leg package" in trader_prompt
    assert "rules_path=" in source


def test_spx_zero_dte_team_runs_intraday_for_same_day_management():
    source = EXAMPLE.read_text(encoding="utf-8")

    assert 'self.sleeptime = "5M"' in source
