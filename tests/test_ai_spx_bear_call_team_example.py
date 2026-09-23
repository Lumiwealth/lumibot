import json
from pathlib import Path

from lumibot.example_strategies.ai_spx_zero_dte_bear_call_team import (
    AISpxZeroDteBearCallTeamStrategy,
    build_research_prompt,
    build_trader_prompt,
)


EXAMPLE = (
    Path(__file__).resolve().parents[1]
    / "lumibot"
    / "example_strategies"
    / "ai_spx_zero_dte_bear_call_team.py"
)
RULES = EXAMPLE.with_name("agent_rules") / "ai_spx_zero_dte_bear_call_team.rules.json"


def test_spx_experiment_is_a_two_agent_research_then_trade_flow():
    source = EXAMPLE.read_text(encoding="utf-8")

    assert '"researcher"' in source
    assert 'allow_trading=False' in source
    assert '"trader"' in source
    assert 'allow_trading=True' in source
    assert "run_cycle(" in source
    assert "orders_submit_multileg" in source
    trader_prompt = _flat(build_trader_prompt(AISpxZeroDteBearCallTeamStrategy.parameters))
    assert "verify the submitted order" in trader_prompt
    assert "deterministic executor" not in source.lower()


def test_spx_experiment_requires_one_atomic_five_point_package():
    source = EXAMPLE.read_text(encoding="utf-8")

    assert "SPX" in source
    assert "0 DTE" in source
    assert "bear call" in source.lower()
    trader_prompt = _flat(build_trader_prompt(AISpxZeroDteBearCallTeamStrategy.parameters))
    assert "exactly 5 points" in trader_prompt
    assert "one atomic multi-leg package" in trader_prompt
    assert "rules_path=" in source


def _flat(text: str) -> str:
    return " ".join(text.split())


def test_prompts_follow_the_configured_underlying_and_asset_type():
    spy = {**AISpxZeroDteBearCallTeamStrategy.parameters, "underlying": "SPY", "wing_width": 1}
    research = _flat(build_research_prompt(spy))
    trader = _flat(build_trader_prompt(spy))

    for prompt in (research, trader):
        assert "SPY" in prompt
        assert "SPX" not in prompt
        assert "asset type stock" in prompt
    assert "exactly 1 points higher" in trader

    default = _flat(build_trader_prompt(AISpxZeroDteBearCallTeamStrategy.parameters))
    assert "SPX" in default
    assert "asset type index" in default


def test_rules_file_defers_underlying_and_wing_to_strategy_parameters():
    rules = json.loads(RULES.read_text(encoding="utf-8"))["rules"]
    text = " ".join(rule["interpretation"] for rule in rules)

    assert "SPX" not in text
    assert "five strike points" not in text
    assert "configured underlying" in text
    assert "configured wing_width" in text


def test_spx_zero_dte_team_runs_intraday_for_same_day_management():
    source = EXAMPLE.read_text(encoding="utf-8")

    assert 'self.sleeptime = str(self.parameters.get("sleeptime", "5M"))' in source
