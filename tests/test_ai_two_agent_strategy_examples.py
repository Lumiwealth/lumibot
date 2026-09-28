import hashlib
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from lumibot.example_strategies.ai_credit_spread import AICreditSpreadStrategy
from lumibot.example_strategies.ai_iron_condor import AIIronCondorStrategy
from lumibot.example_strategies.ai_opening_range_breakout import AIOpeningRangeBreakoutStrategy
from lumibot.example_strategies.ai_spx_zero_dte_bear_call_team import AISpxZeroDteBearCallTeamStrategy
from lumibot.example_strategies.ai_vwap import AIVWAPStrategy


class _Agents(dict):
    def __init__(self):
        super().__init__()
        self.created = []
        self.calls = []

    def create(self, **kwargs):
        self.created.append(kwargs)
        name = kwargs["name"]

        def run(**run_kwargs):
            self.calls.append((name, run_kwargs))
            return SimpleNamespace(summary=f"{name} evidence")

        self[name] = SimpleNamespace(run=run)

    def run_together(self, jobs):
        results = {}
        for name, task_prompt, context in jobs:
            results[name] = self[name].run(task_prompt=task_prompt, context=context)
        return results


@pytest.mark.parametrize(
    ("strategy_class", "researcher_name"),
    [
        (AIOpeningRangeBreakoutStrategy, "orb_researcher"),
        (AIVWAPStrategy, "vwap_researcher"),
        (AICreditSpreadStrategy, "credit_spread_researcher"),
        (AIIronCondorStrategy, "iron_condor_researcher"),
    ],
)
def test_ai_examples_separate_research_from_trading_and_risk(strategy_class, researcher_name):
    """The recommended team topology requires a dedicated trading/risk owner."""
    agents = _Agents()
    context = SimpleNamespace(
        agents=agents,
        parameters=dict(strategy_class.parameters),
        get_datetime=lambda: datetime(2026, 9, 18, 14, 30),
    )

    strategy_class.initialize(context)
    strategy_class.on_trading_iteration(context)

    assert [(item["name"], item["allow_trading"]) for item in agents.created] == [
        (researcher_name, False),
        ("bull", False),
        ("bear", False),
        ("interpreter", False),
        ("trading_risk_manager", True),
    ]
    assert [name for name, _ in agents.calls] == [
        researcher_name,
        "bull",
        "bear",
        "interpreter",
        "trading_risk_manager",
    ]
    trader_context = agents.calls[-1][1]["context"]
    assert trader_context["research_evidence"] == f"{researcher_name} evidence"

    researcher = agents.created[0]
    trader = agents.created[-1]
    assert "do not submit orders" in researcher["system_prompt"].lower()
    assert "only trading agent" in trader["system_prompt"].lower()
    assert "risk" in trader["system_prompt"].lower()


@pytest.mark.parametrize("strategy_class", [AIOpeningRangeBreakoutStrategy, AIVWAPStrategy])
def test_intraday_teams_wait_for_completed_session_bars(strategy_class):
    """At the 09:30 open no session bar exists, so the team must not spend model calls."""
    eastern = ZoneInfo("America/New_York")
    now = {"value": datetime(2026, 1, 5, 9, 30, tzinfo=eastern)}
    agents = _Agents()
    context = SimpleNamespace(
        agents=agents,
        parameters=dict(strategy_class.parameters),
        get_datetime=lambda: now["value"],
    )

    strategy_class.initialize(context)
    assert not str(context.sleeptime).upper().endswith("D")

    strategy_class.on_trading_iteration(context)
    assert agents.calls == []

    now["value"] = datetime(2026, 1, 5, 10, 30, tzinfo=eastern)
    strategy_class.on_trading_iteration(context)
    assert len(agents.calls) == 5


@pytest.mark.parametrize("strategy_class", [AICreditSpreadStrategy, AIIronCondorStrategy])
def test_option_interpreters_judge_against_the_strategy_policy(strategy_class):
    """The interpreter must not reject the defined structure the strategy exists to trade."""
    agents = _Agents()
    context = SimpleNamespace(
        agents=agents,
        parameters=dict(strategy_class.parameters),
        get_datetime=lambda: datetime(2026, 9, 18, 14, 30),
    )

    strategy_class.initialize(context)

    created = {item["name"]: item for item in agents.created}
    interpreter = created["interpreter"]["system_prompt"]
    trader = created["trading_risk_manager"]["system_prompt"]
    assert "do not submit orders" in interpreter.lower()
    assert "Strategy policy:" in interpreter
    assert f"Wings must be exactly" in interpreter or "long wing exactly" in interpreter
    assert "maximum loss is larger than its credit" in interpreter
    assert "name the failed policy condition" in interpreter.lower()
    assert "Strategy policy:" in trader


@pytest.mark.parametrize(
    "strategy_class", [AICreditSpreadStrategy, AIIronCondorStrategy, AISpxZeroDteBearCallTeamStrategy]
)
def test_option_contract_cap_limits_size_instead_of_blocking_the_trade(strategy_class):
    """A binding contract cap once made every SPY 0 DTE cycle decline a valid spread."""
    agents = _Agents()
    context = SimpleNamespace(agents=agents, parameters=dict(strategy_class.parameters))

    strategy_class.initialize(context)

    for item in agents.created:
        if item["name"] not in ("interpreter", "trader", "trading_risk_manager"):
            continue
        prompt = " ".join(item["system_prompt"].split())
        assert "size to the risk target or the contract cap, whichever is smaller" in prompt
        assert "the cap is never a reason to skip" in prompt


def test_documentation_and_artwork_contracts_describe_the_real_topology():
    repo = Path(__file__).resolve().parents[1]
    two_agent_pages = [
        "agents_example_ai_opening_range_breakout.rst",
        "agents_example_ai_vwap.rst",
        "agents_example_ai_credit_spread.rst",
        "agents_example_ai_iron_condor.rst",
    ]
    for filename in two_agent_pages:
        page = (repo / "docsrc" / filename).read_text(encoding="utf-8").lower()
        assert "research" in page
        assert "trading" in page
        assert "risk" in page
        assert "creates one" not in page

    receipt = (repo / "docs/research/2026-09-21_sunburst-workflow-artwork-receipt.md").read_text(
        encoding="utf-8"
    )
    assert "-> `LumiBot`" not in receipt
    assert "`Macro Idea Meritocracy`" not in receipt
    assert "`Sector Pod Team`" not in receipt
    assert "`Concentrated Quality Team`" not in receipt
    assert "`Long-Term Value Team`" not in receipt

    two_agent_artwork = {
        "ai-opening-range-breakout.png": "Range Researcher",
        "ai-vwap.png": "VWAP Researcher",
        "ai-credit-spread.png": "Spread Researcher",
        "ai-iron-condor.png": "Condor Researcher",
        "ai-congress-disclosures.png": "Disclosure Researcher",
        "ai-sec-insider-filings.png": "Form 4 Researcher",
        "ai-spx-zero-dte-bear-call-team.png": "SPX Researcher",
    }
    for asset, researcher in two_agent_artwork.items():
        row = next(line for line in receipt.splitlines() if f"`{asset}`" in line)
        assert researcher in row
        assert "AGENT" in row
        assert "Trading & Risk" in row
        assert "Trade Order" in row
        assert "INPUT" not in row and "OUTPUT" not in row

    ray_row = next(line for line in receipt.splitlines() if "`ray-dalio-idea-meritocracy.png`" in line)
    for label in (
        "Growth",
        "Inflation",
        "Debt & Liquidity",
        "Disagreement",
        "Trading & Risk",
        "Trade Order",
    ):
        assert label in ray_row
    assert "branches" in ray_row.lower()

    citadel_row = next(line for line in receipt.splitlines() if "`citadel-sector-pods.png`" in line)
    for label in (
        "Technology & Comms",
        "Financials",
        "Healthcare",
        "Energy",
        "Consumer",
        "Risk Manager",
        "Portfolio Manager",
        "Trade Order",
    ):
        assert label in citadel_row
    assert "branches" in citadel_row.lower()


def test_artwork_receipt_hashes_match_committed_assets():
    repo = Path(__file__).resolve().parents[1]
    receipt = (repo / "docs/research/2026-09-21_sunburst-workflow-artwork-receipt.md").read_text(
        encoding="utf-8"
    )
    rows = [line for line in receipt.splitlines() if line.startswith("| `") and ".png` |" in line]

    assert len(rows) == 14
    for row in rows:
        asset = row.split("`", 2)[1]
        expected_sha256 = row.rsplit("`", 2)[1]
        collection = "ai-agent-workflows" if asset.startswith("ai-") else "ai-trading-team-workflows"
        image_path = repo / "docs/assets" / collection / asset

        assert image_path.is_file()
        assert hashlib.sha256(image_path.read_bytes()).hexdigest() == expected_sha256


def _created_prompts(strategy_class):
    agents = _Agents()
    context = SimpleNamespace(agents=agents, parameters=dict(strategy_class.parameters))
    strategy_class.initialize(context)
    return {item["name"]: " ".join(item["system_prompt"].split()) for item in agents.created}


def test_public_ai_examples_ship_conservative_risk_defaults():
    """Users copy these examples into live trading, so the shipped risk limits stay small."""
    assert AIOpeningRangeBreakoutStrategy.parameters["risk_fraction"] <= 0.01
    assert AIVWAPStrategy.parameters["risk_fraction"] <= 0.01
    for strategy_class in (AICreditSpreadStrategy, AIIronCondorStrategy):
        assert strategy_class.parameters["max_risk_pct"] <= 0.02
        assert strategy_class.parameters["max_contracts"] <= 10
    assert AISpxZeroDteBearCallTeamStrategy.parameters["max_risk_pct"] <= 0.01
    assert AISpxZeroDteBearCallTeamStrategy.parameters["max_contracts"] <= 2


def test_stock_example_prompt_builders_share_one_risk_fraction_default():
    from lumibot.example_strategies.ai_opening_range_breakout import (
        build_orb_system_prompt,
        build_orb_trading_prompt,
    )
    from lumibot.example_strategies.ai_vwap import build_vwap_system_prompt

    for prompt in (build_orb_system_prompt({}), build_orb_trading_prompt({}), build_vwap_system_prompt({})):
        flat = " ".join(prompt.split())
        assert "stop risk is at most 1.00% of portfolio value" in flat


def test_orb_trader_uses_the_orb_trading_prompt():
    from lumibot.example_strategies.ai_opening_range_breakout import build_orb_trading_prompt

    trader = _created_prompts(AIOpeningRangeBreakoutStrategy)["trading_risk_manager"]
    assert trader == " ".join(build_orb_trading_prompt(AIOpeningRangeBreakoutStrategy.parameters).split())
    assert "Verify the exact symbol, completed 15-minute opening range" in trader
    assert "stop on the opposite side of the verified range" in trader
    assert "capped at 200 shares" in trader
    assert "only trading agent" in trader.lower()
    assert "take-profit limit" in trader
    assert "protective stop" in trader
    assert "same session as the entry" in trader


def test_vwap_trader_receives_the_entry_threshold_share_cap_and_stop_risk():
    trader = _created_prompts(AIVWAPStrategy)["trading_risk_manager"]
    assert "at least 0.15% below VWAP" in trader
    assert "stop risk is at most 1.00% of portfolio value" in trader
    assert "capped at 200 shares" in trader
    assert "Use about the risk fraction of the account" not in trader
