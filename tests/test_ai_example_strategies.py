"""Guards that keep the AI example strategies short and easy to follow.

On 2026-09-23 the examples grew a shared helper, ``example_strategies/agent_cycle.py``,
and a test that forced every example through it. Every example then ran five
agents (researcher, bull, bear, interpreter, trader), hid its trading rules in
the helper, and the Pelosi bot read three hard-coded PDFs that could never show
a new trade. Rob asked for the opposite: two agents, only LumiBot imports, about
60 lines, and data from a live website or API. These tests keep it that way.
"""

import ast
import hashlib
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

EXAMPLES = Path(__file__).resolve().parents[1] / "lumibot" / "example_strategies"

# file -> (class, agent names in run order, agents that may browse the web)
REBUILT = {
    "ai_nancy_pelosi_trading_bot.py": ("NancyPelosiTradingBot", ["researcher", "trader"], {"researcher"}),
    "ai_insider_trading_bot.py": ("InsiderTradingBot", ["researcher", "trader"], set()),
    "ai_fear_and_greed_trading_bot.py": ("FearAndGreedTradingBot", ["researcher", "trader"], {"researcher"}),
    "ai_iron_condor.py": ("AIIronCondorStrategy", ["researcher", "trader"], set()),
    "ai_credit_spread.py": ("AICreditSpreadStrategy", ["researcher", "trader"], set()),
    "ai_0dte_options_trading_bot.py": ("ZeroDTEOptionsTradingBot", ["researcher", "trader"], set()),
    "ai_vwap.py": ("AIVWAPStrategy", ["researcher", "trader"], set()),
    "ai_opening_range_breakout.py": ("AIOpeningRangeBreakoutStrategy", ["researcher", "trader"], set()),
    "ai_trading_team_warren_buffett_value.py": (
        "AITradingTeamWarrenBuffettValueStrategy", ["researcher", "trader"], set()
    ),
    "ai_trading_team_bill_ackman_concentrated.py": (
        "AITradingTeamBillAckmanConcentratedStrategy", ["researcher", "trader"], set()
    ),
    "ai_trading_team_bull_bear_large_cap_stocks.py": (
        "AITradingTeamBullBearLargeCapStocksStrategy", ["researcher", "bull", "bear", "trader"], set()
    ),
    "ai_trading_team_bull_bear_leveraged_etf.py": (
        "AITradingTeamBullBearLeveragedETFStrategy", ["researcher", "bull", "bear", "trader"], set()
    ),
}
BULL_BEAR_FILES = {"ai_trading_team_bull_bear_large_cap_stocks.py", "ai_trading_team_bull_bear_leveraged_etf.py"}

# Citadel and Ray Dalio run on BotSpot with a live track record. Rob wants the
# LumiBot copies to stay byte-for-byte the BotSpot revision (mainFileHash).
MARKETPLACE_COPIES = {
    "ai_trading_team_citadel_sector_pods.py": "50e78b923a9548994ba593f91a792c34f2d3ed384cc405ca3d2abecf5166a758",
    "ai_trading_team_citadel_sector_pods_leveraged.py": "3e9bc4330b0d8bab021f7844fa41541bcd86a7b24b67f371c4967bcf8e36f915",
    "ai_trading_team_ray_dalio_idea_meritocracy.py": "a2a02db9ad0db1b8ce8d9e339fe0f0cd8b0698b1ce36281c077291fa077e2914",
    "ai_trading_team_ray_dalio_idea_meritocracy_leveraged.py": (
        "7f8f2d4ef5363669926080d86504f68bdbd7ab30618fbac94dc2f0e469a304f1"
    ),
}


def _tree(name: str) -> ast.Module:
    return ast.parse((EXAMPLES / name).read_text())


def _created_agents(name: str) -> list[dict]:
    agents = []
    for node in ast.walk(_tree(name)):
        if isinstance(node, ast.Call) and ast.unparse(node.func) == "self.agents.create":
            agents.append({kw.arg: ast.literal_eval(kw.value) if kw.arg != "system_prompt" else None for kw in node.keywords})
    return agents


def test_no_ai_example_imports_example_helpers():
    for path in [*EXAMPLES.glob("ai_*.py"), *EXAMPLES.glob("agent_*.py")]:
        for node in ast.walk(ast.parse(path.read_text())):
            if isinstance(node, ast.ImportFrom) and node.module:
                assert not node.module.startswith("lumibot.example_strategies"), path.name
            if isinstance(node, ast.Import):
                assert not any(alias.name.startswith("lumibot.example_strategies") for alias in node.names), path.name
    assert not (EXAMPLES / "agent_cycle.py").exists()
    assert not (EXAMPLES / "agent_rules").exists()


@pytest.mark.parametrize("name", sorted(REBUILT))
def test_rebuilt_example_is_short_and_imports_only_lumibot(name):
    source = (EXAMPLES / name).read_text()
    assert len(source.splitlines()) <= 70, name
    modules = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.ImportFrom):
            modules.add(node.module)
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
    assert modules <= {"datetime", "lumibot.strategies", "lumibot.backtesting"}, (name, modules)
    for banned in ("rules_path", "os.environ", "budget=", "run_cycle", "interpreter"):
        assert banned not in source, (name, banned)


@pytest.mark.parametrize("name", sorted(REBUILT))
def test_rebuilt_example_has_its_agents_and_one_trader(name):
    _, order, browsing = REBUILT[name]
    agents = _created_agents(name)
    assert [agent["name"] for agent in agents] == [n for n in ["researcher", "bull", "bear", "trader"] if n in order]
    traders = [agent["name"] for agent in agents if agent["allow_trading"]]
    assert traders == ["trader"], name
    assert {agent["name"] for agent in agents if agent.get("allow_network")} == browsing, name


def test_only_the_two_bull_bear_bots_debate():
    for path in EXAMPLES.glob("ai_*.py"):
        if path.name in BULL_BEAR_FILES or path.name in MARKETPLACE_COPIES:
            continue
        # Any string that names a debate agent counts, however the agent is created.
        strings = {
            node.value for node in ast.walk(_tree(path.name))
            if isinstance(node, ast.Constant) and isinstance(node.value, str)
        }
        assert not strings & {"bull", "bear", "interpreter"}, path.name


@pytest.mark.parametrize("name", sorted(REBUILT))
def test_rebuilt_example_runs_as_a_backtest_or_live(name):
    runner = next(
        ast.unparse(node)
        for node in _tree(name).body
        if isinstance(node, ast.If) and "__main__" in ast.unparse(node.test)
    )
    assert "IS_BACKTESTING" in runner
    assert ".backtest(" in runner
    assert ".run_live()" in runner


class _Agent:
    def __init__(self, name, calls):
        self.name = name
        self.calls = calls

    def run(self, task_prompt, context=None):
        self.calls.append((self.name, dict(context or {})))
        return SimpleNamespace(summary=f"{self.name} summary")


class _Agents:
    def __init__(self):
        self.calls = []

    def __getitem__(self, name):
        return _Agent(name, self.calls)

    def run_together(self, jobs):
        return {name: _Agent(name, self.calls).run(task, context) for name, task, context in jobs}


@pytest.mark.parametrize("name", sorted(REBUILT))
def test_research_is_handed_to_the_trader(name):
    class_name, order, _ = REBUILT[name]
    module = __import__(f"lumibot.example_strategies.{name[:-3]}", fromlist=[class_name])
    strategy_class = getattr(module, class_name)
    strategy = object.__new__(strategy_class)
    agents = _Agents()
    strategy._agents_for_test = agents
    strategy.__dict__["parameters"] = dict(strategy_class.parameters)
    strategy.get_datetime = lambda: datetime(2026, 1, 6, 11, 30, tzinfo=ZoneInfo("America/New_York"))
    type(strategy).agents = property(lambda self: self._agents_for_test)
    try:
        strategy.on_trading_iteration()
    finally:
        del type(strategy).agents

    assert [call[0] for call in agents.calls] == order
    trader_context = agents.calls[-1][1]
    assert trader_context["research"] == "researcher summary"
    if name in BULL_BEAR_FILES:
        assert trader_context["bull"] == "bull summary"
        assert trader_context["bear"] == "bear summary"


@pytest.mark.parametrize("name", ["ai_vwap.py", "ai_opening_range_breakout.py"])
def test_intraday_bots_wait_for_the_first_bars(name):
    class_name, _, _ = REBUILT[name]
    module = __import__(f"lumibot.example_strategies.{name[:-3]}", fromlist=[class_name])
    strategy_class = getattr(module, class_name)
    strategy = object.__new__(strategy_class)
    agents = _Agents()
    strategy._agents_for_test = agents
    strategy.__dict__["parameters"] = dict(strategy_class.parameters)
    strategy.get_datetime = lambda: datetime(2026, 1, 6, 9, 30, tzinfo=ZoneInfo("America/New_York"))
    type(strategy).agents = property(lambda self: self._agents_for_test)
    try:
        strategy.on_trading_iteration()
    finally:
        del type(strategy).agents
    assert agents.calls == []


@pytest.mark.parametrize("name", sorted(MARKETPLACE_COPIES))
def test_citadel_and_dalio_match_the_botspot_marketplace_revision(name):
    digest = hashlib.sha256((EXAMPLES / name).read_bytes()).hexdigest()
    assert digest == MARKETPLACE_COPIES[name], (
        f"{name} no longer matches the BotSpot revision with the live track record. "
        "Change it on BotSpot first, then copy that exact file here."
    )
