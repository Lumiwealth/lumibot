"""Guards that keep the AI example strategies short and easy to follow.

On 2026-09-23 the examples grew a shared helper, ``example_strategies/agent_cycle.py``,
and a test that forced every example through it. Every example then ran five
agents (researcher, bull, bear, interpreter, trader), hid its trading rules in
the helper, and the Pelosi bot read three hard-coded PDFs that could never show
a new trade. Rob asked for the opposite: only LumiBot imports, short files, data
from a live website or API, and agents that fit the strategy. The number of
agents is not fixed (Rob, 2026-09-29): Citadel runs pods in parallel, Pelosi can
split lookup, rebalance planning, and trading. What is never allowed is bull and
bear agents bolted onto a strategy that is not a debate.
"""

import ast
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from lumibot.strategies._strategy import Vars

EXAMPLES = Path(__file__).resolve().parents[1] / "lumibot" / "example_strategies"

# file -> (class, agent names in run order, agents that may browse the web)
REBUILT = {
    "ai_nancy_pelosi_trading_bot.py": ("NancyPelosiTradingBot", ["researcher", "portfolio", "trader"], {"researcher"}),
    "ai_nancy_pelosi_copy_trading_bot.py": (
        "NancyPelosiCopyTradingBot", ["researcher", "portfolio", "trader"], {"researcher"}
    ),
    "ai_insider_trading_bot.py": ("InsiderTradingBot", ["researcher", "trader"], set()),
    "ai_fear_and_greed_trading_bot.py": ("FearAndGreedTradingBot", ["researcher", "trader"], {"researcher"}),
    "ai_iron_condor.py": ("AIIronCondorStrategy", ["trader"], set()),
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
# Older one-agent demos, rebuilt 2026-09-29 on LumiBot's generic tools (they
# used to hand-write their own FRED, price, and news tools with requests).
REBUILT.update({
    "agent_m2_liquidity.py": ("M2LiquidityStrategy", ["trader"], set()),
    "agent_m2_liquidity_openai.py": ("M2LiquidityOpenAIStrategy", ["trader"], set()),
    "agent_m2_liquidity_anthropic.py": ("M2LiquidityAnthropicStrategy", ["trader"], set()),
    "agent_m2_liquidity_grok.py": ("M2LiquidityGrokStrategy", ["trader"], set()),
    "agent_macro_risk.py": ("MacroRiskStrategy", ["trader"], set()),
    "agent_momentum_allocator.py": ("MomentumAllocatorStrategy", ["trader"], set()),
    "agent_news_sentiment.py": ("NewsSentimentStrategy", ["trader"], set()),
    "agent_alpaca_news_builtin.py": ("AlpacaNewsBuiltinStrategy", ["trader"], set()),
    "agent_discretionary.py": ("DiscretionaryTraderStrategy", ["trader"], set()),
})
BULL_BEAR_FILES = {"ai_trading_team_bull_bear_large_cap_stocks.py", "ai_trading_team_bull_bear_leveraged_etf.py"}

# Citadel and Ray Dalio run on BotSpot with a live track record and stay
# byte-for-byte the BotSpot revision; tests/test_public_docs_community_links.py
# checks their hashes.
MARKETPLACE_COPIES = {
    "ai_trading_team_citadel_sector_pods.py",
    "ai_trading_team_citadel_sector_pods_leveraged.py",
    "ai_trading_team_ray_dalio_idea_meritocracy.py",
    "ai_trading_team_ray_dalio_idea_meritocracy_leveraged.py",
}


def _tree(name: str) -> ast.Module:
    return ast.parse((EXAMPLES / name).read_text())


def _created_agents(name: str) -> list[dict]:
    agents = []
    for node in ast.walk(_tree(name)):
        if isinstance(node, ast.Call) and ast.unparse(node.func) == "self.agents.create":
            # Only the name and permission flags matter here; prompts may be any expression.
            agents.append(
                {kw.arg: ast.literal_eval(kw.value) for kw in node.keywords
                 if kw.arg in {"name", "allow_trading", "allow_network"}}
            )
    return agents


def test_no_ai_example_writes_its_own_tools():
    # Rob, 2026-09-29: examples show off LumiBot's generic tools. A tool written
    # for one example (or one website) is never allowed, in the example or in core.
    for path in [*EXAMPLES.glob("ai_*.py"), *EXAMPLES.glob("agent_*.py")]:
        source = path.read_text()
        for banned in ("@agent_tool", "agent_tool(", "import requests", "tools=[", "mcp_servers="):
            assert banned not in source, (path.name, banned)


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
    # Short, not a hard target: Rob wants about 60 lines and mostly prompts.
    assert len(source.splitlines()) <= 100, name
    modules = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.ImportFrom):
            modules.add(node.module)
        if isinstance(node, ast.Import):
            modules.update(alias.name for alias in node.names)
    assert modules <= {"datetime", "lumibot.strategies", "lumibot.backtesting", "lumibot.credentials"}, (name, modules)
    for banned in ("rules_path", "os.environ", "budget=", "run_cycle", "interpreter"):
        assert banned not in source, (name, banned)


@pytest.mark.parametrize("name", sorted(REBUILT))
def test_rebuilt_example_has_exactly_one_trading_agent(name):
    _, _, browsing = REBUILT[name]
    agents = _created_agents(name)
    assert agents, name
    traders = [agent["name"] for agent in agents if agent["allow_trading"]]
    assert len(traders) == 1, (name, traders)
    web = {agent["name"] for agent in agents if agent.get("allow_network")}
    assert traders[0] not in web, "the trading agent never browses; it gets the research as evidence"
    assert bool(web) == bool(browsing), name


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


# Rob, 2026-09-29: every example ends with the same main block as a BotSpot main.py.
STANDARD_MAIN = """if __name__ == "__main__":
    from lumibot.credentials import IS_BACKTESTING

    if IS_BACKTESTING:
        from lumibot.backtesting import {source}

        {cls}.backtest({source})
    else:
        {cls}().run_live()
"""
# Pages owned by dedicated sessions (2026-09-29); they follow the same rules and
# remove themselves from this set when their rewrite lands.
OWNED_ELSEWHERE: set[str] = set()


@pytest.mark.parametrize("name", sorted(set(REBUILT) - OWNED_ELSEWHERE))
def test_rebuilt_example_ends_with_the_standard_main_block(name):
    source = (EXAMPLES / name).read_text()
    cls = REBUILT[name][0]
    main = source[source.index('if __name__ == "__main__":'):]
    assert any(main == STANDARD_MAIN.format(source=ds, cls=cls) for ds in ("YahooDataBacktesting", "AlpacaBacktesting")), main


# Prompts are plain English a non-technical trader could write. LumiBot's
# skills and tools know the mechanics; the example never names them.
TECH_WORDS = ("duckdb", "sql", "skill", "multi-leg", "multileg", "get_filings", "get_filing", "http_request",
              "house_public_disclosures", "get_indicator", "rules.json", "tool")


@pytest.mark.parametrize("name", sorted(set(REBUILT) - OWNED_ELSEWHERE))
def test_prompts_are_plain_english(name):
    prompts = " ".join(
        node.value for node in ast.walk(_tree(name))
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    ).lower()
    for word in TECH_WORDS:
        assert word not in prompts, (name, word)


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


# Bots that run the AI at set times (not every iteration) have their own tests below.
TIMED = {"ai_iron_condor.py"}


@pytest.mark.parametrize("name", sorted(set(REBUILT) - TIMED))
def test_research_is_handed_to_the_trader(name):
    class_name, _, _ = REBUILT[name]
    module = __import__(f"lumibot.example_strategies.{name[:-3]}", fromlist=[class_name])
    strategy_class = getattr(module, class_name)
    strategy = object.__new__(strategy_class)
    agents = _Agents()
    strategy._agents_for_test = agents
    strategy.vars = Vars()
    strategy.__dict__["parameters"] = dict(getattr(strategy_class, "parameters", {}) or {})
    strategy.get_datetime = lambda: datetime(2026, 1, 6, 11, 30, tzinfo=ZoneInfo("America/New_York"))
    type(strategy).agents = property(lambda self: self._agents_for_test)
    try:
        strategy.on_trading_iteration()
    finally:
        del type(strategy).agents

    trader = next(agent["name"] for agent in _created_agents(name) if agent["allow_trading"])
    called = [call[0] for call in agents.calls]
    assert called[-1] == trader, called
    if len(called) > 1:
        earlier = {f"{agent} summary" for agent in called[:-1]}
        assert earlier & set(map(str, agents.calls[-1][1].values())), "the trader must get the other agents' work"
    if name in BULL_BEAR_FILES:
        assert {"bull", "bear"} <= set(called)


@pytest.mark.parametrize("name", ["ai_vwap.py", "ai_opening_range_breakout.py"])
def test_intraday_bots_wait_for_the_first_bars(name):
    class_name, _, _ = REBUILT[name]
    module = __import__(f"lumibot.example_strategies.{name[:-3]}", fromlist=[class_name])
    strategy_class = getattr(module, class_name)
    strategy = object.__new__(strategy_class)
    agents = _Agents()
    strategy._agents_for_test = agents
    strategy.__dict__["parameters"] = dict(getattr(strategy_class, "parameters", {}) or {})
    strategy.get_datetime = lambda: datetime(2026, 1, 6, 9, 30, tzinfo=ZoneInfo("America/New_York"))
    type(strategy).agents = property(lambda self: self._agents_for_test)
    try:
        strategy.on_trading_iteration()
    finally:
        del type(strategy).agents
    assert agents.calls == []


def _iron_condor(when, positions=(), orders=(), price=595.0):
    from lumibot.example_strategies.ai_iron_condor import AIIronCondorStrategy

    strategy = object.__new__(AIIronCondorStrategy)
    agents = _Agents()
    strategy._agents_for_test = agents
    strategy.__dict__["parameters"] = dict(AIIronCondorStrategy.parameters)
    strategy.get_datetime = lambda: when
    strategy.get_positions = lambda: list(positions)
    strategy.get_orders = lambda: list(orders)
    strategy.get_last_price = lambda asset: price
    return strategy, agents


def _option(strike, quantity):
    return SimpleNamespace(asset=SimpleNamespace(asset_type="option", strike=strike), quantity=quantity)


def _condor(put=590.0, call=600.0):
    return [_option(put, -10), _option(put - 1, 10), _option(call, -10), _option(call + 1, 10)]


def _run(strategy, method):
    type(strategy).agents = property(lambda self: self._agents_for_test)
    try:
        getattr(strategy, method)()
    finally:
        del type(strategy).agents


def test_iron_condor_opens_once_a_day_at_345():
    """The AI runs once a day to open the condor, at 3:45 PM, not on every 5-minute check."""
    from lumibot.example_strategies.ai_iron_condor import AIIronCondorStrategy

    tz = ZoneInfo("America/New_York")
    strategy, agents = _iron_condor(datetime(2026, 1, 6, 11, 30, tzinfo=tz))
    _run(strategy, "on_trading_iteration")
    assert agents.calls == [], "no condor held: the 5-minute check never calls the AI"
    _run(strategy, "before_market_closes")
    assert [call[0] for call in agents.calls] == ["trader"]
    source = (EXAMPLES / "ai_iron_condor.py").read_text()
    assert "self.minutes_before_closing = 15" in source and 'self.sleeptime = "5M"' in source
    assert AIIronCondorStrategy.parameters["stop_at"] == 0.4


def test_iron_condor_stop_is_plain_python_and_wakes_the_ai_only_near_a_short_strike():
    """Old 1DTE condor backtests only made money with this stop (close when SPY runs 40% of the way
    from the middle toward a short strike). It is checked in Python every 5 minutes at no AI cost."""
    tz = ZoneInfo("America/New_York")
    when = datetime(2026, 1, 7, 11, 30, tzinfo=tz)
    # Shorts at 590 and 600: middle 595, halfway to a short strike is 5 points; 40% of that is 2 points.
    for price, woken in ((595.0, False), (596.9, False), (597.0, True), (592.5, True)):
        strategy, agents = _iron_condor(when, positions=_condor(), price=price)
        _run(strategy, "on_trading_iteration")
        assert bool(agents.calls) is woken, price
    strategy, agents = _iron_condor(when, positions=_condor(), price=599.0,
                                    orders=[SimpleNamespace(is_active=lambda: True)])
    _run(strategy, "on_trading_iteration")
    assert agents.calls == [], "a close order is already working: do not call the AI again"


@pytest.mark.parametrize("name", ["ai_nancy_pelosi_trading_bot.py", "ai_nancy_pelosi_copy_trading_bot.py"])
def test_pelosi_bots_rebalance_only_when_her_newest_filing_changes(name):
    # 2026-09-30 switch-window backtests: a full answer that mentioned "NOTHING NEW"
    # in passing skipped day one, and answers like "Newest filing: May 15, 2026,
    # DocID 10075701. NOTHING NEW." triggered a rebalance and option churn. The
    # bot now remembers her newest filing's DocID and rebalances only when it changes.
    class_name = REBUILT[name][0]
    module = __import__(f"lumibot.example_strategies.{name[:-3]}", fromlist=[class_name])
    strategy_class = getattr(module, class_name)
    strategy = object.__new__(strategy_class)
    strategy.vars = Vars()
    strategy.__dict__["parameters"] = dict(strategy_class.parameters)

    def run_day(answer):
        calls = []

        class _Named(_Agent):
            def run(self, task_prompt, context=None):
                calls.append(self.name)
                return SimpleNamespace(summary=answer if self.name == "researcher" else f"{self.name} summary")

        class _Team(_Agents):
            def __getitem__(self, agent_name):
                return _Named(agent_name, [])

        strategy._agents_for_test = _Team()
        type(strategy).agents = property(lambda self: self._agents_for_test)
        try:
            strategy.on_trading_iteration()
        finally:
            del type(strategy).agents
        return calls

    first = "Newest filing: January 23, 2026, DocID 20033725. This is not NOTHING NEW for me yet. Holdings: AAPL."
    assert run_day(first) == ["researcher", "portfolio", "trader"]
    assert run_day("Newest filing: January 23, 2026, DocID 20033725. Holdings: AAPL.") == ["researcher"]
    assert run_day("RESULT: NOTHING NEW") == ["researcher"]
    assert run_day("Newest filing: May 15, 2026 — DocID 10075701. NOTHING NEW.") == ["researcher", "portfolio", "trader"]
    assert run_day("Newest filing: May 15, 2026 — DocID 10075701. NOTHING NEW.") == ["researcher"]
    # A partial answer without an ID must not erase the last known filing.
    assert run_day("Holdings: AAPL. The filing ID could not be read.") == ["researcher", "portfolio", "trader"]
    assert run_day("Newest filing: May 15, 2026 — DocID 10075701. NOTHING NEW.") == ["researcher"]
