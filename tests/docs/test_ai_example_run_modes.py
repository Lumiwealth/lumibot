"""Keep AI example entry points and their public run-mode guidance in sync."""

import ast
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
EXAMPLES = ROOT / "lumibot" / "example_strategies"
DOCS = ROOT / "docsrc"
PUBLISHED_SOURCE_FILES = {
    "ai_trading_team_citadel_sector_pods.py",
    "ai_trading_team_citadel_sector_pods_leveraged.py",
    "ai_trading_team_ray_dalio_idea_meritocracy.py",
    "ai_trading_team_ray_dalio_idea_meritocracy_leveraged.py",
}


def _direct_mode(path: Path) -> str:
    tree = ast.parse(path.read_text())
    main_blocks = [
        node
        for node in tree.body
        if isinstance(node, ast.If)
        and isinstance(node.test, ast.Compare)
        and "__main__" in ast.unparse(node.test)
        and "__name__" in ast.unparse(node.test)
    ]
    if not main_blocks:
        return "no direct runner"
    runner = "\n".join(ast.unparse(node) for node in main_blocks)
    backtest = ".backtest(" in runner or ".run_backtest(" in runner
    broker = ".run_all(" in runner or ".run_live(" in runner
    if backtest and broker:
        return "backtest and broker"
    if backtest:
        return "backtest only"
    if broker:
        return "broker only"
    return "no direct runner"


def _ai_sources():
    return sorted(path for path in EXAMPLES.glob("*.py") if path.name.startswith(("ai_", "agent_")))


# The rebuilt two-agent bots end with one IS_BACKTESTING switch between
# .backtest() and .run_live(); tests/test_ai_example_strategies.py checks that.
ONE_SWITCH_FILES = {
    "ai_nancy_pelosi_trading_bot.py", "ai_insider_trading_bot.py", "ai_fear_and_greed_trading_bot.py",
    "ai_iron_condor.py", "ai_credit_spread.py", "ai_0dte_options_trading_bot.py", "ai_vwap.py",
    "ai_opening_range_breakout.py", "ai_trading_team_warren_buffett_value.py",
    "ai_trading_team_bill_ackman_concentrated.py", "ai_trading_team_bull_bear_large_cap_stocks.py",
    "ai_trading_team_bull_bear_leveraged_etf.py",
}


def test_every_ai_source_states_what_direct_execution_does():
    for path in _ai_sources():
        # Four exact bytes are published as BotSpot main.py; their run-mode
        # labels live in the Sphinx pages so this docs fix cannot change them.
        # Files with the standard IS_BACKTESTING block say what they do in code.
        if (
            path.name in PUBLISHED_SOURCE_FILES
            or path.name in ONE_SWITCH_FILES
            or "from lumibot.credentials import IS_BACKTESTING" in path.read_text()
        ):
            continue
        docstring = ast.get_docstring(ast.parse(path.read_text())) or ""
        assert f"Direct run: {_direct_mode(path)}." in docstring, path.name


def test_internal_ai_guides_do_not_leave_the_runner_implicit():
    for name in ("AI_TRADING_AGENT_CANONICAL_DEMOS.md", "AI_TRADING_AGENT_COMPONENT_GUIDE.md"):
        text = (ROOT / "docs" / name).read_text()
        assert "strategy_run_modes.html" in text, name
        assert "backtest" in text and "broker runner" in text, name


def test_run_mode_guide_covers_every_ai_source_and_core_routes():
    guide = (DOCS / "strategy_run_modes.rst").read_text()
    for source in _ai_sources():
        assert source.name in guide, source.name
    for name in (
        "index.rst", "getting_started.rst", "agent_start_here.rst", "agents_examples.rst",
        "agents_quickstart.rst", "agents_canonical_demos.rst", "agents.rst",
        "backtesting.how_to_backtest.rst", "backtesting.yahoo.rst",
        "backtesting.thetadata.rst", "backtesting.polygon.rst",
        "backtesting.pandas.rst", "deployment.rst", "environment_variables.rst",
        "faq.rst",
    ):
        assert "strategy_run_modes" in (DOCS / name).read_text(), name


def test_getting_started_never_starts_a_broker_after_backtesting_in_one_snippet():
    page = (DOCS / "getting_started.rst").read_text()
    for code_block in page.split(".. code-block:: python"):
        assert not (".run_backtest(" in code_block and "trader.run_all()" in code_block)


def test_marketplace_team_pages_explain_the_environment_switch():
    guide = " ".join((DOCS / "strategy_run_modes.rst").read_text().split())
    assert "Backtest or live, chosen by the environment:" in guide
    assert "from lumibot.credentials import IS_BACKTESTING" in guide
    for name in ("citadel_sector_pods", "ray_dalio_idea_meritocracy"):
        source = (EXAMPLES / f"ai_trading_team_{name}.py").read_text()
        page = (DOCS / f"agents_example_{name}.rst").read_text()
        assert "from lumibot.credentials import IS_BACKTESTING" in source, name
        assert "``IS_BACKTESTING=true``" in page, name
    team_doc = " ".join((ROOT / "docs" / "AI_TRADING_TEAM_EXAMPLES.md").read_text().split())
    assert "four Citadel and Ray Dalio files read `IS_BACKTESTING` from the environment" in team_doc
