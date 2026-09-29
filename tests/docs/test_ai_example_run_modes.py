"""Keep AI example entry points and their public run-mode guidance in sync."""

import ast
from pathlib import Path
import textwrap


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
    return sorted(
        path
        for path in EXAMPLES.glob("*.py")
        if path.name.startswith(("ai_", "agent_"))
        and path.name not in {"agent_cycle.py", "ai_trading_team.py"}
    )


def test_every_ai_source_states_what_direct_execution_does():
    for path in _ai_sources():
        # Four exact bytes are published as BotSpot main.py; their run-mode
        # labels live in the Sphinx pages so this docs fix cannot change them.
        if path.name in PUBLISHED_SOURCE_FILES:
            continue
        docstring = ast.get_docstring(ast.parse(path.read_text())) or ""
        assert f"Direct run: {_direct_mode(path)}." in docstring, path.name


def test_every_public_ai_example_page_states_its_source_mode():
    for page in sorted(DOCS.glob("agents_example_*.rst")):
        text = page.read_text()
        source_lines = [line for line in text.splitlines() if line.startswith(".. literalinclude:: ../lumibot/example_strategies/")]
        assert source_lines, page.name
        sources = [(DOCS / line.split(":: ", 1)[1]).resolve() for line in source_lines]
        assert all(source.is_file() for source in sources), page.name
        modes = {_direct_mode(source) for source in sources}
        assert len(modes) == 1, page.name
        assert "Run mode\n" in text, page.name
        assert f"**Direct file execution:** {modes.pop()}." in text, page.name
        assert ":doc:`strategy_run_modes`" in text, page.name


def test_team_pages_explain_their_actual_mode_selector():
    for name in (
        "citadel_sector_pods", "ray_dalio_idea_meritocracy",
        "bill_ackman_concentrated", "bull_bear_large_cap_stocks",
        "bull_bear_leveraged_etf", "warren_buffett_value",
    ):
        source = (EXAMPLES / f"ai_trading_team_{name}.py").read_text()
        page = (DOCS / f"agents_example_{name}.rst").read_text()
        if "from lumibot.credentials import IS_BACKTESTING" in source:
            assert "reads ``IS_BACKTESTING`` from the environment" in page, name
            assert "export IS_BACKTESTING=true" in page, name
        else:
            assert "runner-local ``IS_BACKTESTING`` assignment" in page, name

    guide = " ".join((DOCS / "strategy_run_modes.rst").read_text().split())
    assert "Four team files import ``IS_BACKTESTING`` from ``lumibot.credentials``" in guide
    assert "Four other team files assign a local ``IS_BACKTESTING``" in guide
    team_doc = " ".join((ROOT / "docs" / "AI_TRADING_TEAM_EXAMPLES.md").read_text().split())
    assert "four Citadel and Ray Dalio files read `IS_BACKTESTING` from the environment" in team_doc


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


def test_orb_page_has_a_separate_explicit_paper_broker_runner():
    page = (DOCS / "agents_example_ai_opening_range_breakout.rst").read_text()
    section = page.split("Start the same class with an Alpaca paper broker", 1)[1]
    snippet = section.split(".. code-block:: python", 1)[1].split("\n\nSet ``OPENAI_API_KEY``", 1)[0]
    source = textwrap.dedent(snippet)
    ast.parse(source)
    assert '"PAPER": True' in source
    assert "AIOpeningRangeBreakoutStrategy(" in source
    assert "_parameters_from_env(AIOpeningRangeBreakoutStrategy.parameters)" in source
    assert "strategy.run_live()" in source
    assert ".backtest(" not in source


def test_orb_page_names_all_five_source_agents():
    source = (EXAMPLES / "ai_opening_range_breakout.py").read_text()
    page = (DOCS / "agents_example_ai_opening_range_breakout.rst").read_text()
    tree = ast.parse(source)
    strategy = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == "AIOpeningRangeBreakoutStrategy")
    initialize = next(node for node in strategy.body if isinstance(node, ast.FunctionDef) and node.name == "initialize")
    agents = [node for node in ast.walk(initialize) if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "add_agent"]
    assert len(agents) == 5
    assert "five-agent equity strategy" in page
    assert "bull, bear, and interpreter" in page
