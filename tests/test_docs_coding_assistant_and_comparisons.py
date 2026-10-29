"""Docs entry points for AI coding assistants and framework comparisons.

People who land on the LumiBot docs from a search for another framework, or
who build with Claude Code, Codex or Cursor, should find a page that answers
their question and then a clear next step: install the free LumiBot trading
skills and connect the free BotSpot MCP. These tests fail if a page, its link
from the main entry pages, or its call to action disappears.
"""

import re
import hashlib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DOCSRC = ROOT / "docsrc"

ASSISTANT_PAGE = "claude_code_codex_cursor"
SKILLS_REPO = "https://github.com/Lumiwealth/lumibot-trading-skills"
CALLOUT = ".. include:: _snippets/botspot_mcp_callout.rst"

COMPARISONS = {
    "lumibot_vs_freqtrade": "https://github.com/freqtrade/freqtrade",
    "lumibot_vs_backtrader": "https://github.com/mementum/backtrader",
    "lumibot_vs_lean": "https://github.com/QuantConnect/Lean",
    "lumibot_vs_openalice": "https://github.com/TraderAlice/OpenAlice",
    "lumibot_vs_tradingagents": "https://github.com/TauricResearch/TradingAgents",
}


def _read(name):
    return (DOCSRC / f"{name}.rst").read_text(encoding="utf-8")


def test_assistant_page_has_one_command_skill_install_and_mcp_connect():
    page = _read(ASSISTANT_PAGE)
    assert "/plugin marketplace add Lumiwealth/lumibot-trading-skills" in page
    assert "/plugin install lumibot-trading-skills@lumiwealth" in page
    assert "npx skills add Lumiwealth/lumibot-trading-skills" in page
    assert "Node 22" in page
    assert SKILLS_REPO in page
    # Setup now has one maintained destination rather than copied client instructions.
    assert "https://botspot.trade/agents" in (DOCSRC / "_snippets/botspot_mcp_connect.rst").read_text()
    assert CALLOUT in page
    assert ":description:" in page


def test_assistant_page_is_linked_from_home_getting_started_and_agents():
    for entry in ("index", "getting_started", "agents"):
        text = _read(entry)
        assert f":doc:`" in text and ASSISTANT_PAGE in text, f"{entry}.rst does not link {ASSISTANT_PAGE}"
    # Reachable from the sidebar, not only from body links.
    index = _read("index")
    toctree_entries = re.findall(r"^\s+.*<(\w+)>\s*$|^\s{3}(\w+)\s*$", index, flags=re.MULTILINE)
    flat = {a or b for a, b in toctree_entries}
    assert ASSISTANT_PAGE in flat


def test_every_comparison_page_exists_with_sources_and_cta():
    hub = _read("ai_trading_project_comparison")
    for page, repo in COMPARISONS.items():
        text = _read(page)
        assert ":description:" in text, page
        assert repo in text, f"{page} does not cite {repo}"
        assert "checked on" in text.lower(), f"{page} has no checked date"
        assert CALLOUT in text, f"{page} has no BotSpot MCP callout"
        assert ASSISTANT_PAGE in text, f"{page} does not link the coding-assistant page"
        assert SKILLS_REPO in text, f"{page} does not link the trading skills repo"
        assert re.search(rf"^\s+{page}\s*$", hub, flags=re.MULTILINE), f"hub toctree missing {page}"


def test_readme_points_to_trading_skills_once():
    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    assert readme.count("github.com/Lumiwealth/lumibot-trading-skills") == 1


def test_all_localized_entry_summaries_match_current_readme_and_homepage():
    # A change to either entry source must invalidate all old localized summaries.
    sources = (ROOT / "README.md", DOCSRC / "index.rst")
    content = b"\0".join(path.read_bytes() for path in sources)
    expected = hashlib.sha256(content).hexdigest()
    shipped = {"zh-CN", "es", "fr", "de", "ja", "ko", "pt", "ru"}
    assert {p.name.removeprefix("README.").removesuffix(".md")
            for p in ROOT.glob("README.*.md")} == shipped
    for language in shipped:
        text = (ROOT / f"README.{language}.md").read_text()
        assert f"<!-- entry-source-sha256: {expected} -->" in text, language
        for destination in (
            "https://botspot.trade/agents",
            "README.md#run-your-first-ai-backtest",
            "README.md#backtest-a-strategy",
            "README.md#build-a-hybrid-strategy",
            "https://github.com/Lumiwealth/lumibot-trading-skills",
        ):
            assert destination in text, (language, destination)
        assert "pip install lumibot" in text
        assert "lumibot demo" in text


def test_home_strategy_choices_link_to_sections_in_the_same_order():
    home = _read("index")
    labels = ("first-ai-strategy", "first-python-backtest", "first-hybrid-strategy")
    assert home.index("Native AI trading") < home.index("Three ways to build a strategy")
    assert home.index("See what your assistant gets") < home.index("Three ways to build a strategy")
    choices = home[home.index("Three ways to build a strategy"):home.index(".. _first-ai-strategy:")]
    for label in labels:
        assert f"<{label}>`" in choices
        assert home.count(f".. _{label}:") == 1
    assert [choices.index(f"<{label}>`") for label in labels] == sorted(
        choices.index(f"<{label}>`") for label in labels
    )
    assert [home.index(f".. _{label}:") for label in labels] == sorted(
        home.index(f".. _{label}:") for label in labels
    )


# October 26: replace broken client deep links and duplicate rows with one guide CTA.
def test_connect_cta_has_one_destination_and_no_client_install_badges():
    snippet = (DOCSRC / "_snippets" / "botspot_mcp_connect.rst").read_text()
    assert snippet.count('<a ') == 1
    assert 'href="https://botspot.trade/agents"' in snippet
    assert "Connect your AI assistant" in snippet
    assert "<img" not in snippet
    assert not (DOCSRC / "_snippets" / "botspot_mcp_install_buttons.rst").exists()
    for source in [*_all_doc_sources(), ROOT / "README.md"]:
        text = source.read_text()
        for obsolete in ("cursor.com/en/install-mcp", "vscode.dev/redirect/mcp/install", "claude.ai/customize/connectors"):
            assert obsolete not in text, source.name


def _all_doc_sources():
    return sorted(DOCSRC.rglob("*.rst"))


def test_callout_is_compact_and_lists_every_client_and_separate_benefits():
    text = (DOCSRC / "_snippets" / "botspot_mcp_callout.rst").read_text()
    for client in ("Claude", "ChatGPT", "Codex", "Cursor", "VS Code"):
        assert client in text
    assert "**Community:**" in text and "**Marketplace:**" in text
    assert "**Broker linking:**" in text
    assert len(text.split()) < 120
    assert "Add custom connector" not in text
    assert text.count(".. include:: /_snippets/botspot_mcp_connect.rst") == 1
    page = _read(ASSISTANT_PAGE)
    assert page.count(CALLOUT) == 1
    assert page.index(CALLOUT) > page.index("Step 2:")
    assert "botspot_mcp_connect.rst" not in page


def test_home_and_readme_distinguish_strategy_types_from_coding_assistants():
    # October 26 correction: show strategy code; connection setup is its own section.
    home = _read("index")
    readme = (ROOT / "README.md").read_text()
    for text in (home, readme):
        for kind in ("Python strategies", "AI-native strategies", "Hybrid strategies"):
            assert kind in text
        for stale in ("AI is optional", "Choose your market", "Keep your strategy", "Own your code"):
            assert stale not in text
        assert "Backtest before trading" in text
        assert "Same strategy for backtesting and trading" in text
        assert "Multiple asset classes" in text
    assert "How to build an AI trading strategy" not in home
    assert home.index("Build an AI-native strategy") < home.index("Connect your AI assistant")
    assert home.count("Connect your AI assistant\n***") == 1
    assert home.index(CALLOUT) > home.index("Connect your AI assistant\n***")
    assert "or another MCP-compatible" in _read("_snippets/botspot_mcp_callout")
    assert "system_prompt=" in readme
    assert "literalinclude:: _snippets/ai_strategy.py" in home
    assert home.count(".. _first-python-backtest:") == 1
    assert home.count("Why build with LumiBot?") == 1
    assert "Example: Research, Bull, Bear, and Trader Agents" not in home



def test_no_paid_plan_required_wording():
    paths = (
        DOCSRC / "_snippets" / "botspot_mcp_connect.rst",
        DOCSRC / "_snippets" / "botspot_mcp_callout.rst",
        DOCSRC / f"{ASSISTANT_PAGE}.rst",
        ROOT / "README.md",
    )
    for path in paths:
        assert "paid plan required" not in path.read_text(encoding="utf-8").lower(), path.name


def test_editable_ai_example_runs_the_documented_lifecycle_and_matches_readme():
    # Exercise the published snippet's plumbing without a model call or broker action.
    # Recorder shape follows test_growth_entrypoints.test_two_roles_preserve_evidence_and_trading_ownership.
    from datetime import datetime
    from types import SimpleNamespace
    source = (DOCSRC / "_snippets/ai_strategy.py").read_text()
    namespace = {"__name__": "documentation_example"}
    exec(compile(source, "ai_strategy.py", "exec"), namespace)
    created, calls = [], []

    class Agents(dict):
        def create(self, **kwargs):
            created.append(kwargs)
            self[kwargs["name"]] = SimpleNamespace(run=lambda **args: calls.append(args))

    ctx = SimpleNamespace(agents=Agents(), get_datetime=lambda: datetime(2026, 4, 6))
    strategy = namespace["MyAIStrategy"]
    strategy.initialize(ctx)
    strategy.on_trading_iteration(ctx)
    strategy.on_trading_iteration(ctx)
    assert len(created) == 1
    assert created[0]["name"] == "trader" and created[0]["allow_trading"] is True
    assert created[0]["system_prompt"]
    assert len(calls) == 2
    assert all(c["context"] == {"as_of": "2026-04-06T00:00:00"} for c in calls)
    assert source.strip() in (ROOT / "README.md").read_text()
