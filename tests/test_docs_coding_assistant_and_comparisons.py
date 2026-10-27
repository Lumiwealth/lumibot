"""Docs entry points for AI coding assistants and framework comparisons.

People who land on the LumiBot docs from a search for another framework, or
who build with Claude Code, Codex or Cursor, should find a page that answers
their question and then a clear next step: install the free LumiBot trading
skills and connect the free BotSpot MCP. These tests fail if a page, its link
from the main entry pages, or its call to action disappears.
"""

import re
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


def test_home_explains_framework_and_ai_workflow_before_mcp():
    home = _read("index")
    opening = home.split(CALLOUT)[0]
    assert "free, open-source Python framework" in opening
    assert "How to build an AI trading strategy" in opening
    assert home.index("Why build with LumiBot?") < home.index(CALLOUT)
    assert "Strategy" in opening and "research" in opening and "backtest" in opening
    assert home.count(CALLOUT) == 1
    assert home.index("strategy class can stay the same") > home.index("Run an AI strategy")


def test_no_paid_plan_required_wording():
    paths = (
        DOCSRC / "_snippets" / "botspot_mcp_connect.rst",
        DOCSRC / "_snippets" / "botspot_mcp_callout.rst",
        DOCSRC / f"{ASSISTANT_PAGE}.rst",
        ROOT / "README.md",
    )
    for path in paths:
        assert "paid plan required" not in path.read_text(encoding="utf-8").lower(), path.name
