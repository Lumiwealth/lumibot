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
MCP_URL = "https://mcp.botspot.trade/mcp"
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
    assert MCP_URL in page
    assert "claude mcp add --transport http botspot https://mcp.botspot.trade/mcp" in page
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


# One-click install buttons for the free BotSpot MCP. Formats come from each
# client's own docs; see the comment in _snippets/botspot_mcp_install_buttons.rst.
CURSOR_INSTALL = (
    "https://cursor.com/en/install-mcp?name=botspot&config="
    "eyJ1cmwiOiJodHRwczovL21jcC5ib3RzcG90LnRyYWRlL21jcCJ9"
)
VSCODE_INSTALL = (
    "https://vscode.dev/redirect/mcp/install?name=botspot&config="
    "%7B%22type%22%3A%22http%22%2C%22url%22%3A%22https%3A%2F%2Fmcp.botspot.trade%2Fmcp%22%7D"
)
CLAUDE_CONNECTORS = "https://claude.ai/customize/connectors"


def test_install_links_point_at_the_botspot_mcp():
    import base64
    import json
    from urllib.parse import parse_qs, unquote, urlparse

    cursor_cfg = json.loads(base64.b64decode(parse_qs(urlparse(CURSOR_INSTALL).query)["config"][0]))
    vscode_cfg = json.loads(unquote(VSCODE_INSTALL.split("config=", 1)[1]))
    assert cursor_cfg == {"url": MCP_URL}
    assert vscode_cfg == {"type": "http", "url": MCP_URL}


def test_install_buttons_are_in_snippet_callout_assistant_page_and_readme():
    snippet = (DOCSRC / "_snippets" / "botspot_mcp_install_buttons.rst").read_text(encoding="utf-8")
    for link in (CURSOR_INSTALL, VSCODE_INSTALL, CLAUDE_CONNECTORS):
        assert link.replace("&", "&amp;") in snippet
    assert "free to connect" in snippet.lower()

    callout = (DOCSRC / "_snippets" / "botspot_mcp_callout.rst").read_text(encoding="utf-8")
    assert "botspot_mcp_install_buttons.rst" in callout

    page = _read(ASSISTANT_PAGE)
    assert "botspot_mcp_install_buttons.rst" in page.split("Step 1:", 1)[0], "buttons must sit at the top"

    readme = (ROOT / "README.md").read_text(encoding="utf-8")
    mcp_line = next(line for line in readme.splitlines() if "Strongly recommended: connect the free" in line)
    for link in (CURSOR_INSTALL, VSCODE_INSTALL, CLAUDE_CONNECTORS):
        assert link in mcp_line, f"README MCP line is missing {link}"


def test_no_paid_plan_required_wording():
    paths = (
        DOCSRC / "_snippets" / "botspot_mcp_install_buttons.rst",
        DOCSRC / "_snippets" / "botspot_mcp_callout.rst",
        DOCSRC / f"{ASSISTANT_PAGE}.rst",
        ROOT / "README.md",
    )
    for path in paths:
        assert "paid plan required" not in path.read_text(encoding="utf-8").lower(), path.name
