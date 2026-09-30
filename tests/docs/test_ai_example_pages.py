"""Every AI example page follows the same simple, searchable layout.

Rob's page contract (2026-09-29):
1. A title people actually search for (no "Form 4", "disclosure", "experiment").
2. The workflow image right under the title.
3. A plain-English description, then "How it works" with numbered steps that
   say what each agent does. Jargon such as "Run mode" or "Availability" never
   comes first.
4. A "Run it on BotSpot" section.
5. A "Backtest tear sheet" section with the real tear sheet, before the code.
6. "The code" section with the full example file.
"""

import re
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
DOCS = ROOT / "docsrc"

PAGES = {
    "agents_example_nancy_pelosi_trading_bot": ("Nancy Pelosi Stock Trading Bot", "ai_nancy_pelosi_trading_bot.py"),
    "agents_example_nancy_pelosi_copy_trading_bot": (
        "Nancy Pelosi Copy Trading Bot", "ai_nancy_pelosi_copy_trading_bot.py"
    ),
    "agents_example_insider_trading_bot": ("Insider Trading Bot", "ai_insider_trading_bot.py"),
    "agents_example_fear_and_greed_index_trading_bot": (
        "Fear and Greed Index Trading Bot", "ai_fear_and_greed_trading_bot.py"
    ),
    "agents_example_iron_condor_ai_trading_bot": ("AI Iron Condor Trading Bot", "ai_iron_condor.py"),
    "agents_example_put_credit_spread_ai_trading_bot": ("Put Credit Spread AI Trading Bot", "ai_credit_spread.py"),
    "agents_example_0dte_options_ai_trading_bot": ("0DTE Options AI Trading Bot", "ai_0dte_options_trading_bot.py"),
    "agents_example_vwap_strategy_ai_trading_bot": ("VWAP Strategy AI Trading Bot", "ai_vwap.py"),
    "agents_example_opening_range_breakout_ai_trading_bot": (
        "Opening Range Breakout AI Trading Bot", "ai_opening_range_breakout.py"
    ),
    "agents_example_warren_buffett_ai_stock_picker": (
        "Warren Buffett AI Stock Picker", "ai_trading_team_warren_buffett_value.py"
    ),
    "agents_example_bill_ackman_portfolio_ai_trading_bot": (
        "Bill Ackman Portfolio AI Trading Bot", "ai_trading_team_bill_ackman_concentrated.py"
    ),
    "agents_example_bull_vs_bear_ai_stock_trading_bot": (
        "Bull vs Bear AI Stock Trading Bot", "ai_trading_team_bull_bear_large_cap_stocks.py"
    ),
    "agents_example_tqqq_strategy_ai_trading_bot": (
        "TQQQ Strategy AI Trading Bot", "ai_trading_team_bull_bear_leveraged_etf.py"
    ),
    "agents_example_citadel_sector_pods": (
        "Citadel Sector Pods AI Trading Team", "ai_trading_team_citadel_sector_pods.py"
    ),
    "agents_example_ray_dalio_idea_meritocracy": (
        "Ray Dalio Idea Meritocracy AI Trading Team", "ai_trading_team_ray_dalio_idea_meritocracy.py"
    ),
}
OLD_SLUGS = {
    "agents_example_congress_disclosures": "agents_example_nancy_pelosi_trading_bot",
    "agents_example_sec_insider_filings": "agents_example_insider_trading_bot",
    "agents_example_browser_research_showcase": "agents_example_fear_and_greed_index_trading_bot",
    "agents_example_ai_iron_condor": "agents_example_iron_condor_ai_trading_bot",
    "agents_example_ai_credit_spread": "agents_example_put_credit_spread_ai_trading_bot",
    "agents_example_ai_spx_zero_dte_bear_call_team": "agents_example_0dte_options_ai_trading_bot",
    "agents_example_ai_vwap": "agents_example_vwap_strategy_ai_trading_bot",
    "agents_example_ai_opening_range_breakout": "agents_example_opening_range_breakout_ai_trading_bot",
    "agents_example_warren_buffett_value": "agents_example_warren_buffett_ai_stock_picker",
    "agents_example_bill_ackman_concentrated": "agents_example_bill_ackman_portfolio_ai_trading_bot",
    "agents_example_bull_bear_large_cap_stocks": "agents_example_bull_vs_bear_ai_stock_trading_bot",
    "agents_example_bull_bear_leveraged_etf": "agents_example_tqqq_strategy_ai_trading_bot",
}
# Tear sheet still being produced, or page owned by a dedicated agent (2026-09-29).
TEAR_SHEET_PENDING = {
    "agents_example_0dte_options_ai_trading_bot",
    "agents_example_nancy_pelosi_trading_bot",
    "agents_example_nancy_pelosi_copy_trading_bot",
    "agents_example_iron_condor_ai_trading_bot",
}
JARGON = ("form 4", "disclosure agent", "congressional disclosure", "experiment", "two-agent", "showcase",
          "authenticated", "interpreter", "agent_cycle", "run_cycle")


def _sections(text: str) -> list[str]:
    lines = text.splitlines()
    return [lines[i - 1] for i in range(1, len(lines)) if re.fullmatch(r"-{3,}", lines[i]) and lines[i - 1].strip()]


def test_every_example_page_is_listed():
    assert {path.stem for path in DOCS.glob("agents_example_*.rst")} == set(PAGES)


@pytest.mark.parametrize("slug", sorted(PAGES))
def test_page_follows_the_contract(slug):
    title, source = PAGES[slug]
    text = (DOCS / f"{slug}.rst").read_text()
    lines = text.splitlines()
    assert lines[0] == title and set(lines[1]) == {"="}

    head = text.split("\nHow it works\n", 1)[0]
    assert ".. image:: ../docs/assets/" in head
    image = head.split(".. image:: ", 1)[1].split("\n", 1)[0]
    assert (DOCS / image).resolve().is_file(), image
    description = head.split(":width: 100%", 1)[1].strip()
    assert len(description.split()) >= 25, "needs a plain-English description before How it works"

    sections = _sections(text)
    assert sections[0] == "How it works", sections
    assert "Run it on BotSpot" in sections
    assert "The code" in sections
    assert sections.index("Run it on BotSpot") < sections.index("The code")

    how = text.split("\nHow it works\n", 1)[1].split("\n\n", 1)[1]
    how = how.split("\nRun it on BotSpot\n", 1)[0]
    assert re.search(r"^1\. ", how, re.M) and re.search(r"^2\. ", how, re.M)
    assert "agent" in how.lower()

    if slug not in TEAR_SHEET_PENDING:
        assert "Backtest tear sheet" in sections
        assert sections.index("Backtest tear sheet") < sections.index("The code")
        sheet = text.split("\nBacktest tear sheet\n", 1)[1].split("\nThe code\n", 1)[0]
        target = re.search(r":target: tearsheets/(.+\.html)", sheet).group(1)
        assert (DOCS / "_extra" / "tearsheets" / target).is_file(), target
        shot = re.search(r"\.\. image:: (\S+)", sheet).group(1)
        assert (DOCS / shot).resolve().is_file(), shot

    code = text.split("\nThe code\n", 1)[1]
    assert f".. literalinclude:: ../lumibot/example_strategies/{source}" in code

    visible = (title + "\n" + head).lower()
    for word in JARGON:
        assert word not in visible, (slug, word)
    assert "Run mode" not in head and "Availability" not in head


@pytest.mark.parametrize("old,new", sorted(OLD_SLUGS.items()))
def test_old_urls_redirect_to_the_new_pages(old, new):
    assert not (DOCS / f"{old}.rst").exists()
    stub = (DOCS / "_extra" / f"{old}.html").read_text()
    assert f'url={new}.html"' in stub
    assert f'rel="canonical" href="https://lumibot.lumiwealth.com/{new}.html"' in stub
    assert (DOCS / f"{new}.rst").is_file()
