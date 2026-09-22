import re
from pathlib import Path
from types import SimpleNamespace

from lumibot.components.agents.builtins import BuiltinTools

ROOT = Path(__file__).resolve().parents[1]


def test_agent_architecture_docs_recommend_two_or_more_without_making_it_a_requirement():
    text = (ROOT / "docsrc" / "agents.rst").read_text(encoding="utf-8")
    assert "two or more agents" in text.lower()
    assert "dedicated trading and risk agent" in text.lower()
    assert "recommendation, not a framework requirement" in text.lower()
    assert "deterministic Python" in text


def test_web_and_browser_tools_are_documented_with_full_capabilities():
    builtins = (ROOT / "docsrc" / "agents_builtin_tools.rst").read_text(encoding="utf-8")
    browser = (ROOT / "docsrc" / "agents_browser_tools.rst").read_text(encoding="utf-8")
    for tool in (
        "http_request",
        "rss_fetch",
        "browser_session_open",
        "browser_tabs",
        "browser_login",
        "browser_screenshot",
    ):
        assert tool in builtins or tool in browser
    for method in ("GET", "POST", "PUT", "PATCH", "DELETE"):
        assert method in builtins
    assert "lumibot[browser]" in browser
    assert "patchright install chromium" in browser
    assert "persistent" in browser.lower()
    assert "domain-scoped" in browser.lower() or "host-scoped" in browser.lower()


def test_new_flagship_examples_are_in_navigation_with_historical_data_warnings():
    examples = (ROOT / "docsrc" / "agents_examples.rst").read_text(encoding="utf-8")
    congress = (ROOT / "docsrc" / "agents_example_congress_disclosures.rst").read_text(encoding="utf-8")
    insider = (ROOT / "docsrc" / "agents_example_sec_insider_filings.rst").read_text(encoding="utf-8")
    showcase = (ROOT / "docsrc" / "agents_example_browser_research_showcase.rst").read_text(encoding="utf-8")

    assert "agents_example_congress_disclosures" in examples
    assert "agents_example_sec_insider_filings" in examples
    assert "agents_example_browser_research_showcase" in examples
    assert "ReportDate" in congress and "TransactionDate" in congress
    assert "45 days" in congress
    assert "does not ship sample trades" in congress.lower()
    assert "frozen synthetic fixture" not in congress.lower()
    assert "licensed" not in congress.lower()
    assert "Form 4" in insider and "acceptance" in insider.lower()
    assert "does not ship sample trades" in insider.lower()
    assert "frozen synthetic fixture" not in examples.lower()
    assert "publish_enabled" in showcase and "owned" in showcase.lower()


def test_old_handoff_is_explicitly_superseded_by_implemented_architecture():
    handoff = (ROOT / "docs" / "research" / "2026-09-19_agentic-tools-and-strategies-handoff.md").read_text(
        encoding="utf-8"
    )
    assert "SUPERSEDED" in handoff[:800]
    assert "http_request" in handoff[:1600]
    assert "BrowserSession" in handoff[:1600]


def test_every_agent_example_has_a_simple_workflow_image_asset():
    pages = (
        "agents_example_ai_credit_spread.rst",
        "agents_example_ai_iron_condor.rst",
        "agents_example_ai_opening_range_breakout.rst",
        "agents_example_ai_spx_zero_dte_bear_call_team.rst",
        "agents_example_ai_vwap.rst",
        "agents_example_bill_ackman_concentrated.rst",
        "agents_example_browser_research_showcase.rst",
        "agents_example_bull_bear_large_cap_stocks.rst",
        "agents_example_bull_bear_leveraged_etf.rst",
        "agents_example_citadel_sector_pods.rst",
        "agents_example_congress_disclosures.rst",
        "agents_example_ray_dalio_idea_meritocracy.rst",
        "agents_example_sec_insider_filings.rst",
        "agents_example_warren_buffett_value.rst",
    )

    for page in pages:
        page_path = ROOT / "docsrc" / page
        text = page_path.read_text(encoding="utf-8")
        match = re.search(r"^\.\. image:: (.+)$", text, re.MULTILINE)
        assert match, f"{page} is missing its workflow image"
        asset = (page_path.parent / match.group(1)).resolve()
        assert asset.is_file(), f"{page} references missing image {asset}"
        assert asset.suffix.lower() == ".png", f"{page} must use an inspected PNG asset"


def test_all_nine_sec_agent_tools_declare_point_in_time_behavior():
    sec_tool_names = {
        "get_income_statement",
        "get_balance_sheet",
        "get_cash_flow",
        "get_company_facts",
        "get_filings",
        "search_filing",
        "get_filing_document",
        "list_filing_sections",
        "get_filing_section",
    }
    definitions = {tool.name: tool for tool in BuiltinTools.all() if tool.name in sec_tool_names}

    assert set(definitions) == sec_tool_names
    strategy = SimpleNamespace(fundamentals=object())
    for definition in definitions.values():
        bound = definition.binder(strategy, None)
        assert bound.metadata["temporal"] == "published_at_as_of"


def test_every_external_data_agent_tool_declares_temporal_behavior():
    external_tool_names = {
        "market_last_price",
        "market_last_prices",
        "market_historical_prices",
        "market_load_history_table",
        "options_get_chain",
        "options_get_strikes",
        "options_get_greeks",
        "options_find_strike_for_delta",
        "options_evaluate_market",
        "options_find_expiration",
        "options_check_spread_profit",
        "alpaca_news",
        "http_request",
        "rss_fetch",
        "get_indicator",
        "get_indicators",
        "get_income_statement",
        "get_balance_sheet",
        "get_cash_flow",
        "get_company_facts",
        "get_filings",
        "search_filing",
        "get_filing_document",
        "list_filing_sections",
        "get_filing_section",
        "get_fred_series",
        "get_fred_latest",
        "get_fred_snapshot",
        "browser_navigate",
        "browser_observe",
        "browser_extract",
        "browser_storage_state",
        "browser_screenshot",
    }
    definitions = {tool.name: tool for tool in BuiltinTools.all() if tool.name in external_tool_names}
    strategy = SimpleNamespace(
        fundamentals=object(),
        macro=object(),
        broker=SimpleNamespace(name=""),
        is_backtesting=False,
    )

    assert set(definitions) == external_tool_names
    for name, definition in definitions.items():
        bound = definition.binder(strategy, None)
        assert bound.metadata.get("temporal"), f"{name} must declare its temporal behavior"
