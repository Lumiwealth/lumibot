from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from lumibot.components.disclosure_signals import (
    parse_form4_xml,
    visible_congress_disclosures,
    visible_insider_transactions,
)
from lumibot.example_strategies.ai_congress_disclosures import AICongressDisclosuresStrategy
from lumibot.example_strategies.ai_sec_insider_filings import AISECInsiderFilingsStrategy


def test_congress_disclosures_are_visible_on_report_date_not_transaction_date():
    records = [
        {
            "Politician": "Clock Test Member",
            "Ticker": "NVDA",
            "Transaction": "Purchase",
            "TransactionDate": "2026-01-05",
            "ReportDate": "2026-02-14",
            "Amount": "$100,001 - $250,000",
            "fetched_at": "2026-02-14T00:05:00+00:00",
        }
    ]

    assert visible_congress_disclosures(records, as_of="2026-02-13T23:59:59+00:00") == []
    visible = visible_congress_disclosures(records, as_of="2026-02-14T00:00:00+00:00")
    assert visible[0]["ticker"] == "NVDA"
    assert visible[0]["transaction_date"] == "2026-01-05"
    assert visible[0]["published_at"].startswith("2026-02-14")
    assert visible[0]["amount_min"] == 100001
    assert visible[0]["amount_max"] == 250000
    assert visible[0]["fetched_at"] == "2026-02-14T00:05:00+00:00"
    assert visible[0]["source"] == "congress_disclosure"


def test_congress_amendment_supersedes_earlier_duplicate_and_missing_ticker_is_rejected():
    records = [
        {
            "id": "disclosure-1",
            "Politician": "Example Member",
            "Ticker": "NVDA",
            "Transaction": "Purchase",
            "TransactionDate": "2026-08-01",
            "ReportDate": "2026-09-10",
            "Amount": "$1,001 - $15,000",
        },
        {
            "id": "disclosure-1",
            "Politician": "Example Member",
            "Ticker": "NVDA",
            "Transaction": "Purchase",
            "TransactionDate": "2026-08-01",
            "ReportDate": "2026-09-12",
            "Amount": "$15,001 - $50,000",
            "Amendment": True,
        },
        {
            "id": "missing-ticker",
            "Politician": "Example Member",
            "Ticker": "N/A",
            "Transaction": "Purchase",
            "TransactionDate": "2026-08-01",
            "ReportDate": "2026-09-12",
        },
    ]

    visible = visible_congress_disclosures(records, as_of="2026-09-20T00:00:00+00:00")

    assert len(visible) == 1
    assert visible[0]["id"] == "disclosure-1"
    assert visible[0]["amendment"] is True
    assert visible[0]["amount_min"] == 15001


def test_quiver_component_loads_without_credentials_and_filters_by_report_date(monkeypatch, tmp_path):
    monkeypatch.delenv("QUIVER_API_KEY", raising=False)
    from lumibot.components.quiver_helper import QuiverHelper

    helper = QuiverHelper(SimpleNamespace(log_message=lambda *args, **kwargs: None), cache_path=tmp_path / "quiver.csv")
    records = [
        {
            "Ticker": "NVDA",
            "Transaction": "Purchase",
            "TransactionDate": "2026-01-05",
            "ReportDate": "2026-02-14",
            "Amount": "1000",
        }
    ]

    assert helper.filter_disclosures_as_of(records, datetime(2026, 2, 13).date()) == []
    assert helper.filter_disclosures_as_of(records, datetime(2026, 2, 14).date()) == records
    with pytest.raises(ValueError, match="QUIVER_API_KEY"):
        helper.fetch_congress_trading_data("P000197")


def test_form4_parser_distinguishes_open_market_derivative_and_amendment_rows():
    xml = """<ownershipDocument>
      <documentType>4/A</documentType><periodOfReport>2026-09-18</periodOfReport>
      <issuer><issuerCik>0000320193</issuerCik><issuerTradingSymbol>AAPL</issuerTradingSymbol></issuer>
      <reportingOwner><reportingOwnerId><rptOwnerCik>0001</rptOwnerCik>
        <rptOwnerName>Jane Doe</rptOwnerName></reportingOwnerId></reportingOwner>
      <nonDerivativeTable><nonDerivativeTransaction>
        <securityTitle><value>Common Stock</value></securityTitle>
        <transactionDate><value>2026-09-17</value></transactionDate>
        <transactionCoding><transactionCode>P</transactionCode></transactionCoding>
        <transactionAmounts><transactionShares><value>100</value></transactionShares><transactionPricePerShare><value>200</value></transactionPricePerShare><transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode></transactionAmounts>
        <ownershipNature><directOrIndirectOwnership><value>D</value></directOrIndirectOwnership></ownershipNature>
      </nonDerivativeTransaction></nonDerivativeTable>
      <derivativeTable><derivativeTransaction>
        <securityTitle><value>Option</value></securityTitle>
        <transactionDate><value>2026-09-17</value></transactionDate>
        <transactionCoding><transactionCode>A</transactionCode></transactionCoding>
        <transactionAmounts><transactionShares><value>50</value></transactionShares><transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode></transactionAmounts>
        <ownershipNature><directOrIndirectOwnership><value>I</value></directOrIndirectOwnership></ownershipNature>
      </derivativeTransaction></derivativeTable>
    </ownershipDocument>"""

    rows = parse_form4_xml(
        xml,
        accession_number="0000320193-26-000001",
        acceptance_datetime="2026-09-18T12:30:00+00:00",
        fetched_at="2026-09-18T12:31:00+00:00",
    )

    assert len(rows) == 2
    assert rows[0]["transaction_code"] == "P"
    assert rows[0]["open_market"] is True
    assert rows[0]["derivative"] is False
    assert rows[0]["amendment"] is True
    assert rows[1]["transaction_code"] == "A"
    assert rows[1]["open_market"] is False
    assert rows[1]["derivative"] is True
    assert rows[1]["ownership"] == "indirect"
    assert rows[0]["source"] == "sec_edgar_form4"
    assert rows[0]["fetched_at"] == "2026-09-18T12:31:00+00:00"
    assert visible_insider_transactions(rows, as_of="2026-09-18T12:29:59+00:00") == []
    assert len(visible_insider_transactions(rows, as_of="2026-09-18T12:30:00+00:00")) == 2


def test_form4_parser_classifies_sale_gift_exercise_plan_ownership_and_derivatives():
    xml = """<ownershipDocument>
      <documentType>4</documentType><periodOfReport>2026-09-18</periodOfReport><aff10b5One>true</aff10b5One>
      <issuer><issuerCik>0000320193</issuerCik><issuerTradingSymbol>AAPL</issuerTradingSymbol></issuer>
      <reportingOwner>
        <reportingOwnerId>
          <rptOwnerCik>0001</rptOwnerCik>
          <rptOwnerName>Jane Doe</rptOwnerName>
        </reportingOwnerId>
      </reportingOwner>
      <nonDerivativeTable>
        <nonDerivativeTransaction>
          <securityTitle><value>Common Stock</value></securityTitle>
          <transactionDate><value>2026-09-17</value></transactionDate>
          <transactionCoding><transactionCode>S</transactionCode></transactionCoding>
          <transactionAmounts>
            <transactionShares><value>10</value></transactionShares>
            <transactionPricePerShare><value>200</value></transactionPricePerShare>
            <transactionAcquiredDisposedCode><value>D</value></transactionAcquiredDisposedCode>
          </transactionAmounts>
          <ownershipNature><directOrIndirectOwnership><value>D</value></directOrIndirectOwnership></ownershipNature>
        </nonDerivativeTransaction>
        <nonDerivativeTransaction>
          <securityTitle><value>Common Stock</value></securityTitle>
          <transactionDate><value>2026-09-17</value></transactionDate>
          <transactionCoding><transactionCode>G</transactionCode></transactionCoding>
          <transactionAmounts>
            <transactionShares><value>5</value></transactionShares>
            <transactionAcquiredDisposedCode><value>D</value></transactionAcquiredDisposedCode>
          </transactionAmounts>
          <ownershipNature><directOrIndirectOwnership><value>I</value></directOrIndirectOwnership></ownershipNature>
        </nonDerivativeTransaction>
      </nonDerivativeTable>
      <derivativeTable><derivativeTransaction><securityTitle><value>Option</value></securityTitle><transactionDate><value>2026-09-17</value></transactionDate><transactionCoding><transactionCode>M</transactionCode></transactionCoding><transactionAmounts><transactionShares><value>25</value></transactionShares><transactionAcquiredDisposedCode><value>A</value></transactionAcquiredDisposedCode></transactionAmounts><ownershipNature><directOrIndirectOwnership><value>D</value></directOrIndirectOwnership></ownershipNature></derivativeTransaction></derivativeTable>
    </ownershipDocument>"""

    rows = parse_form4_xml(
        xml,
        accession_number="0000320193-26-000002",
        acceptance_datetime="2026-09-18T12:30:00+00:00",
    )

    assert [(row["transaction_code"], row["transaction_kind"]) for row in rows] == [
        ("S", "open_market_sale"),
        ("G", "gift"),
        ("M", "option_exercise"),
    ]
    assert [row["ownership"] for row in rows] == ["direct", "indirect", "direct"]
    assert [row["derivative"] for row in rows] == [False, False, True]
    assert all(row["automatic_plan"] is True for row in rows)


def test_form4_amendment_supersedes_the_same_transaction_without_hiding_distinct_rows():
    base = {
        "ticker": "AAPL",
        "owner_cik": "0001",
        "transaction_code": "P",
        "transaction_date": "2026-09-17",
        "security_title": "Common Stock",
        "shares": 100,
        "price_per_share": 200,
        "derivative": False,
        "transaction_key": "same-economic-transaction",
        "source": "sec_edgar_form4",
        "fetched_at": "2026-09-18T12:31:00+00:00",
    }
    records = [
        {
            **base,
            "id": "original",
            "accession_number": "original",
            "published_at": "2026-09-18T12:30:00+00:00",
            "amendment": False,
        },
        {
            **base,
            "id": "amended",
            "accession_number": "amended",
            "published_at": "2026-09-19T12:30:00+00:00",
            "amendment": True,
            "shares": 125,
        },
        {
            **base,
            "id": "distinct",
            "transaction_key": "different-transaction",
            "published_at": "2026-09-19T12:31:00+00:00",
            "amendment": False,
            "transaction_code": "S",
        },
    ]

    visible = visible_insider_transactions(records, as_of="2026-09-20T00:00:00+00:00")

    assert [row["id"] for row in visible] == ["amended", "distinct"]
    assert visible[0]["shares"] == 125


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


def _exercise_strategy(strategy_class):
    agents = _Agents()
    context = SimpleNamespace(
        agents=agents,
        parameters=dict(strategy_class.parameters),
        get_datetime=lambda: datetime(2026, 9, 20, tzinfo=timezone.utc),
        log_message=lambda *args, **kwargs: None,
    )
    strategy_class.initialize(context)
    strategy_class.on_trading_iteration(context)
    return agents


def test_congress_strategy_has_researcher_and_dedicated_trading_risk_agent():
    agents = _exercise_strategy(AICongressDisclosuresStrategy)

    assert [(item["name"], item["allow_trading"]) for item in agents.created] == [
        ("congress_researcher", False),
        ("bull", False),
        ("bear", False),
        ("interpreter", False),
        ("trading_risk_manager", True),
    ]
    assert [name for name, _ in agents.calls] == [
        "congress_researcher",
        "bull",
        "bear",
        "interpreter",
        "trading_risk_manager",
    ]
    trader_context = agents.calls[-1][1]["context"]
    assert trader_context["research_evidence"] == "congress_researcher evidence"
    assert "disclosures" not in trader_context
    assert "report date" in trader_context["clock_rule"]
    trader = next(item for item in agents.created if item["name"] == "trading_risk_manager")
    assert "account value" in trader["system_prompt"]
    assert "0% to 5%" in trader["system_prompt"]
    assert trader_context["risk_policy"] == {
        "cash_target": "0% to 5%",
        "sizing": "scale visible filing range midpoints to account value",
        "never_short": True,
    }


def test_congress_research_line_carries_the_asset_code_the_trader_checks():
    agents = _exercise_strategy(AICongressDisclosuresStrategy)
    prompts = {item["name"]: " ".join(item["system_prompt"].split()) for item in agents.created}
    assert "TICKER [CODE] buy_dollars sell_dollars net_dollars" in prompts["congress_researcher"]
    assert "research lines already exclude [OP]" in prompts["trading_risk_manager"]


def test_insider_strategy_has_researcher_and_dedicated_trading_risk_agent():
    agents = _exercise_strategy(AISECInsiderFilingsStrategy)

    assert [(item["name"], item["allow_trading"]) for item in agents.created] == [
        ("insider_trade_researcher", False),
        ("bull", False),
        ("bear", False),
        ("interpreter", False),
        ("trading_risk_manager", True),
    ]
    assert [name for name, _ in agents.calls] == [
        "insider_trade_researcher",
        "bull",
        "bear",
        "interpreter",
        "trading_risk_manager",
    ]
    trader = next(item for item in agents.created if item["name"] == "trading_risk_manager")
    assert "amendments" in agents.created[0]["system_prompt"]
    assert "open-market" in trader["system_prompt"]
    assert agents.calls[-1][1]["context"]["research_evidence"] == "insider_trade_researcher evidence"
    assert "after as_of" in agents.calls[0][1]["context"]["clock_rule"]


def test_insider_strategy_reads_point_in_time_form4_filings_for_a_watchlist():
    agents = _exercise_strategy(AISECInsiderFilingsStrategy)

    research_prompt = agents.created[0]["system_prompt"]
    research_context = agents.calls[0][1]["context"]
    trader_context = agents.calls[-1][1]["context"]
    # The live getcurrent feed shows today's filings, so a backtest would see the future.
    assert "feed_url" not in AISECInsiderFilingsStrategy.parameters
    assert "getcurrent" not in repr(research_context)
    assert "get_filings" in research_prompt and "form='4'" in research_prompt
    assert "get_filing_document" in research_prompt
    assert research_context["watchlist"] == list(AISECInsiderFilingsStrategy.parameters["watchlist"])
    assert len(research_context["watchlist"]) >= 8
    assert research_context["lookback_days"] == AISECInsiderFilingsStrategy.parameters["lookback_days"]
    assert trader_context["watchlist"] == research_context["watchlist"]
    trader = next(item for item in agents.created if item["name"] == "trading_risk_manager")
    assert "equal weight" in trader["system_prompt"]


def test_public_page_strategy_follows_published_purchases_sized_from_our_account():
    """public-fetch-luna-v2 (2026-01-26) read a published House report listing AB, GOOGL, AMZN,
    and NVDA purchases, then refused because the page did not name a size for our account."""
    from lumibot.example_strategies.ai_public_web_fetch import AIPublicWebFetchStrategy

    agents = _exercise_strategy(AIPublicWebFetchStrategy)
    prompts = {item["name"]: item["system_prompt"] for item in agents.created}
    trader = prompts["trading_risk_manager"]

    assert "with a ticker and a size" not in trader
    assert "does not need to state a size for this account" in trader
    assert "purchase" in trader and "weight" in trader
    assert "account value" in trader
    assert "risk_calculate_stock_quantity" in trader
    assert "published" in prompts["interpreter"] and "purchase" in prompts["interpreter"]
    assert "Argue why the page is not a trade" not in prompts["bear"]
    assert "not published" in trader or "published after" in trader


_ONE_PATH_FILES = (
    "ai_congress_disclosures.py",
    "ai_sec_insider_filings.py",
    "ai_public_web_fetch.py",
    "ai_vwap.py",
    "ai_opening_range_breakout.py",
    "ai_credit_spread.py",
    "ai_iron_condor.py",
    "ai_spx_zero_dte_bear_call_team.py",
    "ai_trading_team_bull_bear_large_cap_stocks.py",
    "ai_trading_team_bull_bear_leveraged_etf.py",
    "ai_trading_team_warren_buffett_value.py",
    "ai_trading_team_bill_ackman_concentrated.py",
)
_BANNED_SNIPPETS = (
    "create_order(",
    "submit_order(",
    "execution_mode",
    "proof_modes",
    "filing_rule",
    "price_rule",
    "minute_proof",
    "multileg_proof",
    "source_proof",
    "WebClient",
    "download_house_pdf",
    "pdf_bytes_to_text",
)


def test_example_strategies_only_run_the_agent_cycle():
    from pathlib import Path

    folder = Path(__file__).resolve().parents[1] / "lumibot" / "example_strategies"
    for name in _ONE_PATH_FILES:
        source = (folder / name).read_text(encoding="utf-8")
        assert "def initialize" in source
        assert "def on_trading_iteration" in source
        assert "run_cycle(" in source
        for snippet in _BANNED_SNIPPETS:
            assert snippet not in source, f"{name} still contains {snippet}"
