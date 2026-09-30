from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from lumibot.components.disclosure_signals import (
    parse_form4_xml,
    visible_congress_disclosures,
    visible_insider_transactions,
)


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
