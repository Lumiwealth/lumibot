from datetime import datetime, timezone

from lumibot.fundamentals import SECFundamentals


class _Response:
    def __init__(self, *, payload=None, text=""):
        self._payload = payload
        self.text = text
        self.content = text.encode()

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


def test_sec_fundamentals_are_point_in_time_gated_and_cached(monkeypatch, tmp_path):
    calls = []

    def fake_get(url, **kwargs):
        calls.append(url)
        if url.endswith("company_tickers.json"):
            return _Response(payload={"0": {"ticker": "AAPL", "cik_str": 320193, "title": "Apple Inc."}})
        if "companyfacts" in url:
            return _Response(
                payload={
                    "facts": {
                        "us-gaap": {
                            "Revenues": {
                                "units": {
                                    "USD": [
                                        {"val": 100, "filed": "2024-01-01", "form": "10-K", "fy": 2023, "fp": "FY", "accn": "old"},
                                        {"val": 200, "filed": "2026-01-01", "form": "10-K", "fy": 2025, "fp": "FY", "accn": "future"},
                                    ]
                                }
                            },
                            "NetIncomeLoss": {
                                "units": {
                                    "USD": [
                                        {"val": 10, "filed": "2024-01-01", "form": "10-K", "fy": 2023, "fp": "FY", "accn": "old"}
                                    ]
                                }
                            },
                        }
                    }
                }
            )
        raise AssertionError(url)

    monkeypatch.setattr("lumibot.fundamentals.sec.requests.get", fake_get)
    sec = SECFundamentals(cache_dir=tmp_path, min_request_interval_seconds=0)

    result = sec.get_income_statement("AAPL", as_of=datetime(2025, 1, 1, tzinfo=timezone.utc))
    assert result["values"]["revenue"]["value"] == 100
    assert result["values"]["net_income"]["value"] == 10

    result_again = sec.get_income_statement("AAPL", as_of=datetime(2025, 1, 1, tzinfo=timezone.utc))
    assert result_again["values"]["revenue"]["value"] == 100
    assert len(calls) == 2


def test_income_statement_uses_freshest_fact_across_mapped_tags(monkeypatch, tmp_path):
    def fake_get(url, **kwargs):
        if url.endswith("company_tickers.json"):
            return _Response(payload={"0": {"ticker": "NVDA", "cik_str": 1045810, "title": "NVIDIA Corp."}})
        if "companyfacts" in url:
            return _Response(
                payload={
                    "facts": {
                        "us-gaap": {
                            "RevenueFromContractWithCustomerExcludingAssessedTax": {
                                "units": {
                                    "USD": [
                                        {
                                            "val": 26914000000,
                                            "filed": "2022-03-18",
                                            "form": "10-K",
                                            "fy": 2022,
                                            "fp": "FY",
                                            "frame": "CY2021",
                                            "start": "2021-02-01",
                                            "end": "2022-01-30",
                                            "accn": "old-revenue-tag",
                                        }
                                    ]
                                }
                            },
                            "Revenues": {
                                "units": {
                                    "USD": [
                                        {
                                            "val": 81615000000,
                                            "filed": "2026-05-20",
                                            "form": "10-Q",
                                            "fy": 2027,
                                            "fp": "Q1",
                                            "frame": "CY2026Q1",
                                            "start": "2026-01-26",
                                            "end": "2026-04-26",
                                            "accn": "fresh-revenue-tag",
                                        }
                                    ]
                                }
                            },
                        }
                    }
                }
            )
        raise AssertionError(url)

    monkeypatch.setattr("lumibot.fundamentals.sec.requests.get", fake_get)
    sec = SECFundamentals(cache_dir=tmp_path, min_request_interval_seconds=0)

    result = sec.get_income_statement("NVDA", as_of=datetime(2026, 5, 22, tzinfo=timezone.utc))

    assert result["values"]["revenue"]["tag"] == "Revenues"
    assert result["values"]["revenue"]["value"] == 81615000000
    assert result["values"]["revenue"]["accession_number"] == "fresh-revenue-tag"


def test_income_statement_does_not_mix_old_fact_into_new_statement(monkeypatch, tmp_path):
    def fake_get(url, **kwargs):
        if url.endswith("company_tickers.json"):
            return _Response(payload={"0": {"ticker": "COST", "cik_str": 909832, "title": "Costco Wholesale Corp."}})
        if "companyfacts" in url:
            return _Response(
                payload={
                    "facts": {
                        "us-gaap": {
                            "Revenues": {
                                "units": {
                                    "USD": [
                                        {
                                            "val": 63720000000,
                                            "filed": "2026-03-12",
                                            "form": "10-Q",
                                            "fy": 2026,
                                            "fp": "Q2",
                                            "start": "2025-09-01",
                                            "end": "2026-02-15",
                                            "accn": "fresh-10q",
                                        }
                                    ]
                                }
                            },
                            "NetIncomeLoss": {
                                "units": {
                                    "USD": [
                                        {
                                            "val": 1788000000,
                                            "filed": "2026-03-12",
                                            "form": "10-Q",
                                            "fy": 2026,
                                            "fp": "Q2",
                                            "start": "2025-09-01",
                                            "end": "2026-02-15",
                                            "accn": "fresh-10q",
                                        }
                                    ]
                                }
                            },
                            "GrossProfit": {
                                "units": {
                                    "USD": [
                                        {
                                            "val": 17400000000,
                                            "filed": "2019-10-11",
                                            "form": "10-K",
                                            "fy": 2019,
                                            "fp": "FY",
                                            "start": "2018-09-03",
                                            "end": "2019-09-01",
                                            "accn": "stale-10k",
                                        }
                                    ]
                                }
                            },
                        }
                    }
                }
            )
        raise AssertionError(url)

    monkeypatch.setattr("lumibot.fundamentals.sec.requests.get", fake_get)
    sec = SECFundamentals(cache_dir=tmp_path, min_request_interval_seconds=0)

    result = sec.get_income_statement("COST", as_of=datetime(2026, 5, 22, tzinfo=timezone.utc))

    assert result["values"]["revenue"]["accession_number"] == "fresh-10q"
    assert result["values"]["net_income"]["accession_number"] == "fresh-10q"
    assert "gross_profit" not in result["values"]
    assert result["warnings"][0]["field"] == "gross_profit"


def test_sec_filings_and_keyword_search(monkeypatch, tmp_path):
    def fake_get(url, **kwargs):
        if url.endswith("company_tickers.json"):
            return _Response(payload={"0": {"ticker": "AAPL", "cik_str": 320193, "title": "Apple Inc."}})
        if "submissions" in url:
            return _Response(
                payload={
                    "cik": "0000320193",
                    "filings": {
                        "recent": {
                            "form": ["10-K", "10-Q"],
                            "accessionNumber": ["0000320193-24-000001", "0000320193-26-000001"],
                            "filingDate": ["2024-11-01", "2026-01-01"],
                            "reportDate": ["2024-09-30", "2025-12-31"],
                            "acceptanceDateTime": ["2024-11-01T12:00:00.000Z", "2026-01-01T12:00:00.000Z"],
                            "primaryDocument": ["aapl-20240930.htm", "aapl-20251231.htm"],
                            "primaryDocDescription": ["10-K", "10-Q"],
                        }
                    },
                }
            )
        if "Archives/edgar/data" in url:
            return _Response(text="<html><body>Revenue recognition changed. Customer concentration risk is low.</body></html>")
        raise AssertionError(url)

    monkeypatch.setattr("lumibot.fundamentals.sec.requests.get", fake_get)
    sec = SECFundamentals(cache_dir=tmp_path, min_request_interval_seconds=0)

    filings = sec.get_filings("AAPL", as_of="2025-01-01T00:00:00+00:00", limit=10)
    assert len(filings["filings"]) == 1
    assert filings["filings"][0]["form"] == "10-K"

    matches = sec.search_filing(
        "AAPL",
        accession_number="0000320193-24-000001",
        primary_document="aapl-20240930.htm",
        query="customer concentration",
    )
    assert matches["match_count"] >= 1
    assert "Customer concentration" in matches["matches"][0]["context"]


def test_sec_filing_sections_can_be_listed_and_read(monkeypatch, tmp_path):
    filing_html = """
    <html><body>
    <p>Item 7. Management's Discussion and Analysis</p>
    <p>short table of contents entry</p>
    <h1>Item 1A. Risk Factors</h1>
    <p>Customer concentration and supply chain risks could affect results.</p>
    <h1>Item 7. Management's Discussion and Analysis</h1>
    <p>Revenue increased because demand improved. Liquidity remains strong.</p>
    <h1>Item 8. Financial Statements and Supplementary Data</h1>
    <p>Audited financial statements follow.</p>
    </body></html>
    """

    def fake_get(url, **kwargs):
        if url.endswith("company_tickers.json"):
            return _Response(payload={"0": {"ticker": "AAPL", "cik_str": 320193, "title": "Apple Inc."}})
        if "submissions" in url:
            return _Response(
                payload={
                    "cik": "0000320193",
                    "filings": {
                        "recent": {
                            "form": ["10-K"],
                            "accessionNumber": ["0000320193-24-000001"],
                            "filingDate": ["2024-11-01"],
                            "reportDate": ["2024-09-30"],
                            "acceptanceDateTime": ["2024-11-01T12:00:00.000Z"],
                            "primaryDocument": ["aapl-20240930.htm"],
                            "primaryDocDescription": ["10-K"],
                        }
                    },
                }
            )
        if "Archives/edgar/data" in url:
            return _Response(text=filing_html)
        raise AssertionError(url)

    monkeypatch.setattr("lumibot.fundamentals.sec.requests.get", fake_get)
    sec = SECFundamentals(cache_dir=tmp_path, min_request_interval_seconds=0)

    sections = sec.list_filing_sections("AAPL", accession_number="0000320193-24-000001")
    assert [section["section_id"] for section in sections["sections"]] == ["item_7", "item_1a", "item_7", "item_8"]

    mda = sec.get_filing_section("AAPL", accession_number="0000320193-24-000001", section="mda")
    assert mda["ok"] is True
    assert mda["section_id"] == "item_7"
    assert "Revenue increased" in mda["text"]


def test_company_facts_are_compact_by_default(monkeypatch, tmp_path):
    def fake_get(url, **kwargs):
        if url.endswith("company_tickers.json"):
            return _Response(payload={"0": {"ticker": "AAPL", "cik_str": 320193, "title": "Apple Inc."}})
        if "companyfacts" in url:
            facts = {
                "RevenueFromContractWithCustomerExcludingAssessedTax": {
                    "units": {"USD": [{"val": 100, "filed": "2024-01-01", "form": "10-K"}]}
                }
            }
            for index in range(10):
                facts[f"CustomFact{index}"] = {
                    "units": {"USD": [{"val": index, "filed": "2024-01-01", "form": "10-K"}]}
                }
            return _Response(payload={"facts": {"us-gaap": facts}})
        raise AssertionError(url)

    monkeypatch.setattr("lumibot.fundamentals.sec.requests.get", fake_get)
    sec = SECFundamentals(cache_dir=tmp_path, min_request_interval_seconds=0)

    compact = sec.get_company_facts("AAPL", as_of="2025-01-01", max_facts=3)
    assert compact["fact_count"] == 3
    assert compact["truncated"] is True
    assert "RevenueFromContractWithCustomerExcludingAssessedTax" in compact["facts"]

    full = sec.get_company_facts("AAPL", as_of="2025-01-01", max_facts=None)
    assert full["fact_count"] == 11
    assert full["truncated"] is False
