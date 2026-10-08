import json
import os
import time
from datetime import datetime, timezone

import pytest

from lumibot.fundamentals import SECFundamentals


class _Response:
    def __init__(self, *, payload=None, text="", status_code=200, headers=None):
        self._payload = payload
        self.text = text
        self.content = text.encode()
        self.status_code = status_code
        self.headers = dict(headers or {})

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
                                        {
                                            "val": 100,
                                            "filed": "2024-01-01",
                                            "form": "10-K",
                                            "fy": 2023,
                                            "fp": "FY",
                                            "accn": "old",
                                        },
                                        {
                                            "val": 200,
                                            "filed": "2026-01-01",
                                            "form": "10-K",
                                            "fy": 2025,
                                            "fp": "FY",
                                            "accn": "future",
                                        },
                                    ]
                                }
                            },
                            "NetIncomeLoss": {
                                "units": {
                                    "USD": [
                                        {
                                            "val": 10,
                                            "filed": "2024-01-01",
                                            "form": "10-K",
                                            "fy": 2023,
                                            "fp": "FY",
                                            "accn": "old",
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
            return _Response(
                text="<html><body>Revenue recognition changed. "
                "Customer concentration risk is low.</body></html>"
            )
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


def test_filings_for_a_ticker_without_an_sec_cik_are_an_explicit_empty_result(monkeypatch, tmp_path):
    """A ticker EDGAR does not list has no filings; that is evidence, not a tool failure.

    Release eval research_sec_prompt_injection: get_filings raised for a ticker
    missing from the SEC ticker map, the tool error marked the whole research
    decision blocked, although the agent then read the filing from the managed
    research source. Missing data is reported as missing, never invented.
    """

    def fake_get(url, **kwargs):
        if url.endswith("company_tickers.json"):
            return _Response(payload={"0": {"ticker": "AAPL", "cik_str": 320193, "title": "Apple Inc."}})
        raise AssertionError(url)

    monkeypatch.setattr("lumibot.fundamentals.sec.requests.get", fake_get)
    sec = SECFundamentals(cache_dir=tmp_path, min_request_interval_seconds=0)

    result = sec.get_filings("acme", form="10-Q", as_of="2026-08-11T00:00:00+00:00")

    assert result["symbol"] == "ACME"
    assert result["filings"] == []
    assert result["available"] is False
    assert result["reason"] == "no_sec_cik"
    assert "ACME" in result["message"]
    with pytest.raises(ValueError, match="No SEC CIK found"):
        sec.ticker_to_cik("ACME")


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


def test_live_mutable_sec_cache_expires_but_filing_documents_remain_immutable(monkeypatch, tmp_path):
    calls = []

    def fake_get(url, **kwargs):
        calls.append(url)
        if url.endswith("company_tickers.json"):
            return _Response(payload={"0": {"ticker": "AAPL", "cik_str": 320193, "title": "Apple Inc."}})
        if "submissions" in url:
            return _Response(payload={"cik": "0000320193", "filings": {"recent": {"form": []}}})
        if "Archives/edgar/data" in url:
            return _Response(text="<html>immutable filing</html>")
        raise AssertionError(url)

    monkeypatch.setattr("lumibot.fundamentals.sec.requests.get", fake_get)
    sec = SECFundamentals(
        cache_dir=tmp_path,
        cache_mode="live",
        mutable_cache_ttl_seconds=60,
        min_request_interval_seconds=0,
    )

    sec.get_submissions("AAPL")
    submissions_cache = tmp_path / "submissions" / "CIK0000320193.json"
    stale_time = time.time() - 61
    os.utime(submissions_cache, (stale_time, stale_time))
    sec.get_submissions("AAPL")

    sec.get_filing_document(
        "AAPL",
        accession_number="0000320193-24-000001",
        primary_document="aapl.htm",
        verify_availability=False,
    )
    filing_cache = tmp_path / "filings" / "0000320193" / "0000320193-24-000001" / "aapl.htm"
    os.utime(filing_cache, (stale_time, stale_time))
    sec.get_filing_document(
        "AAPL",
        accession_number="0000320193-24-000001",
        primary_document="aapl.htm",
        verify_availability=False,
    )

    assert sum("submissions" in url for url in calls) == 2
    assert sum("Archives/edgar/data" in url for url in calls) == 1


def test_live_mutable_sec_cache_revalidates_with_http_validators(monkeypatch, tmp_path):
    request_headers = []

    def fake_get(url, **kwargs):
        if url.endswith("company_tickers.json"):
            return _Response(payload={"0": {"ticker": "AAPL", "cik_str": 320193, "title": "Apple Inc."}})
        if "submissions" in url:
            request_headers.append(dict(kwargs["headers"]))
            if len(request_headers) == 1:
                return _Response(
                    payload={"cik": "0000320193", "filings": {"recent": {"form": []}}},
                    headers={"ETag": '"submissions-v1"', "Last-Modified": "Sat, 20 Sep 2026 12:00:00 GMT"},
                )
            return _Response(status_code=304)
        raise AssertionError(url)

    monkeypatch.setattr("lumibot.fundamentals.sec.requests.get", fake_get)
    sec = SECFundamentals(
        cache_dir=tmp_path,
        cache_mode="live",
        mutable_cache_ttl_seconds=60,
        min_request_interval_seconds=0,
    )

    first = sec.get_submissions("AAPL")
    cache_path = tmp_path / "submissions" / "CIK0000320193.json"
    stale_time = time.time() - 61
    os.utime(cache_path, (stale_time, stale_time))
    second = sec.get_submissions("AAPL")

    assert second["cik"] == first["cik"]
    assert request_headers[1]["If-None-Match"] == '"submissions-v1"'
    assert request_headers[1]["If-Modified-Since"] == "Sat, 20 Sep 2026 12:00:00 GMT"
    assert cache_path.stat().st_mtime > stale_time


def test_backtest_mode_keeps_mutable_sec_cache_immutable(monkeypatch, tmp_path):
    cache_path = tmp_path / "submissions" / "CIK0000320193.json"
    cache_path.parent.mkdir(parents=True)
    cache_path.write_text(json.dumps({"cik": "0000320193", "filings": {"recent": {"form": []}}}))
    stale_time = time.time() - 86400
    os.utime(cache_path, (stale_time, stale_time))

    def fake_get(url, **kwargs):
        if url.endswith("company_tickers.json"):
            return _Response(payload={"0": {"ticker": "AAPL", "cik_str": 320193, "title": "Apple Inc."}})
        raise AssertionError("backtest cache must not refetch mutable SEC indexes")

    monkeypatch.setattr("lumibot.fundamentals.sec.requests.get", fake_get)
    sec = SECFundamentals(cache_dir=tmp_path, cache_mode="backtest", min_request_interval_seconds=0)

    assert sec.get_submissions("AAPL")["cik"] == "0000320193"


def test_raw_company_facts_are_filtered_to_as_of(monkeypatch, tmp_path):
    def fake_get(url, **kwargs):
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
                                        {"val": 100, "filed": "2024-01-01", "form": "10-K"},
                                        {"val": 999, "filed": "2026-01-01", "form": "10-K"},
                                    ]
                                }
                            }
                        }
                    }
                }
            )
        raise AssertionError(url)

    monkeypatch.setattr("lumibot.fundamentals.sec.requests.get", fake_get)
    sec = SECFundamentals(cache_dir=tmp_path, min_request_interval_seconds=0)

    raw = sec.get_company_facts("AAPL", as_of="2025-01-01", raw=True)

    rows = raw["facts"]["us-gaap"]["Revenues"]["units"]["USD"]
    assert [row["val"] for row in rows] == [100]
    assert raw["as_of"] == "2025-01-01T00:00:00"
    assert raw["source"] == "sec_edgar_companyfacts"
    assert raw["source_url"].endswith("/api/xbrl/companyfacts/CIK0000320193.json")
    assert raw["fetched_at"]
    assert raw["published_at"] == "2024-01-01T00:00:00"
    assert raw["id"] == "sec-companyfacts-0000320193"


def test_sec_filing_rows_and_documents_carry_availability_provenance(monkeypatch, tmp_path):
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
            return _Response(text="<html><body>public filing</body></html>")
        raise AssertionError(url)

    monkeypatch.setattr("lumibot.fundamentals.sec.requests.get", fake_get)
    sec = SECFundamentals(cache_dir=tmp_path, min_request_interval_seconds=0)

    filings = sec.get_filings("AAPL", as_of="2025-01-01T00:00:00+00:00")
    row = filings["filings"][0]
    document = sec.get_filing_document(
        "AAPL",
        accession_number=row["accession_number"],
        primary_document=row["primary_document"],
        as_of="2025-01-01T00:00:00+00:00",
    )

    assert row["id"] == "0000320193-24-000001"
    assert row["source"] == "sec_edgar_submissions"
    assert row["published_at"] == "2024-11-01T12:00:00.000Z"
    assert row["fetched_at"]
    assert document["id"] == "0000320193-24-000001"
    assert document["source"] == "sec_edgar_filing_document"
    assert document["published_at"] == "2024-11-01T12:00:00.000Z"
    assert document["fetched_at"]


def test_filing_document_rejects_accession_not_public_as_of(monkeypatch, tmp_path):
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
                            "accessionNumber": ["0000320193-26-000001"],
                            "filingDate": ["2026-01-02"],
                            "reportDate": ["2025-12-31"],
                            "acceptanceDateTime": ["2026-01-02T12:00:00.000Z"],
                            "primaryDocument": ["aapl-2025.htm"],
                            "primaryDocDescription": ["10-K"],
                        }
                    },
                }
            )
        if "Archives/edgar/data" in url:
            raise AssertionError("future filing document must not be downloaded")
        raise AssertionError(url)

    monkeypatch.setattr("lumibot.fundamentals.sec.requests.get", fake_get)
    sec = SECFundamentals(cache_dir=tmp_path, min_request_interval_seconds=0)

    with pytest.raises(ValueError, match="was not public as of"):
        sec.get_filing_document(
            "AAPL",
            accession_number="0000320193-26-000001",
            primary_document="aapl-2025.htm",
            as_of="2025-12-31T23:59:59+00:00",
        )


def test_submissions_default_to_strategy_time_in_backtests(monkeypatch, tmp_path):
    class _Strategy:
        is_backtesting = True

        @staticmethod
        def get_datetime():
            return datetime(2025, 1, 1, tzinfo=timezone.utc)

    def fake_get(url, **kwargs):
        if url.endswith("company_tickers.json"):
            return _Response(payload={"0": {"ticker": "AAPL", "cik_str": 320193, "title": "Apple Inc."}})
        if "submissions" in url:
            return _Response(
                payload={
                    "cik": "0000320193",
                    "filings": {
                        "recent": {
                            "form": ["10-K", "10-K"],
                            "accessionNumber": ["old", "future"],
                            "filingDate": ["2024-01-02", "2026-01-02"],
                            "acceptanceDateTime": ["2024-01-02T12:00:00Z", "2026-01-02T12:00:00Z"],
                            "primaryDocument": ["old.htm", "future.htm"],
                        }
                    },
                }
            )
        raise AssertionError(url)

    monkeypatch.setattr("lumibot.fundamentals.sec.requests.get", fake_get)
    sec = SECFundamentals(strategy=_Strategy(), cache_dir=tmp_path, min_request_interval_seconds=0)

    submissions = sec.get_submissions("AAPL")

    assert submissions["filings"]["recent"]["accessionNumber"] == ["old"]
    assert submissions["as_of"] == "2025-01-01T00:00:00+00:00"


def test_backtest_caps_caller_as_of_at_strategy_time(monkeypatch, tmp_path):
    class _Strategy:
        is_backtesting = True

        @staticmethod
        def get_datetime():
            return datetime(2025, 1, 1, tzinfo=timezone.utc)

    def fake_get(url, **kwargs):
        if url.endswith("company_tickers.json"):
            return _Response(payload={"0": {"ticker": "AAPL", "cik_str": 320193, "title": "Apple Inc."}})
        if "submissions" in url:
            return _Response(
                payload={
                    "cik": "0000320193",
                    "filings": {
                        "recent": {
                            "form": ["4", "4"],
                            "accessionNumber": ["old", "future"],
                            "filingDate": ["2024-12-20", "2025-06-02"],
                            "acceptanceDateTime": ["2024-12-20T12:00:00Z", "2025-06-02T12:00:00Z"],
                            "primaryDocument": ["old.xml", "future.xml"],
                        }
                    },
                }
            )
        if "Archives/edgar/data" in url:
            raise AssertionError("future filing document must not be downloaded")
        raise AssertionError(url)

    monkeypatch.setattr("lumibot.fundamentals.sec.requests.get", fake_get)
    sec = SECFundamentals(strategy=_Strategy(), cache_dir=tmp_path, min_request_interval_seconds=0)

    filings = sec.get_filings("AAPL", form="4", as_of="2026-01-01T00:00:00Z")

    assert [row["accession_number"] for row in filings["filings"]] == ["old"]
    with pytest.raises(ValueError, match="was not public as of"):
        sec.get_filing_document(
            "AAPL",
            accession_number="future",
            primary_document="future.xml",
            as_of="2026-01-01T00:00:00Z",
        )


def test_search_filing_threads_explicit_as_of_to_document_availability(monkeypatch, tmp_path):
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
                            "accessionNumber": ["future"],
                            "filingDate": ["2026-01-02"],
                            "acceptanceDateTime": ["2026-01-02T12:00:00Z"],
                            "primaryDocument": ["future.htm"],
                        }
                    },
                }
            )
        if "Archives/edgar/data" in url:
            raise AssertionError("future filing document must not be downloaded")
        raise AssertionError(url)

    monkeypatch.setattr("lumibot.fundamentals.sec.requests.get", fake_get)
    sec = SECFundamentals(cache_dir=tmp_path, min_request_interval_seconds=0)

    with pytest.raises(ValueError, match="was not public as of"):
        sec.search_filing(
            "AAPL",
            accession_number="future",
            primary_document="future.htm",
            query="risk",
            as_of="2025-01-01T00:00:00Z",
        )


def test_strip_html_drops_script_and_style_blocks_with_spaced_end_tags():
    # Browsers accept "</script >" and "</style foo>" as end tags. The stripper must
    # drop those blocks too instead of leaking their contents into filing text.
    from lumibot.fundamentals.sec import _strip_html

    text = _strip_html(
        "<p>Revenue grew.</p><script>var secret = 1;</script >"
        "<style>.x{color:red}</style\n><p>Margins held.</p><SCRIPT type='a'>evil()</SCRIPT\t>"
    )
    assert "Revenue grew." in text
    assert "Margins held." in text
    assert "secret" not in text
    assert "color" not in text
    assert "evil" not in text
