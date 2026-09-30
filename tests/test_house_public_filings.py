"""Point-in-time House filings. Future index rows are never downloaded."""

from datetime import datetime, timezone

from lumibot.components import house_ptr
from lumibot.components.agents import BuiltinTools


_INDEX = (
    "Prefix\tLast\tFirst\tSuffix\tFilingType\tStateDst\tYear\tFilingDate\tDocID\n"
    "Hon.\tPelosi\tNancy\t\tP\tCA11\t2026\t08/01/2026\t111\n"
    "Hon.\tPelosi\tNancy\t\tP\tCA11\t2026\t2026-09-15\t222\n"
    "Hon.\tSmith\tAnn\t\tP\tCA12\t2026\t08/01/2026\t333\n"
)

_PUBLIC_ROW = {
    "Ticker": "AAPL",
    "Politician": "Nancy Pelosi",
    "Transaction": "P",
    "TransactionDate": "2026-07-28",
    "ReportDate": "2026-08-01",
    "Amount": "$1,001 - $15,000",
    "side": "buy",
    "asset_code": "ST",
    "doc_id": "111",
    "source": "house_ptr",
}

_FUTURE_ROW = {
    "Ticker": "ZZZZ",
    "Politician": "Nancy Pelosi",
    "Transaction": "P",
    "TransactionDate": "2026-09-10",
    "ReportDate": "2026-09-15",
    "Amount": "$1,001 - $15,000",
    "side": "buy",
    "asset_code": "ST",
    "doc_id": "222",
    "source": "house_ptr",
}


def test_public_house_filings_does_not_download_a_filing_after_as_of(monkeypatch):
    downloaded = []

    monkeypatch.setattr(house_ptr, "download_house_index", lambda year: _INDEX)

    def fake_pdf(year, doc_id):
        downloaded.append(str(doc_id))
        return b"%PDF"

    monkeypatch.setattr(house_ptr, "download_house_pdf", fake_pdf)
    monkeypatch.setattr(house_ptr, "pdf_bytes_to_text", lambda pdf: "filing")
    monkeypatch.setattr(
        house_ptr,
        "parse_house_ptr_text",
        lambda text, source_url=None: [dict(_PUBLIC_ROW)],
    )

    result = house_ptr.public_house_filings(
        2026,
        last_names=["Pelosi"],
        as_of=datetime(2026, 8, 11, tzinfo=timezone.utc),
    )

    assert downloaded == ["111"]
    assert result["omitted_future_count"] == 1
    assert [row["ticker"] for row in result["filings"]] == ["AAPL"]
    blob = str(result)
    assert "ZZZZ" not in blob
    assert "222" not in blob


def test_house_public_disclosures_hides_recorded_filings_after_the_strategy_clock():
    class _Strategy:
        is_backtesting = True
        house_disclosure_records = [dict(_PUBLIC_ROW), dict(_FUTURE_ROW)]

        def get_datetime(self):
            return datetime(2026, 8, 11, 14, 35, tzinfo=timezone.utc)

    tool = BuiltinTools.disclosures.house_public_disclosures().binder(_Strategy(), None)
    result = tool.function(last_name="Pelosi")

    assert result["ok"] is True
    assert result["as_of"].startswith("2026-08-11")
    assert [row["ticker"] for row in result["filings"]] == ["AAPL"]
    blob = str(result)
    assert "ZZZZ" not in blob
    assert "222" not in blob
    assert result["omitted_future_count"] == 1


# A House index row only has a date. The Clerk can post the report any time
# that day, so a bot running at the 9:30 AM open must not see it until the
# next day. On 2026-09-29 a backtest saw Pelosi's 1/23/2026 report at 9:30 AM
# on 1/23 (the filing was posted later that morning).
_SAME_DAY_INDEX = (
    "Prefix\tLast\tFirst\tSuffix\tFilingType\tStateDst\tYear\tFilingDate\tDocID\n"
    "Hon.\tPelosi\tNancy\t\tP\tCA11\t2026\t1/23/2026\t20033725\n"
)
_SAME_DAY_ROW = {**_PUBLIC_ROW, "Ticker": "AB", "ReportDate": "2026-01-23", "doc_id": "20033725"}
_OPEN_ON_FILING_DAY = datetime(2026, 1, 23, 14, 30, tzinfo=timezone.utc)
_OPEN_NEXT_DAY = datetime(2026, 1, 26, 14, 30, tzinfo=timezone.utc)


def _patch_same_day(monkeypatch, downloaded):
    monkeypatch.setattr(house_ptr, "download_house_index", lambda year: _SAME_DAY_INDEX)

    def fake_pdf(year, doc_id):
        downloaded.append(str(doc_id))
        return b"%PDF"

    monkeypatch.setattr(house_ptr, "download_house_pdf", fake_pdf)
    monkeypatch.setattr(house_ptr, "pdf_bytes_to_text", lambda pdf: "filing")
    monkeypatch.setattr(house_ptr, "parse_house_ptr_text", lambda text, source_url=None: [dict(_SAME_DAY_ROW)])


def test_public_house_filings_hides_a_report_on_the_morning_it_is_filed(monkeypatch):
    downloaded = []
    _patch_same_day(monkeypatch, downloaded)

    early = house_ptr.public_house_filings(2026, last_names=["Pelosi"], as_of=_OPEN_ON_FILING_DAY)
    assert downloaded == []
    assert early["filings"] == []
    assert early["omitted_future_count"] == 1

    later = house_ptr.public_house_filings(2026, last_names=["Pelosi"], as_of=_OPEN_NEXT_DAY)
    assert [row["ticker"] for row in later["filings"]] == ["AB"]


def test_house_public_disclosures_hides_recorded_rows_on_the_morning_they_are_filed():
    class _Strategy:
        is_backtesting = True
        house_disclosure_records = [dict(_SAME_DAY_ROW)]
        now = _OPEN_ON_FILING_DAY

        def get_datetime(self):
            return self.now

    strategy = _Strategy()
    tool = BuiltinTools.disclosures.house_public_disclosures().binder(strategy, None)
    assert tool.function(last_name="Pelosi")["filings"] == []
    strategy.now = _OPEN_NEXT_DAY
    assert [row["ticker"] for row in tool.function(last_name="Pelosi")["filings"]] == ["AB"]
