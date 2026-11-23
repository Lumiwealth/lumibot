"""Stock listings from SEC EDGAR: which companies sold shares to the public for the first time,
when, under which ticker, and (once it was public) when they left the exchange.

All inputs are recorded or hand-made SEC shapes; no network.
"""

from __future__ import annotations

import gzip
import json
from datetime import date, datetime, timezone

import pytest

from lumibot.fundamentals import SECFundamentals
from lumibot.fundamentals.listings import (
    ListingsTable,
    classify_listing,
    parse_form_index,
    ticker_from_prospectus,
)

FORM_INDEX = """Description:           Master Index of EDGAR Dissemination Feed by Form Type
Form Type   Company Name                                                  CIK         Date Filed  File Name
---------------------------------------------------------------------------------------------------------------------------------------------
424B4            Casper Sleep Inc.                                             1598674     2020-02-07  edgar/data/1598674/0001047469-20-000749.txt
424B4            Old Company Inc                                               1000001     2020-03-02  edgar/data/1000001/0001-20-1.txt
424B1            Blank Check Acquisition Corp                                  1800002     2020-01-15  edgar/data/1800002/0002-20-1.txt
S-1              Casper Sleep Inc.                                             1598674     2020-01-10  edgar/data/1598674/0003-20-1.txt
424B3            Not An Offering Price Form                                    1700003     2020-01-20  edgar/data/1700003/0004-20-1.txt
"""


def _submissions(forms, sic="2510", name="Casper Sleep Inc.", tickers=None):
    return {
        "name": name,
        "sic": sic,
        "entityType": "operating",
        "tickers": tickers or [],
        "exchanges": [],
        "filings": {"recent": {"form": [f for f, _ in forms], "filingDate": [d for _, d in forms]}, "files": []},
    }


def test_form_index_keeps_only_final_prospectus_forms():
    rows = parse_form_index(FORM_INDEX)
    assert [(r["form"], r["cik"], r["filed"]) for r in rows] == [
        ("424B4", 1598674, "2020-02-07"),
        ("424B4", 1000001, "2020-03-02"),
        ("424B1", 1800002, "2020-01-15"),
    ]
    assert rows[0]["path"] == "edgar/data/1598674/0001047469-20-000749.txt"


@pytest.mark.parametrize(
    "text, expected",
    [
        ('<p>approved for listing on the NYSE under the symbol &#8220;CSPR.&#8221;</p>', "CSPR"),
        ("listed on The Nasdaq Global Select Market under the trading symbol “RDDT”", "RDDT"),
        ('Our Class A common stock will trade under the ticker symbol "BRK.B" on the NYSE', "BRK.B"),
        ("Units under the symbol “ABCDU” and shares under the symbol “ABCD”", "ABCD"),
        ("There is no public market and no symbol mentioned here.", None),
    ],
)
def test_ticker_is_read_from_the_prospectus_cover(text, expected):
    assert ticker_from_prospectus(text) == expected


def test_first_offering_by_a_new_company_is_a_listing_with_its_delisting_date():
    sub = _submissions([("S-1", "2020-01-10"), ("424B4", "2020-02-07"), ("10-Q", "2020-05-01"), ("25-NSE", "2022-01-25")])
    row = classify_listing({"cik": 1598674, "form": "424B4", "filed": "2020-02-07", "path": "p"}, sub)
    assert row["kind"] == "operating_company"
    assert row["prospectus_date"] == "2020-02-07"
    assert row["delisting_filed"] == "2022-01-25"
    assert row["sic"] == "2510"


def test_offering_after_periodic_reports_is_not_a_listing():
    sub = _submissions([("10-K", "2018-03-01"), ("424B4", "2020-03-02")], name="Old Company Inc")
    assert classify_listing({"cik": 1000001, "form": "424B4", "filed": "2020-03-02", "path": "p"}, sub) is None


def test_blank_check_companies_are_labeled_not_dropped():
    sub = _submissions([("S-1", "2019-12-01"), ("424B1", "2020-01-15")], sic="6770", name="Blank Check Acquisition Corp")
    row = classify_listing({"cik": 1800002, "form": "424B1", "filed": "2020-01-15", "path": "p"}, sub)
    assert row["kind"] == "blank_check"


ROWS = [
    {"cik": 1, "name": "Alive Co", "ticker": "ALIV", "prospectus_date": "2020-02-07", "form": "424B4",
     "kind": "operating_company", "sic": "7372", "delisting_filed": None, "prospectus_url": "u1"},
    {"cik": 2, "name": "Dead Co", "ticker": "DEAD", "prospectus_date": "2020-06-01", "form": "424B4",
     "kind": "operating_company", "sic": "2510", "delisting_filed": "2022-01-25", "prospectus_url": "u2"},
    {"cik": 3, "name": "Later Co", "ticker": "LATR", "prospectus_date": "2023-09-14", "form": "424B4",
     "kind": "operating_company", "sic": "3674", "delisting_filed": None, "prospectus_url": "u3"},
    {"cik": 4, "name": "Shell Acquisition Corp", "ticker": "SHEL", "prospectus_date": "2021-03-01", "form": "424B4",
     "kind": "blank_check", "sic": "6770", "delisting_filed": None, "prospectus_url": "u4"},
]


def test_table_never_shows_a_listing_or_a_delisting_before_it_happened():
    table = ListingsTable(ROWS, built_at="2026-11-22T00:00:00+00:00")
    seen = table.between(date(2019, 1, 1), date(2026, 12, 31), as_of=date(2021, 6, 30))
    assert [r["ticker"] for r in seen] == ["ALIV", "DEAD"]
    assert seen[1]["delisting_filed"] is None  # delisted in 2022: unknown on 2021-06-30
    later = table.between(date(2019, 1, 1), date(2026, 12, 31), as_of=date(2026, 1, 1))
    assert [r["ticker"] for r in later] == ["ALIV", "DEAD", "LATR"]
    assert later[1]["delisting_filed"] == "2022-01-25"


def test_blank_checks_only_when_asked():
    table = ListingsTable(ROWS, built_at="x")
    rows = table.between(date(2019, 1, 1), date(2026, 12, 31), as_of=date(2026, 1, 1), include_blank_checks=True)
    assert "SHEL" in [r["ticker"] for r in rows]


def test_table_round_trips_through_gzip(tmp_path):
    path = tmp_path / "listings.json.gz"
    ListingsTable(ROWS, built_at="2026-11-22T00:00:00+00:00").save(path)
    loaded = ListingsTable.load(path)
    assert loaded.rows == sorted(ROWS, key=lambda r: (r["prospectus_date"], r["cik"])) and loaded.built_at == "2026-11-22T00:00:00+00:00"


class _Clock:
    is_backtesting = True

    def __init__(self, now):
        self._now = now

    def get_datetime(self):
        return self._now


def _sec_with_table(tmp_path, now):
    sec = SECFundamentals(_Clock(now), cache_dir=tmp_path, remote_cache=None, split_history=None)
    ListingsTable(ROWS, built_at="2026-11-22T00:00:00+00:00").save(tmp_path / "listings" / "v1" / "listings.json.gz")
    return sec


def test_strategy_reads_listings_as_of_the_backtest_clock(tmp_path):
    sec = _sec_with_table(tmp_path, datetime(2021, 6, 30, 16, tzinfo=timezone.utc))
    df = sec.get_listings("2019-01-01", "2026-12-31")
    assert list(df["ticker"]) == ["ALIV", "DEAD"]
    assert df["delisting_filed"].isna().all()
    # Asking for a later date in a backtest is capped at the backtest clock.
    assert list(sec.get_listings("2019-01-01", "2026-12-31", as_of="2026-01-01")["ticker"]) == ["ALIV", "DEAD"]


def test_one_symbol_listing_lookup(tmp_path):
    sec = _sec_with_table(tmp_path, datetime(2026, 1, 1, tzinfo=timezone.utc))
    assert sec.get_listing("latr")["prospectus_date"] == "2023-09-14"
    assert sec.get_listing("NOPE") is None


def test_build_keeps_first_listings_labels_shells_and_reads_tickers():
    from lumibot.fundamentals.listings import build_listings

    subs = {
        1598674: _submissions([("S-1", "2020-01-10"), ("424B4", "2020-02-07"), ("25-NSE", "2022-01-25")]),
        1000001: _submissions([("10-K", "2018-03-01"), ("424B4", "2020-03-02")], name="Old Company Inc"),
        1800002: _submissions([("S-1", "2019-12-01"), ("424B1", "2020-01-15")], sic="6770", name="Blank Check Acquisition Corp"),
    }
    covers = {
        "https://www.sec.gov/Archives/edgar/data/1598674/0001047469-20-000749.txt": "under the symbol &#8220;CSPR&#8221;",
        "https://www.sec.gov/Archives/edgar/data/1800002/0002-20-1.txt": "units under the symbol “BCACU”",
    }

    def fetch_text(url):
        if "2020/QTR1" in url:
            return FORM_INDEX
        raise RuntimeError("no index")

    table = build_listings(
        None,
        [2020],
        fetch_text=fetch_text,
        fetch_head=lambda url: covers[url],
        fetch_submissions=lambda cik: subs[cik],
        max_workers=2,
    )
    assert [(r["ticker"], r["kind"], r["delisting_filed"]) for r in table.rows] == [
        ("BCACU", "blank_check", None),
        ("CSPR", "operating_company", "2022-01-25"),
    ]
