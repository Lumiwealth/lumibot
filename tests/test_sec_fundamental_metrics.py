"""Ready-made point-in-time fundamental metrics (TTM, margins, ratios, valuation).

Synthetic filer, calendar fiscal year:

* FY2022 10-K (filed 2023-02-10), Q1 2023 10-Q (filed 2023-05-05), H1 2023 10-Q (2023-08-04)
* FY2023 10-K (filed 2024-02-09), Q1 2024 10-Q (filed 2024-05-03), H1 2024 10-Q (2024-08-02)
* FY2024 10-K (filed 2025-02-07) that restates FY2023 revenue to 999.

No acceptance times are published, so each filing is usable from 09:30 New York the
day after its filing date. Everything is synthetic; nothing talks to the SEC.
"""

import math
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from lumibot.fundamentals import SECFundamentals

NY = ZoneInfo("America/New_York")
CIK = "0000222222"


@pytest.fixture(autouse=True)
def _no_shared_remote_cache(monkeypatch):
    from lumibot.tools import backtest_cache

    disabled = backtest_cache.BacktestCacheManager(None)
    monkeypatch.setattr(backtest_cache, "get_backtest_cache", lambda: disabled)


class _Response:
    def __init__(self, payload=None, status_code=200, text=""):
        self._payload = payload
        self.status_code = status_code
        self.headers = {}
        self.text = text

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


FILINGS = {
    "k22": ("2023-02-10", "10-K", "FY", 2022),
    "q123": ("2023-05-05", "10-Q", "Q1", 2023),
    "q223": ("2023-08-04", "10-Q", "Q2", 2023),
    "k23": ("2024-02-09", "10-K", "FY", 2023),
    "q124": ("2024-05-03", "10-Q", "Q1", 2024),
    "q224": ("2024-08-02", "10-Q", "Q2", 2024),
    "k24": ("2025-02-07", "10-K", "FY", 2024),
}
FY = {2022: ("2022-01-01", "2022-12-31"), 2023: ("2023-01-01", "2023-12-31"), 2024: ("2024-01-01", "2024-12-31")}
Q1 = {2022: ("2022-01-01", "2022-03-31"), 2023: ("2023-01-01", "2023-03-31"), 2024: ("2024-01-01", "2024-03-31")}
H1 = {2022: ("2022-01-01", "2022-06-30"), 2023: ("2023-01-01", "2023-06-30"), 2024: ("2024-01-01", "2024-06-30")}


def _r(accn, period, val):
    filed, form, fp, fy = FILINGS[accn]
    row = {"val": val, "accn": accn, "filed": filed, "form": form, "fp": fp, "fy": fy}
    if isinstance(period, tuple):
        row["start"], row["end"] = period
    else:
        row["end"] = period
    return row


def _flow(fy22, fy23, q1_22, q1_23, q1_24, h1_22=None, h1_23=None, h1_24=None, fy24=None, fy23_restated=None):
    rows = [
        _r("k22", FY[2022], fy22),
        _r("q123", Q1[2023], q1_23), _r("q123", Q1[2022], q1_22),
        _r("k23", FY[2023], fy23), _r("k23", FY[2022], fy22),
        _r("q124", Q1[2024], q1_24), _r("q124", Q1[2023], q1_23),
    ]
    if h1_23 is not None:
        rows += [_r("q223", H1[2023], h1_23), _r("q223", H1[2022], h1_22)]
    if h1_24 is not None:
        rows += [_r("q224", H1[2024], h1_24), _r("q224", H1[2023], h1_23)]
    if fy24 is not None:
        rows += [_r("k24", FY[2024], fy24), _r("k24", FY[2023], fy23_restated if fy23_restated is not None else fy23)]
    return {"units": {"USD": rows}}


def _instant(**by_accn):
    ends = {"k23": "2023-12-31", "q124": "2024-03-31", "k22": "2022-12-31", "q224": "2024-06-30"}
    return {"units": {"USD": [_r(accn, ends[accn], val) for accn, val in by_accn.items()]}}


def _facts(*, eps_q1_24=1.5, gross=True, debt=True):
    us_gaap = {
        "RevenueFromContractWithCustomerExcludingAssessedTax": _flow(
            400, 500, 90, 110, 130, h1_22=190, h1_23=240, h1_24=270, fy24=600, fy23_restated=999
        ),
        "NetIncomeLoss": _flow(40, 50, 8, 10, 15),
        "NetCashProvidedByUsedInOperatingActivities": _flow(70, 80, 15, 20, 25),
        "PaymentsToAcquirePropertyPlantAndEquipment": _flow(25, 30, 5, 6, 8),
        "EarningsPerShareDiluted": {"units": {"USD/shares": _flow(4.0, 5.0, 0.8, 1.0, eps_q1_24)["units"]["USD"]}},
        "AssetsCurrent": _instant(k23=280, q124=300),
        "LiabilitiesCurrent": _instant(k23=140, q124=150),
        "StockholdersEquity": _instant(k23=380, q124=400),
    }
    if gross:
        us_gaap["GrossProfit"] = _flow(160, 200, 36, 44, 52)
    if debt:
        us_gaap["LongTermDebt"] = _instant(k23=190, q124=200)
    dei = {
        "EntityCommonStockSharesOutstanding": {
            "units": {"shares": [_r("k23", "2024-01-31", 10_000_000 / 1_000_000), _r("q124", "2024-04-25", 10)]}
        }
    }
    return {"cik": int(CIK), "facts": {"us-gaap": us_gaap, "dei": dei}}


def _sec(monkeypatch, tmp_path, facts=None, *, strategy=None, extra_tickers=None, extra_facts=None, documents=None):
    tickers = {"0": {"ticker": "EXM", "cik_str": int(CIK), "title": "Example"}}
    tickers.update(extra_tickers or {})
    all_facts = {CIK: facts or _facts(), **(extra_facts or {})}

    def fake_get(url, **kwargs):
        if url.endswith("company_tickers.json"):
            return _Response(tickers)
        for cik, payload in all_facts.items():
            if url.endswith(f"companyfacts/CIK{cik}.json"):
                return _Response(payload)
        if "submissions" in url:
            return _Response({"filings": {"recent": {"accessionNumber": [], "acceptanceDateTime": []}}})
        for suffix, text in (documents or {}).items():  # filing documents, by URL ending
            if url.endswith(suffix):
                return _Response(text=text)
        return _Response({}, status_code=404)

    monkeypatch.setattr("lumibot.fundamentals.sec.requests.get", fake_get)
    return SECFundamentals(
        strategy=strategy, cache_dir=tmp_path, min_request_interval_seconds=0, remote_cache=None,
        split_history=lambda _symbol: [],  # no splits; never call Yahoo from a unit test
    )


AFTER_Q1 = datetime(2024, 6, 3, 10, 0, tzinfo=NY)


def test_ttm_flows_margins_and_balance_ratios_as_of_a_date(monkeypatch, tmp_path):
    sec = _sec(monkeypatch, tmp_path)

    metrics = sec.get_metrics("EXM", as_of=AFTER_Q1, price=100.0)
    values = metrics["values"]

    # TTM = FY2023 + Q1 2024 - Q1 2023
    assert values["ttm_revenue"] == 500 + 130 - 110
    assert values["ttm_net_income"] == 50 + 15 - 10
    assert values["ttm_eps_diluted"] == pytest.approx(5.0 + 1.5 - 1.0)
    assert values["ttm_free_cash_flow"] == (80 + 25 - 20) - (30 + 8 - 6)
    # Year over year: TTM to 2024-03-31 vs TTM to 2023-03-31 (400 + 110 - 90).
    assert values["revenue_growth_yoy"] == pytest.approx(520 / 420 - 1)
    assert values["gross_margin"] == pytest.approx((200 + 52 - 44) / 520)
    assert values["net_margin"] == pytest.approx(55 / 520)
    assert values["debt_to_equity"] == pytest.approx(200 / 400)
    assert values["current_ratio"] == pytest.approx(300 / 150)
    assert values["shares_outstanding"] == 10
    assert values["book_value_per_share"] == pytest.approx(40)
    assert values["market_cap"] == pytest.approx(1000)
    assert values["pe_ratio"] == pytest.approx(100 / 5.5)
    assert values["ps_ratio"] == pytest.approx(1000 / 520)
    assert values["pb_ratio"] == pytest.approx(2.5)
    assert metrics["reasons"] == {}
    assert metrics["periods"]["ttm_revenue"]["end"] == "2024-03-31"


def test_one_line_metric_matches_the_full_result(monkeypatch, tmp_path):
    sec = _sec(monkeypatch, tmp_path)
    assert sec.get_metric("EXM", "ttm_revenue", as_of=AFTER_Q1) == 520
    with pytest.raises(ValueError, match="Unknown fundamental metric"):
        sec.get_metric("EXM", "not_a_metric", as_of=AFTER_Q1)


def test_filing_day_values_are_not_visible_and_restatements_wait_for_their_filing(monkeypatch, tmp_path):
    sec = _sec(monkeypatch, tmp_path)

    # The Q1 2024 10-Q is filed on May 3 but has no acceptance time: not usable that day.
    on_filing_day = sec.get_metrics("EXM", as_of=datetime(2024, 5, 3, 15, 0, tzinfo=NY))["values"]
    assert on_filing_day["ttm_revenue"] == 500
    assert on_filing_day["current_ratio"] == pytest.approx(280 / 140)
    next_day = sec.get_metrics("EXM", as_of=datetime(2024, 5, 4, 9, 30, tzinfo=NY))["values"]
    assert next_day["ttm_revenue"] == 520

    # After the H1 10-Q: FY2023 + H1 2024 - H1 2023, growth vs TTM to 2023-06-30 (400 + 240 - 190).
    after_h1 = sec.get_metrics("EXM", as_of=datetime(2024, 9, 3, 10, tzinfo=NY))["values"]
    assert after_h1["ttm_revenue"] == 500 + 270 - 240
    assert after_h1["revenue_growth_yoy"] == pytest.approx(530 / 450 - 1)

    # The FY2023 restatement to 999 (filed 2025-02-07) never leaks into 2024 dates.
    assert all(value != 999 for value in after_h1.values() if value is not None)
    after_restatement = sec.get_metrics("EXM", as_of=datetime(2025, 3, 3, 10, tzinfo=NY))["values"]
    assert after_restatement["ttm_revenue"] == 600


def test_price_defaults_to_the_strategys_last_price_at_the_bar(monkeypatch, tmp_path):
    class _Strategy:
        is_backtesting = True
        _backtesting_end = datetime(2024, 12, 31, tzinfo=NY)

        def __init__(self):
            self.asked = []

        def get_datetime(self):
            return AFTER_Q1

        def get_last_price(self, asset):
            self.asked.append(getattr(asset, "symbol", asset))
            return 50.0

    strategy = _Strategy()
    sec = _sec(monkeypatch, tmp_path, strategy=strategy)

    values = sec.get_metrics("EXM")["values"]

    assert strategy.asked == ["EXM"]
    assert values["market_cap"] == pytest.approx(500)
    assert values["pe_ratio"] == pytest.approx(50 / 5.5)


def test_no_price_is_loaded_for_a_company_with_no_filing_yet(monkeypatch, tmp_path):
    """A screen over many companies must not load price history for one that had filed
    nothing yet: its price metrics are None either way, and each price load is a download."""

    class _Strategy:
        is_backtesting = True
        _backtesting_end = datetime(2024, 12, 31, tzinfo=NY)

        def __init__(self):
            self.asked = []

        def get_datetime(self):
            return datetime(2021, 6, 1, 10, tzinfo=NY)

        def get_last_price(self, asset):
            self.asked.append(getattr(asset, "symbol", asset))
            return 50.0

    strategy = _Strategy()
    sec = _sec(monkeypatch, tmp_path, strategy=strategy)

    metrics = sec.get_metrics("EXM", ["market_cap", "pe_ratio"])
    table = sec.get_metrics_table(["EXM"], ["market_cap"])

    assert strategy.asked == []
    assert metrics["values"]["market_cap"] is None
    assert "no SEC filings" in metrics["reasons"]["market_cap"]
    assert table.loc["EXM", "market_cap"] is None


@pytest.mark.parametrize(
    "facts_kwargs, as_of, price, metric, reason_fragment",
    [
        ({}, AFTER_Q1, None, "pe_ratio", "price"),
        ({"eps_q1_24": -9.0}, AFTER_Q1, 100.0, "pe_ratio", "not positive"),
        ({"gross": False}, AFTER_Q1, None, "gross_margin", "gross profit"),
        ({"debt": False}, AFTER_Q1, None, "debt_to_equity", "debt"),
        ({}, datetime(2023, 3, 1, 10, tzinfo=NY), None, "revenue_growth_yoy", "year earlier"),
        ({}, datetime(2028, 6, 1, 10, tzinfo=NY), 100.0, "ttm_revenue", "18 months"),
        ({}, datetime(2021, 6, 1, 10, tzinfo=NY), 100.0, "ttm_revenue", "no"),
    ],
)
def test_missing_inputs_return_none_with_a_reason(monkeypatch, tmp_path, facts_kwargs, as_of, price, metric, reason_fragment):
    sec = _sec(monkeypatch, tmp_path, _facts(**facts_kwargs))

    metrics = sec.get_metrics("EXM", as_of=as_of, price=price)

    assert metrics["values"][metric] is None
    assert reason_fragment in metrics["reasons"][metric].lower()


def test_missing_debt_is_never_treated_as_zero(monkeypatch, tmp_path):
    sec = _sec(monkeypatch, tmp_path, _facts(debt=False))
    assert sec.get_metric("EXM", "debt_to_equity", as_of=AFTER_Q1) is None


def test_bulk_table_for_a_scanner_universe(monkeypatch, tmp_path):
    other_cik = "0000333333"
    sec = _sec(
        monkeypatch,
        tmp_path,
        extra_tickers={"1": {"ticker": "OTH", "cik_str": int(other_cik), "title": "Other"}},
        extra_facts={other_cik: _facts(gross=False)},
    )

    table = sec.get_metrics_table(
        ["EXM", "OTH", "NOSUCH"],
        metrics=["ttm_revenue", "gross_margin", "pe_ratio"],
        as_of=AFTER_Q1,
        prices={"EXM": 100.0, "OTH": 20.0},
    )

    assert isinstance(table, pd.DataFrame)
    assert list(table.index) == ["EXM", "OTH", "NOSUCH"]
    assert table.loc["EXM", "ttm_revenue"] == 520
    assert table.loc["OTH", "pe_ratio"] == pytest.approx(20 / 5.5)
    assert table.loc["OTH", "gross_margin"] is None or math.isnan(table.loc["OTH", "gross_margin"])
    assert "gross profit" in table.loc["OTH", "reasons"]["gross_margin"].lower()
    assert all(
        table.loc["NOSUCH", name] is None or math.isnan(table.loc["NOSUCH", name])
        for name in ["ttm_revenue", "gross_margin", "pe_ratio"]
    )
    assert "sec" in table.loc["NOSUCH", "reasons"]["ttm_revenue"].lower()


# ---- coverage: real companies whose facts use other tags (recorded SEC rows, public data) ----

def _recorded(monkeypatch, tmp_path):
    import json
    from pathlib import Path

    companies = json.loads((Path(__file__).parent / "fixtures" / "sec_companyfacts_coverage.json").read_text())["companies"]
    tickers = {str(i + 10): {"ticker": s, "cik_str": c["cik"], "title": c["entityName"]} for i, (s, c) in enumerate(companies.items())}
    facts = {f"{c['cik']:010d}": {"cik": c["cik"], "facts": c["facts"]} for c in companies.values()}
    return _sec(monkeypatch, tmp_path, extra_tickers=tickers, extra_facts=facts)


def test_share_count_falls_back_to_weighted_average_when_no_total_is_filed(monkeypatch, tmp_path):
    """A company with several share classes files no single cover-page count (META)."""
    sec = _recorded(monkeypatch, tmp_path)

    result = sec.get_metrics("META", ["shares_outstanding", "market_cap"], as_of=AFTER_Q1, price=100.0)

    assert result["values"]["shares_outstanding"] == 2_545_000_000  # Q1 2024 quarter, filed 2024-04-25
    assert result["values"]["market_cap"] == pytest.approx(254.5e9)
    assert result["periods"]["shares_outstanding"]["inputs"][0]["tag"] == "WeightedAverageNumberOfSharesOutstandingBasic"
    before = sec.get_metrics("META", ["shares_outstanding"], as_of=datetime(2024, 4, 25, 10, tzinfo=NY))
    assert before["values"]["shares_outstanding"] == 2_574_000_000  # filing day: still the FY2023 figure


def test_debt_to_equity_reads_equity_filed_with_noncontrolling_interest(monkeypatch, tmp_path):
    result = _recorded(monkeypatch, tmp_path).get_metrics("PG", ["debt_to_equity"], as_of=AFTER_Q1)

    assert result["values"]["debt_to_equity"] is not None, result["reasons"]
    assert 0.4 < result["values"]["debt_to_equity"] < 0.8  # 24.3 B long-term (+ current portion) over 50.3 B


def test_debt_to_equity_reads_debt_filed_together_with_lease_obligations(monkeypatch, tmp_path):
    result = _recorded(monkeypatch, tmp_path).get_metrics("ABBV", ["debt_to_equity"], as_of=AFTER_Q1)

    assert result["values"]["debt_to_equity"] == pytest.approx((63_805e6 + 10_193e6) / 8_007e6)


def test_debt_to_equity_uses_the_latest_balance_sheet_that_has_both(monkeypatch, tmp_path):
    """Debt totals are often filed only in the annual report (FRT): use that date for both sides."""
    result = _recorded(monkeypatch, tmp_path).get_metrics("FRT", ["debt_to_equity"], as_of=AFTER_Q1)

    assert result["values"]["debt_to_equity"] == pytest.approx(4_615_731e3 / 2_963_509e3)
    assert result["periods"]["debt_to_equity"]["end"] == "2023-12-31"


# ---- registrant history and share classes (recorded SEC rows and filing cover pages, public data) ----

def _fixture_path(name):
    from pathlib import Path

    return Path(__file__).parent / "fixtures" / name


def _alphabet(monkeypatch, tmp_path):
    """GOOGL and GOOG map to Alphabet Inc., which first filed in October 2015; Google Inc. filed before."""
    import json

    data = json.loads(_fixture_path("sec_companyfacts_registrant_history.json").read_text())
    return _sec(
        monkeypatch, tmp_path,
        extra_tickers={
            "20": {"ticker": "GOOGL", "cik_str": 1652044, "title": "Alphabet Inc."},
            "21": {"ticker": "GOOG", "cik_str": 1652044, "title": "Alphabet Inc."},
        },
        extra_facts={"0001652044": data["successor"], "0001288776": data["predecessor"]},
    )


def test_a_date_before_the_current_registrant_existed_uses_its_predecessor(monkeypatch, tmp_path):
    sec = _alphabet(monkeypatch, tmp_path)

    old = sec.get_metrics("GOOGL", ["shares_outstanding", "market_cap"], as_of=datetime(2015, 6, 1, 10, tzinfo=NY), price=549.0)
    new = sec.get_metrics("GOOGL", ["shares_outstanding"], as_of=AFTER_Q1)

    assert old["cik"] == "0001288776" and old["reasons"] == {}
    assert old["values"]["shares_outstanding"] == 680_915_000  # Google Inc., quarter ended 2015-03-31
    assert old["values"]["market_cap"] == pytest.approx(549.0 * 680_915_000)
    assert new["cik"] == "0001652044"
    assert new["values"]["shares_outstanding"] == 12_381_000_000  # Alphabet, all classes, 2024-03-31


def test_the_table_names_the_company_so_two_tickers_of_one_company_are_not_counted_twice(monkeypatch, tmp_path):
    sec = _alphabet(monkeypatch, tmp_path)

    table = sec.get_metrics_table(["GOOGL", "GOOG"], ["market_cap"], as_of=AFTER_Q1, prices={"GOOGL": 174.0, "GOOG": 175.0})

    assert table.loc["GOOGL", "cik"] == table.loc["GOOG", "cik"] == "0001652044"
    assert table.loc["GOOGL", "market_cap"] == pytest.approx(174.0 * 12_381_000_000)  # the whole company, not one class
    assert list(table.drop_duplicates("cik").index) == ["GOOGL"]


def _share_classes(monkeypatch, tmp_path):
    import json

    companies = json.loads(_fixture_path("sec_companyfacts_share_classes.json").read_text())["companies"]
    covers = _fixture_path("sec_cover_shares")

    def index(*names):
        return json.dumps({"directory": {"item": [{"name": name} for name in names]}})

    documents = {
        "/000095017024053185/index.json": index("brka-20240331_htm.xml", "FilingSummary.xml"),
        "/000095017024053185/brka-20240331_htm.xml": (covers / "brk_2024q1.xml").read_text(),
        "/000119312515166595/index.json": index("brka-20150331.xml", "brka-20150331_cal.xml", "brka-20150331_lab.xml", "FilingSummary.xml"),
        "/000119312515166595/brka-20150331.xml": (covers / "brk_2015q1.xml").read_text(),
        "/000140316124000030/index.json": index("FilingSummary.xml", "v-20240331_cal.xml", "v-20240331_htm.xml"),
        "/000140316124000030/v-20240331_htm.xml": (covers / "v_2024q2.xml").read_text(),
    }
    return _sec(
        monkeypatch, tmp_path,
        extra_tickers={
            "30": {"ticker": "BRK-B", "cik_str": companies["BRK-B"]["cik"], "title": "Berkshire Hathaway Inc"},
            "31": {"ticker": "V", "cik_str": companies["V"]["cik"], "title": "Visa Inc."},
        },
        extra_facts={f"{c['cik']:010d}": c for c in companies.values()},
        documents=documents,
    )


def test_share_classes_filed_only_per_class_add_up_to_the_whole_company(monkeypatch, tmp_path):
    """Berkshire files Class A and Class B counts separately; one Class A share is 1,500 Class B shares."""
    sec = _share_classes(monkeypatch, tmp_path)

    now = sec.get_metrics("BRK-B", ["shares_outstanding", "market_cap"], as_of=AFTER_Q1, price=407.0)
    then = sec.get_metrics("BRK-B", ["shares_outstanding"], as_of=datetime(2015, 6, 1, 10, tzinfo=NY))

    assert now["values"]["shares_outstanding"] == 562_539 * 1500 + 1_311_384_883  # in Class B shares
    assert now["values"]["market_cap"] == pytest.approx(407.0 * 2_155_193_383)
    assert now["periods"]["shares_outstanding"]["accession_number"] == "0000950170-24-053185"
    # 2015: the filed weighted average (1,642,951) counts Class A equivalents and must not be used as B shares.
    assert then["values"]["shares_outstanding"] == 824_730 * 1500 + 1_227_452_053


def test_share_classes_without_a_known_conversion_are_added_one_for_one(monkeypatch, tmp_path):
    sec = _share_classes(monkeypatch, tmp_path)

    visa = sec.get_metrics("V", ["shares_outstanding"], as_of=AFTER_Q1)

    assert visa["values"]["shares_outstanding"] == 1_574_151_974 + 245_513_385 + 9_273_174
    filing_day = sec.get_metrics("V", ["shares_outstanding"], as_of=datetime(2024, 4, 24, 15, tzinfo=NY))
    assert filing_day["values"]["shares_outstanding"] is None  # that filing is not public until the next morning


# ---- compact facts: a small per-company file with only the facts the metrics use ----

class _FakeRemote:
    """Stands in for the shared backtest cache: a dict of remote key -> bytes."""

    enabled = True

    def __init__(self, store):
        self.store = store

    def ensure_local_file(self, local_path, remote_relative=None, **_):
        if remote_relative not in self.store:
            return False
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.write_bytes(self.store[remote_relative])
        return True

    def on_local_update(self, local_path, remote_relative=None, **_):
        self.store[remote_relative] = local_path.read_bytes()
        return True


def _recorded_with_ballast(monkeypatch, tmp_path, **sec_kwargs):
    """The recorded companies, each with many facts the metrics never read (as real files have)."""
    import json
    from pathlib import Path

    companies = json.loads((Path(__file__).parent / "fixtures" / "sec_companyfacts_coverage.json").read_text())["companies"]
    tickers = {str(i): {"ticker": s, "cik_str": c["cik"], "title": c["entityName"]} for i, (s, c) in enumerate(companies.items())}
    facts = {}
    for company in companies.values():
        us_gaap = dict(company["facts"].get("us-gaap", {}))
        template = next(iter(us_gaap.values()))
        for index in range(40):
            us_gaap[f"UnusedDisclosureDetail{index}"] = template
        facts[f"{company['cik']:010d}"] = {"cik": company["cik"], "facts": {**company["facts"], "us-gaap": us_gaap}}
    calls = []

    def fake_get(url, **kwargs):
        calls.append(url)
        if url.endswith("company_tickers.json"):
            return _Response(tickers)
        for cik, payload in facts.items():
            if url.endswith(f"companyfacts/CIK{cik}.json"):
                return _Response(payload)
        return _Response({"filings": {"recent": {"accessionNumber": [], "acceptanceDateTime": []}}})

    monkeypatch.setattr("lumibot.fundamentals.sec.requests.get", fake_get)

    def make(cache_dir, **kwargs):
        return SECFundamentals(
            cache_dir=cache_dir, min_request_interval_seconds=0, split_history=lambda _symbol: [],
            **{"remote_cache": None, **sec_kwargs, **kwargs},
        )

    return make, list(companies), calls


_COMPACT_DATES = [datetime(2019, 6, 3, 10, 0, tzinfo=NY), datetime(2022, 3, 1, 10, 0, tzinfo=NY), AFTER_Q1]


def _tables(sec, symbols):
    prices = {symbol: 50.0 for symbol in symbols}
    return [sec.get_metrics_table(symbols, as_of=day, prices=prices) for day in _COMPACT_DATES]


def test_compact_facts_give_the_same_tables_without_the_full_files(monkeypatch, tmp_path):
    make, symbols, calls = _recorded_with_ballast(monkeypatch, tmp_path)
    first = make(tmp_path)
    expected = _tables(first, symbols)
    statements = first.get_income_statement(symbols[0], as_of=AFTER_Q1)
    history = first.get_fact_history(symbols[0], "NetIncomeLoss", as_of=AFTER_Q1)

    full = sorted(p for p in (tmp_path / "companyfacts").glob("CIK*.json") if not p.name.endswith(".meta.json"))
    compact = sorted((tmp_path / "companyfacts_compact").glob("CIK*.json.gz"))
    assert len(compact) == len(full) == len(symbols)
    assert sum(path.stat().st_size for path in compact) * 5 < sum(path.stat().st_size for path in full)

    # A later run reads only the compact files: the full files are gone and the SEC is not asked.
    for path in full:
        path.unlink()
    calls.clear()
    second = make(tmp_path)
    for got, want in zip(_tables(second, symbols), expected):
        pd.testing.assert_frame_equal(got, want)
    assert second.get_income_statement(symbols[0], as_of=AFTER_Q1) == statements
    assert second.get_fact_history(symbols[0], "NetIncomeLoss", as_of=AFTER_Q1) == history
    assert not [url for url in calls if "companyfacts" in url]


def test_a_fact_outside_the_compact_set_still_comes_from_the_full_file(monkeypatch, tmp_path):
    make, symbols, calls = _recorded_with_ballast(monkeypatch, tmp_path)
    expected = make(tmp_path).get_fact_history(symbols[0], "UnusedDisclosureDetail3", as_of=AFTER_Q1)
    assert expected["values"]
    for path in (tmp_path / "companyfacts").glob("CIK*.json"):
        path.unlink()
    calls.clear()

    second = make(tmp_path)
    _tables(second, symbols[:1])
    assert not [url for url in calls if "companyfacts" in url]
    got = second.get_fact_history(symbols[0], "UnusedDisclosureDetail3", as_of=AFTER_Q1)

    assert got["values"] == expected["values"]
    assert len([url for url in calls if "companyfacts" in url]) == 1


def test_compact_facts_are_shared_through_the_remote_backtest_cache(monkeypatch, tmp_path):
    store = {}
    make, symbols, calls = _recorded_with_ballast(
        monkeypatch, tmp_path, cache_mode="backtest", backtest_end=datetime(2024, 12, 31, tzinfo=NY)
    )
    expected = _tables(make(tmp_path / "machine_a", remote_cache=_FakeRemote(store)), symbols)
    compact_keys = [key for key in store if key.startswith("sec/companyfacts_compact/")]
    assert len(compact_keys) == len(symbols)

    # Another machine: no local files; only the compact objects are downloaded.
    calls.clear()
    compact_only = _FakeRemote({key: store[key] for key in compact_keys})
    for got, want in zip(_tables(make(tmp_path / "machine_b", remote_cache=compact_only), symbols), expected):
        pd.testing.assert_frame_equal(got, want)
    assert not [url for url in calls if "companyfacts" in url]



# ---- speed: small derived files instead of large SEC files inside a backtest's loop ----

def _recording(monkeypatch):
    """Record every SEC request made from here on (wraps the fake already installed)."""
    import lumibot.fundamentals.sec as module

    inner, calls = module.requests.get, []

    def get(url, **kwargs):
        calls.append(url)
        return inner(url, **kwargs)

    monkeypatch.setattr("lumibot.fundamentals.sec.requests.get", get)
    return calls


def _machine(cache_dir, store):
    return SECFundamentals(
        cache_dir=cache_dir, min_request_interval_seconds=0, remote_cache=_FakeRemote(store),
        split_history=lambda _symbol: [], cache_mode="backtest", backtest_end=datetime(2024, 12, 31, tzinfo=NY),
    )


def test_cover_share_counts_are_read_from_a_filing_once_and_shared(monkeypatch, tmp_path):
    """The per-class counts of a filing are a few numbers: later runs must not download its XBRL again."""
    _share_classes(monkeypatch, tmp_path / "unused")
    calls = _recording(monkeypatch)
    store = {}
    asks = [("BRK-B", AFTER_Q1), ("BRK-B", datetime(2015, 6, 1, 10, tzinfo=NY)), ("V", AFTER_Q1)]

    first = _machine(tmp_path / "machine_a", store)
    expected = [first.get_metrics(symbol, ["shares_outstanding"], as_of=day) for symbol, day in asks]
    assert expected[0]["values"]["shares_outstanding"] == 562_539 * 1500 + 1_311_384_883
    assert [url for url in calls if url.endswith(".xml")]  # the first run reads the filings

    # Another machine: the filings themselves are not in the shared cache, only what was read from them.
    calls.clear()
    small = {key: value for key, value in store.items() if "/filings/" not in key}
    assert any(key.startswith("sec/cover_share_counts/") for key in small)
    second = _machine(tmp_path / "machine_b", small)
    got = [second.get_metrics(symbol, ["shares_outstanding"], as_of=day) for symbol, day in asks]

    assert [g["values"] for g in got] == [e["values"] for e in expected]
    assert [g["periods"] for g in got] == [e["periods"] for e in expected]
    assert not [url for url in calls if "/Archives/" in url or url.endswith((".xml", "index.json"))]


def test_acceptance_times_come_from_a_small_shared_file_with_the_same_answers(monkeypatch, tmp_path):
    """Exact filing times are two columns of a large submissions file: a later run reads only those."""
    from tests.test_sec_fundamentals_point_in_time import CIK as TIMING_CIK
    from tests.test_sec_fundamentals_point_in_time import _FakeSEC, _timing_facts, _timing_submissions

    fake = _FakeSEC(companyfacts={TIMING_CIK: _timing_facts()}, submissions={TIMING_CIK: _timing_submissions()})
    monkeypatch.setattr("lumibot.fundamentals.sec.requests.get", fake)
    store = {}
    moments = [
        datetime(2024, 2, 1, 17, 30, tzinfo=NY),  # after hours on a filing day
        datetime(2024, 2, 2, 9, 30, tzinfo=NY),
        datetime(2024, 11, 1, 9, 30, tzinfo=NY),  # accepted 7:00 AM: visible at that open
    ]

    def revenue(sec):
        return [sec.get_income_statement("EXM", as_of=moment)["values"]["revenue"]["value"] for moment in moments]

    def machine(name, remote):
        return SECFundamentals(
            cache_dir=tmp_path / name, min_request_interval_seconds=0, remote_cache=_FakeRemote(remote),
            split_history=lambda _symbol: [], cache_mode="backtest", backtest_end=datetime(2024, 12, 31, tzinfo=NY),
        )

    expected = revenue(machine("machine_a", store))
    assert expected == [1, 2, 4] and fake.count("submissions") == 1

    fake.calls.clear()
    without_submissions = {key: value for key, value in store.items() if not key.startswith("sec/submissions/")}
    assert any(key.startswith("sec/submissions_acceptance/") for key in without_submissions)
    assert revenue(machine("machine_b", without_submissions)) == expected
    assert fake.count("submissions") == 0


def test_a_universe_table_loads_needed_acceptance_times_before_the_company_loop(monkeypatch, tmp_path):
    """Same table; the acceptance files a date needs are requested together, not one company after another."""
    from tests.test_sec_fundamentals_point_in_time import _FakeSEC

    symbols = [f"S{index}" for index in range(6)]
    tickers = {str(i): {"ticker": s, "cik_str": 900 + i, "title": s} for i, s in enumerate(symbols)}
    facts, submissions = {}, {}
    for i, _symbol in enumerate(symbols):
        cik = f"{900 + i:010d}"
        payload = _facts()
        payload["cik"] = 900 + i
        facts[cik] = payload
        submissions[cik] = {"filings": {"recent": {"accessionNumber": [], "acceptanceDateTime": []}}}
    fake = _FakeSEC(companyfacts=facts, submissions=submissions, tickers=tickers, delay=0.05)
    monkeypatch.setattr("lumibot.fundamentals.sec.requests.get", fake)
    day = datetime(2024, 5, 3, 15, 0, tzinfo=NY)  # a filing day: exact times matter for every company
    metrics = ["ttm_revenue", "shares_outstanding"]

    def new(name):
        return SECFundamentals(cache_dir=tmp_path / name, min_request_interval_seconds=0, remote_cache=None,
                               split_history=lambda _symbol: [])

    sec = new("a")
    sec.preload(symbols)
    fake.max_active = 0
    got = sec.get_metrics_table(symbols, metrics, as_of=day, preload=False)

    assert fake.count("submissions") == len(symbols)
    assert fake.max_active > 1, "acceptance times were requested one company at a time"
    one_by_one = new("b")
    for symbol in symbols:
        row = one_by_one.get_metrics(symbol, metrics, as_of=day)["values"]
        assert got.loc[symbol, "ttm_revenue"] == row["ttm_revenue"]
        assert got.loc[symbol, "shares_outstanding"] == row["shares_outstanding"]
