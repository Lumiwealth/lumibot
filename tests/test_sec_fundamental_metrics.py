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
    def __init__(self, payload=None, status_code=200):
        self._payload = payload
        self.status_code = status_code
        self.headers = {}
        self.text = ""

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


def _sec(monkeypatch, tmp_path, facts=None, *, strategy=None, extra_tickers=None, extra_facts=None):
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
