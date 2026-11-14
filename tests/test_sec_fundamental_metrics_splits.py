"""Price ratios across stock splits use one share basis.

Filings report EPS and share counts on the share basis in force when they were filed;
prices trade on the basis of the day (or come split-adjusted from backtest data). Before
this fix a filing made before a split was combined with a post-split price, so P/E, P/S,
P/B and market cap were off by the split ratio until the next filing, and a TTM built from
a pre-split 10-K plus a post-split 10-Q mixed both bases.

Synthetic filer, calendar fiscal year, split effective 2024-06-10 with ratio ``r``
(2.0 = 2-for-1, 0.5 = 1-for-2 reverse):

* FY2023 10-K filed 2024-02-09: FY2023 diluted EPS 10, weighted diluted shares 1,000
* Q1 2024 10-Q filed 2024-05-03: Q1 2024 EPS 3, Q1 2023 EPS 2, shares outstanding 1,000
* split on 2024-06-10
* H1 2024 10-Q filed 2024-08-02, on the new basis: H1 2024 EPS 6/r, H1 2023 EPS 4.5/r,
  Q1 2024 restated to 3/r with 1,000*r shares, shares outstanding 1,000*r
* FY2024 10-K filed 2025-02-07: FY2023 EPS restated to 10/r

The price as traded is 220 before the split and 220/r after it, so the company is worth
220,000 throughout. Everything is synthetic; nothing talks to the SEC or Yahoo.
"""

from datetime import date, datetime
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from lumibot.fundamentals import SECFundamentals

NY = ZoneInfo("America/New_York")
CIK = "0000333333"
SPLIT_DAY = date(2024, 6, 10)
BEFORE_SPLIT = datetime(2024, 6, 7, 11, 0, tzinfo=NY)
AFTER_SPLIT = datetime(2024, 6, 12, 11, 0, tzinfo=NY)
AFTER_NEXT_10Q = datetime(2024, 8, 5, 11, 0, tzinfo=NY)

FILINGS = {
    "k23": ("2024-02-09", "10-K", "FY", 2023),
    "q124": ("2024-05-03", "10-Q", "Q1", 2024),
    "q224": ("2024-08-02", "10-Q", "Q2", 2024),
    "k24": ("2025-02-07", "10-K", "FY", 2024),
}
FY23 = ("2023-01-01", "2023-12-31")
FY24 = ("2024-01-01", "2024-12-31")
Q1_23 = ("2023-01-01", "2023-03-31")
Q1_24 = ("2024-01-01", "2024-03-31")
H1_23 = ("2023-01-01", "2023-06-30")
H1_24 = ("2024-01-01", "2024-06-30")


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


def _r(accn, period, val):
    filed, form, fp, fy = FILINGS[accn]
    row = {"val": val, "accn": accn, "filed": filed, "form": form, "fp": fp, "fy": fy}
    if isinstance(period, tuple):
        row["start"], row["end"] = period
    else:
        row["end"] = period
    return row


def _facts(r: float, *, restated_without_split: bool = False):
    eps = [
        _r("k23", FY23, 10.0),
        _r("q124", Q1_24, 3.0), _r("q124", Q1_23, 2.0),
        _r("q224", H1_24, 6.0 / r), _r("q224", H1_23, 4.5 / r), _r("q224", Q1_24, 3.0 / r),
        _r("k24", FY24, 14.0 / r), _r("k24", FY23, 10.0 / r),
    ]
    shares_after = 1000.0 if restated_without_split else 1000.0 * r
    weighted = [
        _r("k23", FY23, 1000.0),
        _r("q124", Q1_24, 1000.0),
        _r("q224", Q1_24, shares_after),
        _r("k24", FY23, shares_after),
    ]
    revenue = [
        _r("k23", FY23, 100_000.0),
        _r("q124", Q1_24, 30_000.0), _r("q124", Q1_23, 20_000.0),
        _r("q224", H1_24, 60_000.0), _r("q224", H1_23, 45_000.0),
    ]
    return {
        "cik": int(CIK),
        "facts": {
            "us-gaap": {
                "EarningsPerShareDiluted": {"units": {"USD/shares": eps}},
                "WeightedAverageNumberOfDilutedSharesOutstanding": {"units": {"shares": weighted}},
                "RevenueFromContractWithCustomerExcludingAssessedTax": {"units": {"USD": revenue}},
                "StockholdersEquity": {"units": {"USD": [
                    _r("k23", "2023-12-31", 50_000.0), _r("q124", "2024-03-31", 60_000.0),
                    _r("q224", "2024-06-30", 70_000.0),
                ]}},
            },
            "dei": {
                "EntityCommonStockSharesOutstanding": {"units": {"shares": [
                    _r("k23", "2024-01-31", 1000.0),
                    _r("q124", "2024-04-25", 1000.0),
                    _r("q224", "2024-07-26", shares_after),
                ]}},
            },
        },
    }


def _sec(monkeypatch, tmp_path, facts, **kwargs):
    tickers = {"0": {"ticker": "SPL", "cik_str": int(CIK), "title": "Split Example"}}

    def fake_get(url, **_kwargs):
        if url.endswith("company_tickers.json"):
            return _Response(tickers)
        if url.endswith(f"companyfacts/CIK{CIK}.json"):
            return _Response(facts)
        if "submissions" in url:
            return _Response({"filings": {"recent": {"accessionNumber": [], "acceptanceDateTime": []}}})
        return _Response({}, status_code=404)

    monkeypatch.setattr("lumibot.fundamentals.sec.requests.get", fake_get)
    kwargs.setdefault("split_history", lambda _symbol: [])
    return SECFundamentals(cache_dir=tmp_path, min_request_interval_seconds=0, remote_cache=None, **kwargs)


def _traded_price(r, when):
    return 220.0 if when.date() < SPLIT_DAY else 220.0 / r


@pytest.mark.parametrize("r", [2.0, 0.5], ids=["2-for-1", "1-for-2-reverse"])
@pytest.mark.parametrize("when", [BEFORE_SPLIT, AFTER_SPLIT], ids=["before-split", "after-split-before-next-filing"])
def test_ratios_stay_on_one_share_basis_around_a_split(monkeypatch, tmp_path, r, when):
    sec = _sec(monkeypatch, tmp_path, _facts(r), split_history=lambda _s: [(SPLIT_DAY, r)])
    price = _traded_price(r, when)
    result = sec.get_metrics("SPL", as_of=when, price=price)
    values = result["values"]

    shares = 1000.0 if when.date() < SPLIT_DAY else 1000.0 * r
    ttm_eps = 11.0 if when.date() < SPLIT_DAY else 11.0 / r  # 10 + 3 - 2 on the pre-split basis

    assert values["ttm_eps_diluted"] == pytest.approx(ttm_eps)
    assert values["shares_outstanding"] == pytest.approx(shares)
    assert values["market_cap"] == pytest.approx(220_000.0)
    assert values["pe_ratio"] == pytest.approx(20.0)
    assert values["book_value_per_share"] == pytest.approx(60_000.0 / shares)
    assert values["pb_ratio"] == pytest.approx(220_000.0 / 60_000.0)
    assert values["ps_ratio"] == pytest.approx(220_000.0 / 110_000.0)


@pytest.mark.parametrize("r", [2.0, 0.5], ids=["2-for-1", "1-for-2-reverse"])
def test_ttm_never_mixes_a_pre_split_10k_with_a_post_split_10q(monkeypatch, tmp_path, r):
    # No split history is needed here: the 10-K (pre-split) and the 10-Q (post-split) bracket
    # the split, so SEC data alone proves it happened in between.
    sec = _sec(monkeypatch, tmp_path, _facts(r), split_history=None)
    values = sec.get_metrics("SPL", as_of=AFTER_NEXT_10Q, price=220.0 / r)["values"]

    ttm_eps = 11.5 / r  # 10 + 6 - 4.5 on the pre-split basis, converted to the new basis
    assert values["ttm_eps_diluted"] == pytest.approx(ttm_eps)
    assert values["pe_ratio"] == pytest.approx((220.0 / r) / ttm_eps)
    assert values["market_cap"] == pytest.approx(220_000.0)


def test_an_undatable_split_gives_none_with_a_reason_never_a_wrong_number(monkeypatch, tmp_path):
    sec = _sec(monkeypatch, tmp_path, _facts(2.0), split_history=None)
    result = sec.get_metrics("SPL", as_of=AFTER_SPLIT, price=110.0)

    assert result["values"]["pe_ratio"] is None
    assert result["values"]["market_cap"] is None
    assert "split" in result["reasons"]["pe_ratio"]
    assert "2-for-1" in result["reasons"]["market_cap"]


def test_an_eps_restatement_without_a_share_change_is_not_a_split(monkeypatch, tmp_path):
    # Q1 2024 EPS is restated from 3 to 1.5 but the weighted share count does not move: an
    # accounting restatement, not a split. Nothing is converted and nothing goes None.
    sec = _sec(monkeypatch, tmp_path, _facts(2.0, restated_without_split=True), split_history=None)
    values = sec.get_metrics("SPL", as_of=AFTER_SPLIT, price=220.0)["values"]

    assert values["shares_outstanding"] == pytest.approx(1000.0)
    assert values["ttm_eps_diluted"] == pytest.approx(11.0)
    assert values["pe_ratio"] == pytest.approx(20.0)


class _DataSource:
    """Backtest price data that is split-adjusted to the post-split basis."""

    def __init__(self, r):
        days = pd.date_range("2024-05-01", "2024-07-31", freq="B", tz=NY)
        self.frame = pd.DataFrame(
            {
                "close": [220.0 / r] * len(days),  # adjusted: no jump at the split
                "stock_splits": [r if day.date() == SPLIT_DAY else 0.0 for day in days],
            },
            index=days,
        )

    def _get_backtest_daily_corporate_action_frame(self, asset, quote=None):
        return self.frame

    def should_apply_stock_splits_to_positions(self, asset, quote=None):
        return False


class _Strategy:
    is_backtesting = True
    _backtesting_end = datetime(2024, 12, 31, tzinfo=NY)

    def __init__(self, r, when):
        self.broker = type("Broker", (), {})()
        self.broker.data_source = _DataSource(r)
        self._when = when
        self._r = r

    def get_datetime(self):
        return self._when

    def get_last_price(self, asset):
        return 220.0 / self._r  # adjusted price, whatever the day


@pytest.mark.parametrize("r", [2.0, 0.5], ids=["2-for-1", "1-for-2-reverse"])
@pytest.mark.parametrize("when", [BEFORE_SPLIT, AFTER_SPLIT], ids=["before-split", "after-split"])
def test_split_adjusted_backtest_prices_are_converted_to_the_price_traded_that_day(monkeypatch, tmp_path, r, when):
    strategy = _Strategy(r, when)
    sec = _sec(monkeypatch, tmp_path, _facts(r), strategy=strategy, split_history=None)
    result = sec.get_metrics("SPL")

    assert result["price"] == pytest.approx(_traded_price(r, when))
    assert result["values"]["pe_ratio"] == pytest.approx(20.0)
    assert result["values"]["market_cap"] == pytest.approx(220_000.0)
