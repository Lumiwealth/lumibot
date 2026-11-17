"""Market cap as of a date is right, or it is None with a reason. Never a wrong number.

Market cap = shares outstanding on the share basis of the date x the price traded that day.
Ranking a large universe by market cap exposed four ways that rule was broken:

1. A filer reports the cover-page share count at the wrong scale (a thousand or a million
   times too large). The number was used as filed, so a mid-size company ranked first.
2. Backtest prices adjusted for splits and dividends, from a data source with no split
   history, were either refused (None for every company that split later) or used as if
   they were the prices traded that day (too small by every later dividend).
3. An accounting restatement of EPS was read as a stock split when one of the restated
   periods had no share count to compare.
4. A share-class ticker written with a dot ("AB.B") was not found: the SEC ticker file
   writes it with a dash ("AB-B").

Everything here is made up: no SEC, Yahoo or price provider data and no network.
"""

from datetime import date, datetime
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from lumibot.fundamentals import SECFundamentals

NY = ZoneInfo("America/New_York")
CIK = "0000444444"

FILINGS = {
    "k23": ("2024-02-09", "10-K", "FY", 2023),
    "q124": ("2024-05-03", "10-Q", "Q1", 2024),
    "q224": ("2024-08-02", "10-Q", "Q2", 2024),
    "q324": ("2024-11-01", "10-Q", "Q3", 2024),
    "k24": ("2025-02-07", "10-K", "FY", 2024),
}
FY23 = ("2023-01-01", "2023-12-31")
Q1_23 = ("2023-01-01", "2023-03-31")
Q1_24 = ("2024-01-01", "2024-03-31")
Q2_24 = ("2024-04-01", "2024-06-30")
Q3_24 = ("2024-07-01", "2024-09-30")
AFTER_Q3 = datetime(2024, 11, 5, 11, 0, tzinfo=NY)


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


def _payload(*, cover=(), balance=(), weighted_basic=(), eps=(), weighted_diluted=()):
    gaap = {}
    if balance:
        gaap["CommonStockSharesOutstanding"] = {"units": {"shares": list(balance)}}
    if weighted_basic:
        gaap["WeightedAverageNumberOfSharesOutstandingBasic"] = {"units": {"shares": list(weighted_basic)}}
    if eps:
        gaap["EarningsPerShareDiluted"] = {"units": {"USD/shares": list(eps)}}
    if weighted_diluted:
        gaap["WeightedAverageNumberOfDilutedSharesOutstanding"] = {"units": {"shares": list(weighted_diluted)}}
    facts = {"us-gaap": gaap}
    if cover:
        facts["dei"] = {"EntityCommonStockSharesOutstanding": {"units": {"shares": list(cover)}}}
    return {"cik": int(CIK), "facts": facts}


def _sec(monkeypatch, tmp_path, facts, *, ticker="CAP", **kwargs):
    tickers = {"0": {"ticker": ticker, "cik_str": int(CIK), "title": "Market Cap Example"}}

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


# -- 1. a share count filed at the wrong scale ------------------------------------------------


@pytest.mark.parametrize("scale", [1_000.0, 1_000_000.0, 0.001], ids=["x1000", "x1000000", "div1000"])
def test_a_cover_share_count_filed_at_the_wrong_scale_is_not_used(monkeypatch, tmp_path, scale):
    # Three filings report about 400 million shares. The Q3 cover page then reports the
    # count at the wrong scale. The balance sheet of the same filing and the weighted
    # average both still say about 380 million, so the cover number is a filing error.
    facts = _payload(
        cover=[
            _r("k23", "2024-01-31", 400e6), _r("q124", "2024-04-25", 395e6),
            _r("q224", "2024-07-26", 390e6), _r("q324", "2024-10-25", 380e6 * scale),
        ],
        balance=[_r("q224", "2024-06-30", 391e6), _r("q324", "2024-09-30", 381e6)],
        weighted_basic=[_r("q224", Q2_24, 392e6), _r("q324", Q3_24, 385e6)],
    )
    sec = _sec(monkeypatch, tmp_path, facts)
    result = sec.get_metrics("CAP", ["shares_outstanding", "market_cap"], as_of=AFTER_Q3, price=50.0)

    # The latest filed count that agrees with the company's other filed counts is used.
    assert result["values"]["shares_outstanding"] == pytest.approx(381e6)
    assert result["values"]["market_cap"] == pytest.approx(381e6 * 50.0)
    assert result["periods"]["shares_outstanding"]["end"] == "2024-09-30"


def test_a_real_jump_in_the_share_count_is_kept(monkeypatch, tmp_path):
    # A company issues 60 times more shares in the quarter (a merger). Earlier filings
    # disagree with the new count, but the weighted average of the same quarter agrees.
    facts = _payload(
        cover=[
            _r("k23", "2024-01-31", 10e6), _r("q124", "2024-04-25", 10e6),
            _r("q224", "2024-07-26", 10e6), _r("q324", "2024-10-25", 600e6),
        ],
        weighted_basic=[_r("q224", Q2_24, 10e6), _r("q324", Q3_24, 400e6)],
    )
    sec = _sec(monkeypatch, tmp_path, facts)

    assert sec.get_metric("CAP", "shares_outstanding", as_of=AFTER_Q3) == pytest.approx(600e6)


def test_a_share_count_that_nothing_else_confirms_is_none_with_a_reason(monkeypatch, tmp_path):
    # Only one cover count has ever been filed and it is 1,000 times the weighted average.
    # Either could be the wrong one, so there is no market cap rather than a guess.
    facts = _payload(
        cover=[_r("q324", "2024-10-25", 380e9)],
        weighted_basic=[_r("q324", Q3_24, 385e6)],
    )
    sec = _sec(monkeypatch, tmp_path, facts)
    result = sec.get_metrics("CAP", ["shares_outstanding", "market_cap"], as_of=AFTER_Q3, price=50.0)

    assert result["values"]["shares_outstanding"] is None
    assert result["values"]["market_cap"] is None
    assert "scale" in result["reasons"]["market_cap"]


# -- 3. an EPS restatement is not a split -----------------------------------------------------


def test_a_restatement_is_not_a_split_when_one_restated_period_has_no_share_count(monkeypatch, tmp_path):
    # A new accounting rule restates EPS by 25% in the next filings. The share counts filed
    # for the same periods do not move (1,000 before and after), which proves it is not a
    # split. One restated quarter has no share count in either filing: it used to be read as
    # a 4-for-5 split, and every price ratio went None until the "split" window had passed.
    eps = [
        _r("k23", FY23, 8.0), _r("k24", FY23, 10.0),      # restated; share count unchanged
        _r("q124", Q1_23, 2.0), _r("q224", Q1_23, 2.5),   # restated; no share count filed
    ]
    weighted_diluted = [_r("k23", FY23, 1000.0), _r("k24", FY23, 1000.0)]
    facts = _payload(
        cover=[_r("k23", "2024-01-31", 1000.0), _r("q124", "2024-04-25", 1000.0)],
        eps=eps,
        weighted_diluted=weighted_diluted,
    )
    sec = _sec(monkeypatch, tmp_path, facts, split_history=None)
    when = datetime(2024, 5, 6, 11, 0, tzinfo=NY)
    result = sec.get_metrics("CAP", ["shares_outstanding", "market_cap"], as_of=when, price=220.0)

    assert result["values"]["shares_outstanding"] == pytest.approx(1000.0)
    assert result["values"]["market_cap"] == pytest.approx(220_000.0)


# -- 4. share-class tickers -------------------------------------------------------------------


@pytest.mark.parametrize("asked", ["AB.B", "AB/B", "AB B", "ab.b", "AB-B"])
def test_a_share_class_ticker_is_found_however_the_class_is_written(monkeypatch, tmp_path, asked):
    facts = _payload(cover=[_r("q324", "2024-10-25", 1000.0)])
    sec = _sec(monkeypatch, tmp_path, facts, ticker="AB-B")

    assert sec.ticker_to_cik(asked) == CIK
    table = sec.get_metrics_table([asked], ["market_cap"], as_of=AFTER_Q3, prices={asked.upper(): 10.0})
    assert table["market_cap"].iloc[0] == pytest.approx(10_000.0)


def test_an_unknown_ticker_is_still_reported_as_unknown(monkeypatch, tmp_path):
    sec = _sec(monkeypatch, tmp_path, _payload(cover=[_r("q324", "2024-10-25", 1000.0)]), ticker="AB-B")

    table = sec.get_metrics_table(["AB.C"], ["market_cap"], as_of=AFTER_Q3, prices={"AB.C": 10.0})
    assert table["market_cap"].iloc[0] is None
    assert "no SEC data" in table["reasons"].iloc[0]["market_cap"]


# -- 2. adjusted backtest prices --------------------------------------------------------------

SPLIT_DAY = date(2024, 6, 10)          # 4-for-1
DIVIDEND_DAY = date(2024, 9, 16)       # prices before it are also adjusted down by 20%


def _split_facts():
    """1,000 shares at 220 before the split, 4,000 shares at 55 after it: worth 220,000 throughout."""
    return _payload(
        cover=[
            _r("k23", "2024-01-31", 1000.0), _r("q124", "2024-04-25", 1000.0),
            _r("q224", "2024-07-26", 4000.0), _r("q324", "2024-10-25", 4000.0),
        ],
        eps=[_r("q124", Q1_24, 4.0), _r("q224", Q1_24, 1.0)],
        weighted_diluted=[_r("q124", Q1_24, 1000.0), _r("q224", Q1_24, 4000.0)],
    )


def _traded(day):
    return 220.0 if day < SPLIT_DAY else 55.0


def _adjusted(day):
    return 55.0 * (0.8 if day < DIVIDEND_DAY else 1.0)


class _AdjustedSource:
    """Backtest prices adjusted for splits and dividends, with no split history.

    The source can also say what each day's close was as traded (the only way to undo a
    dividend adjustment exactly).
    """

    def __init__(self, known=("CAP",)):
        self.known = set(known)
        self.prefetched = []
        self.asked = []

    def should_apply_stock_splits_to_positions(self, asset, quote=None):
        return False

    def prefetch_backtest_as_traded_daily_closes(self, assets):
        self.prefetched.append([asset.symbol for asset in assets])

    def get_backtest_as_traded_daily_closes(self, asset, quote=None):
        self.asked.append(asset.symbol)
        if asset.symbol not in self.known:
            return None
        days = pd.date_range("2024-03-01", "2024-12-31", freq="B", tz=NY)
        return pd.DataFrame(
            {
                "close": [_adjusted(day.date()) for day in days],
                "as_traded_close": [_traded(day.date()) for day in days],
            },
            index=days,
        )


class _Strategy:
    is_backtesting = True
    _backtesting_end = datetime(2024, 12, 31, tzinfo=NY)

    def __init__(self, when, source=None):
        self.broker = type("Broker", (), {})()
        self.broker.data_source = source or _AdjustedSource()
        self._when = when

    def get_datetime(self):
        return self._when

    def get_last_price(self, asset):
        return _adjusted(self._when.date())


@pytest.mark.parametrize(
    "when, shares",
    [
        (datetime(2024, 5, 20, 11, 0, tzinfo=NY), 1000.0),   # before the split
        (datetime(2024, 6, 10, 11, 0, tzinfo=NY), 4000.0),   # the split day itself
        (datetime(2024, 6, 12, 11, 0, tzinfo=NY), 4000.0),   # after the split, before the next filing
        (datetime(2024, 8, 5, 11, 0, tzinfo=NY), 4000.0),    # next filing out, dividend still ahead
        (datetime(2024, 11, 5, 11, 0, tzinfo=NY), 4000.0),   # nothing left to adjust
    ],
    ids=["before-split", "split-day", "after-split", "after-next-filing", "after-dividend"],
)
def test_adjusted_backtest_prices_become_the_price_traded_that_day(monkeypatch, tmp_path, when, shares):
    strategy = _Strategy(when)
    sec = _sec(monkeypatch, tmp_path, _split_facts(), strategy=strategy, split_history=None)
    result = sec.get_metrics("CAP", ["shares_outstanding", "market_cap"])

    assert result["price"] == pytest.approx(_traded(when.date()))
    assert result["values"]["shares_outstanding"] == pytest.approx(shares)
    assert result["values"]["market_cap"] == pytest.approx(220_000.0)


def test_adjusted_prices_without_an_as_traded_price_give_none_with_a_reason(monkeypatch, tmp_path):
    # The source has no as-traded close for this symbol and no split history: the adjusted
    # price cannot be turned into the price traded before the split.
    when = datetime(2024, 5, 20, 11, 0, tzinfo=NY)
    strategy = _Strategy(when, _AdjustedSource(known=()))
    sec = _sec(monkeypatch, tmp_path, _split_facts(), strategy=strategy, split_history=None)
    result = sec.get_metrics("CAP", ["market_cap"])

    assert result["values"]["market_cap"] is None
    assert "split" in result["reasons"]["market_cap"]


def test_the_table_asks_the_price_source_for_as_traded_closes_once_for_the_whole_universe(monkeypatch, tmp_path):
    when = datetime(2024, 11, 5, 11, 0, tzinfo=NY)
    source = _AdjustedSource()
    sec = _sec(monkeypatch, tmp_path, _split_facts(), strategy=_Strategy(when, source), split_history=None)
    table = sec.get_metrics_table(["CAP", "NOPE"], ["market_cap"])

    assert source.prefetched == [["CAP", "NOPE"]]
    assert table.loc["CAP", "market_cap"] == pytest.approx(220_000.0)
    assert pd.isna(table.loc["NOPE", "market_cap"])


def test_metrics_that_need_no_price_do_not_ask_for_as_traded_closes(monkeypatch, tmp_path):
    when = datetime(2024, 11, 5, 11, 0, tzinfo=NY)
    source = _AdjustedSource()
    sec = _sec(monkeypatch, tmp_path, _split_facts(), strategy=_Strategy(when, source), split_history=None)
    sec.get_metrics_table(["CAP"], ["shares_outstanding"])

    assert source.prefetched == []
