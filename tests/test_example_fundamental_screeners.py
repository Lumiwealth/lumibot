"""The two fundamentals screener examples run a whole backtest on synthetic data.

Four made-up companies, each with a FY2022 and a FY2023 annual report (filed 2023-02-10 and
2024-02-09) and daily prices for the first half of 2024. Nothing talks to the SEC or to a
price vendor.

| Symbol | Shares | Price Jan-Mar | Price Apr-Jun | Revenue growth | Debt / equity |
| AAA    | 100    | 10            | 10            | 50%            | 0.2           |
| BBB    | 100    | 5             | 30            | 30%            | 0.9           |
| CCC    | 100    | 8             | 8             | 25%            | 0.1           |
| DDD    | 100    | 2             | 2             | 5%             | 0.1           |
"""

from datetime import datetime

import pandas as pd
import pytest

from lumibot.backtesting import PandasDataBacktesting
from lumibot.entities import Asset, Data
from lumibot.example_strategies.stock_revenue_growth_low_debt import RevenueGrowthLowDebt
from lumibot.example_strategies.stock_top_market_cap import TopMarketCap

QUOTE = Asset("USD", Asset.AssetType.FOREX)
COMPANIES = {  # symbol: (cik, price before April, price from April, FY22 revenue, FY23 revenue, debt, equity)
    "AAA": (1001, 10.0, 10.0, 100, 150, 20, 100),
    "BBB": (1002, 5.0, 30.0, 100, 130, 90, 100),
    "CCC": (1003, 8.0, 8.0, 100, 125, 10, 100),
    "DDD": (1004, 2.0, 2.0, 100, 105, 10, 100),
}


def _row(accn, filed, fy, val, end, start=None):
    row = {"val": val, "accn": accn, "filed": filed, "form": "10-K", "fp": "FY", "fy": fy, "end": end}
    if start:
        row["start"] = start
    return row


def _facts(cik, rev22, rev23, debt, equity):
    k22, k23 = ("k22", "2023-02-10", 2022), ("k23", "2024-02-09", 2023)
    revenue = [_row(*k22, rev22, "2022-12-31", "2022-01-01"), _row(*k23, rev23, "2023-12-31", "2023-01-01")]
    return {
        "cik": cik,
        "facts": {
            "us-gaap": {
                "Revenues": {"units": {"USD": revenue}},
                "LongTermDebt": {"units": {"USD": [_row(*k23, debt, "2023-12-31")]}},
                "StockholdersEquity": {"units": {"USD": [_row(*k23, equity, "2023-12-31")]}},
            },
            "dei": {"EntityCommonStockSharesOutstanding": {"units": {"shares": [_row(*k23, 100, "2024-01-31")]}}},
        },
    }


class _Response:
    status_code = 200
    headers = {}
    text = ""

    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


def _prices():
    index = pd.date_range("2024-01-02", "2024-06-28", freq="B", tz="America/New_York")
    data = {}
    for symbol, (_cik, early, late, *_rest) in COMPANIES.items():
        close = [early if day < pd.Timestamp("2024-04-01", tz="America/New_York") else late for day in index]
        frame = pd.DataFrame({"open": close, "high": close, "low": close, "close": close, "volume": 1e6}, index=index)
        asset = Asset(symbol)
        data[(asset, QUOTE)] = Data(asset, frame, timestep="day", quote=QUOTE)
    return data


@pytest.fixture
def synthetic_market(monkeypatch, tmp_path):
    from lumibot.tools import backtest_cache
    from lumibot.tools.yahoo_helper import YahooHelper

    tickers = {str(i): {"ticker": s, "cik_str": v[0], "title": s} for i, (s, v) in enumerate(COMPANIES.items())}
    facts = {f"CIK{v[0]:010d}.json": _facts(v[0], *v[3:]) for v in COMPANIES.values()}

    def fake_get(url, **_kwargs):
        if url.endswith("company_tickers.json"):
            return _Response(tickers)
        if "submissions" in url:
            return _Response({"filings": {"recent": {"accessionNumber": [], "acceptanceDateTime": []}}})
        return _Response(facts[url.rsplit("/", 1)[-1]])

    disabled = backtest_cache.BacktestCacheManager(None)
    monkeypatch.setattr(backtest_cache, "get_backtest_cache", lambda: disabled)
    monkeypatch.setattr("lumibot.fundamentals.sec.requests.get", fake_get)
    monkeypatch.setattr(YahooHelper, "get_symbol_splits", staticmethod(lambda _symbol: None))
    monkeypatch.setenv("LUMIBOT_SEC_CACHE_DIR", str(tmp_path / "sec"))
    monkeypatch.setenv("BACKTESTING_DATA_SOURCE", "none")
    monkeypatch.chdir(tmp_path)


def _run(strategy_class, parameters):
    _results, strategy = strategy_class.run_backtest(
        PandasDataBacktesting, datetime(2024, 3, 1), datetime(2024, 6, 27), pandas_data=_prices(),
        benchmark_asset=None, show_plot=False, show_tearsheet=False, save_tearsheet=False,
        show_indicators=False, quiet_logs=True, show_progress_bar=False, budget=10_000, parameters=parameters)
    return {p.asset.symbol: float(p.quantity) for p in strategy.get_positions() if p.asset.asset_type == "stock" and p.quantity}


def test_top_market_cap_example_follows_the_largest_company(synthetic_market):
    held = _run(TopMarketCap, {"universe": list(COMPANIES), "top_n": 1})

    # March: AAA is largest (100 x 10). From April BBB is (100 x 30), so the monthly rebalance moves there.
    assert list(held) == ["BBB"] and held["BBB"] > 0


def test_revenue_growth_low_debt_example_holds_only_companies_that_pass(synthetic_market):
    held = _run(RevenueGrowthLowDebt, {"universe": list(COMPANIES), "min_revenue_growth": 0.20, "max_debt_to_equity": 0.5, "top_n": 5})

    # BBB grows 30% but debt is 0.9x equity; DDD grows only 5%.
    assert sorted(held) == ["AAA", "CCC"]
