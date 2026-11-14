"""A ticker now mapped to a registrant with no history before the date asked for.

The SEC ticker file is today's. When a company reorganizes under a new holding company,
its ticker moves to the new registrant's CIK, which has no filings before the
reorganization. SEC data does not link the new CIK to its predecessor, so LumiBot does not
guess one: it says plainly why there is no data instead of a pile of "not filed" reasons.
Synthetic data only.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from lumibot.fundamentals import SECFundamentals

NY = ZoneInfo("America/New_York")
NEW_CIK = "0000444444"


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


def _row(val, start, end, filed, accn="h1"):
    return {"val": val, "start": start, "end": end, "accn": accn, "filed": filed, "form": "10-Q", "fp": "Q3", "fy": 2026}


FACTS = {
    "cik": int(NEW_CIK),
    "facts": {
        "us-gaap": {
            # The new holding company's first report, filed 2026-11-02, includes comparatives
            # for 2025; they only became public in 2026.
            "NetIncomeLoss": {"units": {"USD": [
                _row(30.0, "2026-01-01", "2026-09-30", "2026-11-02"),
                _row(25.0, "2025-01-01", "2025-09-30", "2026-11-02"),
            ]}},
        }
    },
}


@pytest.fixture
def sec(monkeypatch, tmp_path):
    tickers = {"0": {"ticker": "HLD", "cik_str": int(NEW_CIK), "title": "Example Holdings Corp"}}

    def fake_get(url, **kwargs):
        if url.endswith("company_tickers.json"):
            return _Response(tickers)
        if url.endswith(f"companyfacts/CIK{NEW_CIK}.json"):
            return _Response(FACTS)
        if "submissions" in url:
            return _Response({"name": "Example Holdings Corp", "formerNames": [],
                              "filings": {"recent": {"accessionNumber": [], "acceptanceDateTime": []}}})
        return _Response({}, status_code=404)

    monkeypatch.setattr("lumibot.fundamentals.sec.requests.get", fake_get)
    return SECFundamentals(cache_dir=tmp_path, min_request_interval_seconds=0, remote_cache=None,
                           split_history=lambda _s: [])


def test_metrics_before_the_new_registrant_existed_explain_the_ticker_map(sec):
    result = sec.get_metrics("HLD", as_of=datetime(2025, 6, 2, 10, tzinfo=NY), price=100.0)

    assert all(value is None for value in result["values"].values())
    reason = result["reasons"]["ttm_net_income"]
    assert "ticker map" in reason
    assert NEW_CIK.lstrip("0") in reason or NEW_CIK in reason
    assert "2026-11-02" in reason
    # Every metric carries the same explanation, not "not filed" noise.
    assert len(set(result["reasons"].values())) == 1


def test_statements_before_the_new_registrant_existed_carry_the_same_warning(sec):
    statement = sec.get_income_statement("HLD", as_of=datetime(2025, 6, 2, 10, tzinfo=NY))

    assert statement["values"] == {} or all(not value for value in statement["values"].values())
    messages = " ".join(str(item.get("message")) for item in statement["warnings"])
    assert "ticker map" in messages
    assert "2026-11-02" in messages


def test_after_its_first_filing_the_registrant_is_used_normally(sec):
    statement = sec.get_income_statement("HLD", as_of=datetime(2026, 11, 10, 10, tzinfo=NY))

    assert statement["values"]["net_income"]["value"] == 30.0
    assert not any("ticker map" in str(item.get("message")) for item in statement["warnings"])
