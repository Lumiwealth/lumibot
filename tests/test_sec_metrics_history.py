"""Stored daily fundamentals (lumibot/fundamentals/metrics_history.py).

A screen over many symbols reads one stored table instead of loading price history for every
candidate. Synthetic filer from test_sec_fundamental_metrics (10 shares outstanding); nothing
talks to the SEC.
"""

from datetime import date, datetime
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from lumibot.fundamentals import SECFundamentals
from lumibot.fundamentals.metrics_history import build_metrics_history, remote_key, save_metrics_history
from tests.test_sec_fundamental_metrics import CIK, _FakeRemote, _facts, _Response

NY = ZoneInfo("America/New_York")
OTHER_CIK = "0000333333"
CLOSES = {"EXM": pd.Series({date(2024, 5, 30): 40.0, date(2024, 5, 31): 50.0, date(2024, 6, 3): 70.0})}


@pytest.fixture(autouse=True)
def _fake_sec(monkeypatch):
    from lumibot.tools import backtest_cache

    disabled = backtest_cache.BacktestCacheManager(None)
    monkeypatch.setattr(backtest_cache, "get_backtest_cache", lambda: disabled)
    tickers = {
        "0": {"ticker": "EXM", "cik_str": int(CIK), "title": "Example"},
        "1": {"ticker": "OTH", "cik_str": int(OTHER_CIK), "title": "Other"},
    }
    facts = {CIK: _facts(), OTHER_CIK: _facts()}

    def fake_get(url, **kwargs):
        if url.endswith("company_tickers.json"):
            return _Response(tickers)
        for cik, payload in facts.items():
            if url.endswith(f"companyfacts/CIK{cik}.json"):
                return _Response(payload)
        if "submissions" in url:
            return _Response({"filings": {"recent": {"accessionNumber": [], "acceptanceDateTime": []}}})
        return _Response({}, status_code=404)

    monkeypatch.setattr("lumibot.fundamentals.sec.requests.get", fake_get)
    monkeypatch.delenv("LUMIBOT_SEC_METRICS_HISTORY", raising=False)


class _Strategy:
    is_backtesting = True
    _backtesting_end = datetime(2024, 12, 31, tzinfo=NY)

    def __init__(self, now, price=99.0):
        self.now, self.price, self.asked = now, price, []

    def get_datetime(self):
        return self.now

    def get_last_price(self, asset):
        self.asked.append(getattr(asset, "symbol", asset))
        return self.price


def _sec(cache_dir, store, strategy=None):
    return SECFundamentals(
        strategy=strategy, cache_dir=cache_dir, min_request_interval_seconds=0, remote_cache=_FakeRemote(store),
        split_history=lambda _symbol: [], cache_mode="backtest", backtest_end=datetime(2024, 12, 31, tzinfo=NY),
    )


def _stored(tmp_path):
    store = {}
    builder = _sec(tmp_path / "builder", store)
    frame = build_metrics_history(builder, CLOSES)
    assert save_metrics_history(builder, frame) == [remote_key(2024)]
    return store, frame


def test_built_rows_are_get_metrics_at_the_session_close_with_that_close(tmp_path):
    _store, frame = _stored(tmp_path)
    row = frame[(frame["symbol"] == "EXM") & (frame["date"] == date(2024, 5, 31))].iloc[0]
    assert row["close"] == 50.0
    assert row["market_cap"] == pytest.approx(500)  # 10 shares x 50
    assert row["available_at"] == datetime(2024, 5, 31, 16, 0, tzinfo=NY)


def test_a_screen_reads_the_stored_table_without_asking_for_any_price(tmp_path):
    store, _ = _stored(tmp_path)
    strategy = _Strategy(datetime(2024, 6, 3, 10, 0, tzinfo=NY))
    table = _sec(tmp_path / "machine", store, strategy).get_metrics_table(["EXM"], ["market_cap", "shares_outstanding"])
    assert strategy.asked == []
    assert table.loc["EXM", "market_cap"] == pytest.approx(500)
    assert table.loc["EXM", "shares_outstanding"] == pytest.approx(10)
    assert table.loc["EXM", "cik"] == CIK


def test_a_session_is_only_seen_after_its_close(tmp_path):
    store, _ = _stored(tmp_path)
    before_close = _sec(tmp_path / "a", store, _Strategy(datetime(2024, 6, 3, 15, 59, tzinfo=NY)))
    after_close = _sec(tmp_path / "b", store, _Strategy(datetime(2024, 6, 3, 16, 0, tzinfo=NY)))
    assert before_close.get_metrics_table(["EXM"], ["market_cap"]).loc["EXM", "market_cap"] == pytest.approx(500)
    assert after_close.get_metrics_table(["EXM"], ["market_cap"]).loc["EXM", "market_cap"] == pytest.approx(700)


def test_symbols_not_stored_are_computed_as_before(tmp_path):
    store, _ = _stored(tmp_path)
    strategy = _Strategy(datetime(2024, 6, 3, 10, 0, tzinfo=NY), price=20.0)
    table = _sec(tmp_path / "machine", store, strategy).get_metrics_table(["EXM", "OTH"], ["market_cap"])
    assert strategy.asked == ["OTH"]
    assert table.loc["EXM", "market_cap"] == pytest.approx(500)
    assert table.loc["OTH", "market_cap"] == pytest.approx(200)


def test_an_old_stored_row_is_never_carried_forward(tmp_path):
    store, _ = _stored(tmp_path)
    strategy = _Strategy(datetime(2024, 6, 20, 10, 0, tzinfo=NY), price=30.0)
    table = _sec(tmp_path / "machine", store, strategy).get_metrics_table(["EXM"], ["market_cap"])
    assert strategy.asked == ["EXM"]
    assert table.loc["EXM", "market_cap"] == pytest.approx(300)


def test_given_prices_and_the_off_switch_skip_the_stored_table(tmp_path, monkeypatch):
    store, _ = _stored(tmp_path)
    now = datetime(2024, 6, 3, 10, 0, tzinfo=NY)
    given = _sec(tmp_path / "a", store, _Strategy(now)).get_metrics_table(["EXM"], ["market_cap"], prices={"EXM": 60.0})
    assert given.loc["EXM", "market_cap"] == pytest.approx(600)
    monkeypatch.setenv("LUMIBOT_SEC_METRICS_HISTORY", "0")
    strategy = _Strategy(now, price=30.0)
    off = _sec(tmp_path / "b", store, strategy).get_metrics_table(["EXM"], ["market_cap"])
    assert strategy.asked == ["EXM"]
    assert off.loc["EXM", "market_cap"] == pytest.approx(300)


def test_a_missing_close_is_stored_as_none_with_a_reason(tmp_path):
    builder = _sec(tmp_path / "builder", {})
    frame = build_metrics_history(builder, {"EXM": pd.Series({date(2024, 5, 31): float("nan")})})
    row = frame.iloc[0]
    assert pd.isna(row["market_cap"])
    assert "close" in row["reasons"]
