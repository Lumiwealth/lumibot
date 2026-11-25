"""The risk-free rate download overlaps the backtest instead of following it.

Why this exists: the end-of-backtest statistics read the 13-week Treasury rate (^IRX) for
the last simulated day. On a fresh container the Yahoo download takes 7-10 s (two
deliberate rate-limit sleeps plus the request) and used to start only after the simulation
ended, on every production backtest. The prefetch starts the same download when the
backtest starts; the later lookup must reuse it and return exactly the same value.
"""

from __future__ import annotations

import threading
import time
from datetime import datetime

import pandas as pd
import pytz
import pytest

from lumibot.tools import report_speedups
from lumibot.tools.yahoo_helper import YahooHelper

NY = pytz.timezone("America/New_York")


def _irx_frame():
    idx = pd.date_range("2026-01-02", "2026-11-20", freq="B", tz=NY)
    close = [4.0 + (i % 7) * 0.01 for i in range(len(idx))]
    return pd.DataFrame(
        {"Open": close, "High": close, "Low": close, "Close": close, "Adj Close": close, "Volume": 0},
        index=idx,
    )


@pytest.fixture
def fake_yahoo(tmp_path, monkeypatch):
    calls = []

    def fake_download(symbol, interval="1d"):
        calls.append((symbol, threading.current_thread().name))
        time.sleep(0.3)
        return _irx_frame()

    monkeypatch.setattr(YahooHelper, "LUMIBOT_YAHOO_CACHE_FOLDER", str(tmp_path))
    monkeypatch.setattr(YahooHelper, "CACHING_ENABLED", True)
    monkeypatch.setattr(YahooHelper, "download_symbol_data", staticmethod(fake_download))
    monkeypatch.setattr(report_speedups, "_risk_free_threads", {})
    return calls


def test_prefetch_downloads_in_background_and_lookup_reuses_it(fake_yahoo):
    from lumibot.tools.indicators import get_risk_free_rate

    end = NY.localize(datetime(2026, 11, 17, 16))
    thread = report_speedups.prefetch_risk_free_rate(end)
    assert isinstance(thread, threading.Thread) and thread.daemon

    rate = get_risk_free_rate(end)

    assert fake_yahoo == [("^IRX", "lumibot-risk-free-prefetch")]
    frame = _irx_frame()
    nearest = abs(frame.index - end).argmin()  # the bar closest to the asked time
    assert rate == pytest.approx(frame["Close"].iloc[nearest] / 100)


def test_prefetched_value_equals_a_fresh_lookup(fake_yahoo, tmp_path, monkeypatch):
    from lumibot.tools.indicators import get_risk_free_rate

    end = NY.localize(datetime(2026, 11, 17, 16))
    report_speedups.prefetch_risk_free_rate(end)
    prefetched = get_risk_free_rate(end)

    fresh_dir = tmp_path / "fresh"
    fresh_dir.mkdir()
    monkeypatch.setattr(YahooHelper, "LUMIBOT_YAHOO_CACHE_FOLDER", str(fresh_dir))
    monkeypatch.setattr(report_speedups, "_risk_free_threads", {})
    fresh = get_risk_free_rate(end)

    assert prefetched == fresh
    assert len(fake_yahoo) == 2


def test_prefetch_starts_once_per_end_date(fake_yahoo):
    end = NY.localize(datetime(2026, 11, 17, 16))
    first = report_speedups.prefetch_risk_free_rate(end)
    assert report_speedups.prefetch_risk_free_rate(end) is first
    report_speedups.wait_for_risk_free_prefetch()
    assert len(fake_yahoo) == 1


def test_prefetch_can_be_switched_off(fake_yahoo, monkeypatch):
    monkeypatch.setenv("LUMIBOT_PREFETCH_RISK_FREE_RATE", "0")
    assert report_speedups.prefetch_risk_free_rate(NY.localize(datetime(2026, 11, 17, 16))) is None
    assert fake_yahoo == []
