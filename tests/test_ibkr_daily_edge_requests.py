"""Daily history requests at the edges of a backtest window.

A long daily backtest that ends today used to send a few tiny provider requests per
symbol that could never return a bar: a page dated tomorrow, and the same empty
pre-listing window again in every new run. Each waited in the shared download queue.

1. No request asks for bars after now (no future dates).
2. A window that is fully in the past and lies before the first bar the cache holds,
   answered empty, is remembered for a bounded time so the next run does not ask again.
   A recent window is never remembered, so a real later bar is never hidden.
"""
from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd
import pytest

from lumibot.entities import Asset
from lumibot.tools import ibkr_helper

NOW = datetime(2026, 11, 18, 7, 35, tzinfo=timezone.utc)  # 2:35 AM New York, Wednesday
STOCK = Asset("NEWCO", asset_type=Asset.AssetType.STOCK)
USD = Asset("USD", asset_type=Asset.AssetType.FOREX)


@pytest.fixture(autouse=True)
def _setup(monkeypatch, tmp_path):
    from lumibot.tools.ibkr_equity_basis import EquityBasisSnapshot

    ledger = pd.DataFrame(columns=["Dividends", "Stock Splits"])
    basis = EquityBasisSnapshot.from_verified_actions(
        ledger, as_of=pd.Timestamp("2026-11-17T17:00Z"), coverage_through="2026-11-16")
    monkeypatch.setattr(ibkr_helper, "_get_current_equity_basis", lambda _: basis)
    monkeypatch.setenv("DATADOWNLOADER_BASE_URL", "http://localhost:8080")
    monkeypatch.setenv("DATADOWNLOADER_API_KEY", "x")
    monkeypatch.delenv("IBKR_HISTORY_SOURCE", raising=False)
    monkeypatch.setattr(ibkr_helper, "LUMIBOT_CACHE_FOLDER", tmp_path.as_posix())
    monkeypatch.setattr(ibkr_helper, "_resolve_conid", lambda **_: 4242)
    monkeypatch.setattr(ibkr_helper, "_ibkr_history_now_utc", lambda: NOW)
    ibkr_helper._RUNTIME_HISTORY_NO_DATA_WINDOWS.clear()
    ibkr_helper._RUNTIME_DAILY_GAP_CHECKED_WINDOWS.clear()
    for name in ("_RUNTIME_ATTEMPTED_HISTORY_SEGMENTS",):
        store = getattr(ibkr_helper, name, None)
        if hasattr(store, "clear"):
            store.clear()
    yield
    ibkr_helper._RUNTIME_HISTORY_NO_DATA_WINDOWS.clear()
    ibkr_helper._RUNTIME_DAILY_GAP_CHECKED_WINDOWS.clear()


def _request_end(start_time) -> pd.Timestamp:
    stamp = pd.Timestamp(start_time)
    return stamp.tz_localize("UTC") if stamp.tzinfo is None else stamp.tz_convert("UTC")


def _payload(days):
    return {"volumeFactor": 1, "data": [
        {"t": int(pd.Timestamp(f"{day} 16:00", tz="America/New_York").value // 1_000_000),
         "o": 10.0, "h": 11.0, "l": 9.0, "c": 10.5, "v": 1000} for day in days]}


def test_daily_request_never_asks_for_a_date_after_now(monkeypatch):
    seen = []

    def fake_request(**kwargs):
        seen.append(_request_end(kwargs["start_time"]))
        return _payload(["2026-11-16", "2026-11-17"])

    monkeypatch.setattr(ibkr_helper, "_ibkr_history_request", fake_request)
    ibkr_helper._fetch_history_between_dates(
        asset=STOCK, quote=USD, timestep="day",
        start_dt=datetime(2026, 11, 16, 5, tzinfo=timezone.utc),
        end_dt=datetime(2026, 11, 19, 5, tzinfo=timezone.utc),  # tomorrow midnight New York
        exchange=None, include_after_hours=True, source="Trades", source_was_explicit=False)
    assert seen, "the window holds finished sessions, so it is still requested"
    assert max(seen) <= pd.Timestamp(NOW)


def test_daily_window_with_no_finished_session_makes_no_request(monkeypatch):
    calls = []
    monkeypatch.setattr(ibkr_helper, "_ibkr_history_request", lambda **kw: calls.append(kw) or _payload([]))
    frame = ibkr_helper._fetch_history_between_dates(
        asset=STOCK, quote=USD, timestep="day",
        start_dt=datetime(2026, 11, 18, 5, tzinfo=timezone.utc),   # today, session not started
        end_dt=datetime(2026, 11, 19, 5, tzinfo=timezone.utc),
        exchange=None, include_after_hours=True, source="Trades", source_was_explicit=False)
    assert frame.empty
    assert calls == []


def _new_process():
    ibkr_helper._RUNTIME_HISTORY_NO_DATA_WINDOWS.clear()
    ibkr_helper._RUNTIME_DAILY_GAP_CHECKED_WINDOWS.clear()
    store = getattr(ibkr_helper, "_RUNTIME_ATTEMPTED_HISTORY_SEGMENTS", None)
    if hasattr(store, "clear"):
        store.clear()


def test_empty_answer_before_the_listing_is_asked_once_across_runs(monkeypatch):
    listed = [d.date().isoformat() for d in pd.bdate_range("2020-09-30", "2020-10-20")]
    asked = []

    def vendor(**kwargs):
        # A page holds the sessions that closed before its end, as the observed provider
        # answers for pre-listing windows were empty.
        end = _request_end(kwargs["start_time"])
        asked.append(end)
        days = [d for d in listed if pd.Timestamp(f"{d} 16:00", tz="America/New_York") < end]
        return _payload(days)

    monkeypatch.setattr(ibkr_helper, "_ibkr_history_request", vendor)
    window = dict(asset=STOCK, quote=USD, timestep="day", include_after_hours=True,
                  start_dt=datetime(2020, 1, 2, 5, tzinfo=timezone.utc),
                  end_dt=datetime(2020, 10, 21, 4, tzinfo=timezone.utc))
    # Run 1 loads the listed bars.
    first = ibkr_helper.get_price_data(**window)
    assert not first.empty
    # Run 2 asks for the pre-listing head once and gets nothing.
    _new_process()
    ibkr_helper.get_price_data(**window)
    head_requests = len(asked)
    # Run 3 must not ask for the same empty pre-listing head again.
    _new_process()
    third = ibkr_helper.get_price_data(**window)
    assert len(asked) == head_requests
    # The real bars are untouched and no price was invented for the pre-listing days.
    assert pd.DatetimeIndex(third.index).tz_convert("America/New_York").min().date().isoformat() == "2020-09-30"
    assert third["close"].notna().all()


def test_recent_empty_tail_is_never_remembered(monkeypatch):
    asked = []
    old = [d.date().isoformat() for d in pd.bdate_range("2026-10-01", "2026-11-13")]

    def vendor(**kwargs):
        end = _request_end(kwargs["start_time"])
        asked.append(end)
        return _payload([d for d in old if pd.Timestamp(d, tz="UTC") <= end])

    monkeypatch.setattr(ibkr_helper, "_ibkr_history_request", vendor)
    window = dict(asset=STOCK, quote=USD, timestep="day", include_after_hours=True,
                  start_dt=datetime(2026, 10, 1, 4, tzinfo=timezone.utc),
                  end_dt=datetime(2026, 11, 18, 5, tzinfo=timezone.utc))
    ibkr_helper.get_price_data(**window)
    _new_process()
    before = len(asked)
    ibkr_helper.get_price_data(**window)
    # The last week is recent: the next run asks again, so a bar published later is seen.
    assert len(asked) > before
