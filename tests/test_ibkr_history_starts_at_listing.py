"""A rule that asks for more history than a symbol has must not fail the backtest.

A newly listed symbol has fewer bars than a long lookback needs. When the provider was
asked for the earlier window and it holds nothing before the first bar, the shortfall is
the symbol's real start of history: the run says "history starts on DATE" and is not
marked failed. Missing data, interior holes and failed requests still fail it.
"""
from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

import pandas as pd
import pytest

from lumibot.tools.ibkr_history_health import (
    HistoryOutcome,
    ibkr_history_health_snapshot,
    record_history_health,
    record_required_history,
    reset_ibkr_history_health_for_testing,
)

ASSET = SimpleNamespace(symbol="NEWETF", asset_type="stock")
LISTED = pd.Timestamp("2010-02-11")


@pytest.fixture(autouse=True)
def _reset():
    reset_ibkr_history_health_for_testing()
    yield
    reset_ibkr_history_health_for_testing()


def _bars(count, start=LISTED):
    index = pd.bdate_range(start=start, periods=count)
    return pd.DataFrame({"close": range(count)}, index=index)


def _health(outcome, *, missing=(), start="2009-01-02", reason=None):
    record_history_health(
        symbol="NEWETF", asset_type="stock", timestep="day",
        requested_start=datetime.fromisoformat(start).replace(tzinfo=timezone.utc),
        requested_end=datetime(2010, 6, 30, tzinfo=timezone.utc),
        outcome=outcome, missing_sessions=missing, reason=reason)


def _ask(frame, requested=200):
    when = pd.Timestamp(frame.index.max() + pd.Timedelta(hours=16), tz="UTC")
    record_required_history(asset=ASSET, timestep="day", requested_bars=requested, frame=frame, when=when)


def test_short_history_at_the_listing_is_not_a_failure_and_says_when_it_starts():
    _health(HistoryOutcome.COMPLETE)
    _ask(_bars(60))
    health = ibkr_history_health_snapshot()
    assert health["required_complete"] is True
    assert health["required_failures"] == []
    start = health["history_starts"][0]
    assert (start["symbol"], start["first_available"]) == ("NEWETF", "2010-02-11")
    assert start["requested_bars"] == 200 and start["returned_bars"] == 60


def test_missing_sessions_only_before_the_first_bar_also_count_as_the_start():
    before = [d.date().isoformat() for d in pd.bdate_range("2009-01-02", "2010-02-10")]
    _health(HistoryOutcome.PARTIAL, missing=before, reason="unresolved_daily_sessions_after_bounded_repair")
    _ask(_bars(60))
    assert ibkr_history_health_snapshot()["required_complete"] is True


def test_no_evidence_that_earlier_bars_were_asked_for_still_fails():
    _ask(_bars(60))
    health = ibkr_history_health_snapshot()
    assert health["required_complete"] is False
    assert health["required_failures"][0]["reason"] == "insufficient_required_history"


def test_failed_earlier_request_still_fails():
    _health(HistoryOutcome.TRANSIENT_FAILURE, reason="rate_limited")
    _ask(_bars(60))
    assert ibkr_history_health_snapshot()["required_complete"] is False


def test_a_hole_inside_the_returned_bars_still_fails():
    _health(HistoryOutcome.PARTIAL, missing=["2010-03-03"], reason="unresolved_daily_sessions_after_bounded_repair")
    _ask(_bars(60))
    assert ibkr_history_health_snapshot()["required_complete"] is False


def test_enough_bars_records_nothing_extra():
    _health(HistoryOutcome.COMPLETE)
    _ask(_bars(210))
    health = ibkr_history_health_snapshot()
    assert health["required_complete"] is True and health["history_starts"] == []
