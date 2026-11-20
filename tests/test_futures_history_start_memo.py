"""The futures lookback start is computed once per (end, length, bar size) and gives the same answer.

Why: a profiled 3-month MNQ 1-minute backtest spent 142 of 527 profiled seconds in
``ibkr_helper._futures_history_start_for_bars`` (277,648 calls, 1.5 million session-bound
calculations with pytz localization). Routed futures valuation and indicator reads ask the
same question several times per simulated minute, and every answer walked the session
calendar back from the end time. The answer is a pure function of its inputs, so it is now
remembered, and each session's open/close is computed once per day.

Invariant under test: for many end times (naive and aware, around the daily break, weekends,
holidays, DST changes), lengths and bar sizes, the memoized answer equals the uncached one.
"""

from datetime import datetime, timedelta

import pandas as pd
import pytest

from lumibot.tools import ibkr_helper


def _ends():
    base = pd.Timestamp("2026-03-06 09:31", tz="America/New_York")  # spans the March DST change
    ends = [base + pd.Timedelta(minutes=37 * i) for i in range(0, 900)]
    ends += [pd.Timestamp("2026-11-26 12:00", tz="America/New_York"),  # Thanksgiving
             pd.Timestamp("2026-11-29 18:30", tz="America/New_York"),  # Sunday reopen
             pd.Timestamp("2026-11-02 16:30", tz="America/New_York")]  # daily break
    naive = [datetime(2026, 8, 18, 0, 0) + timedelta(minutes=53 * i) for i in range(300)]
    return ends + naive


@pytest.mark.parametrize("timestep", ["minute", "5 minutes", "15 minutes", "hour"])
@pytest.mark.parametrize("holiday_sessions", [True, False])
def test_memoized_start_equals_uncached(timestep, holiday_sessions):
    ibkr_helper._FUTURES_HISTORY_START_MEMO.clear()
    for end in _ends():
        for length in (1, 10, 63, 400):
            expected = ibkr_helper._futures_history_start_for_bars_uncached(
                end, length, timestep, holiday_sessions=holiday_sessions)
            first = ibkr_helper._futures_history_start_for_bars(end, length, timestep, holiday_sessions=holiday_sessions)
            again = ibkr_helper._futures_history_start_for_bars(end, length, timestep, holiday_sessions=holiday_sessions)
            assert first == expected == again, (end, length, timestep)


def test_repeated_question_walks_the_calendar_once(monkeypatch):
    ibkr_helper._FUTURES_HISTORY_START_MEMO.clear()
    calls = {"n": 0}
    original = ibkr_helper._futures_history_start_for_bars_uncached

    def counting(*args, **kwargs):
        calls["n"] += 1
        return original(*args, **kwargs)

    monkeypatch.setattr(ibkr_helper, "_futures_history_start_for_bars_uncached", counting)
    end = pd.Timestamp("2026-10-22 06:21", tz="America/New_York")
    for _ in range(5):
        ibkr_helper._futures_history_start_for_bars(end, 63, "minute")
    # The same instant given in another time zone is the same question.
    ibkr_helper._futures_history_start_for_bars(end.tz_convert("UTC"), 63, "minute")
    assert calls["n"] == 1


def test_errors_are_not_remembered():
    ibkr_helper._FUTURES_HISTORY_START_MEMO.clear()
    end = pd.Timestamp("2026-10-22 06:21", tz="America/New_York")
    for _ in range(2):
        with pytest.raises(ValueError):
            ibkr_helper._futures_history_start_for_bars(end, 10, "day")
    assert not ibkr_helper._FUTURES_HISTORY_START_MEMO
