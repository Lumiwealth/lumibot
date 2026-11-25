"""An idle live bot must not rebuild the market calendar every second.

The live executor's main loop wakes once a second and asks the broker for the
market open and close time (``_handle_lifecycle_methods``). Before this fix each
of those calls rebuilt a one-week ``pandas_market_calendars`` schedule and
re-walked the initialized trading-days frame row by row. Profiling an idle
Alpaca paper bot showed that work was about three quarters of all the CPU the
bot used while it was waiting for its next iteration.

These tests pin two things:
1. the answers are exactly the same as the uncached calculation, and
2. repeated calls on the same day do not rebuild the schedule or re-walk the frame.
"""
import logging
from datetime import datetime as real_datetime, timedelta, timezone
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pandas as pd
import pandas_market_calendars as mcal
import pytest

import lumibot.brokers.broker as broker_module
from lumibot.brokers.broker import Broker

NEW_YORK = ZoneInfo("America/New_York")


class LiveBrokerStub:
    IS_BACKTESTING_BROKER = False
    extended_trading_minutes = 0

    market_hours = Broker.market_hours
    utc_to_local = Broker.utc_to_local
    initialize_market_calendars = Broker.initialize_market_calendars
    _session_close_time = Broker._session_close_time
    market_close_time = Broker.market_close_time
    market_open_time = Broker.market_open_time

    def __init__(self, market):
        self.market = market
        self._trading_days = None
        self.logger = logging.getLogger("idle-calendar-cache-test")


def _freeze(monkeypatch, moment_utc):
    state = {"now": moment_utc}

    class FrozenDatetime(real_datetime):
        @classmethod
        def now(cls, tz=None):
            now = state["now"]
            moment = now.astimezone(tz) if tz is not None else now.astimezone(NEW_YORK).replace(tzinfo=None)
            return cls(
                moment.year, moment.month, moment.day, moment.hour, moment.minute, moment.second,
                moment.microsecond, tzinfo=moment.tzinfo,
            )

    monkeypatch.setattr(broker_module, "datetime", FrozenDatetime)
    monkeypatch.setattr(broker_module, "_local_tz", lambda: NEW_YORK)
    return state


def _uncached_market_hours(market, now_utc, close, next_, extended=0):
    cal = mcal.get_calendar(market)
    hours = cal.schedule(start_date=now_utc, end_date=now_utc + pd.DateOffset(weeks=1)).head(2)
    th = hours.iloc[1 if next_ else 0, :]
    return th.iloc[1] + timedelta(minutes=extended) if close else th.iloc[0]


def _count_schedule_calls(monkeypatch):
    calls = {"n": 0}
    real_get_calendar = mcal.get_calendar

    def counting_get_calendar(*args, **kwargs):
        cal = real_get_calendar(*args, **kwargs)
        real_schedule = cal.schedule

        def schedule(*a, **k):
            calls["n"] += 1
            return real_schedule(*a, **k)

        cal.schedule = schedule
        return cal

    # Replace the module reference, not the attribute: pandas_market_calendars serves
    # get_calendar lazily, so patching the attribute would delete it on undo.
    monkeypatch.setattr(broker_module, "mcal", SimpleNamespace(get_calendar=counting_get_calendar))
    return calls


@pytest.mark.parametrize("market", ["NYSE", "us_futures", "24/7"])
def test_market_hours_matches_uncached_answer_across_a_week(monkeypatch, market):
    broker = LiveBrokerStub(market)
    start = real_datetime(2026, 11, 20, 0, 0, tzinfo=timezone.utc)
    state = _freeze(monkeypatch, start)
    # Every 97 minutes for 8 days crosses day, weekend and holiday boundaries.
    for step in range(0, 8 * 24 * 60, 97):
        now = start + timedelta(minutes=step)
        state["now"] = now
        for close in (True, False):
            for next_ in (False, True):
                assert broker.market_hours(close=close, next=next_) == _uncached_market_hours(
                    market, now, close, next_
                ), (market, now, close, next_)


def test_market_hours_explicit_date_matches_uncached_answer(monkeypatch):
    broker = LiveBrokerStub("NYSE")
    _freeze(monkeypatch, real_datetime(2026, 11, 25, 5, 0, tzinfo=timezone.utc))
    for day in range(10):
        date = real_datetime(2026, 12, 20, 15, 0, tzinfo=timezone.utc) + timedelta(days=day)
        assert broker.market_hours(close=True, date=date) == _uncached_market_hours("NYSE", date, True, False)


def test_market_hours_does_not_rebuild_schedule_every_second(monkeypatch):
    calls = _count_schedule_calls(monkeypatch)
    broker = LiveBrokerStub("NYSE")
    start = real_datetime(2026, 11, 25, 5, 0, tzinfo=timezone.utc)
    state = _freeze(monkeypatch, start)
    for second in range(300):
        state["now"] = start + timedelta(seconds=second)
        broker.market_open_time()
        broker.market_hours(close=True)
    assert calls["n"] == 1

    # A new UTC day must build a fresh schedule.
    state["now"] = start + timedelta(days=1)
    broker.market_open_time()
    assert calls["n"] == 2


def _broker_with_initialized_calendar(monkeypatch, market, now_utc):
    _freeze(monkeypatch, now_utc)
    broker = LiveBrokerStub(market)
    cal = mcal.get_calendar(market)
    broker.initialize_market_calendars(
        cal.schedule(start_date=now_utc - timedelta(days=5), end_date=now_utc + timedelta(days=30))
    )
    return broker


def _uncached_session_close(trading_days, now_utc, extended=0):
    rows = trading_days.reset_index() if "market_close" not in trading_days.columns else trading_days
    for row in rows.itertuples(index=False):
        mo = pd.Timestamp(row.market_open)
        mc = pd.Timestamp(row.market_close)
        mo = mo.tz_localize("UTC") if mo.tzinfo is None else mo.tz_convert("UTC")
        mc = mc.tz_localize("UTC") if mc.tzinfo is None else mc.tz_convert("UTC")
        mc = mc + timedelta(minutes=extended)
        if mo <= now_utc <= mc:
            return mc
    return None


@pytest.mark.parametrize("market", ["NYSE", "us_futures"])
@pytest.mark.parametrize("extended", [0, 30])
def test_session_close_time_matches_uncached_answer(monkeypatch, market, extended):
    start = real_datetime(2026, 11, 20, 0, 0, tzinfo=timezone.utc)
    broker = _broker_with_initialized_calendar(monkeypatch, market, start)
    broker.extended_trading_minutes = extended
    for step in range(0, 8 * 24 * 60, 61):
        now = start + timedelta(minutes=step)
        assert broker._session_close_time(now) == _uncached_session_close(broker._trading_days, now, extended), now


def test_session_close_time_does_not_rewalk_the_frame_every_call(monkeypatch):
    start = real_datetime(2026, 11, 24, 16, 0, tzinfo=timezone.utc)
    broker = _broker_with_initialized_calendar(monkeypatch, "NYSE", start)
    walks = {"n": 0}
    real_itertuples = pd.DataFrame.itertuples

    def counting_itertuples(self, *a, **k):
        walks["n"] += 1
        return real_itertuples(self, *a, **k)

    monkeypatch.setattr(pd.DataFrame, "itertuples", counting_itertuples)
    for second in range(300):
        broker._session_close_time(start + timedelta(seconds=second))
    assert walks["n"] <= 1

    # A newly initialized calendar is honored at once.
    new_days = mcal.get_calendar("NYSE").schedule(
        start_date=start - timedelta(days=2), end_date=start + timedelta(days=2)
    )
    broker.initialize_market_calendars(new_days)
    assert broker._session_close_time(start) == _uncached_session_close(broker._trading_days, start)
