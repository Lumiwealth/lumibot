"""Shared market calendar objects keep NYSE holiday rules from being rebuilt per call.

Why: a 10-year daily stock backtest built a new NYSE calendar 21 times (once per
session-bounds year and trading-calendar lookup); each new object rebuilt the full
holiday rule set. One shared object per name gives identical schedules.
"""

import pandas as pd
import pandas_market_calendars as mcal

from lumibot.tools import ibkr_helper
from lumibot.tools.shared_market_calendars import clear_shared_market_calendars, get_market_calendar


def test_same_object_per_name_and_factory():
    clear_shared_market_calendars()
    assert get_market_calendar("NYSE") is get_market_calendar("NYSE")
    assert get_market_calendar("NYSE") is not get_market_calendar("us_futures")


def test_factory_replacement_gets_its_own_object():
    clear_shared_market_calendars()
    made = []

    def factory(name):
        made.append(name)
        return mcal.get_calendar(name)

    first = get_market_calendar("NYSE", factory)
    assert get_market_calendar("NYSE", factory) is first
    assert made == ["NYSE"]
    assert get_market_calendar("NYSE") is not first


def test_session_bounds_build_holidays_once_and_match_fresh_calendars(monkeypatch):
    clear_shared_market_calendars()
    ibkr_helper._us_equity_session_bounds_for_year.cache_clear()
    built = {"n": 0}
    original = mcal.get_calendar

    def counting(name, *args, **kwargs):
        built["n"] += 1
        return original(name, *args, **kwargs)

    monkeypatch.setattr(mcal, "get_calendar", counting)
    years = list(range(2012, 2027))
    shared = {year: ibkr_helper._us_equity_session_bounds_for_year(year, False) for year in years}
    assert built["n"] == 1

    for year in (2012, 2020, 2026):
        fresh = original("NYSE").schedule(start_date=f"{year}-01-01", end_date=f"{year}-12-31")
        opens = pd.DatetimeIndex(pd.to_datetime(fresh["market_open"], utc=True)).as_unit("ns").asi8
        assert list(shared[year][0]) == sorted(opens)
    ibkr_helper._us_equity_session_bounds_for_year.cache_clear()
