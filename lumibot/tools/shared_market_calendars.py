"""One shared market calendar object per calendar name.

Why: ``pandas_market_calendars`` builds its holiday rules once per calendar *object*
(``MarketCalendar.holidays`` is cached on the instance). Code that called
``mcal.get_calendar(name)`` before every ``schedule()`` rebuilt those rules each time;
a 10-year daily stock backtest profiled 21 rebuilds (about 10 profiled seconds).
Calendar objects are read-only here, so one instance per name gives the same schedules.
"""

from __future__ import annotations

import functools
import threading
from typing import Callable, Optional

_LOCK = threading.Lock()


@functools.lru_cache(maxsize=128)
def _calendar(name: str, factory: Callable):
    return factory(name)


def get_market_calendar(name: str, factory: Optional[Callable] = None):
    """Return the shared calendar for ``name``.

    ``factory`` defaults to ``pandas_market_calendars.get_calendar``. Callers pass their own
    module's ``mcal.get_calendar`` so a test that replaces it gets its own instances.
    Errors from the factory (unknown calendar) propagate unchanged and are not cached.
    """
    if factory is None:
        import pandas_market_calendars as mcal

        factory = mcal.get_calendar
    with _LOCK:
        return _calendar(name, factory)


def clear_shared_market_calendars() -> None:
    with _LOCK:
        _calendar.cache_clear()
