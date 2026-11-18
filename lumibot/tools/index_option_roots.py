"""Option roots for index options (SPX and SPXW, NDX and NDXP, RUT and RUTW, VIX and VIXW).

Index options trade in two classes with their own OCC root:

- the AM-settled standard monthly contract uses the index symbol (``SPX261120P07555000``);
- every other expiration (weeklies, end-of-month, 0DTE) is PM-settled and uses the weekly
  root (``SPXW261113P07555000``).

Strategies normally write ``Asset("SPX", asset_type="option", expiration=...)`` for both. This
module is the one place that turns (index symbol, expiration) into the root a provider lists,
and maps a weekly root back to its index. XSP and other single-root indexes are unchanged.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from functools import lru_cache
from typing import Optional

# Weekly/PM-settled root -> index root.
INDEX_WEEKLY_ROOTS = {
    "SPXW": "SPX",
    "RUTW": "RUT",
    "VIXW": "VIX",
    "NDXP": "NDX",
}
_MONTHLY_TO_WEEKLY = {monthly: weekly for weekly, monthly in INDEX_WEEKLY_ROOTS.items()}


def index_underlying_symbol(symbol: str) -> str:
    """Index symbol for an option root: ``SPXW`` -> ``SPX``. Other symbols are returned unchanged."""
    upper = str(symbol or "").upper()
    return INDEX_WEEKLY_ROOTS.get(upper, str(symbol or ""))


def index_weekly_root(symbol: str) -> Optional[str]:
    """Weekly root of an index with two option classes (``SPX`` -> ``SPXW``), else None."""
    return _MONTHLY_TO_WEEKLY.get(str(symbol or "").upper())


@lru_cache(maxsize=1)
def _exchange_holidays() -> frozenset:
    import pandas as pd
    import pandas_market_calendars as mcal

    return frozenset(pd.Timestamp(d).date() for d in mcal.get_calendar("NYSE").holidays().holidays)


def _as_date(value) -> Optional[date]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    if hasattr(value, "date"):
        try:
            return value.date()
        except Exception:
            return None
    try:
        return datetime.strptime(str(value)[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def is_standard_monthly_expiration(day: date) -> bool:
    """The third Friday, or the Thursday before it when that Friday is an exchange holiday."""
    if day.weekday() == 4 and 15 <= day.day <= 21:
        return True
    friday = day + timedelta(days=1)
    return day.weekday() == 3 and 15 <= friday.day <= 21 and friday in _exchange_holidays()


def is_index_monthly_expiration(symbol: str, expiration) -> bool:
    """True when ``expiration`` is the AM-settled monthly date listed under the index root.

    VIX monthly options expire on the Wednesday 30 days before the third Friday of the next
    month (Cboe rule), or the business day before that Wednesday when it is a holiday. The
    other indexes use the standard monthly date.
    """
    day = _as_date(expiration)
    if day is None:
        return False
    if str(symbol or "").upper() != "VIX":
        return is_standard_monthly_expiration(day)
    friday = day + timedelta(days=30)
    if day.weekday() == 2 and friday.weekday() == 4 and 15 <= friday.day <= 21:
        return True
    friday = day + timedelta(days=31)
    return (
        day.weekday() == 1
        and friday.weekday() == 4
        and 15 <= friday.day <= 21
        and (day + timedelta(days=1)) in _exchange_holidays()
    )


def index_option_root(symbol: str, expiration) -> str:
    """OCC root for an option on ``symbol`` expiring on ``expiration``.

    ``SPX`` on a non-monthly date gives ``SPXW``; on the monthly date it stays ``SPX``. A
    symbol that is already a weekly root, or any non-index symbol, is returned unchanged.
    """
    weekly = index_weekly_root(symbol)
    if weekly is None or _as_date(expiration) is None:
        return str(symbol)
    return str(symbol).upper() if is_index_monthly_expiration(symbol, expiration) else weekly


def occ_option_symbol(asset) -> str:
    """Alpaca-style OCC symbol (no padding) for an option asset, with the index root applied."""
    strike_formatted = f"{float(asset.strike):08.3f}".replace(".", "").rjust(8, "0")
    expiration = _as_date(asset.expiration)
    right = str(asset.right or "C")[0].upper()
    return f"{index_option_root(asset.symbol, expiration)}{expiration:%y%m%d}{right}{strike_formatted}"
