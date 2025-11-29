"""Centralised futures roll logic shared by assets, data sources, and brokers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pytz

from lumibot.constants import LUMIBOT_DEFAULT_PYTZ

_FUTURES_MONTH_CODES: Dict[int, str] = {
    1: "F",
    2: "G",
    3: "H",
    4: "J",
    5: "K",
    6: "M",
    7: "N",
    8: "Q",
    9: "U",
    10: "V",
    11: "X",
    12: "Z",
}


@dataclass(frozen=True)
class RollRule:
    offset_business_days: int
    anchor: str
    contract_months: Optional[Tuple[int, ...]] = None


_DEFAULT_CONTRACT_MONTHS: Tuple[int, ...] = (3, 6, 9, 12)


ROLL_RULES: Dict[str, RollRule] = {
    symbol: RollRule(offset_business_days=8, anchor="third_friday", contract_months=_DEFAULT_CONTRACT_MONTHS)
    for symbol in {"ES", "MES", "NQ", "MNQ", "YM", "MYM"}
}

ROLL_RULES.update(
    {
        "GC": RollRule(
            offset_business_days=7,
            anchor="third_last_business_day",
            contract_months=(2, 4, 6, 8, 10, 12),
        ),
        "SI": RollRule(
            offset_business_days=7,
            anchor="third_last_business_day",
            contract_months=(1, 3, 5, 7, 9, 12),
        ),
    }
)

YearMonth = Tuple[int, int]


def _to_timezone(dt: datetime, tz=pytz.timezone("America/New_York")) -> datetime:
    if dt.tzinfo is None:
        return tz.localize(dt)
    return dt.astimezone(tz)


def _normalize_reference_date(reference_date: Optional[datetime]) -> datetime:
    if reference_date is None:
        reference_date = datetime.utcnow()
    return _to_timezone(reference_date, LUMIBOT_DEFAULT_PYTZ)


def _third_friday(year: int, month: int) -> datetime:
    first = datetime(year, month, 1)
    first = _to_timezone(first)
    weekday = first.weekday()
    days_until_friday = (4 - weekday) % 7
    first_friday = first + timedelta(days=days_until_friday)
    third_friday = first_friday + timedelta(weeks=2)
    return third_friday.replace(hour=0, minute=0, second=0, microsecond=0)


def _subtract_business_days(dt: datetime, days: int) -> datetime:
    result = dt
    remaining = days
    while remaining > 0:
        result -= timedelta(days=1)
        if result.weekday() < 5:
            remaining -= 1
    return result


def _third_last_business_day(year: int, month: int) -> datetime:
    if month == 12:
        next_month = 1
        next_year = year + 1
    else:
        next_month = month + 1
        next_year = year

    last_day = _to_timezone(datetime(next_year, next_month, 1)) - timedelta(days=1)

    remaining = 3
    cursor = last_day
    while remaining > 0:
        if cursor.weekday() < 5:
            remaining -= 1
            if remaining == 0:
                break
        cursor -= timedelta(days=1)
    return cursor.replace(hour=0, minute=0, second=0, microsecond=0)


def _calculate_roll_trigger(year: int, month: int, rule: RollRule) -> datetime:
    if rule.anchor == "third_friday":
        anchor = _third_friday(year, month)
    elif rule.anchor == "third_last_business_day":
        anchor = _third_last_business_day(year, month)
    else:
        anchor = _to_timezone(datetime(year, month, 15))
    if rule.offset_business_days <= 0:
        return anchor
    return _subtract_business_days(anchor, rule.offset_business_days)


def _get_contract_months(rule: Optional[RollRule]) -> Tuple[int, ...]:
    if rule and rule.contract_months:
        return tuple(sorted(rule.contract_months))
    return _DEFAULT_CONTRACT_MONTHS


def _advance_contract(current_month: int, current_year: int, months: Tuple[int, ...]) -> YearMonth:
    months_sorted = tuple(sorted(months))
    idx = months_sorted.index(current_month)
    next_idx = (idx + 1) % len(months_sorted)
    next_month = months_sorted[next_idx]
    next_year = current_year + (1 if next_idx <= idx else 0)
    return next_year, next_month


def _select_contract(year: int, month: int, months: Tuple[int, ...]) -> YearMonth:
    for candidate in sorted(months):
        if month <= candidate:
            return year, candidate
    return year + 1, sorted(months)[0]


def _legacy_mid_month(reference_date: datetime) -> YearMonth:
    quarter_months = [3, 6, 9, 12]
    year = reference_date.year
    month = reference_date.month
    day = reference_date.day

    if month == 12 and day >= 15:
        return year + 1, 3
    if month >= 10:
        return year, 12
    if month == 9 and day >= 15:
        return year, 12
    if month >= 7:
        return year, 9
    if month == 6 and day >= 15:
        return year, 9
    if month >= 4:
        return year, 6
    if month == 3 and day >= 15:
        return year, 6
    return year, 3


def determine_contract_year_month(symbol: str, reference_date: Optional[datetime] = None) -> YearMonth:
    ref = _normalize_reference_date(reference_date)
    symbol_upper = symbol.upper()
    rule = ROLL_RULES.get(symbol_upper)
    year = ref.year
    month = ref.month

    if rule is None:
        return _legacy_mid_month(ref)

    contract_months = _get_contract_months(rule)

    if month in contract_months:
        target_year, target_month = year, month
    else:
        target_year, target_month = _select_contract(year, month, contract_months)

    roll_point = _calculate_roll_trigger(target_year, target_month, rule)
    if ref >= roll_point:
        target_year, target_month = _advance_contract(target_month, target_year, contract_months)

    return target_year, target_month


def build_contract_symbol(root: str, year: int, month: int, year_digits: int = 2) -> str:
    month_code = _FUTURES_MONTH_CODES.get(month)
    if month_code is None:
        raise ValueError(f"Unsupported futures month: {month}")
    if year_digits == 1:
        return f"{root}{month_code}{year % 10}"
    if year_digits == 4:
        return f"{root}{month_code}{year}"
    return f"{root}{month_code}{year % 100:02d}"


def resolve_symbol_for_datetime(asset, dt: datetime, year_digits: int = 2) -> str:
    year, month = determine_contract_year_month(asset.symbol, dt)
    return build_contract_symbol(asset.symbol, year, month, year_digits=year_digits)


def resolve_symbols_for_range(asset, start: datetime, end: datetime, year_digits: int = 2) -> List[str]:
    if start is None or end is None:
        return []

    start = _normalize_reference_date(start)
    end = _normalize_reference_date(end)
    if start > end:
        start, end = end, start

    symbols: List[str] = []
    seen: set[str] = set()
    cursor = start
    step = timedelta(days=30)

    while cursor <= end + timedelta(days=45):
        symbol = resolve_symbol_for_datetime(asset, cursor, year_digits=year_digits)
        if symbol not in seen:
            seen.add(symbol)
            symbols.append(symbol)
        cursor += step

    final_symbol = resolve_symbol_for_datetime(asset, end, year_digits=year_digits)
    if final_symbol not in seen:
        symbols.append(final_symbol)

    if final_symbol in symbols:
        final_index = symbols.index(final_symbol)
        symbols = symbols[: final_index + 1]

    return symbols

def build_roll_schedule(asset, start: datetime, end: datetime, year_digits: int = 2):
    if start is None or end is None:
        return []

    start = _normalize_reference_date(start)
    end = _normalize_reference_date(end)
    if start > end:
        start, end = end, start

    symbol_upper = asset.symbol.upper()
    rule = ROLL_RULES.get(symbol_upper)
    contract_months = _get_contract_months(rule)

    schedule = []
    cursor = start
    previous_start = start

    while cursor <= end + timedelta(days=90):
        year, month = determine_contract_year_month(symbol_upper, cursor)
        symbol = build_contract_symbol(symbol_upper, year, month, year_digits=year_digits)

        if rule:
            roll_dt = _calculate_roll_trigger(year, month, rule)
        else:
            roll_dt = _to_timezone(datetime(year, month, 15))

        schedule.append((symbol, previous_start, roll_dt))

        cursor = roll_dt + timedelta(minutes=1)
        previous_start = cursor
        if roll_dt >= end:
            break

    clipped = []
    for symbol, s, e in schedule:
        start_clip = max(s, start)
        end_clip = min(e, end)
        if end_clip <= start_clip:
            continue
        clipped.append((symbol, start_clip, end_clip))

    if not clipped:
        return [(
            symbol,
            s.astimezone(pytz.UTC),
            e.astimezone(pytz.UTC),
        ) for symbol, s, e in schedule]

    last_symbol, s, e = clipped[-1]
    if e < end:
        clipped[-1] = (last_symbol, s, end)

    return [
        (
            symbol,
            start_clip.astimezone(pytz.UTC),
            end_clip.astimezone(pytz.UTC),
        )
        for symbol, start_clip, end_clip in clipped
    ]
