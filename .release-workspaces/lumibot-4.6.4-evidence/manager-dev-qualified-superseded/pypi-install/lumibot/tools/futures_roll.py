"""Centralised futures roll logic shared by assets, data sources, and brokers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
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
_MONTHLY_CONTRACT_MONTHS: Tuple[int, ...] = tuple(range(1, 13))


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
        "MGC": RollRule(
            offset_business_days=7,
            anchor="third_last_business_day",
            contract_months=(2, 4, 6, 8, 10, 12),
        ),
        "SI": RollRule(
            offset_business_days=7,
            anchor="third_last_business_day",
            contract_months=(1, 3, 5, 7, 9, 12),
        ),
        # Crude oil (WTI): monthly contracts. IBKR's `expirationDate` aligns with the common
        # last-trade rule (3 business days before the 25th calendar day of the prior month).
        "CL": RollRule(
            offset_business_days=5,
            anchor="cl_last_trade",
            contract_months=_MONTHLY_CONTRACT_MONTHS,
        ),
        "MCL": RollRule(
            offset_business_days=5,
            anchor="mcl_last_trade",
            contract_months=_MONTHLY_CONTRACT_MONTHS,
        ),
        # CME Crypto futures (IBKR roots). These expire on the last Friday trading day of the
        # contract month (holiday-adjusted; e.g. Good Friday -> Thursday).
        #
        # Note: The roll offset is a synthetic convention for cont-futures stitching; 8 business
        # days matches our equity-index cont-futures roll behavior and keeps exposure away from
        # the final week when liquidity can fragment.
        "BRR": RollRule(
            offset_business_days=8,
            anchor="last_friday",
            contract_months=_MONTHLY_CONTRACT_MONTHS,
        ),
        "MBT": RollRule(
            offset_business_days=8,
            anchor="last_friday",
            contract_months=_MONTHLY_CONTRACT_MONTHS,
        ),
        "ETHUSDRR": RollRule(
            offset_business_days=8,
            anchor="last_friday",
            contract_months=_MONTHLY_CONTRACT_MONTHS,
        ),
        "MET": RollRule(
            offset_business_days=8,
            anchor="last_friday",
            contract_months=_MONTHLY_CONTRACT_MONTHS,
        ),
        # EUR variants (CME crypto reference rate futures).
        "BTCEURRR": RollRule(
            offset_business_days=8,
            anchor="last_friday",
            contract_months=_MONTHLY_CONTRACT_MONTHS,
        ),
        "EBMEUR": RollRule(
            offset_business_days=8,
            anchor="last_friday",
            contract_months=_MONTHLY_CONTRACT_MONTHS,
        ),
        "EEM": RollRule(
            offset_business_days=8,
            anchor="last_friday",
            contract_months=_MONTHLY_CONTRACT_MONTHS,
        ),
        "ETHEURRR": RollRule(
            offset_business_days=8,
            anchor="last_friday",
            contract_months=_MONTHLY_CONTRACT_MONTHS,
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

_US_FUTURES_CALENDAR = None


def _get_us_futures_calendar():
    global _US_FUTURES_CALENDAR
    if _US_FUTURES_CALENDAR is not None:
        return _US_FUTURES_CALENDAR
    try:
        import pandas_market_calendars as mcal  # type: ignore

        _US_FUTURES_CALENDAR = mcal.get_calendar("us_futures")
    except Exception:
        _US_FUTURES_CALENDAR = False
    return _US_FUTURES_CALENDAR


def _previous_us_futures_trading_day(day: date) -> date:
    cal = _get_us_futures_calendar()
    if not cal:
        while day.weekday() >= 5:
            day -= timedelta(days=1)
        return day

    valid = cal.valid_days(start_date=day - timedelta(days=45), end_date=day)
    if valid is None or len(valid) == 0:
        return day
    return valid[-1].date()

def _prior_month_25th_minus_trading_days(year: int, month: int, trading_days_before_25th: int) -> datetime:
    if month == 1:
        anchor_year, anchor_month = year - 1, 12
    else:
        anchor_year, anchor_month = year, month - 1

    anchor_day = date(anchor_year, anchor_month, 25)
    anchor_day = _previous_us_futures_trading_day(anchor_day)

    cal = _get_us_futures_calendar()
    if not cal:
        last_trade_dt = _subtract_business_days(
            _to_timezone(datetime.combine(anchor_day, datetime.min.time())),
            trading_days_before_25th,
        )
        return last_trade_dt.replace(hour=0, minute=0, second=0, microsecond=0)

    valid = cal.valid_days(start_date=anchor_day - timedelta(days=120), end_date=anchor_day)
    if valid is None or len(valid) == 0:
        return _to_timezone(datetime.combine(anchor_day, datetime.min.time()))

    offset = trading_days_before_25th + 1
    if len(valid) < offset:
        target = valid[0].date()
    else:
        target = valid[-offset].date()
    return _to_timezone(datetime.combine(target, datetime.min.time()))

def _last_friday_trading_day(year: int, month: int) -> datetime:
    """Last Friday *trading* day of the given month (holiday-adjusted).

    Several CME crypto futures (IBKR roots like BRR/MBT/ETHUSDRR/MET) expire on the last Friday
    trading day of the month. If that Friday is a holiday (e.g., Good Friday), use the prior
    futures trading day.
    """
    if month == 12:
        next_year, next_month = year + 1, 1
    else:
        next_year, next_month = year, month + 1

    last_day = date(next_year, next_month, 1) - timedelta(days=1)
    cursor = last_day
    while cursor.weekday() != 4:  # Friday
        cursor -= timedelta(days=1)

    cursor = _previous_us_futures_trading_day(cursor)
    return _to_timezone(datetime.combine(cursor, datetime.min.time()))


def _cl_last_trade_date(year: int, month: int) -> datetime:
    """WTI crude oil last trade date for the contract delivery month (CL/MCL).

    Rule: 3 business days prior to the 25th calendar day of the month preceding delivery.
    If the 25th is not a trading day, use the preceding trading day as the anchor.
    """
    return _prior_month_25th_minus_trading_days(year, month, trading_days_before_25th=3)

def _mcl_last_trade_date(year: int, month: int) -> datetime:
    """Micro WTI crude oil last trade date for the contract delivery month (MCL).

    IBKR's `expirationDate` for MCL is typically 1 trading day earlier than CL (4 trading days
    before the prior month's 25th anchor).
    """
    return _prior_month_25th_minus_trading_days(year, month, trading_days_before_25th=4)


def _calculate_roll_trigger(year: int, month: int, rule: RollRule) -> datetime:
    if rule.anchor == "third_friday":
        anchor = _third_friday(year, month)
    elif rule.anchor == "last_friday":
        anchor = _last_friday_trading_day(year, month)
    elif rule.anchor == "third_last_business_day":
        anchor = _third_last_business_day(year, month)
    elif rule.anchor == "cl_last_trade":
        anchor = _cl_last_trade_date(year, month)
    elif rule.anchor == "mcl_last_trade":
        anchor = _mcl_last_trade_date(year, month)
    else:
        anchor = _to_timezone(datetime(year, month, 15))
    if rule.offset_business_days <= 0:
        roll = anchor
    else:
        roll = _subtract_business_days(anchor, rule.offset_business_days)

    # Align roll boundaries away from exact midnight.
    #
    # WHY: Several data sources (notably IBKR Client Portal history) treat request boundaries as
    # effectively exclusive/inclusive in a way that can drop or duplicate bars exactly at the
    # roll timestamp. Shifting by a few minutes keeps the roll deterministic while avoiding
    # edge-case gaps/overwrites at `00:00`.
    return roll + timedelta(minutes=5)


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

    # Some assets (notably monthly futures like CL/MCL) can be past multiple roll points even
    # when the calendar month is in the contract month set. Advance repeatedly until the chosen
    # contract's roll point is in the future to avoid selecting an already-rolled contract month.
    #
    # Guard the loop to ensure we never hang on a misconfigured rule.
    for _ in range(len(contract_months) + 2):
        roll_point = _calculate_roll_trigger(target_year, target_month, rule)
        if ref < roll_point:
            break
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

        if roll_dt <= cursor:
            raise RuntimeError(
                f"Non-increasing roll schedule for {symbol_upper}: roll_dt={roll_dt.isoformat()} cursor={cursor.isoformat()}. "
                "Check the roll rule anchor/offset configuration."
            )

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
