"""Centralised futures roll logic shared by assets, data sources, and brokers."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from functools import lru_cache
from typing import Dict, List, Optional, Tuple

import pytz

from lumibot.constants import LUMIBOT_DEFAULT_PYTZ
from lumibot.tools.lumibot_logger import get_logger

logger = get_logger(__name__)

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
    roll_anchor: Optional[str] = None


_DEFAULT_CONTRACT_MONTHS: Tuple[int, ...] = (3, 6, 9, 12)
_MONTHLY_CONTRACT_MONTHS: Tuple[int, ...] = tuple(range(1, 13))


# Roll rules by product root.
#
# ``anchor`` is the contract's last trade day rule and ``contract_months`` the delivery months
# that become the front (most traded) contract. ``roll_anchor="first_notice"`` rolls a number of
# business days before the last business day of the month before delivery, so a continuous
# series never holds a physically delivered contract into its notice period. Without a
# ``roll_anchor`` the roll is ``offset_business_days`` before the last trade day.
# The offsets are LumiBot's roll convention (close to when volume moves), not exchange rules.
# Last trade rules and listed months come from the exchange contract specifications cited on
# each group (https://www.cmegroup.com/markets/<group>/<product>.contractSpecs.html,
# https://www.cboe.com/tradable_products/vix/vix_futures/specifications/).
_QUARTERLY: Tuple[int, ...] = _DEFAULT_CONTRACT_MONTHS


def _rules(symbols: str, **kwargs) -> Dict[str, RollRule]:
    return {symbol: RollRule(**kwargs) for symbol in symbols.split()}


ROLL_RULES: Dict[str, RollRule] = {
    # CME/CBOT equity index (E-mini and Micro S&P 500, Nasdaq-100, Russell 2000, Dow):
    # quarterly, last trade on the third Friday of the contract month.
    **_rules("ES MES NQ MNQ RTY M2K YM MYM", offset_business_days=8, anchor="third_friday",
             contract_months=_QUARTERLY),
}

ROLL_RULES.update(
    {
        "GC": RollRule(
            offset_business_days=7,
            anchor="third_last_business_day",
            contract_months=(2, 4, 6, 8, 10, 12),
            roll_anchor="metal_first_notice",
        ),
        "MGC": RollRule(
            offset_business_days=7,
            anchor="third_last_business_day",
            contract_months=(2, 4, 6, 8, 10, 12),
            roll_anchor="metal_first_notice",
        ),
        # COMEX silver (SI) and micro silver (SIL): front months are Mar, May, Jul, Sep, Dec;
        # last trade on the third last business day of the contract month; first notice is the
        # last business day of the month before.
        **_rules("SI SIL", offset_business_days=7, anchor="third_last_business_day",
                 contract_months=(3, 5, 7, 9, 12), roll_anchor="first_notice"),
        # COMEX copper (HG): same month cycle and notice rule as silver.
        "HG": RollRule(offset_business_days=7, anchor="third_last_business_day",
                       contract_months=(3, 5, 7, 9, 12), roll_anchor="first_notice"),
        # COMEX micro copper (MHG): cash settled, last trade on the third last business day of
        # the month BEFORE the contract month.
        "MHG": RollRule(offset_business_days=5, anchor="third_last_business_day_prior_month",
                        contract_months=(3, 5, 7, 9, 12)),
        # NYMEX platinum (PL): front months are Jan, Apr, Jul, Oct; last trade on the third
        # last business day of the contract month.
        "PL": RollRule(offset_business_days=7, anchor="third_last_business_day",
                       contract_months=(1, 4, 7, 10), roll_anchor="first_notice"),
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
        # Henry Hub gas trades every delivery month and expires three trading
        # days before that month starts (CME/NYMEX rule 220102.F). The five-day
        # roll offset is LumiBot's synthetic energy convention, not an expiry.
        "NG": RollRule(
            offset_business_days=5,
            anchor="ng_last_trade",
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
        # CME Globex symbols for the same bitcoin and ether contracts.
        **_rules("BTC ETH", offset_business_days=8, anchor="last_friday",
                 contract_months=_MONTHLY_CONTRACT_MONTHS),
        # NYMEX micro Henry Hub gas (MNG): monthly, last trade on the fourth last business day
        # of the month before the contract month (one day before NG).
        "MNG": RollRule(offset_business_days=5, anchor="fourth_last_business_day_prior_month",
                        contract_months=_MONTHLY_CONTRACT_MONTHS),
        # NYMEX RBOB gasoline (RB) and NY Harbor ULSD (HO): monthly, last trade on the last
        # business day of the month before the contract month.
        **_rules("RB HO", offset_business_days=5, anchor="last_business_day_prior_month",
                 contract_months=_MONTHLY_CONTRACT_MONTHS),
        # CBOT Treasury futures: quarterly; delivery notices start at the end of the month
        # before delivery. 10-year (ZN), Ultra 10-year (TN), bond (ZB) and Ultra bond (UB) stop
        # trading seven business days before the last business day of the contract month;
        # 2-year (ZT) and 5-year (ZF) on the last business day.
        **_rules("ZN TN ZB UB", offset_business_days=5, anchor="eighth_last_business_day",
                 contract_months=_QUARTERLY, roll_anchor="first_notice"),
        **_rules("ZT ZF", offset_business_days=5, anchor="last_business_day",
                 contract_months=_QUARTERLY, roll_anchor="first_notice"),
        # CME FX futures: quarterly, last trade two business days before the third Wednesday
        # (Canadian dollar: one business day before).
        **_rules("6E 6J 6B 6A 6S 6N M6E M6A M6B", offset_business_days=5, anchor="fx_last_trade",
                 contract_months=_QUARTERLY),
        "6C": RollRule(offset_business_days=5, anchor="fx_cad_last_trade", contract_months=_QUARTERLY),
        # CBOT grains and oilseeds: last trade on the business day before the 15th of the
        # contract month; delivery notices start at the end of the month before.
        # Corn (ZC) and wheat (ZW): Mar, May, Jul, Sep, Dec.
        **_rules("ZC ZW", offset_business_days=5, anchor="business_day_before_15th",
                 contract_months=(3, 5, 7, 9, 12), roll_anchor="first_notice"),
        # Soybeans (ZS): Jan, Mar, May, Jul, Aug, Sep, Nov.
        "ZS": RollRule(offset_business_days=5, anchor="business_day_before_15th",
                       contract_months=(1, 3, 5, 7, 8, 9, 11), roll_anchor="first_notice"),
        # Soybean oil (ZL) and meal (ZM): Jan, Mar, May, Jul, Aug, Sep, Oct, Dec.
        **_rules("ZL ZM", offset_business_days=5, anchor="business_day_before_15th",
                 contract_months=(1, 3, 5, 7, 8, 9, 10, 12), roll_anchor="first_notice"),
        # CME live cattle (LE): Feb, Apr, Jun, Aug, Oct, Dec; physically delivered, last trade
        # on the last business day of the contract month.
        "LE": RollRule(offset_business_days=5, anchor="last_business_day",
                       contract_months=(2, 4, 6, 8, 10, 12), roll_anchor="first_notice"),
        # CME lean hogs (HE): Feb, Apr, May, Jun, Jul, Aug, Oct, Dec; cash settled, last trade
        # on the tenth business day of the contract month.
        "HE": RollRule(offset_business_days=5, anchor="tenth_business_day",
                       contract_months=(2, 4, 5, 6, 7, 8, 10, 12)),
        # Cboe VIX futures (VX): monthly; final settlement on the Wednesday 30 days before the
        # third Friday of the following month.
        "VX": RollRule(offset_business_days=5, anchor="vx_expiry",
                       contract_months=_MONTHLY_CONTRACT_MONTHS),
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

_CME_BUSINESS_DAY_CALENDAR = None


def _get_cme_business_day_calendar():
    """CME business (trade) days. Unlike the Globex session calendar it excludes
    holidays such as Memorial Day or Thanksgiving even when Globex is open."""
    global _CME_BUSINESS_DAY_CALENDAR
    if _CME_BUSINESS_DAY_CALENDAR is not None:
        return _CME_BUSINESS_DAY_CALENDAR
    try:
        import pandas_market_calendars as mcal  # type: ignore

        _CME_BUSINESS_DAY_CALENDAR = mcal.get_calendar("CME_TradeDate")
    except Exception:
        _CME_BUSINESS_DAY_CALENDAR = False
    return _CME_BUSINESS_DAY_CALENDAR


def _prior_month_25th_minus_trading_days(year: int, month: int, trading_days_before_25th: int) -> datetime:
    """NYMEX crude rule, counted in CME business days.

    Counting on the Globex session calendar treated Memorial Day, Juneteenth,
    Thanksgiving and similar holidays as business days and put the last trade a
    day late (2026-11-13: CLM26 computed 2026-05-20, IBKR says 2026-05-19).
    Verified against every recorded IBKR CL/MCL last trade date 2024-2037.
    """
    if month == 1:
        anchor_year, anchor_month = year - 1, 12
    else:
        anchor_year, anchor_month = year, month - 1

    anchor_day = date(anchor_year, anchor_month, 25)
    cal = _get_cme_business_day_calendar()
    if cal:
        valid = cal.valid_days(start_date=anchor_day - timedelta(days=45), end_date=anchor_day)
        if valid is not None and len(valid):
            anchor_day = valid[-1].date()
    else:
        anchor_day = _previous_us_futures_trading_day(anchor_day)

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


@lru_cache(maxsize=4096)
def _ng_last_trade_date(year: int, month: int) -> datetime:
    """Henry Hub gas: three trading days before the first delivery-month day."""
    delivery_start = date(year, month, 1)
    # Expiry business days exclude holidays such as Thanksgiving even when
    # Globex opens for a shortened session. Use CME's trade-date calendar.
    import pandas_market_calendars as mcal
    cal = mcal.get_calendar("CME_TradeDate")
    valid = cal.valid_days(start_date=delivery_start - timedelta(days=45),
                           end_date=delivery_start - timedelta(days=1))
    if valid is None or len(valid) < 3:
        raise ValueError("CME expiry calendar did not provide three prior business days")
    return _to_timezone(datetime.combine(valid[-3].date(), datetime.min.time()))



@lru_cache(maxsize=2048)
def _month_business_days(year: int, month: int) -> Tuple[date, ...]:
    """Exchange business days of one month (CME trade dates; weekdays when no calendar)."""
    first = date(year, month, 1)
    last = (date(year + (month == 12), month % 12 + 1, 1)) - timedelta(days=1)
    cal = _get_cme_business_day_calendar()
    if cal:
        valid = cal.valid_days(start_date=first, end_date=last)
        if valid is not None and len(valid):
            return tuple(day.date() for day in valid)
    return tuple(first + timedelta(days=i) for i in range((last - first).days + 1)
                 if (first + timedelta(days=i)).weekday() < 5)


def _previous_month(year: int, month: int) -> YearMonth:
    return (year - 1, 12) if month == 1 else (year, month - 1)


def _at_midnight(day: date) -> datetime:
    return _to_timezone(datetime.combine(day, datetime.min.time()))


def _business_day_on_or_before(day: date) -> date:
    for _ in range(10):
        if day in _month_business_days(day.year, day.month):
            return day
        day -= timedelta(days=1)
    return day


def _business_days_before(day: date, count: int) -> date:
    """The business day ``count`` business days before ``day`` (``day`` itself not counted)."""
    while count > 0:
        day = _business_day_on_or_before(day - timedelta(days=1))
        count -= 1
    return day


def _third_wednesday(year: int, month: int) -> date:
    first = date(year, month, 1)
    return first + timedelta(days=(2 - first.weekday()) % 7 + 14)


def _vx_expiry(year: int, month: int) -> datetime:
    following = (year + 1, 1) if month == 12 else (year, month + 1)
    third_friday = _third_friday(*following).date()
    return _at_midnight(_business_day_on_or_before(third_friday - timedelta(days=30)))


# Last trade day by anchor name: (contract year, contract month) -> datetime.
_EXPIRY_ANCHORS = {
    "third_friday": _third_friday,
    "last_friday": _last_friday_trading_day,
    "third_last_business_day": _third_last_business_day,
    "cl_last_trade": _cl_last_trade_date,
    "mcl_last_trade": _mcl_last_trade_date,
    "ng_last_trade": _ng_last_trade_date,
    "last_business_day": lambda y, m: _at_midnight(_month_business_days(y, m)[-1]),
    "eighth_last_business_day": lambda y, m: _at_midnight(_month_business_days(y, m)[-8]),
    "tenth_business_day": lambda y, m: _at_midnight(_month_business_days(y, m)[9]),
    "last_business_day_prior_month": lambda y, m: _at_midnight(
        _month_business_days(*_previous_month(y, m))[-1]),
    "third_last_business_day_prior_month": lambda y, m: _at_midnight(
        _month_business_days(*_previous_month(y, m))[-3]),
    "fourth_last_business_day_prior_month": lambda y, m: _at_midnight(
        _month_business_days(*_previous_month(y, m))[-4]),
    "business_day_before_15th": lambda y, m: _at_midnight(_business_days_before(date(y, m, 15), 1)),
    "fx_last_trade": lambda y, m: _at_midnight(_business_days_before(_third_wednesday(y, m), 2)),
    "fx_cad_last_trade": lambda y, m: _at_midnight(_business_days_before(_third_wednesday(y, m), 1)),
    "vx_expiry": _vx_expiry,
}


def contract_last_trade_date(symbol: str, year: int, month: int) -> Optional[datetime]:
    """Last trade day of ``symbol``'s contract for a delivery month, or None when no rule is known."""
    rule = ROLL_RULES.get(str(symbol).upper())
    resolve = _EXPIRY_ANCHORS.get(rule.anchor) if rule else None
    return resolve(year, month) if resolve else None


def supported_roll_symbols() -> Tuple[str, ...]:
    """Product roots that have their own roll rule."""
    return tuple(sorted(ROLL_RULES))


@lru_cache(maxsize=8192)
def _calculate_roll_trigger(year: int, month: int, rule: RollRule) -> datetime:
    # RollRule is immutable and part of the key: replacing a rule cannot reuse
    # the old date. Calendar failures raise and are never cached by lru_cache.
    roll_anchor = rule.roll_anchor or rule.anchor
    if roll_anchor in ("metal_first_notice", "first_notice"):
        # COMEX 706.C: first notice is the last business day of the prior
        # month, well before the last trade in the delivery month. Keeping
        # the latter as the roll anchor selects thin delivery-month gold.
        # Seven business days is our synthetic roll convention, not a CME
        # mandated roll date. Preserve actual expiry in rule.anchor.
        import pandas_market_calendars as mcal

        delivery_start = date(year, month, 1)
        valid = mcal.get_calendar("CME_TradeDate").valid_days(
            start_date=delivery_start - timedelta(days=60),
            end_date=delivery_start - timedelta(days=1),
        )
        offset = max(0, rule.offset_business_days) + 1
        if valid is None or len(valid) < offset:
            raise ValueError("CME notice calendar did not provide enough prior business days")
        roll = _to_timezone(datetime.combine(valid[-offset].date(), datetime.min.time()))
        return roll + timedelta(minutes=5)
    resolve = _EXPIRY_ANCHORS.get(roll_anchor)
    anchor = resolve(year, month) if resolve else _to_timezone(datetime(year, month, 15))
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


_DEFAULT_RULE_NOTED: set = set()


def _note_default_rule(symbol: str) -> None:
    """Say once per symbol that the generic rule is in use, so a wrong roll is easy to spot."""
    if symbol in _DEFAULT_RULE_NOTED:
        return
    _DEFAULT_RULE_NOTED.add(symbol)
    logger.warning(
        "No futures roll rule for %s: using the default (March, June, September, December contracts, "
        "rolling on the 15th of the contract month). Add the product to "
        "lumibot.tools.futures_roll.ROLL_RULES if it trades other months.", symbol)


def determine_contract_year_month(symbol: str, reference_date: Optional[datetime] = None) -> YearMonth:
    ref = _normalize_reference_date(reference_date)
    symbol_upper = symbol.upper()
    rule = ROLL_RULES.get(symbol_upper)
    year = ref.year
    month = ref.month

    if rule is None:
        _note_default_rule(symbol_upper)
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
