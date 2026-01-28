from __future__ import annotations
import os
import re
import sys
import time
import weakref
from functools import lru_cache
from decimal import Decimal, ROUND_HALF_EVEN

import pytz
import datetime as dt

import pandas as pd
import pandas_market_calendars as mcal
from pandas_market_calendars.market_calendar import MarketCalendar
from termcolor import colored

from ..constants import LUMIBOT_DEFAULT_PYTZ, LUMIBOT_DEFAULT_TIMEZONE

# ============================================================================
# PERFORMANCE CACHES - Critical for backtesting performance
# ============================================================================
# Trading calendar cache.
#
# NOTE: `pandas_market_calendars` schedule generation is surprisingly expensive because it
# computes holidays/valid-days even for tiny windows. Options backtests can call
# `get_trading_days()` thousands of times (ThetaData coverage checks, session-bound clamping,
# order-fill utilities), and caching by the *exact* (start,end) range is not effective when
# callers pass slightly different sliding windows.
#
# We cache schedules by **year** (market, year, tz) and then slice to the requested window.
# Key: (market, year, tz_str)
# Slice cache (best-effort; primarily avoids repeated `.loc[]` slicing for identical windows).
# Key: (market, start_date_str, end_date_str, tz_str)
_TRADING_CALENDAR_CACHE = {}

# Progress bar throttling: when BACKTESTING_QUIET_LOGS=false we print progress as newline-separated
# lines. For fast simulations this can spam thousands of lines in a single second and drown out
# strategy logs. Throttle to at most ~1 line/second per (output, prefix) when not in quiet mode.
_PROGRESS_LAST_PRINT: "weakref.WeakKeyDictionary[object, dict[str, tuple[float, str]]]" = weakref.WeakKeyDictionary()
_PROGRESS_LAST_PRINT_FALLBACK: dict[tuple[int, str], tuple[float, str]] = {}


def _format_datetime_to_tz(dtm, tzinfo: pytz.BaseTzInfo):
    if pd.isna(dtm):
        return dtm
    return pd.Timestamp(dtm).tz_convert(tzinfo).to_pydatetime()


@lru_cache(maxsize=256)
def _get_trading_schedule_for_year(market: str, year: int, tz_name: str) -> pd.DataFrame:
    """Return a cached trading schedule for a full calendar year.

    NOTE: This function is intentionally module-scoped and `lru_cache`'d (thread-safe) to
    avoid repeated expensive schedule builds under backtest concurrency.
    """
    tzinfo = pytz.timezone(tz_name)
    year_start = pd.Timestamp(year=year, month=1, day=1)
    year_end = pd.Timestamp(year=year, month=12, day=31)

    if market == "24/7":
        cal = TwentyFourSevenCalendar(tzinfo=tzinfo)
    else:
        cal = mcal.get_calendar(market)

    schedule = cal.schedule(start_date=year_start, end_date=year_end, tz=tzinfo)
    schedule.market_open = schedule.market_open.apply(lambda v: _format_datetime_to_tz(v, tzinfo))
    schedule.market_close = schedule.market_close.apply(lambda v: _format_datetime_to_tz(v, tzinfo))
    return schedule


def get_chunks(l, chunk_size):
    chunks = []
    for i in range(0, len(l), chunk_size):
        chunks.append(l[i: i + chunk_size])
    return chunks


def deduplicate_sequence(seq, key=""):
    seen = set()
    pos = 0

    if key:
        get_ref = lambda item: getattr(item, key)
    else:
        get_ref = lambda item: item

    for item in seq:
        ref = get_ref(item)
        if ref not in seen:
            seen.add(ref)
            seq[pos] = item
            pos += 1
    del seq[pos:]
    return seq


class TwentyFourSevenCalendar(MarketCalendar):
    """
    Calendar for markets that trade 24/7, like crypto markets.
    Market open is set to midnight (00:00) and close to 23:59 for each day.
    """

    regular_market_times = {
        'market_open': [(None, dt.time(0, 0))],
        'market_close': [(None, dt.time(23, 59, 59, 999999))],
    }

    def __init__(self, tzinfo: str | pytz.BaseTzInfo = 'UTC'):
        self._tzinfo = pytz.timezone(tzinfo) if isinstance(tzinfo, str) else tzinfo
        super().__init__()

    @property
    def name(self):
        return "24/7"

    @property
    def tz(self):
        return self._tzinfo

    @property
    def open_time_default(self):
        return dt.time(0, 0)

    @property
    def close_time_default(self):
        return dt.time(23, 59)

    @property
    def regular_holidays(self):
        return []

    @property
    def special_closes(self):
        return []

    @property
    def special_closes_adherence(self):
        return []

    @property
    def special_opens(self):
        return []

    @property
    def special_opens_adherence(self):
        return []

    def valid_days(self, start_date, end_date, tz=None):
        return pd.date_range(start=start_date, end=end_date, freq='D')


def get_trading_days(
        market="NYSE",
        start_date="1950-01-01",
        end_date=None,
        tzinfo: pytz.tzinfo = pytz.timezone(LUMIBOT_DEFAULT_TIMEZONE)
) -> pd.DataFrame:
    """
    Gets a schedule of trading days and corresponding market open/close times
    for a specified market between given start and end dates, including proper
    timezone handling for datetime objects.

    PERFORMANCE OPTIMIZATION: Caches calendar schedules to avoid expensive
    holiday calculations. Caches by (market, year, tz) and slices to the
    requested window.

    Args:
        market (str, optional): Market identifier for which the trading days
            are to be retrieved. Defaults to "NYSE".
        start_date (str or datetime-like, optional): The start date for the
            range of trading days. Defaults to "1950-01-01".
        end_date (str or datetime-like, optional): The end date (exclusive) for
            the range of trading days. If not specified, the current date is used.
            Defaults to None.
        tzinfo (pytz.timezone, optional): Timezone information used for
            converting datetime objects. Defaults to pytz.timezone(LUMIBOT_DEFAULT_TIMEZONE).

    Returns:
        DataFrame: A pandas DataFrame containing the trading schedule with
            columns for 'market_open' and 'market_close', adjusted to the
            specified timezone.
    """
    if not isinstance(tzinfo, pytz.BaseTzInfo):
        raise TypeError('tzinfo must be a pytz.tzinfo object.')

    def ensure_tz_aware(dtm, tzinfo):
        dtm = pd.to_datetime(dtm)
        return dtm.tz_convert(tzinfo) if dtm.tz is not None else dtm.tz_localize(tzinfo)

    start_date = ensure_tz_aware(start_date, tzinfo)
    if end_date is not None:
        end_date = ensure_tz_aware(end_date, tzinfo)
    else:
        end_date = ensure_tz_aware(get_lumibot_datetime(), tzinfo)

    # Normalize to date-only boundaries (pandas_market_calendars schedules use a tz-naive date index).
    try:
        start_day = pd.Timestamp(start_date.date())
    except Exception:
        start_day = pd.Timestamp(pd.to_datetime(start_date).date())
    try:
        end_day_exclusive = pd.Timestamp(end_date.date())
    except Exception:
        end_day_exclusive = pd.Timestamp(pd.to_datetime(end_date).date())

    # Make end_date exclusive by moving it one day earlier.
    schedule_end_day = end_day_exclusive - pd.Timedelta(days=1)
    if schedule_end_day < start_day:
        return pd.DataFrame(columns=["market_open", "market_close"])

    # Create cache key from market, dates, and timezone
    cache_key = (
        market,
        str(start_date.date()),
        str(end_date.date()),
        str(tzinfo)
    )

    # Check cache first
    if cache_key in _TRADING_CALENDAR_CACHE:
        return _TRADING_CALENDAR_CACHE[cache_key].copy()

    start_year = int(start_day.year)
    end_year = int(schedule_end_day.year)
    tz_name = getattr(tzinfo, "zone", None) or str(tzinfo)
    year_schedules = []
    for year in range(start_year, end_year + 1):
        year_schedules.append(_get_trading_schedule_for_year(market, int(year), tz_name))

    full_schedule = year_schedules[0] if len(year_schedules) == 1 else pd.concat(year_schedules, axis=0)

    # Slice to the requested window (inclusive of schedule_end_day).
    days = full_schedule.loc[start_day:schedule_end_day].copy()

    # Cache the result
    _TRADING_CALENDAR_CACHE[cache_key] = days.copy()

    return days


def get_trading_times(
        pcal: pd.DataFrame,
        timestep: str = 'day'
) -> pd.DatetimeIndex:
    """
    Generate a DatetimeIndex of trading times based on market calendar and timestep

    Parameters:
    -----------
    pcal : pd.DataFrame
        DataFrame with columns 'market_open' and 'market_close' containing datetime objects
    timestep : str
        'day' for daily bars or 'minute' for minute bars

    Returns:
    --------
    pd.DatetimeIndex : Index of all trading times
    """

    if timestep.lower() not in ['day', 'minute']:
        raise ValueError("timestep must be 'day' or 'minute'")

    if timestep.lower() == 'day':
        dates = pd.DatetimeIndex(pcal['market_open'])
        return dates

    # For minute bars, we need to generate minutes between open and close
    trading_minutes = []

    for _, row in pcal.iterrows():
        start = row['market_open']
        end = row['market_close']

        # Check if it's a 24/7 market by checking if close time is 23:59
        is_24_7 = end.hour == 23 and end.minute == 59

        # Generate minute bars between open and close
        minutes = pd.date_range(start=start, end=end, freq='min')

        # Only remove the last minute for non-24/7 markets
        if not is_24_7:
            minutes = minutes[:-1]

        trading_minutes.extend(minutes)

    return pd.DatetimeIndex(trading_minutes)


def date_n_trading_days_from_date(
        n_days: int,
        start_datetime: dt.datetime,
        market: str = "NYSE"
) -> dt.date:
    """
    Get the trading date n_days from start_datetime.
    Positive n_days means going backwards in time (earlier dates).
    Negative n_days means going forwards in time (later dates).
    Works with tz-aware indices and cross-midnight sessions.

    Semantics:
    - n_days > 0: move backward n trading sessions (earlier dates).
    - n_days < 0: move forward |n_days| trading sessions (later dates).
    - The "current" session is determined by the last market_open that is
      less than or equal to start_datetime in the given market tz.
    """
    if n_days == 0:
        return start_datetime.date()
    if not isinstance(start_datetime, dt.datetime):
        raise ValueError("start_datetime must be datetime")

    # Ensure timezone-aware start
    if start_datetime.tzinfo is None:
        start_datetime = LUMIBOT_DEFAULT_PYTZ.localize(start_datetime)

    tzinfo = start_datetime.tzinfo

    # 24/7 special case identical to legacy behavior
    if market == "24/7":
        return (start_datetime - dt.timedelta(days=n_days)).date()

    # Padding for non-trading days/holidays and to cover lookaround range
    buffer_days = max(10, abs(n_days) + (abs(n_days) // 5) * 3)

    # Build date window around start_datetime depending on direction
    if n_days > 0:
        start_date = (start_datetime - dt.timedelta(days=n_days + buffer_days)).date().isoformat()
        end_date = (start_datetime + dt.timedelta(days=1)).date().isoformat()
    else:
        start_date = start_datetime.date().isoformat()
        end_date = (start_datetime + dt.timedelta(days=abs(n_days) + buffer_days + 1)).date().isoformat()

    sched = get_trading_days(
        market=market,
        start_date=start_date,
        end_date=end_date,
        tzinfo=tzinfo,
    )

    if sched.empty:
        # Fallback: no sessions found; return start date to avoid crash
        return start_datetime.date()

    # Determine reference index position based on session DATE (schedule index)
    session_idx = pd.DatetimeIndex(sched.index)
    # Build target date matching index tz-awareness
    if getattr(session_idx, 'tz', None) is None:
        target_date_val = pd.Timestamp(start_datetime.astimezone(tzinfo).date())
    else:
        target_date_val = pd.Timestamp(start_datetime.astimezone(tzinfo).date(), tz=tzinfo)

    # Equivalent to bfill on dates: find first session with date >= target date
    pos = session_idx.searchsorted(target_date_val, side='left')
    if pos >= len(session_idx):
        pos = len(session_idx) - 1

    target_index = pos - n_days  # subtract because positive n_days means go back

    # If target index is outside range, attempt a single retry with larger buffer
    if target_index < 0 or target_index >= len(session_idx):
        extra = abs(n_days) + buffer_days + 30
        if n_days > 0:
            start_date = (start_datetime - dt.timedelta(days=abs(n_days) + extra)).date().isoformat()
            end_date = (start_datetime + dt.timedelta(days=1)).date().isoformat()
        else:
            start_date = start_datetime.date().isoformat()
            end_date = (start_datetime + dt.timedelta(days=abs(n_days) + extra + 1)).date().isoformat()
        sched = get_trading_days(market=market, start_date=start_date, end_date=end_date, tzinfo=tzinfo)
        if sched.empty:
            return start_datetime.date()
        session_idx = pd.DatetimeIndex(sched.index)
        # Match tz-awareness again on retry
        if getattr(session_idx, 'tz', None) is None:
            retry_target = pd.Timestamp(start_datetime.astimezone(tzinfo).date())
        else:
            retry_target = pd.Timestamp(start_datetime.astimezone(tzinfo).date(), tz=tzinfo)
        pos = session_idx.searchsorted(retry_target, side='left')
        if pos >= len(session_idx):
            pos = len(session_idx) - 1
        target_index = pos - n_days

    # Final clamp to valid range (should be valid after retry)
    target_index = max(0, min(target_index, len(session_idx) - 1))

    # Return the trading date (date component of the session index)
    session_date = session_idx[target_index].date()

    return session_date



def is_market_open(
        dtm: dt.datetime,
        market: str = "NYSE"
) -> bool:
    """
    Checks if the market is open at a given timezone-aware datetime.

    Args:
        dtm: A timezone-aware datetime object.
        market: A string representing the market (e.g., "NYSE").

    Returns:
        True if the market is open, False otherwise.
    """
    try:
        cal = mcal.get_calendar(market)
    except RuntimeError:
        print(f"Market calendar '{market}' not found.")
        return False

    try:
        schedule = cal.schedule(
            start_date=dtm - dt.timedelta(days=1),
            end_date=dtm,
            tz=dtm.tzinfo
        )
    except Exception as e:
        print(e)
        return False

    try:
        return cal.open_at_time(schedule, dtm)
    except ValueError:
        return False
    except Exception as e:
        print(e)


class ComparaisonMixin:
    COMPARAISON_PROP = "timestamp"

    def __eq__(self, other):
        return getattr(self, self.COMPARAISON_PROP) == getattr(other, self.COMPARAISON_PROP)

    def __ne__(self, other):
        return getattr(self, self.COMPARAISON_PROP) != getattr(other, self.COMPARAISON_PROP)

    def __gt__(self, other):
        return getattr(self, self.COMPARAISON_PROP) > getattr(other, self.COMPARAISON_PROP)

    def __ge__(self, other):
        return getattr(self, self.COMPARAISON_PROP) >= getattr(other, self.COMPARAISON_PROP)

    def __lt__(self, other):
        return getattr(self, self.COMPARAISON_PROP) < getattr(other, self.COMPARAISON_PROP)

    def __le__(self, other):
        return getattr(self, self.COMPARAISON_PROP) >= getattr(other, self.COMPARAISON_PROP)


def print_progress_bar(
    value,
    start_value,
    end_value,
    backtesting_started,
    file=sys.stdout,
    length=None,
    prefix="Progress",
    suffix="",
    decimals=2,
    fill=chr(9608),
    cash=None,
    portfolio_value=None,
    eta_override=None,
):
    # Progress bar should ALWAYS show, even with quiet logs
    # This is the ONLY output users want to see during quiet backtesting
    total_length = end_value - start_value
    current_length = value - start_value
    percent = min((current_length / total_length) * 100, 100)
    percent_str = ("  {:.%df}" % decimals).format(percent)
    percent_str = percent_str[-decimals - 4 :]

    # Check if quiet logs mode is enabled.
    # When quiet_logs=true: no newline, progress bar overwrites itself in place
    # When quiet_logs=false: add newline so log messages appear on their own lines
    quiet_logs = os.environ.get("BACKTESTING_QUIET_LOGS", "true").lower() == "true"

    # Progress output can be extremely chatty (especially minute-bar backtests) and, in
    # non-interactive log sinks (CloudWatch, CI), carriage returns don't overwrite prior output.
    # Cap progress printing to ~1 line/sec in all modes. Always allow the final 100% line through.
    now_mono = time.monotonic()
    prefix_key = str(prefix)
    try:
        per_file = _PROGRESS_LAST_PRINT.get(file)
        if per_file is None:
            per_file = {}
            _PROGRESS_LAST_PRINT[file] = per_file
        last = per_file.get(prefix_key)
        if last is not None:
            last_time, _ = last
            if (now_mono - last_time) < 1.0 and percent < 100:
                return
        per_file[prefix_key] = (now_mono, percent_str)
    except TypeError:
        # Fallback for file-like objects that aren't weakrefable/hashable.
        key = (id(file), prefix_key)
        last = _PROGRESS_LAST_PRINT_FALLBACK.get(key)
        if last is not None:
            last_time, _ = last
            if (now_mono - last_time) < 1.0 and percent < 100:
                return
        _PROGRESS_LAST_PRINT_FALLBACK[key] = (now_mono, percent_str)

    now = dt.datetime.now()
    elapsed = now - backtesting_started

    if percent > 0:
        if eta_override is not None:
            eta = eta_override
        else:
            eta = (elapsed * (100 / percent)) - elapsed
        eta_str = f"[Elapsed: {str(elapsed).split('.')[0]} ETA: {str(eta).split('.')[0]}]"
    else:
        eta_str = ""

    # Make the simulation datetime string (value is the current backtest datetime)
    sim_date_str = ""
    if hasattr(value, 'strftime'):
        sim_date_str = f"| Sim Time: {value.strftime('%Y-%m-%d %H:%M')}"

    # Make the portfolio value string
    if portfolio_value is not None:
        portfolio_value_str = f"| Val: ${portfolio_value:,.0f}"
    else:
        portfolio_value_str = ""

    if not isinstance(length, int):
        try:
            terminal_length, _ = os.get_terminal_size()
            # Calculate space needed for all components
            fixed_chars = len(prefix) + len(suffix) + decimals + len(eta_str) + len(portfolio_value_str) + len(sim_date_str) + 20
            length = max(10, terminal_length - fixed_chars)
        except:
            length = 30  # Default bar length if terminal size unavailable

    filled_length = int(length * percent / 100)
    bar = fill * filled_length + "-" * (length - filled_length)

    # Build the line and pad with spaces to clear any previous content
    line = f"\r{prefix} |{colored(bar, 'green')}| {percent_str}% {eta_str} {sim_date_str} {portfolio_value_str}"
    # Clear rest of line with ANSI escape code
    line += "\033[K"

    if not quiet_logs:
        line += "\n"

    file.write(line)
    file.flush()


def get_lumibot_datetime():
    return dt.datetime.now().astimezone(LUMIBOT_DEFAULT_PYTZ)


def to_datetime_aware(dt_in):
    """Convert naive time to datetime aware on default timezone."""
    if not dt_in:
        return dt_in
    elif isinstance(dt_in, dt.datetime) and (dt_in.tzinfo is None):
        return LUMIBOT_DEFAULT_PYTZ.localize(dt_in)
    elif isinstance(dt_in, dt.datetime) and (dt_in.tzinfo.utcoffset(dt_in) is None):
        # TODO: This will fail because an exception is thrown if tzinfo is not None.
        return LUMIBOT_DEFAULT_PYTZ.localize(dt_in)
    else:
        return dt_in


def parse_symbol(symbol):
    """
    Parse the given symbol and determine if it's an option or a stock.
    For options, extract and return the stock symbol, expiration date (as a datetime.date object),
    type (call or put), and strike price.
    For stocks, simply return the stock symbol.
    TODO: Crypto and Forex support
    """
    # Check that the symbol is a string
    if not isinstance(symbol, str):
        return {"type": None}
    
    # Pattern to match the option symbol format
    option_pattern = r"([A-Z]+)(\d{6})([CP])(\d+)"

    match = re.match(option_pattern, symbol)
    if match:
        stock_symbol, expiration, option_type, strike_price = match.groups()
        expiration_date = dt.datetime.strptime(expiration, "%y%m%d").date()
        option_type = "CALL" if option_type == "C" else "PUT"
        return {
            "type": "option",
            "stock_symbol": stock_symbol,
            "expiration_date": expiration_date,
            "option_type": option_type,
            "strike_price": round(float(strike_price) / 1000, 3),  # assuming strike price is in thousandths
        }
    else:
        return {"type": "stock", "stock_symbol": symbol}


def create_options_symbol(stock_symbol, expiration_date, option_type, strike_price):
    """
    Create an option symbol string from its components.

    Parameters
    ----------
    stock_symbol : str
        The stock symbol, e.g., 'AAPL'.
    expiration_date : dtm.date or datetime
        The expiration date of the option.
    option_type : str
        The type of the option, either 'Call' or 'Put'.
    strike_price : float
        The strike price of the option.

    Returns
    -------
    str
        The formatted option symbol.
    """
    # Format the expiration date
    if isinstance(expiration_date, str):
        expiration_date = dt.datetime.strptime(expiration_date, "%Y-%m-%d").date()
    expiration_str = expiration_date.strftime("%y%m%d")

    # Determine the option type character
    option_char = "C" if option_type.lower() == "call" else "P"

    # Format the strike price, assuming it needs to be in thousandths
    strike_price_str = f"{int(strike_price * 1000):08d}"

    return f"{stock_symbol}{expiration_str}{option_char}{strike_price_str}"


@lru_cache(maxsize=256)
def _parse_timestep_qty_and_unit_cached(timestep_str: str) -> tuple[int, str]:
    """Cached implementation of `parse_timestep_qty_and_unit()`.

    This is a hot path in backtesting: strategies frequently request history using the same
    few timesteps (`minute`, `day`, and common multi-minute multiples). Caching avoids repeated
    regex parsing and normalization work.
    """
    quantity = 1
    unit = timestep_str
    m = re.search(r"(\d+)\s*(\w+)", timestep_str)
    if m:
        quantity = int(m.group(1))
        unit = m.group(2).rstrip("s")  # remove trailing 's' if any

    raw_unit = str(unit or "").strip().lower()
    canonical_unit = {
        "m": "minute",
        "min": "minute",
        "minute": "minute",
        "h": "hour",
        "hr": "hour",
        "hour": "hour",
        "d": "day",
        "day": "day",
    }.get(raw_unit, raw_unit)

    return quantity, canonical_unit


def parse_timestep_qty_and_unit(timestep):
    """
    Parse the timestep string and return the quantity and unit.

    Parameters
    ----------
    timestep : str
        The timestep string to parse.

    Returns
    -------
    tuple
        The quantity and unit.
    """
    return _parse_timestep_qty_and_unit_cached(str(timestep or ""))


def get_decimals(number):
    return len(str(number).split('.')[-1]) if '.' in str(number) else 0


def quantize_to_num_decimals(num: float, num_decimals: int) -> float:
    if isinstance(num, Decimal):
        num = num
    elif isinstance(num, float):
        num = Decimal(str(num))
    else:
        raise ValueError(f"{num} is not a Decimal or float")

    # Create the proper decimal format (e.g., '0.01' for 2 decimals)
    decimal_format = Decimal('0.' + '0' * num_decimals)

    # quantize num using ROUND_HALF_EVEN
    quantized_num = num.quantize(decimal_format, rounding=ROUND_HALF_EVEN)
    return float(quantized_num)


def has_more_than_n_decimal_places(number: float, n: int) -> bool:
    """Return True if the number has more than n decimal places, False otherwise."""

    # Convert the number to a string
    number_str = str(number)

    # Split the string at the decimal point
    if '.' in number_str:
        decimal_part = number_str.split('.')[1]
        # Check if the length of the decimal part is greater than n
        return len(decimal_part) > n
    else:
        return False


def get_timezone_from_datetime(dtm: dt.datetime) -> pytz.timezone:
    """Convert datetime's timezone to pytz.timezone, handling both pytz and zoneinfo cases"""
    if dtm.tzinfo is None:
        return LUMIBOT_DEFAULT_PYTZ

    # If it's already a pytz timezone (checking both DstTzInfo and StaticTzInfo)
    if isinstance(dtm.tzinfo, (pytz.tzinfo.DstTzInfo, pytz.tzinfo.StaticTzInfo)):
        return dtm.tzinfo

    # Try different ways to get timezone name
    try:
        # Try key or zone attribute (works for both zoneinfo and pytz)
        if hasattr(dtm.tzinfo, 'key'):
            return pytz.timezone(dtm.tzinfo.key)
        elif hasattr(dtm.tzinfo, 'zone'):
            return pytz.timezone(dtm.tzinfo.zone)
        # Try getting string representation (fallback)
        timezone_name = str(dtm.tzinfo)
        return pytz.timezone(timezone_name)
    except (AttributeError, pytz.exceptions.UnknownTimeZoneError):
        return LUMIBOT_DEFAULT_PYTZ
