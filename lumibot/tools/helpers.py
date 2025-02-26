import os
import re
import sys
from datetime import datetime, timedelta, date

import pandas_market_calendars as mcal
from termcolor import colored

from lumibot import LUMIBOT_DEFAULT_PYTZ
import pandas as pd


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

import pandas_market_calendars as mcal
from pandas_market_calendars.market_calendar import MarketCalendar
from datetime import time
from pytz import timezone


class TwentyFourSevenCalendar(MarketCalendar):
    @property
    def name(self):
        return "24/7"

    @property
    def tz(self):
        return timezone('UTC')

    @property
    def open_time_default(self):
        return time(0, 0)

    @property
    def close_time_default(self):
        return time(23, 59, 59)

    @property
    def regular_holidays(self):
        return []

    @property
    def special_closes(self):
        return []

    @property
    def special_opens(self):
        return []


def get_trading_days(market="NYSE", start_date="1950-01-01", end_date=None):
    format_datetime = lambda dt: dt.to_pydatetime().astimezone(LUMIBOT_DEFAULT_PYTZ)

    # Ensure start_date and end_date are datetime or Timestamp objects
    start_date = to_datetime_aware(pd.to_datetime(start_date))
    end_date = to_datetime_aware(pd.to_datetime(end_date)) if end_date else to_datetime_aware(pd.to_datetime(get_lumibot_datetime()))

    cal = mcal.get_calendar(market)
    days = cal.schedule(start_date=start_date, end_date=end_date)
    days.market_open = days.market_open.apply(format_datetime)
    days.market_close = days.market_close.apply(format_datetime)
    return days


def date_n_days_from_date(
        n_bars: int,
        start_datetime: datetime,
        market: str = "NYSE",
) -> date:

    if n_bars == 0:
        return start_datetime.date()
    if not isinstance(start_datetime, datetime):
        raise ValueError("start_datetime must be datetime")

    # Remove timezone information from a datetime object
    start_datetime = start_datetime.replace(tzinfo=None)

    # Let's add 3 days per week for weekends and holidays.
    weeks_requested = n_bars // 5  # Full trading week is 5 days
    extra_padding_days = weeks_requested * 3  # to account for 3day weekends
    buffer_bars = max(10, n_bars + extra_padding_days)  # Get at least 10 days

    # Get trading days around the backtesting_start date
    trading_days = get_trading_days(
        market=market,
        start_date=(start_datetime - timedelta(days=n_bars+buffer_bars)).date().isoformat(),
        end_date=(start_datetime + timedelta(days=n_bars+buffer_bars)).date().isoformat(),
    )

    # Check if start_datetime is in trading_days
    if start_datetime in trading_days.index:
        start_index = trading_days.index.get_loc(start_datetime)
    else:
        # Find the first trading date after start_datetime
        start_index = trading_days.index.get_indexer([start_datetime], method='bfill')[0]

    # get the date of the last trading n_bars before the start_datetime date
    date_n_bars_away = trading_days.index[start_index - n_bars].date()
    return date_n_bars_away


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
):
    total_length = end_value - start_value
    current_length = value - start_value
    percent = min((current_length / total_length) * 100, 100)
    percent_str = ("  {:.%df}" % decimals).format(percent)
    percent_str = percent_str[-decimals - 4 :]

    now = datetime.now()
    elapsed = now - backtesting_started

    if percent > 0:
        eta = (elapsed * (100 / percent)) - elapsed
        eta_str = f"[Elapsed: {str(elapsed).split('.')[0]} ETA: {str(eta).split('.')[0]}]"
    else:
        eta_str = ""

    # Make the portfolio value string
    if portfolio_value is not None:
        portfolio_value_str = f"Portfolio Val: {portfolio_value:,.2f}"
    else:
        portfolio_value_str = ""

    if not isinstance(length, int):
        try:
            terminal_length, _ = os.get_terminal_size()
            length = max(
                0,
                terminal_length - len(prefix) - len(suffix) - decimals - len(eta_str) - len(portfolio_value_str) - 13,
            )
        except:
            length = 0

    filled_length = int(length * percent / 100)
    bar = fill * filled_length + "-" * (length - filled_length)

    line = f"\r{prefix} |{colored(bar, 'green')}| {percent_str}% {suffix} {eta_str} {portfolio_value_str}"
    file.write(line)
    file.flush()


def get_lumibot_datetime():
    return datetime.now().astimezone(LUMIBOT_DEFAULT_PYTZ)


def to_datetime_aware(dt_in):
    """Convert naive time to datetime aware on default timezone."""
    if not dt_in:
        return dt_in
    elif isinstance(dt_in, datetime) and (dt_in.tzinfo is None):
        return LUMIBOT_DEFAULT_PYTZ.localize(dt_in)
    elif isinstance(dt_in, datetime) and (dt_in.tzinfo.utcoffset(dt_in) is None):
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
        expiration_date = datetime.strptime(expiration, "%y%m%d").date()
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
    expiration_date : dt.date or datetime
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
        expiration_date = datetime.strptime(expiration_date, "%Y-%m-%d").date()
    expiration_str = expiration_date.strftime("%y%m%d")

    # Determine the option type character
    option_char = "C" if option_type.lower() == "call" else "P"

    # Format the strike price, assuming it needs to be in thousandths
    strike_price_str = f"{int(strike_price * 1000):08d}"

    return f"{stock_symbol}{expiration_str}{option_char}{strike_price_str}"


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

    quantity = 1
    unit = timestep
    m = re.search(r"(\d+)\s*(\w+)", timestep)
    if m:
        quantity = int(m.group(1))
        unit = m.group(2).rstrip("s")  # remove trailing 's' if any

    return quantity, unit


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