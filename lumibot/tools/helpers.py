import os
import re
import sys
import datetime as dt

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


def get_trading_days(market="NYSE", start_date="1950-01-01", end_date=None):
    format_datetime = lambda dt: dt.to_pydatetime().astimezone(LUMIBOT_DEFAULT_PYTZ)
    start_date = to_datetime_aware(pd.to_datetime(start_date))
    today = get_lumibot_datetime()
    # macl's "24/7" calendar doesn't return consecutive days, so need to be generated manually.
    if market == "24/7":
        market_open = pd.date_range(
            start=start_date, end=end_date or today).to_frame(index=False,name="market_open")
        market_close = pd.date_range(
            start=start_date.replace(hour=23,minute=59,second=59,microsecond=999999), 
            end=end_date or today.replace(hour=23,minute=59,second=59,microsecond=999999)).to_frame(index=False,name="market_close")
        index = pd.date_range(
            start=start_date.replace(tzinfo=None), end=end_date or today.replace(tzinfo=None))
        days = pd.concat([market_open, market_close], axis=1)
        days.index = index
    else:
        nyse = mcal.get_calendar(market)
        days = nyse.schedule(start_date=start_date, end_date=end_date or today)
        days.market_open = days.market_open.apply(format_datetime)
        days.market_close = days.market_close.apply(format_datetime)
    return days


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

    now = dt.datetime.now()
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
    return dt.datetime.now().astimezone(LUMIBOT_DEFAULT_PYTZ)


def to_datetime_aware(dt_in):
    """Convert naive time to datetime aware on default timezone."""
    if not dt_in:
        return dt_in
    elif isinstance(dt_in, dt.datetime) and (dt_in.tzinfo is None or dt_in.tzinfo.utcoffset(dt_in) is None):
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
    expiration_date : dt.date or dt.datetime
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
