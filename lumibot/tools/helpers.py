import os
import sys
from datetime import datetime

import pandas as pd
import pandas_market_calendars as mcal
from lumibot import LUMIBOT_DEFAULT_PYTZ
from termcolor import colored


def get_chunks(l, chunk_size):
    chunks = []
    for i in range(0, len(l), chunk_size):
        chunks.append(l[i : i + chunk_size])
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
    today = get_lumibot_datetime().date()
    nyse = mcal.get_calendar(market)
    days = nyse.schedule(start_date=start_date, end_date=end_date or today)
    days.market_open = days.market_open.apply(format_datetime)
    days.market_close = days.market_close.apply(format_datetime)
    return days


class ComparaisonMixin:
    COMPARAISON_PROP = "timestamp"

    def __eq__(self, other):
        return getattr(self, self.COMPARAISON_PROP) == getattr(
            other, self.COMPARAISON_PROP
        )

    def __ne__(self, other):
        return getattr(self, self.COMPARAISON_PROP) != getattr(
            other, self.COMPARAISON_PROP
        )

    def __gt__(self, other):
        return getattr(self, self.COMPARAISON_PROP) > getattr(
            other, self.COMPARAISON_PROP
        )

    def __ge__(self, other):
        return getattr(self, self.COMPARAISON_PROP) >= getattr(
            other, self.COMPARAISON_PROP
        )

    def __lt__(self, other):
        return getattr(self, self.COMPARAISON_PROP) < getattr(
            other, self.COMPARAISON_PROP
        )

    def __le__(self, other):
        return getattr(self, self.COMPARAISON_PROP) >= getattr(
            other, self.COMPARAISON_PROP
        )


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
        eta_str = (
            f"[Elapsed: {str(elapsed).split('.')[0]} ETA: {str(eta).split('.')[0]}]"
        )
    else:
        eta_str = ""

    if not isinstance(length, int):
        try:
            terminal_length, _ = os.get_terminal_size()
            length = max(
                0,
                terminal_length
                - len(prefix)
                - len(suffix)
                - decimals
                - len(eta_str)
                - 12,
            )
        except:
            length = 0

    filled_length = int(length * percent / 100)
    bar = fill * filled_length + "-" * (length - filled_length)

    line = f"\r{prefix} |{colored(bar, 'green')}| {percent_str}% {suffix} {eta_str}"
    file.write(line)
    file.flush()


def get_lumibot_datetime():
    return datetime.now().astimezone(LUMIBOT_DEFAULT_PYTZ)


def to_datetime_aware(dt):
    """Convert naive time to datetime aware on default timezone. """
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        return LUMIBOT_DEFAULT_PYTZ.localize(dt)
    else:
        return dt
