import logging
import os
import sys

import yfinance as yf


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


def get_trading_days():
    """Requesting data for the oldest company,
    Consolidated Edison from yahoo finance.
    Storing the trading days."""
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logging.info("Fetching past trading days")
    ticker = yf.Ticker("ED")
    history = ticker.history(period="max")
    days = [d.date() for d in history.index]
    return days


def df_day_deduplicate(df_):
    df_copy = df_.copy()
    df_copy["should_keep"] = True
    n_rows = len(df_copy.index)
    for index, row in df_copy.iterrows():
        position = df_copy.index.get_loc(index)
        if position + 1 == n_rows:
            break

        if index.date() == df_copy.index[position + 1].date():
            df_copy.loc[index, "should_keep"] = False

    df_copy = df_copy[df_copy["should_keep"]]
    df_copy = df_copy.drop(["should_keep"], axis=1)
    return df_copy


def add_comparaison_mixins(class_obj, scalar_prop):
    def __eq__(self, other):
        return getattr(self, scalar_prop) == getattr(other, scalar_prop)

    def __ne__(self, other):
        return getattr(self, scalar_prop) != getattr(other, scalar_prop)

    def __gt__(self, other):
        return getattr(self, scalar_prop) > getattr(other, scalar_prop)

    def __ge__(self, other):
        return getattr(self, scalar_prop) >= getattr(other, scalar_prop)

    def __lt__(self, other):
        return getattr(self, scalar_prop) < getattr(other, scalar_prop)

    def __le__(self, other):
        return getattr(self, scalar_prop) >= getattr(other, scalar_prop)

    class_obj.__eq__ = __eq__
    class_obj.__ne__ = __ne__
    class_obj.__gt__ = __gt__
    class_obj.__ge__ = __ge__
    class_obj.__lt__ = __lt__
    class_obj.__le__ = __le__


def print_progress_bar(
    value,
    start_value,
    end_value,
    file=sys.stdout,
    length=None,
    prefix="Progress",
    suffix="Complete",
    decimals=2,
    fill=chr(9608),
):
    total_length = end_value - start_value
    current_length = value - start_value
    percent = min((current_length / total_length) * 100, 100)
    percent_str = ("  {:.%df}" % decimals).format(percent)
    percent_str = percent_str[-decimals - 4 :]
    if not isinstance(length, int):
        try:
            terminal_length, _ = os.get_terminal_size()
            length = max(0, terminal_length - len(prefix) - len(suffix) - decimals - 10)
        except:
            length = 0

    filled_length = int(length * percent / 100)
    bar = fill * filled_length + "-" * (length - filled_length)
    line = f"\r{prefix} |{bar}| {percent_str}% {suffix}"
    file.write(line)
    file.flush()
