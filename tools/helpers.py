import logging

import yfinance as yf


def get_chunks(l, chunk_size):
    chunks = []
    for i in range(0, len(l), chunk_size):
        chunks.append(l[i : i + chunk_size])
    return chunks


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
