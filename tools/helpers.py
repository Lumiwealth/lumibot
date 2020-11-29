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
