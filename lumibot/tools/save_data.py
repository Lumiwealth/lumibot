import logging
import os
import sys

from lumibot.credentials import ALPACA_TEST_CONFIG
from lumibot.backtesting import AlpacaBacktesting

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if __name__ == "__main__":

    tickers = [
        "BTC/USD",
        "AMZN"
    ]
    start_date = "2021-01-01"
    end_date = "2021-01-10"
    # timestep = 'day'
    timestep = 'hour'
    # timestep = 'minute'
    refresh_cache = False

    data_source = AlpacaBacktesting(
        tickers=tickers,
        start_date=start_date,
        end_date=end_date,
        timestep=timestep,
        config=ALPACA_TEST_CONFIG,
        refresh_cache=refresh_cache,
    )
