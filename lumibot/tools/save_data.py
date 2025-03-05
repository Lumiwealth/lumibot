import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from lumibot.credentials import ALPACA_TEST_CONFIG
from lumibot.backtesting import AlpacaBacktesting

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if __name__ == "__main__":

    datetime_start = datetime(2025, 1, 13)
    datetime_end = datetime(2025, 1, 18)
    timestep = 'day'
    tzinfo = ZoneInfo("America/New_York")
    tickers = "TSLA"
    refresh_cache = False
    warm_up_trading_days = 0

    data_source = AlpacaBacktesting(
        datetime_start=datetime_start,
        datetime_end=datetime_end,
        config=ALPACA_TEST_CONFIG,
        tickers=tickers,
        timestep=timestep,
        refresh_cache=refresh_cache,
        tzinfo=tzinfo,
        warm_up_trading_days=warm_up_trading_days
    )
