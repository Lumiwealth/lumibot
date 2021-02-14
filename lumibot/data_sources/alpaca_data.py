from datetime import datetime

import alpaca_trade_api as tradeapi
import pandas as pd
from alpaca_trade_api.common import URL
from alpaca_trade_api.entity import Bar

from lumibot.entities import Bars

from .data_source import DataSource


class AlpacaData(DataSource):
    SOURCE = "ALPACA"
    MIN_TIMESTEP = "minute"
    TIMESTEP_MAPPING = [
        {"timestep": "minute", "represntations": ["1Min", "minute"]},
        {"timestep": "day", "represntations": ["1D", "day"]},
    ]

    """Common base class for data_sources/alpaca and brokers/alpaca"""

    @staticmethod
    def _format_datetime(dt):
        return pd.Timestamp(dt).isoformat()

    def __init__(self, config, max_workers=20, chunk_size=100):
        # Alpaca authorize 200 requests per minute and per API key
        # Setting the max_workers for multithreading with a maximum
        # of 200
        self.name = "alpaca"
        self.max_workers = min(max_workers, 200)

        # When requesting data for assets for example,
        # if there is too many assets, the best thing to do would
        # be to split it into chunks and request data for each chunk
        self.chunk_size = min(chunk_size, 100)

        # Connection to alpaca REST API
        self.config = config
        self.api_key = config.API_KEY
        self.api_secret = config.API_SECRET
        if hasattr(config, "ENDPOINT"):
            self.endpoint = URL(config.ENDPOINT)
        else:
            self.endpoint = URL("https://paper-api.alpaca.markets")
        if hasattr(config, "VERSION"):
            self.version = config.VERSION
        else:
            self.version = "v2"
        self.api = tradeapi.REST(
            self.api_key, self.api_secret, self.endpoint, self.version
        )

    def _pull_source_symbol_bars(
        self, symbol, length, timestep=MIN_TIMESTEP, timeshift=None
    ):
        """pull broker bars for a given symbol"""
        response = self._pull_source_bars(
            [symbol], length, timestep=timestep, timeshift=timeshift
        )
        return response[symbol]

    def _pull_source_bars(self, symbols, length, timestep=MIN_TIMESTEP, timeshift=None):
        """pull broker bars for a list symbols"""
        parsed_timestep = self._parse_source_timestep(timestep, reverse=True)
        kwargs = dict(limit=length)
        if timeshift:
            end = datetime.now() - timeshift
            end = self.to_default_timezone(end)
            kwargs["end"] = self._format_datetime(end)
        response = self.api.get_barset(symbols, parsed_timestep, **kwargs)
        result = {k: v.df for k, v in response.items()}
        return result

    def _parse_source_symbol_bars(self, response, symbol):
        df = response.copy()
        df["price_change"] = df["close"].pct_change()
        df["dividend"] = 0
        df["stock_splits"] = 0
        df["dividend_yield"] = df["dividend"] / df["close"]
        df["return"] = df["dividend_yield"] + df["price_change"]
        bars = Bars(df, self.SOURCE, symbol, raw=response)
        return bars
