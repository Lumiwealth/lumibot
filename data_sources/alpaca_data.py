from datetime import datetime, timedelta, timezone

import alpaca_trade_api as tradeapi
import pandas as pd
from alpaca_trade_api.common import URL

from lumibot.entities import Bars

from .data_source import DataSource


class AlpacaData(DataSource):
    NY_TIMEZONE = "America/New_York"

    """Common base class for data_sources/alpaca and brokers/alpaca"""

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

    def _parse_source_time_unit(self, time_unit, reverse=False):
        """parse the data source time_unit variable
        into a datetime.timedelta. set reverse to True to parse
        timedelta to data_source time_unit representation"""
        mapping = [
            {"timedelta": timedelta(minutes=1), "represntations": ["1Min", "minute"]},
            {"timedelta": timedelta(minutes=5), "represntations": ["5Min"]},
            {"timedelta": timedelta(minutes=15), "represntations": ["15Min"]},
            {"timedelta": timedelta(days=1), "represntations": ["1D", "day"]},
        ]
        for item in mapping:
            if reverse:
                if time_unit == item["timedelta"]:
                    return item["represntations"][0]
            else:
                if time_unit in item["represntations"]:
                    return item["timedelta"]

        raise ValueError("time_unit %r did not match" % time_unit)

    def _pull_source_symbol_bars(self, symbol, length, time_unit, time_delta=None):
        """pull broker bars for a given symbol"""
        response = self._pull_source_bars(
            [symbol], length, time_unit, time_delta=time_delta
        )
        return response.df[symbol]

    def _parse_source_symbol_bars(self, response):
        df = response.copy()
        df["price_change"] = df["close"].pct_change()
        df["dividend"] = 0
        df["dividend_yield"] = df["dividend"] / df["close"]
        df["return"] = df["dividend_yield"] + df["price_change"]
        bars = Bars(df, raw=response)
        return bars

    def _parse_source_bars(self, response):
        result = {}
        for symbol, bars in response.items():
            result[symbol] = self._parse_source_symbol_bars(bars.df)
        return result

    def _pull_source_bars(self, symbols, length, time_unit, time_delta=None):
        """pull broker bars for a list symbols"""
        parsed_time_unit = self._parse_source_time_unit(time_unit, reverse=True)
        kwargs = dict(limit=length)
        if time_delta:
            end = datetime.now() - time_delta
            kwargs["end"] = pd.Timestamp(end, tz=self.NY_TIMEZONE).isoformat()
        response = self.api.get_barset(symbols, parsed_time_unit, **kwargs)
        return response
