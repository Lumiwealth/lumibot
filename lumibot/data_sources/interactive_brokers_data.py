import logging
from collections import defaultdict, deque, namedtuple
from datetime import datetime
import pandas as pd

from lumibot.entities import Bars

from .data_source import DataSource

from ibapi.wrapper import *
from ibapi.client import *
from ibapi.contract import *
from ibapi.order import *
from threading import Thread
import queue
import datetime
import time


class InteractiveBrokersData(DataSource):
    """Make Interactive Brokers connection and gets data.

    Create connection to Interactive Brokers market through either Gateway or TWS
    which must be running locally for connection to be made.
    """

    SOURCE = "InteractiveBrokers"
    MIN_TIMESTEP = "minute"
    TIMESTEP_MAPPING = [
        {
            "timestep": "minute",
            "representations": [
                "1 min",
            ],
        },
        {
            "timestep": "day",
            "representations": [
                "1 day",
            ],
        },
    ]

    @staticmethod
    def _format_datetime(dt):
        return pd.Timestamp(dt).isoformat()

    @staticmethod
    def _format_ib_datetime(dt):
        return pd.Timestamp(dt).strftime("%Y%m%d %H:%M:%S")

    def __init__(self, config, max_workers=20, chunk_size=100, **kwargs):
        self.name = "interactivebrokers"
        self.max_workers = min(max_workers, 200)
        self.chunk_size = min(chunk_size, 100)

    def _parse_duration(self, length, timestep):
        # Converts length and timestep into IB `durationStr`
        if timestep == "minute":
            # IB has a max for seconds of 86400.
            return f"{str(min(length * 60, 86400))} S"
        elif timestep == "day":
            return f"{str(length)} D"
        else:
            raise ValueError(
                f"Timestep must be `day` or `minute`, you entered: {timestep}"
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

        response = dict()

        parsed_timestep = self._parse_source_timestep(timestep, reverse=True)
        parsed_duration = self._parse_duration(length, timestep)

        if timeshift:
            end = datetime.datetime.now() - timeshift
            end = self.to_default_timezone(end)
            end_date_time = self._format_ib_datetime(end)
            type = "TRADES"
        else:
            end_date_time = ""
            type = "ADJUSTED_LAST"

        # Call data.
        reqId = 0
        for symbol in symbols:
            reqId += 1
            result = self.ib.get_historical_data(
                reqId,
                symbol,
                end_date_time,
                parsed_duration,
                parsed_timestep,
                type,
                1,
                2,
                False,
                [],
            )
            df = pd.DataFrame(result)
            cols = [
                "date",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "barCount",
                "average",
            ]
            df = df[cols]
            if parsed_timestep == "1 min":
                df["date"] = pd.to_datetime(df["date"], unit="s", origin="unix")
            elif parsed_timestep == "1 day":
                df["date"] = pd.to_datetime(df["date"], format="%Y%m%d")
            response[symbol] = df
        return response

    def _parse_source_symbol_bars(self, response, symbol):
        df = response.copy()
        df["date"] = pd.to_datetime(df["date"])
        df = df.set_index("date")
        df["price_change"] = df["close"].pct_change()
        df["dividend"] = 0
        df["stock_splits"] = 0
        df["dividend_yield"] = df["dividend"] / df["close"]
        df["return"] = df["dividend_yield"] + df["price_change"]

        df = df[
            [
                "open",
                "high",
                "low",
                "close",
                "volume",
                "price_change",
                "dividend",
                "stock_splits",
                "dividend_yield",
                "return",
            ]
        ]
        bars = Bars(df, self.SOURCE, symbol, raw=response)
        return bars

    def get_yesterday_dividend(self, symbol):
        """ Unavailable """
        return 0

    def get_yesterday_dividends(self, symbols):
        """ Unavailable """
        return 0
