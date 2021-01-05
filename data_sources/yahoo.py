import logging
from datetime import datetime, timedelta

import pandas as pd
import yfinance as yf

from lumibot.entities import Bars

from .data_source import DataSource


class YahooData(DataSource):
    MIN_TIME_STEP = timedelta(days=1)

    def __init__(self):
        self.name = "yahoo"
        self._data_store = {}

    def _parse_source_time_unit(self, time_unit, reverse=False):
        """parse the data source time_unit variable
        into a datetime.timedelta. set reverse to True to parse
        timedelta to data_source time_unit representation"""
        mapping = [
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
        self._parse_source_time_unit(time_unit, reverse=True)
        if symbol in self._data_store:
            data = self._data_store[symbol]
        else:
            data = yf.Ticker(symbol).history(period="max")
            self._data_store[symbol] = data

        if time_delta:
            end = datetime.now() - time_delta
            data = data[data.index <= end]

        result = data.tail(length)
        return result

    def _pull_source_bars(self, symbols, length, time_unit, time_delta=None):
        """pull broker bars for a list symbols"""
        self._parse_source_time_unit(time_unit, reverse=True)
        missing_symbols = [
            symbol for symbol in symbols if symbol not in self._data_store
        ]
        tickers = yf.Tickers(" ".join(missing_symbols))
        for ticker in tickers.tickers:
            self._data_store[ticker.ticker] = ticker.history(period="max")

        result = {}
        for symbol in symbols:
            result[symbol] = self._pull_source_symbol_bars(
                symbol, length, time_unit, time_delta=time_delta
            )
        return result

    def _parse_source_symbol_bars(self, response):
        df = response.copy()
        df.columns = [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "dividend",
            "stock_splits",
        ]
        df["price_change"] = df["close"].pct_change()
        df["dividend_yield"] = df["dividend"] / df["close"]
        df["return"] = df["dividend_yield"] + df["price_change"]
        bars = Bars(df, raw=response)
        return bars

    def _parse_source_bars(self, response):
        result = {}
        for symbol, bars in response.items():
            result[symbol] = self._parse_source_symbol_bars(bars)
        return result
