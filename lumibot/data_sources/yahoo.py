from datetime import datetime

import yfinance as yf

from lumibot.data_sources.exceptions import NoDataFound
from lumibot.entities import Bars

from .data_source import DataSource


class YahooData(DataSource):
    SOURCE = "YAHOO"
    MIN_TIMESTEP = "day"
    TIMESTEP_MAPPING = [
        {"timestep": "day", "represntations": ["1D", "day"]},
    ]

    def __init__(self, config=None, auto_adjust=True, **kwargs):
        self.name = "yahoo"
        self.auto_adjust = auto_adjust
        self._data_store = {}

    def _append_data(self, symbol, data):
        data.index = data.index.tz_localize(self.DEFAULT_TIMEZONE)
        if "Adj Close" in data:
            del data["Adj Close"]
        data.columns = [
            "open",
            "high",
            "low",
            "close",
            "volume",
            "dividend",
            "stock_splits",
        ]
        data["price_change"] = data["close"].pct_change()
        data["dividend_yield"] = data["dividend"] / data["close"]
        data["return"] = data["dividend_yield"] + data["price_change"]
        self._data_store[symbol] = data
        return data

    def _pull_source_symbol_bars(
        self, symbol, length, timestep=MIN_TIMESTEP, timeshift=None
    ):
        self._parse_source_timestep(timestep, reverse=True)
        if symbol in self._data_store:
            data = self._data_store[symbol]
        else:
            data = yf.Ticker(symbol).history(period="max", auto_adjust=self.auto_adjust)
            if data.shape[0] == 0:
                raise NoDataFound(self.SOURCE, symbol)
            data = self._append_data(symbol, data)

        if timeshift:
            end = datetime.now() - timeshift
            end = self.to_default_timezone(end)
            data = data[data.index <= end]

        result = data.tail(length)
        return result

    def _pull_source_bars(self, symbols, length, timestep=MIN_TIMESTEP, timeshift=None):
        """pull broker bars for a list symbols"""
        self._parse_source_timestep(timestep, reverse=True)
        missing_symbols = [
            symbol for symbol in symbols if symbol not in self._data_store
        ]
        tickers = yf.Tickers(" ".join(missing_symbols))
        for ticker in tickers.tickers:
            data = ticker.history(period="max", auto_adjust=self.auto_adjust)
            self._append_data(ticker.ticker, data)

        result = {}
        for symbol in symbols:
            result[symbol] = self._pull_source_symbol_bars(
                symbol, length, timestep=timestep, timeshift=timeshift
            )
        return result

    def _parse_source_symbol_bars(self, response, symbol):
        bars = Bars(response, self.SOURCE, symbol, raw=response)
        return bars
