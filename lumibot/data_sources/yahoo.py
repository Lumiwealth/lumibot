from datetime import datetime

import yfinance as yf

from lumibot.entities import Bars

from .data_source import DataSource


class YahooData(DataSource):
    SOURCE = "YAHOO"
    MIN_TIMESTEP = "day"
    TIMESTEP_MAPPING = [
        {"timestep": "day", "represntations": ["1D", "day"]},
    ]

    def __init__(self):
        self.name = "yahoo"
        self._data_store = {}

    def _append_data(self, symbol, data):
        data.index = data.index.tz_localize(self.DEFAULT_TIMEZONE)
        self._data_store[symbol] = data

    def _pull_source_symbol_bars(
        self, symbol, length, timestep=MIN_TIMESTEP, timeshift=None
    ):
        self._parse_source_timestep(timestep, reverse=True)
        if symbol in self._data_store:
            data = self._data_store[symbol]
        else:
            data = yf.Ticker(symbol).history(period="max")
            self._append_data(symbol, data)

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
            data = ticker.history(period="max")
            self._append_data(ticker.ticker, data)

        result = {}
        for symbol in symbols:
            result[symbol] = self._pull_source_symbol_bars(
                symbol, length, timestep=timestep, timeshift=timeshift
            )
        return result

    def _parse_source_symbol_bars(self, response, symbol):
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
        bars = Bars(df, self.SOURCE, symbol, raw=response)
        return bars
