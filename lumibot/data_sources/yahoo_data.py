from datetime import datetime, timedelta

from lumibot.data_sources.exceptions import NoDataFound
from lumibot.entities import Bars
from lumibot.tools import YahooHelper as yh

from .data_source import DataSource


class YahooData(DataSource):
    SOURCE = "YAHOO"
    MIN_TIMESTEP = "day"
    TIMESTEP_MAPPING = [
        {"timestep": "day", "representations": ["1D", "day"]},
    ]

    def __init__(self, config=None, auto_adjust=True, **kwargs):
        self.name = "yahoo"
        self.auto_adjust = auto_adjust
        self._data_store = {}

    def _append_data(self, asset, data):
        if "Adj Close" in data:
            del data["Adj Close"]
        data = data.rename(
            columns={
                "Open": "open",
                "High": "high",
                "Low": "low",
                "Close": "close",
                "Volume": "volume",
                "Dividends": "dividend",
                "Stock Splits": "stock_splits",
            },
        )
        data["price_change"] = data["close"].pct_change()
        data["dividend_yield"] = data["dividend"] / data["close"]
        data["return"] = data["dividend_yield"] + data["price_change"]
        self._data_store[asset] = data
        return data

    def _pull_source_symbol_bars(
        self, asset, length, timestep=MIN_TIMESTEP, timeshift=None
    ):
        self._parse_source_timestep(timestep, reverse=True)
        if asset in self._data_store:
            data = self._data_store[asset]
        else:
            data = yh.get_symbol_data(asset.symbol, auto_adjust=self.auto_adjust)
            if data.shape[0] == 0:
                raise NoDataFound(self.SOURCE, asset.symbol)
            data = self._append_data(asset, data)

        if timeshift:
            end = datetime.now() - timeshift
            end = self.to_default_timezone(end)
            data = data[data.index <= end]

        result = data.tail(length)
        return result

    def _pull_source_bars(self, assets, length, timestep=MIN_TIMESTEP, timeshift=None):
        """pull broker bars for a list assets"""
        self._parse_source_timestep(timestep, reverse=True)
        missing_assets = [
            asset.symbol for asset in assets if asset not in self._data_store
        ]

        if missing_assets:
            dfs = yh.get_symbols_data(missing_assets, auto_adjust=self.auto_adjust)
            for symbol, df in dfs.items():
                self._append_data(symbol, df)

        result = {}
        for asset in assets:
            result[asset] = self._pull_source_symbol_bars(
                asset, length, timestep=timestep, timeshift=timeshift
            )
        return result

    def _parse_source_symbol_bars(self, response, asset):
        bars = Bars(response, self.SOURCE, asset, raw=response)
        return bars
