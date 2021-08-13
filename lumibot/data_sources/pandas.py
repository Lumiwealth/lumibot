from datetime import datetime, timedelta

import pandas as pd

from lumibot.data_sources.exceptions import NoDataFound
from lumibot.entities import Bars
from lumibot.tools import YahooHelper as yh

from .data_source import DataSource


class PandasData(DataSource):
    IS_BACKTESTING_DATA_SOURCE = True
    SOURCE = "PANDAS"
    MIN_TIMESTEP = "minute"
    TIMESTEP_MAPPING = [
        {"timestep": "day", "representations": ["1D", "day"]},
        {"timestep": "minute", "representations": ["1M", "minute"]},
    ]

    def __init__(self, pandas_data, config=None, auto_adjust=True, **kwargs):
        self.name = "pandas"
        self.pandas_data = pandas_data
        self.auto_adjust = auto_adjust
        self._data_store = {}
        self._date_index = None
        self._date_supply = None

    def load_data(self, pandas_data):
        for asset, data in pandas_data.items():
            if "Date" in data.columns:
                data = data.set_index("Date")
                data.index = pd.to_datetime(data.index)
                data.index = data.index.tz_localize(self.DEFAULT_PYTZ)
            self._append_data(asset, data)

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
        if "dividend" in data:
            data["dividend_yield"] = data["dividend"] / data["close"]
        else:
            data["dividend_yield"] = 0
        data["return"] = data["dividend_yield"] + data["price_change"]
        self._data_store[asset] = data

        self.update_date_index(data.index)

        return data

    def get_assets(self):
        return list(self._data_store.keys())

    def get_asset_by_name(self, name):
        return [asset for asset in self.get_assets() if asset.name == name]

    def get_asset_by_symbol(self, name):
        return [asset for asset in self.get_assets() if asset.name == name]

    def update_date_index(self, new_date_index):

        if self._date_index is None:
            self._date_index = new_date_index
        else:
            self._date_index = self._date_index.union(new_date_index)

    def _pull_source_symbol_bars(
        self, asset, length, timestep=MIN_TIMESTEP, timeshift=None
    ):
        self._parse_source_timestep(timestep, reverse=True)  # todo, doing nothing
        if asset in self._data_store:
            data = self._data_store[asset]
        else:
            raise ValueError(f"Asset {asset} does not have data.")

        if timeshift:
            now = datetime.now()
            now_local = self.localize_datetime(now)
            end = now_local - timeshift
            end = pd.Timestamp(self.to_default_timezone(end))
            data = data.loc[data.index < end, :]
            # TODO: If this date doesn't exactly match the date (or if some dates are missing)
            # then do a ffill or something with zero volume? How should we handle missing dates?

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
