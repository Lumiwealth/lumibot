from datetime import datetime, timedelta

import numba
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
        self._timestep = 'day'

    def load_data(self, pandas_data):
        self._data_store = pandas_data
        self.update_date_index()

    def get_assets(self):
        return list(self._data_store.keys())

    def get_asset_by_name(self, name):
        return [asset for asset in self.get_assets() if asset.name == name]

    def get_asset_by_symbol(self, name):
        return [asset for asset in self.get_assets() if asset.name == name]

    def update_date_index(self):
        for asset, data in self._data_store.items():
            if self._date_index is None:
                self._date_index = data.datetime
            # else:
                # set([tuple(i) for i in arr.tolist()])
                # self._date_index = self._date_index.union(new_date_index)

    def get_last_price(self, asset, timestep=None):
        """Takes an asset and returns the last known price"""
        if timestep is None:
            timestep = self.get_timestep()
        last_price = self._data_store[asset].get_last_price(self._iter_count)
        return last_price

    def _pull_source_symbol_bars(
        self, asset, length, timestep=MIN_TIMESTEP, timeshift=0
    ):
        if not timeshift:
            timeshift = 0

        # self._parse_source_timestep(timestep, reverse=True)  # todo, doing nothing
        if asset in self._data_store:
            data = self._data_store[asset]
        else:
            raise ValueError(f"Asset {asset} does not have data.")


        # todo if timeshift is greater than iter_count there will be an error.
        # result = data.tail(length)
        res = data.get_bars(self._iter_count, length, timestep, timeshift)
        return res

    def _pull_source_bars(self, assets, length, timestep=MIN_TIMESTEP, timeshift=None):
        """pull broker bars for a list assets"""
        self._parse_source_timestep(timestep, reverse=True)

        result = {}
        for asset in assets:
            result[asset] = self._pull_source_symbol_bars(
                asset, length, timestep=timestep, timeshift=timeshift
            )
        return result


    def _parse_source_symbol_bars(self, response, asset):
        return response
        bars = Bars(response, self.SOURCE, asset, raw=response)
        return bars

    def get_yesterday_dividend(self, asset):
        pass

    def get_yesterday_dividends(self, assets):
        pass