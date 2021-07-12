from datetime import datetime

import pandas as pd
from alpha_vantage.timeseries import TimeSeries

from credentials import AlphaVantageConfig
from lumibot import LUMIBOT_DEFAULT_PYTZ, LUMIBOT_DEFAULT_TIMEZONE
from lumibot.data_sources.exceptions import NoDataFound
from lumibot.entities import Bars

from .data_source import DataSource


class AlphaVantageData(DataSource):
    SOURCE = "ALPHA_VANTAGE"
    MIN_TIMESTEP = "minute"
    TIMESTEP_MAPPING = [
        {"timestep": "day", "representations": ["1D", "day"]},
    ]

    def __init__(self, config=None, auto_adjust=True, **kwargs):
        self.name = "alpha vantage"
        self.auto_adjust = auto_adjust
        self._data_store = {}

    def _append_data(self, asset, data):
        result = data.rename(
            columns={
                "1. open": "open",
                "2. high": "high",
                "3. low": "low",
                "4. close": "close",
                "5. volume": "volume",
            }
        )

        return result

    def _pull_source_symbol_bars(
        self, asset, length, timestep=MIN_TIMESTEP, timeshift=None
    ):
        ts = TimeSeries(key=AlphaVantageConfig.API_KEY)
        # Get json object with the intraday data and another with the call's metadata

        # TODO: make sure this grabs the correct days, this is currently resulting in a bug
        data, meta_data = ts.get_intraday(asset.symbol)

        asset_data = pd.DataFrame(data)
        asset_data = asset_data.transpose()

        asset_data = asset_data.tail(length)

        asset_data = self._append_data(asset, asset_data)
        asset_data.index = (
            pd.to_datetime(asset_data.index)
            .tz_localize(tz=LUMIBOT_DEFAULT_PYTZ)
            .astype("O")
        )
        return asset_data

    def _pull_source_bars(self, assets, length, timestep=MIN_TIMESTEP, timeshift=None):
        """pull broker bars for a list assets"""

        result = {}
        for asset in assets:

            asset_data = self._pull_source_symbol_bars(
                asset, length, timestep, timeshift
            )
            result[asset] = asset_data

        return result

    def _parse_source_symbol_bars(self, response, asset):
        bars = Bars(response, self.SOURCE, asset, raw=response)
        return bars
