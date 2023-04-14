import logging
import os.path
import time
from datetime import datetime, timedelta

import pandas as pd
from alpha_vantage.timeseries import TimeSeries

from lumibot import LUMIBOT_DEFAULT_PYTZ, LUMIBOT_DEFAULT_TIMEZONE
from lumibot.data_sources.exceptions import NoDataFound
from lumibot.entities import Bars

from .data_source import DataSource


class AlphaVantageData(DataSource):
    SOURCE = "ALPHA_VANTAGE"
    MIN_TIMESTEP = "minute"
    DATA_STALE_AFTER = timedelta(days=1)

    def __init__(self, config=None, auto_adjust=True, **kwargs):
        self.name = "alpha vantage"
        self.auto_adjust = auto_adjust
        self._data_store = {}
        self.config = config

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
        self,
        asset,
        length,
        timestep=MIN_TIMESTEP,
        timeshift=None,
        quote=None,
        exchange=None,
        include_after_hours=True
    ):
        if exchange is not None:
            logging.warning(
                f"the exchange parameter is not implemented for AlphaVantageData, but {exchange} was passed as the exchange"
            )

        symbol = asset.symbol

        # Check if file exists in the current folder, if not then download the data
        data = None
        filename = f"{symbol}_{timestep}.csv"
        if asset in self._data_store:
            data = self._data_store[asset]
        else:
            got_data = False
            if os.path.exists(filename):
                mtime = os.path.getmtime(filename)
                dt = datetime.fromtimestamp(mtime)
                # Check to see if we got the data recently
                if dt > (datetime.now() - self.DATA_STALE_AFTER):
                    data = pd.read_csv(filename)
                    if "timestamp" in data.columns:
                        data = data.set_index("timestamp")
                    elif "time" in data.columns:
                        data = data.set_index("time")

                    got_data = True

            if not got_data:
                # Couldn't get the data from the file, so download it
                if timestep == "minute":
                    interval = "1min"
                    years = 2
                    months = 12
                    dfs = []
                    logging.info(
                        f"Downloading minute data for {symbol}, this can 6 minutes or more per symbol"
                    )
                    for y in range(years):
                        for m in range(months):
                            slice = f"year{y+1}month{m+1}"
                            url = f"https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol={symbol}&interval={interval}&slice={slice}&apikey={self.config.API_KEY}"
                            data = pd.read_csv(url)
                            dfs.append(data)
                            time.sleep(13)

                    data = pd.concat(dfs)
                    data.to_csv(filename)
                elif timestep == "day":
                    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={symbol}&outputsize=full&datatype=csv&apikey={self.config.API_KEY}"
                    data = pd.read_csv(url)
                    data = data.set_index("timestamp")
                    data.to_csv(filename)

            data.index = (
                pd.to_datetime(data.index)
                .tz_localize(tz=LUMIBOT_DEFAULT_PYTZ)
                .astype("O")
            )
            self._data_store[asset] = data

        data = data[data.index <= self._datetime]

        # TODO: Make timeshift work
        # if timeshift:
        #     end = datetime.now() - timeshift
        #     end = self.to_default_timezone(end)
        #     data = data[data.index <= end]

        data = data.tail(length)

        return data

    def _pull_source_bars(
        self, assets, length, timestep=MIN_TIMESTEP, timeshift=None, quote=None
    ):
        """pull broker bars for a list assets"""

        result = {}
        for asset in assets:
            asset_data = self._pull_source_symbol_bars(
                asset, length, timestep, timeshift, quote=quote
            )
            result[asset] = asset_data

        return result

    def _parse_source_symbol_bars(self, response, asset, quote=None, length=None):
        bars = Bars(response, self.SOURCE, asset, raw=response, quote=quote)
        return bars
