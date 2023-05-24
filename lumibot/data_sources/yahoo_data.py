import logging
from datetime import datetime, timedelta
from decimal import Decimal

import numpy
from lumibot.data_sources.exceptions import NoDataFound
from lumibot.entities import Asset, Bars
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
                f"the exchange parameter is not implemented for YahooData, but {exchange} was passed as the exchange"
            )

        if quote is not None:
            logging.warning(
                f"quote is not implemented for YahooData, but {quote} was passed as the quote"
            )

        self._parse_source_timestep(timestep, reverse=True)
        if asset in self._data_store:
            data = self._data_store[asset]
        else:
            data = yh.get_symbol_data(
                asset.symbol,
                auto_adjust=self.auto_adjust,
                last_needed_datetime=self.datetime_end,
            )
            if data.shape[0] == 0:
                message = (
                    f"{self.SOURCE} did not return data for symbol {asset}. "
                    f"Make sure there is no symbol typo or use another data source"
                )
                logging.error(message)
                return None
            data = self._append_data(asset, data)

        # Get the last minute of self._datetime to get the current bar
        dt = self._datetime.replace(hour=23, minute=59, second=59, microsecond=999999)

        # End should be yesterday because otherwise you can see the future
        end = dt - timedelta(days=1)
        if timeshift:
            end = end - timeshift

        end = self.to_default_timezone(end)
        result_data = data[data.index < end]

        result = result_data.tail(length)
        return result

    def _pull_source_bars(
        self, assets, length, timestep=MIN_TIMESTEP, timeshift=None, quote=None, include_after_hours=False
    ):
        """pull broker bars for a list assets"""

        if quote is not None:
            logging.warning(
                f"quote is not implemented for YahooData, but {quote} was passed as the quote"
            )

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

    def _parse_source_symbol_bars(self, response, asset, quote=None, length=None):
        if quote is not None:
            logging.warning(
                f"quote is not implemented for YahooData, but {quote} was passed as the quote"
            )

        bars = Bars(response, self.SOURCE, asset, raw=response)
        return bars

    def get_last_price(self, asset, timestep=None, quote=None, exchange=None, **kwargs):
        """Takes an asset and returns the last known price"""
        if timestep is None:
            timestep = self.get_timestep()

        bars = self.get_historical_prices(
            asset, 1, timestep=timestep, quote=quote  # , timeshift=timedelta(days=-1)
        )
        if isinstance(bars, float):
            return bars
        elif bars is None:
            return None

        open = bars.df.iloc[0].open
        if type(open) == numpy.int64:
            open = Decimal(open.item())
        return open
