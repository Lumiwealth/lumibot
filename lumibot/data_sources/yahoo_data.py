import logging
from datetime import timedelta
from decimal import Decimal

import numpy

from lumibot.data_sources import DataSourceBacktesting
from lumibot.entities import Asset, Bars
from lumibot.tools import YahooHelper


class YahooData(DataSourceBacktesting):
    SOURCE = "YAHOO"
    MIN_TIMESTEP = "day"
    TIMESTEP_MAPPING = [
        {"timestep": "day", "representations": ["1D", "day"]},
    ]

    def __init__(self, *args, auto_adjust=True, **kwargs):
        super().__init__(*args, **kwargs)
        self.name = "yahoo"
        self.auto_adjust = auto_adjust
        self._data_store = {}

    def _append_data(self, asset, data):
        """

        Parameters
        ----------
        asset : Asset
        data

        Returns
        -------

        """
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
        self, asset, length, timestep=MIN_TIMESTEP, timeshift=None, quote=None, exchange=None, include_after_hours=True
    ):
        if exchange is not None:
            logging.warning(
                f"the exchange parameter is not implemented for YahooData, but {exchange} was passed as the exchange"
            )

        if quote is not None:
            logging.warning(f"quote is not implemented for YahooData, but {quote} was passed as the quote")

        self._parse_source_timestep(timestep, reverse=True)
        if asset in self._data_store:
            data = self._data_store[asset]
        else:
            data = YahooHelper.get_symbol_data(
                asset.symbol,
                auto_adjust=self.auto_adjust,
                last_needed_datetime=self.datetime_end,
            )
            if data is None or data.shape[0] == 0:
                message = f"{self.SOURCE} did not return data for symbol {asset}. Make sure this symbol is valid."
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
            logging.warning(f"quote is not implemented for YahooData, but {quote} was passed as the quote")

        self._parse_source_timestep(timestep, reverse=True)
        missing_assets = [asset.symbol for asset in assets if asset not in self._data_store]

        if missing_assets:
            dfs = YahooHelper.get_symbols_data(missing_assets, auto_adjust=self.auto_adjust)
            for symbol, df in dfs.items():
                self._append_data(symbol, df)

        result = {}
        for asset in assets:
            result[asset] = self._pull_source_symbol_bars(asset, length, timestep=timestep, timeshift=timeshift)
        return result

    def _parse_source_symbol_bars(self, response, asset, quote=None, length=None):
        if quote is not None:
            logging.warning(f"quote is not implemented for YahooData, but {quote} was passed as the quote")

        bars = Bars(response, self.SOURCE, asset, raw=response)
        return bars

    def get_last_price(self, asset, timestep=None, quote=None, exchange=None, **kwargs):
        """Takes an asset and returns the last known price"""
        if timestep is None:
            timestep = self.get_timestep()

        # Use -1 timeshift to get the price for the current bar (otherwise gets yesterdays prices)
        bars = self.get_historical_prices(asset, 1, timestep=timestep, quote=quote, timeshift=timedelta(days=-1))

        if isinstance(bars, float):
            return bars
        elif bars is None:
            return None

        open_ = bars.df.iloc[0].open
        if isinstance(open_, numpy.int64):
            open_ = Decimal(open_.item())
        return open_

    def get_chains(self, asset: Asset, quote: Asset = None, exchange: str = None):
        """
        Get the chains for a given asset.  This is not implemented for YahooData becuase Yahoo does not support
        historical options data.

        yfinance module does support getting some of the info for current options chains, but it is not implemented.
        See yf methods:
        >>>    import yfinance as yf
        >>>    spy = yf.Ticker("SPY")
        >>>    expirations = spy.options
        >>>    chain_data = spy.option_chain()
        """
        raise NotImplementedError(
            "Lumibot YahooData does not support historical options data. If you need this "
            "feature, please use a different data source."
        )

    def get_strikes(self, asset):
        raise NotImplementedError(
            "Lumibot YahooData does not support historical options data. If you need this "
            "feature, please use a different data source."
        )

    def get_historical_prices(
        self, asset, length, timestep="", timeshift=None, quote=None, exchange=None, include_after_hours=True
    ):
        """Get bars for a given asset"""
        if isinstance(asset, str):
            asset = Asset(symbol=asset)

        if not timestep:
            timestep = self.get_timestep()

        response = self._pull_source_symbol_bars(
            asset,
            length,
            timestep=timestep,
            timeshift=timeshift,
            quote=quote,
            exchange=exchange,
            include_after_hours=include_after_hours,
        )
        if isinstance(response, float):
            return response
        elif response is None:
            return None

        bars = self._parse_source_symbol_bars(response, asset, quote=quote, length=length)
        return bars
