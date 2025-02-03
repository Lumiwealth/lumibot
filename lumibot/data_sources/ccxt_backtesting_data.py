import logging
from decimal import Decimal

import numpy as np
import pytz
from datetime import timedelta,datetime

from lumibot import LUMIBOT_DEFAULT_PYTZ
from lumibot.data_sources import DataSourceBacktesting
from lumibot.entities import Asset, Bars

from lumibot.tools import CcxtCacheDB
from pandas import DataFrame

from typing import Union, Any, Dict


class CcxtBacktestingData(DataSourceBacktesting):
    """Use CcxtCacheDB to download and cache data.
    """
    # SOURCE must be `CCXT` for the DataSourceBacktesting to work
    # `CCXT` is used in DataSource name
    SOURCE = "CCXT"
    MIN_TIMESTEP = "day"
    TIMESTEP_MAPPING = [
        {"timestep": "minute", "representations": ["1m"]},
        {"timestep": "day", "representations": ["1d"]},
    ]

    def __init__(self, *args, auto_adjust:bool=False, **kwargs):
        # max data download limit
        # from current date to max data download limit
        download_limit = None
        exchange_id = "binance"
        if kwargs:
            download_limit = kwargs.pop("max_data_download_limit", download_limit)
            exchange_id = kwargs.pop("exchange_id", exchange_id)

        super().__init__(*args, **kwargs)
        self.name = exchange_id
        self.auto_adjust = auto_adjust
        self._data_store = {}
        # The number of historical data is downloaded earlier than the start date when downloading historical data.
        self._download_start_dt_prebuffer = 300

        self.cache_db = CcxtCacheDB(self.name,max_download_limit=download_limit)


    def _to_utc_timezone(self, dt:datetime)->datetime:
        if not dt.tzinfo is None:
            dt = dt.astimezone(pytz.utc)
        else:
            dt = pytz.utc.localize(dt)
        return dt


    def _append_data(self, key:str, data:DataFrame)->DataFrame:
        """Adds data to a dict and returns the data.

        Args:
            key (str): BTC_USDT_1d, ETH_USDT_1d, etc
            data (DataFrame): ohlcv data (datetime, open, high, low, close, volume)

        Returns:
            DataFrame: ohlcv data
        """
        data["price_change"] = data["close"].pct_change()
        data["dividend_yield"] = 0
        data["return"] = data["dividend_yield"] + data["price_change"]
        self._data_store[key] = data
        return data


    def _pull_source_symbol_bars(
        self, asset:tuple[Asset,Asset], length:int = None, timestep:str=MIN_TIMESTEP,
            timeshift:int=None, quote=Asset, exchange:Any=None, include_after_hours:bool=True
    )->Union[DataFrame,None]:
        """Gets the OHCLV data for a specific asset.

        Args:
            asset (tuple[Asset,Asset]): base asset and quote asset
                                        ex) (Asset(symbol="SOL",asset_type="crypto"),Asset(symbol="USDT",asset_type="crypto"))
            length (int, optional): Number of data to import. Defaults to None.
            timestep (str, optional): "day", "minute". Defaults to "minute".
            timeshift (int, optional): The amount of shift for a given datetime. Defaults to None.
            quote (Asset, optional): quote asset. Defaults to Asset.
            exchange (Any, optional): exchange. Defaults to None.
            include_after_hours (bool, optional): include_after_hours. Defaults to True.

        Returns:
            DataFrame: candle data
        """
        if exchange is not None:
            logging.warning(
                f"the exchange parameter is not implemented for CcxtData, but {exchange} was passed as the exchange"
            )

        if isinstance(asset, tuple):
            symbol = f"{asset[0].symbol.upper()}/{asset[1].symbol.upper()}"
        elif quote is not None:
            symbol = f"{asset.symbol.upper()}/{quote.symbol.upper()}"
        else:
            symbol = asset

        parsed_timestep = self._parse_source_timestep(timestep, reverse=True)
        symbol_timestep = f"{symbol}_{parsed_timestep}"
        if symbol_timestep in self._data_store:
            data = self._data_store[symbol_timestep]
        else:
            data = self._pull_source_bars([asset],length,timestep,timeshift,quote,include_after_hours)
            if data is None or data[symbol] is None or data[symbol].empty:
                message = f"{self.SOURCE} did not return data for asset {symbol}. Make sure this symbol is valid."
                logging.error(message)
                return None
            data = self._append_data(symbol_timestep, data[symbol])

        end = self.get_datetime()
        if timeshift:
            end = end - timeshift

        end = self.to_default_timezone(end)
        result_data = data[data.index <= end]

        if length is None:
            return result_data

        return result_data.tail(length)

    def _pull_source_bars(
            self,
            assets: tuple[Asset,Asset],
            length: int,
            timestep: str = MIN_TIMESTEP,
            timeshift: int = None,
            quote: Asset = None,
            include_after_hours: bool = False
    ) -> Dict:
        """pull broker bars for a list assets"""
        parsed_timestep = self._parse_source_timestep(timestep, reverse=True)

        result = {}
        for asset in assets:
            if isinstance(asset, tuple):
                symbol = f"{asset[0].symbol.upper()}/{asset[1].symbol.upper()}"
            elif quote is not None:
                symbol = f"{asset.symbol.upper()}/{quote.symbol.upper()}"
            else:
                symbol = asset

            # convert native timezone aware
            start_dt = self._to_utc_timezone(self.datetime_start)
            end_dt = self._to_utc_timezone(self.datetime_end)

            if parsed_timestep == "1d":
                start_dt = start_dt - timedelta(days=self._download_start_dt_prebuffer)
            else:
                start_dt = start_dt - timedelta(minutes=self._download_start_dt_prebuffer)

            data = self.cache_db.download_ohlcv(
                symbol,parsed_timestep,
                start_dt,
                end_dt
            )

            data.index = data.index.tz_localize("UTC")
            data.index = data.index.tz_convert(LUMIBOT_DEFAULT_PYTZ)
            result[symbol] = data

        return result

    def get_historical_prices(self, asset:tuple[Asset,Asset], length:int, timestep:str=None,
            timeshift:int=None, quote:Asset=None, exchange:Any=None, include_after_hours:bool=True
    )->Bars:
        """Get bars for a given asset"""
        if isinstance(asset, str):
            asset = Asset(symbol=asset,asset_type="crypto")

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

    # Get pricing data for an asset for the entire backtesting period
    def get_historical_prices_between_dates(
        self,
        asset:tuple[Asset,Asset],
        timestep:str="minute",
        quote:Asset=None,
        exchange:Any=None,
        include_after_hours:bool=True,
        start_date:datetime=None,
        end_date:datetime=None,
    )->Bars:
        parsed_timestep = self._parse_source_timestep(timestep, reverse=True)

        if isinstance(asset, tuple):
            symbol = f"{asset[0].symbol.upper()}/{asset[1].symbol.upper()}"
        elif quote is not None:
            symbol = f"{asset.symbol.upper()}/{quote.symbol.upper()}"
        else:
            symbol = asset

        # convert utc timezone
        start_dt = self._to_utc_timezone(start_date)
        end_dt = self._to_utc_timezone(end_date)

        # Cache data is stored in UTC time
        data = self.cache_db.get_data_from_cache(symbol, parsed_timestep, start_dt, end_dt)
        if data is None or data.empty:
            return None

        # convert to lumibot default timezone
        data.index  = data.index.tz_localize("UTC")
        data.index = data.index.tz_convert(LUMIBOT_DEFAULT_PYTZ)

        bars = self._parse_source_symbol_bars(data, asset, quote=quote)
        return bars


    def _parse_source_symbol_bars(self, response:DataFrame, asset:tuple[Asset,Asset],
                                  quote:Asset=None, length:int=None)->Bars:
        # Parse the dataframe returned from CCXT.
        bars = Bars(response, self.SOURCE, asset, quote=quote, raw=response)
        return bars

    def get_last_price(self, asset, timestep=None, quote=None, exchange=None, **kwargs) -> Union[float, Decimal, None]:
        """Takes an asset and returns the last known price of close"""
        if timestep is None:
            timestep = self.get_timestep()

        bars = self.get_historical_prices(asset, 1, timestep=timestep, quote=quote, timeshift=None)

        if isinstance(bars, float):
            return bars
        elif bars is None or bars.df.empty:
            return None

        close_ = bars.df.iloc[0].close
        if isinstance(close_, np.int64):
            close_ = Decimal(close_.item())
        return close_


    def get_chains(self, asset):
        """
        Get the chains for a given asset.  This is not implemented for BinanceData becuase Yahoo does not support
        historical options data."""

        raise NotImplementedError(
            "CcxtBactestingData does not support historical options data. If you need this "
            "feature, please use a different data source."
        )


    def get_strikes(self, asset):
        raise NotImplementedError(
            "CcxtBactestingData does not support historical options data. If you need this "
            "feature, please use a different data source."
        )

if __name__ == "__main__":
    # kwargs = {
    #     "max_data_download_limit":10000,
    # }

    start_date = datetime(2023,12,15)
    end_date = datetime(2023,12,31)

    # b = BinanceData(start_date,end_date, **kwargs)
    b = CcxtBacktestingData(start_date,end_date)
    r = b.get_historical_prices(
        asset=(Asset(symbol="SOL",asset_type="crypto"),Asset(symbol="USDT",asset_type="crypto")),
        length=20,
        timestep="day",
    )
    print(r)
    r = b.get_last_price(
        asset=(Asset(symbol="SOL",asset_type="crypto"),Asset(symbol="USDT",asset_type="crypto")),
        timestep="day",
    )
    print(r)