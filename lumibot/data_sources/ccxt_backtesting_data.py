from __future__ import annotations

# pyright: reportIncompatibleMethodOverride=false
import logging
from datetime import datetime, timedelta
from decimal import Decimal
from importlib import import_module
from typing import Any, TypeAlias, cast

import pytz

from lumibot.constants import LUMIBOT_DEFAULT_PYTZ
from lumibot.data_sources.data_source_backtesting import DataSourceBacktesting
from lumibot.entities.asset import Asset

logger = logging.getLogger(__name__)

PandasDataFrame: TypeAlias = Any  # noqa: UP040
BarsEntity: TypeAlias = Any  # noqa: UP040
AssetPair: TypeAlias = tuple[Asset, Asset]  # noqa: UP040
AssetInput: TypeAlias = Asset | str | AssetPair  # noqa: UP040


class _LazyModule:
    __slots__ = ("_module_name", "_module")

    def __init__(self, module_name: str) -> None:
        self._module_name = module_name
        self._module: Any | None = None

    def _load(self) -> Any:
        module = self._module
        if module is None:
            module = import_module(self._module_name)
            self._module = module
        return module

    def __getattr__(self, name: str) -> Any:
        return getattr(self._load(), name)


np = _LazyModule("numpy")
_ccxt_cache_db_class_cache: type[Any] | None = None
_bars_class_cache: type[Any] | None = None


def _ccxt_cache_db_class() -> type[Any]:
    global _ccxt_cache_db_class_cache
    if _ccxt_cache_db_class_cache is None:
        from lumibot.tools import CcxtCacheDB

        _ccxt_cache_db_class_cache = CcxtCacheDB
    if _ccxt_cache_db_class_cache is None:
        raise RuntimeError("CcxtCacheDB import did not initialize")
    return _ccxt_cache_db_class_cache


def _bars_class() -> type[Any]:
    global _bars_class_cache
    if _bars_class_cache is None:
        from lumibot.entities.bars import Bars

        _bars_class_cache = Bars
    return _bars_class_cache


def _asset_symbol(asset: Asset | str) -> str:
    if isinstance(asset, str):
        return asset.upper()
    return str(asset.symbol or "").upper()


def _ccxt_symbol(asset: AssetInput, quote: Asset | None = None) -> str:
    if isinstance(asset, tuple):
        return f"{_asset_symbol(asset[0])}/{_asset_symbol(asset[1])}"
    if quote is not None:
        return f"{_asset_symbol(asset)}/{_asset_symbol(quote)}"
    return _asset_symbol(asset)


class CcxtBacktestingData(DataSourceBacktesting):
    """Use CcxtCacheDB to download and cache data."""

    # SOURCE must be `CCXT` for the DataSourceBacktesting to work
    # `CCXT` is used in DataSource name
    SOURCE = "CCXT"
    MIN_TIMESTEP = "day"
    TIMESTEP_MAPPING = [
        {"timestep": "minute", "representations": ["1m"]},
        {"timestep": "day", "representations": ["1d"]},
    ]

    def __init__(self, *args: Any, auto_adjust: bool = False, **kwargs: Any) -> None:
        # max data download limit
        # from current date to max data download limit
        download_limit: int | None = None
        exchange_id = "binance"
        if kwargs:
            raw_download_limit = kwargs.pop("max_data_download_limit", download_limit)
            download_limit = int(raw_download_limit) if raw_download_limit is not None else None
            exchange_id = str(kwargs.pop("exchange_id", exchange_id))

        super().__init__(*args, **kwargs)
        self.name: str = exchange_id
        self.auto_adjust: bool = auto_adjust
        self._data_store: dict[str, PandasDataFrame] = {}
        # The number of historical data is downloaded earlier than the start date when downloading historical data.
        self._download_start_dt_prebuffer: int = 300

        self.cache_db: Any = _ccxt_cache_db_class()(self.name, max_download_limit=download_limit)

    def _to_utc_timezone(self, dt: datetime) -> datetime:
        if dt.tzinfo is not None:
            dt = dt.astimezone(pytz.utc)
        else:
            dt = pytz.utc.localize(dt)
        return dt

    def _append_data(self, key: str, data: PandasDataFrame) -> PandasDataFrame:
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
        self,
        asset: AssetInput,
        length: int | None = None,
        timestep: str = MIN_TIMESTEP,
        timeshift: timedelta | None = None,
        quote: Asset | None = None,
        exchange: str | None = None,
        include_after_hours: bool = True,
    ) -> PandasDataFrame | None:
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
            logger.warning(
                f"the exchange parameter is not implemented for CcxtData, but {exchange} was passed as the exchange"
            )

        symbol = _ccxt_symbol(asset, quote)

        parsed_timestep = self._parse_source_timestep(timestep, reverse=True)
        symbol_timestep = f"{symbol}_{parsed_timestep}"
        if symbol_timestep in self._data_store:
            data = self._data_store[symbol_timestep]
        else:
            data = self._pull_source_bars([asset], length, timestep, timeshift, quote, include_after_hours)
            frame = data.get(symbol)
            if frame is None or frame.empty:
                message = f"{self.SOURCE} did not return data for asset {symbol}. Make sure this symbol is valid."
                logger.error(message)
                return None
            data = self._append_data(symbol_timestep, frame)

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
        assets: list[AssetInput],
        length: int | None,
        timestep: str = MIN_TIMESTEP,
        timeshift: timedelta | None = None,
        quote: Asset | None = None,
        include_after_hours: bool = False,
    ) -> dict[str, PandasDataFrame | None]:
        """pull broker bars for a list assets"""
        parsed_timestep = self._parse_source_timestep(timestep, reverse=True)

        result: dict[str, PandasDataFrame | None] = {}
        for asset in assets:
            symbol = _ccxt_symbol(asset, quote)

            # convert native timezone aware
            start_dt = self._to_utc_timezone(self.datetime_start)
            end_dt = self._to_utc_timezone(self.datetime_end or self.get_datetime())

            if parsed_timestep == "1d":
                start_dt = start_dt - timedelta(days=self._download_start_dt_prebuffer)
            else:
                start_dt = start_dt - timedelta(minutes=self._download_start_dt_prebuffer)

            data = self.cache_db.download_ohlcv(symbol, parsed_timestep, start_dt, end_dt)

            data.index = data.index.tz_localize("UTC")
            data.index = data.index.tz_convert(LUMIBOT_DEFAULT_PYTZ)
            result[symbol] = data

        return result

    def get_historical_prices(
        self,
        asset: AssetInput,
        length: int,
        timestep: str | None = None,
        timeshift: timedelta | None = None,
        quote: Asset | None = None,
        exchange: str | None = None,
        include_after_hours: bool = True,
    ) -> Any:
        """Get bars for a given asset"""
        if isinstance(asset, str):
            asset = Asset(symbol=asset, asset_type="crypto")

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
        asset: AssetInput,
        timestep: str = "minute",
        quote: Asset | None = None,
        exchange: str | None = None,
        include_after_hours: bool = True,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> BarsEntity | None:
        parsed_timestep = self._parse_source_timestep(timestep, reverse=True)

        symbol = _ccxt_symbol(asset, quote)

        # convert utc timezone
        start_dt = self._to_utc_timezone(start_date or self.datetime_start)
        end_dt = self._to_utc_timezone(end_date or self.datetime_end or self.get_datetime())

        # Cache data is stored in UTC time
        data = self.cache_db.get_data_from_cache(symbol, parsed_timestep, start_dt, end_dt)
        if data is None or data.empty:
            return None

        # convert to lumibot default timezone
        data.index = data.index.tz_localize("UTC")
        data.index = data.index.tz_convert(LUMIBOT_DEFAULT_PYTZ)

        bars = self._parse_source_symbol_bars(data, asset, quote=quote)
        return bars

    def _parse_source_symbol_bars(
        self,
        response: PandasDataFrame,
        asset: AssetInput,
        quote: Asset | None = None,
        length: int | None = None,
    ) -> BarsEntity:
        # Parse the dataframe returned from CCXT.
        bars = _bars_class()(response, self.SOURCE, asset, quote=quote, raw=response)
        return bars

    def get_last_price(
        self,
        asset: AssetInput,
        timestep: str | None = None,
        quote: Asset | None = None,
        exchange: str | None = None,
        **kwargs: Any,
    ) -> float | Decimal | None:
        """Takes an asset and returns the last known price of close"""
        if timestep is None:
            timestep = self.get_timestep()

        bars = self.get_historical_prices(asset, 1, timestep=timestep, quote=quote, timeshift=None)

        if isinstance(bars, float):
            return bars
        elif bars is None or bars.df.empty:
            return None

        df_local = bars.df
        if hasattr(df_local, "iloc"):
            close_ = df_local["close"].iat[0]
        else:
            close_ = df_local["close"][0]
        if isinstance(close_, np.int64):
            close_ = Decimal(close_.item())
        return cast(float | Decimal | None, close_)

    def get_chains(self, asset: Asset, quote: Asset | None = None, exchange: str | None = None) -> dict[str, Any]:
        """
        Get the chains for a given asset.  This is not implemented for BinanceData becuase Yahoo does not support
        historical options data."""

        raise NotImplementedError(
            "CcxtBactestingData does not support historical options data. If you need this "
            "feature, please use a different data source."
        )

    def get_strikes(self, asset: Asset) -> list[float]:
        raise NotImplementedError(
            "CcxtBactestingData does not support historical options data. If you need this "
            "feature, please use a different data source."
        )


if __name__ == "__main__":
    # kwargs = {
    #     "max_data_download_limit":10000,
    # }

    start_date = datetime(2023, 12, 15)
    end_date = datetime(2023, 12, 31)

    # b = BinanceData(start_date,end_date, **kwargs)
    b = CcxtBacktestingData(start_date, end_date)
    r = b.get_historical_prices(
        asset=(Asset(symbol="SOL", asset_type="crypto"), Asset(symbol="USDT", asset_type="crypto")),
        length=20,
        timestep="day",
    )
    print(r)
    r = b.get_last_price(
        asset=(Asset(symbol="SOL", asset_type="crypto"), Asset(symbol="USDT", asset_type="crypto")),
        timestep="day",
    )
    print(r)
