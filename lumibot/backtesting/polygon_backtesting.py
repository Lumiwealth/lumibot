# pyright: reportIncompatibleMethodOverride=false

from __future__ import annotations

import traceback
from collections import OrderedDict
from datetime import datetime, timedelta
from decimal import Decimal
from importlib import import_module
from types import ModuleType
from typing import Any, TypeAlias, cast

from termcolor import colored

from lumibot.data_sources.pandas_data import PandasData
from lumibot.entities.asset import Asset
from lumibot.tools.lumibot_logger import get_logger

logger = get_logger(__name__)
START_BUFFER = timedelta(days=5)

AssetInput: TypeAlias = Asset | str | tuple[Any, Any]  # noqa: UP040 - keep Python 3.11 parser compatibility.
PandasDataFrame: TypeAlias = Any  # noqa: UP040 - keep Python 3.11 parser compatibility.
DataEntity: TypeAlias = Any  # noqa: UP040 - keep Python 3.11 parser compatibility.
BarsEntity: TypeAlias = Any  # noqa: UP040 - keep Python 3.11 parser compatibility.


class _LazyModule(ModuleType):
    _module_name: str
    _module: ModuleType | None

    __slots__ = ("_module_name", "_module")

    def __init__(self, module_name: str) -> None:
        super().__init__(module_name)
        self._module_name = module_name
        self._module = None

    def _load(self) -> ModuleType:
        module = self._module
        if module is None:
            module = import_module(self._module_name)
            self._module = module
        return module

    def __getattr__(self, name: str) -> Any:
        return getattr(self._load(), name)

    def __setattr__(self, name: str, value: Any) -> None:
        if name in {"_module_name", "_module"}:
            object.__setattr__(self, name, value)
            return
        setattr(self._load(), name, value)

    def __delattr__(self, name: str) -> None:
        if name in {"_module_name", "_module"}:
            object.__delattr__(self, name)
        else:
            delattr(self._load(), name)


polygon_helper: Any = _LazyModule("lumibot.tools.polygon_helper")
_polygon_client_class_cache: Any | None = None
_bad_response_class_cache: type[BaseException] | None = None
_data_class_cache: type[Any] | None = None


def _polygon_client_class() -> Any:
    global _polygon_client_class_cache
    if _polygon_client_class_cache is None:
        from lumibot.tools.polygon_helper import PolygonClient

        _polygon_client_class_cache = PolygonClient
    return _polygon_client_class_cache


def _bad_response_class() -> type[BaseException]:
    global _bad_response_class_cache
    if _bad_response_class_cache is None:
        polygon_exceptions = cast(Any, import_module("polygon.exceptions"))
        _bad_response_class_cache = cast(type[BaseException], polygon_exceptions.BadResponse)
    return _bad_response_class_cache


def _data_class() -> type[Any]:
    global _data_class_cache
    if _data_class_cache is None:
        from lumibot.entities import Data

        _data_class_cache = Data
    assert _data_class_cache is not None
    return _data_class_cache


def _dataframe_memory_usage_bytes(df: PandasDataFrame) -> int:
    return int(df.memory_usage().sum())


class PolygonDataBacktesting(PandasData):
    """
    Backtesting implementation of Polygon
    """

    option_quote_fallback_allowed: bool = True
    _max_storage_bytes: int | None

    def __init__(
        self,
        datetime_start: datetime | None = None,
        datetime_end: datetime | None = None,
        pandas_data: dict[Any, Any] | list[Any] | None = None,
        api_key: str | None = None,
        max_memory: int | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            pandas_data=pandas_data,
            api_key=api_key,
            allow_option_quote_fallback=True,
            **kwargs,
        )

        # Memory limit, off by default
        self._max_storage_bytes = max_memory

        # Store errors CSV path for use in data retrieval

        # RESTClient API for Polygon.io polygon-api-client
        self.polygon_client = _polygon_client_class().create(api_key=api_key)

    @property
    def MAX_STORAGE_BYTES(self) -> int | None:
        return self._max_storage_bytes

    @MAX_STORAGE_BYTES.setter
    def MAX_STORAGE_BYTES(self, value: int | None) -> None:
        self._max_storage_bytes = None if value is None else int(value)

    def _enforce_storage_limit(self, pandas_data: OrderedDict[Any, Any]) -> None:
        max_storage_bytes = self.MAX_STORAGE_BYTES
        if max_storage_bytes is None:
            return

        storage_used = sum(_dataframe_memory_usage_bytes(data.df) for data in pandas_data.values())
        logger.info(f"{storage_used = :,} bytes for {len(pandas_data)} items")
        while storage_used > max_storage_bytes and pandas_data:
            k, d = pandas_data.popitem(last=False)
            mu = _dataframe_memory_usage_bytes(d.df)
            storage_used -= mu
            logger.info(f"Storage limit exceeded. Evicted LRU data: {k} used {mu:,} bytes")

    def _update_pandas_data(
        self,
        asset: AssetInput,
        quote: Asset | None,
        length: int,
        timestep: str,
        start_dt: datetime | None = None,
    ) -> None:
        """
        Get asset data and update the self.pandas_data dictionary.

        Parameters
        ----------
        asset : Asset
            The asset to get data for.
        quote : Asset
            The quote asset to use. For example, if asset is "SPY" and quote is "USD", the data will be for "SPY/USD".
        length : int
            The number of data points to get.
        timestep : str
            The timestep to use. For example, "1minute" or "1hour" or "1day".
        start_dt : datetime
            The start datetime to use. If None, the current self.start_datetime will be used.
        """
        search_asset: Any = asset
        asset_separated: Asset | str = cast(Asset | str, asset)
        quote_asset = quote if quote is not None else Asset("USD", "forex")

        if isinstance(search_asset, tuple):
            asset_separated = cast(Asset | str, search_asset[0])
            quote_asset = cast(Asset, search_asset[1])
        else:
            search_asset = (search_asset, quote_asset)

        # Get the start datetime and timestep unit
        start_datetime, ts_unit = cast(
            tuple[datetime, str],
            self.get_start_datetime_and_ts_unit(length, timestep, start_dt, start_buffer=START_BUFFER),
        )
        # Check if we have data for this asset
        if search_asset in self.pandas_data:
            asset_data = self.pandas_data[search_asset]
            asset_data_df = asset_data.df
            data_start_datetime = asset_data_df.index[0]

            # Get the timestep of the data
            data_timestep = asset_data.timestep

            # If the timestep is the same, we don't need to update the data
            if data_timestep == ts_unit:
                # Check if we have enough data (5 days is the buffer we subtracted from the start datetime)
                if (data_start_datetime - start_datetime) < START_BUFFER:
                    return

            # Always try to get the lowest timestep possible because we can always resample
            # If day is requested then make sure we at least have data that's less than a day
            if ts_unit == "day":
                if data_timestep == "minute":
                    # Check if we have enough data (5 days is the buffer we subtracted from the start datetime)
                    if (data_start_datetime - start_datetime) < START_BUFFER:
                        return
                    else:
                        # We don't have enough data, so we need to get more (but in minutes)
                        ts_unit = "minute"
                elif data_timestep == "hour":
                    # Check if we have enough data (5 days is the buffer we subtracted from the start datetime)
                    if (data_start_datetime - start_datetime) < START_BUFFER:
                        return
                    else:
                        # We don't have enough data, so we need to get more (but in hours)
                        ts_unit = "hour"

            # If hour is requested then make sure we at least have data that's less than an hour
            if ts_unit == "hour":
                if data_timestep == "minute":
                    # Check if we have enough data (5 days is the buffer we subtracted from the start datetime)
                    if (data_start_datetime - start_datetime) < START_BUFFER:
                        return
                    else:
                        # We don't have enough data, so we need to get more (but in minutes)
                        ts_unit = "minute"

        # Download data from Polygon
        try:
            # Get data from Polygon
            df = polygon_helper.get_price_data_from_polygon(
                self._api_key,
                asset_separated,
                start_datetime,
                self.datetime_end,
                timespan=ts_unit,
                quote_asset=quote_asset,
            )
        except _bad_response_class() as e:
            # Assuming e.message or similar attribute contains the error message
            formatted_start_datetime = start_datetime.strftime("%Y-%m-%d")
            formatted_end_datetime = self.datetime_end.strftime("%Y-%m-%d")
            text = str(e)
            plan_msgs = (
                "Your plan doesn't include this data timeframe",
                "Your plan doesn\u2019t include this data timeframe",
                "not entitled to this data",
                "NOT_AUTHORIZED",
            )
            invalid_key_msgs = ("Unknown API Key", "Invalid API Key")
            if any(m in text for m in plan_msgs) and not any(m in text for m in invalid_key_msgs):
                msg = (
                    "Polygon Access Denied: Your subscription does not allow you to backtest that far back in time. "
                    f"Requested {asset_separated} {ts_unit} bars from {formatted_start_datetime} to {formatted_end_datetime}. "
                    "We strongly recommend switching to ThetaData (https://www.thetadata.net/ with promo code 'BotSpot10') for better coverage, speed, and LumiBot-native support. "
                    "If you must stay on Polygon, consider starting later or upgrading your Polygon plan (https://polygon.io/?utm_source=affiliate&utm_campaign=lumi10, code 'LUMI10')."
                )
                logger.error(colored(msg, color="red"))
                return
            elif "Unknown API Key" in str(e):
                error_message = colored(
                    "Polygon Access Denied: Your API key is invalid. "
                    "Please check your API key and try again. "
                    "You can get an API key at https://polygon.io/?utm_source=affiliate&utm_campaign=lumi10 "
                    "Please use the full link to give us credit for the sale, it helps support this project. "
                    "You can use the coupon code 'LUMI10' for 10% off. "
                    "We recommend switching to ThetaData (https://www.thetadata.net/ with promo code 'BotSpot10') for higher-quality, faster data and first-class support in LumiBot. ",
                    color="red",
                )
                raise Exception(error_message) from e
            else:
                # Handle other BadResponse exceptions not related to plan limitations
                logger.error(traceback.format_exc())
                raise
        except Exception as e:
            # Handle all other exceptions
            logger.error(traceback.format_exc())
            raise Exception("Error getting data from Polygon") from e

        if (df is None) or df.empty:
            return
        data = _data_class()(asset_separated, df, timestep=ts_unit, quote=quote_asset)
        pandas_data_update = self._set_pandas_data_keys([data])
        # Add the keys to the self.pandas_data dictionary
        self.pandas_data.update(pandas_data_update)
        if self.MAX_STORAGE_BYTES is not None:
            self._enforce_storage_limit(self.pandas_data)

    def _pull_source_symbol_bars(
        self,
        asset: Asset,
        length: int,
        timestep: str = "day",
        timeshift: int | timedelta | None = None,
        quote: Asset | None = None,
        exchange: str | None = None,
        include_after_hours: bool = True,
    ) -> Any | None:
        # Get the current datetime and calculate the start datetime
        current_dt = self.get_datetime()
        # Get data from Polygon
        self._update_pandas_data(asset, quote, length, timestep, current_dt)
        return super()._pull_source_symbol_bars(
            asset, length, timestep, timeshift, quote, exchange, include_after_hours
        )

    # Get pricing data for an asset for the entire backtesting period
    def get_historical_prices_between_dates(
        self,
        asset: AssetInput,
        timestep: str = "minute",
        quote: Asset | None = None,
        exchange: str | None = None,
        include_after_hours: bool = True,
        start_date: Any | None = None,
        end_date: Any | None = None,
    ) -> BarsEntity | None:
        self._update_pandas_data(asset, quote, 1, timestep)

        response = super()._pull_source_symbol_bars_between_dates(
            cast(Asset, asset), timestep, quote, exchange, include_after_hours, start_date, end_date
        )

        if response is None:
            return None

        bars = self._parse_source_symbol_bars(response, cast(Asset | tuple[Any, Any], asset), quote=quote)
        return bars

    def get_last_price(
        self,
        asset: AssetInput,
        timestep: str = "minute",
        quote: Asset | None = None,
        exchange: str | None = None,
        **kwargs: Any,
    ) -> float | Decimal | None:
        dt: datetime | None = None
        try:
            dt = self.get_datetime()
            self._update_pandas_data(asset, quote, 1, timestep, dt)
        except Exception as e:
            print(f"Error get_last_price from Polygon: {e}")
            print(f"Error get_last_price from Polygon: {asset=} {quote=} {timestep=} {dt=} {e}")

        return super().get_last_price(asset=cast(Asset, asset), quote=quote, exchange=exchange)

    def get_chains(self, asset: Asset, quote: Asset | None = None, exchange: str | None = None) -> dict[str, Any]:
        """
        Integrates the Polygon client library into the LumiBot backtest for Options Data
        in the same structure as Interactive Brokers options chain data.

        Parameters
        ----------
        asset : Asset
            The underlying asset symbol. Typically an equity like "SPY" or "NVDA".
        quote : Asset, optional
            The quote asset to use, e.g. Asset("USD"). (Usually unused for equities.)
        exchange : str, optional
            The exchange to which the chain belongs (e.g., "SMART").

        Returns
        -------
        dict
            A dictionary of dictionaries describing the option chain.

            Format:
            - "Multiplier": int
                e.g. 100
            - "Exchange": str
                e.g. "NYSE"
            - "Chains": dict
                Dictionary with "CALL" and "PUT" keys.
                Each key is itself a dictionary mapping expiration dates (YYYY-MM-DD) to a list of strikes.

            Example
            -------
            {
                "Multiplier": 100,
                "Exchange": "NYSE",
                "Chains": {
                    "CALL": {
                        "2023-07-31": [100.0, 101.0, ...],
                        "2023-08-07": [...],
                        ...
                    },
                    "PUT": {
                        "2023-07-31": [100.0, 101.0, ...],
                        ...
                    }
                }
            }

        Notes
        -----
        This function simply calls :func:`get_chains_cached` from polygon_helper,
        which may reuse recent chain data to speed up backtests.
        """
        logger.debug(f"polygon_backtesting.get_chains called for {asset.symbol}")

        # Call the caching helper
        option_contracts = polygon_helper.get_chains_cached(
            api_key=self._api_key,
            asset=asset,
            quote=quote,
            exchange=exchange,
            current_date=self.get_datetime().date(),
            polygon_client=self.polygon_client,
        )

        return option_contracts
