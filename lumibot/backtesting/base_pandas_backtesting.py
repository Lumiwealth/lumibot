import traceback
import logging
from decimal import Decimal
from typing import Union
from datetime import timedelta
from datetime import datetime

import pandas as pd
from collections import OrderedDict
from termcolor import colored

from lumibot.data_sources import PandasData
from lumibot.entities import Asset, Data


class BasePandasBacktesting(PandasData):
    """
    Base class for backtesting data sources that rely on PandasData.
    Handles shared functionality for data storage, timestep calculations, 
    and common fetch/update operations.
    """

    def __init__(self, datetime_start, datetime_end, max_memory=None, **kwargs):
        super().__init__(datetime_start=datetime_start, datetime_end=datetime_end, **kwargs)

        # Memory limit (optional)
        self.MAX_STORAGE_BYTES = max_memory
        self.START_BUFFER = timedelta(days=5)

        # Initialize pandas data storage
        self.pandas_data = OrderedDict()

    def _fetch_data_from_source(
            self,
            *,
            base_asset: Asset,
            quote_asset: Asset,
            start_datetime: datetime,
            end_datetime: datetime,
            timestep: str = "minute",
            **kwargs
    ) -> pd.DataFrame:
        """
        Abstract method to fetch data from the specific source.
        Subclasses must implement this.
        """
        raise NotImplementedError("Subclasses must implement `_fetch_data_from_source`.")

    def _handle_api_errors(self, exception):
        """
        Handle any errors specific to Alpaca's API.
        Placeholder for error handling.
        """
        logging.error(f"Error while fetching data: {exception}")
        # Add logic to handle specific errors such as rate limits, missing permissions, etc.

    def _enforce_storage_limit(self):
        """Enforce memory/storage limits by evicting old data (if applicable)."""
        if self.MAX_STORAGE_BYTES:
            storage_used = sum(data.df.memory_usage().sum() for data in self.pandas_data.values())
            while storage_used > self.MAX_STORAGE_BYTES:
                # Evict the least recently used item
                key, data = self.pandas_data.popitem(last=False)
                storage_used -= data.df.memory_usage().sum()
                logging.info(f"Evicted {key} to stay within memory limit.")

    def _update_pandas_data(
            self,
            asset: Asset,
            quote: Asset,
            length: int,
            timestep: str = "day",
            start_dt: datetime = None
    ):
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
        search_asset = asset
        asset_separated = asset
        quote_asset = quote if quote is not None else Asset("USD", "forex")

        if isinstance(search_asset, tuple):
            asset_separated, quote_asset = search_asset
        else:
            search_asset = (search_asset, quote_asset)

        # Get the start datetime and timestep unit
        start_datetime, ts_unit = self.get_start_datetime_and_ts_unit(
            length, timestep, start_dt, start_buffer=self.START_BUFFER
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
                if (data_start_datetime - start_datetime) < self.START_BUFFER:
                    return

            # Always try to get the lowest timestep possible because we can always resample
            # If day is requested then make sure we at least have data that's less than a day
            if ts_unit == "day":
                if data_timestep == "minute":
                    # Check if we have enough data (5 days is the buffer we subtracted from the start datetime)
                    if (data_start_datetime - start_datetime) < self.START_BUFFER:
                        return
                    else:
                        # We don't have enough data, so we need to get more (but in minutes)
                        ts_unit = "minute"
                elif data_timestep == "hour":
                    # Check if we have enough data (5 days is the buffer we subtracted from the start datetime)
                    if (data_start_datetime - start_datetime) < self.START_BUFFER:
                        return
                    else:
                        # We don't have enough data, so we need to get more (but in hours)
                        ts_unit = "hour"

            # If hour is requested then make sure we at least have data that's less than an hour
            if ts_unit == "hour":
                if data_timestep == "minute":
                    # Check if we have enough data (5 days is the buffer we subtracted from the start datetime)
                    if (data_start_datetime - start_datetime) < self.START_BUFFER:
                        return
                    else:
                        # We don't have enough data, so we need to get more (but in minutes)
                        ts_unit = "minute"

        # Download data from Polygon
        try:
            df = self._fetch_data_from_source(
                base_asset=asset_separated,
                quote_asset=quote,
                start_datetime=start_datetime,
                end_datetime=self.datetime_end,
                timestep=ts_unit,
            )

        except Exception as e:
            self._handle_api_errors(e)

        if (df is None) or df.empty:
            return
        data = Data(asset_separated, df, timestep=ts_unit, quote=quote)
        pandas_data_update = self._set_pandas_data_keys([data])
        # Add the keys to the self.pandas_data dictionary
        self.pandas_data.update(pandas_data_update)
        if self.MAX_STORAGE_BYTES:
            self._enforce_storage_limit(self.pandas_data)

    def _pull_source_symbol_bars(
        self,
        asset: Asset,
        length: int,
        timestep: str = "day",
        timeshift: int = None,
        quote: Asset = None,
        exchange: str = None,
        include_after_hours: bool = True,
    ):
        # Get the current datetime and calculate the start datetime
        current_dt = self.get_datetime()
        # Get data from source
        self._update_pandas_data(asset, quote, length, timestep, current_dt)

        # Call base to return it.
        return super()._pull_source_symbol_bars(
            asset, length, timestep, timeshift, quote, exchange, include_after_hours
        )

    # Get pricing data for an asset for the entire backtesting period
    def get_historical_prices_between_dates(
        self,
        asset,
        timestep="minute",
        quote=None,
        exchange=None,
        include_after_hours=True,
        start_date=None,
        end_date=None,
    ):
        self._update_pandas_data(asset, quote, 1, timestep)

        response = super()._pull_source_symbol_bars_between_dates(
            asset, timestep, quote, exchange, include_after_hours, start_date, end_date
        )

        if response is None:
            return None

        bars = self._parse_source_symbol_bars(response, asset, quote=quote)
        return bars

    def get_last_price(
            self,
            asset,
            timestep="minute",
            quote=None,
            exchange=None,
            **kwargs
    ) -> Union[float, Decimal, None]:
        try:
            dt = self.get_datetime()
            self._update_pandas_data(asset, quote, 1, timestep, dt)
        except Exception as e:
            print(f"Error get_last_price from Polygon: {e}")
            print(f"Error get_last_price from Polygon: {asset=} {quote=} {timestep=} {dt=} {e}")

        return super().get_last_price(asset=asset, quote=quote, exchange=exchange)