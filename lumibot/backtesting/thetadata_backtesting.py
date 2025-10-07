from decimal import Decimal
from typing import Union

import logging
import pandas as pd
import subprocess
from datetime import date, timedelta

from lumibot.data_sources import PandasData
from lumibot.entities import Asset, Data
from lumibot.tools import thetadata_helper

logger = logging.getLogger(__name__)


START_BUFFER = timedelta(days=5)


class ThetaDataBacktesting(PandasData):
    """
    Backtesting implementation of ThetaData
    """

    # Enable fallback to last_price when bid/ask quotes are unavailable for options
    option_quote_fallback_allowed = True

    def __init__(
        self,
        datetime_start,
        datetime_end,
        pandas_data=None,
        username=None,
        password=None,
        use_quote_data=True,
        **kwargs,
    ):
        # Pass allow_option_quote_fallback to parent to enable fallback mechanism
        super().__init__(datetime_start=datetime_start, datetime_end=datetime_end, pandas_data=pandas_data,
                         allow_option_quote_fallback=True, **kwargs)

        self._username       = username
        self._password       = password
        self._use_quote_data = use_quote_data

        self.kill_processes_by_name("ThetaTerminal.jar")

    def is_weekend(self, date):
        """
        Check if the given date is a weekend.

        :param date: datetime.date object
        :return: Boolean, True if weekend, False otherwise
        """
        return date.weekday() >= 5  # 5 = Saturday, 6 = Sunday

    def kill_processes_by_name(self, keyword):
        try:
            # Find all processes related to the keyword
            result = subprocess.run(['pgrep', '-f', keyword], capture_output=True, text=True)
            pids = result.stdout.strip().split('\n')

            if pids:
                for pid in pids:
                    if pid:  # Ensure the PID is not empty
                        logger.info(f"Killing process with PID: {pid}")
                        subprocess.run(['kill', '-9', pid])
                logger.info(f"All processes related to '{keyword}' have been killed.")
            else:
                logger.info(f"No processes found related to '{keyword}'.")

        except Exception as e:
            print(f"An error occurred during kill process: {e}")

    def _update_pandas_data(self, asset, quote, length, timestep, start_dt=None):
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

        Returns
        -------
        dict
            A dictionary with the keys being the asset and the values being the PandasData objects.
        """
        # DEBUG: Log when strike 157 is requested
        if hasattr(asset, 'strike') and asset.strike == 157:
            import traceback
            logger.info(f"\n[DEBUG STRIKE 157] _update_pandas_data called for asset: {asset}")
            logger.info(f"[DEBUG STRIKE 157] Traceback:\n{''.join(traceback.format_stack())}")

        search_asset = asset
        asset_separated = asset
        quote_asset = quote if quote is not None else Asset("USD", "forex")

        if isinstance(search_asset, tuple):
            asset_separated, quote_asset = search_asset
        else:
            search_asset = (search_asset, quote_asset)

        if asset_separated.asset_type == "option":
            expiry = asset_separated.expiration
            if self.is_weekend(expiry):
                logger.info(f"\nSKIP: Expiry {expiry} date is a weekend, no contract exists: {asset_separated}")
                return None

        # Get the start datetime and timestep unit
        start_datetime, ts_unit = self.get_start_datetime_and_ts_unit(
            length, timestep, start_dt, start_buffer=START_BUFFER
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
                    return None

            # Always try to get the lowest timestep possible because we can always resample
            # If day is requested then make sure we at least have data that's less than a day
            if ts_unit == "day":
                if data_timestep == "minute":
                    # Check if we have enough data (5 days is the buffer we subtracted from the start datetime)
                    if (data_start_datetime - start_datetime) < START_BUFFER:
                        return None
                    else:
                        # We don't have enough data, so we need to get more (but in minutes)
                        ts_unit = "minute"
                elif data_timestep == "hour":
                    # Check if we have enough data (5 days is the buffer we subtracted from the start datetime)
                    if (data_start_datetime - start_datetime) < START_BUFFER:
                        return None
                    else:
                        # We don't have enough data, so we need to get more (but in hours)
                        ts_unit = "hour"

            # If hour is requested then make sure we at least have data that's less than an hour
            if ts_unit == "hour":
                if data_timestep == "minute":
                    # Check if we have enough data (5 days is the buffer we subtracted from the start datetime)
                    if (data_start_datetime - start_datetime) < START_BUFFER:
                        return None
                    else:
                        # We don't have enough data, so we need to get more (but in minutes)
                        ts_unit = "minute"

        # Download data from ThetaData
        try:
            # Get ohlc data from ThetaData
            date_time_now = self.get_datetime()
            df_ohlc = None
            df_ohlc = thetadata_helper.get_price_data(
                self._username,
                self._password,
                asset_separated,
                start_datetime,
                self.datetime_end,
                timespan=ts_unit,
                quote_asset=quote_asset,
                dt=date_time_now,
                datastyle="ohlc",
                include_after_hours=True  # Default to True for extended hours data
            )
            if df_ohlc is None:
                logger.info(f"\nSKIP: No OHLC data found for {asset_separated} from ThetaData")
                return None

            # Quote data (bid/ask) is only available for intraday data (minute, hour, second)
            # For daily+ data, only use OHLC
            if self._use_quote_data and ts_unit in ["minute", "hour", "second"]:
                # Get quote data from ThetaData
                df_quote = thetadata_helper.get_price_data(
                    self._username,
                    self._password,
                    asset_separated,
                    start_datetime,
                    self.datetime_end,
                    timespan=ts_unit,
                    quote_asset=quote_asset,
                    dt=date_time_now,
                    datastyle="quote",
                    include_after_hours=True  # Default to True for extended hours data
                )

                # Check if we have data
                if df_quote is None:
                    logger.info(f"\nSKIP: No QUOTE data found for {quote_asset} from ThetaData")
                    return None

                # Combine the ohlc and quote data using outer join to preserve all data
                # Use forward fill for missing quote values (ThetaData's recommended approach)
                df = pd.concat([df_ohlc, df_quote], axis=1, join='outer')

                # Forward fill missing quote values
                quote_columns = ['bid', 'ask', 'bid_size', 'ask_size', 'bid_condition', 'ask_condition', 'bid_exchange', 'ask_exchange']
                existing_quote_cols = [col for col in quote_columns if col in df.columns]
                if existing_quote_cols:
                    df[existing_quote_cols] = df[existing_quote_cols].fillna(method='ffill')

                    # Log how much forward filling occurred
                    if 'bid' in df.columns and 'ask' in df.columns:
                        remaining_nulls = df[['bid', 'ask']].isna().sum().sum()
                        if remaining_nulls > 0:
                            logger.info(f"Forward-filled missing quote values for {asset_separated}. {remaining_nulls} nulls remain at start of data.")
            else:
                df = df_ohlc

        except Exception as e:
            raise Exception("Error getting data from ThetaData") from e

        if df is None or df.empty:
            return None

        data = Data(asset_separated, df, timestep=ts_unit, quote=quote_asset)
        pandas_data_update = self._set_pandas_data_keys([data])
        if pandas_data_update is not None:
            # Add the keys to the self.pandas_data dictionary
            self.pandas_data.update(pandas_data_update)
            self._data_store.update(pandas_data_update)


    def _pull_source_symbol_bars(
        self,
        asset,
        length,
        timestep=None,
        timeshift=None,
        quote=None,
        exchange=None,
        include_after_hours=True,
    ):
        try:
            dt = self.get_datetime()
            self._update_pandas_data(asset, quote, 1, timestep, dt)
        except Exception as e:
            logger.error(f"\nERROR: _pull_source_symbol_bars from ThetaData: {e}, {dt}, asset:{asset}")

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

    def get_last_price(self, asset, timestep="minute", quote=None, exchange=None, **kwargs) -> Union[float, Decimal, None]:
        try:
            dt = self.get_datetime()
            self._update_pandas_data(asset, quote, 1, timestep, dt)
        except Exception as e:
            logger.error(f"\nERROR: get_last_price from ThetaData: {e}, {dt}, asset:{asset}")

        return super().get_last_price(asset=asset, quote=quote, exchange=exchange)

    def get_quote(self, asset, timestep="minute", quote=None, exchange=None, **kwargs):
        """
        Get quote data for an asset during backtesting.

        Parameters
        ----------
        asset : Asset object
            The asset for which the quote is needed.
        timestep : str, optional
            The timestep to use for the data.
        quote : Asset object, optional
            The quote asset for cryptocurrency pairs.
        exchange : str, optional
            The exchange to get the quote from.
        **kwargs : dict
            Additional keyword arguments.

        Returns
        -------
        Quote
            A Quote object with the quote information.
        """
        try:
            dt = self.get_datetime()
            self._update_pandas_data(asset, quote, 1, timestep, dt)
        except Exception as e:
            logger.error(f"\nnERROR: get_quote from ThetaData: {e}, {dt}, asset:{asset}")

        return super().get_quote(asset=asset, quote=quote, exchange=exchange)

    def get_chains(self, asset):
        """
        Get option chains using cached implementation (matches Polygon pattern).

        Parameters
        ----------
        asset : Asset
            The asset to get data for.

        Returns
        -------
        Chains:
            A Chains entity object (dict subclass) with the structure:
            {
                "Multiplier": 100,
                "Exchange": "SMART",
                "Chains": {
                    "CALL": {
                        "2023-07-31": [100.0, 101.0, ...],
                        ...
                    },
                    "PUT": {
                        "2023-07-31": [100.0, 101.0, ...],
                        ...
                    }
                }
            }
        """
        from lumibot.entities import Chains

        chains_dict = thetadata_helper.get_chains_cached(
            username=self._username,
            password=self._password,
            asset=asset,
            current_date=self.get_datetime().date()
        )

        # Wrap in Chains entity for modern API
        return Chains(chains_dict)
