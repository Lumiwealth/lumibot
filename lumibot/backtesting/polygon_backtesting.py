import logging
import re
import traceback
from datetime import date, timedelta

from polygon import RESTClient

from lumibot.data_sources import PandasData
from lumibot.entities import Asset, Data
from lumibot.tools import polygon_helper

START_BUFFER = timedelta(days=5)


class PolygonDataBacktesting(PandasData):
    """
    Backtesting implementation of Polygon
    """

    def __init__(
        self,
        datetime_start,
        datetime_end,
        pandas_data=None,
        api_key=None,
        has_paid_subscription=True,  # TODO: Set to False after new backtest is released
        **kwargs,
    ):
        super().__init__(
            datetime_start=datetime_start, datetime_end=datetime_end, pandas_data=pandas_data, api_key=api_key, **kwargs
        )
        self.has_paid_subscription = has_paid_subscription

        # RESTClient API for Polygon.io polygon-api-client
        self.polygon_client = RESTClient(self._api_key)

    def get_start_datetime_and_ts_unit(self, length, timestep):
        """
        Get the start datetime for the data.

        Parameters
        ----------
        length : int
            The number of data points to get.
        timestep : str
            The timestep to use. For example, "1minute" or "1hour" or "1day".

        Returns
        -------
        datetime
            The start datetime.
        str
            The timestep unit.
        """
        # Convert timestep string to timedelta and get start datetime
        td, ts_unit = self.convert_timestep_str_to_timedelta(timestep)
        # Multiply td by length to get the end datetime
        td *= length
        start_datetime = self.datetime_start - td

        # Subtract an extra 5 days to the start datetime to make sure we have enough
        # data when it's a sparsely traded asset, especially over weekends
        start_datetime = start_datetime - START_BUFFER

        return start_datetime, ts_unit

    def update_pandas_data(self, asset, quote, length, timestep):
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
        search_asset = asset
        asset_separated = asset
        quote_asset = quote if quote is not None else Asset("USD", "forex")

        if isinstance(search_asset, tuple):
            asset_separated, quote_asset = search_asset
        else:
            search_asset = (search_asset, quote_asset)

        # Get the start datetime and timestep unit
        start_datetime, ts_unit = self.get_start_datetime_and_ts_unit(length, timestep)

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
                has_paid_subscription=self.has_paid_subscription,
            )
        except Exception as e:
            logging.error(traceback.format_exc())
            raise Exception("Error getting data from Polygon") from e

        if df is None:
            return None

        pandas_data = []
        data = Data(asset_separated, df, timestep=ts_unit, quote=quote_asset)
        pandas_data.append(data)
        pandas_data_updated = self._set_pandas_data_keys(pandas_data)

        return pandas_data_updated

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
        pandas_data_update = self.update_pandas_data(asset, quote, length, timestep)

        if pandas_data_update is not None:
            # Add the keys to the self.pandas_data dictionary
            self.pandas_data.update(pandas_data_update)

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
        pandas_data_update = self.update_pandas_data(asset, quote, 1, timestep)
        if pandas_data_update is not None:
            # Add the keys to the self.pandas_data dictionary
            self.pandas_data.update(pandas_data_update)

        response = super()._pull_source_symbol_bars_between_dates(
            asset, timestep, quote, exchange, include_after_hours, start_date, end_date
        )

        if response is None:
            return None

        bars = self._parse_source_symbol_bars(response, asset, quote=quote)
        return bars

    def get_last_price(self, asset, timestep="minute", quote=None, exchange=None, **kwargs):
        try:
            pandas_data_update = self.update_pandas_data(asset, quote, 1, timestep)
            if pandas_data_update is not None:
                # Add the keys to the self.pandas_data dictionary
                self.pandas_data.update(pandas_data_update)
                self._data_store.update(pandas_data_update)
        except Exception as e:
            print(f"Error get_last_price from Polygon: {e}")

        return super().get_last_price(asset=asset, quote=quote, exchange=exchange)

    def get_chains(self, asset):
        """
        Integrates the Polygon client library into the LumiBot backtest for Options Data in the same
        structure as Interactive Brokers options chain data

        Parameters
        ----------
        asset : Asset
            The asset to get data for.

        Returns
        -------
        dictionary:
            A dictionary nested with a dictionarty of Polygon Option Contracts information broken out by Exchange,
            with embedded lists for Expirations and Strikes.
            {'SMART': {'TradingClass': 'SPY', 'Multiplier': 100, 'Expirations': [], 'Strikes': []}}

            - `TradingClass` (str) eg: `FB`
            - `Multiplier` (str) eg: `100`
            - `Expirations` (list of str) eg: [`20230616`, ...]
            - `Strikes` (list of floats) eg: [`100.0`, ...]
        """

        # All Option Contracts | get_chains matching IBKR |
        # {'SMART': {'TradingClass': 'SPY', 'Multiplier': 100, 'Expirations': [], 'Strikes': []}}
        option_contracts = {"SMART": {"TradingClass": None, "Multiplier": None, "Expirations": [], "Strikes": []}}
        contracts = option_contracts["SMART"]  # initialize contracts
        today = self.get_datetime().date()
        real_today = date.today()

        # All Contracts | to match lumitbot, more inputs required from get_chains()
        # If the strategy is using a recent backtest date, some contracts might not be expired yet, query those too
        expired_list = [True, False] if real_today - today <= timedelta(days=31) else [True]
        polygon_contracts = []
        for expired in expired_list:
            polygon_contracts.extend(
                list(
                    self.polygon_client.list_options_contracts(
                        underlying_ticker=asset.symbol,
                        expiration_date_gte=today,
                        expired=expired,  # Needed so BackTest can look at old contracts to find the expirations/strikes
                        limit=1000,
                    )
                )
            )

        for polygon_contract in polygon_contracts:
            # Return to Loop and Skip if Multipler is not 100 because non-standard contracts are not supported
            if polygon_contract.shares_per_contract != 100:
                continue

            # Contract Data | Attributes
            exchange = polygon_contract.primary_exchange
            contracts["TradingClass"] = polygon_contract.underlying_ticker
            contracts["Multiplier"] = polygon_contract.shares_per_contract
            contracts["Expirations"].append(polygon_contract.expiration_date)
            contracts["Strikes"].append(polygon_contract.strike_price)

            option_contracts["SMART"] = contracts
            option_contracts[exchange] = contracts

        return option_contracts
