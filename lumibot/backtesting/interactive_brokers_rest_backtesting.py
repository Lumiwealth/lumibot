from collections import OrderedDict
from datetime import datetime, timedelta

import pandas as pd

from lumibot.data_sources import InteractiveBrokersRESTData, DataSourceBacktesting
from lumibot.entities import Asset, Data


class InteractiveBrokersRESTBacktesting(DataSourceBacktesting, InteractiveBrokersRESTData):
    """
    Backtesting implementation of Interactive Brokers REST API

    This class allows using Interactive Brokers REST API data for backtesting
    while maintaining compatibility with the live trading implementation.
    """

    def __init__(self, datetime_start, datetime_end, pandas_data=None, **kwargs):
        # Initialize the data store before anything else 
        self.pandas_data = OrderedDict()
        self._data_store = self.pandas_data
        self._date_index = None
        self._timestep = "minute"

        # Initialize the parent classes
        # Note: The order matters for multiple inheritance
        InteractiveBrokersRESTData.__init__(self, **kwargs)
        DataSourceBacktesting.__init__(self, datetime_start, datetime_end, **kwargs)

        # Set pandas data if provided
        if pandas_data is not None:
            self._set_pandas_data(pandas_data)

    def _set_pandas_data(self, pandas_data):
        """Set the pandas data to use for backtesting"""
        if pandas_data is None:
            return

        # Process each data item and add to the pandas_data dictionary
        for data in pandas_data:
            key = self._get_data_key(data)
            self.pandas_data[key] = data
            self._data_store[key] = data

    def _get_data_key(self, data):
        """Get the key to use for the data in the pandas_data dictionary"""
        if isinstance(data.asset, tuple):
            return data.asset
        elif isinstance(data.asset, Asset):
            if data.quote is None:
                return (data.asset, Asset(symbol="USD", asset_type="forex"))
            return (data.asset, data.quote)
        else:
            raise ValueError("Asset must be an Asset or a tuple of Asset and quote")

    def _update_pandas_data(self, asset, quote, length, timestep, start_dt=None):
        """
        Get asset data and update the self.pandas_data dictionary.

        This method retrieves historical data from Interactive Brokers REST API
        and stores it in the pandas_data dictionary for use during backtesting.
        """
        try:
            # Form the key for the data store
            key = asset if isinstance(asset, tuple) else (asset, quote or Asset(symbol="USD", asset_type="forex"))

            # Check if we already have data for this asset
            if key in self.pandas_data:
                # We already have data for this asset
                return

            # Get data from IB REST API
            bars = super().get_historical_prices(
                asset=asset,
                length=length,
                timestep=timestep,
                quote=quote
            )

            if bars and not bars.df.empty:
                data = Data(asset, bars.df, timestep=timestep, quote=quote)
                self.pandas_data[key] = data
                self._data_store[key] = data
        except Exception as e:
            print(f"Error in _update_pandas_data: {e}")

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
        """
        Get bars for a given asset during backtesting.

        This method will first try to update the pandas data store with historical data
        for the asset, and then delegate to the parent class to retrieve the bars.
        """
        if not timestep:
            timestep = self.get_timestep()

        # Try to update the data store with historical data
        try:
            dt = self.get_datetime()
            self._update_pandas_data(asset, quote, length, timestep, dt)
        except Exception as e:
            print(f"Error updating data for {asset}: {e}")

        # Let the parent class handle the rest using the updated data store
        return super()._pull_source_symbol_bars(
            asset, length, timestep, timeshift, quote, exchange, include_after_hours
        )

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
        """
        Get pricing data for an asset between specific dates during backtesting.
        """
        # Try to update the data store with historical data
        self._update_pandas_data(asset, quote, 1, timestep)

        # Let the parent class handle the rest using the updated data store
        response = super()._pull_source_symbol_bars_between_dates(
            asset, timestep, quote, exchange, include_after_hours, start_date, end_date
        )

        if response is None:
            return None

        bars = self._parse_source_symbol_bars(response, asset, quote=quote)
        return bars

    def get_last_price(self, asset, timestep="minute", quote=None, exchange=None, **kwargs):
        """
        Get the last price for an asset during backtesting.
        """
        try:
            dt = self.get_datetime()
            self._update_pandas_data(asset, quote, 1, timestep, dt)
        except Exception as e:
            print(f"Error get_last_price from Interactive Brokers REST: {e}")

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
            print(f"Error get_quote from Interactive Brokers REST: {e}")

        return super().get_quote(asset=asset, quote=quote, exchange=exchange)
