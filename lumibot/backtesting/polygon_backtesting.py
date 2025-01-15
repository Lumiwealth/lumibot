import logging
import traceback
from collections import OrderedDict, defaultdict
from datetime import date, timedelta

from polygon.exceptions import BadResponse
from termcolor import colored

from lumibot.data_sources import PandasData
from lumibot.entities import Asset, Data
from lumibot.tools import polygon_helper
from lumibot.tools.polygon_helper import PolygonClient

START_BUFFER = timedelta(days=5)


class PolygonDataBacktesting(PandasData):
    """
    Backtesting implementation of Polygon using a local DuckDB database cache.
    """

    def __init__(
        self,
        datetime_start,
        datetime_end,
        pandas_data=None,
        api_key=None,
        max_memory=None,
        **kwargs,
    ):
        super().__init__(
            datetime_start=datetime_start, datetime_end=datetime_end, pandas_data=pandas_data, api_key=api_key, **kwargs
        )

        # Memory limit, off by default
        self.MAX_STORAGE_BYTES = max_memory

        # RESTClient API for Polygon.io polygon-api-client
        self.polygon_client = PolygonClient.create(api_key=api_key)

    def _enforce_storage_limit(pandas_data: OrderedDict):
        """
        If there's a memory limit set, ensure we do not exceed it by evicting data.
        """
        storage_used = sum(data.df.memory_usage().sum() for data in pandas_data.values())
        logging.info(f"{storage_used = :,} bytes for {len(pandas_data)} items")
        while storage_used > PolygonDataBacktesting.MAX_STORAGE_BYTES:
            k, d = pandas_data.popitem(last=False)
            mu = d.df.memory_usage().sum()
            storage_used -= mu
            logging.info(f"Storage limit exceeded. Evicted LRU data: {k} used {mu:,} bytes")

    def _update_pandas_data(self, asset, quote, length, timestep, start_dt=None):
        """
        Get asset data and update the self.pandas_data dictionary using our local DuckDB cache.

        Parameters
        ----------
        asset : Asset
            The asset to get data for.
        quote : Asset
            The quote asset to use. e.g., if asset is "SPY" and quote is "USD", data is for "SPY/USD".
        length : int
            The number of data points to get.
        timestep : str
            The timestep to use. e.g. "1minute", "1hour", or "1day".
        start_dt : datetime
            The start datetime to use. If None, we use self.start_datetime.
        """
        search_asset = asset
        asset_separated = asset
        quote_asset = quote if quote is not None else Asset("USD", "forex")

        if isinstance(search_asset, tuple):
            asset_separated, quote_asset = search_asset
        else:
            search_asset = (search_asset, quote_asset)

        # Determine the date range and timeframe
        start_datetime, ts_unit = self.get_start_datetime_and_ts_unit(
            length, timestep, start_dt, start_buffer=START_BUFFER
        )

        # If we've fetched data for this asset before, see if we already have enough
        if search_asset in self.pandas_data:
            asset_data = self.pandas_data[search_asset]
            asset_data_df = asset_data.df
            data_start_datetime = asset_data_df.index[0]
            data_timestep = asset_data.timestep

            # If the timestep is the same and we have enough data, skip
            if data_timestep == ts_unit:
                # Check if we have enough data (5 days is the buffer)
                if (data_start_datetime - start_datetime) < START_BUFFER:
                    return

            # If we request daily data but have minute data, we might be good, etc.
            if ts_unit == "day":
                if data_timestep == "minute":
                    if (data_start_datetime - start_datetime) < START_BUFFER:
                        return
                    else:
                        ts_unit = "minute"
                elif data_timestep == "hour":
                    if (data_start_datetime - start_datetime) < START_BUFFER:
                        return
                    else:
                        ts_unit = "hour"

            if ts_unit == "hour":
                if data_timestep == "minute":
                    if (data_start_datetime - start_datetime) < START_BUFFER:
                        return
                    else:
                        ts_unit = "minute"

        # Download data from Polygon (with DuckDB caching in polygon_helper.py)
        try:
            df = polygon_helper.get_price_data_from_polygon(
                self._api_key,
                asset_separated,
                start_datetime,
                self.datetime_end,
                timespan=ts_unit,
                quote_asset=quote_asset,
                force_cache_update=False,  # could be parameterized
            )
        except BadResponse as e:
            formatted_start_datetime = start_datetime.strftime("%Y-%m-%d")
            formatted_end_datetime = self.datetime_end.strftime("%Y-%m-%d")
            if "Your plan doesn't include this data timeframe" in str(e):
                error_message = colored(
                    "Polygon Access Denied: Your subscription does not allow you to backtest that far back in time. "
                    f"You requested data for {asset_separated} {ts_unit} bars "
                    f"from {formatted_start_datetime} to {formatted_end_datetime}. "
                    "Consider changing your backtesting timeframe or upgrading your Polygon subscription at "
                    "https://polygon.io/?utm_source=affiliate&utm_campaign=lumi10 "
                    "You can use coupon code 'LUMI10' for 10% off. ",
                    color="red")
                raise Exception(error_message) from e
            elif "Unknown API Key" in str(e):
                error_message = colored(
                    "Polygon Access Denied: Your API key is invalid. "
                    "Check your API key and try again. "
                    "You can get an API key at https://polygon.io/?utm_source=affiliate&utm_campaign=lumi10 "
                    "Please use the full link to give us credit. Use coupon code 'LUMI10' for 10% off. ",
                    color="red")
                raise Exception(error_message) from e
            else:
                logging.error(traceback.format_exc())
                raise
        except Exception as e:
            logging.error(traceback.format_exc())
            raise Exception("Error getting data from Polygon") from e

        if (df is None) or df.empty:
            return

        data = Data(asset_separated, df, timestep=ts_unit, quote=quote_asset)
        pandas_data_update = self._set_pandas_data_keys([data])
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
        """
        Override for pulling data from local DuckDB (through get_price_data_from_polygon).
        """
        current_dt = self.get_datetime()
        self._update_pandas_data(asset, quote, length, timestep, current_dt)
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
        Retrieve historical prices for a date range, using local DuckDB caching.
        """
        self._update_pandas_data(asset, quote, 1, timestep)

        response = super()._pull_source_symbol_bars_between_dates(
            asset, timestep, quote, exchange, include_after_hours, start_date, end_date
        )

        if response is None:
            return None

        bars = self._parse_source_symbol_bars(response, asset, quote=quote)
        return bars

    def get_last_price(self, asset, timestep="minute", quote=None, exchange=None, **kwargs):
        """
        Return the last price, ensuring we have local data from DuckDB.
        """
        try:
            dt = self.get_datetime()
            self._update_pandas_data(asset, quote, 1, timestep, dt)
        except Exception as e:
            print(f"Error get_last_price from Polygon: {e}")
            print(f"Error get_last_price from Polygon: {asset=} {quote=} {timestep=} {dt=} {e}")

        return super().get_last_price(asset=asset, quote=quote, exchange=exchange)

    def get_chains(self, asset: Asset, quote: Asset = None, exchange: str = None):
        """
        Integrates the Polygon client library into LumiBot backtest for Options Data,
        using the new caching approach for chains (calls + puts).
        """
        from lumibot.tools.polygon_helper import get_option_chains_with_cache

        return get_option_chains_with_cache(
            polygon_client=self.polygon_client,
            asset=asset,
            current_date=self.get_datetime().date()
        )