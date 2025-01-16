import logging
import traceback
from collections import OrderedDict, defaultdict
from datetime import date, timedelta
from typing import Optional

from polygon.exceptions import BadResponse
from termcolor import colored

from lumibot.data_sources import PandasData
from lumibot.entities import Asset, Data
from lumibot.tools import polygon_helper
from lumibot.tools.polygon_helper import PolygonClient

START_BUFFER = timedelta(days=5)


class PolygonDataBacktesting(PandasData):
    """
    A backtesting data source implementation for Polygon.io, backed by a local DuckDB cache.

    This class fetches data in "minute" or "day" bars from Polygon, stores it locally in
    DuckDB for reuse, then surfaces the data to LumiBot for historical/backtesting usage.

    Attributes
    ----------
    MAX_STORAGE_BYTES : Optional[int]
        If set, indicates the maximum number of bytes we want to store in memory for
        self.pandas_data. Exceeding this triggers LRU eviction.

    polygon_client : PolygonClient
        A rate-limited REST client for Polygon.
    """

    def __init__(
        self,
        datetime_start,
        datetime_end,
        pandas_data=None,
        api_key: Optional[str] = None,
        max_memory: Optional[int] = None,
        **kwargs,
    ):
        """
        Constructor for the PolygonDataBacktesting class.

        Parameters
        ----------
        datetime_start : datetime
            The start datetime for the backtest.
        datetime_end : datetime
            The end datetime for the backtest.
        pandas_data : dict or OrderedDict, optional
            Pre-loaded data, if any. Typically None, meaning we fetch from scratch.
        api_key : str, optional
            Polygon.io API key. If not provided, it may fall back to lumibot.credentials.
        max_memory : int, optional
            Maximum bytes to store in memory. Exceeding triggers LRU eviction.
        kwargs : dict
            Additional arguments passed to the parent PandasData constructor.
        """
        super().__init__(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            pandas_data=pandas_data,
            api_key=api_key,
            **kwargs
        )

        self.MAX_STORAGE_BYTES = max_memory
        self.polygon_client = PolygonClient.create(api_key=api_key)

    def _enforce_storage_limit(pandas_data: OrderedDict) -> None:
        """
        Evict oldest data from self.pandas_data if we exceed the max memory storage.
        This uses an LRU approach: pop the earliest inserted item until under limit.
        """
        storage_used = sum(data.df.memory_usage().sum() for data in pandas_data.values())
        logging.info(f"{storage_used = :,} bytes for {len(pandas_data)} items")
        while storage_used > PolygonDataBacktesting.MAX_STORAGE_BYTES:
            k, d = pandas_data.popitem(last=False)  # pop oldest
            mu = d.df.memory_usage().sum()
            storage_used -= mu
            logging.info(f"Storage limit exceeded. Evicted LRU data: {k} used {mu:,} bytes")

    def _update_pandas_data(
        self,
        asset: Asset,
        quote: Optional[Asset],
        length: int,
        timestep: str,
        start_dt=None
    ) -> None:
        """
        Ensure we have enough data for (asset, quote) in self.pandas_data by fetching from
        Polygon (via the local DuckDB cache) if needed.

        Parameters
        ----------
        asset : Asset
            The Asset to fetch data for.
        quote : Asset, optional
            The quote asset, e.g. USD for crypto. If None, defaults to Asset("USD","forex").
        length : int
            The number of bars we want to make sure we have at minimum.
        timestep : str
            "minute" or "day".
        start_dt : datetime, optional
            If given, treat that as the "current" datetime. Otherwise we use self.get_datetime().
        """
        search_asset = asset
        asset_separated = asset
        quote_asset = quote if quote is not None else Asset("USD", "forex")

        if isinstance(search_asset, tuple):
            asset_separated, quote_asset = search_asset
        else:
            search_asset = (search_asset, quote_asset)

        # Determine needed start date range
        start_datetime, ts_unit = self.get_start_datetime_and_ts_unit(
            length, timestep, start_dt, start_buffer=START_BUFFER
        )

        # If we already have data in self.pandas_data, check if it's enough
        if search_asset in self.pandas_data:
            asset_data = self.pandas_data[search_asset]
            asset_data_df = asset_data.df
            data_start_datetime = asset_data_df.index[0]
            data_timestep = asset_data.timestep

            # If timesteps match and we have a buffer, skip the fetch
            if data_timestep == ts_unit:
                if (data_start_datetime - start_datetime) < START_BUFFER:
                    return

            # If we request day but have minute, we might have enough
            if ts_unit == "day" and data_timestep == "minute":
                if (data_start_datetime - start_datetime) < START_BUFFER:
                    return
                else:
                    # Otherwise, we must re-fetch as minute
                    ts_unit = "minute"

        # Otherwise, fetch from polygon_helper
        try:
            df = polygon_helper.get_price_data_from_polygon(
                api_key=self._api_key,
                asset=asset_separated,
                start=start_datetime,
                end=self.datetime_end,
                timespan=ts_unit,
                quote_asset=quote_asset,
                force_cache_update=False,
            )
        except BadResponse as e:
            # Handle subscription or API key errors
            formatted_start = start_datetime.strftime("%Y-%m-%d")
            formatted_end = self.datetime_end.strftime("%Y-%m-%d")
            if "Your plan doesn't include this data timeframe" in str(e):
                error_message = colored(
                    f"Polygon Access Denied: Subscription does not allow that timeframe.\n"
                    f"Requested {asset_separated} {ts_unit} bars from {formatted_start} to {formatted_end}.\n"
                    f"Consider upgrading or adjusting your timeframe.\n",
                    color="red"
                )
                raise Exception(error_message) from e
            elif "Unknown API Key" in str(e):
                error_message = colored(
                    "Polygon Access Denied: Invalid API key.\n"
                    "Get an API key at https://polygon.io/?utm_source=affiliate&utm_campaign=lumi10\n"
                    "Use coupon code 'LUMI10' for 10% off.\n",
                    color="red"
                )
                raise Exception(error_message) from e
            else:
                logging.error(traceback.format_exc())
                raise
        except Exception as e:
            logging.error(traceback.format_exc())
            raise Exception("Error getting data from Polygon") from e

        if df is None or df.empty:
            return

        # Store newly fetched data in self.pandas_data
        data = Data(asset_separated, df, timestep=ts_unit, quote=quote_asset)
        pandas_data_update = self._set_pandas_data_keys([data])
        self.pandas_data.update(pandas_data_update)

        # Enforce memory limit
        if self.MAX_STORAGE_BYTES:
            self._enforce_storage_limit(self.pandas_data)

    def _pull_source_symbol_bars(
        self,
        asset: Asset,
        length: int,
        timestep: str = "day",
        timeshift: Optional[int] = None,
        quote: Optional[Asset] = None,
        exchange: Optional[str] = None,
        include_after_hours: bool = True
    ):
        """
        Overridden method to pull data using the local DuckDB caching approach.

        Parameters
        ----------
        asset : Asset
        length : int
        timestep : str
            "minute" or "day"
        timeshift : int, optional
        quote : Asset, optional
        exchange : str, optional
        include_after_hours : bool
            Not used in the duckdb fetch, but required signature from parent.

        Returns
        -------
        Bars in the PandasData parent format.
        """
        current_dt = self.get_datetime()
        self._update_pandas_data(asset, quote, length, timestep, current_dt)
        return super()._pull_source_symbol_bars(
            asset, length, timestep, timeshift, quote, exchange, include_after_hours
        )

    def get_historical_prices_between_dates(
        self,
        asset: Asset,
        timestep: str = "minute",
        quote: Optional[Asset] = None,
        exchange: Optional[str] = None,
        include_after_hours: bool = True,
        start_date=None,
        end_date=None
    ):
        """
        Retrieve historical OHLCV data between start_date and end_date, caching in DuckDB.

        Parameters
        ----------
        asset : Asset
        timestep : str
            "minute" or "day".
        quote : Asset, optional
        exchange : str, optional
        include_after_hours : bool
        start_date : datetime, optional
        end_date : datetime, optional

        Returns
        -------
        pd.DataFrame or None
            The bars for [start_date, end_date], or None if no data.
        """
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
        asset: Asset,
        timestep: str = "minute",
        quote: Optional[Asset] = None,
        exchange: Optional[str] = None,
        **kwargs
    ):
        """
        Return the last (most recent) price from local DuckDB data, ensuring data is updated.

        Parameters
        ----------
        asset : Asset
        timestep : str
            "minute" or "day"
        quote : Asset, optional
        exchange : str, optional

        Returns
        -------
        float
            The last (close) price for the given asset.
        """
        try:
            dt = self.get_datetime()
            self._update_pandas_data(asset, quote, 1, timestep, dt)
        except Exception as e:
            print(f"Error get_last_price from Polygon: {e}")
            print(f"Asset={asset}, Quote={quote}, Timestep={timestep}, Dt={dt}, Exception={e}")

        return super().get_last_price(asset=asset, quote=quote, exchange=exchange)

    def get_chains(
        self,
        asset: Asset,
        quote: Optional[Asset] = None,
        exchange: Optional[str] = None
    ):
        """
        Retrieve Option Chains from Polygon, with caching for the contract definitions.

        Parameters
        ----------
        asset : Asset
            The underlying symbol as a LumiBot Asset.
        quote : Asset, optional
        exchange : str, optional

        Returns
        -------
        dict
            A dictionary of calls and puts with their strikes by expiration date.
        """
        from lumibot.tools.polygon_helper import get_option_chains_with_cache
        return get_option_chains_with_cache(
            polygon_client=self.polygon_client,
            asset=asset,
            current_date=self.get_datetime().date()
        )
