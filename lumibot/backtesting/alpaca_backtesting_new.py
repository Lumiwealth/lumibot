import logging
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from decimal import Decimal

import pandas as pd
import numpy as np
from alpaca.data.historical import CryptoHistoricalDataClient
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import CryptoBarsRequest, StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

from lumibot.data_sources import DataSourceBacktesting
from lumibot.entities import Asset, Bars
from lumibot import LUMIBOT_CACHE_FOLDER, LUMIBOT_DEFAULT_TIMEZONE
from lumibot.tools.helpers import (
    date_n_days_from_date,
    parse_timestep_qty_and_unit,
    get_trading_days,
    get_trading_times
)


class AlpacaBacktestingNew(DataSourceBacktesting):
    SOURCE = "ALPACA"
    MIN_TIMESTEP = "minute"

    # noinspection PyMethodMayBeStatic
    def _alpaca_timeframe_from_timestep(self, timestep: str) -> TimeFrame:
        """Convert a timestep string to an Alpaca TimeFrame."""

        if ' ' in timestep or '/' in timestep:
            raise ValueError("Timestep cannot contain spaces or slashes.")

        timestep = timestep.lower()

        if timestep in ['day', '1d']:
            return TimeFrame.Day
        elif timestep in ['minute', '1m']:
            return TimeFrame.Minute
        elif timestep in ['hour', '1h']:
            return TimeFrame.Hour
        elif timestep in ['30m']:
            return TimeFrame(30, TimeFrameUnit.Minute)
        else:
            raise ValueError(f"Unsupported timestep: {timestep}")

    def __init__(
            self,
            datetime_start: datetime,
            datetime_end: datetime,
            backtesting_started: datetime | None = None,
            config: dict | None = None,
            api_key: str | None = None,
            show_progress_bar: bool = True,
            delay: int | None = None,
            pandas_data: dict | list = None,
            **kwargs
    ):
        """
        Initializes a class instance for handling backtesting data and parameters. This initialization 
        process involves setting up key configurations, verifying account types, and preparing backtesting 
        timings, timezones, and historical data clients. Data caching and warm-up trading days are also 
        appropriately configured.

        Args:
            datetime_start (datetime): The starting datetime for the backtesting process. Inclusive.
            datetime_end (datetime): The ending datetime for the backtesting process. Exclusive.
            backtesting_started (datetime | None): Represents the datetime when backtesting started. Defaults to None.
            config (dict | None): Configuration dictionary containing required API keys and account details.
                Cannot be None as it's critical for API connections.
            api_key (str | None): API key for authorized data access. Optional as it can typically be found 
                within the provided config.
            show_progress_bar (bool): Indicates whether to show a progress bar during data operations. 
                Defaults to True.
            delay (int | None): Delay in seconds added between operations to simulate real-world activity. 
                Defaults to None.
            pandas_data (dict | list): Data to be loaded directly into pandas, allowing analysis or backtesting 
                without requiring external API calls.
            **kwargs: Additional keyword arguments, such as:
                - timestep (str): Interval for data ("day", "hour", etc.). Defaults to "day".
                - refresh_cache (bool): Whether to force cache refresh. Defaults to False.
                - tzinfo (ZoneInfo): The timezone information. Defaults to the system’s default timezone.
                - warm_up_trading_days (int): The number of trading days used for warm-up before processing 
                  the primary dataset. Defaults to 0.
                - market (str): Indicates the stock exchange or market (e.g., "NYSE"). Defaults to "NYSE".
                - auto_adjust (bool): Determines whether to auto-adjust data, such as stock splits. Defaults 
                  to True.

        Raises:
            ValueError: If the `config` argument is None or lacks a valid paper account setup.

        """
        self._datetime = None
        
        # Call the base class. We will override most stuff later.
        super().__init__(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            backtesting_started=backtesting_started,
            show_progress_bar=show_progress_bar,
            delay=delay,
            pandas_data=None,
        )

        self._timestep: str = kwargs.get('timestep', 'day')
        refresh_cache: bool = kwargs.get('refresh_cache', False)
        self._tzinfo: ZoneInfo = kwargs.get('tzinfo', ZoneInfo(LUMIBOT_DEFAULT_TIMEZONE))
        warm_up_trading_days: int = kwargs.get('warm_up_trading_days', 0)
        self._market: str = kwargs.get('market', "NYSE")
        self._auto_adjust: bool = kwargs.get('auto_adjust', True)

        self.CACHE_SUBFOLDER = 'alpaca'

        if config is None:
            raise ValueError("Config cannot be None. Please provide a valid configuration.")
        if not config.get("PAPER", True):
            raise ValueError("Backtesting is restricted to paper accounts. Pass in a paper account config.")

        self._crypto_client = CryptoHistoricalDataClient(
            api_key=config["API_KEY"],
            secret_key=config["API_SECRET"]
        )

        self._stock_client = StockHistoricalDataClient(
            api_key=config["API_KEY"],
            secret_key=config["API_SECRET"]
        )

        # We want self.datetime_start and self.datetime_end to be the start and end dates
        # of the data for the entire backtest including the warmup dates.
        # Also, they should be midnight of the tzinfo passed in.
        start_dt = datetime(
            year=datetime_start.year,
            month=datetime_start.month,
            day=datetime_start.day,
            tzinfo=self._tzinfo
        )

        end_dt = datetime(
            year=datetime_end.year,
            month=datetime_end.month,
            day=datetime_end.day,
            tzinfo=self._tzinfo
        )

        if warm_up_trading_days > 0:
            warm_up_start_dt = date_n_days_from_date(
                n_days=warm_up_trading_days,
                start_datetime=start_dt,
                market=self._market,
            )
            # Combine with a default time (midnight)
            warm_up_start_dt = datetime.combine(warm_up_start_dt, datetime.min.time())
            # Make it timezone-aware
            warm_up_start_dt = pd.to_datetime(warm_up_start_dt).tz_localize(self._tzinfo)

        else:
            warm_up_start_dt = start_dt

        self.datetime_start = warm_up_start_dt
        self.datetime_end = end_dt

        self._data_store: dict[str, pd.DataFrame] = {}
        
    def get_last_price(
            self, 
            asset: Asset, 
            quote: Asset | None = None,
            exchange: str | None = None
    ) -> float | Decimal | None:
        """Takes an asset and returns the last known price"""

        _, timestep_unit = parse_timestep_qty_and_unit(self._timestep)

        bars = self.get_historical_prices(
            asset=asset,
            length=1,
            timestep=timestep_unit,
            quote=quote,
            timeshift=None
        )

        if bars is None or bars.df.empty:
            return None

        open_ = bars.df.iloc[0].open
        if isinstance(open_, np.int64):
            return Decimal(open_.item())
        elif isinstance(open_, float):
            return Decimal(open_)
        elif isinstance(open_, Decimal):
            return open_
        else:
            raise ValueError(f"Invalid open value type: {type(open)} for asset {asset.symbol}")

    def get_historical_prices(
            self,
            asset: Asset,
            length: int,
            timestep: str = "",
            timeshift: timedelta | None = None,
            quote: Asset | None = None,
            exchange: str | None = None,
            include_after_hours: bool = True
    ) -> Bars | None:
        """
        Get bars for a given asset, going back in time from now, getting length number of bars by timestep.
        For example, with a length of 10 and a timestep of "1day", and now timeshift, this
        would return the last 10 daily bars.
    
        - Higher-level method that returns a `Bars` object
        - Handles timezone conversions automatically
        - Includes additional metadata and processing
        - Preferred for strategy development and backtesting
        - Returns normalized data with consistent format across data sources
    
        Parameters
        ----------
        asset : Asset
            The asset to get the bars for.
        length : int
            The number of bars to get.
        timestep : str
            The timestep to get the bars at. For example, "1minute" or "1hour" or "1day".
        timeshift : datetime.timedelta
            The amount of time to shift the bars by. For example, if you want the bars from 1 hour ago to now,
            you would set timeshift to 1 hour.
        quote : Asset
            The quote asset to get the bars for.
        exchange : str
            The exchange to get the bars for.
        include_after_hours : bool
            Whether to include after hours data.
    
        Returns
        -------
        pd.DataFrame | None
            The bars for the asset.
        """

        key = self._get_asset_key(base_asset=asset, quote_asset=quote)

        if key not in self._data_store and not self._load_ohlcv_into_data_store(key):
            self._download_and_cache_ohlcv_data(
                base_asset=asset,
                quote_asset=quote,
                timestep=self._timestep,
                market=self._market,
                tzinfo=self._tzinfo,
                datetime_start=self.datetime_start,
                datetime_end=self.datetime_end,
                auto_adjust=self._auto_adjust,
            )

        df = self._data_store[key]

        # Locate the index of self._datetime in the data by matching the timestamp
        current_index = df.index.searchsorted(self._datetime)
        if current_index >= len(df):
            raise ValueError(f"Current datetime {self._datetime} not found in the dataset for {key}.")

        # Shift the index by the number of timeshift bars
        if timeshift:
            shifted_index = current_index - timeshift
        else:
            shifted_index = current_index

        # Ensure the shifted index is within valid range
        if shifted_index < 0 or shifted_index + length > len(df):
            logging.warning(
                f"Requested range [{shifted_index}:{shifted_index + length}] is out of bounds for the dataset.")
            return None

        # Extract the last `length` number of bars starting from the shifted index
        result = df.iloc[shifted_index: shifted_index + length]

        bars = Bars(result, self.SOURCE, asset=asset, quote=quote)
        return bars

    def get_chains(self, asset, quote=None):
        """Mock implementation for getting option chains"""
        return {}

    def _get_asset_key(
            self,
            *,
            base_asset: Asset,
            quote_asset: Asset,
            timestep: str = None,
            market: str = None,
            tzinfo: ZoneInfo = None,
            datetime_start: datetime = None,
            datetime_end: datetime = None,
            auto_adjust: bool = None,
    ) -> str:
        """
        Generate a unique key for an asset combination with specific parameters.

        Parameters
        ----------
        base_asset: Asset - Base asset of the pair.
        quote_asset: Asset - Quote asset of the pair.
        market: str - Market or exchange identifier.
        tzinfo: ZoneInfo - Timezone information.
        timestep: str - Timestep for price data granularity.
        datetime_start: datetime - The start date for data retrieval.
        datetime_end: datetime - The end date for data retrieval.
        auto_adjust: bool - Flag to indicate if auto-adjustment is applied.

        Returns
        -------
        str - A unique key string.
        """

        if base_asset is None:
            raise ValueError("Base asset must be provided.")

        if market is None:
            market = self._market

        if datetime_start is None:
            datetime_start = self.datetime_start

        if datetime_end is None:
            datetime_end = self.datetime_end

        if tzinfo is None:
            tzinfo = self._tzinfo

        if auto_adjust is None:
            auto_adjust = self._auto_adjust

        if timestep is None:
            timestep = self._timestep

        if not quote_asset:
            base_quote = f"{base_asset.symbol}"
        else:
            base_quote = f"{base_asset.symbol}-{quote_asset.symbol}"
        market = market
        tzinfo_str = str(tzinfo).replace("_", "-")
        start_date_str = datetime_start.strftime("%Y-%m-%d")
        end_date_str = datetime_end.strftime("%Y-%m-%d")
        auto_adjust_str = "AA" if auto_adjust else ""

        key_parts = [
            base_quote, market, timestep, tzinfo_str,
            auto_adjust_str, start_date_str, end_date_str
        ]
        key = "_".join(part for part in key_parts if part).upper()
        key = key.replace("/", "-")
        return key

    def _download_and_cache_ohlcv_data(
            self,
            *,
            base_asset: Asset = None,
            quote_asset: Asset = None,
            timestep: str = None,
            market: str = None,
            tzinfo: ZoneInfo = None,
            datetime_start: datetime = None,
            datetime_end: datetime = None,
            auto_adjust: bool = None,
    ) -> None:
        if base_asset is None:
            raise ValueError("The parameter 'base_asset' cannot be None.")
        if quote_asset is None:
            raise ValueError("The parameter 'quote_asset' cannot be None.")
        if timestep is None:
            raise ValueError("The parameter 'timestep' cannot be None.")
        if market is None:
            raise ValueError("The parameter 'market' cannot be None.")
        if tzinfo is None:
            raise ValueError("The parameter 'tzinfo' cannot be None.")
        if datetime_start is None:
            raise ValueError("The parameter 'datetime_start' cannot be None.")
        if datetime_end is None:
            raise ValueError("The parameter 'datetime_end' cannot be None.")
        if auto_adjust is None:
            raise ValueError("The parameter 'auto_adjust' cannot be None.")

        key = self._get_asset_key(base_asset=base_asset, quote_asset=quote_asset)

        # Directory to save cached data.
        cache_dir = os.path.join(LUMIBOT_CACHE_FOLDER, self.CACHE_SUBFOLDER)
        os.makedirs(cache_dir, exist_ok=True)

        # File path based on the unique key
        filename = f"{key}.csv"
        filepath = os.path.join(cache_dir, filename)

        logging.info(f"Fetching data for {key}")

        if base_asset.asset_type == 'crypto':
            client = self._crypto_client

            symbol = base_asset.symbol + '/' + quote_asset.symbol

            # noinspection PyArgumentList
            request_params = CryptoBarsRequest(
                symbol_or_symbols=symbol,
                timeframe=self._alpaca_timeframe_from_timestep(timestep),
                start=datetime_start,
                end=datetime_end,
            )
        else:
            client = self._stock_client
            adjustment = 'all' if auto_adjust else 'split'

            # noinspection PyArgumentList
            request_params = StockBarsRequest(
                symbol_or_symbols=base_asset.symbol,
                timeframe=self._alpaca_timeframe_from_timestep(timestep),
                start=datetime_start,
                end=datetime_end,
                adjustment=adjustment,
            )

        try:
            if isinstance(request_params, CryptoBarsRequest):
                bars = client.get_crypto_bars(request_params)
            else:
                bars = client.get_stock_bars(request_params)
        except Exception as e:
            raise RuntimeError(f"Failed to fetch data for {key}: {e}")

        df = bars.df.reset_index()
        if df.empty:
            raise RuntimeError(f"No data fetched for {key}.")

        # Ensure 'timestamp' is a pandas timestamp object
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        if df['timestamp'].dt.tz is None:
            df['timestamp'] = df['timestamp'].dt.tz_localize(tzinfo)
        else:
            df['timestamp'] = df['timestamp'].dt.tz_convert(tzinfo)
        df.drop(columns=['symbol'], inplace=True, errors='ignore')

        # Only keep rows within the specified date range
        df = df[(df['timestamp'] >= datetime_start) & (df['timestamp'] < datetime_end)]

        # Save to cache
        df.to_csv(filepath, index=False)

        # Store in _data_store
        df.set_index('timestamp', inplace=True)
        self._data_store[key] = df

    def _load_ohlcv_into_data_store(self, key: str) -> bool:
        """
        Loads OHLCV data from a cached file into the data store. If the loading is successful, returns True;
        otherwise, returns False.
    
        Parameters
        ----------
        key : str
            The unique key for the cached data file.
    
        Returns
        -------
        bool
            True if data is successfully loaded into the _data_store, False otherwise.
        """
        # Directory to find the cached data file.
        cache_dir = os.path.join(LUMIBOT_CACHE_FOLDER, self.CACHE_SUBFOLDER)
        filename = f"{key}.csv"
        filepath = os.path.join(cache_dir, filename)

        # Check if the file exists
        if not os.path.exists(filepath):
            return False

        try:
            df = pd.read_csv(filepath, parse_dates=['timestamp'])
            df['timestamp'] = df['timestamp'].dt.tz_convert(self._tzinfo)
            df.set_index('timestamp', inplace=True)
            self._data_store[key] = df
            logging.info(f"Loaded data for key: {key} from cache.")
            return True
        except Exception as e:
            logging.error(f"Failed to load cached data for key: {key}. Error: {e}")
            return False
