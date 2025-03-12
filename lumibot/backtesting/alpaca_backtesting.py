import logging
import os
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from decimal import Decimal, ROUND_HALF_UP, ROUND_HALF_EVEN

import pandas as pd
import numpy as np
from alpaca.data.historical import CryptoHistoricalDataClient
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import CryptoBarsRequest, StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

from lumibot.data_sources import DataSourceBacktesting
from lumibot.entities import Asset, Bars
from lumibot import (
    LUMIBOT_CACHE_FOLDER,
    LUMIBOT_DEFAULT_TIMEZONE,
    LUMIBOT_DEFAULT_QUOTE_ASSET_SYMBOL,
    LUMIBOT_DEFAULT_QUOTE_ASSET_TYPE
)
from lumibot.tools.helpers import (
    date_n_days_from_date,
    get_trading_days,
    get_trading_times
)


class AlpacaBacktesting(DataSourceBacktesting):
    SOURCE = "ALPACA"
    MIN_TIMESTEP = "minute"
    TIMESTEP_MAPPING = [
        {"timestep": "day", "representations": [TimeFrame.Day]},
        {"timestep": "minute", "representations": [TimeFrame.Minute]},
    ]
    LUMIBOT_DEFAULT_QUOTE_ASSET = Asset(LUMIBOT_DEFAULT_QUOTE_ASSET_SYMBOL, LUMIBOT_DEFAULT_QUOTE_ASSET_TYPE)
    ALPACA_STOCK_PRECISION = Decimal('0.0001')
    ALPACA_CRYPTO_PRECISION = Decimal('0.000000001')

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
            datetime_end (datetime): The ending datetime for the backtesting process. Inclusive.
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
                - timestep (str): Interval for data ("day" or "minute"). Defaults to "day".
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
        
        # Call the base class.
        super().__init__(
            datetime_start=datetime_start,
            datetime_end=datetime_end,
            backtesting_started=backtesting_started,
            show_progress_bar=show_progress_bar,
            delay=delay,
            pandas_data=None,
        )

        self._timestep: str = kwargs.get('timestep', 'day')
        self._tzinfo: ZoneInfo = kwargs.get('tzinfo', ZoneInfo(LUMIBOT_DEFAULT_TIMEZONE))
        warm_up_trading_days: int = kwargs.get('warm_up_trading_days', 0)
        self._market: str = kwargs.get('market', "NYSE")
        self._auto_adjust: bool = kwargs.get('auto_adjust', True)
        self.CACHE_SUBFOLDER = 'alpaca'
        self._data_store: dict[str, pd.DataFrame] = {}
        self._refreshed_keys = {}
        self._refresh_cache: bool = kwargs.get('refresh_cache', False)

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

        # We want self._data_datetime_start and self._data_datetime_end to be the start and end dates
        # of the data for the entire backtest including the warmup dates.

        # The start should be midnight.
        start_dt = datetime(
            year=datetime_start.year,
            month=datetime_start.month,
            day=datetime_start.day,
            tzinfo=self._tzinfo
        )

        # The end should be the last minute of the day.
        end_dt = datetime(
            year=datetime_end.year,
            month=datetime_end.month,
            day=datetime_end.day,
            hour=23,
            minute=59,
            second=59,
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

        self._data_datetime_start = warm_up_start_dt
        self._data_datetime_end = end_dt

        if self._timestep not in ['day', 'minute']:
            raise ValueError("Invalid timestep passed. Must be 'day' or 'minute'.")

        self._trading_days = get_trading_days(
            self._market,
            self._data_datetime_start,
            self._data_datetime_end + timedelta(days=1),  # end_date is exclusive in this function
            tzinfo=self._tzinfo
        )

        # I think lumibot's got a bug in the strategy_executor when backtesting daily strategies.
        # After the backtest is over, it calls on_market_close() which calls get_last_price.
        # So if you run the backtest until the last day of data, lumibot will crash when it tries to calculate
        # the portfolio value. To avoid that crash (and because im avoiding dealing with people complaining about
        # backtest behavior changing if i fix it) im just hacking this so the backtest ends before the data runs out.
        if self._timestep == 'day':
            end_shift = -3
        else:
            end_shift = -3

        # stop backtesting before the last trading date of the backtest
        # so there's one day of data the backtester has to calculate all its stuff.
        last_trading_day = self._trading_days.iloc[end_shift]['market_open']
        self.datetime_end = last_trading_day

        self.datetime_start = start_dt
        self._datetime = self.datetime_start

    def get_last_price(
            self, 
            asset: Asset, 
            quote: Asset | None = None,
            exchange: str | None = None
    ) -> float | Decimal | None:
        """Takes an asset and returns the last known price"""

        if isinstance(asset, tuple):
            # Grr... Who made this a tuple?
            quote = asset[1]
            asset = asset[0]

        bars = self.get_historical_prices(
            asset=asset,
            length=1,  # Get one bar
            timestep=self._timestep,
            quote=quote,
            timeshift=None  # Get the current bar
        )

        if bars is None or bars.df.empty:
            return None

        precision = self.ALPACA_CRYPTO_PRECISION if asset.asset_type == 'crypto' else self.ALPACA_STOCK_PRECISION

        open_ = bars.df.iloc[0].open
        return Decimal(str(open_)).quantize(precision, rounding=ROUND_HALF_EVEN)

    def get_historical_prices(
            self,
            asset: Asset,
            length: int,
            timestep: str | None = None,
            timeshift: timedelta | None = None,
            quote: Asset | None = None,
            exchange: str | None = None,
            include_after_hours: bool = True
    ) -> Bars | None:
        """
        Get bars for a given asset, going back in time from now, getting length number of bars by timestep.
        For example, with a length of 10 and a timestep of "day", and now timeshift, this
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
            The timestep to get the bars at. Accepts "day" or "minute".
        timeshift : datetime.timedelta
            The amount of time to shift the reference point (self._datetime).
            If you want 10 daily bars from 1 week ago (not including the last week),
            you'd use timeshift=timedelta(days=7)
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

        if isinstance(asset, tuple):
            # Grr... Who made this a tuple?
            quote = asset[1]
            asset = asset[0]

        if timestep is None:
            timestep = self._timestep

        key = self._get_asset_key(base_asset=asset, quote_asset=quote, timestep=timestep)

        if self._refresh_cache and key not in self._refreshed_keys:
            # If we need are refreshing cache and we didn't refresh this key's cache yet, refresh it.
            self._download_and_cache_ohlcv_data(
                base_asset=asset,
                quote_asset=quote,
                timestep=timestep,
                market=self._market,
                tzinfo=self._tzinfo,
                data_datetime_start=self._data_datetime_start,
                data_datetime_end=self._data_datetime_end,
                auto_adjust=self._auto_adjust,
            )
            self._refreshed_keys[key] = True
        elif key not in self._data_store and not self._load_ohlcv_into_data_store(key):
            # If not refreshing or already refreshed, try to load from cache or download
            self._download_and_cache_ohlcv_data(
                base_asset=asset,
                quote_asset=quote,
                timestep=timestep,
                market=self._market,
                tzinfo=self._tzinfo,
                data_datetime_start=self._data_datetime_start,
                data_datetime_end=self._data_datetime_end,
                auto_adjust=self._auto_adjust,
            )

        df = self._data_store[key]

        # Locate the index of self._datetime adjusted by timeshift
        search_datetime = self._datetime
        if timeshift:
            search_datetime = self._datetime - timeshift

        current_index = df.index.searchsorted(search_datetime)
        if current_index >= len(df):
            raise ValueError(f"Datetime {search_datetime} not found in the dataset for {key}.")

        if length == 0:
            raise ValueError("Length must be non-zero")

        if length == 1:
            result = df.iloc[[current_index]]  # Return just the current row as DataFrame
        elif length > 1:
            # Check if we have enough historical data
            if current_index - length < 0:
                raise ValueError(
                    f"Not enough historical data. Requested {length} bars but only have {current_index} "
                    f"bars before the reference time."
                )
            result = df.iloc[current_index - length:current_index]
        else:  # length < 0
            # Check if we have enough forward data
            if current_index - length > len(df):  # Note: minus a negative is plus
                raise ValueError(
                    f"Not enough forward data. Requested {abs(length)} bars but only have "
                    f"{len(df) - current_index} bars after the reference time."
                )
            result = df.iloc[current_index:current_index - length]  # Note: minus a negative is plus

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
            data_datetime_start: datetime = None,
            data_datetime_end: datetime = None,
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
        timestep: str - Timestep of the source data. Accepts "day" or "minute".
        data_datetime_start: datetime - The start date of the data in the backtest.
        data_datetime_end: datetime - The end date of the data in the backtest. Inclusive.
        auto_adjust: bool - Flag to indicate if auto-adjustment is applied.

        Returns
        -------
        str - A unique key string.
        """

        if base_asset is None:
            raise ValueError("Base asset must be provided.")

        if quote_asset is None:
            quote_asset = self.LUMIBOT_DEFAULT_QUOTE_ASSET

        if market is None:
            market = self._market

        if data_datetime_start is None:
            data_datetime_start = self._data_datetime_start

        if data_datetime_end is None:
            data_datetime_end = self._data_datetime_end

        if tzinfo is None:
            tzinfo = self._tzinfo

        if auto_adjust is None:
            auto_adjust = self._auto_adjust

        if timestep is None:
            timestep = self._timestep

        if timestep not in ['day', 'minute']:
            raise ValueError(f"Invalid timestep {timestep}. Must be 'day' or 'minute'.")

        base_quote = f"{base_asset.symbol}-{base_asset.asset_type}_{quote_asset.symbol}-{quote_asset.asset_type}"
        market = market
        tzinfo_str = str(tzinfo).replace("_", "-")
        start_date_str = data_datetime_start.strftime("%Y-%m-%d")
        end_date_str = data_datetime_end.strftime("%Y-%m-%d")
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
            data_datetime_start: datetime = None,
            data_datetime_end: datetime = None,
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
        if data_datetime_start is None:
            raise ValueError("The parameter 'data_datetime_start' cannot be None.")
        if data_datetime_end is None:
            raise ValueError("The parameter 'data_datetime_end' cannot be None.")
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
                timeframe=self._parse_source_timestep(timestep, reverse=True),
                start=data_datetime_start,
                end=data_datetime_end + timedelta(days=1),  # alpaca end dates are exclusive
            )
        else:
            client = self._stock_client
            adjustment = 'all' if auto_adjust else 'split'

            # noinspection PyArgumentList
            request_params = StockBarsRequest(
                symbol_or_symbols=base_asset.symbol,
                timeframe=self._parse_source_timestep(timestep, reverse=True),
                start=data_datetime_start,
                end=data_datetime_end + timedelta(days=1),  # alpaca end dates are exclusive,
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

        df = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']]

        if self._timestep == 'day':
            # daily bars are NORMALLY indexed at midnight (the open of the bar).
            # To enable lumibot to use the open price of the bar for the get_last_price and fills,
            # the alpaca backtester adjusts daily bars to the open bar of the market.
            market_open = self._trading_days.iloc[0]['market_open']
            df['timestamp'] = df['timestamp'].map(
                lambda x: x.replace(hour=market_open.hour, minute=market_open.minute)
            )

        trading_times = get_trading_times(
            pcal=self._trading_days,
            timestep=self._timestep,
        )

        # Reindex the dataframe with a row for each bar we should have a trading iteration for.
        # Fill any empty bars with previous data.
        df = self._reindex_and_fill(df=df, trading_times=trading_times)

        # If asset is of type stock, quantize OHLC prices to ALPACA_STOCK_PRECISION
        if base_asset.asset_type == 'stock':
            for column in ['open', 'high', 'low', 'close']:
                df[column] = df[column].apply(
                    lambda x: Decimal(str(x)).quantize(self.ALPACA_STOCK_PRECISION, rounding=ROUND_HALF_EVEN)
                )
        elif base_asset.asset_type == 'crypto':
            for column in ['open', 'high', 'low', 'close']:
                df[column] = df[column].apply(
                    lambda x: Decimal(str(x)).quantize(self.ALPACA_CRYPTO_PRECISION, rounding=ROUND_HALF_EVEN)
                )

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

    def _reindex_and_fill(self, df: pd.DataFrame, trading_times: pd.DatetimeIndex) -> pd.DataFrame:
        # Check if all required columns are present
        required_columns = {"timestamp", "open", "high", "low", "close", "volume"}
        missing_columns = required_columns - set(df.columns)
        if missing_columns:
            raise ValueError(f"The dataframe is missing the following required columns: {', '.join(missing_columns)}")

        # Ensure timestamp is the index
        if df.index.name != "timestamp":
            df.set_index("timestamp", inplace=True)

        # Reindex and fill missing dates
        df = df.reindex(trading_times)

        # Set missing volume values to 0.0
        df['volume'] = df['volume'].fillna(0.0)

        # Forward fill missing close prices
        df['close'] = df['close'].fillna(method='ffill')

        # Fill NaN in 'open', 'high', and 'low' columns with the value from the 'close' column
        for column in ['open', 'high', 'low']:
            df[column] = df[column].fillna(df['close'])
            
        # Backward fill missing data to address gaps at the front
        df['open'] = df['open'].fillna(method='bfill')

        # Backfill NaN in 'open', 'high', and 'low' columns with the value from the 'open' column
        for column in ['high', 'low', 'close']:
            df[column] = df[column].fillna(df['open'])

        df.rename_axis("timestamp", inplace=True)
        df.reset_index(inplace=True)
        return df
