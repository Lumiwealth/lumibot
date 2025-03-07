import logging
from datetime import datetime, timezone, time, timedelta
from zoneinfo import ZoneInfo
from typing import List
import os

import pandas as pd
from alpaca.data.historical import CryptoHistoricalDataClient
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import CryptoBarsRequest, StockBarsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit

from lumibot.data_sources import PandasData
from lumibot.entities import Data, Asset
from lumibot import LUMIBOT_CACHE_FOLDER, LUMIBOT_DEFAULT_TIMEZONE
from lumibot.tools.helpers import (
    date_n_days_from_date,
    parse_timestep_qty_and_unit,
    get_trading_days,
    get_trading_times
)

logger = logging.getLogger(__name__)


def replace_slashes(string: str) -> str:
    """Clean a string by removing any special characters."""
    return string.replace('/', '-')


class AlpacaBacktesting(PandasData):

    # noinspection PyMethodMayBeStatic
    def alpaca_timeframe_from_timestep(self, timestep: str) -> TimeFrame:
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
            config: dict | None = None,
            api_key: str | None = None,
            show_progress_bar: bool = True,
            delay: int | None = None,
            pandas_data: dict | list = None,
            auto_adjust: bool = True,
            *args,
            **kwargs
    ):
        """
        Initializes an instance for fetching and managing historical data from Alpaca,
        either for crypto or stock symbols, with specified date range and adjustments.
        Uses Alpaca's Crypto and Stock Historical Data clients to retrieve the data.
        Supports caching and time interval customization. Also supports warm-up bars
        for initializing strategies.
        
        Args:
            datetime_start (datetime): The start datetime of the historical data range 
                (inclusive, set to midnight in the specified timezone).
            datetime_end (datetime): The end datetime of the historical data range 
                (exclusive, set to midnight in the specified timezone).
            config (dict | None): Configuration dictionary containing `API_KEY` and 
                `API_SECRET` for authenticating with Alpaca APIs.
            api_key (str | None): API key for authentication.
            show_progress_bar (bool): Whether to show a progress bar during data fetching. Default is True.
            delay (int | None): Optional delay (in seconds) for data fetching operations.
            pandas_data (dict | list | None): Preloaded pandas data to avoid fetching/caching. Optional.
            auto_adjust (bool): Whether to auto-adjust prices for splits and dividends. Default is True.
            args: Additional positional arguments for the parent class.
            kwargs: Additional keyword arguments for customization.
        
        Keyword Args (kwargs):
            tickers (List[str] | str | None): List of ticker symbols or a single ticker 
                symbol for the required data. Default is None.
            trading_hours_start (time): Start time for trading hours (inclusive). Applicable 
                when timestep is 'minute'. Default is 09:30.
            trading_hours_end (time): End time for trading hours (inclusive). Applicable 
                when timestep is 'minute'. Default is 15:59.
            timestep (str): Time interval for the historical data ('minute', 'hour', 'day').
                Default is 'day'.
            refresh_cache (bool): Whether to refresh the cached historical data. Default is False.
            tzinfo (ZoneInfo): Timezone to localize datetime values. Default is 
                ZoneInfo(LUMIBOT_DEFAULT_TIMEZONE).
            warm_up_trading_days (int): Number of additional trading days to fetch before 
                `datetime_start` for warming up strategies. Default is 0.
            market (str): The market to fetch data for. Default is 'NYSE'.
        """

        tickers: List[str] | str | None = kwargs.get('tickers', None)
        trading_hours_start: time = kwargs.get('trading_hours_start', time(9, 30))
        trading_hours_end: time = kwargs.get('trading_hours_end', time(15, 59))
        self._timestep: str = kwargs.get('timestep', 'day')
        refresh_cache: bool = kwargs.get('refresh_cache', False)
        self.tzinfo: ZoneInfo = kwargs.get('tzinfo', ZoneInfo(LUMIBOT_DEFAULT_TIMEZONE))
        warm_up_trading_days: int = kwargs.get('warm_up_trading_days', 0)
        self._market: str = kwargs.get('market', "NYSE")

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

        start_dt = datetime(
            year=datetime_start.year,
            month=datetime_start.month,
            day=datetime_start.day,
            tzinfo=self.tzinfo
        )

        end_dt = datetime(
            year=datetime_end.year,
            month=datetime_end.month,
            day=datetime_end.day,
            tzinfo=self.tzinfo
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
            warm_up_start_dt = pd.to_datetime(warm_up_start_dt).tz_localize(self.tzinfo)

        else:
            warm_up_start_dt = start_dt

        pandas_data = self._fetch_cache_and_load_data(
            tickers=tickers,
            start_dt=warm_up_start_dt,
            end_dt=end_dt,
            trading_hours_start=trading_hours_start,
            trading_hours_end=trading_hours_end,
            timestep=self._timestep,
            refresh_cache=refresh_cache,
            auto_adjust=auto_adjust,
            tzinfo=self.tzinfo
        )

        super().__init__(
            datetime_start=start_dt,
            datetime_end=end_dt,
            pandas_data=pandas_data,
            auto_adjust=auto_adjust
        )

    def _fetch_cache_and_load_data(
            self,
            *,
            tickers: List[str] | str,
            start_dt: datetime,
            end_dt: datetime,
            trading_hours_start: time,
            trading_hours_end: time,
            timestep: str = 'day',
            refresh_cache: bool = False,
            auto_adjust: bool = True,
            tzinfo: ZoneInfo = None,
    ) -> List[Data]:
        """ Fetches, caches, and loads data for a list of tickers.

        Parameters:
            tickers: A list of tickers to fetch data for.
            start_dt: The start date to fetch data for (inclusive from midnight) in YYYY-MM-DD format.
            end_dt: The end date to fetch data for (exclusive) in YYYY-MM-DD format.
            trading_hours_start: The start time for trading days in HH:MM (inclusive).
            trading_hours_end: The end time for trading days in HH:MM (inclusive).
            timestep: The interval to fetch data for. Options: 'day' (default), 'minute', 'hour'.
            refresh_cache: Ignore current cache and fetch from source again. Default is False.
            auto_adjust (bool): Split and dividend adjusted if true, split adjusted only if false.
            tzinfo : If not None, then localize the timezone of the dataframe to the 
            given timezone as a string. The values can be any supported by tz_localize,
            e.g. "US/Eastern", "UTC", etc.
        """

        # Directory to save cached data.
        cache_dir = os.path.join(LUMIBOT_CACHE_FOLDER, self.CACHE_SUBFOLDER)
        os.makedirs(cache_dir, exist_ok=True)

        # list to hold the data
        data_list = []

        # Convert tickers to a list if it is a string
        if isinstance(tickers, str):
            tickers = [tickers]

        adj = 'aat' if not auto_adjust else 'aaf'

        # We need to get the timeframe to pass into alpaca
        alpaca_timeframe = self.alpaca_timeframe_from_timestep(timestep)
        qty, alpaca_timestep = parse_timestep_qty_and_unit(timestep)
        if alpaca_timestep in ['day', 'minute']:
            self._timestep = alpaca_timestep
        else:
            self._timestep = 'minute'
        
        for ticker in tickers:
            cleaned_ticker = replace_slashes(ticker)
            filename = f"{cleaned_ticker}_{timestep}_{adj}_{start_dt.date().isoformat()}_{end_dt.date().isoformat()}_{tzinfo}"
            filename = replace_slashes(filename).upper()
            filename += '.csv'
            filepath = os.path.join(cache_dir, filename)

            quote_asset = Asset(symbol='USD', asset_type="forex")
            if '/' in ticker:
                base_name = ticker.split('/')[0]
                quote_name = ticker.split('/')[1]
                base_asset = Asset(symbol=base_name, asset_type='crypto')
                if quote_name != 'USD':
                    raise RuntimeError(f"Invalid quote: {quote_name}. We only support USD quotes.")
            else:
                base_asset = Asset(symbol=ticker, asset_type='stock')

            # Check if file is in the cache
            if not refresh_cache and os.path.exists(filepath):
                logger.info(f"Loading {ticker} data from cache.")
                try:
                    # Parse timestamp column and localize to the original timezone
                    df = pd.read_csv(filepath, parse_dates=['timestamp'])

                    # First convert strings to datetime with timezone
                    df["timestamp"] = pd.to_datetime(df["timestamp"])

                    # Then apply the timezone conversion
                    df["timestamp"] = df["timestamp"].apply(lambda ts: ts.tz_convert(self.tzinfo))

                except FileNotFoundError:
                    raise RuntimeError(f"No data found for {ticker}.")
            else:
                if '/' in ticker:
                    logger.info(
                        f"Fetching crypto data from for {ticker} "
                        f"from {start_dt.isoformat()} to {end_dt.isoformat()} with timestep {timestep}."
                    )
                    client = self._crypto_client
                    # noinspection PyArgumentList
                    request_params = CryptoBarsRequest(
                        symbol_or_symbols=ticker,
                        timeframe=alpaca_timeframe,
                        start=start_dt,
                        end=end_dt,
                    )
                else:
                    # all is dividend and split adjusted. 
                    adjustment = 'all' if auto_adjust else 'split'

                    logger.info(
                        f"Fetching stock data from for {ticker} "
                        f"from {start_dt.isoformat()} to {end_dt.isoformat()} with timestep {timestep} "
                        f"and adjustment {adjustment}."
                    )
                    client = self._stock_client
                    # noinspection PyArgumentList
                    request_params = StockBarsRequest(
                        symbol_or_symbols=ticker,
                        timeframe=alpaca_timeframe,
                        start=start_dt,
                        end=end_dt,
                        adjustment=adjustment,
                    )
                df = self._download_and_save_data(client, request_params, filepath, start_dt, end_dt)
                df.set_index('timestamp', inplace=True)

            new_data = Data(
                asset=base_asset,
                df=df,
                date_start=start_dt,
                date_end=end_dt,
                trading_hours_start=trading_hours_start,
                trading_hours_end=trading_hours_end,
                timestep=self._timestep,
                quote=quote_asset,
                tzinfo=tzinfo
            )
            data_list.append(new_data)

        return data_list

    # noinspection PyMethodMayBeStatic
    def _download_and_save_data(
            self,
            client,
            request_params,
            filepath,
            start_dt,
            end_dt
    ):
        try:
            if isinstance(request_params, CryptoBarsRequest):
                bars = client.get_crypto_bars(request_params)
            else:
                bars = client.get_stock_bars(request_params)
        except Exception as e:  # noqa
            raise RuntimeError(f"Failed to fetch data from {request_params}: {e}")

        df = bars.df.reset_index()
        if df.empty:
            raise RuntimeError(f"Failed to fetch data from {request_params}.")

        # Ensure 'timestamp' is a pandas timestamp objects
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        if df['timestamp'].dt.tz is None:
            df['timestamp'] = df['timestamp'].dt.tz_localize(timezone.utc)
        else:
            df['timestamp'] = df['timestamp'].dt.tz_convert(timezone.utc)
        df.drop(columns=['symbol'], inplace=True)

        # Only keep rows that are between start_dt (inclusive) and end_dt (exclusive)
        df = df.set_index('timestamp')
        df = df[(df.index >= start_dt) & (df.index < end_dt)]
        df.reset_index(inplace=True)

        df.to_csv(filepath, index=False)
        return df
    
    def load_data(self):
        """
        Loads the data, updates date index, and prepares the data for use by repairing
        times and filling missing data based on trading days and times.

        This method initializes the internal data store from the Pandas dataset and
        determines the timestep frequency. It calculates the trading days and trading
        times based on the market calendar, ensuring that the data is aligned and
        consistent by repairing and filling missing dates and times using the updated
        date index.

        Args:
            None

        Returns:
            list: A list of trading days as calculated by the `get_trading_days`
            function.

        Raises:
            None
        """
        self._data_store = self.pandas_data
        self._date_index = self.update_date_index()

        if len(self._data_store.values()) > 0:
            self._timestep = list(self._data_store.values())[0].timestep

        # # Add one minute back because get_trading_days end_date is exclusive and
        # # DataSourceBacktesting subtracted a minute from datetime_end in init.
        # end_date = self.datetime_end + timedelta(minutes=1)
        #
        # pcal = get_trading_days(
        #     market=self._market,
        #     start_date=self.datetime_start,
        #     end_date=end_date,
        #     tzinfo=self.tzinfo
        # )

        end_date = self.datetime_end + timedelta(days=1)

        pcal = get_trading_days(
            market=self._market,
            start_date=self._date_index[0],
            end_date=end_date,
            tzinfo=self.tzinfo
        )

        self._date_index = get_trading_times(
            pcal=pcal,
            timestep=self._timestep
        )
        for _, data in self._data_store.items():
            data.repair_times_and_fill(self._date_index)
        return pcal
