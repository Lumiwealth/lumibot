import logging
from datetime import datetime, timezone, time
from zoneinfo import ZoneInfo
from typing import List
import os

import pandas as pd
from alpaca.data.historical import CryptoHistoricalDataClient
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import CryptoBarsRequest, StockBarsRequest
from alpaca.data.timeframe import TimeFrame

from lumibot.data_sources import PandasData
from lumibot.entities import Data, Asset
from lumibot import LUMIBOT_CACHE_FOLDER, LUMIBOT_DEFAULT_TIMEZONE
from lumibot.tools.helpers import date_n_days_from_date

logger = logging.getLogger(__name__)


def replace_slashes(string: str) -> str:
    """Clean a string by removing any special characters."""
    return string.replace('/', '-')


def alpaca_timeframe_from_timestep(timestep: str) -> TimeFrame:
    """Convert a timestep string to an Alpaca TimeFrame."""
    if timestep == 'day':
        return TimeFrame.Day
    elif timestep == 'minute':
        return TimeFrame.Minute
    elif timestep == 'hour':
        return TimeFrame.Hour
    else:
        raise ValueError(f"Unsupported timestep: {timestep}")


class AlpacaBacktesting(PandasData):

    def __init__(
            self,
            tickers: List[str] | str,
            start_date: str,
            end_date: str,
            trading_hours_start=time(9, 30),
            trading_hours_end=time(15, 59),
            timestep: str = 'day',
            refresh_cache: bool = False,
            config: dict | None = None,
            tzinfo: ZoneInfo = ZoneInfo(LUMIBOT_DEFAULT_TIMEZONE),
            warm_up_trading_days: int = 0,
            market: str = "NYSE",
            auto_adjust: bool = True,
    ):
        """
        Initializes an instance for fetching and managing historical data from Alpaca,
        either for crypto or stock symbols, with specified date range and adjustments.
        Uses Alpaca's Crypto and Stock Historical Data clients to retrieve the data.
        Supports caching and time interval customization. Also supports warm-up bars
        for initializing strategies.

        Args:
            tickers (List[str] | str): List of ticker symbols or a single ticker symbol
                for the required data.
            start_date (str): The start date of the historical data range in string
                format. Must comply with the `pandas.to_datetime` format.
            end_date (str): The end date of the historical data range in string
                format. Must comply with the `pandas.to_datetime` format.
            trading_hours_start : datetime.time (inclusive) or None. Only applicable when timestep is 'minute'.
                If not supplied, then default is 09:30 (to align with the default LUMIBOT_TIMEZONE).
            trading_hours_end : datetime.time (inclusive) or None. Only applicable when timestep is 'minute'.
                If not supplied, then default is 15:59 hrs (to align with the default LUMIBOT_TIMEZONE).
            timestep (str): The time interval for the historical data (e.g., 'minute', 'hour' and
                'day'). Default is 'day'. When hour bars are used, the timestep will be set to minute.
            refresh_cache (bool): Whether to refresh the cached historical data or use
                existing cache. Default is False.
            auto_adjust (bool): Split and dividend adjusted if true, split adjusted only if false.
            config (dict | None): Configuration dictionary containing `API_KEY` and
                `API_SECRET` for authenticating with Alpaca APIs.
            tzinfo (ZoneInfo): The name of the timezone to localize datetime values.
                Default is `ZoneInfo(LUMIBOT_DEFAULT_TIMEZONE)`.
            warm_up_trading_days (int): The number of additional trading days to fetch before
                `start_date`, useful for warming up trading algorithms. Default is 0.
            market (str): The market to fetch data for. Default is 'NYSE'.
        """
        self.CACHE_SUBFOLDER = 'alpaca'
        self.tzinfo = tzinfo
        self._timestep = timestep

        self._crypto_client = CryptoHistoricalDataClient(
            api_key=config["API_KEY"],
            secret_key=config["API_SECRET"]
        )

        self._stock_client = StockHistoricalDataClient(
            api_key=config["API_KEY"],
            secret_key=config["API_SECRET"]
        )

        # Convert to midnight in the specified timezone
        start_dt = pd.to_datetime(start_date).replace(
            hour=0, minute=0, second=0, microsecond=0
        ).tz_localize(
            self.tzinfo)
        end_dt = pd.to_datetime(end_date).replace(
            hour=0, minute=0, second=0, microsecond=0
        ).tz_localize(self.tzinfo)

        if warm_up_trading_days > 0:
            warm_up_start_dt = date_n_days_from_date(
                n_bars=warm_up_trading_days,
                start_datetime=start_dt,
                market=market,
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
            timestep=timestep,
            refresh_cache=refresh_cache,
            auto_adjust=auto_adjust,
            tzinfo=tzinfo
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
            trading_hours_start: datetime,
            trading_hours_end: datetime,
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
                        timeframe=alpaca_timeframe_from_timestep(timestep),
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
                        timeframe=alpaca_timeframe_from_timestep(timestep),
                        start=start_dt,
                        end=end_dt,
                        adjustment=adjustment,
                    )
                df = self._download_and_save_data(client, request_params, filepath, start_dt, end_dt)
                df.set_index('timestamp', inplace=True)

            # If we were using hourly data, set the timestep to minute since the rest of lumibot only operates
            # with day or minute timestep.
            if self._timestep == 'hour':
                self._timestep = 'minute'

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
