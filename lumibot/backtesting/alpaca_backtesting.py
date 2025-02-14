import logging
from datetime import datetime, timezone
from typing import List
import os

import pandas as pd
from alpaca.data.historical import CryptoHistoricalDataClient
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import CryptoBarsRequest, StockBarsRequest
from alpaca.data.timeframe import TimeFrame

from lumibot.data_sources import PandasData
from lumibot.entities import Data, Asset
from lumibot import LUMIBOT_CACHE_FOLDER

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
    else:
        raise ValueError(f"Unsupported timestep: {timestep}")


class AlpacaBacktesting(PandasData):

    def __init__(
            self,
            tickers: List[str] | str,
            start_date: str,
            end_date: str,
            timestep: str = 'day',
            refresh_cache: bool = False,
            adjustment: str = 'all',
            config: dict | None = None,
            tz_name: str = timezone.utc,
    ):
        self.CACHE_SUBFOLDER = 'alpaca'
        self.tz_name = tz_name

        self._crypto_client = CryptoHistoricalDataClient(
            api_key=config["API_KEY"],
            secret_key=config["API_SECRET"]
        )

        self._stock_client = StockHistoricalDataClient(
            api_key=config["API_KEY"],
            secret_key=config["API_SECRET"]
        )

        # Convert the date string to UTC midnight datetime.
        start_dt = pd.to_datetime(start_date).tz_localize(self.tz_name)
        end_dt = pd.to_datetime(end_date).tz_localize(self.tz_name)

        pandas_data = self._fetch_cache_and_load_data(
            tickers=tickers,
            start_dt=start_dt,
            end_dt=end_dt,
            timestep=timestep,
            refresh_cache=refresh_cache,
            adjustment=adjustment,
            tz_name=tz_name
        )

        super().__init__(datetime_start=start_dt, datetime_end=end_dt, pandas_data=pandas_data)

    def _fetch_cache_and_load_data(
            self,
            *,
            tickers: List[str] | str,
            start_dt: datetime,
            end_dt: datetime,
            timestep: str = '1d',
            refresh_cache: bool = False,
            adjustment: str = 'all',
            tz_name: str = None,
    ) -> List[Data]:
        """ Fetches, caches, and loads data for a list of tickers.

        Parameters:
            tickers: A list of tickers to fetch data for.
            start_dt: The start date to fetch data for (inclusive from midnight) in YYYY-MM-DD format.
            end_dt: The end date to fetch data for (exclusive) in YYYY-MM-DD format.
            timestep: The interval to fetch data for. Default is '1d' Options are 1d 60m, 1m.
            refresh_cache: Ignore current cache and fetch from source again. Default is False.
            adjustment: The adjustment to make to the data. Default is 'all'.
            tz_name : If not None, then localize the timezone of the dataframe to the 
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

        for ticker in tickers:
            cleaned_ticker = replace_slashes(ticker)
            filename = f"{cleaned_ticker}_{timestep}_{adjustment}_{start_dt.date().isoformat()}_{end_dt.date().isoformat()}.csv"
            filename = replace_slashes(filename).upper()
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
                    df = pd.read_csv(filepath, parse_dates=['timestamp'])
                    # Parse timestamp column and localize to the original timezone
                    df["timestamp"] = df["timestamp"].apply(lambda ts: ts.tz_convert(self.tz_name))
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

            new_data = Data(
                asset=base_asset,
                df=df,
                date_start=start_dt,
                date_end=end_dt,
                timestep=timestep,
                quote=quote_asset,
                timezone=tz_name
            )
            data_list.append(new_data)

        return data_list

    # noinspection PyMethodMayBeStatic
    def _download_and_save_data(
            self,
            client ,
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


