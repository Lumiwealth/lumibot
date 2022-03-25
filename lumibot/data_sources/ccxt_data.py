import logging
import time
import datetime

import ccxt
# from credentials import CcxtConfig
import pandas as pd

from lumibot.entities import Bars

from .data_source import DataSource


class CcxtData(DataSource):
    SOURCE = "CCXT"
    MIN_TIMESTEP = "minute"
    TIMESTEP_MAPPING = [
        {"timestep": "minute", "representations": ["1m"]},
        {"timestep": "day", "representations": ["1d"]},
    ]
    IS_BACKTESTING_DATA_SOURCE = False

    """Common base class for data_sources/ccxt and brokers/ccxt"""

    @staticmethod
    def _format_datetime(dt):
        return pd.Timestamp(dt).isoformat()

    def __init__(self, api_keys, max_workers=20, chunk_size=100, **kwargs):
        self.name = "ccxt"
        self.max_workers = min(max_workers, 200)

        # When requesting data for assets for example,
        # if there is too many assets, the best thing to do would
        # be to split it into chunks and request data for each chunk
        self.chunk_size = min(chunk_size, 100)

        exchange_class = getattr(
            ccxt, api_keys["exchange_id"]
        )

        self.api = exchange_class(api_keys)
        self.api.set_sandbox_mode(True if 'sandbox' not in api_keys else api_keys['sandbox'])
        self.api.load_markets()
        # Recommended two or less api calls per second.
        self.api.enableRateLimit = True

    def _pull_source_symbol_bars(
        self, asset, length, timestep=MIN_TIMESTEP, timeshift=None
    ):
        """pull broker bars for a given asset"""
        response = self._pull_source_bars(
            [asset], length, timestep=timestep, timeshift=timeshift
        )
        return response[asset]

    def _pull_source_bars(self, assets, length, timestep=MIN_TIMESTEP, timeshift=None):
        """pull broker bars for a list assets"""
        parsed_timestep = self._parse_source_timestep(timestep, reverse=True)
        kwargs = dict(limit=length)
        if timeshift:
            end = datetime.datetime.now() - timeshift
            kwargs["end"] = self.to_default_timezone(end)

        result = {}
        for asset in assets:
            if isinstance(asset, tuple):
                symbol = f"{asset[0].symbol.upper()}/{asset[1].symbol.upper()}"
            else:
                symbol = asset
            data = self.get_barset_from_api(self.api, symbol, parsed_timestep, **kwargs)
            result[asset] = data

        return result


    def get_barset_from_api(self, api, symbol, freq, limit=None, end=None):
        """
        gets historical bar data for the given stock symbol
        and time params.

        outputs a dataframe open, high, low, close columns and
        a UTC timezone aware index.
        """
        if not api.has["fetchOHLCV"]:
            logging.error("Exchange does not support fetching OHLCV data")

        market = self.api.markets.get(symbol, None)
        if market is None:
            logging.error(
                f"A request for market data for {symbol} was submitted. "
                f"The market for that pair does not exist"
            )
            return None

        if limit is None:
            limit = 300

        if end is None:
            end = datetime.datetime.utcnow()

        endunix = self.api.parse8601(end.strftime("%Y-%m-%d %H:%M:%S"))
        buffer = 10  # A few extra datapoints in the download then trim the df.
        if freq == "1m":
            start = end - datetime.timedelta(minutes=limit+buffer)
        else:
            start = end - datetime.timedelta(days=limit+buffer)
        df_ret = None
        curr_start = self.api.parse8601(start.strftime("%Y-%m-%d %H:%M:%S"))
        cnt = 0
        last_curr_end = None
        # loop_limit = 300 if limit > 300 else limit
        loop_limit = 300
        rate_limit = 10  # 10 requests per second in burst.

        while True:
            cnt += 1
            candles = self.api.fetch_ohlcv(
                symbol, freq, since=curr_start, limit=loop_limit, params={}
            )

            df = pd.DataFrame(
                candles, columns=["datetime", "open", "high", "low", "close", "volume"]
            )
            df["datetime"] = pd.to_datetime(df["datetime"], unit="ms")
            df = df.set_index("datetime")

            if df_ret is None:
                df_ret = df
            else:
                df_ret = pd.concat([df_ret, df])

            df_ret = df_ret.sort_index()


            last_curr_end = self.api.parse8601(df.index[-1].strftime("%Y-%m-%d %H:%M:%S"))
            if len(df_ret) >= limit:
                break
            elif last_curr_end > endunix:
                break

            curr_start = last_curr_end
            if cnt % 10 == 0:
                time.sleep(.5)

            # Catch if endless loop.
            if cnt > 500:
                break


        df_ret = df_ret[~df_ret.index.duplicated(keep="first")]
        df_ret = df_ret.loc[:end]
        df_ret = df_ret.iloc[-limit:]

        return df_ret


    def _parse_source_symbol_bars(self, response, asset):
        # Parse the dataframe returned from CCXT.
        response["return"] = response["close"].pct_change()
        bars = Bars(response, self.SOURCE, asset[0], quote=asset[1], raw=response)
        return bars
