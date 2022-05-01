import logging
import re
import time
from datetime import datetime, timezone

import alpaca_trade_api as tradeapi
import pandas as pd
from alpaca_trade_api.common import URL
from alpaca_trade_api.entity import Bar
from alpaca_trade_api.rest import TimeFrame, TimeFrameUnit

from lumibot.entities import Bars

from .data_source import DataSource


class AlpacaData(DataSource):
    SOURCE = "ALPACA"
    MIN_TIMESTEP = "minute"
    TIMESTEP_MAPPING = [
        {
            "timestep": "minute",
            "representations": [TimeFrame(1, TimeFrameUnit.Minute), "minute"],
        },
        {
            "timestep": "day",
            "representations": [TimeFrame(1, TimeFrameUnit.Day), "day"],
        },
    ]

    """Common base class for data_sources/alpaca and brokers/alpaca"""

    @staticmethod
    def _format_datetime(dt):
        return pd.Timestamp(dt).isoformat()

    def __init__(self, config, max_workers=20, chunk_size=100, **kwargs):
        # Alpaca authorize 200 requests per minute and per API key
        # Setting the max_workers for multithreading with a maximum
        # of 200
        self.name = "alpaca"
        self.max_workers = min(max_workers, 200)

        # When requesting data for assets for example,
        # if there is too many assets, the best thing to do would
        # be to split it into chunks and request data for each chunk
        self.chunk_size = min(chunk_size, 100)

        # Connection to alpaca REST API
        self.config = config
        self.api_key = config.API_KEY
        self.api_secret = config.API_SECRET
        if hasattr(config, "ENDPOINT"):
            self.endpoint = URL(config.ENDPOINT)
        else:
            self.endpoint = URL("https://paper-api.alpaca.markets")
        if hasattr(config, "VERSION"):
            self.version = config.VERSION
        else:
            self.version = "v2"
        self.api = tradeapi.REST(
            self.api_key, self.api_secret, self.endpoint, self.version
        )

    def _pull_source_symbol_bars(
        self, asset, length, timestep=MIN_TIMESTEP, timeshift=None
    ):
        """pull broker bars for a given asset"""
        response = self._pull_source_bars(
            [asset], length, timestep=timestep, timeshift=timeshift
        )
        return response[asset]

    def get_barset_from_api(self, api, asset, freq, limit=None, end=None):
        """
        gets historical bar data for the given stock symbol
        and time params.

        outputs a dataframe open, high, low, close columns and
        a UTC timezone aware index.
        """
        if limit is None:
            limit = 1000

        if end is None:
            end = datetime.now(timezone.utc)

        df_ret = None

        curr_end = end.isoformat() if not isinstance(end, str) else end
        cnt = 0
        last_curr_end = None
        loop_limit = 1000 if limit > 1000 else limit
        while True:
            cnt += 1
            # freqnum = re.search(r'\d+', freq).group()
            # freqtimelen = freq[len(freqnum):]
            if isinstance(asset, tuple):
                symbol = f"{asset[0].symbol}{asset[1].symbol}"
                barset = api.get_crypto_bars(
                    symbol, freq, limit=loop_limit, end=curr_end
                )
            else:
                symbol = asset.symbol
                barset = api.get_bars(symbol, freq, limit=loop_limit, end=curr_end)
            df = barset.df

            if df.empty:
                logging.error(
                    f"Could not get any pricing data from Alpaca for {symbol}, the DataFrame came back empty"
                )
                return None

            if df_ret is None:
                df_ret = df
            elif str(df.index[0]) < str(df_ret.index[0]):
                df_ret = df.append(df_ret)

            if len(df_ret) >= limit:
                break
            else:
                curr_end = (
                    datetime.fromisoformat(str(df_ret.index[0])).strftime(
                        "%Y-%m-%dT%H:%M:%S"
                    )
                    + "-04:00"
                )

            # Sometimes the beginning date we put in is not a trading date,
            # this makes sure that we end when we're close enough
            # (it's just returning the same thing over and over)
            if curr_end == last_curr_end:
                break
            else:
                last_curr_end = curr_end

            # Sleep so that we don't trigger rate limiting
            if cnt >= 50:
                time.sleep(10)
                cnt = 0

        df_ret = df_ret[~df_ret.index.duplicated(keep="first")]
        df_ret = df_ret.iloc[-limit:]

        return df_ret[df_ret.close > 0]

    def _pull_source_bars(self, assets, length, timestep=MIN_TIMESTEP, timeshift=None):
        """pull broker bars for a list assets"""
        parsed_timestep = self._parse_source_timestep(timestep, reverse=True)
        kwargs = dict(limit=length)
        if timeshift:
            end = datetime.now() - timeshift
            end = self.to_default_timezone(end)
            kwargs["end"] = self._format_datetime(end)

        result = {}
        for asset in assets:
            data = self.get_barset_from_api(self.api, asset, parsed_timestep, **kwargs)
            result[asset] = data

        return result

    def _parse_source_symbol_bars(self, response, asset):
        # TODO: Alpaca return should also include dividend yield
        response["return"] = response["close"].pct_change()
        bars = Bars(response, self.SOURCE, asset, raw=response)
        return bars
