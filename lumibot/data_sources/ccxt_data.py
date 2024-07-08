import datetime
import logging
import time

import ccxt
import pandas as pd

from lumibot.entities import Asset, Bars

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

    def __init__(self, config, max_workers=20, chunk_size=100, **kwargs):
        super().__init__(**kwargs)
        self.name = "ccxt"
        self.max_workers = min(max_workers, 200)

        # When requesting data for assets for example,
        # if there is too many assets, the best thing to do would
        # be to split it into chunks and request data for each chunk
        self.chunk_size = min(chunk_size, 100)

        try:
            exchange_class = getattr(ccxt, config["exchange_id"])
        except:
            raise Exception(
                "Could not find exchange named '{}'. Are you sure you are spelling the exchange_id correctly?".format(
                    config["exchange_id"]
                )
            )

        self.config = config
        self.api = exchange_class(config)
        is_sandbox = True if "sandbox" not in config else config["sandbox"]
        self.api.set_sandbox_mode(is_sandbox)
        self.api.load_markets()
        # Recommended two or less api calls per second.
        self.api.enableRateLimit = True

    def _pull_source_symbol_bars(
        self, asset, length, timestep=MIN_TIMESTEP, timeshift=None, quote=None, exchange=None, include_after_hours=True
    ):
        if exchange is not None:
            logging.warning(
                f"the exchange parameter is not implemented for CcxtData, but {exchange} was passed as the exchange"
            )

        """pull broker bars for a given asset"""
        response = self._pull_source_bars([asset], length, timestep=timestep, timeshift=timeshift, quote=quote)
        return response[asset]

    def _pull_source_bars(
        self, assets, length, timestep=MIN_TIMESTEP, timeshift=None, quote=None, include_after_hours=True
    ):
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
            elif quote is not None:
                symbol = f"{asset.symbol.upper()}/{quote.symbol.upper()}"
            else:
                symbol = asset
            data = self.get_barset_from_api(self.api, symbol, parsed_timestep, **kwargs)
            result[asset] = data

        return result

    def get_chains(self, asset: Asset, quote: Asset = None, exchange: str = None):
        raise NotImplementedError(
            "Lumibot CcxtData does not support historical options data. If you need this "
            "feature, please use a different data source."
        )

    def get_historical_prices(
        self, asset, length, timestep="", timeshift=None, quote=None, exchange=None, include_after_hours=True
    ):
        """Get bars for a given asset"""
        if isinstance(asset, str):
            asset = Asset(symbol=asset)

        if not timestep:
            timestep = self.get_timestep()

        response = self._pull_source_symbol_bars(
            asset,
            length,
            timestep=timestep,
            timeshift=timeshift,
            quote=quote,
            exchange=exchange,
            include_after_hours=include_after_hours,
        )
        if isinstance(response, float):
            return response
        elif response is None:
            return None

        bars = self._parse_source_symbol_bars(response, asset, quote=quote, length=length)
        return bars

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
                f"A request for market data for {symbol} was submitted. " f"The market for that pair does not exist"
            )
            return None

        if limit is None:
            limit = 300

        if end is None:
            end = datetime.datetime.utcnow()

        endunix = self.api.parse8601(end.strftime("%Y-%m-%d %H:%M:%S"))
        buffer = 10  # A few extra datapoints in the download then trim the df.
        if freq == "1m":
            start = end - datetime.timedelta(minutes=limit + buffer)
        else:
            start = end - datetime.timedelta(days=limit + buffer)
        df_ret = None
        curr_start = self.api.parse8601(start.strftime("%Y-%m-%d %H:%M:%S"))
        cnt = 0
        last_curr_end = None
        # loop_limit = 300 if limit > 300 else limit
        loop_limit = 300
        rate_limit = 10  # Requests per second in burst.

        while True:
            cnt += 1
            candles = self.api.fetch_ohlcv(symbol, freq, since=curr_start, limit=loop_limit, params={})

            df = pd.DataFrame(candles, columns=["datetime", "open", "high", "low", "close", "volume"])
            df["datetime"] = pd.to_datetime(df["datetime"], unit="ms")
            df = df.set_index("datetime")

            if df_ret is None:
                df_ret = df
            else:
                df_ret = pd.concat([df_ret, df])

            df_ret = df_ret.sort_index()

            if len(df) > 0:
                last_curr_end = self.api.parse8601(df.index[-1].strftime("%Y-%m-%d %H:%M:%S"))
            else:
                last_curr_end = None

            if len(df_ret) >= limit:
                break
            elif last_curr_end is None:
                break
            elif last_curr_end > endunix:
                break

            if curr_start == last_curr_end:
                break
            else:
                curr_start = last_curr_end

            # Sleep for half a second every rate_limit requests to prevent rate limiting issues
            if cnt % rate_limit == 0:
                time.sleep(1)

            # Catch if endless loop.
            if cnt > 500:
                break

        df_ret = df_ret[~df_ret.index.duplicated(keep="first")]
        df_ret = df_ret.loc[:end]
        df_ret = df_ret.iloc[-limit:]

        return df_ret

    def _parse_source_symbol_bars(self, response, asset, quote=None, length=None):
        # Parse the dataframe returned from CCXT.
        response["return"] = response["close"].pct_change()
        bars = Bars(response, self.SOURCE, asset, quote=quote, raw=response)
        return bars

    def get_last_price(self, asset, quote=None, exchange=None, **kwargs):
        if quote is not None:
            symbol = f"{asset.symbol}/{quote.symbol}"
        else:
            symbol = asset.symbol

        ticker = self.api.fetch_ticker(symbol)
        price = ticker["last"]

        return price