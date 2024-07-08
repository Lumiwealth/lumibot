import logging
from datetime import datetime, timedelta, timezone

import pandas as pd
from alpaca.data.historical import CryptoHistoricalDataClient, StockHistoricalDataClient
from alpaca.data.requests import (
    CryptoBarsRequest,
    CryptoLatestQuoteRequest,
    CryptoLatestTradeRequest,
    StockBarsRequest,
    StockLatestTradeRequest,
)
from alpaca.data.timeframe import TimeFrame

from lumibot.entities import Asset, Bars

from .data_source import DataSource


class AlpacaData(DataSource):
    SOURCE = "ALPACA"
    MIN_TIMESTEP = "minute"
    TIMESTEP_MAPPING = [
        {
            "timestep": "minute",
            "representations": [TimeFrame.Minute, "minute"],
        },
        {
            "timestep": "5 minutes",
            "representations": [
                [f"5{TimeFrame.Minute}", "minute"],
            ],
        },
        {
            "timestep": "10 minutes",
            "representations": [
                [f"10{TimeFrame.Minute}", "minute"],
            ],
        },
        {
            "timestep": "15 minutes",
            "representations": [
                [f"15{TimeFrame.Minute}", "minute"],
            ],
        },
        {
            "timestep": "30 minutes",
            "representations": [
                [f"30{TimeFrame.Minute}", "minute"],
            ],
        },
        {
            "timestep": "1 hour",
            "representations": [
                [f"{TimeFrame.Hour}", "hour"],
            ],
        },
        {
            "timestep": "2 hours",
            "representations": [
                [f"2{TimeFrame.Hour}", "hour"],
            ],
        },
        {
            "timestep": "4 hours",
            "representations": [
                [f"4{TimeFrame.Hour}", "hour"],
            ],
        },
        {
            "timestep": "day",
            "representations": [TimeFrame.Day, "day"],
        },
    ]

    """Common base class for data_sources/alpaca and brokers/alpaca"""

    @staticmethod
    def _format_datetime(dt):
        return pd.Timestamp(dt).isoformat()

    def __init__(self, config, max_workers=20, chunk_size=100):
        super().__init__()
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

        if isinstance(config, dict) and "API_KEY" in config:
            self.api_key = config["API_KEY"]
        elif hasattr(config, "API_KEY"):
            self.api_key = config.API_KEY
        else:
            raise ValueError("API_KEY not found in config")

        if isinstance(config, dict) and "API_SECRET" in config:
            self.api_secret = config["API_SECRET"]
        elif hasattr(config, "API_SECRET"):
            self.api_secret = config.API_SECRET
        else:
            raise ValueError("API_SECRET not found in config")

        # If an ENDPOINT is provided, warn the user that it is not used anymore
        # Instead they should use the "PAPER" parameter, which is boolean
        if isinstance(config, dict) and "ENDPOINT" in config:
            logging.warning(
                """The ENDPOINT parameter is not used anymore for AlpacaData, please use the PAPER parameter instead.
                The 'PAPER' parameter is boolean, and defaults to True.
                The ENDPOINT parameter will be removed in a future version of lumibot."""
            )

        # Get the PAPER parameter, which defaults to True
        if isinstance(config, dict) and "PAPER" in config:
            self.is_paper = config["PAPER"]
        elif hasattr(config, "PAPER"):
            self.is_paper = config.PAPER
        else:
            self.is_paper = True

        if isinstance(config, dict) and "VERSION" in config:
            self.version = config["VERSION"]
        elif hasattr(config, "VERSION"):
            self.version = config.VERSION
        else:
            self.version = "v2"

    def get_chains(self, asset: Asset, quote=None, exchange: str = None):
        """
        Alpaca doesn't support option trading. This method is here to comply with the DataSource interface
        """
        raise NotImplementedError(
            "Lumibot AlpacaData does not support get_chains() options data. If you need this "
            "feature, please use a different data source."
        )

    def get_last_price(self, asset, quote=None, exchange=None, **kwargs):
        if quote is not None:
            # If the quote is not None, we use it even if the asset is a tuple
            if type(asset) == Asset and asset.asset_type == "stock":
                symbol = asset.symbol
            elif isinstance(asset, tuple):
                symbol = f"{asset[0].symbol}/{quote.symbol}"
            else:
                symbol = f"{asset.symbol}/{quote.symbol}"
        elif isinstance(asset, tuple):
            symbol = f"{asset[0].symbol}/{asset[1].symbol}"
        else:
            symbol = asset.symbol

        if isinstance(asset, tuple) and asset[0].asset_type == "crypto":
            client = CryptoHistoricalDataClient()
            quote_params = CryptoLatestQuoteRequest(symbol_or_symbols=symbol)
            quote = client.get_crypto_latest_quote(quote_params)

            # Get the first item in the dictionary
            quote = quote[list(quote.keys())[0]]

            # The price is the average of the bid and ask
            price = (quote.bid_price + quote.ask_price) / 2

        elif isinstance(asset, Asset) and asset.asset_type == "crypto":
            client = CryptoHistoricalDataClient()
            quote_params = CryptoLatestQuoteRequest(symbol_or_symbols=symbol)
            quote = client.get_crypto_latest_quote(quote_params)

            # Get the first item in the dictionary
            quote = quote[list(quote.keys())[0]]

            # The price is the average of the bid and ask
            price = (quote.bid_price + quote.ask_price) / 2
        else:
            # Stocks
            client = StockHistoricalDataClient(self.api_key, self.api_secret)
            params = StockLatestTradeRequest(symbol_or_symbols=symbol)
            trade = client.get_stock_latest_trade(params)[symbol]
            price = trade.price

        return price

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

    def get_barset_from_api(self, asset, freq, limit=None, end=None, start=None, quote=None):
        """
        gets historical bar data for the given stock symbol
        and time params.

        outputs a dataframe open, high, low, close columns and
        a UTC timezone aware index.
        """
        if isinstance(asset, tuple):
            if quote is None:
                quote = asset[1]
            asset = asset[0]

        if not limit:
            limit = 1000

        if not end:
            # Alpaca limitation of not getting the most recent 15 minutes
            # TODO: This is only needed if you dont have a paid alpaca subscription
            end = datetime.now(timezone.utc) - timedelta(minutes=15)

        if not start:
            if str(freq) == "1Min":
                if datetime.now().weekday() == 0:  # for Mondays as prior days were off
                    loop_limit = (
                        limit + 4896
                    )  # subtract 4896 minutes to take it from Monday to Friday, as there is no data between Friday 4:00 pm and Monday 9:30 pm causing an incomplete or empty dataframe
                else:
                    loop_limit = limit

            elif str(freq) == "1Day":
                loop_limit = limit * 1.5  # number almost perfect for normal weeks where only weekends are off

                # Add 3 days to the start date to make sure we get enough data on extra long weekends (like Thanksgiving)
                loop_limit += 3

        df = []  # to use len(df) below without an error

        # arbitrary limit of upto 4 calls after which it will give up
        while loop_limit / limit <= 64 and len(df) < limit:
            if str(freq) == "1Min":
                start = end - timedelta(minutes=loop_limit)

            elif str(freq) == "1Day":
                start = end - timedelta(days=loop_limit)

            if asset.asset_type == "crypto":
                symbol = f"{asset.symbol}/{quote.symbol}"

                client = CryptoHistoricalDataClient()
                params = CryptoBarsRequest(symbol_or_symbols=symbol, timeframe=freq, start=start, end=end)
                barset = client.get_crypto_bars(params)

            else:
                symbol = asset.symbol

                client = StockHistoricalDataClient(self.api_key, self.api_secret)
                params = StockBarsRequest(symbol_or_symbols=symbol, timeframe=freq, start=start, end=end)

                try:
                    barset = client.get_stock_bars(params)
                except Exception as e:
                    logging.error(f"Could not get pricing data from Alpaca for {symbol} with the following error: {e}")
                    return None

            df = barset.df

            # Alpaca now returns a dataframe with a MultiIndex. We only want an index of timestamps
            df = df.reset_index(level=0, drop=True)

            if df.empty:
                logging.error(f"Could not get any pricing data from Alpaca for {symbol}, the DataFrame came back empty")
                return None

            df = df[~df.index.duplicated(keep="first")]
            df = df.iloc[-limit:]
            df = df[df.close > 0]
            loop_limit *= 2

        if len(df) < limit:
            logging.warning(
                f"Dataframe for {symbol} has {len(df)} rows while {limit} were requested. Further data does not exist for Alpaca"
            )

        return df

    def _pull_source_bars(
        self, assets, length, timestep=MIN_TIMESTEP, timeshift=None, quote=None, include_after_hours=True
    ):
        """pull broker bars for a list assets"""
        if timeshift is None and timestep == "day":
            # Alpaca throws an error if we don't do this and don't have a data subscription because
            # they require a subscription for historical data less than 15 minutes old
            timeshift = timedelta(minutes=16)

        parsed_timestep = self._parse_source_timestep(timestep, reverse=True)
        kwargs = dict(limit=length)
        if timeshift:
            end = datetime.now() - timeshift
            end = self.to_default_timezone(end)
            kwargs["end"] = end

        result = {}
        for asset in assets:
            data = self.get_barset_from_api(asset, parsed_timestep, quote=quote, **kwargs)
            result[asset] = data

        return result

    def _pull_source_symbol_bars(
        self, asset, length, timestep=MIN_TIMESTEP, timeshift=None, quote=None, exchange=None, include_after_hours=True
    ):
        if exchange is not None:
            logging.warning(
                f"the exchange parameter is not implemented for AlpacaData, but {exchange} was passed as the exchange"
            )

        """pull broker bars for a given asset"""
        response = self._pull_source_bars([asset], length, timestep=timestep, timeshift=timeshift, quote=quote)
        return response[asset]

    def _parse_source_symbol_bars(self, response, asset, quote=None, length=None):
        # TODO: Alpaca return should also include dividend yield
        response["return"] = response["close"].pct_change()
        bars = Bars(response, self.SOURCE, asset, raw=response, quote=quote)
        return bars
