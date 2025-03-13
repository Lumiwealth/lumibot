import logging
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Union
from zoneinfo import ZoneInfo

import pandas as pd
import re
from alpaca.data.historical import (
    CryptoHistoricalDataClient,
    StockHistoricalDataClient,
    OptionHistoricalDataClient
)
from alpaca.data.requests import (
    CryptoBarsRequest,
    CryptoLatestQuoteRequest,
    StockBarsRequest,
    StockLatestTradeRequest,
    OptionLatestTradeRequest,
    OptionBarsRequest,
    StockLatestQuoteRequest
)
from alpaca.data.timeframe import TimeFrame

from lumibot.entities import Asset, Bars
from lumibot import (
    LUMIBOT_DEFAULT_TIMEZONE,
    LUMIBOT_DEFAULT_QUOTE_ASSET_SYMBOL,
    LUMIBOT_DEFAULT_QUOTE_ASSET_TYPE
)

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
            "timestep": "hour",
            "representations": [
                [f"{TimeFrame.Hour}", "hour"],
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
    LUMIBOT_DEFAULT_QUOTE_ASSET = Asset(LUMIBOT_DEFAULT_QUOTE_ASSET_SYMBOL, LUMIBOT_DEFAULT_QUOTE_ASSET_TYPE)
    ALPACA_STOCK_PRECISION = Decimal('0.0001')
    ALPACA_CRYPTO_PRECISION = Decimal('0.000000001')

    """Common base class for data_sources/alpaca and brokers/alpaca"""

    @staticmethod
    def _format_datetime(dt):
        return pd.Timestamp(dt).isoformat()

    def _get_stock_client(self):
        """Lazily initialize and return the stock client."""
        if self._stock_client is None:
            self._stock_client = StockHistoricalDataClient(self.api_key, self.api_secret)
        return self._stock_client

    def _get_crypto_client(self):
        """Lazily initialize and return the crypto client."""
        if self._crypto_client is None:
            self._crypto_client = CryptoHistoricalDataClient(self.api_key, self.api_secret)
        return self._crypto_client

    def _get_option_client(self):
        """Lazily initialize and return the option client."""
        if self._option_client is None:
            self._option_client = OptionHistoricalDataClient(self.api_key, self.api_secret)
        return self._option_client

    def __init__(
            self,
            config,
            max_workers=20,
            chunk_size=100,

            # A delay parameter to control how many minutes to delay non-crypto data for.
            # Alpaca limits you to 15-min delayed non-crypto data unless you're on a paid data plan.
            # Set the delay to 0 if you are on a paid plan.
            delay=16,

            # Setting this causes all calls to historical data endpoints to request data in this timezone
            # and datetimes in dataframes are adjusted to this timezone. Useful if you want UTC time for
            # crypto for example.
            tzinfo=ZoneInfo(LUMIBOT_DEFAULT_TIMEZONE)
    ):

        super().__init__(delay=delay)
        self._tzinfo = tzinfo

        self.name = "alpaca"
        self.max_workers = min(max_workers, 200)

        # When requesting data for assets for example,
        # if there is too many assets, the best thing to do would
        # be to split it into chunks and request data for each chunk
        self.chunk_size = min(chunk_size, 100)

        # Connection to alpaca REST API
        self.config = config

        # Initialize these to none so they will be lazily created and kept around
        # for better performance.
        self._stock_client = self._crypto_client = self._option_client = None

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

    def get_last_price(self, asset, quote=None, exchange=None, **kwargs) -> Union[float, Decimal, None]:
        if quote is not None:
            # If the quote is not None, we use it even if the asset is a tuple
            if isinstance(asset, Asset) and asset.asset_type == Asset.AssetType.STOCK:
                symbol = asset.symbol
            elif isinstance(asset, tuple):
                symbol = f"{asset[0].symbol}/{quote.symbol}"
            else:
                symbol = f"{asset.symbol}/{quote.symbol}"
        elif isinstance(asset, Asset) and asset.asset_type == Asset.AssetType.OPTION:
            strike_formatted = f"{asset.strike:08.3f}".replace('.', '').rjust(8, '0')
            date = asset.expiration.strftime("%y%m%d")
            symbol = f"{asset.symbol}{date}{asset.right[0]}{strike_formatted}"
        elif isinstance(asset, tuple):
            symbol = f"{asset[0].symbol}/{asset[1].symbol}"
        elif isinstance(asset, str):
            symbol = asset
        else:
            symbol = asset.symbol

        if (isinstance(asset, tuple) and asset[0].asset_type == Asset.AssetType.CRYPTO) or (
                isinstance(asset, Asset) and asset.asset_type == Asset.AssetType.CRYPTO):
            client = self._get_crypto_client()
            quote_params = CryptoLatestQuoteRequest(symbol_or_symbols=symbol)
            latest_quote = client.get_crypto_latest_quote(quote_params)

            # Get the first item in the dictionary
            latest_quote = latest_quote[list(latest_quote.keys())[0]]

            # The price is the average of the bid and ask
            price = (latest_quote.bid_price + latest_quote.ask_price) / 2
        elif (isinstance(asset, tuple) and asset[0].asset_type == Asset.AssetType.OPTION) or (
                isinstance(asset, Asset) and asset.asset_type == Asset.AssetType.OPTION):
            logging.info(f"Getting {asset} option price")
            client = self._get_option_client()
            params = OptionLatestTradeRequest(symbol_or_symbols=symbol)
            trade = client.get_option_latest_trade(params)
            print(f'This {trade} {symbol}')
            price = trade[symbol].price
        else:
            # Stocks
            client = self._get_stock_client()
            params = StockLatestQuoteRequest(symbol_or_symbols=symbol)
            latest_quote = client.get_stock_latest_quote(params)[symbol]

            # Get bid and ask prices from the quote
            bid_price = latest_quote.bid_price
            ask_price = latest_quote.ask_price

            # You can use either bid, ask, or their average as your price
            price = (bid_price + ask_price) / 2  # Using midpoint price

        return price

    def get_historical_prices(
            self, asset, length, timestep="", timeshift=None, quote=None, exchange=None, include_after_hours=True
    ):
        """Get bars for a given asset"""
        if isinstance(asset, str):
            # Check if the string matches an option contract
            pattern = r'^[A-Z]{1,5}\d{6,7}[CP]\d{8}$'
            if re.match(pattern, asset) is not None:
                asset = Asset(symbol=asset, asset_type=Asset.AssetType.OPTION)
            else:
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
        a timezone aware index.
        """
        if isinstance(asset, tuple):
            if quote is None:
                quote = asset[1]
            asset = asset[0]

        if not limit:
             limit = 1000
        loop_limit = 0

        if not end and asset.asset_type != Asset.AssetType.CRYPTO:
            # Alpaca limitation of not getting the most recent 15 minutes
            end = datetime.now(self._tzinfo) - self._delay

        if not start:
            if asset.asset_type == Asset.AssetType.CRYPTO:
                # Crypto trades 24/7, no adjustments needed
                loop_limit = limit
            else:
                # Traditional assets (stocks, etc.)
                if str(freq) == "1Min":
                    if datetime.now().weekday() == 0:  # Monday
                        # Add 4896 minutes to account for:
                        # - Saturday (1440 minutes)
                        # - Sunday (1440 minutes)
                        # - Gap between Friday 4:00 PM to Monday 9:30 AM (390 minutes)
                        loop_limit = limit + 4896
                    else:
                        # Regular trading days
                        loop_limit = limit

                elif str(freq) == "1Day":
                    # Calculate padding for weekends and holidays
                    weeks_requested = limit // 5  # Full trading week is 5 days
                    extra_padding_days = weeks_requested * 3  # Account for weekends and holidays
                    loop_limit = max(5, limit + extra_padding_days)  # Get at least 5 days
                else:
                    # For any other frequency
                    loop_limit = limit

        df = []  # to use len(df) below without an error

        # arbitrary limit of upto 4 calls after which it will give up
        while loop_limit / limit <= 64 and len(df) < limit:
            if str(freq) == "1Min":
                start = end - timedelta(minutes=loop_limit)

            elif str(freq) == "1Day":
                start = end - timedelta(days=loop_limit)

            if asset.asset_type == Asset.AssetType.CRYPTO:
                symbol = f"{asset.symbol}/{quote.symbol}"
                client = self._get_crypto_client()
                params = CryptoBarsRequest(symbol_or_symbols=symbol, timeframe=freq, start=start, end=end)
                try:
                    barset = client.get_crypto_bars(params)
                except Exception as e:
                    logging.error(f"Could not get crypto pricing data from Alpaca for {symbol} with the following error: {e}")
                    return None

            elif asset.asset_type == Asset.AssetType.OPTION:
                strike_formatted = f"{asset.strike:08.3f}".replace('.', '').rjust(8, '0')
                date = asset.expiration.strftime("%y%m%d")
                symbol = f"{asset.symbol}{date}{asset.right[0]}{strike_formatted}"
                client = self._get_option_client()
                params = OptionBarsRequest(symbol_or_symbols=symbol, timeframe=freq, start=start, end=end)
                try:
                    barset = client.get_option_bars(params)
                except Exception as e:
                    logging.error(
                        f"Could not get option pricing data from Alpaca for {symbol} with the following error: {e}")
                    return None
            else:
                symbol = asset.symbol
                client = self._get_stock_client()
                params = StockBarsRequest(symbol_or_symbols=symbol, timeframe=freq, start=start, end=end)
                try:
                    barset = client.get_stock_bars(params)
                except Exception as e:
                    logging.error(f"Could not get pricing data from Alpaca for {symbol} with the following error: {e}")
                    return None

            df = barset.df

            # Alpaca now returns a dataframe with a MultiIndex. We only want an index of timestamps
            df = df.reset_index(level=0, drop=True)

            # Adjust the timezone of the data to whatever the user wanted.
            if hasattr(df.index, 'tz') and df.index.tz.__class__.__name__ == 'TzInfo':
                df.index = df.index.tz_convert(self._tzinfo)
            elif df.index.tz is None:
                df.index = df.index.tz_localize(self._tzinfo)

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
        result = {}
        for asset in assets:
            if timeshift or asset.asset_type == Asset.AssetType.CRYPTO:
                # Crypto asset prices are not delayed.
                asset_timeshift = timeshift
            else:
                # Alpaca throws an error if we don't do this and don't have a data subscription because
                # they require a subscription for historical data less than 15 minutes old
                asset_timeshift = self._delay

            parsed_timestep = self._parse_source_timestep(timestep, reverse=True)
            kwargs = dict(limit=length)
            if asset_timeshift:
                end = datetime.now() - asset_timeshift
                end = end.astimezone(self._tzinfo)
                kwargs["end"] = end

            data = self.get_barset_from_api(asset, parsed_timestep, quote=quote, **kwargs)
            result[asset] = data

        return result

    def _pull_source_symbol_bars(
            self, asset, length, timestep=MIN_TIMESTEP, timeshift=None, quote=None, exchange=None,
            include_after_hours=True
    ):
        if exchange is not None:
            logging.warning(
                f"the exchange parameter is not implemented for AlpacaData, but {exchange} was passed as the exchange"
            )

        """pull broker bars for a given asset"""
        response = self._pull_source_bars([asset], length, timestep=timestep, timeshift=timeshift, quote=quote)
        return response[asset]

    def _parse_source_symbol_bars(self, response, asset, quote=None, length=None):
        # TODO: Alpaca return should also include dividends
        bars = Bars(response, self.SOURCE, asset, raw=response, quote=quote)
        return bars
