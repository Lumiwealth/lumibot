import logging
import math
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Union
import pytz
import time

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
    StockLatestQuoteRequest,
    OptionBarsRequest,
    OptionLatestTradeRequest,
)
from alpaca.data.timeframe import TimeFrame

from lumibot.entities import Asset, Bars
from lumibot import (
    LUMIBOT_DEFAULT_TIMEZONE,
    LUMIBOT_DEFAULT_QUOTE_ASSET_SYMBOL,
    LUMIBOT_DEFAULT_QUOTE_ASSET_TYPE
)
from lumibot.tools.helpers import (
    get_decimals,
    quantize_to_num_decimals
)
from lumibot.tools.alpaca_helpers import sanitize_base_and_quote_asset

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
            tzinfo=None
    ):

        super().__init__(delay=delay)

        if tzinfo is None:
            tzinfo = pytz.timezone(LUMIBOT_DEFAULT_TIMEZONE)
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

    def _sanitize_base_and_quote_asset(self, base_asset, quote_asset) -> tuple[Asset, Asset]:
        asset, quote = sanitize_base_and_quote_asset(base_asset, quote_asset)
        return asset, quote

    def get_chains(self, asset: Asset, quote=None, exchange: str = None):
        """
        Alpaca doesn't support option trading. This method is here to comply with the DataSource interface
        """
        raise NotImplementedError(
            "Lumibot AlpacaData does not support get_chains() options data. If you need this "
            "feature, please use a different data source."
        )

    def get_last_price(self, asset, quote=None, exchange=None, **kwargs) -> Union[float, Decimal, None]:

        asset, quote = self._sanitize_base_and_quote_asset(asset, quote)

        if asset.asset_type == Asset.AssetType.CRYPTO:
            symbol = f"{asset.symbol}/{quote.symbol}"
            client = self._get_crypto_client()
            quote_params = CryptoLatestQuoteRequest(symbol_or_symbols=symbol)
            latest_quote = client.get_crypto_latest_quote(quote_params)

            # Get the first item in the dictionary
            latest_quote = latest_quote[list(latest_quote.keys())[0]]

            # The price is the average of the bid and ask
            price = (latest_quote.bid_price + latest_quote.ask_price) / 2
            num_decimals = max(get_decimals(latest_quote.bid_price), get_decimals(latest_quote.ask_price))

        elif asset.asset_type == Asset.AssetType.OPTION:
            strike_formatted = f"{asset.strike:08.3f}".replace('.', '').rjust(8, '0')
            date = asset.expiration.strftime("%y%m%d")
            symbol = f"{asset.symbol}{date}{asset.right[0]}{strike_formatted}"
            logging.info(f"Getting {asset} option price")
            client = self._get_option_client()
            params = OptionLatestTradeRequest(symbol_or_symbols=symbol)
            trade = client.get_option_latest_trade(params)
            print(f'This {trade} {symbol}')
            price = trade[symbol].price
            num_decimals = get_decimals(price)
        else:
            # Stocks
            symbol = asset.symbol
            client = self._get_stock_client()
            params = StockLatestQuoteRequest(symbol_or_symbols=symbol)
            latest_quote = client.get_stock_latest_quote(params)[symbol]

            # The price is the average of the bid and ask
            price = (latest_quote.bid_price + latest_quote.ask_price) / 2
            num_decimals = max(get_decimals(latest_quote.bid_price), get_decimals(latest_quote.ask_price))

        price = quantize_to_num_decimals(price, num_decimals)

        return price

    def get_historical_prices(
            self, asset, length, timestep="", timeshift=None, quote=None, exchange=None, include_after_hours=True
    ):
        """Get bars for a given asset"""

        asset, quote = self._sanitize_base_and_quote_asset(asset, quote)

        if not timestep:
            timestep = self.get_timestep()

        df = self._pull_source_symbol_bars(
            asset,
            length,
            timestep=timestep,
            timeshift=timeshift,
            quote=quote,
            exchange=exchange,
            include_after_hours=include_after_hours,
        )
        if df is None:
            return None

        bars = self._parse_source_symbol_bars(df, asset, quote=quote, length=length)
        return bars

    def _get_dataframe_from_api(self, asset, freq, limit=None, end=None, start=None, quote=None, include_after_hours=True) -> pd.DataFrame | None:
        """
        Gets historical bar data for the given asset and time parameters.

        Args:
            asset: Asset object or tuple (asset, quote)
            freq: Bar frequency (e.g. "1Min", "1Day")
            limit: Maximum number of bars to return
            end: End datetime (timezone aware)
            start: Start datetime (timezone aware)
            quote: Quote asset for crypto pairs

        Returns:
            DataFrame with OHLCV data and timezone aware index
        """

        # Set default limit
        if not limit:
            limit = 1000

        # Handle end time
        if not end:
            if asset.asset_type != Asset.AssetType.CRYPTO:
                # Stocks/options need delay for last 15 minutes
                end = datetime.now(self._tzinfo) - self._delay
            else:
                end = datetime.now(self._tzinfo)

            if str(freq) == "1Day":
                # Round to the next full day
                end = end + timedelta(days=1)
            else:
                # Round to last full minute
                end = end + timedelta(minutes=1)

            end = end.replace(second=0, microsecond=0)

        # Initialize pagination parameters
        max_bars_per_request = 5000
        df_list = []
        remaining_bars = limit
        current_end = end

        while remaining_bars > 0:
            # Calculate request size for this iteration
            request_size = min(remaining_bars, max_bars_per_request)

            # Calculate start time based on frequency and asset type
            if str(freq) == "1Day":
                # For daily bars, directly calculate days
                if asset.asset_type == Asset.AssetType.CRYPTO:
                    request_start = current_end - timedelta(days=request_size)
                else:
                    # For stocks/options, account for weekends (multiply by 7/5)
                    calendar_days = math.ceil(request_size * 7 / 5)
                    request_start = current_end - timedelta(days=calendar_days)

            elif str(freq) == "1Min":
                if asset.asset_type == Asset.AssetType.CRYPTO:
                    # Crypto trades 24/7, so direct minute calculation
                    request_start = current_end - timedelta(minutes=request_size)
                else:
                    # For stocks/options with extended hours (12 hours per day)
                    minutes_per_day = 12 * 60
                    trading_days = math.ceil(request_size / minutes_per_day)
                    calendar_days = math.ceil(trading_days * 7 / 5)
                    request_start = current_end - timedelta(days=calendar_days)

            else:
                # For other frequencies, treat similar to minutes
                request_start = current_end - timedelta(minutes=request_size)

            # Make API request based on asset type
            try:
                if asset.asset_type == Asset.AssetType.CRYPTO:
                    symbol = f"{asset.symbol}/{quote.symbol}"
                    client = self._get_crypto_client()
                    params = CryptoBarsRequest(
                        symbol_or_symbols=symbol,
                        timeframe=freq,
                        start=request_start,
                        end=current_end
                    )
                    barset = client.get_crypto_bars(params)

                elif asset.asset_type == Asset.AssetType.OPTION:
                    strike_formatted = f"{asset.strike:08.3f}".replace('.', '').rjust(8, '0')
                    date = asset.expiration.strftime("%y%m%d")
                    symbol = f"{asset.symbol}{date}{asset.right[0]}{strike_formatted}"
                    client = self._get_option_client()
                    params = OptionBarsRequest(
                        symbol_or_symbols=symbol,
                        timeframe=freq,
                        start=request_start,
                        end=current_end
                    )
                    barset = client.get_option_bars(params)

                else:  # Stock/ETF
                    symbol = asset.symbol
                    client = self._get_stock_client()
                    params = StockBarsRequest(
                        symbol_or_symbols=symbol,
                        timeframe=freq,
                        start=request_start,
                        end=current_end
                    )
                    barset = client.get_stock_bars(params)

                chunk_df = barset.df

                if not chunk_df.empty:
                    df_list.append(chunk_df)
                    received_bars = len(chunk_df)
                    remaining_bars -= received_bars
                    current_end = chunk_df.index[0][1]  # Start next request from earliest received data
                else:
                    break  # No more data available

            except Exception as e:
                logging.error(f"Could not get pricing data from Alpaca for {symbol} with error: {e}")
                break

            # Rate limiting pause
            time.sleep(0.1)

        # Handle case where no data was received
        if not df_list:
            logging.warning(f"No pricing data available from Alpaca for {symbol}")
            return None
        else:
            # Combine all chunks and process the final dataframe
            df = pd.concat(df_list)
            df = df.reset_index(level=0, drop=True)  # Remove MultiIndex

            # Handle timezone conversion
            if hasattr(df.index, 'tz'):
                if df.index.tz is not None:
                    df.index = df.index.tz_convert(self._tzinfo)
                else:
                    df.index = self._tzinfo.localize(df.index)

            # Clean up the dataframe
            df = df[~df.index.duplicated(keep="first")]
            df = df.sort_index()
            df = df.iloc[-limit:]  # Ensure we don't exceed the requested limit
            df = df[df.close > 0]

            if len(df) < limit:
                logging.warning(
                    f"Only got {len(df)} bars for {symbol} while {limit} were requested"
                )

        if not include_after_hours and str(freq) == "1Min" and self._tzinfo == pytz.timezone("America/New_York"):
            # Filter the data to only include regular market hours (9:30 AM to 4:00 PM ET)
            df = df[(df.index.hour >= 9) & (df.index.minute >= 30) & (df.index.hour < 16)]

        df = df.sort_index()
        return df

    def _pull_source_symbol_bars(
            self, asset, length, timestep=MIN_TIMESTEP, timeshift=None, quote=None, exchange=None,
            include_after_hours=True
    ) -> pd.DataFrame | None:

        if exchange is not None:
            logging.warning(
                f"the exchange parameter is not implemented for AlpacaData, but {exchange} was passed as the exchange"
            )

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
            end = datetime.now(tz=self._tzinfo) - asset_timeshift  # Create datetime directly in target timezone
            kwargs["end"] = end

        df = self._get_dataframe_from_api(
            asset=asset,
            freq=parsed_timestep,
            quote=quote,
            include_after_hours=include_after_hours,
            **kwargs
        )

        return df

    def _parse_source_symbol_bars(self, response, asset, quote=None, length=None):
        bars = Bars(response, self.SOURCE, asset, raw=response, quote=quote)
        return bars
