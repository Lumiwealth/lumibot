import logging
import math
import datetime as dt
from decimal import Decimal
from typing import Union, Tuple, Optional
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
    quantize_to_num_decimals,
    get_trading_days,
    date_n_trading_days_from_date
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
            config: dict,
            max_workers: int = 20,
            chunk_size: int = 100,
            delay: Optional[int] = 16,
            tzinfo: Optional[pytz.timezone] = None,
            remove_incomplete_current_bar: bool = False
    ) -> None:
        """
        Initializes the Alpaca Data Source.

        Parameters:
        - config (dict): Configuration containing API keys for Alpaca.
        - max_workers (int, optional): The maximum number of workers for parallel processing. Default is 20.
        - chunk_size (int, optional): The size of chunks for batch requests. Default is 100.
        - delay (Optional[int], optional): A delay parameter to control how many minutes to delay non-crypto data for. 
          Alpaca limits you to 15-min delayed non-crypto data unless you're on a paid data plan. Set to 0 if on a paid plan. Default is 16.
        - tzinfo (Optional[pytz.timezone], optional): The timezone used for historical data endpoints. Datetimes in 
          dataframes are adjusted to this timezone. Useful for setting UTC for crypto. Default is None.
        - remove_incomplete_current_bar (bool, optional): Default False.
          Whether to remove the incomplete current bar from the data.
          Alpaca includes incomplete bars for the current bar (ie: it gives you a daily bar for the current day even if
          the day isn't over yet). Some Lumibot users night not expect that, so this option will remove the incomplete
          bar from the data.

        Returns:
        - None
        """
        super().__init__(delay=delay, tzinfo=tzinfo)

        self.name = "alpaca"
        self.max_workers = min(max_workers, 200)
        self._remove_incomplete_current_bar = remove_incomplete_current_bar

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
            self,
            asset: Asset,
            length: int,
            timestep: str = "",
            timeshift: Optional[dt.timedelta] = None,
            quote: Optional[Asset] = None,
            exchange: Optional[str] = None,
            include_after_hours: bool = True
    ) -> Optional[Bars]:

        """Get bars for a given asset"""

        if exchange is not None:
            logging.warning(
                f"the exchange parameter is not implemented for AlpacaData, but {exchange} was passed as the exchange"
            )

        asset, quote = self._sanitize_base_and_quote_asset(asset, quote)

        if not timestep:
            timestep = self.get_timestep()

        df = self._get_dataframe_from_api(
            asset=asset,
            length=length,
            timestep=timestep,
            timeshift=timeshift,
            quote=quote,
            include_after_hours=include_after_hours
        )
        if df is None:
            return None

        bars = self._parse_source_symbol_bars(df, asset, quote=quote, length=length)
        return bars

    def _get_dataframe_from_api(
            self,
            asset: Asset,
            length: int,
            timestep: str = "",
            timeshift: Optional[dt.timedelta] = None,
            quote: Optional[Asset] = None,
            exchange: Optional[str] = None,
            include_after_hours: bool = True
    ) -> Optional[pd.DataFrame]:

        timeframe = self._parse_source_timestep(timestep, reverse=True)

        now = dt.datetime.now(self._tzinfo)

        # Create end time
        if asset.asset_type != Asset.AssetType.CRYPTO and isinstance(self._delay, dt.timedelta):
            # Stocks/options need delay for last 15 minutes
            end_dt = now - self._delay
        else:
            end_dt = now

        if timeshift is not None:
            if not isinstance(timeshift, dt.timedelta):
                raise TypeError("timeshift must be a timedelta")
            end_dt = end_dt - timeshift

        # Calculate the start_dt
        if timestep == 'day':
            days_needed = length
        else:
            # For minute bars, calculate additional days needed accounting for weekends/holidays
            minutes_per_day = 390  # ~6.5 hours of trading per day
            days_needed = (length // minutes_per_day) + 1

        start_date = date_n_trading_days_from_date(
            n_days=days_needed,
            start_datetime=end_dt,
            # TODO: pass market into DataSource
            # This works for now. Crypto gets more bars but throws them out.
            market='NYSE'
        )
        start_dt = self._tzinfo.localize(dt.datetime.combine(start_date, dt.datetime.min.time()))

        # Make API request based on asset type
        try:
            if asset.asset_type == Asset.AssetType.CRYPTO:
                symbol = f"{asset.symbol}/{quote.symbol}"
                client = self._get_crypto_client()

                # noinspection PyArgumentList
                params = CryptoBarsRequest(
                    symbol_or_symbols=symbol,
                    timeframe=timeframe,
                    start=start_dt,
                    end=end_dt,
                )
                barset = client.get_crypto_bars(params)

            elif asset.asset_type == Asset.AssetType.OPTION:
                strike_formatted = f"{asset.strike:08.3f}".replace('.', '').rjust(8, '0')
                date = asset.expiration.strftime("%y%m%d")
                symbol = f"{asset.symbol}{date}{asset.right[0]}{strike_formatted}"
                client = self._get_option_client()

                # noinspection PyArgumentList
                params = OptionBarsRequest(
                    symbol_or_symbols=symbol,
                    timeframe=timeframe,
                    start=start_dt,
                    end=end_dt,
                )
                barset = client.get_option_bars(params)

            else:  # Stock/ETF
                symbol = asset.symbol
                client = self._get_stock_client()

                # noinspection PyArgumentList
                params = StockBarsRequest(
                    symbol_or_symbols=symbol,
                    timeframe=timeframe,
                    start=start_dt,
                    end=end_dt,
                )
                barset = client.get_stock_bars(params)

            df = barset.df

        except Exception as e:
            logging.error(f"Could not get pricing data from Alpaca for {symbol} with error: {e}")
            return None

        # Handle case where no data was received
        if df.empty:
            logging.warning(f"No pricing data available from Alpaca for {symbol}")
            return None

        # Remove MultiIndex
        df = df.reset_index(level=0, drop=True)

        # Timezone conversion
        if hasattr(df.index, 'tz'):
            if df.index.tz is not None:
                df.index = df.index.tz_convert(self._tzinfo)
            else:
                df.index = self._tzinfo.localize(df.index)

        # Clean up the dataframe
        df = df[~df.index.duplicated(keep="first")]
        df = df.sort_index()
        df = df[df.close > 0]

        if not include_after_hours and timestep == 'minute' and self._tzinfo == pytz.timezone("America/New_York"):
            # Filter data to include only regular market hours
            df = df[(df.index.hour >= 9) & (df.index.minute >= 30) & (df.index.hour < 16)]

        # Check for incomplete bars
        if self._remove_incomplete_current_bar:
            if timestep == "minute":
                # For minute bars, remove the current minute
                current_minute = now.replace(second=0, microsecond=0)
                df = df[df.index < current_minute]
            else:
                # For daily bars, remove today's bar if market is open
                current_date = now.date()
                df = df[df.index.date < current_date]

        # Ensure df only contains the last N bars
        if len(df) > length:
            df = df.iloc[-length:]

        return df

    def _parse_source_symbol_bars(self, response, asset, quote=None, length=None):
        bars = Bars(response, self.SOURCE, asset, raw=response, quote=quote)
        return bars
