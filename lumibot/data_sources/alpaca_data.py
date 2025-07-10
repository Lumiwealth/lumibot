import logging
import os
import datetime as dt
from decimal import Decimal
from typing import Union, Tuple, Optional
import pytz
import time

import pandas as pd
import re
import requests
from collections import defaultdict
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
    OptionChainRequest,
    OptionSnapshotRequest,
)
from alpaca.data.timeframe import TimeFrame
from alpaca.data.enums import Adjustment

from lumibot.entities import Asset, Bars, Quote
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

    def _handle_auth_error(self, e, operation="data request"):
        """
        Handle authentication errors with helpful error messages.
        This will mark the strategy as failed to stop execution.
        """
        error_message = str(e).lower()
        if "unauthorized" in error_message or "401" in error_message:
            auth_method = "OAuth token" if self.oauth_token else "API key/secret"
            error_msg = (
                f"❌ ALPACA AUTHENTICATION ERROR: Your {auth_method} appears to be invalid or expired during {operation}.\n\n"
                f"🔧 To fix this:\n"
            )
            if self.oauth_token:
                error_msg += (
                    f"1. Check that your ALPACA_OAUTH_TOKEN environment variable is set correctly\n"
                    f"2. Verify your OAuth token is valid and not expired\n"
                    f"3. **MOST LIKELY**: Your OAuth token lacks MARKET DATA permissions\n"
                    f"   - OAuth tokens need separate scopes for trading vs market data\n"
                    f"   - Visit botspot.trade to re-authenticate with market data permissions\n"
                    f"4. Alternative: Use API key/secret instead by setting ALPACA_API_KEY and ALPACA_API_SECRET\n"
                    f"5. For paper trading, ensure your OAuth token has paper trading permissions\n\n"
                    f"🔑 Current OAuth token: {self.oauth_token[:10]}... (first 10 chars)\n"
                    f"📋 Paper trading: {getattr(self, 'is_paper', 'Unknown')}\n"
                    f"💡 Note: Trading operations may work while market data fails due to different OAuth scopes\n\n"
                )
            else:
                error_msg += (
                    f"1. Check that your ALPACA_API_KEY and ALPACA_API_SECRET environment variables are set correctly\n"
                    f"2. Verify your API credentials are valid\n"
                    f"3. Check that your account has proper data permissions\n\n"
                )
            error_msg += f"💀 STOPPING STRATEGY EXECUTION\n\nOriginal error: {e}"
            logging.error(error_msg)

            # Mark the data source as failed to stop further requests
            self._auth_failed = True

            # Raise a regular exception that will be caught by the strategy
            raise ValueError(f"Authentication failed: {auth_method} is invalid or expired. {error_msg}")
        else:
            # Re-raise the original exception for other errors
            raise e

    def _get_stock_client(self):
        """Lazily initialize and return the stock client."""
        if self._stock_client is None:
            try:
                if self.oauth_token:
                    self._stock_client = StockHistoricalDataClient(oauth_token=self.oauth_token)
                else:
                    self._stock_client = StockHistoricalDataClient(self.api_key, self.api_secret)
            except Exception as e:
                self._handle_auth_error(e, "stock client initialization")
        return self._stock_client

    def _get_crypto_client(self):
        """Lazily initialize and return the crypto client."""
        if self._crypto_client is None:
            try:
                if self.oauth_token:
                    self._crypto_client = CryptoHistoricalDataClient(oauth_token=self.oauth_token)
                else:
                    self._crypto_client = CryptoHistoricalDataClient(self.api_key, self.api_secret)
            except Exception as e:
                self._handle_auth_error(e, "crypto client initialization")
        return self._crypto_client

    def _get_option_client(self):
        """Lazily initialize and return the option client."""
        if self._option_client is None:
            try:
                if self.oauth_token:
                    self._option_client = OptionHistoricalDataClient(oauth_token=self.oauth_token)
                else:
                    self._option_client = OptionHistoricalDataClient(self.api_key, self.api_secret)
            except Exception as e:
                self._handle_auth_error(e, "option client initialization")
        return self._option_client

    def __init__(
            self,
            config: dict,
            max_workers: int = 20,
            chunk_size: int = 100,
            delay: Optional[int] = None,
            tzinfo: Optional[pytz.timezone] = None,
            remove_incomplete_current_bar: bool = False,
            **kwargs
    ) -> None:
        """
        Initializes the Alpaca Data Source.

        Parameters:
        - config (dict): Configuration containing API keys for Alpaca.
        - max_workers (int, optional): The maximum number of workers for parallel processing. Default is 20.
        - chunk_size (int, optional): The size of chunks for batch requests. Default is 100.
        - delay (Optional[int], optional): A delay parameter to control how many minutes to delay non-crypto data for. 
          Alpaca limits you to 15-min delayed non-crypto data unless you're on a paid data plan. 
          If not specified, uses DATA_SOURCE_DELAY environment variable or defaults to 16.
        - tzinfo (Optional[pytz.timezone], optional): The timezone used for historical data endpoints. Datetimes in 
          dataframes are adjusted to this timezone. Useful for setting UTC for crypto. Default is None.
        - remove_incomplete_current_bar (bool, optional): Default False.
          Whether to remove the incomplete current bar from the data.
          Alpaca includes incomplete bars for the current bar (ie: it gives you a daily bar for the current day even if
          the day isn't over yet). Some Lumibot users night not expect that, so this option will remove the incomplete
          bar from the data.
        **kwargs: Additional keyword arguments, such as:
                - auto_adjust (bool): if false, data is raw. If true, data is split and dividend automatically adjusted.
                Default is True.

        Returns:
        - None
        """
        # If delay is None, the parent class will use the environment variable or default to 0
        # For Alpaca, we want to default to 16 if neither delay nor environment variable is specified
        if delay is None and os.environ.get("DATA_SOURCE_DELAY") is None:
            delay = 16

        super().__init__(delay=delay, tzinfo=tzinfo)

        self.name = "alpaca"
        self.max_workers = min(max_workers, 200)
        self._remove_incomplete_current_bar = remove_incomplete_current_bar
        self._auto_adjust: bool = kwargs.get('auto_adjust', True)

        # When requesting data for assets for example,
        # if there is too many assets, the best thing to do would
        # be to split it into chunks and request data for each chunk
        self.chunk_size = min(chunk_size, 100)

        # Connection to alpaca REST API
        self.config = config

        # Initialize these to none so they will be lazily created and kept around
        # for better performance.
        self._stock_client = self._crypto_client = self._option_client = None

        # Initialize authentication credentials
        self.api_key = None
        self.api_secret = None
        self.oauth_token = None
        self._auth_failed = False  # Flag to track authentication failures

        # Check for OAuth token first
        if isinstance(config, dict) and "OAUTH_TOKEN" in config and config["OAUTH_TOKEN"]:
            self.oauth_token = config["OAUTH_TOKEN"]
        elif hasattr(config, "OAUTH_TOKEN") and config.OAUTH_TOKEN:
            self.oauth_token = config.OAUTH_TOKEN
        # If no OAuth token, check for API key/secret
        elif isinstance(config, dict) and "API_KEY" in config and config["API_KEY"]:
            self.api_key = config["API_KEY"]
            if "API_SECRET" in config and config["API_SECRET"]:
                self.api_secret = config["API_SECRET"]
            else:
                raise ValueError("API_SECRET not found in config when API_KEY is provided")
        elif hasattr(config, "API_KEY") and config.API_KEY:
            self.api_key = config.API_KEY
            if hasattr(config, "API_SECRET") and config.API_SECRET:
                self.api_secret = config.API_SECRET
            else:
                raise ValueError("API_SECRET not found in config when API_KEY is provided")
        else:
            raise ValueError("Either OAuth token or API key/secret must be provided for Alpaca authentication")

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

    def get_chains(self, asset: Asset) -> dict:
        """
        Get the options chain for the given asset.

        Parameters
        ----------
        asset : Asset
            The asset to get the chain data for.

        Returns
        -------
        chains : dict
            A dictionary containing the chain data in lumibot format:
            {
                "Chains": {
                    "PUT": {
                        "2025-01-17": [560, 565, 570, ...],
                        "2025-01-24": [560, 565, 570, ...],
                    },
                    "CALL": {
                        "2025-01-17": [560, 565, 570, ...],
                        "2025-01-24": [560, 565, 570, ...],
                    }
                }
            }
        """
        # Check if authentication has previously failed
        if getattr(self, '_auth_failed', False):
            raise ValueError("Authentication previously failed - cannot make further API requests")

        try:
            # Use the existing option client getter which has proper error handling
            client = self._get_option_client()

            # Use OptionChainRequest with underlying_symbol for stock assets
            req = OptionChainRequest(
                underlying_symbol=asset.symbol,
            )

            # Get the option chain data from Alpaca
            raw_chain_data: dict = client.get_option_chain(req)

            # Transform the raw Alpaca data into lumibot format
            chains_data = {
                "Chains": {
                    "PUT": {},
                    "CALL": {}
                }
            }

            # The Alpaca API may return option symbols in different structures
            # Let's check what we actually got and parse accordingly
            option_symbols = []

            if isinstance(raw_chain_data, dict):
                # Check for different possible structures
                if "next_page_token" in raw_chain_data and "option_chains" in raw_chain_data:
                    # New structure: {"option_chains": {"SPY250731C00501000": {...}, ...}, "next_page_token": ...}
                    option_symbols = list(raw_chain_data["option_chains"].keys())
                elif "snapshots" in raw_chain_data:
                    # Old structure: {"snapshots": {"SPY250731C00501000": {...}, ...}}
                    option_symbols = list(raw_chain_data["snapshots"].keys())
                else:
                    # Direct structure: {"SPY250731C00501000": {...}, ...}
                    # Filter to only option symbols (they should start with the underlying symbol)
                    option_symbols = [key for key in raw_chain_data.keys() if key.startswith(asset.symbol) and len(key) > len(asset.symbol)]

            if not option_symbols:
                logging.warning(f"No option symbols found for {asset.symbol}")
                return chains_data

            # Parse each option symbol
            parsed_count = 0
            for symbol in option_symbols:
                # Parse option symbol to extract details
                # Alpaca option symbols format: SPYYYMMDDCPPPPPPPPP
                # Where: SPY = underlying, YY = year, MM = month, DD = day, 
                #        C/P = call/put, PPPPPPPPP = strike price (padded)

                if len(symbol) < 15:  # Skip invalid symbols
                    continue

                # Extract the underlying symbol (everything before the date)
                underlying_len = len(asset.symbol)
                if not symbol.startswith(asset.symbol):
                    continue

                # Extract date and option type
                date_and_type = symbol[underlying_len:underlying_len+8]  # YYMMDDCP
                if len(date_and_type) < 7:
                    continue

                try:
                    year = int("20" + date_and_type[:2])
                    month = int(date_and_type[2:4])
                    day = int(date_and_type[4:6])
                    option_type = date_and_type[6]  # C or P

                    # Extract strike price (remaining digits after C/P)
                    strike_str = symbol[underlying_len+7:]
                    # Strike is usually in format like 00595000 = $595.00
                    strike = float(strike_str) / 1000.0

                    # Format expiration date
                    expiration_date = f"{year}-{month:02d}-{day:02d}"

                    # Determine option type
                    if option_type == "C":
                        option_type_key = "CALL"
                    elif option_type == "P":
                        option_type_key = "PUT"
                    else:
                        continue

                    # Add to chains data
                    if expiration_date not in chains_data["Chains"][option_type_key]:
                        chains_data["Chains"][option_type_key][expiration_date] = []

                    if strike not in chains_data["Chains"][option_type_key][expiration_date]:
                        chains_data["Chains"][option_type_key][expiration_date].append(strike)
                        parsed_count += 1

                except (ValueError, IndexError):
                    continue

            # Sort strikes for each expiration date
            for option_type in ["PUT", "CALL"]:
                for expiration_date in chains_data["Chains"][option_type]:
                    chains_data["Chains"][option_type][expiration_date].sort()

            logging.debug(f"Successfully retrieved option chains for {asset.symbol}: {len(chains_data['Chains']['PUT'])} PUT expirations, {len(chains_data['Chains']['CALL'])} CALL expirations")

            return chains_data

        except Exception as e:
            # Handle any additional errors not caught by client initialization
            self._handle_auth_error(e, "option chain retrieval")

    def get_last_price(self, asset, quote=None, exchange=None, **kwargs) -> Union[float, Decimal, None]:
        """
        Get the last price for an asset by calling get_quote and returning the last price.
        """
        quote_data = self.get_quote(asset, quote, exchange)
        if quote_data and hasattr(quote_data, 'price') and quote_data.price is not None:
            return quote_data.price
        elif quote_data and hasattr(quote_data, 'bid') and quote_data.bid:
            return quote_data.bid
        elif quote_data and hasattr(quote_data, 'ask') and quote_data.ask:
            return quote_data.ask
        return None

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

        now = dt.datetime.now(self.tzinfo)

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
        start_dt = self.tzinfo.localize(dt.datetime.combine(start_date, dt.datetime.min.time()))

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
                    adjustment=Adjustment.ALL if self._auto_adjust else Adjustment.RAW
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
                df.index = df.index.tz_convert(self.tzinfo)
            else:
                df.index = self.tzinfo.localize(df.index)

        # Clean up the dataframe
        df = df[~df.index.duplicated(keep="first")]
        df = df.sort_index()
        df = df[df.close > 0]

        if not include_after_hours and timestep == 'minute' and self.tzinfo == pytz.timezone("America/New_York"):
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

    def get_quote(self, asset: Asset, quote: Asset = None, exchange=None) -> Quote:
        """
        Get the latest quote for an asset (stock, option, or crypto).
        Returns a Quote object with bid, ask, last, and other fields if available.
        """

        # Check if authentication has previously failed
        if getattr(self, '_auth_failed', False):
            raise ValueError("Authentication previously failed - cannot make further API requests")

        asset, quote = self._sanitize_base_and_quote_asset(asset, quote)
        if asset.asset_type == Asset.AssetType.CRYPTO:
            symbol = f"{asset.symbol}/{quote.symbol if quote else 'USD'}"
            client = self._get_crypto_client()
            from alpaca.data.requests import CryptoLatestQuoteRequest
            req = CryptoLatestQuoteRequest(symbol_or_symbols=symbol)
            result = client.get_crypto_latest_quote(req)
            q = result[list(result.keys())[0]]

            # Calculate mid price if both bid and ask are available
            last_price = None
            if hasattr(q, "bid_price") and hasattr(q, "ask_price") and q.bid_price and q.ask_price:
                last_price = (q.bid_price + q.ask_price) / 2

            return Quote(
                asset=asset,
                price=last_price,
                bid=getattr(q, "bid_price", None),
                ask=getattr(q, "ask_price", None),
                timestamp=getattr(q, "timestamp", None),
                raw_data={
                    "exchange": getattr(q, "exchange", None),
                    "symbol": symbol,
                    "original_response": q
                }
            )
        elif asset.asset_type == Asset.AssetType.OPTION:
            # Note: Alpaca only supports "market" and "limit" as valid order types for multi-leg orders.
            # If you pass "credit" or "debit" as the type, Alpaca will return an "invalid order type" error.
            strike_formatted = f"{asset.strike:08.3f}".replace('.', '').rjust(8, '0')
            date = asset.expiration.strftime("%y%m%d")
            symbol = f"{asset.symbol}{date}{asset.right[0]}{strike_formatted}"
            client = self._get_option_client()
            from alpaca.data.requests import OptionLatestTradeRequest
            req = OptionLatestTradeRequest(symbol_or_symbols=symbol)
            trade = client.get_option_latest_trade(req)
            t = trade[symbol]
            # Option trades may not have bid/ask, so use price for both
            price = t.price if hasattr(t, "price") and t.price is not None else 0.0

            return Quote(
                asset=asset,
                price=price,
                bid=price,
                ask=price,
                volume=getattr(t, "size", None),
                timestamp=getattr(t, "timestamp", None),
                raw_data={
                    "exchange": getattr(t, "exchange", None),
                    "conditions": getattr(t, "conditions", None),
                    "symbol": symbol,
                    "original_response": t
                }
            )
        else:
            # Stocks
            symbol = asset.symbol
            client = self._get_stock_client()
            from alpaca.data.requests import StockLatestQuoteRequest
            req = StockLatestQuoteRequest(symbol_or_symbols=symbol)
            q = client.get_stock_latest_quote(req)[symbol]

            # Calculate mid price if both bid and ask are available
            last_price = None
            if hasattr(q, "bid_price") and hasattr(q, "ask_price") and q.bid_price and q.ask_price:
                last_price = (q.bid_price + q.ask_price) / 2

            return Quote(
                asset=asset,
                price=last_price,
                bid=getattr(q, "bid_price", None),
                ask=getattr(q, "ask_price", None),
                timestamp=getattr(q, "timestamp", None),
                raw_data={
                    "exchange": getattr(q, "exchange", None),
                    "symbol": symbol,
                    "original_response": q
                }
            )

    def query_greeks(self, asset: Asset):
        """
        Get the option greeks for an option asset via Alpaca Market Data API.
        Returns a dict mapping greek names to float values, e.g., {'delta': ..., 'gamma': ..., 'theta': ..., 'vega': ..., 'rho': ...}.
        """
        # Only options have greeks
        if asset.asset_type != Asset.AssetType.OPTION:
            return {}

        # Format option symbol for Alpaca Data API
        strike_formatted = f"{asset.strike:08.3f}".replace('.', '').rjust(8, '0')
        date = asset.expiration.strftime("%y%m%d")
        option_symbol = f"{asset.symbol}{date}{asset.right[0]}{strike_formatted}"

        # Initialize the historical data client
        if self.oauth_token:
            client = OptionHistoricalDataClient(oauth_token=self.oauth_token)
        else:
            client = OptionHistoricalDataClient(self.api_key, self.api_secret)
        request = OptionSnapshotRequest(symbol_or_symbols=option_symbol)
        try:
            snapshots = client.get_option_snapshot(request)
            snapshot = snapshots.get(option_symbol)
            if not snapshot or not snapshot.greeks:
                return {}
            greeks_obj = snapshot.greeks
            return {
                'delta': greeks_obj.delta,
                'gamma': greeks_obj.gamma,
                'theta': greeks_obj.theta,
                'vega': greeks_obj.vega,
                'rho': greeks_obj.rho,
            }
        except Exception as e:
            logging.error(f"Error fetching greeks from Alpaca Data API: {e}")
            return {}
