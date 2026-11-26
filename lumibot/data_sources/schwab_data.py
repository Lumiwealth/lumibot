from __future__ import annotations

import copy
import math
import os
import threading
import time

from lumibot._lazy_imports import LazyLogger, LazyModule, LazyPytzTimezoneRef, lazy_class
from lumibot.data_sources import DataSource

logger = LazyLogger(__name__)
TYPE_CHECKING = False
datetime = LazyModule("datetime")
timedelta = lazy_class("datetime", "timedelta")
pd = LazyModule("pandas")
Asset = lazy_class("lumibot.entities", "Asset")
_DEFAULT_PYTZ = LazyPytzTimezoneRef("America/New_York")
LUMIBOT_DEFAULT_TIMEZONE = "America/New_York"

if TYPE_CHECKING:
    from lumibot.entities import Bars, Quote


def _default_pytz():
    return _DEFAULT_PYTZ._load()


def __getattr__(name):
    if name == "LUMIBOT_DEFAULT_PYTZ":
        return _default_pytz()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def colored(*args, **kwargs):
    from termcolor import colored as _colored

    return _colored(*args, **kwargs)


def _get_chains_class():
    from lumibot.entities import Chains

    return Chains


def _get_bars_class():
    from lumibot.entities import Bars

    return Bars


def _shared_bar_cache():
    from lumibot.tools import shared_bar_cache

    return shared_bar_cache


def _get_quote_class():
    from lumibot.entities import Quote

    return Quote


def _get_trading_days(*args, **kwargs):
    from lumibot.tools.helpers import get_trading_days

    return get_trading_days(*args, **kwargs)


def _parse_timestep_qty_and_unit(*args, **kwargs):
    from lumibot.tools.helpers import parse_timestep_qty_and_unit

    return parse_timestep_qty_and_unit(*args, **kwargs)


def _schwab_option_symbol(asset) -> str:
    """Schwab OCC-style option symbol used by quotes and price history."""
    from lumibot.tools.index_option_roots import index_option_root

    root_symbol = index_option_root(asset.symbol, asset.expiration).ljust(6)
    year_str = asset.expiration.strftime("%y")
    month_str = asset.expiration.strftime("%m")
    day_str = asset.expiration.strftime("%d")
    option_type = "C" if str(asset.right).upper() == "CALL" else "P"
    # Round to whole thousandths: truncating (2.3 - 2) * 1000 = 299.999... gives strike 2.299.
    strike_whole, strike_decimal = divmod(round(float(asset.strike) * 1000), 1000)
    return f"{root_symbol}{year_str}{month_str}{day_str}{option_type}{strike_whole:05d}{strike_decimal:03d}"


def _schwab_market_symbol(asset) -> str | None:
    """Symbol Schwab price history and quotes actually accept.

    Option candles come back only for the OCC symbol, not the underlying root.
    Crypto has no Schwab Trader API symbol. Equity tickers stay unchanged:
    a live read on 2026-09-22 showed ticker BTC is the Grayscale Bitcoin Mini
    Trust ETF, so Asset("BTC") still requests "BTC". Only asset_type crypto
    is refused.
    """
    asset_type = getattr(asset, "asset_type", None)
    if asset_type == "crypto":
        return None
    if asset_type == "option":
        return _schwab_option_symbol(asset)
    if asset_type == "future":
        symbol = str(asset.symbol)
        return symbol if symbol.startswith("/") else f"/{symbol}"
    return str(getattr(asset, "symbol", "") or "").strip()

class SchwabData(DataSource):
    """
    Data source that connects to the Schwab Broker API.

    This class provides methods to fetch historical price data, option chains, and other information
    from the Schwab API. It requires a Schwab API client to be passed in during initialization.

    Link to Schwab API documentation: https://developer.schwab.com/ and create an account to get API doc access.
    Link to the Python client library: https://schwab-py.readthedocs.io/en/latest/
    """

    MIN_TIMESTEP = "minute"
    SOURCE = "Schwab"
    CHAIN_FULL_INFO_REQUIRES_PRICING_INPUTS = False

    def __init__(self, client=None, api_key=None, secret=None, account_number=None, auto_create_client=True, **kwargs):
        """
        Initialize the Schwab data source with a client connection.
        
        Args:
            client: Schwab API client instance
            api_key: Schwab API key (used if client is None)
            secret: Schwab API secret (used if client is None)
            account_number: Schwab account number (used if client is None)
        """
        super().__init__()

        # If client is provided, use it
        if client is not None:
            self.client = client
        elif auto_create_client:
            # Otherwise try to create a client with provided credentials
            self.client = self.create_schwab_client(api_key, secret, account_number)
        else:
            self.client = None

        if self.client is None:
            logger.warning(colored("SchwabData initialized without client. Methods will not work until a client is provided.", "yellow"))

    @staticmethod
    def create_schwab_client(api_key=None, secret=None, account_number=None):
        """
        Create and return a Schwab client instance.
        
        Args:
            api_key (str): Schwab API key
            secret (str): Schwab API secret
            account_number (str): Schwab account number
            
        Returns:
            client: Configured Schwab client or None if credentials are missing
        """
        if not all([api_key, secret, account_number]):
            # Try to load from environment variables
            api_key = api_key or os.environ.get('SCHWAB_API_KEY')
            secret = secret or os.environ.get('SCHWAB_SECRET')
            account_number = account_number or os.environ.get('SCHWAB_ACCOUNT_NUMBER')

            if not all([api_key, secret, account_number]):
                logger.warning(colored("Missing Schwab API credentials. Ensure SCHWAB_API_KEY, SCHWAB_SECRET, and SCHWAB_ACCOUNT_NUMBER are set in .env file or passed as parameters.", "yellow"))
                return None

        try:
            # Import Schwab-specific libraries
            from schwab.auth import easy_client

            # Store Schwab token in the working directory (or override with SCHWAB_TOKEN_PATH)
            token_path_value = os.environ.get('SCHWAB_TOKEN_PATH')
            if token_path_value:
                token_path = os.path.abspath(os.path.expanduser(token_path_value))
            else:
                token_path = os.path.join(os.getcwd(), 'schwab_token.json')

            # Ensure directory exists
            os.makedirs(os.path.dirname(token_path), exist_ok=True)

            # Create Schwab API client
            client = easy_client(api_key, secret, os.environ.get( 'SCHWAB_BACKEND_CALLBACK_URL', 'https://127.0.0.1:8182'), token_path)

            logger.info(colored("Successfully created Schwab client", "green"))
            return client
        except Exception as e:
            logger.error(colored(f"Error creating Schwab client: {e}", "red"))
            return None

    def set_client(self, client):
        """
        Set the client for this data source.
        
        Args:
            client: Schwab API client instance
        """
        self.client = client
        logger.info(colored("Schwab client set for data source", "green"))

    # ----- live option chain cache -------------------------------------------
    def get_chains(self, asset: Asset, quote: Asset = None, exchange: str = None, strike_count: int = 100) -> dict:
        """Option chain (expirations and strikes) for ``asset``, kept for a while.

        Expirations and strikes change about once a day, so a chain is fetched once
        per underlying per ``LUMIBOT_CHAINS_CACHE_SECONDS`` (default 900; 0 fetches
        every call). Every call gets its own copy, so a strategy that edits the
        returned chain never changes the kept one. When a kept chain is handed out
        after 4:00 PM New York time, today's expiration (and any earlier one) is
        removed from the copy. Prices are never kept here; read them with quotes.
        """
        ttl = self._env_seconds("LUMIBOT_CHAINS_CACHE_SECONDS", 900.0)
        if ttl <= 0:
            return self._get_chains_uncached(asset, quote=quote, exchange=exchange, strike_count=strike_count)
        key = (getattr(asset, "symbol", str(asset)), str(getattr(asset, "asset_type", "")), strike_count)
        lock, _, _ = self._quote_state()
        cache = self.__dict__.setdefault("_chains_cache", {})
        now = self._cache_clock()
        with lock:
            hit = cache.get(key)
        if hit is not None and 0 <= now - hit[0] < ttl:
            logger.debug(f"[ChainCache] {key[0]} served, {now - hit[0]:.0f} s old (keep {ttl:.0f} s)")
            return self._chain_copy(hit[1], drop_ended=True)
        chains = self._get_chains_uncached(asset, quote=quote, exchange=exchange, strike_count=strike_count)
        if chains:
            with lock:
                cache[key] = (now, chains)
            return self._chain_copy(chains, drop_ended=False)
        return chains

    def _new_york_now(self):
        clock = getattr(self, "_new_york_clock", None)
        if callable(clock):
            return clock()
        return datetime.datetime.now(_default_pytz())

    def _chain_copy(self, chains, drop_ended: bool):
        """A copy whose expiration and strike lists can be edited without touching the kept chain."""
        data = dict(chains)
        nested = data.get("Chains")
        if isinstance(nested, dict):
            data["Chains"] = {
                right: ({expiry: list(strikes) for expiry, strikes in expirations.items()}
                        if isinstance(expirations, dict) else copy.copy(expirations))
                for right, expirations in nested.items()
            }
        if drop_ended:
            try:
                now_ny = self._new_york_now()
                today = now_ny.date().isoformat()
                after_close = (now_ny.hour, now_ny.minute) >= (16, 0)
                for right in ("CALL", "PUT"):
                    expirations = (data.get("Chains") or {}).get(right)
                    if not isinstance(expirations, dict):
                        continue
                    for expiry in list(expirations):
                        day = str(expiry)[:10]
                        if day < today or (after_close and day == today):
                            expirations.pop(expiry, None)
            except Exception as exc:
                logger.debug(f"[ChainCache] could not drop ended expirations: {exc}")
        if type(chains) is dict:
            return data
        try:
            return type(chains)(data)
        except Exception:
            return data

    def _get_chains_uncached(self, asset: Asset, quote: Asset = None, exchange: str = None, strike_count: int = 100) -> dict:
        """
        Obtains option chain information for the asset (stock) from each
        of the exchanges the options trade on and returns a dictionary
        for each exchange.

        Parameters
        ----------
        asset : Asset
            The asset to get the option chains for
        quote : Asset | None
            The quote asset to get the option chains for
        exchange: str | None
            The exchange to get the option chains for
        strike_count: int
            Number of strikes to return above and below the at-the-money price (default: 10)

        Returns
        -------
        dictionary of dictionary
            Format:
            - `Multiplier` (str) eg: `100`
            - 'Chains' - paired Expiration/Strike info to guarantee that the strikes are valid for the specific
                         expiration date.
                         Format:
                           chains['Chains']['CALL'][exp_date] = [strike1, strike2, ...]
                         Expiration Date Format: 2023-07-31
        """
        if self.client is None:
            logger.error(colored("No Schwab client available for get_chains", "red"))
            return {}

        # Initialize chains structure
        chains = {
            "Multiplier": 100,  # Standard option contracts are for 100 shares
            "Exchange": "SMART",  # Default exchange routing
            "Chains": {"CALL": {}, "PUT": {}}
        }

        try:
            # Find an appropriate strike range value
            strike_range = None
            try:
                strike_range_options = dir(self.client.Options.StrikeRange)
                # Look for near the money options - options may have different naming
                if 'NTM' in strike_range_options:
                    strike_range = self.client.Options.StrikeRange.NTM
                elif 'NEAR_THE_MONEY' in strike_range_options:
                    strike_range = self.client.Options.StrikeRange.NEAR_THE_MONEY
                elif 'ATM' in strike_range_options:
                    strike_range = self.client.Options.StrikeRange.ATM
                elif 'AT_THE_MONEY' in strike_range_options:
                    strike_range = self.client.Options.StrikeRange.AT_THE_MONEY
                elif 'SNK' in strike_range_options:
                    strike_range = self.client.Options.StrikeRange.SNK
                elif 'STRIKES_NEAR_MARKET' in strike_range_options:
                    strike_range = self.client.Options.StrikeRange.STRIKES_NEAR_MARKET
                else:
                    # Fallback to all strikes
                    strike_range = self.client.Options.StrikeRange.ALL

                logger.debug(colored(f"Using strike range: {strike_range}", "blue"))
            except Exception as e:
                logger.warning(colored(f"Error finding strike range options: {e}. Using None.", "yellow"))
                strike_range = None

            # Fetch both call and put options in a single API call using ALL contract type
            logger.info(colored(f"Fetching option chains for {asset.symbol}", "blue"))

            params = {
                "symbol": asset.symbol,
                "contract_type": self.client.Options.ContractType.ALL,  # Get both calls and puts
                "strategy": self.client.Options.Strategy.SINGLE,
                "include_underlying_quote": False,
                "strike_count": strike_count
            }

            # Add strike_range if available
            if strike_range is not None:
                params["strike_range"] = strike_range

            def fetch_chain_data():
                self._before_market_data_request("option chain")
                response = self.client.get_option_chain(**params)
                self._after_market_data_response(response, "option chain")
                if not response:
                    logger.error(colored(f"No response from API for {asset.symbol}", "red"))
                    return None
                if hasattr(response, "status_code"):
                    if response.status_code != 200:
                        logger.error(colored(f"Error fetching options for {asset.symbol}: {response.status_code}", "red"))
                        return None
                    result = response.json()
                else:
                    result = response
                if (not isinstance(result, dict) or result.get("error")
                        or str(result.get("status", "SUCCESS")).upper() != "SUCCESS"):
                    logger.error(colored(f"Invalid or unsuccessful option chain for {asset.symbol}", "red"))
                    return None
                return result

            data = fetch_chain_data()
            if data is None:
                return {}

            # Extract option data for both call and put types
            success = False

            # Helper function to extract option data for a specific type (CALL/PUT)
            def extract_option_data(option_type):
                map_key = f"{option_type.lower()}ExpDateMap"
                if map_key not in data:
                    return False

                option_dates = data[map_key]
                for exp_date_str, strikes_data in option_dates.items():
                    # Format the expiration date (assumed format: YYYY-MM-DD:days_to_expiry)
                    exp_date = exp_date_str.split(':')[0]  # Extract just the date part

                    # Initialize list to store strikes for this expiration
                    chains["Chains"][option_type][exp_date] = []

                    # Add all available strikes for this expiration date
                    for strike_str, strike_data in strikes_data.items():
                        strike = float(strike_str)
                        chains["Chains"][option_type][exp_date].append(strike)

                    # Sort the strikes in ascending order
                    chains["Chains"][option_type][exp_date].sort()

                return any(chains["Chains"][option_type].values())

            # Extract data for both call and put options
            call_success = extract_option_data("CALL")
            put_success = extract_option_data("PUT")
            success = call_success or put_success

            # A retry must actually change the request. Recursing through this
            # method recreated NTM every time and could issue unbounded reads.
            if not success and strike_range is not None and strike_range != self.client.Options.StrikeRange.ALL:
                logger.warning(colored("No option data found with current strike range. Trying with ALL strikes...", "yellow"))
                params["strike_range"] = self.client.Options.StrikeRange.ALL
                data = fetch_chain_data()
                if data is None:
                    return {}
                chains["Chains"] = {"CALL": {}, "PUT": {}}
                call_success = extract_option_data("CALL")
                put_success = extract_option_data("PUT")
                success = call_success or put_success

            # Extract underlying data if available
            if 'underlying' in data and (not chains.get("Exchange") or not chains.get("Multiplier")):
                underlying = data.get('underlying', {})
                if underlying:
                    # Update multiplier if available
                    multiplier = underlying.get('multiplier')
                    if multiplier:
                        chains["Multiplier"] = int(multiplier)

                    # Update exchange if available
                    exchange_name = underlying.get('exchange')
                    if exchange_name:
                        chains["Exchange"] = exchange_name

            if not success:
                logger.error(colored(f"No option data found for {asset.symbol}", "red"))

            # Wrap into Chains entity for richer interface (backwards-compatible: Chains inherits dict)
            try:
                return _get_chains_class()(chains)
            except Exception:
                return chains

        except Exception as e:
            logger.error(colored(f"Error getting option chains for {asset.symbol}: {str(e)}", "red"))
            return {}

    def get_chain_full_info(
        self, asset: Asset, expiry, chains=None, underlying_price=None,
        risk_free_rate=None, strike_min=None, strike_max=None,
    ):
        """Fetch one expiration's prices and Greeks in one Schwab request.

        This overrides DataSource's per-contract reconstruction using the
        existing public full-chain API. ``chains`` may be previously fetched
        strike metadata; it is not a quote cache. Each call requests a fresh
        snapshot. Missing prices and timestamps stay missing, so callers must
        apply their own liquidity and freshness requirements before trading.
        Provider failures raise rather than masquerading as an empty market.
        """
        if isinstance(expiry, datetime.datetime):
            expiry = expiry.date()
        elif isinstance(expiry, str):
            expiry = datetime.date.fromisoformat(expiry)
        elif not isinstance(expiry, datetime.date):
            raise TypeError("expiry must be an ISO date string, datetime.date or datetime.datetime instance")
        if self.client is None:
            raise ConnectionError("No Schwab client available for full option chain")
        if strike_min is not None and strike_max is not None and strike_min > strike_max:
            raise ValueError("strike_min must not exceed strike_max")

        self._before_market_data_request("option chain")
        response = self.client.get_option_chain(
            symbol=asset.symbol,
            contract_type=self.client.Options.ContractType.ALL,
            strategy=self.client.Options.Strategy.SINGLE,
            from_date=expiry,
            to_date=expiry,
            include_underlying_quote=False,
        )
        self._after_market_data_response(response, "option chain")
        status = getattr(response, "status_code", None)
        if status != 200:
            # Do not include provider bodies: they may contain account details.
            raise ConnectionError(f"Schwab full option chain request failed (status {status})")
        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("Invalid Schwab full option chain response")
        if payload.get("error") or str(payload.get("status", "SUCCESS")).upper() != "SUCCESS":
            raise ConnectionError("Schwab full option chain reported failure")

        rows = []
        for right, map_name in (("CALL", "callExpDateMap"), ("PUT", "putExpDateMap")):
            expiration_map = payload.get(map_name) or {}
            if not isinstance(expiration_map, dict):
                raise ValueError("Invalid Schwab option expiration map")
            for expiration_key, strikes in expiration_map.items():
                if str(expiration_key).split(":", 1)[0] != expiry.isoformat():
                    continue
                if not isinstance(strikes, dict):
                    raise ValueError("Invalid Schwab option strike map")
                for contracts in strikes.values():
                    if not isinstance(contracts, list):
                        raise ValueError("Invalid Schwab option contracts")
                    for item in contracts:
                        if not isinstance(item, dict):
                            raise ValueError("Invalid Schwab option contract")
                        strike = item.get("strikePrice")
                        if strike is None or not item.get("symbol"):
                            continue
                        strike = float(strike)
                        if strike_min is not None and strike < strike_min:
                            continue
                        if strike_max is not None and strike > strike_max:
                            continue
                        row = {
                            "symbol": item["symbol"], "last": item.get("last"),
                            "expiration_date": expiry, "strike": strike,
                            "option_type": right, "underlying": asset.symbol,
                            "open_interest": item.get("openInterest"),
                            "bid": item.get("bid"), "ask": item.get("ask"),
                            "bidsize": item.get("bidSize"), "asksize": item.get("askSize"),
                            "volume": item.get("totalVolume"),
                            "last_volume": item.get("lastSize"),
                            "average_volume": None, "type": "option",
                            "implied_volatility": item.get("volatility"),
                            "is_delayed": payload.get("isDelayed"),
                            "non_standard": item.get("nonStandard"),
                            "multiplier": item.get("multiplier"),
                            "deliverables": item.get("optionDeliverablesList"),
                            "quote_time": self.convert_epoch_ms_to_datetime(item.get("quoteTimeInLong")),
                            "trade_time": self.convert_epoch_ms_to_datetime(item.get("tradeTimeInLong")),
                        }
                        for greek in ("delta", "gamma", "theta", "vega", "rho"):
                            value = item.get(greek)
                            row[f"greeks.{greek}"] = None if value == -999 else value
                        rows.append(row)
        return pd.DataFrame(rows).sort_values(["strike", "option_type"]).reset_index(drop=True) if rows else pd.DataFrame()

    def convert_timestep_str_to_timedelta(self, timestep_str):
        """
        Convert a timestep string to a timedelta object.
        
        Args:
            timestep_str: String representing the timestep (e.g., '1minute', '1day')
            
        Returns:
            tuple: (timedelta object, timestep_unit string)
        """
        qty, unit = _parse_timestep_qty_and_unit(timestep_str)

        if unit == "minute":
            return timedelta(minutes=qty), unit
        elif unit == "hour":
            return timedelta(hours=qty), unit
        elif unit == "day":
            return timedelta(days=qty), unit
        elif unit == "week":
            return timedelta(weeks=qty), unit
        elif unit == "month":
            # Approximation - months vary in length
            return timedelta(days=30 * qty), unit
        else:
            logger.warning(colored(f"Unknown timestep unit {unit}. Using 'day' as default.", "yellow"))
            return timedelta(days=qty), "day"

    def get_historical_prices(
        self, asset, length, timestep="", timeshift=None, quote=None, exchange=None, include_after_hours=True, return_polars: bool = False
    ) -> Bars:
        """
        Get historical price data for an asset from Schwab API.
        
        Parameters
        ----------
        asset : Asset
            The asset to get the bars for.
        length : int
            The number of bars to get.
        timestep : str
            The timestep to get the bars at. For example, "minute" or "day".
        timeshift : datetime.timedelta
            The amount of time to shift the bars by. For example, if you want the bars from 1 hour ago to now,
            you would set timeshift to 1 hour.
        quote : Asset
            The quote asset to get the bars for.
        exchange : str
            The exchange to get the bars for.
        include_after_hours : bool
            Whether to include after hours data.
            
        Returns
        -------
        Bars
            Historical price data as a Bars object, or None if there was an error.
        """
        if self.client is None:
            logger.error(colored("No Schwab client available for get_historical_prices", "red"))
            return None

        # Schwab futures candle history is not available. Option candles are:
        # a live call on 2026-09-22 returned daily bars for an SPY call when
        # the request used the OCC symbol. Crypto is not on this API: ticker
        # BTC is an ETF, not bitcoin.
        if asset.asset_type == "future":
            logger.error(colored("Schwab doesn't provide price history for futures", "red"))
            return None
        if asset.asset_type == "crypto":
            logger.error(colored(
                "Schwab does not support the crypto asset type. Equity tickers such as BTC and ETH are unchanged.",
                "red",
            ))
            return None

        symbol = _schwab_market_symbol(asset)
        if not symbol:
            logger.error(colored(f"No Schwab symbol for {asset.symbol}", "red"))
            return None

        # Use default timestep if not provided
        timestep = timestep if timestep else self.MIN_TIMESTEP

        # Parse the timestep
        timestep_qty, timestep_unit = _parse_timestep_qty_and_unit(timestep)
        if timestep_unit == "second":
            logger.error(colored(
                "Schwab does not provide second bars. Minute bars are 1, 5, 10, 15, and 30.",
                "red",
            ))
            return None

        # Calculate end date in Eastern time
        end_date = datetime.datetime.now()
        eastern = _default_pytz()
        end_date = end_date.astimezone(eastern)

        # Apply timeshift if provided
        if timeshift:
            end_date = end_date - timeshift

        # Calculate the start date based on length and timestep
        td, _ = self.convert_timestep_str_to_timedelta(timestep)
        start_date = end_date - (td * length)

        # Special handling for daily bars to ensure we get the correct number of trading days
        if timestep_unit == 'day' and timeshift is None:
            # What we really want is the last n bars, not the bars from the last n days.
            # Get twice as many days as we need to ensure we get enough bars, then add 3 days for long weekends
            tcal_start_date = end_date - (td * length * 2 + timedelta(days=3))

            try:
                trading_days = _get_trading_days(market='NYSE', start_date=tcal_start_date, end_date=end_date)
                # Filter out trading days when the market_open is after the end_date
                trading_days = trading_days[trading_days['market_open'] < end_date]
                # Now, start_date is the length bars before the last trading day
                if len(trading_days) >= length:
                    start_date = trading_days.index[-length]
            except Exception as e:
                logger.warning(colored(f"Could not calculate trading days, using calendar days instead: {e}", "yellow"))

        try:
            # Map timestep to Schwab API parameters
            period_type = None
            frequency_type = None
            frequency = None

            # Set appropriate frequency_type and frequency based on timestep_unit
            if timestep_unit == "minute":
                frequency_type = self.client.PriceHistory.FrequencyType.MINUTE
                # Use the closest supported frequency value
                if timestep_qty in [1, 5, 10, 15, 30]:
                    frequency = timestep_qty
                else:
                    # Find the closest supported frequency
                    supported_frequencies = [1, 5, 10, 15, 30]
                    frequency = min(supported_frequencies, key=lambda x: abs(x - timestep_qty))
                    logger.warning(colored(f"Non-standard minute frequency: {timestep_qty}. Using closest supported frequency: {frequency}", "yellow"))
            elif timestep_unit == "hour":
                frequency_type = self.client.PriceHistory.FrequencyType.MINUTE
                # Schwab minute frequencies stop at 30. There is no 60-minute bar.
                frequency = 30
                logger.warning(colored(
                    "Schwab has no 60-minute bars. This hour request uses 30-minute candles.",
                    "yellow",
                ))
            elif timestep_unit == "day":
                # daily handled below with helper call
                frequency_type = None
                frequency = None
            elif timestep_unit == "week":
                frequency_type = self.client.PriceHistory.FrequencyType.WEEKLY
                frequency = 1
            elif timestep_unit == "month":
                frequency_type = self.client.PriceHistory.FrequencyType.MONTHLY
                frequency = 1
            else:
                logger.warning(colored(f"Unknown timestep unit: {timestep_unit}. Using 'daily' as default.", "yellow"))
                frequency_type = self.client.PriceHistory.FrequencyType.DAILY
                frequency = 1

            freq_enum = frequency
            try:
                if isinstance(frequency, int):
                    freq_enum = self.client.PriceHistory.Frequency(frequency)
            except Exception:
                pass

            def fetch(range_start, range_end):
                """One Schwab candle read for [range_start, range_end]. Errors raise; nothing is cached."""
                self._before_market_data_request("price history")
                if timestep_unit == "day":
                    response = self.client.get_price_history_every_day(
                        symbol,
                        start_datetime=range_start.to_pydatetime(),
                        end_datetime=range_end.to_pydatetime(),
                        need_extended_hours_data=include_after_hours,
                    )
                else:
                    response = self.client.get_price_history(
                        symbol=symbol,
                        frequency_type=frequency_type,
                        frequency=freq_enum,
                        start_datetime=range_start.to_pydatetime(),
                        end_datetime=range_end.to_pydatetime(),
                        need_extended_hours_data=include_after_hours,
                    )
                self._after_market_data_response(response, "price history")
                if hasattr(response, 'status_code'):
                    if response.status_code != 200:
                        raise RuntimeError(f"Schwab price history returned {response.status_code}: {response.text}")
                    data = response.json()
                else:
                    data = response
                if not data or 'candles' not in data:
                    raise RuntimeError("Schwab price history response has no candles field")
                frame = pd.DataFrame(data['candles'])
                if frame.empty:
                    return frame
                expected_columns = ['open', 'high', 'low', 'close', 'volume', 'datetime']
                if not all(col in frame.columns for col in expected_columns):
                    logger.warning(colored(f"Missing expected columns in response. Got: {frame.columns.tolist()}", "yellow"))
                # Epoch milliseconds identify UTC instants, never local wall time.
                frame['datetime'] = pd.to_datetime(frame['datetime'], unit='ms', utc=True)
                frame = frame.set_index('datetime')
                return frame[~frame.index.duplicated(keep='first')]

            # Charts, bots and broker-backed backtests share one bar cache per connection.
            provider_timestep = {
                "minute": f"{frequency}minute",
                "hour": "30minute",
                "day": "1day",
                "week": "1week",
                "month": "1month",
            }.get(timestep_unit, "1day")
            series = _shared_bar_cache().BarSeries.make(
                "schwab",
                symbol,
                provider_timestep,
                # The Schwab broker sets one scope for every Schwab connection (Schwab returns
                # the same prices to all); a bare data source without it reads directly.
                scope=self.shared_bars_scope,
                legacy_scopes=(self.shared_bars_legacy_scope,),
                adjusted=True,
                assetType=str(asset.asset_type),
                extendedHours=bool(include_after_hours),
            )
            df = _shared_bar_cache().read_bars(series, start_date, end_date, fetch)

            # If no candles were returned, return None
            if df.empty:
                logger.warning(colored(f"No price data available for {asset.symbol} in the requested time range", "yellow"))
                return None

            df = _shared_bar_cache().to_timezone(df, LUMIBOT_DEFAULT_TIMEZONE)
            df.index.name = 'datetime'

            # Create and return the Bars object
            bars = _get_bars_class()(df, self.SOURCE, asset, raw=df, quote=quote)
            return bars

        except Exception as e:
            logger.error(colored(f"Error getting historical prices for {asset.symbol}: {str(e)}", "red"))
            return None

    def get_last_price(self, asset, quote=None, exchange=None) -> Union[float, Decimal, None]:
        """
        Get the last price of an asset from Schwab API.
        
        Args:
            asset: The asset to get the price for
            quote: The quote asset if applicable
            exchange: The exchange if applicable
            
        Returns:
            The last price of the asset or None if it can't be retrieved
        """
        if self.client is None:
            logger.error(colored("No Schwab client available for get_last_price", "red"))
            return None

        try:
            # Get the quote for the asset
            asset_quote = self.get_quote(asset, quote, exchange)

            # If we have a valid quote with a price, return it
            if asset_quote and asset_quote.price is not None:
                return float(asset_quote.price)

            logger.warning(colored(f"Could not find last price for {asset.symbol}", "yellow"))
            return None

        except Exception as e:
            logger.error(colored(f"Error in get_last_price for {asset.symbol}: {str(e)}", "red"))
            return None

    def convert_epoch_ms_to_datetime(self, epoch_ms):
        """Convert epoch milliseconds to datetime object with timezone info"""
        if not epoch_ms:
            return None
        try:
            # Convert milliseconds to seconds and create UTC datetime with timezone info
            dt = datetime.datetime.fromtimestamp(epoch_ms / 1000, tz=datetime.timezone.utc)
            return dt
        except Exception as e:
            logger.error(colored(f"Error converting timestamp: {e}", "red"))
            return None

    def _quote_from_response(self, asset, symbol, asset_quote):
        """The same exact-symbol parser serves singleton and bulk requests."""
        quote_info = asset_quote.get("quote", {})
        if not isinstance(quote_info, dict):
            raise ValueError("Invalid Schwab quote fields")
        quote_time = self.convert_epoch_ms_to_datetime(quote_info.get("quoteTime"))
        bid_time = self.convert_epoch_ms_to_datetime(quote_info.get("bidTime"))
        ask_time = self.convert_epoch_ms_to_datetime(quote_info.get("askTime"))
        times = [value for value in (quote_time, bid_time, ask_time) if value is not None]
        timestamp = max(times) if times else datetime.datetime.now(datetime.timezone.utc)
        return _get_quote_class()(
            asset=asset, price=quote_info.get("lastPrice"),
            bid=quote_info.get("bidPrice"), ask=quote_info.get("askPrice"),
            volume=quote_info.get("totalVolume"), timestamp=timestamp,
            quote_time=quote_time, bid_time=bid_time, ask_time=ask_time,
            bid_size=quote_info.get("bidSize"), ask_size=quote_info.get("askSize"),
            change=quote_info.get("netChange"),
            percent_change=quote_info.get("netPercentChangeInDouble"),
            raw_data=asset_quote, symbol_used=symbol,
        )

    def _quote_clock(self) -> float:
        clock = getattr(self, "_schwab_now", None)
        return float(clock()) if callable(clock) else time.monotonic()

    def _request_pacer(self):
        from lumibot.brokers.schwab_request_pacer import pacer_for_client

        return pacer_for_client(self.client)

    def _before_market_data_request(self, label: str) -> None:
        """Honor a Schwab Retry-After and the local request budget before calling Schwab.

        Every market-data endpoint (quotes, option chains, price history) shares
        one cooldown: a 429 on any of them pauses all of them. Raises
        RateLimitedError without sending anything.
        """
        pacer = self._request_pacer()
        pacer.check_cooldown("market_data", now=self._quote_clock(), label=label)
        pacer.acquire("data")

    def _after_market_data_response(self, response, label: str) -> None:
        from lumibot.tools.http_retry_after import RateLimitedError

        status = getattr(response, "status_code", None)
        pacer = self._request_pacer()
        if status == 429:
            wait = pacer.record_429("market_data", response, now=self._quote_clock())
            raise RateLimitedError(
                f"Schwab {label} request failed (status 429, rate limited; retry after {wait:.1f} seconds)",
                retry_after_seconds=wait,
            )
        if status == 200:
            pacer.clear_cooldown("market_data")

    # ----- live quote cache and prefetch -------------------------------------
    # A strategy that asks for the same quotes every iteration pays one request per
    # iteration instead of one per symbol: at the start of each live iteration the
    # executor calls prefetch_quotes(), which fetches every asset asked for in the last
    # LUMIBOT_QUOTE_PREFETCH_ITERATIONS iterations (default 2; never older than 300 s)
    # in as few requests as possible (LUMIBOT_QUOTE_PREFETCH_BATCH symbols each,
    # default 250). get_quote(s) then serve a quote only while it is younger than
    # LUMIBOT_QUOTE_MAX_AGE seconds (default 1.0; 0 turns cache and prefetch off) and
    # set quote.cache_age_seconds. An absent quote (None) is never kept; assets Schwab
    # cannot quote (crypto) never enter the prefetch list.
    _PREFETCH_LIMIT = 1000
    _PREFETCH_WINDOW_SECONDS = 300.0

    @staticmethod
    def _env_seconds(name, default):
        raw = os.environ.get(name)
        if raw is None or not str(raw).strip():
            return float(default)
        try:
            value = float(raw)
        except ValueError:
            return float(default)
        return value if value > 0 and math.isfinite(value) else 0.0

    def _cache_clock(self) -> float:
        return time.monotonic()

    def _quote_state(self):
        state = self.__dict__.get("_quote_cache_state")
        if state is None:
            state = self.__dict__.setdefault(
                "_quote_cache_state", (threading.Lock(), {}, {"wanted": {}, "iteration": 0, "stats": None})
            )
        return state

    def _note_quote_stats(self, served_ages, fetched):
        lock, _, meta = self._quote_state()
        now = self._cache_clock()
        line = None
        with lock:
            stats = meta["stats"]
            if stats is None:
                stats = meta["stats"] = {"since": now, "served": 0, "fetched": 0, "oldest": 0.0}
            stats["served"] += len(served_ages)
            stats["fetched"] += fetched
            if served_ages:
                stats["oldest"] = max(stats["oldest"], max(served_ages))
            if now - stats["since"] >= 60 and (stats["served"] or stats["fetched"]):
                line = (
                    f"[QuoteCache] last {now - stats['since']:.0f} s: {stats['served']} quotes served from memory "
                    f"(oldest {stats['oldest']:.2f} s, limit {self._env_seconds('LUMIBOT_QUOTE_MAX_AGE', 1.0):.2f} s), "
                    f"{stats['fetched']} fetched"
                )
                meta["stats"] = None
        if line:
            logger.info(line)

    def prefetch_quotes(self) -> int:
        """Fetch recently asked-for quotes together; returns the number of requests sent."""
        if self._env_seconds("LUMIBOT_QUOTE_MAX_AGE", 1.0) <= 0 or self.client is None:
            return 0
        keep = max(1, int(self._env_seconds("LUMIBOT_QUOTE_PREFETCH_ITERATIONS", 2.0) or 1))
        lock, _, meta = self._quote_state()
        now = self._cache_clock()
        with lock:
            meta["iteration"] += 1
            iteration = meta["iteration"]
            wanted = meta["wanted"]
            for asset, (asked_at, asked_iteration) in list(wanted.items()):
                if now - asked_at > self._PREFETCH_WINDOW_SECONDS or iteration - asked_iteration > keep:
                    wanted.pop(asset, None)
            assets = list(wanted)[: self._PREFETCH_LIMIT]
        if not assets:
            return 0
        size = min(500, max(1, int(self._env_seconds("LUMIBOT_QUOTE_PREFETCH_BATCH", 250.0) or 250)))
        batches = [assets[i:i + size] for i in range(0, len(assets), size)]
        workers = min(len(batches), max(1, int(self._env_seconds("LUMIBOT_QUOTE_PREFETCH_WORKERS", 2.0) or 1)))

        def fetch(batch):
            try:
                self._fetch_quotes_uncached(batch, batch_size=size)
                return True
            except Exception as exc:
                # Never retried here: the strategy's own get_quote asks again and keeps
                # its normal error contract. Drop the batch so a bad symbol cannot
                # fail every iteration's prefetch.
                with lock:
                    for asset in batch:
                        wanted.pop(asset, None)
                logger.warning(f"[QuoteCache] prefetch of {len(batch)} quotes failed: {exc}")
                return False

        if workers > 1:
            from concurrent.futures import ThreadPoolExecutor

            with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="schwab-quote-prefetch") as executor:
                list(executor.map(fetch, batches))
        else:
            for batch in batches:
                fetch(batch)
        return len(batches)

    def get_quotes(self, assets, quote=None, exchange=None):
        """Quotes for many assets; live, served from memory while younger than LUMIBOT_QUOTE_MAX_AGE.

        See the quote cache notes above. Missing quotes are fetched in batches.
        """
        assets = list(dict.fromkeys(assets))
        if not assets:
            return {}
        for asset in assets:
            if not _schwab_market_symbol(asset):
                raise ValueError("Schwab does not support the requested quote asset")
        max_age = self._env_seconds("LUMIBOT_QUOTE_MAX_AGE", 1.0)
        if max_age <= 0:
            return self._fetch_quotes_uncached(assets)
        lock, cache, meta = self._quote_state()
        now = self._cache_clock()
        result, missing, ages = {}, [], []
        with lock:
            for asset in assets:
                meta["wanted"][asset] = (now, meta["iteration"])
                hit = cache.get(asset)
                if hit is not None and hit[1] is not None and 0 <= now - hit[0] <= max_age:
                    try:
                        hit[1].cache_age_seconds = now - hit[0]
                    except Exception:
                        pass
                    result[asset] = hit[1]
                    ages.append(now - hit[0])
                else:
                    missing.append(asset)
        if missing:
            result.update(self._fetch_quotes_uncached(missing))
        self._note_quote_stats(ages, len(missing))
        return {asset: result.get(asset) for asset in assets}

    def _fetch_quotes_uncached(self, assets, quote=None, exchange=None, batch_size=50):
        """Fetch exact stock/option/future contracts in batches of at most 50.

        Return every requested Asset mapped to a Quote or None when its symbol
        is absent. Preserve recorded quote times and missing bid/ask values.
        No last-price substitution, persistent cache or per-contract retries.
        Provider/shape failures raise; callers must not treat them as no market.
        """
        assets = list(dict.fromkeys(assets))
        if not assets:
            return {}
        symbols = {}
        for asset in assets:
            symbol = _schwab_market_symbol(asset)
            if not symbol:
                raise ValueError("Schwab does not support the requested quote asset")
            symbols[asset] = symbol
        if self.client is None:
            raise ConnectionError("No Schwab client available for quotes")
        result = {}
        # Each batch's quotes are as old as that batch's response, not the last one's:
        # a slow later batch must not make earlier quotes look newly fetched.
        observed_at = {}
        for offset in range(0, len(assets), batch_size):
            batch = assets[offset:offset + batch_size]
            # Schwab may have asked us to wait (HTTP 429 + Retry-After). Fail
            # visibly without another request; never invent or reuse a quote.
            self._before_market_data_request("quote")
            response = self.client.get_quotes([symbols[asset] for asset in batch])
            self._after_market_data_response(response, "quote")
            batch_stamp = self._cache_clock()
            status = getattr(response, "status_code", None)
            if status != 200:
                raise ConnectionError(f"Schwab quote request failed (status {status})")
            payload = response.json()
            if not isinstance(payload, dict):
                raise ValueError("Invalid Schwab quotes response")
            for asset in batch:
                symbol = symbols[asset]
                matches = [key for key in payload if isinstance(key, str) and key.upper() == symbol.upper()]
                if len(matches) > 1:
                    raise ValueError("Ambiguous Schwab quote symbol")
                data = payload[matches[0]] if matches else None
                if data is None:
                    result[asset] = None
                    continue
                if not isinstance(data, dict):
                    raise ValueError("Invalid Schwab quote response")
                if not data:
                    result[asset] = None
                    continue
                reported = data.get("symbol")
                if reported is not None and str(reported).upper() != symbol.upper():
                    raise ValueError("Schwab quote contract does not match the requested asset")
                result[asset] = self._quote_from_response(asset, matches[0], data)
                observed_at[asset] = batch_stamp
        if self._env_seconds("LUMIBOT_QUOTE_MAX_AGE", 1.0) > 0:
            lock, cache, _ = self._quote_state()
            now = self._cache_clock()
            with lock:
                for asset, value in result.items():
                    if value is None:
                        cache.pop(asset, None)
                        continue
                    stamp = observed_at.get(asset, now)
                    try:
                        value.cache_age_seconds = max(0.0, now - stamp)
                    except Exception:
                        pass
                    cache[asset] = (stamp, value)
        return result

    def get_last_prices(self, assets, quote=None, exchange=None):
        """Last prices for many assets with one quote request per 50 symbols.

        The generic implementation asks for one quote per asset. If the batch
        fails (for example HTTP 429), every price is None and the failure is
        logged; the batch is never split into per-symbol retries.
        """
        from lumibot.entities import AssetsMapping

        assets = list(dict.fromkeys(assets))
        result = {asset: None for asset in assets}
        if not assets:
            return AssetsMapping(result)
        supported = [asset for asset in assets if _schwab_market_symbol(asset)]
        if not supported:
            return AssetsMapping(result)
        try:
            quotes = self.get_quotes(supported, quote=quote, exchange=exchange)
        except Exception as exc:
            logger.error(colored(f"Error in get_last_prices for {len(supported)} assets: {exc}", "red"))
            return AssetsMapping(result)
        for asset in supported:
            asset_quote = quotes.get(asset)
            if asset_quote is not None and asset_quote.price is not None:
                result[asset] = float(asset_quote.price)
        return AssetsMapping(result)

    def get_quote(self, asset, quote=None, exchange=None) -> Quote:
        """Get one quote, preserving the existing None-on-error contract."""
        try:
            return self.get_quotes([asset], quote=quote, exchange=exchange)[asset]
        except Exception as exc:
            logger.error(colored(f"Error in get_quote for {getattr(asset, 'symbol', asset)}: {exc}", "red"))
            return None
