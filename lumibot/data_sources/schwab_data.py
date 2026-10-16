from __future__ import annotations

import os

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
    root_symbol = asset.symbol.ljust(6)
    year_str = asset.expiration.strftime("%y")
    month_str = asset.expiration.strftime("%m")
    day_str = asset.expiration.strftime("%d")
    option_type = "C" if str(asset.right).upper() == "CALL" else "P"
    strike_whole = int(asset.strike)
    strike_decimal = int((float(asset.strike) - strike_whole) * 1000)
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

    def get_chains(self, asset: Asset, quote: Asset = None, exchange: str = None, strike_count: int = 100) -> dict:
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
                response = self.client.get_option_chain(**params)
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

        response = self.client.get_option_chain(
            symbol=asset.symbol,
            contract_type=self.client.Options.ContractType.ALL,
            strategy=self.client.Options.Strategy.SINGLE,
            from_date=expiry,
            to_date=expiry,
            include_underlying_quote=False,
        )
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

            # Get price history using the simplified API function
            if timestep_unit == "day":
                response = self.client.get_price_history_every_day(
                    symbol,
                    start_datetime=start_date,
                    end_datetime=end_date,
                    need_extended_hours_data=include_after_hours,
                )
            else:
                # Convert raw frequency integer to Enum if required
                freq_enum = frequency
                try:
                    if isinstance(frequency, int):
                        freq_enum = self.client.PriceHistory.Frequency(frequency)
                except Exception:
                    pass

                response = self.client.get_price_history(
                    symbol=symbol,
                    frequency_type=frequency_type,
                    frequency=freq_enum,
                    start_datetime=start_date,
                    end_datetime=end_date,
                    need_extended_hours_data=include_after_hours
                )

            # Check if the response is a Response object and handle accordingly
            if hasattr(response, 'status_code'):
                # It's a Response object from requests library
                if response.status_code != 200:
                    logger.error(colored(f"Error fetching historical prices for {asset.symbol}: {response.status_code}, {response.text}", "red"))
                    return None

                # Parse the JSON response
                try:
                    data = response.json()
                except ValueError as e:
                    logger.error(colored(f"Invalid JSON in response for {asset.symbol}: {e}", "red"))
                    return None
            else:
                # It's already a dictionary or other data structure
                data = response

            # Check if data contains candles data
            if not data or 'candles' not in data:
                logger.error(colored(f"No candles data found in the response for {asset.symbol}", "red"))
                return None

            candles = data['candles']

            # If no candles were returned, return None
            if not candles or len(candles) == 0:
                logger.warning(colored(f"No price data available for {asset.symbol} in the requested time range", "yellow"))
                return None

            # Convert candles to a DataFrame
            df = pd.DataFrame(candles)

            # Ensure expected columns are present
            expected_columns = ['open', 'high', 'low', 'close', 'volume', 'datetime']
            if not all(col in df.columns for col in expected_columns):
                logger.warning(colored(f"Missing expected columns in response. Got: {df.columns.tolist()}", "yellow"))

            # Set datetime as the index
            if 'datetime' in df.columns:
                df['datetime'] = pd.to_datetime(df['datetime'], unit='ms')
                df.set_index('datetime', inplace=True)

            # Drop any duplicate indices
            df = df[~df.index.duplicated(keep='first')]

            # Ensure index is timezone-aware
            if df.index.tz is None:
                df.index = df.index.tz_localize(LUMIBOT_DEFAULT_TIMEZONE)
            else:
                df.index = df.index.tz_convert(LUMIBOT_DEFAULT_TIMEZONE)

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

    def get_quotes(self, assets, quote=None, exchange=None):
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
        for offset in range(0, len(assets), 50):
            batch = assets[offset:offset + 50]
            response = self.client.get_quotes([symbols[asset] for asset in batch])
            if getattr(response, "status_code", None) != 200:
                raise ConnectionError(f"Schwab quote request failed (status {getattr(response, 'status_code', None)})")
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
        return result

    def get_quote(self, asset, quote=None, exchange=None) -> Quote:
        """Get one quote, preserving the existing None-on-error contract."""
        try:
            return self.get_quotes([asset], quote=quote, exchange=exchange)[asset]
        except Exception as exc:
            logger.error(colored(f"Error in get_quote for {getattr(asset, 'symbol', asset)}: {exc}", "red"))
            return None
