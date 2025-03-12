import logging
from decimal import Decimal
from typing import Union
import datetime
import pytz
from termcolor import colored
import pandas as pd
from datetime import date, timedelta

from lumibot.entities import Asset, Bars, Quote
from lumibot.data_sources import DataSource
from lumibot.tools import parse_timestep_qty_and_unit, get_trading_days
from lumibot import LUMIBOT_DEFAULT_PYTZ, LUMIBOT_DEFAULT_TIMEZONE

class SchwabData(DataSource):
    """
    Data source that connects to the Schwab Broker API.
    """

    MIN_TIMESTEP = "minute"
    SOURCE = "Schwab"

    def __init__(self, client=None):
        """
        Initialize the Schwab data source with a client connection.
        
        Args:
            client: Schwab API client instance
        """
        super().__init__()
        self.client = client
        if client is None:
            logging.warning(colored("SchwabData initialized without client. Methods will not work until a client is provided.", "yellow"))

    def get_chains(self, asset: Asset, quote: Asset = None, exchange: str = None, strike_count: int = 10) -> dict:
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
            logging.error(colored("No Schwab client available for get_chains", "red"))
            return {}
        
        # Initialize chains structure
        chains = {
            "Multiplier": 100,  # Standard option contracts are for 100 shares
            "Exchange": "SMART",  # Default exchange routing
            "Chains": {"CALL": {}, "PUT": {}}
        }
        
        try:
            # To handle body buffer overflow error, we'll process one contract type at a time
            # Determine what StrikeRange values are available - need to check the actual enum values
            # Common values might be: ALL, ITM, OTM, ATM, NTM, SNK, SAK, SBK
            try:
                # Try to find a suitable strike range value
                strike_range = None
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
                    # Fallback to all strikes if we can't find a near-the-money option
                    strike_range = self.client.Options.StrikeRange.ALL
                    
                logging.info(colored(f"Using strike range: {strike_range}", "blue"))
                
            except Exception as e:
                logging.warning(colored(f"Error finding strike range options: {e}. Using None.", "yellow"))
                strike_range = None
                
            # Get call options first
            logging.info(colored(f"Fetching CALL options for {asset.symbol}", "blue"))
            call_params = {
                "symbol": asset.symbol,
                "contract_type": self.client.Options.ContractType.CALL,
                "strategy": self.client.Options.Strategy.SINGLE,
                "include_underlying_quote": False,  # Skip underlying quote to reduce response size
                "strike_count": strike_count
            }
            
            # Only add strike_range if a valid one was found
            if strike_range is not None:
                call_params["strike_range"] = strike_range
                
            call_response = self.client.get_option_chain(**call_params)
            
            # Process call options response
            if call_response:
                if hasattr(call_response, 'status_code'):
                    # It's a Response object
                    if call_response.status_code == 200:
                        call_data = call_response.json()
                    else:
                        logging.error(colored(f"Error fetching CALL options for {asset.symbol}: {call_response.status_code}", "red"))
                        call_data = {}
                else:
                    call_data = call_response
                    
                # Extract call option data
                if call_data and 'callExpDateMap' in call_data:
                    call_dates = call_data.get('callExpDateMap', {})
                    for exp_date_str, strikes_data in call_dates.items():
                        # Format the expiration date (assumed format: YYYY-MM-DD:days_to_expiry)
                        exp_date = exp_date_str.split(':')[0]  # Extract just the date part
                        
                        # Initialize list to store strikes for this expiration
                        chains["Chains"]["CALL"][exp_date] = []
                        
                        # Add all available strikes for this expiration date
                        for strike_str, strike_data in strikes_data.items():
                            strike = float(strike_str)
                            chains["Chains"]["CALL"][exp_date].append(strike)
                            
                        # Sort the strikes in ascending order
                        chains["Chains"]["CALL"][exp_date].sort()
                        
                    # If underlying data is available, extract additional information
                    if 'underlying' in call_data:
                        underlying = call_data.get('underlying', {})
                        if underlying:
                            # If available, update the multiplier from the API response
                            multiplier = underlying.get('multiplier')
                            if multiplier:
                                chains["Multiplier"] = int(multiplier)
                                
                            # Get exchange information if available
                            exchange_name = underlying.get('exchange')
                            if exchange_name:
                                chains["Exchange"] = exchange_name
            
            # Now get put options
            logging.info(colored(f"Fetching PUT options for {asset.symbol}", "blue"))
            put_params = {
                "symbol": asset.symbol,
                "contract_type": self.client.Options.ContractType.PUT,
                "strategy": self.client.Options.Strategy.SINGLE,
                "include_underlying_quote": False,
                "strike_count": strike_count
            }
            
            # Only add strike_range if a valid one was found
            if strike_range is not None:
                put_params["strike_range"] = strike_range
                
            put_response = self.client.get_option_chain(**put_params)
            
            # Process put options response
            if put_response:
                if hasattr(put_response, 'status_code'):
                    # It's a Response object
                    if put_response.status_code == 200:
                        put_data = put_response.json()
                    else:
                        logging.error(colored(f"Error fetching PUT options for {asset.symbol}: {put_response.status_code}", "red"))
                        put_data = {}
                else:
                    put_data = put_response
                    
                # Extract put option data
                if put_data and 'putExpDateMap' in put_data:
                    put_dates = put_data.get('putExpDateMap', {})
                    for exp_date_str, strikes_data in put_dates.items():
                        # Format the expiration date
                        exp_date = exp_date_str.split(':')[0]  # Extract just the date part
                        
                        # Initialize list to store strikes for this expiration
                        chains["Chains"]["PUT"][exp_date] = []
                        
                        # Add all available strikes for this expiration date
                        for strike_str, strike_data in strikes_data.items():
                            strike = float(strike_str)
                            chains["Chains"]["PUT"][exp_date].append(strike)
                            
                        # Sort the strikes in ascending order
                        chains["Chains"]["PUT"][exp_date].sort()
                        
                    # If we didn't get underlying info from calls, try to get from puts
                    if 'underlying' in put_data and not chains.get("Exchange"):
                        underlying = put_data.get('underlying', {})
                        if underlying:
                            multiplier = underlying.get('multiplier')
                            if multiplier:
                                chains["Multiplier"] = int(multiplier)
                                
                            exchange_name = underlying.get('exchange')
                            if exchange_name:
                                chains["Exchange"] = exchange_name
            
            # Check if we got any data
            if not chains["Chains"]["CALL"] and not chains["Chains"]["PUT"]:
                # If we got no data with the strike range, try again with ALL strikes
                if strike_range is not None and strike_range != self.client.Options.StrikeRange.ALL:
                    logging.warning(colored(f"No option data found with current strike range. Trying with ALL strikes...", "yellow"))
                    return self.get_chains(asset, quote, exchange, strike_count)
                else:
                    logging.error(colored(f"No option data found for {asset.symbol}", "red"))
                    return {}
            
            return chains
            
        except Exception as e:
            logging.error(colored(f"Error getting option chains for {asset.symbol}: {str(e)}", "red"))
            return {}

    def convert_timestep_str_to_timedelta(self, timestep_str):
        """
        Convert a timestep string to a timedelta object.
        
        Args:
            timestep_str: String representing the timestep (e.g., '1minute', '1day')
            
        Returns:
            tuple: (timedelta object, timestep_unit string)
        """
        qty, unit = parse_timestep_qty_and_unit(timestep_str)
        
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
            logging.warning(colored(f"Unknown timestep unit {unit}. Using 'day' as default.", "yellow"))
            return timedelta(days=qty), "day"

    def get_historical_prices(
        self, asset, length, timestep="", timeshift=None, quote=None, exchange=None, include_after_hours=True
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
            logging.error(colored("No Schwab client available for get_historical_prices", "red"))
            return None
            
        # According to the documentation, Schwab doesn't provide price history for futures
        if asset.asset_type == "future" or asset.asset_type == "option":
            logging.error(colored(f"Schwab doesn't provide price history for {asset.asset_type}s", "red"))
            return None

        # Use default timestep if not provided
        timestep = timestep if timestep else self.MIN_TIMESTEP

        # Parse the timestep
        timestep_qty, timestep_unit = parse_timestep_qty_and_unit(timestep)

        # Calculate end date in Eastern time
        end_date = datetime.datetime.now()
        eastern = LUMIBOT_DEFAULT_PYTZ
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
                trading_days = get_trading_days(market='NYSE', start_date=tcal_start_date, end_date=end_date)
                # Filter out trading days when the market_open is after the end_date
                trading_days = trading_days[trading_days['market_open'] < end_date]
                # Now, start_date is the length bars before the last trading day
                if len(trading_days) >= length:
                    start_date = trading_days.index[-length]
            except Exception as e:
                logging.warning(colored(f"Could not calculate trading days, using calendar days instead: {e}", "yellow"))

        try:
            # Based on the Schwab documentation, we need to use the appropriate helper method 
            # or adjust parameters for get_price_history
            
            symbol = asset.symbol
            
            # Based on timestep_unit and timestep_qty, choose the appropriate method
            if timestep_unit == "minute":
                if timestep_qty == 1:
                    response = self.client.get_price_history_every_minute(
                        symbol=symbol,
                        start_datetime=start_date,
                        end_datetime=end_date,
                        need_extended_hours_data=include_after_hours
                    )
                elif timestep_qty == 5:
                    response = self.client.get_price_history_every_five_minutes(
                        symbol=symbol,
                        start_datetime=start_date,
                        end_datetime=end_date,
                        need_extended_hours_data=include_after_hours
                    )
                elif timestep_qty == 10:
                    response = self.client.get_price_history_every_ten_minutes(
                        symbol=symbol,
                        start_datetime=start_date,
                        end_datetime=end_date,
                        need_extended_hours_data=include_after_hours
                    )
                elif timestep_qty == 15:
                    response = self.client.get_price_history_every_fifteen_minutes(
                        symbol=symbol,
                        start_datetime=start_date,
                        end_datetime=end_date,
                        need_extended_hours_data=include_after_hours
                    )
                elif timestep_qty == 30:
                    response = self.client.get_price_history_every_thirty_minutes(
                        symbol=symbol,
                        start_datetime=start_date,
                        end_datetime=end_date,
                        need_extended_hours_data=include_after_hours
                    )
                else:
                    # For non-standard minute values, use the generic method
                    # Note: This might not work as expected since Schwab has specific frequency values
                    logging.warning(colored(f"Non-standard minute frequency: {timestep_qty}. Using generic method.", "yellow"))
                    response = self.client.get_price_history(
                        symbol=symbol,
                        frequency_type='minute',
                        frequency=min(30, timestep_qty),  # Use closest supported frequency
                        start_datetime=start_date,
                        end_datetime=end_date,
                        need_extended_hours_data=include_after_hours
                    )
            elif timestep_unit == "day":
                response = self.client.get_price_history_every_day(
                    symbol=symbol,
                    start_datetime=start_date,
                    end_datetime=end_date,
                    need_extended_hours_data=include_after_hours
                )
            elif timestep_unit == "week":
                response = self.client.get_price_history_every_week(
                    symbol=symbol,
                    start_datetime=start_date,
                    end_datetime=end_date,
                    need_extended_hours_data=include_after_hours
                )
            else:
                # For other timesteps, use the generic method with best guess parameters
                logging.warning(colored(f"Timestep unit '{timestep_unit}' not directly supported. Using generic method.", "yellow"))
                
                # Map timestep to Schwab frequency types
                if timestep_unit == "hour":
                    frequency_type = 'minute'
                    frequency = 60  # 60 minutes = 1 hour
                elif timestep_unit == "month":
                    frequency_type = 'monthly'
                    frequency = 1
                else:
                    frequency_type = 'daily'  # Default to daily
                    frequency = 1
                    
                response = self.client.get_price_history(
                    symbol=symbol,
                    frequency_type=frequency_type,
                    frequency=frequency,
                    start_datetime=start_date,
                    end_datetime=end_date,
                    need_extended_hours_data=include_after_hours
                )
            
            # Check if the response is a Response object and handle accordingly
            if hasattr(response, 'status_code'):
                # It's a Response object from requests library
                if response.status_code != 200:
                    logging.error(colored(f"Error fetching historical prices for {symbol}: {response.status_code}, {response.text}", "red"))
                    return None
                    
                # Parse the JSON response
                try:
                    data = response.json()
                except ValueError as e:
                    logging.error(colored(f"Invalid JSON in response for {symbol}: {e}", "red"))
                    return None
            else:
                # It's already a dictionary or other data structure
                data = response
                
            # Check if data contains candles data
            if not data or 'candles' not in data:
                logging.error(colored(f"No candles data found in the response for {symbol}", "red"))
                return None
                
            candles = data['candles']
            
            # If no candles were returned, return None
            if not candles or len(candles) == 0:
                logging.warning(colored(f"No price data available for {symbol} in the requested time range", "yellow"))
                return None
            
            # Convert candles to a DataFrame
            df = pd.DataFrame(candles)
            
            # Ensure expected columns are present
            expected_columns = ['open', 'high', 'low', 'close', 'volume', 'datetime']
            if not all(col in df.columns for col in expected_columns):
                logging.warning(colored(f"Missing expected columns in response. Got: {df.columns.tolist()}", "yellow"))
            
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
            bars = Bars(df, self.SOURCE, asset, raw=df, quote=quote)
            return bars
            
        except Exception as e:
            logging.error(colored(f"Error getting historical prices for {asset.symbol}: {str(e)}", "red"))
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
            logging.error(colored("No Schwab client available for get_last_price", "red"))
            return None
            
        try:
            # Get the quote for the asset
            asset_quote = self.get_quote(asset, quote, exchange)
            
            # If we have a valid quote with a price, return it
            if asset_quote and asset_quote.price is not None:
                return float(asset_quote.price)
                
            logging.warning(colored(f"Could not find last price for {asset.symbol}", "yellow"))
            return None
            
        except Exception as e:
            logging.error(colored(f"Error in get_last_price for {asset.symbol}: {str(e)}", "red"))
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
            logging.error(colored(f"Error converting timestamp: {e}", "red"))
            return None
        
    def get_quote(self, asset, quote=None, exchange=None) -> Quote:
        """
        This function returns the quote of an asset as a Quote object.
        
        Parameters
        ----------
        asset: Asset
            The asset to get the quote for
        quote: Asset
            The quote asset to get the quote for (not currently used for Schwab)
        exchange: str
            The exchange to get the quote for (not currently used for Schwab)

        Returns
        -------
        Quote
            Quote object containing detailed information about the asset
        """
        if self.client is None:
            logging.error(colored("No Schwab client available for get_quote", "red"))
            return None
            
        try:
            # Format the symbol according to asset type
            if asset.asset_type == Asset.AssetType.OPTION:
                # For options, construct symbol in Schwab's format: RRRRRRYYMMDDsWWWWWddd
                # Where R is space-filled root symbol, YY is year, MM is month, DD is day,
                # s is side (C/P for call/put), WWWWW is whole strike price, nnn is decimal portion
                
                # Get the root symbol, pad with spaces to 6 characters
                root_symbol = asset.symbol.ljust(6)
                
                # Format date portions
                year_str = asset.expiration.strftime('%y')  # 2-digit year
                month_str = asset.expiration.strftime('%m')  # 2-digit month
                day_str = asset.expiration.strftime('%d')    # 2-digit day
                
                # Determine option type (C for call, P for put)
                option_type = 'C' if asset.right.upper() == 'CALL' else 'P'
                
                # Format strike price (whole and decimal parts)
                strike_whole = int(asset.strike)
                strike_decimal = int((asset.strike - strike_whole) * 1000)  # Get 3 decimal digits
                
                # Construct the full option symbol
                symbol = f"{root_symbol}{year_str}{month_str}{day_str}{option_type}{strike_whole:05d}{strike_decimal:03d}"
                                
            elif asset.asset_type == Asset.AssetType.FUTURE:
                # For futures, add a slash prefix if not already present
                symbol = asset.symbol if asset.symbol.startswith('/') else f"/{asset.symbol}"
            elif asset.asset_type == Asset.AssetType.STOCK:
                # For stocks, ETFs, etc. use the symbol directly
                symbol = asset.symbol
            else:
                # For stocks, ETFs, etc. use the symbol directly
                symbol = asset.symbol
            
            # Get quotes from Schwab API
            response = self.client.get_quotes([symbol])
            
            # Check for valid response
            if not hasattr(response, 'status_code'):
                logging.error(colored(f"Unexpected response type from get_quotes: {type(response)}", "red"))
                return None
                
            if response.status_code != 200:
                logging.error(colored(f"Error fetching quote for {symbol}: {response.status_code}, {response.text}", "red"))
                return None
            
            # Parse response JSON
            quote_data = response.json()
            
            # Get quote for our symbol
            asset_quote = quote_data.get(symbol)
            
            if not asset_quote:
                # Try case-insensitive match
                for key in quote_data:
                    if key.upper() == symbol.upper():
                        asset_quote = quote_data[key]
                        symbol = key  # Update symbol to the matched key
                        break
            
            if not asset_quote:
                logging.warning(colored(f"No quote data found for {symbol}", "yellow"))
                return None
            
            # Extract quote information
            quote_info = asset_quote.get('quote', {})
            
            # Get timestamps as datetime objects
            quote_time = self.convert_epoch_ms_to_datetime(quote_info.get('quoteTime'))
            bid_time = self.convert_epoch_ms_to_datetime(quote_info.get('bidTime'))
            ask_time = self.convert_epoch_ms_to_datetime(quote_info.get('askTime'))
            
            # Use most recent timestamp as the main quote timestamp
            timestamp = quote_time
            if bid_time and (not timestamp or bid_time > timestamp):
                timestamp = bid_time
            if ask_time and (not timestamp or ask_time > timestamp):
                timestamp = ask_time
            
            # If we still don't have a timestamp, use current time
            if not timestamp:
                timestamp = datetime.datetime.now(datetime.timezone.utc)
            
            # Extract additional useful fields that might be available
            bid_size = quote_info.get('bidSize')
            ask_size = quote_info.get('askSize')
            last_price = quote_info.get('lastPrice')
            bid_price = quote_info.get('bidPrice')
            ask_price = quote_info.get('askPrice')
            volume = quote_info.get('totalVolume')
            change = quote_info.get('netChange')
            percent_change = quote_info.get('netPercentChangeInDouble')
            
            # Create and return Quote object
            return Quote(
                asset=asset,
                price=last_price,
                bid=bid_price,
                ask=ask_price,
                volume=volume,
                timestamp=timestamp,
                quote_time=quote_time,
                bid_time=bid_time,
                ask_time=ask_time,
                bid_size=bid_size,
                ask_size=ask_size,
                change=change,
                percent_change=percent_change,
                raw_data=asset_quote,
                symbol_used=symbol
            )
            
        except Exception as e:
            logging.error(colored(f"Error in get_quote for {asset.symbol}: {str(e)}", "red"))
            return None
