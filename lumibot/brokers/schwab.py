import logging
import os
import json
from typing import Union, List, Optional
import dotenv
import traceback
import re
from datetime import datetime, timedelta
from pytz import timezone

from termcolor import colored
from lumibot.brokers import Broker
from lumibot.entities import Order, Asset, Position
from lumibot.data_sources import SchwabData

# Import Schwab specific libraries
from schwab.auth import easy_client
from schwab.client import Client
from schwab.streaming import StreamClient

class Schwab(Broker):
    """
    Broker implementation for Schwab API.
    
    This class provides the integration with Schwab's trading platform,
    implementing all necessary methods required by the Lumibot framework
    to interact with the broker.
    """

    NAME = "Schwab"

    def __init__(
            self,
            config=None,
            data_source=None,
    ):
        """
        Initialize the Schwab broker.
        
        Parameters
        ----------
        config : dict, optional
            Configuration for the broker
        data_source : DataSource, optional
            The data source to use, defaults to SchwabData if not provided
            
        Raises
        ------
        ValueError
            If required environment variables are missing
        ConnectionError
            If connection to Schwab API fails
        """
        # Load environment variables
        dotenv.load_dotenv()
        
        # Get Schwab API credentials from environment
        api_key = os.environ.get('SCHWAB_API_KEY')
        secret = os.environ.get('SCHWAB_SECRET')
        account_number = os.environ.get('SCHWAB_ACCOUNT_NUMBER')
        
        if not all([api_key, secret, account_number]):
            logging.error(colored("Missing Schwab API credentials. Ensure SCHWAB_API_KEY, SCHWAB_SECRET, and SCHWAB_ACCOUNT_NUMBER are set in .env file.", "red"))
            raise ValueError("Missing Schwab API credentials")
            
        # Get the current folder for token path
        current_folder = os.path.dirname(os.path.realpath(__file__))
        token_path = os.path.join(current_folder, 'token.json')
        
        try:
            # Create Schwab API client
            client = easy_client(api_key, secret, 'https://127.0.0.1:8182', token_path)
            
            # Get account numbers and find the hash value for the specified account number
            response = client.get_account_numbers()
            if response.status_code != 200:
                logging.error(colored(f"Error getting account numbers: {response.status_code}, {response.text}", "red"))
                raise ConnectionError(f"Failed to get account numbers: {response.text}")
            
            accounts = response.json()
            
            # Find the hashValue for the accountNumber
            hash_value = None
            for account in accounts:
                if account['accountNumber'] == account_number:
                    hash_value = account['hashValue']
                    break
                
            if hash_value is None:
                logging.error(colored(f"Could not find account number {account_number}", "red"))
                raise ValueError(f"Could not find account number {account_number}")
                
            # Store the client and account info
            self.client = client
            self.account_number = account_number
            self.hash_value = hash_value
            
            # Initialize stream client but don't connect yet
            self.stream_client = StreamClient(client, account_id=account_number)
            
            # Check if the user has provided a data source, if not, create one and pass the client
            if data_source is None:
                data_source = SchwabData(client=client)
            elif isinstance(data_source, SchwabData) and not hasattr(data_source, 'client'):
                # If SchwabData instance exists but no client is set, set it
                data_source.client = client
                
            logging.info(colored("Successfully initialized Schwab broker connection", "green"))
            
        except Exception as e:
            logging.error(colored(f"Error initializing Schwab broker: {str(e)}", "red"))
            raise
            
        super().__init__(
            name=self.NAME,
            data_source=data_source,
            config=config,
        )

    # Account and balance methods
    def _get_balances_at_broker(self, quote_asset: Asset, strategy) -> tuple:
        """
        Get the actual cash balance at the broker.
        
        Parameters
        ----------
        quote_asset : Asset
            The quote asset to get the balance of (e.g., USD, EUR).
        strategy : Strategy
            The strategy object that is requesting the balance.

        Returns
        -------
        tuple of float
            A tuple containing (cash, positions_value, total_liquidation_value).
            - Cash = cash in the account (whatever the quote asset is).
            - Positions value = the value of all the positions in the account.
            - Portfolio value = the total equity value of the account (aka. portfolio value).
        """
        try:
            # Get account information using the hash_value stored during initialization
            response = self.client.get_account(self.hash_value, fields=[self.client.Account.Fields.POSITIONS])
            
            if response.status_code != 200:
                logging.error(colored(f"Error getting account information: {response.status_code}, {response.text}", "red"))
                raise ConnectionError(f"Failed to get account information: {response.text}")
            
            account_data = response.json()
            
            # Try to use aggregated balance first if available
            if 'aggregatedBalance' in account_data:
                # Use aggregated balance data
                aggregated_balance = account_data['aggregatedBalance']
                portfolio_value = float(aggregated_balance.get('currentLiquidationValue', 0))
                
                # Get cash from securitiesAccount
                securities_account = account_data.get('securitiesAccount', {})
                balances = securities_account.get('currentBalances', {})
                cash = float(balances.get('cashBalance', 0))
            else:
                # Fall back to original implementation
                securities_account = account_data.get('securitiesAccount', {})
                account_type = securities_account.get('type', '')
                
                # Get balances based on account type
                balances = securities_account.get('currentBalances', {})
                if account_type.lower() == 'margin':
                    cash = float(balances.get('cashBalance', 0))
                    portfolio_value = float(balances.get('liquidationValue', 0))
                    if portfolio_value == 0:
                        portfolio_value = float(balances.get('equity', 0))
                else:
                    cash = float(balances.get('cashBalance', 0))
                    portfolio_value = float(balances.get('accountValue', 0))
            
            # Calculate positions value (portfolio value minus cash)
            positions_value = portfolio_value - cash
            
            logging.info(colored(f"Account balances: Cash=${cash:.2f}, Positions=${positions_value:.2f}, Portfolio=${portfolio_value:.2f}", "green"))
            
            return cash, positions_value, portfolio_value
            
        except Exception as e:
            logging.error(colored(f"Error getting balances from Schwab: {str(e)}", "red"))
            logging.error(traceback.format_exc())
            
            # Return default values in case of error
            return 0.0, 0.0, 0.0

    # Position methods
    def _pull_positions(self, strategy: 'Strategy') -> List[Position]:
        """
        Get the account positions. Returns a list of position objects.

        Parameters
        ----------
        strategy : Strategy
            The strategy object to pull the positions for.

        Returns
        -------
        list[Position]
            A list of position objects containing details about each position 
            including asset, quantity, and average fill price.
        
        Notes
        -----
        This method handles various asset types including stocks, options, futures,
        bonds, mutual funds, and cash equivalents. Unknown asset types are skipped
        with an appropriate warning message.
        """
        try:
            # Get account details with positions
            response = self.client.get_account(self.hash_value, fields=[self.client.Account.Fields.POSITIONS])

            if response.status_code != 200:
                logging.error(colored(f"Error fetching positions: {response.status_code}, {response.text}", "red"))
                return []
            
            account_data = response.json()

            # Extract positions
            securities_account = account_data.get('securitiesAccount', {})
            schwab_positions = securities_account.get('positions', [])

            # Create a list of Position objects
            position_objects = []
            for schwab_position in schwab_positions:
                # Extract instrument details
                instrument = schwab_position.get('instrument', {})
                asset_type = instrument.get('assetType', '')
                symbol = instrument.get('symbol', '')

                # Initialize Asset object based on asset type
                asset = None
                if asset_type == 'EQUITY':
                    asset = Asset(
                        symbol=symbol,
                        asset_type=Asset.AssetType.STOCK,
                    )
                elif asset_type == 'OPTION':
                    # Parse option details
                    option_symbol = instrument.get('symbol')
                    option_parts = self._parse_option_symbol(option_symbol)

                    if option_parts is None:
                        logging.error(colored(f"Failed to parse option symbol: {option_symbol}", "red"))
                        continue

                    asset = Asset(
                        symbol=option_parts['underlying'],
                        asset_type=Asset.AssetType.OPTION,
                        expiration=option_parts['expiry_date'],
                        strike=option_parts['strike_price'],
                        right=option_parts['option_type'],
                    )
                elif asset_type == 'FUTURE':
                    asset = Asset(
                        symbol=symbol,
                        asset_type=Asset.AssetType.FUTURE,
                    )
                elif asset_type == 'BOND':
                    asset = Asset(
                        symbol=symbol,
                        asset_type=Asset.AssetType.BOND,
                    )
                elif asset_type == 'MUTUAL_FUND':
                    asset = Asset(
                        symbol=symbol,
                        asset_type=Asset.AssetType.MUTUAL_FUND,
                    )
                elif asset_type == 'COLLECTIVE_INVESTMENT':
                    # Handle ETFs like CQQQ, UPRO as stocks
                    asset = Asset(
                        symbol=symbol,
                        asset_type=Asset.AssetType.STOCK,
                    )
                elif asset_type == 'ETF':
                    # Handle ETFs as stocks
                    logging.info(colored(f"Treating ETF {symbol} as STOCK", "blue"))
                    asset = Asset(
                        symbol=symbol,
                        asset_type=Asset.AssetType.STOCK,
                    )
                elif asset_type in ['CASH_EQUIVALENT', 'MONEY_MARKET_FUND', 'CASH']:
                    # Use FOREX as a representation for cash and cash equivalents
                    asset = Asset(
                        symbol=symbol,
                        asset_type=Asset.AssetType.FOREX,
                    )
                else:
                    # Skip unknown asset types
                    logging.warning(colored(f"Skipping unknown asset type: {asset_type} for symbol: {symbol}", "yellow"))
                    continue

                # Calculate net quantity (long - short)
                long_quantity = schwab_position.get('longQuantity', 0)
                short_quantity = schwab_position.get('shortQuantity', 0)
                net_quantity = long_quantity - short_quantity
                
                # Extract position-specific details
                average_price = schwab_position.get('averagePrice', 0.0)
                market_value = schwab_position.get('marketValue', 0.0)
                unrealized_pnl = schwab_position.get('longOpenProfitLoss', 0.0)  
                if net_quantity < 0:  # Use short PnL if it's a short position
                    unrealized_pnl = schwab_position.get('shortOpenProfitLoss', 0.0)

                # Create Position object with strategy name
                position = Position(
                    strategy.name,
                    asset=asset,
                    quantity=net_quantity,
                    avg_fill_price=average_price,
                )

                # Add the Position object to the list
                position_objects.append(position)
                
            return position_objects

        except Exception as e:
            logging.error(colored(f"Error pulling positions from Schwab: {str(e)}", "red"))
            logging.error(traceback.format_exc())
            return []

    def _pull_position(self, strategy: 'Strategy', asset: Asset) -> Optional[Position]:
        """
        Pull a single position from the broker that matches the asset and strategy.

        Parameters
        ----------
        strategy: Strategy
            The strategy object that placed the order to pull.
        asset: Asset
            The asset to pull the position for.

        Returns
        -------
        Position
            The position object for the asset and strategy if found, otherwise None.
            
        Notes
        -----
        This method compares different attributes based on asset type:
        - For stocks and futures: Compares only the symbol
        - For options: Compares symbol, strike, right, and expiration
        """
        positions = self._pull_positions(strategy)
        
        for position in positions:
            # For stocks, just compare the symbol
            if asset.asset_type == Asset.AssetType.STOCK and position.asset.symbol == asset.symbol:
                return position
            # For options, compare all option details
            elif asset.asset_type == Asset.AssetType.OPTION:
                if (position.asset.symbol == asset.symbol and 
                    position.asset.strike == asset.strike and 
                    position.asset.right == asset.right and 
                    position.asset.expiration == asset.expiration):
                    return position
            # For futures, compare symbol
            elif asset.asset_type == Asset.AssetType.FUTURE and position.asset.symbol == asset.symbol:
                return position
                
        return None

    # Symbol parsing methods
    def _parse_option_symbol(self, option_symbol):
        """
        Parse Schwab option symbol format (e.g., 'SPY   240801P00541000') into its components.
        
        Parameters
        ----------
        option_symbol : str
            The option symbol in Schwab format.
            
        Returns
        -------
        dict
            A dictionary containing the parsed components:
            - 'underlying': The underlying symbol (e.g., 'SPY')
            - 'expiry_date': The expiration date as a datetime.date object
            - 'option_type': The option type ('CALL' or 'PUT')
            - 'strike_price': The strike price as a float
            
        Returns None if parsing failed.
        """
        try:
            # Define the regex pattern for the option symbol
            # Format is: symbol(spaces)YYMMDD(C|P)strike(with padding zeros)
            pattern = r'^(?P<underlying>[A-Z]+)\s+(?P<expiry>\d{6})(?P<type>[CP])(?P<strike>\d{8})$'

            # Match the pattern with the option symbol
            match = re.match(pattern, option_symbol)
            if not match:
                logging.error(colored(f"Invalid option symbol format: {option_symbol}", "red"))
                return None

            # Extract the parts from the regex match groups
            underlying = match.group('underlying').strip()
            expiry = match.group('expiry')
            option_type = match.group('type')
            strike_raw = match.group('strike')

            # Convert expiry date string to a date object
            # Format is YYMMDD, convert to YYYY-MM-DD
            expiry_date = datetime.strptime(expiry, '%y%m%d').date()

            # Convert strike price to a float (divide by 1000 to get actual price)
            strike_price = int(strike_raw) / 1000

            # Map option type to CALL or PUT
            option_type_full = 'CALL' if option_type == 'C' else 'PUT'

            return {
                'underlying': underlying,
                'expiry_date': expiry_date,
                'option_type': option_type_full,
                'strike_price': strike_price
            }
            
        except Exception as e:
            logging.error(colored(f"Error parsing option symbol {option_symbol}: {str(e)}", "red"))
            return None

    # Order methods
    def _pull_broker_all_orders(self) -> list:
        """
        Get the broker's open orders.

        Returns
        -------
        list
            A list of order responses from the broker query. These will be passed to 
            _parse_broker_order() to be converted to Order objects.
            
        Notes
        -----
        This method retrieves orders from the past 7 days by default to limit the
        volume of data returned while still capturing relevant recent orders.
        """
        try:
            # Get orders from last 7 days
            seek_start = datetime.now(timezone('UTC')) - timedelta(days=7)
            
            response = self.client.get_orders_for_account(
                self.hash_value,
                from_entered_datetime=seek_start
            )

            if response.status_code != 200:
                logging.error(colored(f"Error fetching orders: {response.status_code}, {response.text}", "red"))
                return []
            
            schwab_orders = response.json()
            
            return schwab_orders
        
        except Exception as e:
            logging.error(colored(f"Error pulling orders from Schwab: {str(e)}", "red"))
            logging.error(traceback.format_exc())
            return []

    def _pull_broker_order(self, identifier: str) -> dict:
        """
        Get a broker order representation by its id.

        Parameters
        ----------
        identifier : str
            The identifier of the order to pull.

        Returns
        -------
        dict
            The order representation from the broker, or None if not found.
        """
        try:
            response = self.client.get_order_by_id(
                self.hash_value,
                identifier
            )

            if response.status_code != 200:
                logging.error(colored(f"Error fetching order {identifier}: {response.status_code}, {response.text}", "red"))
                return None
            
            return response.json()
        
        except Exception as e:
            logging.error(colored(f"Error pulling order {identifier} from Schwab: {str(e)}", "red"))
            logging.error(traceback.format_exc())
            return None

    def _parse_broker_order(self, response: dict, strategy_name: str, strategy_object: 'Strategy' = None) -> Order:
        """
        Parse a broker order representation to an order object.

        Parameters
        ----------
        response : dict
            The broker order representation, typically from API response.
        strategy_name : str
            The name of the strategy that placed the order.
        strategy_object : Strategy, optional
            The strategy object that placed the order.

        Returns
        -------
        Order
            The order object created from the broker's response, or None if parsing fails.
            
        Notes
        -----
        This method handles complex order structures including:
        - Simple orders (direct conversion to Lumibot orders)
        - OCO (One-Cancels-Other) orders with child orders
        - Other order strategies with child orders
        """
        try:
            # Check if there are child order strategies
            child_order_strategies = response.get("childOrderStrategies", None)
            
            # If there are child order strategies, process them
            if child_order_strategies is not None:
                # Create a list to hold the child order objects
                child_order_objects = []

                # Loop through the childOrderStrategies
                for child_order_strategy in child_order_strategies:
                    child_orders = self._parse_simple_order(child_order_strategy, strategy_name)
                    if child_orders:
                        child_order_objects.extend(child_orders)

                # Check if the orderStrategyType is OCO
                order_strategy_type = response.get("orderStrategyType", None)
                if order_strategy_type == "OCO" and len(child_order_objects) > 0:
                    # Set the order type to OCO
                    oco_order_type = Order.OrderType.OCO

                    # Get the asset object from the child_order_objects
                    asset = child_order_objects[0].asset

                    # Make sure this is the same asset for all the child orders
                    same_asset = True
                    for child_order_object in child_order_objects:
                        if child_order_object.asset != asset:
                            logging.error(colored("ERROR: Asset for all child orders in OCO order is not the same", "red"))
                            same_asset = False
                            break

                    if same_asset:
                        # Create an OCO order (using order_type parameter instead of deprecated type)
                        order = Order(
                            strategy=strategy_name,
                            order_type=oco_order_type,  # Use order_type instead of type
                            asset=asset,  # Include asset parameter
                        )

                        # Set the child orders for the OCO order
                        order.child_orders = child_order_objects
                        return order
                
                # If we get here and have child orders, return the first one
                if child_order_objects:
                    return child_order_objects[0]
            else:
                # Process simple order
                simple_orders = self._parse_simple_order(response, strategy_name)
                if simple_orders:
                    return simple_orders[0]  # Return the first order

            # If we couldn't parse anything, return None
            logging.warning(colored(f"Could not parse any valid orders from response", "yellow"))
            return None

        except Exception as e:
            logging.error(colored(f"Error parsing broker order: {str(e)}", "red"))
            logging.error(traceback.format_exc())
            return None

    def _parse_simple_order(self, schwab_order: dict, strategy_name: str) -> List[Order]:
        """
        Parse a simple Schwab order (non-OCO) into Lumibot Order objects.
        
        Parameters
        ----------
        schwab_order : dict
            The Schwab order data.
        strategy_name : str
            The name of the strategy for which to create the order.
            
        Returns
        -------
        List[Order]
            A list of parsed order objects, or an empty list if parsing fails.
            
        Notes
        -----
        This method handles conversion of:
        - Order types (LIMIT, MARKET, STOP, etc.)
        - Order statuses (NEW, FILLED, CANCELED, etc.)
        - Asset types (STOCK, OPTION, FUTURE, etc.)
        - Order sides (BUY, SELL, BUY_TO_OPEN, etc.)
        
        It also extracts important order details such as:
        - Timestamps (entry and close)
        - Prices (limit price, stop price)
        - Order legs for multi-leg orders
        """
        try:
            # Check order entry/close times
            entered_time = datetime.strptime(schwab_order["enteredTime"], "%Y-%m-%dT%H:%M:%S%z")
            close_time = None
            if "closeTime" in schwab_order:
                close_time = datetime.strptime(schwab_order["closeTime"], "%Y-%m-%dT%H:%M:%S%z")

            # Convert to Lumibot Order type
            order_type_map = {
                "LIMIT": Order.OrderType.LIMIT,
                "MARKET": Order.OrderType.MARKET,
                "STOP": Order.OrderType.STOP,
                "STOP_LIMIT": Order.OrderType.STOP_LIMIT,
                "TRAILING_STOP": Order.OrderType.TRAIL
            }
            
            schwab_order_type = schwab_order.get("orderType", None)
            order_type = order_type_map.get(schwab_order_type)
            
            if not order_type and schwab_order_type == "NET_CREDIT":
                logging.info(colored(f"NET_CREDIT order type not supported: {schwab_order.get('orderId', '')}", "yellow"))
                return []
            elif not order_type:
                logging.error(colored(f"Unknown order type: {schwab_order_type}", "red"))
                return []

            # Convert to Lumibot status
            status_map = {
                "ACCEPTED": Order.OrderStatus.NEW,
                "PENDING_ACTIVATION": Order.OrderStatus.NEW,
                "QUEUED": Order.OrderStatus.NEW,
                "WORKING": Order.OrderStatus.NEW,
                "NEW": Order.OrderStatus.NEW,
                "REJECTED": Order.OrderStatus.ERROR,
                "PENDING_CANCEL": Order.OrderStatus.CANCELED,
                "CANCELED": Order.OrderStatus.CANCELED,
                "PENDING_REPLACE": Order.OrderStatus.CANCELED,
                "REPLACED": Order.OrderStatus.CANCELED,
                "EXPIRED": Order.OrderStatus.CANCELED,
                "FILLED": Order.OrderStatus.FILLED
            }
            
            schwab_order_status = schwab_order.get("status", None)
            status = status_map.get(schwab_order_status)
            
            if not status:
                logging.error(colored(f"Unknown order status: {schwab_order_status}", "red"))
                return []

            # Get the order id
            order_id = schwab_order.get("orderId", None)

            # Get prices
            price = schwab_order.get("price", None)
            stop_price = schwab_order.get("stopPrice", None)

            # Get the schwab legs
            schwab_legs = schwab_order.get("orderLegCollection", [])
            if not schwab_legs:
                logging.error(colored(f"No order legs found for order ID: {order_id}", "red"))
                return []

            # Process each leg as a separate order
            order_objects = []
            for schwab_leg in schwab_legs:
                # Get the asset information
                instrument = schwab_leg.get("instrument", {})
                
                # Get the symbol - prefer underlyingSymbol for options if available
                if "underlyingSymbol" in instrument:
                    symbol = instrument["underlyingSymbol"]
                else:
                    symbol = instrument.get("symbol", "")
                
                if not symbol:
                    logging.error(colored(f"No symbol found for order leg in order ID: {order_id}", "red"))
                    continue

                # Get the quantity
                quantity = schwab_leg.get("quantity", 0)
                if quantity <= 0:
                    logging.error(colored(f"Invalid quantity ({quantity}) for order ID: {order_id}", "red"))
                    continue

                # Convert order side
                side_mapping = {
                    "BUY": Order.OrderSide.BUY,
                    "SELL": Order.OrderSide.SELL,
                    "BUY_TO_COVER": Order.OrderSide.BUY_TO_COVER,
                    "SELL_SHORT": Order.OrderSide.SELL_SHORT,
                    "BUY_TO_OPEN": Order.OrderSide.BUY_TO_OPEN,
                    "BUY_TO_CLOSE": Order.OrderSide.BUY_TO_CLOSE,
                    "SELL_TO_OPEN": Order.OrderSide.SELL_TO_OPEN,
                    "SELL_TO_CLOSE": Order.OrderSide.SELL_TO_CLOSE
                }
                
                instruction = schwab_leg.get("instruction", "")
                side = side_mapping.get(instruction)
                
                if not side:
                    logging.error(colored(f"Unknown instruction: {instruction} for order ID: {order_id}", "red"))
                    continue

                # Determine asset type and create appropriate Asset object
                asset_type_map = {
                    "EQUITY": Asset.AssetType.STOCK,
                    "OPTION": Asset.AssetType.OPTION,
                    "FUTURE": Asset.AssetType.FUTURE,
                    "FOREX": Asset.AssetType.FOREX,
                    "INDEX": Asset.AssetType.INDEX
                }
                
                asset_type_str = schwab_leg.get("orderLegType", "")
                asset_type = asset_type_map.get(asset_type_str)
                
                if not asset_type:
                    logging.error(colored(f"Unknown asset type: {asset_type_str} for order ID: {order_id}", "red"))
                    continue

                # Create appropriate Asset object based on type
                asset = None
                if asset_type == Asset.AssetType.STOCK:
                    asset = Asset(
                        symbol=symbol,
                        asset_type=asset_type,
                    )
                elif asset_type == Asset.AssetType.OPTION:
                    option_symbol = instrument.get("symbol", "")
                    option_parts = self._parse_option_symbol(option_symbol)
                    
                    if not option_parts:
                        logging.error(colored(f"Failed to parse option symbol: {option_symbol} for order ID: {order_id}", "red"))
                        continue
                        
                    asset = Asset(
                        symbol=option_parts["underlying"],
                        asset_type=asset_type,
                        expiration=option_parts["expiry_date"],
                        strike=option_parts["strike_price"],
                        right=option_parts["option_type"],
                    )
                elif asset_type == Asset.AssetType.FUTURE:
                    asset = Asset(
                        symbol=symbol,
                        asset_type=asset_type,
                    )
                else:
                    logging.warning(colored(f"Asset type {asset_type} not fully supported yet for order ID: {order_id}", "yellow"))
                    continue

                # Create order object - using order_type instead of type
                order = Order(
                    strategy=strategy_name,
                    asset=asset,
                    quantity=quantity,
                    side=side,
                    order_type=order_type,  # Changed from type to order_type
                    limit_price=price,
                    stop_price=stop_price,
                    identifier=order_id,
                )

                # Set the status and timestamps
                order.status = status
                order.created_at = entered_time
                order.updated_at = close_time if close_time else entered_time

                order_objects.append(order)

            return order_objects

        except Exception as e:
            logging.error(colored(f"Error parsing simple order: {str(e)}", "red"))
            logging.error(traceback.format_exc())
            return []

    # Unimplemented methods with stubs
    def _get_stream_object(self):
        """
        Get the broker stream connection. This method should return an object that handles 
        the streaming connection to the broker's API.
        
        Returns
        -------
        object
            The stream object that will handle the streaming connection.
        """
        logging.info(colored("Method '_get_stream_object' is not yet implemented.", "yellow"))
        return None

    def _submit_order(self, order: Order) -> Order:
        """
        Submit an order to the broker after necessary checks and input sanitization.
        
        Parameters
        ----------
        order : Order
            The order to submit to the broker.

        Returns
        -------
        Order
            Updated order with broker identifier filled in, or None if submission failed.
        """
        try:
            # Create tag for the order (use strategy name if tag not provided)
            tag = order.tag if order.tag else order.strategy
            
            # Replace any characters that might cause issues
            tag = re.sub(r'[^a-zA-Z0-9-]', '-', tag)
            
            # Get the appropriate limit price based on order type
            order_limit_price = order.limit_price
            if order.order_type == Order.OrderType.STOP_LIMIT:
                order_limit_price = order.stop_limit_price
            
            # Create the appropriate order spec based on asset type and order details
            order_spec = None
            
            # Handle different order types
            if order.is_advanced_order():
                logging.error(colored(f"Advanced orders (OCO/OTO/Bracket) are not yet implemented for Schwab broker.", "red"))
                return None
                
            elif order.asset.asset_type == Asset.AssetType.STOCK:
                # Prepare stock order
                order_spec = self._prepare_stock_order_spec(order, order_limit_price, tag)
                
            elif order.asset.asset_type == Asset.AssetType.OPTION:
                # Prepare option order
                order_spec = self._prepare_option_order_spec(order, order_limit_price, tag)
                
            else:
                logging.error(colored(f"Asset type {order.asset.asset_type} is not supported by Schwab broker.", "red"))
                return None
            
            if not order_spec:
                logging.error(colored(f"Failed to create order specification for {order}", "red"))
                return None
            
            # Log the final order request
            logging.info(colored(f"Sending order request to Schwab: {json.dumps(order_spec, indent=2)}", "cyan"))
            
            # Submit the order to Schwab
            response = self.client.place_order(self.hash_value, order_spec)
            
            # Log the response
            logging.info(colored(f"Schwab place_order response: {response}", "cyan"))
            logging.info(colored(f"Response status code: {response.status_code}", "cyan"))
            if hasattr(response, 'text'):
                logging.info(colored(f"Response text: {response.text}", "cyan"))
            if hasattr(response, 'headers'):
                logging.info(colored(f"Response headers: {response.headers}", "cyan"))
                
            # If we get an error response, extract details and return
            if response.status_code >= 400:
                error_msg = f"Error submitting order: HTTP {response.status_code}"
                if hasattr(response, 'text') and response.text:
                    try:
                        error_data = json.loads(response.text)
                        if 'message' in error_data:
                            error_msg += f" - {error_data['message']}"
                    except:
                        error_msg += f" - {response.text}"
                        
                logging.error(colored(error_msg, "red"))
                
                # Dispatch error event
                self.stream.dispatch(self.ERROR_ORDER, order=order, error_msg=error_msg)
                return None
            
            # Extract order ID from response
            order_id = None
            try:
                # Use the appropriate way to extract the order ID from the response
                if hasattr(response, 'headers') and 'Location' in response.headers:
                    location = response.headers.get('Location', '')
                    order_id = location.split('/')[-1] if '/' in location else location.strip()
                    logging.info(colored(f"Extracted order ID from Location header: {order_id}", "green"))
                elif hasattr(response, 'json') and callable(response.json):
                    try:
                        json_data = response.json()
                        if 'orderId' in json_data:
                            order_id = json_data['orderId']
                            logging.info(colored(f"Extracted order ID from JSON response: {order_id}", "green"))
                    except:
                        pass
                        
                # If still no order ID and we have text, try to use it directly
                if not order_id and hasattr(response, 'text') and response.text and response.text.strip():
                    order_id = response.text.strip()
                    logging.info(colored(f"Using response text as order ID: {order_id}", "green"))
            except Exception as e:
                logging.error(colored(f"Error extracting order ID: {e}", "red"))
            
            if not order_id:
                logging.error(colored(f"Failed to get order ID from response", "red"))
                return None
            
            # Update the order with the identifier
            order.identifier = order_id
            order.status = Order.OrderStatus.SUBMITTED
            
            # Store the raw response data
            order_data = {"id": order_id, "status": "SUBMITTED"}
            order.update_raw(order_data)
            
            # Add to unprocessed orders and dispatch to stream
            self._unprocessed_orders.append(order)
            self.stream.dispatch(self.NEW_ORDER, order=order)
            
            logging.info(colored(f"Successfully submitted order {order_id}", "green"))
            
            return order
            
        except Exception as e:
            error_msg = f"Error submitting order {order}: {str(e)}"
            logging.error(colored(error_msg, "red"))
            logging.error(traceback.format_exc())
            
            # Dispatch error event
            if hasattr(self, 'stream') and hasattr(self.stream, 'dispatch'):
                self.stream.dispatch(self.ERROR_ORDER, order=order, error_msg=error_msg)
            
            return None

    def _prepare_stock_order_spec(self, order, limit_price, tag):
        """
        Prepare the order specification for stock orders.
        
        Parameters
        ----------
        order : Order
            The order to prepare the specification for
        limit_price : float
            The limit price for the order
        tag : str
            The tag to associate with the order
            
        Returns
        -------
        dict
            The order specification for Schwab API
        """
        # Debug the order data
        logging.info(colored(f"Preparing stock order spec for: {order.asset.symbol}, Side: {order.side}, Quantity: {order.quantity}", "cyan"))
        
        # Map order type
        schwab_order_type = self._map_order_type_to_schwab(order.order_type)
        tif = self._map_time_in_force_to_schwab(order.time_in_force)
        side = self._map_side_to_schwab(order.side)
        
        # Create the order spec following Schwab's exact format requirements
        order_spec = {
            "orderType": schwab_order_type,
            "session": "NORMAL",
            "duration": tif,
            "orderStrategyType": "SINGLE",
            "orderLegCollection": [
                {
                    "instruction": side,
                    "quantity": int(order.quantity),  # Ensure quantity is an integer
                    "instrument": {
                        "symbol": order.asset.symbol,
                        "assetType": "EQUITY"
                    }
                }
            ]
        }
        
        # Add client order ID if available
        if tag:
            order_spec["clientId"] = tag
        
        # Add price details based on order type
        if order.order_type in [Order.OrderType.LIMIT, Order.OrderType.STOP_LIMIT]:
            if limit_price is not None:
                order_spec["price"] = str(limit_price)
        
        if order.order_type in [Order.OrderType.STOP, Order.OrderType.STOP_LIMIT]:
            if order.stop_price is not None:
                order_spec["stopPrice"] = str(order.stop_price)
                
        if order.order_type == Order.OrderType.TRAIL:
            if isinstance(order.trail_percent, (int, float)) and order.trail_percent > 0:
                current_price = self.get_last_price(order.asset)
                if current_price:
                    order_spec["stopPriceLinkType"] = "PERCENT"
                    order_spec["stopPriceLinkBasis"] = str(order.trail_percent)
                    order_spec["stopType"] = "TRAILING"
            elif isinstance(order.trail_price, (int, float)) and order.trail_price > 0:
                order_spec["stopPriceLinkType"] = "VALUE"
                order_spec["stopPriceLinkBasis"] = str(order.trail_price)
                order_spec["stopType"] = "TRAILING"
        
        # Log the final order spec for debugging
        logging.info(colored(f"Final stock order spec: {order_spec}", "cyan"))
        
        return order_spec

    def _prepare_option_order_spec(self, order, limit_price, tag):
        """
        Prepare the order specification for option orders.
        
        Parameters
        ----------
        order : Order
            The order to prepare the specification for
        limit_price : float
            The limit price for the order
        tag : str
            The tag to associate with the order
            
        Returns
        -------
        dict
            The order specification for Schwab API
        """
        # Debug the order data
        logging.info(colored(f"Preparing option order spec for: {order.asset.symbol}, Side: {order.side}, Quantity: {order.quantity}", "cyan"))
        
        # Create options symbol in the format required by Schwab
        expiry_date = order.asset.expiration
        expiry_str = expiry_date.strftime("%y%m%d")
        
        # Format the strike price with padding
        strike_padded = f"{int(order.asset.strike * 1000):08d}"
        
        # Create the option symbol (e.g., "SPY   240801P00541000")
        option_type = "C" if order.asset.right == "CALL" else "P"
        option_symbol = f"{order.asset.symbol.ljust(6)}{expiry_str}{option_type}{strike_padded}"
        
        # Determine if opening or closing position
        position = self.get_tracked_position(order.strategy, order.asset)
        position_effect = "OPENING"
        
        if position is not None:
            if (position.quantity > 0 and order.side in [Order.OrderSide.SELL, Order.OrderSide.SELL_TO_CLOSE]) or \
               (position.quantity < 0 and order.side in [Order.OrderSide.BUY, Order.OrderSide.BUY_TO_CLOSE]):
                position_effect = "CLOSING"
                
        # Map order parameters
        schwab_order_type = self._map_order_type_to_schwab(order.order_type)
        tif = self._map_time_in_force_to_schwab(order.time_in_force)
        side = self._map_side_to_schwab(order.side)
        
        # Create the order spec following Schwab's exact format requirements
        order_spec = {
            "orderType": schwab_order_type,
            "session": "NORMAL",
            "duration": tif,
            "orderStrategyType": "SINGLE",
            "orderLegCollection": [
                {
                    "instruction": side,
                    "quantity": int(order.quantity),
                    "instrument": {
                        "symbol": option_symbol,
                        "assetType": "OPTION"
                    },
                    "positionEffect": position_effect
                }
            ]
        }
        
        # Add client order ID if available
        if tag:
            order_spec["clientId"] = tag
        
        # Add price details based on order type
        if order.order_type in [Order.OrderType.LIMIT, Order.OrderType.STOP_LIMIT]:
            if limit_price is not None:
                order_spec["price"] = str(limit_price)
        
        if order.order_type in [Order.OrderType.STOP, Order.OrderType.STOP_LIMIT]:
            if order.stop_price is not None:
                order_spec["stopPrice"] = str(order.stop_price)
                
        if order.order_type == Order.OrderType.TRAIL:
            if isinstance(order.trail_percent, (int, float)) and order.trail_percent > 0:
                order_spec["stopPriceLinkType"] = "PERCENT"
                order_spec["stopPriceLinkBasis"] = str(order.trail_percent)
                order_spec["stopType"] = "TRAILING"
            elif isinstance(order.trail_price, (int, float)) and order.trail_price > 0:
                order_spec["stopPriceLinkType"] = "VALUE"
                order_spec["stopPriceLinkBasis"] = str(order.trail_price)
                order_spec["stopType"] = "TRAILING"
        
        # Log the final order spec for debugging
        logging.info(colored(f"Final option order spec: {order_spec}", "cyan"))
        
        return order_spec

    def cancel_order(self, order: Order) -> None:
        """
        Cancel an order at the broker. Nothing will be done for orders that are already cancelled or filled.
        
        Parameters
        ----------
        order : Order
            The order to cancel.
        """
        # Check if the order is already cancelled or filled
        if order.is_filled() or order.is_canceled():
            return

        if not order.identifier:
            logging.error(colored("Order identifier is not set, unable to cancel order. Did you remember to submit it?", "red"))
            return

        try:
            # Cancel the order
            response = self.client.cancel_order(order.identifier, self.hash_value)
            
            if response.status_code not in [200, 201, 202]:
                logging.error(colored(f"Error cancelling order {order.identifier}: {response.status_code}, {response.text}", "red"))
                return
                
            logging.info(colored(f"Successfully cancelled order {order.identifier}", "green"))
            
            # Update order status
            order.status = Order.OrderStatus.CANCELED
            
            # Dispatch cancel event
            if hasattr(self, 'stream') and hasattr(self.stream, 'dispatch'):
                self.stream.dispatch(self.CANCELED_ORDER, order=order)
                
        except Exception as e:
            logging.error(colored(f"Error cancelling order {order.identifier}: {str(e)}", "red"))
            logging.error(traceback.format_exc())

    def _modify_order(self, order: Order, limit_price: Union[float, None] = None,
                      stop_price: Union[float, None] = None):
        """
        Modify an order at the broker. Nothing will be done for orders that are already cancelled or filled. You are
        only allowed to change the limit price and/or stop price. If you want to change the quantity,
        you must cancel the order and submit a new one.
        
        Parameters
        ----------
        order : Order
            The order to modify.
        limit_price : float, optional
            The new limit price for the order.
        stop_price : float, optional
            The new stop price for the order.
        """
        # Check if the order is already cancelled or filled
        if order.is_filled() or order.is_canceled():
            return

        if not order.identifier:
            logging.error(colored("Order identifier is not set, unable to modify order. Did you remember to submit it?", "red"))
            return
            
        try:
            # Get the original order first to use as base for modification
            original_order_data = self._pull_broker_order(order.identifier)
            
            if not original_order_data:
                logging.error(colored(f"Unable to fetch original order {order.identifier} for modification", "red"))
                return
                
            # Create a new order spec based on the original order
            new_order_spec = self._prepare_replacement_order_spec(order, original_order_data, limit_price, stop_price)
            
            if not new_order_spec:
                logging.error(colored(f"Failed to create replacement order specification for {order}", "red"))
                return
                
            # Replace the order
            response = self.client.replace_order(self.hash_value, order.identifier, new_order_spec)
            
            # Extract new order ID from response (the replaced order will have a new ID)
            new_order_id = None
            try:
                if hasattr(response, 'headers') and 'Location' in response.headers:
                    # Extract order ID from the Location header
                    location = response.headers['Location']
                    new_order_id = location.split('/')[-1]
                elif response.status_code == 200 or response.status_code == 201:
                    # Try to extract from text if possible
                    new_order_id = response.text.strip() if response.text else None
            except Exception as e:
                logging.error(colored(f"Error extracting new order ID: {e}", "red"))
                
            if not new_order_id:
                logging.error(colored(f"Failed to get new order ID after replacement", "red"))
                return
                
            logging.info(colored(f"Successfully modified order {order.identifier}, new order ID: {new_order_id}", "green"))
            
            # Update the order with the new identifier
            order.previous_identifiers = order.previous_identifiers or []
            order.previous_identifiers.append(order.identifier)
            order.identifier = new_order_id
            
            # Update price information
            if limit_price is not None:
                order.limit_price = limit_price
            if stop_price is not None:
                order.stop_price = stop_price
                
            # No need to dispatch any events as the order is still considered the same from Lumibot's perspective
                
        except Exception as e:
            logging.error(colored(f"Error modifying order {order.identifier}: {str(e)}", "red"))
            logging.error(traceback.format_exc())

    def _prepare_replacement_order_spec(self, order, original_order_data, limit_price, stop_price):
        """
        Prepare a replacement order specification for order modification.
        
        Parameters
        ----------
        order : Order
            The order to modify
        original_order_data : dict
            The original order data from the broker
        limit_price : float or None
            The new limit price, or None to keep original
        stop_price : float or None
            The new stop price, or None to keep original
            
        Returns
        -------
        dict
            The replacement order specification
        """
        # This will need to be implemented based on the actual structure of Schwab's order specs
        # For now, let's create a basic implementation
        
        # Start with tag for the order
        tag = order.tag if order.tag else order.strategy
        tag = re.sub(r'[^a-zA-Z0-9-]', '-', tag)
        
        # Use original values for prices if new ones are not provided
        final_limit_price = limit_price if limit_price is not None else order.limit_price
        final_stop_price = stop_price if stop_price is not None else order.stop_price
        
        # Create the replacement order spec based on asset type
        if order.asset.asset_type == Asset.AssetType.STOCK:
            return self._prepare_stock_order_spec(order, final_limit_price, tag)
        elif order.asset.asset_type == Asset.AssetType.OPTION:
            return self._prepare_option_order_spec(order, final_limit_price, tag)
        else:
            logging.error(colored(f"Asset type {order.asset.asset_type} is not supported for order modification", "red"))
            return None

    def get_historical_account_value(self) -> dict:
        """
        Get the historical account value.
        
        Returns
        -------
        dict
            A dictionary containing the historical account value with keys 'hourly' and 'daily'.
        """
        logging.error(colored("Method 'get_historical_account_value' is not yet implemented.", "red"))
        return {"hourly": None, "daily": None}

    def _register_stream_events(self):
        """
        Register callbacks for broker stream events.
        """
        logging.error(colored("Method '_register_stream_events' is not yet implemented.", "red"))
        return None

    def _run_stream(self):
        """
        Start and run the broker's data stream.
        """
        logging.error(colored("Method '_run_stream' is not yet implemented.", "red"))
        return None
