import logging
import requests
import json
from typing import Union
from datetime import datetime

from termcolor import colored
from lumibot.brokers import Broker
from lumibot.entities import Asset, Order, Position
from lumibot.data_sources import TradovateData

# Set up module-specific logger for enhanced logging
logger = logging.getLogger(__name__)

class TradovateAPIError(Exception):
    """Exception raised for errors in the Tradovate API."""
    def __init__(self, message, status_code=None, response_text=None, original_exception=None):
        self.status_code = status_code
        self.response_text = response_text
        self.original_exception = original_exception
        super().__init__(message)

class Tradovate(Broker):
    """
    Tradovate broker that implements connection to the Tradovate API.
    """
    NAME = "Tradovate"

    def __init__(self, config=None, data_source=None):
        if config is None:
            config = {}
        
        is_paper = config.get("IS_PAPER", True)
        self.trading_api_url = "https://demo.tradovateapi.com/v1" if is_paper else "https://live.tradovateapi.com/v1"
        self.market_data_url = config.get("MD_URL", "https://md.tradovateapi.com/v1")
        self.username = config.get("USERNAME")
        self.password = config.get("DEDICATED_PASSWORD")
        self.app_id = config.get("APP_ID", "Lumibot")
        self.app_version = config.get("APP_VERSION", "1.0")
        self.cid = config.get("CID")
        self.sec = config.get("SECRET")

        # Authenticate and get tokens before creating data_source
        try:
            tokens = self._get_tokens()
            self.trading_token = tokens["accessToken"]
            self.market_token = tokens["marketToken"]
            self.has_market_data = tokens["hasMarketData"]
            logging.info(colored("Successfully acquired tokens from Tradovate.", "green"))
            
            # Now create the data source with the tokens if it wasn't provided
            if data_source is None:
                # Update config with API URLs for consistency
                config["TRADING_API_URL"] = self.trading_api_url
                config["MD_URL"] = self.market_data_url
                data_source = TradovateData(
                    config=config,
                    trading_token=self.trading_token,
                    market_token=self.market_token
                )
            
            super().__init__(name=self.NAME, data_source=data_source, config=config)
            
            account_info = self._get_account_info(self.trading_token)
            self.account_spec = account_info["accountSpec"]
            self.account_id = account_info["accountId"]
            logging.info(colored(f"Account Info: {account_info}", "green"))

            self.user_id = self._get_user_info(self.trading_token)
            logging.info(colored(f"User ID: {self.user_id}", "green"))
            
        except TradovateAPIError as e:
            logger.error(colored(f"Failed to connect to Tradovate: {e}", "red"))
            raise e

    def _get_headers(self, with_auth=True, with_content_type=False):
        """
        Create standard headers for API requests.
        
        Parameters
        ----------
        with_auth : bool
            Whether to include the Authorization header with the trading token
        with_content_type : bool
            Whether to include Content-Type header for JSON requests
            
        Returns
        -------
        dict
            Dictionary of headers for API requests
        """
        headers = {"Accept": "application/json"}
        if with_auth:
            headers["Authorization"] = f"Bearer {self.trading_token}"
        if with_content_type:
            headers["Content-Type"] = "application/json"
        return headers

    def _get_tokens(self):
        """
        Authenticate with Tradovate and obtain the access tokens.
        """
        url = f"{self.trading_api_url}/auth/accesstokenrequest"
        
        payload = {
            "name": self.username,
            "password": self.password,
            "appId": self.app_id,
            "appVersion": "1.0.0",
            "cid": self.cid,
            "sec": self.sec,
        }
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        try:
            response = requests.post(url, json=payload, headers=headers, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            # Check for authentication errors first
            if "errorText" in data:
                error_text = data["errorText"]
                raise TradovateAPIError(f"Tradovate authentication failed: {error_text}")
            
            # Check if CAPTCHA is required
            if data.get("p-captcha"):
                p_time = data.get("p-time", 0)
                p_ticket = data.get("p-ticket", "")
                
                # p-time is in minutes from Tradovate API
                time_unit = "minutes" if p_time != 1 else "minute"
                
                # Determine correct web login URL
                web_url = "https://tradovate.com/"
                
                raise TradovateAPIError(
                    f"Tradovate API is rate limiting login attempts. "
                    f"Please wait {p_time} {time_unit} before trying again, "
                    f"or log into your Tradovate account through the web interface "
                    f"({web_url}) to clear the restriction immediately."
                )
            
            access_token = data.get("accessToken")
            market_token = data.get("mdAccessToken")
            has_market_data = data.get("hasMarketData", False)
            
            if not access_token or not market_token:
                raise TradovateAPIError("Authentication succeeded but tokens are missing.")
            return {"accessToken": access_token, "marketToken": market_token, "hasMarketData": has_market_data}
        except requests.exceptions.RequestException as e:
            raise TradovateAPIError(f"Authentication failed", 
                                     status_code=getattr(e.response, 'status_code', None), 
                                     response_text=getattr(e.response, 'text', None), 
                                     original_exception=e)

    def _get_account_info(self, trading_token):
        """
        Retrieve account information from Tradovate.
        """
        url = f"{self.trading_api_url}/account/list"
        headers = self._get_headers()
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            accounts = response.json()
            if isinstance(accounts, list) and accounts:
                account = accounts[0]
                return {"accountSpec": account.get("name"), "accountId": account.get("id")}
            else:
                raise TradovateAPIError("No accounts found in the account list response.")
        except requests.exceptions.RequestException as e:
            raise TradovateAPIError(f"Failed to retrieve account list", 
                                     status_code=getattr(e.response, 'status_code', None), 
                                     response_text=getattr(e.response, 'text', None), 
                                     original_exception=e)

    def _get_user_info(self, trading_token):
        """
        Retrieve user information from Tradovate.
        """
        url = f"{self.trading_api_url}/user/list"
        headers = self._get_headers()
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            users = response.json()
            if isinstance(users, list) and users:
                user = users[0]
                return user.get("id")
            else:
                raise TradovateAPIError("No users found in the user list response.")
        except requests.exceptions.RequestException as e:
            raise TradovateAPIError(f"Failed to retrieve user list", 
                                     status_code=getattr(e.response, 'status_code', None), 
                                     response_text=getattr(e.response, 'text', None), 
                                     original_exception=e)

    def _resolve_tradovate_futures_symbol(self, asset) -> str:
        """
        Resolve continuous futures to Tradovate-specific contract format.
        Tradovate uses 1-digit years (e.g., MNQU5 not MNQU25).
        
        Parameters
        ----------
        asset : Asset
            The continuous futures asset to resolve
            
        Returns
        -------
        str
            Tradovate-specific futures contract symbol
        """
        from datetime import datetime
        
        month_codes = {
            1: 'F', 2: 'G', 3: 'H', 4: 'J', 5: 'K', 6: 'M',
            7: 'N', 8: 'Q', 9: 'U', 10: 'V', 11: 'X', 12: 'Z'
        }
        
        now = datetime.now()
        current_month = now.month
        current_year = now.year
        
        # Use quarterly contracts (Mar, Jun, Sep, Dec) which are typically most liquid
        if current_month >= 10:  # October onwards, use December
            target_month = 12  # December
            target_year = current_year
        elif current_month >= 7:  # July-September, use September
            target_month = 9  # September
            target_year = current_year
        elif current_month >= 4:  # April-June, use September
            target_month = 9  # September
            target_year = current_year
        elif current_month >= 1:  # Jan-March, use June
            target_month = 6  # June
            target_year = current_year
        else:  # December (fallback), use March next year
            target_month = 3  # March
            target_year = current_year + 1
        
        month_code = month_codes.get(target_month, 'U')  # Default to September
        
        # Tradovate uses 1-digit year format (e.g., 5 for 2025)
        year_code = target_year % 10
        
        contract = f"{asset.symbol}{month_code}{year_code}"
        return contract

    def _get_contract_details(self, contract_id: int) -> dict:
        """
        Retrieve contract details for a given contract id from Tradeovate using the /contract/item endpoint.
        
        Endpoint: GET /contract/item?id=<contract_id>
        Response Schema: { "id": int, "name": string, "contractMaturityId": int }
        """
        url = f"{self.trading_api_url}/contract/item"
        params = {"id": contract_id}
        headers = self._get_headers()
        try:
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            raise TradovateAPIError(f"Failed to retrieve contract details for contract {contract_id}",
                                     status_code=getattr(e.response, 'status_code', None), 
                                     response_text=getattr(e.response, 'text', None),
                                     original_exception=e)

    def _get_balances_at_broker(self, quote_asset: Asset, strategy) -> tuple:
        """
        Retrieve the account financial snapshot from Tradeovate and compute:
          - Cash balance (totalCashValue)
          - Positions value (netLiq - totalCashValue)
          - Portfolio value (netLiq)
        """
        url = f"{self.trading_api_url}/cashBalance/getcashbalancesnapshot"
        headers = self._get_headers(with_content_type=True)
        payload = {"accountId": self.account_id}
        try:
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()
            cash_balance = data.get("totalCashValue")
            net_liq = data.get("netLiq")
            if cash_balance is None or net_liq is None:
                raise TradovateAPIError("Missing totalCashValue or netLiq in account financials response.")
            positions_value = net_liq - cash_balance
            portfolio_value = net_liq
            return cash_balance, positions_value, portfolio_value
        except requests.exceptions.RequestException as e:
            raise TradovateAPIError(f"Failed to retrieve account financials", 
                                     status_code=getattr(e.response, 'status_code', None),
                                     response_text=getattr(e.response, 'text', None),
                                     original_exception=e)

    def _get_stream_object(self):
        logging.info(colored("Method '_get_stream_object' is not yet implemented.", "yellow"))
        return None  # Return None as a placeholder

    def _parse_broker_order(self, response: dict, strategy_name: str, strategy_object=None) -> Order:
        """
        Convert a Tradeovate order dictionary into a Lumibot Order object.
        
        Expected Tradeovate fields:
        - id: order id
        - contractId: used to get asset details (for futures, asset_type is "future")
        - orderQty: the quantity
        - action: "Buy" or "Sell" (will be normalized to lowercase)
        - ordStatus: order status; possible values include "Working", "Filled", "PartialFill",
                    "Canceled", "Rejected", "Expired", "Submitted", etc.
        - timestamp: an ISO timestamp string (with a trailing 'Z' for UTC)
        - orderType, price, stopPrice: if provided
        
        This function retrieves contract details (using _get_contract_details) to create an Asset,
        maps raw statuses to Lumibot's expected statuses, converts the timestamp into a datetime object,
        and creates the Order. The quote is set to USD.
        """
        try:
            order_id = response.get("id")
            contract_id = response.get("contractId")
            asset = None
            if contract_id:
                try:
                    contract_details = self._get_contract_details(contract_id)
                    # For Tradeovate futures, assume asset_type is "future" and use the contract's name as the symbol.
                    symbol = contract_details.get("name", "")
                    asset = Asset(symbol=symbol, asset_type=Asset.AssetType.FUTURE)
                except TradovateAPIError as e:
                    logging.error(colored(f"Failed to retrieve contract details for order {order_id}: {e}", "red"))
            
            quantity = response.get("orderQty", 0)
            action = response.get("action", "").lower()
            order_type = response.get("orderType", "market").lower()
            limit_price = response.get("price")
            stop_price = response.get("stopPrice")
            
            # Map raw status to Lumibot's order status using common aliases.
            raw_status = response.get("ordStatus", "").lower()
            if raw_status in ["working"]:
                status = Order.OrderStatus.OPEN
            elif raw_status in ["filled"]:
                status = Order.OrderStatus.FILLED
            elif raw_status in ["partialfill", "partial_fill", "partially_filled"]:
                status = Order.OrderStatus.PARTIALLY_FILLED
            elif raw_status in ["canceled", "cancelled", "cancel"]:
                status = Order.OrderStatus.CANCELED
            elif raw_status in ["rejected"]:
                status = Order.OrderStatus.ERROR
            elif raw_status in ["expired"]:
                status = Order.OrderStatus.CANCELED
            elif raw_status in ["submitted", "new", "pending"]:
                status = Order.OrderStatus.NEW
            else:
                status = raw_status

            timestamp_str = response.get("timestamp")
            date_created = None
            if timestamp_str:
                # Replace the trailing 'Z' with '+00:00' to properly parse UTC time.
                date_created = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
            
            # Create the Lumibot Order. For unknown fields, we simply leave them out.
            order_obj = Order(
                strategy=strategy_name,
                asset=asset,
                quantity=quantity,
                side=action,
                order_type=order_type,  # Fixed: use order_type instead of deprecated 'type'
                identifier=order_id,
                quote=Asset("USD", asset_type=Asset.AssetType.FOREX)
            )
            order_obj.status = status
            return order_obj
        except Exception as e:
            logger.error(colored(f"Error parsing order: {e}", "red"))
            return None

    def _pull_broker_all_orders(self) -> list:
        """
        Retrieve all orders from Tradeovate via the /order/list endpoint.
        Returns the raw JSON list of orders (dictionaries) without parsing.
        """
        url = f"{self.trading_api_url}/order/list"
        headers = self._get_headers()
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            raise TradovateAPIError(f"Failed to retrieve orders", 
                                     status_code=getattr(e.response, 'status_code', None),
                                     response_text=getattr(e.response, 'text', None),
                                     original_exception=e)

    def _pull_broker_order(self, identifier: str) -> Order:
        """
        Retrieve a specific order by its order id using the /order/item endpoint.
        """
        url = f"{self.trading_api_url}/order/item"
        params = {"id": identifier}
        headers = self._get_headers()
        try:
            response = requests.get(url, params=params, headers=headers)
            response.raise_for_status()
            order_data = response.json()
            order_obj = self._parse_broker_order(order_data, strategy_name="")  # set strategy as needed
            return order_obj
        except requests.exceptions.RequestException as e:
            raise TradovateAPIError(f"Failed to retrieve order {identifier}", 
                                     status_code=getattr(e.response, 'status_code', None),
                                     response_text=getattr(e.response, 'text', None),
                                     original_exception=e)

    def _pull_position(self, strategy, asset: Asset) -> Position:
        logging.error(colored(f"Method '_pull_position' for asset {asset} is not yet implemented.", "red"))
        return None

    def _pull_positions(self, strategy) -> list[Position]:
        """
        Retrieve all open positions from Tradeovate via the /position/list endpoint.
        For each returned position, create a Position object.
        Assumes that each position dict contains:
          - 'contractId': the contract identifier to retrieve asset details,
          - 'netPos': the position quantity,
          - 'netPrice': the average fill price.
        The asset is created using contract details retrieved from Tradeovate.
        """
        url = f"{self.trading_api_url}/position/list"
        headers = self._get_headers()
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            positions_data = response.json()
            positions = []
            for pos in positions_data:
                contract_id = pos.get("contractId")
                if not contract_id:
                    logging.error("No contractId found in position data.")
                    continue
                try:
                    contract_details = self._get_contract_details(contract_id)
                except TradovateAPIError as e:
                    logging.error(colored(f"Failed to retrieve contract details for contractId {contract_id}: {e}", "red"))
                    continue
                # Extract asset details from the contract details.
                # For Tradeovate futures, assume asset_type is "future" and use the contract name as the symbol.
                symbol = contract_details.get("name", "")
                expiration = None
                multiplier = 1  # default multiplier
                asset = Asset(symbol=symbol, asset_type=Asset.AssetType.FUTURE, expiration=expiration, multiplier=multiplier)
                quantity = pos.get("netPos", 0)
                net_price = pos.get("netPrice", 0)
                hold = 0
                available = 0
                position_obj = Position(
                    strategy,
                    asset,
                    quantity,
                    orders=[],
                    hold=hold,
                    available=available,
                    avg_fill_price=net_price
                )
                positions.append(position_obj)
            return positions
        except requests.exceptions.RequestException as e:
            raise TradovateAPIError(f"Failed to retrieve positions", 
                                     status_code=getattr(e.response, 'status_code', None),
                                     response_text=getattr(e.response, 'text', None),
                                     original_exception=e)

    def _register_stream_events(self):
        logging.error(colored("Method '_register_stream_events' is not yet implemented.", "red"))
        return None

    def _run_stream(self):
        logging.error(colored("Method '_run_stream' is not yet implemented.", "red"))
        return None

    def _submit_order(self, order: Order) -> Order:
        """
        Submit an order to Tradeovate.

        This method takes an Order object, extracts necessary details, builds the payload,
        and sends it to the Tradeovate API to place the order. On success, the order status
        is updated to 'submitted' and the raw response is attached to the order. Otherwise, 
        the order is marked with an error.
        """
        # Pre-submission validation
        if not self.account_spec or not self.account_id:
            error_msg = "Account information not properly initialized"
            logging.error(error_msg)
            order.set_error(error_msg)
            return order
        
        # Check if we have valid tokens
        if not hasattr(self, 'trading_token') or not self.trading_token:
            error_msg = "Trading token not available - authentication may have failed"
            logging.error(error_msg)
            order.set_error(error_msg)
            return order
        
        # Determine the action based on the order side
        action = "Buy" if order.is_buy_order() else "Sell"

        # Extract symbol from the order's asset and handle continuous futures conversion
        if order.asset.asset_type == order.asset.AssetType.CONT_FUTURE:
            # For continuous futures, resolve to the specific contract symbol using Tradovate format
            symbol = self._resolve_tradovate_futures_symbol(order.asset)
            logging.info(f"Resolved continuous future {order.asset.symbol} -> {symbol}")
        else:
            symbol = order.asset.symbol

        # Determine the order type string based on the order type.
        if order.order_type == Order.OrderType.MARKET:
            order_type = "Market"
        elif order.order_type == Order.OrderType.LIMIT:
            order_type = "Limit"
        elif order.order_type == Order.OrderType.STOP:
            order_type = "Stop"
        elif order.order_type == Order.OrderType.STOP_LIMIT:
            order_type = "StopLimit"
        else:
            logging.warning(
                f"Order type '{order.order_type}' is not fully supported. Defaulting to Market order."
            )
            order_type = "Market"

        # Build the payload with numeric values sent as numbers and booleans as True/False.
        payload = {
            "accountSpec": self.account_spec,
            "accountId": self.account_id,
            "action": action,
            "symbol": symbol,
            # Convert order.quantity to an integer rather than a float.
            "orderQty": int(order.quantity),
            "orderType": order_type,
            "isAutomated": True
        }
        # If a limit price is specified for limit orders, include it.
        if order.limit_price is not None:
            payload["price"] = float(order.limit_price)
        # Similarly, include stop price if specified.
        if order.stop_price is not None:
            payload["stopPrice"] = float(order.stop_price)

        url = f"{self.trading_api_url}/order/placeorder"
        headers = self._get_headers(with_content_type=True)

        # Log the request details for debugging (mask sensitive auth data)
        logging.info(f"Submitting order to Tradovate:")
        logging.info(f"  URL: {url}")
        logging.info(f"  Payload: {payload}")
        
        # Log headers but mask the authorization token for security
        safe_headers = headers.copy()
        if 'Authorization' in safe_headers:
            safe_headers['Authorization'] = 'Bearer ***MASKED***'
        logging.info(f"  Headers: {safe_headers}")

        try:
            response = requests.post(url, json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()
            
            # Check if the response indicates a failure
            if data.get('failureReason') or data.get('failureText'):
                failure_reason = data.get('failureReason', 'Unknown')
                failure_text = data.get('failureText', 'No details provided')
                error_message = f"Order rejected by Tradovate: {failure_reason} - {failure_text}"
                logging.error(error_message)
                
                # Add additional context for common errors
                if 'Access is denied' in failure_text:
                    logging.error("Possible causes: Account not authorized for trading, market closed, or insufficient permissions")
                elif 'UnknownReason' in failure_reason:
                    logging.error("Possible causes: Invalid symbol, market hours, account restrictions, or order parameters")
                
                order.set_error(error_message)
                return order
            else:
                # Order was successful
                logging.info(f"Order successfully submitted: {data}")
                order.status = Order.OrderStatus.SUBMITTED
                order.update_raw(data)
                return order
                
        except requests.exceptions.RequestException as e:
            error_message = f"Failed to submit order: {getattr(e.response, 'status_code', None)}, {getattr(e.response, 'text', None)}"
            logging.error(error_message)
            order.set_error(error_message)
            return order
        
    def cancel_order(self, order_id) -> None:
        logging.error(colored(f"Method 'cancel_order' for order_id {order_id} is not yet implemented.", "red"))
        return None

    def _modify_order(self, order: Order, limit_price: Union[float, None] = None,
                      stop_price: Union[float, None] = None):
        logging.error(colored(f"Method '_modify_order' for order {order} is not yet implemented.", "red"))
        return None

    def get_historical_account_value(self) -> dict:
        logging.error(colored("Method 'get_historical_account_value' is not yet implemented.", "red"))
        return {}