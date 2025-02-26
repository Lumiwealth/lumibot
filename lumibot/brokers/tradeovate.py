import logging
import requests
import json
from typing import Union
from datetime import datetime

from termcolor import colored
from lumibot.brokers import Broker
from lumibot.entities import Asset, Order, Position
from lumibot.data_sources import TradeovateData

class Tradeovate(Broker):
    """
    Tradeovate broker that implements connection to the Tradeovate API.
    """
    NAME = "Tradeovate"

    def __init__(self, config=None, data_source=None):
        # Ensure config is a dict and a data source is provided
        if config is None:
            config = {}
        if data_source is None:
            data_source = TradeovateData()

        # Set configuration values from the provided config
        self.trading_api_url = config.get("API_URL", "https://demo.tradovateapi.com/v1")
        self.market_data_url = config.get("MD_URL", "https://md.tradovateapi.com/v1")
        self.username = config.get("USERNAME")
        self.password = config.get("DEDICATED_PASSWORD")
        self.app_id = config.get("APP_ID", "Lumibot")
        self.app_version = config.get("APP_VERSION", "1.0")
        self.cid = config.get("CID")
        self.sec = config.get("SECRET")

        super().__init__(name=self.NAME, data_source=data_source, config=config)

        # Connect to Tradeovate: get tokens, account, and user information
        try:
            tokens = self._get_tokens()
            self.trading_token = tokens["accessToken"]
            self.market_token = tokens["marketToken"]
            self.has_market_data = tokens["hasMarketData"]
            logging.info(colored("Successfully acquired tokens from Tradeovate.", "green"))

            account_info = self._get_account_info(self.trading_token)
            self.account_spec = account_info["accountSpec"]
            self.account_id = account_info["accountId"]
            logging.info(colored(f"Account Info: {account_info}", "green"))

            self.user_id = self._get_user_info(self.trading_token)
            logging.info(colored(f"User ID: {self.user_id}", "green"))
        except Exception as e:
            logging.error(colored(f"Failed to connect to Tradeovate: {e}", "red"))
            raise e

    def _get_tokens(self):
        """
        Authenticate with Tradeovate and obtain the access tokens.
        """
        url = f"{self.trading_api_url}/auth/accesstokenrequest"
        payload = {
            "name": self.username,
            "password": self.password,
            "appId": self.app_id,
            "appVersion": self.app_version,
            "cid": self.cid,
            "sec": self.sec
        }
        headers = {"Content-Type": "application/json", "Accept": "application/json"}
        response = requests.post(url, json=payload, headers=headers)
        if response.status_code == 200:
            data = response.json()
            access_token = data.get("accessToken")
            market_token = data.get("mdAccessToken")
            has_market_data = data.get("hasMarketData", False)
            if not access_token or not market_token:
                raise Exception("Authentication succeeded but tokens are missing.")
            return {"accessToken": access_token, "marketToken": market_token, "hasMarketData": has_market_data}
        else:
            raise Exception(f"Authentication failed: {response.status_code}, {response.text}")

    def _get_account_info(self, trading_token):
        """
        Retrieve account information from Tradeovate.
        """
        url = f"{self.trading_api_url}/account/list"
        headers = {
            "Authorization": f"Bearer {trading_token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            accounts = response.json()
            if isinstance(accounts, list) and accounts:
                account = accounts[0]
                return {"accountSpec": account.get("name"), "accountId": account.get("id")}
            else:
                raise Exception("No accounts found in the account list response.")
        else:
            raise Exception(f"Failed to retrieve account list: {response.status_code}, {response.text}")

    def _get_user_info(self, trading_token):
        """
        Retrieve user information from Tradeovate.
        """
        url = f"{self.trading_api_url}/user/list"
        headers = {
            "Authorization": f"Bearer {trading_token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            users = response.json()
            if isinstance(users, list) and users:
                user = users[0]
                return user.get("id")
            else:
                raise Exception("No users found in the user list response.")
        else:
            raise Exception(f"Failed to retrieve user list: {response.status_code}, {response.text}")

    def _get_contract_details(self, contract_id: int) -> dict:
        """
        Retrieve contract details for a given contract id from Tradeovate using the /contract/item endpoint.
        
        Endpoint: GET /contract/item?id=<contract_id>
        Response Schema: { "id": int, "name": string, "contractMaturityId": int }
        """
        url = f"{self.trading_api_url}/contract/item"
        params = {"id": contract_id}
        headers = {
            "Authorization": f"Bearer {self.trading_token}",
            "Accept": "application/json"
        }
        response = requests.get(url, params=params, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f"Failed to retrieve contract details for contract {contract_id}: {response.status_code}, {response.text}")

    def _get_balances_at_broker(self, quote_asset: Asset, strategy) -> tuple:
        """
        Retrieve the account financial snapshot from Tradeovate and compute:
          - Cash balance (totalCashValue)
          - Positions value (netLiq - totalCashValue)
          - Portfolio value (netLiq)
        """
        url = f"{self.trading_api_url}/cashBalance/getcashbalancesnapshot"
        headers = {
            "Authorization": f"Bearer {self.trading_token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        payload = {"accountId": self.account_id}
        response = requests.post(url, json=payload, headers=headers)
        if response.status_code == 200:
            data = response.json()
            cash_balance = data.get("totalCashValue")
            net_liq = data.get("netLiq")
            if cash_balance is None or net_liq is None:
                raise Exception("Missing totalCashValue or netLiq in account financials response.")
            positions_value = net_liq - cash_balance
            portfolio_value = net_liq
            return cash_balance, positions_value, portfolio_value
        else:
            raise Exception(f"Failed to retrieve account financials: {response.status_code}, {response.text}")

    def _get_stream_object(self):
        logging.info(colored("Method '_get_stream_object' is not yet implemented.", "yellow"))
        return None  # Return None as a placeholder

    def _parse_broker_order(self, response: dict, strategy_name: str, strategy_object=None) -> Order:
        logging.error(colored("Method '_parse_broker_order' is not yet implemented.", "red"))
        return None

    def _pull_broker_all_orders(self) -> list[Order]:
        logging.error(colored("Method '_pull_broker_all_orders' is not yet implemented.", "red"))
        return []

    def _pull_broker_order(self, identifier: str) -> Order:
        logging.error(colored(f"Method '_pull_broker_order' for order_id {identifier} is not yet implemented.", "red"))
        return None

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
        headers = {"Authorization": f"Bearer {self.trading_token}", "Accept": "application/json"}
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            positions_data = response.json()
            positions = []
            for pos in positions_data:
                contract_id = pos.get("contractId")
                if not contract_id:
                    logging.error("No contractId found in position data.")
                    continue
                try:
                    contract_details = self._get_contract_details(contract_id)
                except Exception as e:
                    logging.error(colored(f"Failed to retrieve contract details for contractId {contract_id}: {e}", "red"))
                    continue
                # Extract asset details from the contract details.
                # For Tradeovate futures, assume asset_type is "future" and use the contract name as the symbol.
                symbol = contract_details.get("name", "")
                asset_type = "future"
                expiration = None
                multiplier = 1  # default multiplier
                asset = Asset(symbol=symbol, asset_type=asset_type, expiration=expiration, multiplier=multiplier)
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
        else:
            raise Exception(f"Failed to retrieve positions: {response.status_code}, {response.text}")

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
        # Determine the action based on the order side
        action = "Buy" if order.is_buy_order() else "Sell"

        # Extract symbol from the order's asset
        symbol = order.asset.symbol

        # Determine the order type string based on the order type.
        if order.type.lower() == "market":
            order_type = "Market"
        elif order.type.lower() == "limit":
            order_type = "Limit"
        elif order.type.lower() == "stop":
            order_type = "Stop"
        elif order.type.lower() == "stop_limit":
            order_type = "StopLimit"
        else:
            logging.warning(
                f"Order type '{order.type}' is not fully supported. Defaulting to Market order."
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
            payload["limitPrice"] = float(order.limit_price)
        # Similarly, include stop price if specified.
        if order.stop_price is not None:
            payload["stopPrice"] = float(order.stop_price)

        url = f"{self.trading_api_url}/order/placeorder"
        headers = {
            "Authorization": f"Bearer {self.trading_token}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }

        response = requests.post(url, json=payload, headers=headers)
        if response.status_code == 200:
            data = response.json()
            logging.info(f"Order successfully submitted: {data}")
            order.status = "submitted"
            order.update_raw(data)
            return order
        else:
            error_message = f"Failed to submit order: {response.status_code}, {response.text}"
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