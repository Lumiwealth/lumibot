import random
import re
import threading
import time
import traceback
from collections import deque
from datetime import datetime, timezone
from typing import Optional, Union

import requests
from termcolor import colored

from .broker import Broker
from lumibot.data_sources import TradovateData
from lumibot.entities import Asset, Order, Position
from lumibot.trading_builtins import PollingStream

# Set up module-specific logger for enhanced logging
from lumibot.tools.lumibot_logger import get_logger

logger = get_logger(__name__)

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
    POLL_EVENT = PollingStream.POLL_EVENT

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
        self.polling_interval = float(config.get("POLLING_INTERVAL", 5.0))
        self._seen_fill_ids: set[int] = set()
        self._fill_bootstrap_cutoff = datetime.now(timezone.utc)
        self._active_broker_identifiers: Optional[set[str]] = None

        # Configure lightweight in-process rate limiter for REST calls
        self._rate_limit_per_minute = max(int(config.get("RATE_LIMIT_PER_MINUTE", 60)), 1)
        self._rate_limit_window = 60.0
        self._request_times = deque()
        self._request_lock = threading.Lock()

        # Cache for contract lookups to avoid redundant requests
        self._contract_cache: dict[int, dict] = {}

        # Balance sync throttling state
        self._last_balance_sync: Optional[float] = None
        self._cached_balances: Optional[tuple[float, float, float]] = None
        self._balance_cooldown_seconds = max(int(config.get("BALANCE_SYNC_COOLDOWN", 30)), 1)
        self._balance_retry_cooldown = max(int(config.get("BALANCE_RETRY_COOLDOWN", 300)), 30)
        self._balance_backoff_until: Optional[float] = None

        # Authenticate and get tokens before creating data_source
        try:
            tokens = self._get_tokens()
            self.trading_token = tokens["accessToken"]
            self.market_token = tokens["marketToken"]
            self.has_market_data = tokens["hasMarketData"]
            self.token_acquired_time = time.time()
            self.token_lifetime = 4800  # Tradovate tokens expire after 80 minutes
            logger.info(colored("Successfully acquired tokens from Tradovate.", "green"))

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
            logger.info(colored(f"Account Info: {account_info}", "green"))

            self.user_id = self._get_user_info(self.trading_token)
            logger.info(colored(f"User ID: {self.user_id}", "green"))

        except TradovateAPIError as e:
            logger.warning(colored(f"Failed initial connection to Tradovate: {e}", "yellow"))
            logger.warning(colored("Broker initialization failed due to rate limiting. The script will exit cleanly.", "yellow"))
            raise e

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _throttle_rest(self):
        """Ensure REST calls respect a soft per-minute cap."""
        if self._rate_limit_per_minute <= 0:
            return

        with self._request_lock:
            now = time.time()
            window_start = now - self._rate_limit_window

            # Drop timestamps outside of window
            while self._request_times and self._request_times[0] < window_start:
                self._request_times.popleft()

            if len(self._request_times) >= self._rate_limit_per_minute:
                wait_for = self._rate_limit_window - (now - self._request_times[0])
                wait_for = max(wait_for, 0) + random.uniform(0.05, 0.25)
                logger.debug(
                    "Tradovate REST throttle triggered; sleeping %.2fs to stay under limit",
                    wait_for,
                )
                time.sleep(wait_for)

                # Recalculate after sleeping
                now = time.time()
                window_start = now - self._rate_limit_window
                while self._request_times and self._request_times[0] < window_start:
                    self._request_times.popleft()

            self._request_times.append(now)

    def _request(self, method: str, url: str, **kwargs):
        """Wrapper around requests.request with throttling."""
        self._throttle_rest()
        return requests.request(method=method, url=url, **kwargs)

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
            response = self._request("POST", url, json=payload, headers=headers, timeout=30)
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
            raise TradovateAPIError("Authentication failed",
                                     status_code=getattr(e.response, 'status_code', None),
                                     response_text=getattr(e.response, 'text', None),
                                     original_exception=e)

    def _get_account_info(self, trading_token):
        """
        Retrieve account information from Tradovate with retry logic for rate limiting.
        """
        url = f"{self.trading_api_url}/account/list"
        headers = self._get_headers()
        
        max_retries = 5
        retry_delay = 10  # Start with 10 seconds
        
        for attempt in range(max_retries):
            try:
                response = self._request("GET", url, headers=headers)
                
                # Handle rate limiting with exponential backoff
                if response.status_code == 429:
                    if attempt < max_retries - 1:
                        wait_time = retry_delay * (2 ** attempt)  # Exponential backoff
                        logger.warning(f"Rate limited on account list. Waiting {wait_time} seconds before retry {attempt + 1}/{max_retries}")
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"Account list still rate limited after {max_retries} attempts")
                        raise TradovateAPIError(f"Rate limited after {max_retries} attempts", 
                                                status_code=429,
                                                response_text=response.text)
                
                response.raise_for_status()
                accounts = response.json()
                if isinstance(accounts, list) and accounts:
                    account = accounts[0]
                    return {"accountSpec": account.get("name"), "accountId": account.get("id")}
                else:
                    logger.error(f"No accounts found. Response: {accounts}")
                    raise TradovateAPIError("No accounts found in the account list response.")
            except requests.exceptions.RequestException as e:
                if getattr(e.response, 'status_code', None) != 429:  # Don't log 429s as errors since we handle them
                    logger.error(f"Account list request failed: Status={getattr(e.response, 'status_code', None)}, Response={getattr(e.response, 'text', None)}")
                raise TradovateAPIError("Failed to retrieve account list",
                                         status_code=getattr(e.response, 'status_code', None),
                                         response_text=getattr(e.response, 'text', None),
                                         original_exception=e)

    def _get_user_info(self, trading_token):
        """
        Retrieve user information from Tradovate with retry logic for rate limiting.
        """
        url = f"{self.trading_api_url}/user/list"
        headers = self._get_headers()
        
        max_retries = 5
        retry_delay = 10  # Start with 10 seconds
        
        for attempt in range(max_retries):
            try:
                response = self._request("GET", url, headers=headers)
                
                # Handle rate limiting with exponential backoff
                if response.status_code == 429:
                    if attempt < max_retries - 1:
                        wait_time = retry_delay * (2 ** attempt)  # Exponential backoff
                        logger.warning(f"Rate limited on user list. Waiting {wait_time} seconds before retry {attempt + 1}/{max_retries}")
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"User list still rate limited after {max_retries} attempts")
                        raise TradovateAPIError(f"Rate limited after {max_retries} attempts", 
                                                status_code=429,
                                                response_text=response.text)
                
                response.raise_for_status()
                users = response.json()
                if isinstance(users, list) and users:
                    user = users[0]
                    return user.get("id")
                else:
                    raise TradovateAPIError("No users found in the user list response.")
            except requests.exceptions.RequestException as e:
                if getattr(e.response, 'status_code', None) != 429:  # Don't log 429s as errors since we handle them
                    logger.error(f"User list request failed: Status={getattr(e.response, 'status_code', None)}, Response={getattr(e.response, 'text', None)}")
                raise TradovateAPIError("Failed to retrieve user list",
                                         status_code=getattr(e.response, 'status_code', None),
                                         response_text=getattr(e.response, 'text', None),
                                     original_exception=e)

    def _check_and_renew_token(self):
        """
        Check if the token is expired or about to expire and renew it if necessary.
        """
        current_time = time.time()
        token_age = current_time - self.token_acquired_time

        # Renew token if it's older than 90% of its lifetime (72 minutes for 80 minute tokens)
        if token_age > (self.token_lifetime * 0.9):
            logger.info(colored("Token is about to expire, renewing...", "yellow"))
            try:
                tokens = self._get_tokens()
                self.trading_token = tokens["accessToken"]
                self.market_token = tokens["marketToken"]
                self.has_market_data = tokens["hasMarketData"]
                self.token_acquired_time = time.time()

                # Update the data source tokens if it exists
                if hasattr(self, 'data_source') and self.data_source:
                    self.data_source.trading_token = self.trading_token
                    self.data_source.market_token = self.market_token

                logger.info(colored("Successfully renewed Tradovate tokens.", "green"))
            except Exception as e:
                logger.error(colored(f"Failed to renew tokens: {e}", "red"))
                raise e

    def _handle_api_request(self, request_func, *args, **kwargs):
        """
        Wrapper to handle API requests with automatic token renewal on 401 errors.
        """
        try:
            return request_func(*args, **kwargs)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 401:
                logger.warning(colored("Received 401 error, attempting to renew token...", "yellow"))
                self._check_and_renew_token()
                # Retry the request once after renewal
                return request_func(*args, **kwargs)
            else:
                raise e

    def _resolve_tradovate_futures_symbol(self, asset) -> str:
        """
        Resolve futures assets to the symbol format expected by Tradovate.

        For continuous contracts this method delegates to
        ``Asset.resolve_continuous_futures_contract`` using a single-digit
        year (e.g., ``MNQZ5``). Specific contracts are normalized to the same
        single-digit year format so that cached data and order routing remain
        consistent.

        Parameters
        ----------
        asset : Asset
            The futures or continuous futures asset to resolve.

        Returns
        -------
        str
            Tradovate-specific futures contract symbol (single-digit year).
        """
        if asset.asset_type == Asset.AssetType.CONT_FUTURE:
            contract = asset.resolve_continuous_futures_contract(year_digits=1)
            return contract

        if asset.asset_type == Asset.AssetType.FUTURE:
            return self._format_tradovate_contract(asset.symbol)

        return asset.symbol

    @staticmethod
    def _format_tradovate_contract(contract: str) -> str:
        """Convert a futures contract symbol to Tradovate's single-digit year format."""
        if not contract:
            return contract

        normalized = contract.replace(".", "").upper()
        match = re.match(r"^([A-Z]+)([FGHJKMNQUVXZ])(\d{1,4})$", normalized)
        if not match:
            return normalized

        root, month_code, year_part = match.groups()
        try:
            year_int = int(year_part)
        except ValueError:
            return normalized

        single_digit_year = year_int % 10
        return f"{root}{month_code}{single_digit_year}"

    def _get_contract_details(self, contract_id: int) -> dict:
        """
        Retrieve contract details for a given contract id from Tradeovate using the /contract/item endpoint.
        
        Endpoint: GET /contract/item?id=<contract_id>
        Response Schema: { "id": int, "name": string, "contractMaturityId": int }
        """
        if contract_id in self._contract_cache:
            return self._contract_cache[contract_id]

        url = f"{self.trading_api_url}/contract/item"
        params = {"id": contract_id}
        headers = self._get_headers()
        try:
            response = self._request("GET", url, params=params, headers=headers)
            response.raise_for_status()
            data = response.json()
            if data:
                self._contract_cache[contract_id] = data
            return data
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
        now = time.time()

        # Respect cooldown between successful refreshes
        if (
            self._last_balance_sync is not None
            and now - self._last_balance_sync < self._balance_cooldown_seconds
            and self._cached_balances is not None
        ):
            logger.debug(
                "Skipping Tradovate balance sync; last refresh %.1fs ago < cooldown",
                now - self._last_balance_sync,
            )
            return self._cached_balances

        # Honor temporary backoff window triggered by repeated 429s
        if self._balance_backoff_until and now < self._balance_backoff_until:
            remaining = self._balance_backoff_until - now
            if self._cached_balances is not None:
                logger.warning(
                    "Tradovate balance sync in cooldown for another %.0fs; using cached snapshot",
                    remaining,
                )
                return self._cached_balances
            logger.warning(
                "Tradovate balance sync in cooldown for another %.0fs but no cached value found",
                remaining,
            )
            # fall through to attempt a fetch so we don't return None

        def _make_request():
            url = f"{self.trading_api_url}/cashBalance/getcashbalancesnapshot"
            headers = self._get_headers(with_content_type=True)
            payload = {"accountId": self.account_id}
            response = self._request("POST", url, json=payload, headers=headers)
            response.raise_for_status()
            return response

        max_retries = 5
        retry_delay = 10  # Start with 10 seconds
        last_status_code: Optional[int] = None
        final_exception: Optional[Exception] = None

        for attempt in range(max_retries):
            try:
                response = self._handle_api_request(_make_request)
                data = response.json()
                cash_balance = data.get("totalCashValue")
                net_liq = data.get("netLiq")
                if cash_balance is None or net_liq is None:
                    raise TradovateAPIError("Missing totalCashValue or netLiq in account financials response.")
                positions_value = net_liq - cash_balance
                portfolio_value = net_liq
                self._cached_balances = (cash_balance, positions_value, portfolio_value)
                self._last_balance_sync = time.time()
                # Successful call clears backoff gate
                self._balance_backoff_until = None
                return cash_balance, positions_value, portfolio_value
            except (requests.exceptions.RequestException, TradovateAPIError) as e:
                status_code = getattr(e.response if hasattr(e, 'response') else None, 'status_code', None)
                last_status_code = status_code
                final_exception = e
                
                # Handle rate limiting with exponential backoff
                if status_code == 429:
                    if attempt < max_retries - 1:
                        wait_time = retry_delay * (2 ** attempt)  # Exponential backoff
                        logger.warning(f"Rate limited on balance retrieval. Waiting {wait_time} seconds before retry {attempt + 1}/{max_retries}")
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"Balance retrieval still rate limited after {max_retries} attempts")
                        break
                
                # For non-rate-limiting errors or final attempt, raise the error
                raise TradovateAPIError("Failed to retrieve account financials",
                                         status_code=status_code,
                                         response_text=getattr(e.response if hasattr(e, 'response') else None, 'text', None),
                                         original_exception=e)

        # Only reached when retries exhausted (likely due to rate limiting)
        if last_status_code == 429:
            self._balance_backoff_until = time.time() + self._balance_retry_cooldown
            if self._cached_balances is not None:
                logger.warning(
                    "Returning cached Tradovate balances after repeated 429 responses; next live fetch after cooldown"
                )
                return self._cached_balances

        # No cached value to fall back on: re-raise original exception for visibility
        if final_exception:
            raise TradovateAPIError("Failed to retrieve account financials",
                                     status_code=last_status_code,
                                     response_text=getattr(final_exception.response if hasattr(final_exception, 'response') else None, 'text', None),
                                     original_exception=final_exception)

        raise TradovateAPIError("Failed to retrieve account financials")

    def _get_stream_object(self):
        """Return a polling stream to monitor Tradovate orders."""
        return PollingStream(self.polling_interval)

    def check_token_expiry(self):
        """
        Public method to proactively check and renew token if needed.
        This can be called periodically by the strategy or trading framework.
        """
        self._check_and_renew_token()

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
                    # Log as debug instead of error - this is a common occurrence that doesn't need error-level logging
                    logger.debug(f"Could not retrieve contract details for order {order_id}: {e}")

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
            response = self._request("GET", url, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            raise TradovateAPIError("Failed to retrieve orders",
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
            response = self._request("GET", url, params=params, headers=headers)
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
        logger.error(colored(f"Method '_pull_position' for asset {asset} is not yet implemented.", "red"))
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
            response = self._request("GET", url, headers=headers)
            response.raise_for_status()
            positions_data = response.json()
            positions = []
            for pos in positions_data:
                contract_id = pos.get("contractId")
                if not contract_id:
                    logger.error("No contractId found in position data.")
                    continue
                try:
                    contract_details = self._get_contract_details(contract_id)
                except TradovateAPIError as e:
                    logger.error(colored(f"Failed to retrieve contract details for contractId {contract_id}: {e}", "red"))
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
            raise TradovateAPIError("Failed to retrieve positions",
                                     status_code=getattr(e.response, 'status_code', None),
                                     response_text=getattr(e.response, 'text', None),
                                     original_exception=e)

    def _register_stream_events(self):
        """Register polling callbacks that mirror the standard lifecycle pipeline."""
        stream = getattr(self, "stream", None)
        if stream is None:
            return

        broker = self

        @stream.add_action(self.POLL_EVENT)
        def on_trade_event_poll():
            try:
                self.do_polling()
            except Exception as exc:  # pragma: no cover - defensive logging
                logger.exception("Tradovate polling failure: %s", exc)

        @stream.add_action(self.NEW_ORDER)
        def on_trade_event_new(order):
            logger.info(f"Tradovate processing NEW order event: {order}")
            try:
                broker._process_trade_event(order, broker.NEW_ORDER)
            except Exception:  # pragma: no cover
                logger.error(traceback.format_exc())

        @stream.add_action(self.FILLED_ORDER)
        def on_trade_event_fill(order, price, filled_quantity):
            logger.info(
                f"Tradovate processing FILLED event: {order} price={price} qty={filled_quantity}"
            )
            try:
                broker._process_trade_event(
                    order,
                    broker.FILLED_ORDER,
                    price=price,
                    filled_quantity=filled_quantity,
                    multiplier=order.asset.multiplier if order.asset else 1,
                )
            except Exception:  # pragma: no cover
                logger.error(traceback.format_exc())

        @stream.add_action(self.PARTIALLY_FILLED_ORDER)
        def on_trade_event_partial(order, price, filled_quantity):
            logger.info(
                f"Tradovate processing PARTIAL event: {order} price={price} qty={filled_quantity}"
            )
            try:
                broker._process_trade_event(
                    order,
                    broker.PARTIALLY_FILLED_ORDER,
                    price=price,
                    filled_quantity=filled_quantity,
                    multiplier=order.asset.multiplier if order.asset else 1,
                )
            except Exception:  # pragma: no cover
                logger.error(traceback.format_exc())

        @stream.add_action(self.CANCELED_ORDER)
        def on_trade_event_cancel(order):
            logger.info(f"Tradovate processing CANCEL event: {order}")
            try:
                broker._process_trade_event(order, broker.CANCELED_ORDER)
            except Exception:  # pragma: no cover
                logger.error(traceback.format_exc())

        @stream.add_action(self.ERROR_ORDER)
        def on_trade_event_error(order, error_msg=None):
            logger.error(f"Tradovate processing ERROR event: {order} msg={error_msg}")
            try:
                broker._process_trade_event(order, broker.ERROR_ORDER, error=error_msg)
            except Exception:  # pragma: no cover
                logger.error(traceback.format_exc())

    def _run_stream(self):
        """Start the polling loop and mark the connection as established."""
        self._stream_established()
        if getattr(self, "stream", None) is None:
            return
        try:
            self.stream._run()
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.exception("Tradovate polling stream terminated unexpectedly: %s", exc)

    # ------------------------------------------------------------------
    # Polling helpers
    # ------------------------------------------------------------------
    def _extract_fill_details(self, raw_order: dict, order: Order) -> tuple[Optional[float], Optional[float]]:
        """Attempt to derive fill price and quantity from a Tradovate order payload."""

        def _normalize_number(value):
            try:
                if value in (None, "", "null"):
                    return None
                numeric = float(value)
                if numeric == 0:
                    return None
                return numeric
            except (TypeError, ValueError):
                return None

        def _to_float(value):
            try:
                if value in (None, "", "null"):
                    return None
                return float(value)
            except (TypeError, ValueError):
                return None

        price_candidates = [
            raw_order.get("avgFillPrice"),
            raw_order.get("filledPrice"),
            raw_order.get("tradePrice"),
            raw_order.get("price"),
            raw_order.get("stopPrice"),
            raw_order.get("lastPrice"),
            getattr(order, "avg_fill_price", None),
            getattr(order, "limit_price", None),
        ]
        price = next((val for val in (_to_float(candidate) for candidate in price_candidates) if val is not None), None)

        quantity_candidates = [
            raw_order.get("filledQuantity"),
            raw_order.get("filledQty"),
            raw_order.get("execQuantity"),
            raw_order.get("tradeQuantity"),
            raw_order.get("quantity"),
            getattr(order, "quantity", None),
        ]

        if getattr(order, "child_orders", None):
            for child in order.child_orders:
                quantity_candidates.append(getattr(child, "quantity", None))
                price_candidates.append(getattr(child, "avg_fill_price", None))
        quantity = next(
            (val for val in (_to_float(candidate) for candidate in quantity_candidates) if val is not None),
            None,
        )

        order_identifier = raw_order.get("id") or getattr(order, "identifier", None)
        needs_fill_lookup = (
            order_identifier
            and (
                price is None
                or quantity in (None, 0, 0.0)
            )
        )
        if needs_fill_lookup:
            fill_price, fill_qty = self._fetch_recent_fill_details(order_identifier)
            if fill_qty is not None:
                quantity = _normalize_number(fill_qty)
            if price is None and fill_price is not None:
                price = _normalize_number(fill_price)

        return price, quantity

    def _fetch_recent_fill_details(self, order_identifier) -> tuple[Optional[float], Optional[float]]:
        """Fallback to /fill/list when Tradovate omits fill price/quantity in order payloads."""
        if not order_identifier:
            return None, None

        try:
            params = {"accountId": self.account_id, "orderId": order_identifier}
            response = self._request(
                "GET",
                f"{self.trading_api_url}/fill/list",
                params=params,
                headers=self._get_headers(),
            )
            response.raise_for_status()
            fills = response.json()
        except requests.exceptions.RequestException as exc:
            logger.debug("Tradovate fill lookup failed for order %s: %s", order_identifier, exc)
            return None, None

        if not isinstance(fills, list):
            return None, None

        new_fills = []
        for fill in fills:
            if str(fill.get("orderId")) != str(order_identifier):
                continue

            fill_id = fill.get("id")
            if fill_id in self._seen_fill_ids:
                continue

            timestamp_str = fill.get("timestamp")
            fill_dt = None
            if timestamp_str:
                try:
                    fill_dt = datetime.fromisoformat(timestamp_str.replace("Z", "+00:00"))
                except ValueError:
                    fill_dt = None

            if fill_dt and fill_dt < self._fill_bootstrap_cutoff:
                self._seen_fill_ids.add(fill_id)
                continue

            qty = fill.get("qty")
            price = fill.get("price")
            if qty in (None, "", "null"):
                self._seen_fill_ids.add(fill_id)
                continue

            try:
                qty_val = float(qty)
            except (TypeError, ValueError):
                self._seen_fill_ids.add(fill_id)
                continue

            if qty_val == 0:
                self._seen_fill_ids.add(fill_id)
                continue

            price_val = None
            try:
                price_val = float(price) if price is not None else None
            except (TypeError, ValueError):
                price_val = None

            new_fills.append((fill_id, price_val, qty_val))
            self._seen_fill_ids.add(fill_id)

        if not new_fills:
            return None, None

        total_qty = sum(qty for _, _, qty in new_fills)
        if total_qty <= 0:
            return None, None

        weighted_price = sum((price or 0.0) * qty for _, price, qty in new_fills)
        avg_price = weighted_price / total_qty if weighted_price else None
        return avg_price, total_qty

    def do_polling(self):
        """Poll Tradovate REST endpoints to keep order state synchronized."""
        # Sync positions so position lookups remain accurate.
        try:
            self.sync_positions(None)
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.debug("Tradovate position sync failed during polling: %s", exc)

        try:
            raw_orders = self._pull_broker_all_orders() or []
        except TradovateAPIError as exc:
            logger.error(colored(f"Tradovate polling: failed to retrieve orders ({exc})", "red"))
            return
        except Exception as exc:  # pragma: no cover
            logger.exception("Tradovate polling: unexpected error retrieving orders: %s", exc)
            return

        stored_orders = {
            order.identifier: order
            for order in self.get_all_orders()
            if getattr(order, "identifier", None) is not None
        }
        seen_identifiers: set[str] = set()
        active_identifiers: set[str] = set()

        for raw_order in raw_orders:
            try:
                parsed_order = self._parse_broker_order(
                    raw_order,
                    strategy_name=self._strategy_name or (self.NAME if isinstance(self.NAME, str) else "Tradovate"),
                )
            except Exception as exc:  # pragma: no cover
                logger.debug("Tradovate polling: unable to parse order payload: %s", exc)
                continue

            if parsed_order is None or not parsed_order.identifier:
                continue

            identifier = str(parsed_order.identifier)
            seen_identifiers.add(identifier)
            stored_order = stored_orders.get(parsed_order.identifier)

            status = parsed_order.status
            status_str = status.value if isinstance(status, Order.OrderStatus) else str(status).lower()
            is_active_status = (
                status in {Order.OrderStatus.NEW, Order.OrderStatus.OPEN}
                or status_str in {"new", "submitted", "open", "working", "pending"}
            )
            if is_active_status:
                active_identifiers.add(identifier)

            if stored_order is None:
                logger.debug(f"Tradovate polling: discovered new order {identifier} (status={parsed_order.status})")
                if is_active_status:
                    self.stream.dispatch(self.NEW_ORDER, order=parsed_order)
                price, quantity = self._extract_fill_details(raw_order, parsed_order)

                if (status == Order.OrderStatus.FILLED or status_str == "filled") and price is not None and quantity is not None:
                    self.stream.dispatch(self.FILLED_ORDER, order=parsed_order, price=price, filled_quantity=quantity)
                elif (status == Order.OrderStatus.PARTIALLY_FILLED or status_str in {"partialfill", "partial_fill", "partially_filled"}) and price is not None and quantity is not None:
                    self.stream.dispatch(
                        self.PARTIALLY_FILLED_ORDER,
                        order=parsed_order,
                        price=price,
                        filled_quantity=quantity,
                    )
                elif status == Order.OrderStatus.CANCELED or status_str in {"canceled", "cancelled", "cancel"}:
                    self.stream.dispatch(self.CANCELED_ORDER, order=parsed_order)
                elif status == Order.OrderStatus.ERROR or status_str in {"error", "rejected"}:
                    error_msg = raw_order.get("failureText") or raw_order.get("failureReason")
                    self.stream.dispatch(self.ERROR_ORDER, order=parsed_order, error_msg=error_msg)
                continue

            # Refresh stored order attributes for downstream consumers.
            if parsed_order.limit_price is not None:
                stored_order.limit_price = parsed_order.limit_price
            if parsed_order.stop_price is not None:
                stored_order.stop_price = parsed_order.stop_price
            if parsed_order.avg_fill_price:
                stored_order.avg_fill_price = parsed_order.avg_fill_price
            if parsed_order.quantity:
                stored_order.quantity = parsed_order.quantity

            if not parsed_order.equivalent_status(stored_order):
                price, quantity = self._extract_fill_details(raw_order, parsed_order)

                if status == Order.OrderStatus.FILLED or status_str == "filled":
                    if price is not None and quantity is not None:
                        self.stream.dispatch(
                            self.FILLED_ORDER,
                            order=stored_order,
                            price=price,
                            filled_quantity=quantity,
                        )
                elif status == Order.OrderStatus.PARTIALLY_FILLED or status_str in {"partialfill", "partial_fill", "partially_filled"}:
                    if price is not None and quantity is not None:
                        self.stream.dispatch(
                            self.PARTIALLY_FILLED_ORDER,
                            order=stored_order,
                            price=price,
                            filled_quantity=quantity,
                        )
                elif status == Order.OrderStatus.CANCELED or status_str in {"canceled", "cancelled", "cancel"}:
                    self.stream.dispatch(self.CANCELED_ORDER, order=stored_order)
                elif status == Order.OrderStatus.ERROR or status_str in {"error", "rejected"}:
                    error_msg = raw_order.get("failureText") or raw_order.get("failureReason")
                    self.stream.dispatch(self.ERROR_ORDER, order=stored_order, error_msg=error_msg)
                else:
                    if is_active_status:
                        self.stream.dispatch(self.NEW_ORDER, order=stored_order)

        # Any active order missing from the broker response likely completed; reconcile as canceled.
        for order in list(self.get_all_orders()):
            identifier = getattr(order, "identifier", None)
            if not identifier:
                continue
            if str(identifier) not in seen_identifiers and order.is_active():
                fill_price, fill_qty = self._fetch_recent_fill_details(identifier)
                if fill_qty:
                    if fill_price is None:
                        fill_price = getattr(order, "avg_fill_price", None)
                    if fill_price is None:
                        try:
                            quote = self.get_quote(order.asset)
                            fill_price = getattr(quote, "last", None)
                        except Exception:  # pragma: no cover - defensive
                            fill_price = None

                    if fill_price is not None:
                        self.stream.dispatch(
                            self.FILLED_ORDER,
                            order=order,
                            price=float(fill_price),
                            filled_quantity=float(fill_qty),
                        )
                        continue

                logger.debug(
                    f"Tradovate polling: order {identifier} missing from broker response; dispatching CANCEL to reconcile."
                )
                self.stream.dispatch(self.CANCELED_ORDER, order=order)

        if self._first_iteration:
            self._first_iteration = False

        self._active_broker_identifiers = active_identifiers

    # ------------------------------------------------------------------
    # Order management overrides
    # ------------------------------------------------------------------
    def _mark_order_inactive_locally(self, order: Order, status: str):
        """Update internal tracking lists without dispatching noisy lifecycle logs."""
        identifier = getattr(order, "identifier", None)
        if not identifier:
            return

        safe_lists = (
            self._new_orders,
            self._unprocessed_orders,
            self._partially_filled_orders,
            self._placeholder_orders,
        )
        for safe_list in safe_lists:
            safe_list.remove(identifier, key="identifier")

        if status == self.CANCELED_ORDER:
            self._canceled_orders.remove(identifier, key="identifier")
            order.status = self.CANCELED_ORDER
            order.set_canceled()
            self._canceled_orders.append(order)
        elif status == self.FILLED_ORDER:
            self._filled_orders.remove(identifier, key="identifier")
            order.status = self.FILLED_ORDER
            order.set_filled()
            self._filled_orders.append(order)
        else:
            order.status = status

    def cancel_open_orders(self, strategy, orders: list[Order] | None = None):
        """Cancel only the orders that are still active on Tradovate; prune the rest silently."""
        tracked_orders_source = orders if orders is not None else self.get_tracked_orders(strategy)
        tracked_orders = [order for order in tracked_orders_source if order.is_active()]
        if not tracked_orders:
            self.logger.info("cancel_open_orders(strategy=%s) -> no active orders tracked", strategy)
            return

        active_ids = getattr(self, "_active_broker_identifiers", None)
        if active_ids is None:
            active_ids = self._refresh_active_identifiers_snapshot()
        orders_to_cancel: list[Order] = []
        stale_count = 0

        for order in tracked_orders:
            identifier = getattr(order, "identifier", None)
            identifier_str = str(identifier) if identifier is not None else None
            if identifier_str is not None and identifier_str not in active_ids:
                stale_count += 1
                self._mark_order_inactive_locally(order, self.CANCELED_ORDER)
                continue
            orders_to_cancel.append(order)

        if stale_count:
            self.logger.debug(
                "Tradovate cancel_open_orders removed %d stale local orders not present at broker for strategy=%s",
                stale_count,
                strategy,
            )

        if not orders_to_cancel:
            self.logger.info("cancel_open_orders(strategy=%s) -> nothing to cancel after pruning", strategy)
            return

        order_ids = [
            getattr(order, "identifier", None)
            or getattr(order, "id", None)
            or getattr(order, "order_id", None)
            for order in orders_to_cancel
        ]
        self.logger.info(
            "cancel_open_orders(strategy=%s) -> active=%d ids=%s",
            strategy,
            len(orders_to_cancel),
            order_ids,
        )
        self.cancel_orders(orders_to_cancel)

    def _refresh_active_identifiers_snapshot(self) -> set[str]:
        """Retrieve latest open orders from Tradovate to seed the active-id cache."""
        active_ids: set[str] = set()
        try:
            payloads = self._pull_broker_all_orders() or []
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.debug("Tradovate active id refresh failed: %s", exc)
            self._active_broker_identifiers = active_ids
            return active_ids

        for payload in payloads:
            order_id = payload.get("id")
            if order_id is None:
                continue
            status = str(payload.get("ordStatus", "")).lower()
            if status in {"working", "pending", "submitted", "new", "open"}:
                active_ids.add(str(order_id))

        self._active_broker_identifiers = active_ids
        return active_ids

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
            logger.error(error_msg)
            order.set_error(error_msg)
            return order

        # Check if we have valid tokens
        if not hasattr(self, 'trading_token') or not self.trading_token:
            error_msg = "Trading token not available - authentication may have failed"
            logger.error(error_msg)
            order.set_error(error_msg)
            return order

        # Determine the action based on the order side
        action = "Buy" if order.is_buy_order() else "Sell"

        # Extract symbol from the order's asset and handle continuous futures conversion
        if order.asset.asset_type == order.asset.AssetType.CONT_FUTURE:
            # For continuous futures, resolve to the specific contract symbol using Tradovate format
            symbol = self._resolve_tradovate_futures_symbol(order.asset)
            logger.info(f"Resolved continuous future {order.asset.symbol} -> {symbol}")
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
            logger.warning(
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


        try:
            response = self._request("POST", url, json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()

            # Check if the response indicates a failure
            if data.get('failureReason') or data.get('failureText'):
                failure_reason = data.get('failureReason', 'Unknown')
                failure_text = data.get('failureText', 'No details provided')
                error_message = f"Order rejected by Tradovate: {failure_reason} - {failure_text}"
                logger.error(error_message)

                # Add additional context for common errors
                if 'Access is denied' in failure_text:
                    logger.error("Possible causes: Account not authorized for trading, market closed, or insufficient permissions")
                elif 'UnknownReason' in failure_reason:
                    logger.error("Possible causes: Invalid symbol, market hours, account restrictions, or order parameters")

                order.set_error(error_message)
                return order
            else:
                # Order was successful
                order.status = Order.OrderStatus.SUBMITTED
                order.update_raw(data)
                order_id = (
                    data.get("orderId")
                    or data.get("id")
                    or (data.get("order") or {}).get("id")
                    or (data.get("orders") or {}).get("id")
                )
                if order_id is not None:
                    order.set_identifier(str(order_id))

                if hasattr(self, "_new_orders"):
                    try:
                        self._process_trade_event(order, self.NEW_ORDER)
                    except Exception:  # pragma: no cover - defensive
                        logger.error(traceback.format_exc())
                return order

        except requests.exceptions.RequestException as e:
            error_message = f"Failed to submit order: {getattr(e.response, 'status_code', None)}, {getattr(e.response, 'text', None)}"
            logger.error(error_message)
            order.set_error(error_message)
            return order

    def cancel_order(self, order) -> None:
        """Cancel an order at Tradovate and propagate lifecycle events."""
        target_order = None
        identifier = None

        if isinstance(order, Order):
            target_order = order
            identifier = order.identifier
        else:
            identifier = order

        if not identifier:
            raise ValueError("Order identifier is not set; unable to cancel order.")

        try:
            order_id_value = int(identifier)
        except (TypeError, ValueError):
            order_id_value = identifier

        payload = {
            "accountSpec": self.account_spec,
            "accountId": self.account_id,
            "orderId": order_id_value,
        }
        url = f"{self.trading_api_url}/order/cancelorder"
        headers = self._get_headers(with_content_type=True)

        try:
            response = self._request("POST", url, json=payload, headers=headers)
            response.raise_for_status()
        except requests.exceptions.RequestException as exc:
            logger.error(
                colored(
                    f"Tradovate cancel failed for order {identifier}: "
                    f"{getattr(exc.response, 'status_code', None)} {getattr(exc.response, 'text', None)}",
                    "red",
                )
            )
            raise TradovateAPIError(
                "Failed to cancel Tradovate order",
                status_code=getattr(exc.response, "status_code", None),
                response_text=getattr(exc.response, "text", None),
                original_exception=exc,
            ) from exc

        if target_order is None:
            target_order = self.get_tracked_order(identifier, use_placeholders=True)

        if target_order is not None and hasattr(self, "stream"):
            self.stream.dispatch(self.CANCELED_ORDER, order=target_order)

    def _pull_all_orders(self, strategy_name, strategy_object) -> list[Order]:
        """Skip returning legacy orders during the initial sync to avoid duplicate NEW events."""
        if getattr(self, "_first_iteration", False):
            logger.debug("Tradovate initial order sync skipped to allow polling to reconcile legacy orders")
            return []
        return super()._pull_all_orders(strategy_name, strategy_object)

    def _modify_order(self, order: Order, limit_price: Union[float, None] = None,
                      stop_price: Union[float, None] = None):
        logger.error(colored(f"Method '_modify_order' for order {order} is not yet implemented.", "red"))
        return None

    def get_historical_account_value(self) -> dict:
        logger.error(colored("Method 'get_historical_account_value' is not yet implemented.", "red"))
        return {}
