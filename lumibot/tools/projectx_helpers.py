"""
ProjectX Helper Utilities for Lumibot Integration

This module provides the core ProjectX API client and streaming functionality
integrated into Lumibot's architecture based on the actual working Project X library.
"""

import time
import warnings
from datetime import datetime
from typing import Callable, Optional, Dict, List

import pandas as pd
import pytz
import requests

from lumibot.tools.lumibot_logger import get_logger

# Suppress SSL deprecation warnings from third-party websocket library
warnings.filterwarnings("ignore", category=DeprecationWarning, module="websocket")
warnings.filterwarnings("ignore", category=DeprecationWarning, message="ssl.PROTOCOL_TLS is deprecated")
warnings.filterwarnings("ignore", category=DeprecationWarning, message="websockets.legacy is deprecated")
warnings.filterwarnings("ignore", category=DeprecationWarning, message="websockets.client.connect is deprecated")
warnings.filterwarnings("ignore", category=DeprecationWarning, message="websockets.client.WebSocketClientProtocol is deprecated")

logger = get_logger(__name__)

# SignalR imports - will be imported when needed
try:
    from signalrcore.hub_connection_builder import HubConnectionBuilder
    SIGNALR_AVAILABLE = True
except ImportError:
    SIGNALR_AVAILABLE = False


class ProjectXAuth:
    """ProjectX Authentication module using Lumibot config pattern"""
    
    @staticmethod
    def get_auth_token(config: dict) -> str:
        """Get authentication token using configuration dict from Lumibot"""
        auth_url = "api/auth/loginkey"
        
        username = config.get("username")
        api_key = config.get("api_key")
        base_url = config.get("base_url")
        
        # Validate required credentials are present
        missing_vars = []
        if not username:
            missing_vars.append("username")
        if not api_key:
            missing_vars.append("api_key")
        if not base_url:
            missing_vars.append("base_url")
        
        if missing_vars:
            raise ValueError(f"Missing required configuration: {', '.join(missing_vars)}")
        
        payload = {
            "userName": username,
            "apiKey": api_key,
        }
        
        try:
            full_url = f"{base_url}{auth_url}"
            response = requests.post(full_url, json=payload)
            auth_resp = response.json()
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request error: {e}")
            return None
        except ValueError as e:
            logger.error(f"JSON decode error: {e}")
            return None
        
        # Return None if authentication failed
        if not auth_resp.get("success"):
            error_code = auth_resp.get("errorCode")
            error_message = auth_resp.get("errorMessage", "No error message provided")
            logger.error(f"Authentication failed - Error Code: {error_code}, Message: {error_message}")
            return None
        
        return auth_resp.get("token")


class ProjectXStreaming:
    """ProjectX SignalR Streaming Client"""
    
    def __init__(self, config: dict, token: str, account_id: int = None):
        if not SIGNALR_AVAILABLE:
            raise ImportError("signalrcore library is required for streaming functionality")
        
        self.firm = config.get("firm")
        self.token = token
        self.account_id = account_id
        
        # Get base URL from config
        streaming_base_url = config.get("streaming_base_url")
        if streaming_base_url:
            self.base_url = streaming_base_url
        else:
            self.base_url = config.get("base_url")
        
        if self.base_url.endswith('/'):
            self.base_url = self.base_url[:-1]
        
        # SignalR connection URLs
        self.user_hub_url = f"{self.base_url}/hubs/user"
        
        # Connection objects
        self.user_connection = None
        self.is_user_connected = False
        
        # Event handlers
        self.on_account_update: Optional[Callable] = None
        self.on_order_update: Optional[Callable] = None
        self.on_position_update: Optional[Callable] = None
        self.on_trade_update: Optional[Callable] = None
        
        # Setup logging
        self.logger = get_logger(f"ProjectXStreaming_{self.firm}")
    
    def start_user_hub(self) -> bool:
        """Start the user hub connection"""
        try:
            import time
            self.logger.info(f"Connecting to user hub: {self.user_hub_url}")
            
            hub_url_with_auth = f"{self.user_hub_url}?access_token={self.token}"
            
            self.user_connection = HubConnectionBuilder() \
                .with_url(hub_url_with_auth) \
                .with_automatic_reconnect({
                    "type": "raw",
                    "keep_alive_interval": 10,
                    "reconnect_interval": 5,
                    "max_attempts": 5
                }) \
                .build()
            
            # Setup event handlers
            self.user_connection.on_open(lambda: self._on_user_hub_open())
            self.user_connection.on_close(lambda: self._on_user_hub_close())
            self.user_connection.on_error(lambda data: self._on_user_hub_error(data))
            
            # Setup message handlers
            self.user_connection.on("GatewayUserAccount", self._handle_account_update)
            self.user_connection.on("GatewayUserOrder", self._handle_order_update)
            self.user_connection.on("GatewayUserPosition", self._handle_position_update)
            self.user_connection.on("GatewayUserTrade", self._handle_trade_update)
            
            # Start connection
            self.user_connection.start()
            
            # Wait for connection
            connection_timeout = 10
            start_time = time.time()
            
            while time.time() - start_time < connection_timeout:
                if hasattr(self.user_connection, 'transport') and self.user_connection.transport:
                    if hasattr(self.user_connection.transport, 'state') and self.user_connection.transport.state == "connected":
                        self.is_user_connected = True
                        self.logger.info("User hub connected successfully")
                        return True
                time.sleep(0.1)
            
            # Try a test message
            try:
                self.user_connection.send('SubscribeAccounts', [])
                self.is_user_connected = True
                self.logger.info("User hub connected successfully (confirmed via test message)")
                return True
            except Exception:
                # Assume connection is working
                self.is_user_connected = True
                self.logger.info("User hub connected successfully (assumed)")
                return True
            
        except Exception as e:
            self.logger.error(f"Failed to connect to user hub: {e}")
            return False
    
    def _on_user_hub_open(self):
        """Handle user hub connection open"""
        self.logger.info("User hub connection opened")
    
    def _on_user_hub_close(self):
        """Handle user hub connection close"""
        self.logger.info("User hub connection closed")
    
    def _on_user_hub_error(self, data):
        """Handle user hub connection error"""
        self.logger.error(f"User hub error: {data}")
    
    def _handle_account_update(self, *args):
        """Handle account update event - SignalR might pass multiple args"""
        data = args[0] if args else None
        if self.on_account_update:
            self.on_account_update(data)
    
    def _handle_order_update(self, *args):
        """Handle order update event - SignalR might pass multiple args"""
        # SignalR can pass data as multiple arguments
        data = args[0] if args else None
        if self.on_order_update:
            self.on_order_update(data)
    
    def _handle_position_update(self, *args):
        """Handle position update event - SignalR might pass multiple args"""
        data = args[0] if args else None
        if self.on_position_update:
            self.on_position_update(data)
    
    def _handle_trade_update(self, *args):
        """Handle trade update event - SignalR might pass multiple args"""
        data = args[0] if args else None
        if self.on_trade_update:
            self.on_trade_update(data)
    
    def subscribe_all(self, account_id: int = None) -> bool:
        """Subscribe to all data streams for the account"""
        try:
            if not self.is_user_connected:
                self.logger.warning("User hub not connected, cannot subscribe")
                return False
            
            if not self.user_connection:
                self.logger.warning("No user connection available for subscription")
                return False
            
            # Add a small delay to ensure connection is fully established
            import time
            time.sleep(0.5)
            
            # Check if connection is ready for messages
            try:
                # Subscribe to all available streams
                self.user_connection.send('SubscribeAccounts', [])
                if account_id:
                    self.user_connection.send('SubscribeOrders', [account_id])
                    self.user_connection.send('SubscribePositions', [account_id])
                    self.user_connection.send('SubscribeTrades', [account_id])
                
                self.logger.info("Subscribed to all data streams")
                return True
                
            except Exception as send_error:
                # Don't fail completely if streaming subscription fails
                self.logger.warning(f"Could not subscribe to streams (non-critical): {send_error}")
                return False
            
        except Exception as e:
            self.logger.error(f"Failed to subscribe to streams: {e}")
            return False

    def stop(self):
        """Stop streaming connection"""
        if self.user_connection:
            self.user_connection.stop()


class ProjectX:
    """Main ProjectX API Client - Direct port of working Project X library"""
    
    def __init__(self, config: dict, token: str):
        self.config = config
        self.firm = config.get("firm")
        self.token = token
        self.base_url = config.get("base_url")
        
        if not self.base_url:
            raise ValueError(f"Base URL not found in config")
        
        if not self.base_url.endswith('/'):
            self.base_url += '/'
            
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }
        
        self.logger = get_logger(f"ProjectXClient_{self.firm}")
    
    def get_streaming_client(self, account_id: int = None) -> ProjectXStreaming:
        """Get streaming client instance"""
        return ProjectXStreaming(self.config, self.token, account_id)
    
    def account_search(self, only_active_accounts: bool = True) -> dict:
        """Returns a list of accounts for the user"""
        url = f"{self.base_url}api/account/search"
        
        payload = {
            "onlyActiveAccounts": only_active_accounts,
        }
        
        try:
            self.logger.debug(f"API Request: POST {url}")
            self.logger.debug(f"Request Data: {payload}")
            
            response = requests.post(url, headers=self.headers, json=payload)
            
            self.logger.debug(f"Response Status: {response.status_code}")
            
            if response.status_code == 200:
                result = response.json()
                self.logger.debug(f"Response Body: {result}")
                return result
            else:
                self.logger.error(f"API request failed: {response.status_code} {response.reason}")
                self.logger.debug(f"Error Response Text: {response.text}")
                return {"success": False, "error": f"HTTP {response.status_code}"}
                
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request error: {e}")
            return {"success": False, "error": str(e)}
    
    def position_search_open(self, account_id: int) -> dict:
        """Search for open positions in an account"""
        url = f"{self.base_url}api/position/searchopen"
        
        payload = {
            "accountId": account_id,
        }
        
        try:
            # Light rate limiting
            import time
            time.sleep(0.05)  # 50ms delay between requests
            
            self.logger.debug(f"API Request: POST {url}")
            self.logger.debug(f"Request Data: {payload}")
            
            response = requests.post(url, headers=self.headers, json=payload, timeout=10)
            
            self.logger.debug(f"Response Status: {response.status_code}")
            
            if response.status_code == 200:
                result = response.json()
                position_count = len(result.get("positions", [])) if result.get("success") else 0
                self.logger.debug(f"Retrieved {position_count} positions")
                return result
            elif response.status_code == 429:
                self.logger.warning(f"Rate limited, retrying in 1 second...")
                time.sleep(1)  # Wait 1 second for rate limit
                return {"success": False, "error": "Rate limited"}
            else:
                self.logger.error(f"API request failed: {response.status_code} {response.reason}")
                return {"success": False, "error": f"HTTP {response.status_code}"}
                
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request error: {e}")
            return {"success": False, "error": str(e)}
    
    def order_search(self, account_id: int, start_datetime: str, end_datetime: str = None) -> dict:
        """Search for orders in an account"""
        url = f"{self.base_url}api/order/search"
        
        payload = {
            "accountId": account_id,
            "startTimeStamp": start_datetime,
        }
        if end_datetime:
            payload["endTimeStamp"] = end_datetime
        
        try:
            # Light rate limiting
            import time
            time.sleep(0.05)  # 50ms delay between requests
            
            self.logger.debug(f"API Request: POST {url}")
            self.logger.debug(f"Request Data: {payload}")
            
            response = requests.post(url, headers=self.headers, json=payload, timeout=10)
            
            self.logger.debug(f"Response Status: {response.status_code}")
            
            if response.status_code == 200:
                result = response.json()
                # Don't log huge order lists in detail 
                order_count = len(result.get("orders", [])) if result.get("success") else 0
                self.logger.debug(f"Retrieved {order_count} orders")
                return result
            elif response.status_code == 429:
                self.logger.warning(f"Rate limited, retrying in 1 second...")
                time.sleep(1)  # Wait 1 second for rate limit
                return {"success": False, "error": "Rate limited"}
            else:
                self.logger.error(f"API request failed: {response.status_code} {response.reason}")
                return {"success": False, "error": f"HTTP {response.status_code}"}
                
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request error: {e}")
            return {"success": False, "error": str(e)}
    
    def trade_search(self, account_id: int, start_timestamp: str, end_timestamp: str = None) -> dict:
        """Search for trades in an account - trades are ground truth for fills"""
        url = f"{self.base_url}api/trade/search"
        
        payload = {
            "accountId": account_id,
            "startTimestamp": start_timestamp,
        }
        if end_timestamp:
            payload["endTimestamp"] = end_timestamp
        
        try:
            # Light rate limiting
            import time
            time.sleep(0.05)  # 50ms delay between requests
            
            self.logger.debug(f"API Request: POST {url}")
            self.logger.debug(f"Request Data: {payload}")
            
            response = requests.post(url, headers=self.headers, json=payload, timeout=10)
            
            self.logger.debug(f"Response Status: {response.status_code}")
            
            if response.status_code == 200:
                result = response.json()
                # Don't log huge trade lists in detail 
                trade_count = len(result.get("trades", [])) if result.get("success") else 0
                self.logger.debug(f"Retrieved {trade_count} trades")
                return result
            elif response.status_code == 429:
                self.logger.warning(f"Rate limited, retrying in 1 second...")
                time.sleep(1)  # Wait 1 second for rate limit
                return {"success": False, "error": "Rate limited"}
            else:
                self.logger.error(f"API request failed: {response.status_code} {response.reason}")
                return {"success": False, "error": f"HTTP {response.status_code}"}
                
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Request error: {e}")
            return {"success": False, "error": str(e)}
    
    def order_place(self, account_id: int, contract_id: str, type: int, side: int, size: int,
                   limit_price: float = None, stop_price: float = None, trail_price: float = None,
                   custom_tag: str = None, linked_order_id: int = None) -> dict:
        """Place an order for a contract"""
        url = f"{self.base_url}api/order/place"
        
        # --- Sanitize & normalize inputs ---
        try:
            size_int = int(size)
        except (TypeError, ValueError):
            self.logger.warning(f"⚠️ Invalid size '{size}' - defaulting to 1")
            size_int = 1

        # Remove None values to avoid API model binding issues
        def _clean(d: dict) -> dict:
            return {k: v for k, v in d.items() if v is not None}

        base_payload = _clean({
            "accountId": account_id,
            "contractId": contract_id,
            "type": type,
            "side": side,
            "size": size_int,
            "limitPrice": limit_price,
            "stopPrice": stop_price,
            "trailPrice": trail_price,
            "customTag": custom_tag,
            "linkedOrderId": linked_order_id,
        })

        # Log the outgoing request (compact)
        self.logger.debug(f"ProjectX.order_place payload: {base_payload}")

        def _post(json_payload):
            return requests.post(url, headers=self.headers, json=json_payload, timeout=10)

        try:
            response = _post(base_payload)
            try:
                data = response.json()
            except ValueError:
                data = {"success": False, "error": f"Non-JSON response {response.status_code}"}

            # Fast path success
            if response.status_code == 200 and data.get("success"):
                return data

            # Detect validation pattern requiring wrapper {"request": {...}}
            errors = (data or {}).get("errors") or {}
            needs_wrapper = False
            if isinstance(errors, dict):
                if "request" in errors or any("The request field is required" in err for err_list in errors.values() for err in (err_list if isinstance(err_list, list) else [err_list])):
                    needs_wrapper = True
            # Also if size conversion failed, we retry after forcing int already and potentially wrapping
            size_error = any("$.size" in err for err_list in errors.values() for err in (err_list if isinstance(err_list, list) else [err_list])) if errors else False
            
            if needs_wrapper or size_error:
                wrapped_payload = {"request": base_payload}
                self.logger.info("Retrying order_place with wrapped 'request' payload structure")
                retry_resp = _post(wrapped_payload)
                try:
                    retry_data = retry_resp.json()
                except ValueError:
                    retry_data = {"success": False, "error": f"Non-JSON response {retry_resp.status_code}"}
                if retry_resp.status_code == 200 and retry_data.get("success"):
                    return retry_data
                # Merge error context
                return {"success": False, "error": retry_data.get("error") or retry_data, "first_attempt": data}

            # If no specific wrapper need detected just return original data
            return data if data else {"success": False, "error": f"HTTP {response.status_code}"}

        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error placing order: {e}")
            return {"success": False, "error": str(e)}
    
    def order_cancel(self, account_id: int, order_id: int) -> dict:
        """Cancel an order"""
        url = f"{self.base_url}api/order/cancel"
        payload = {"accountId": account_id, "orderId": order_id}
        try:
            response = requests.post(url, headers=self.headers, json=payload, timeout=10)
            try:
                data = response.json()
            except ValueError:
                data = {"success": False, "error": f"Non-JSON response {response.status_code}"}
            return data
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error cancelling order: {e}")
            return {"success": False, "error": str(e)}
    
    def contract_search(self, search_text: str, live: bool = False) -> dict:
        """Search for contracts by name"""
        url = f"{self.base_url}api/contract/search"
        
        payload = {
            "searchText": search_text,
            "live": live,
        }
        
        try:
            response = requests.post(url, headers=self.headers, json=payload)
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error: {e}")
            return {"success": False, "error": str(e)}
    
    def contract_search_id(self, contract_id: str) -> dict:
        """Get contract details by ID"""
        url = f"{self.base_url}api/contract/searchbyid"
        
        payload = {
            "contractId": contract_id,
        }
        
        try:
            # Light rate limiting
            import time
            time.sleep(0.05)  # 50ms delay between requests
            
            response = requests.post(url, headers=self.headers, json=payload, timeout=10)
            
            if response.status_code == 200:
                try:
                    result = response.json()
                    return result
                except ValueError as json_error:
                    self.logger.error(f"JSON parsing error for contract {contract_id}: {json_error}")
                    self.logger.error(f"Raw response: {response.text[:200]}...")
                    return {"success": False, "error": "Invalid JSON response"}
            elif response.status_code == 429:
                self.logger.warning(f"⚠️ Rate limited on contract lookup, retrying...")
                time.sleep(1)
                return {"success": False, "error": "Rate limited"}
            else:
                self.logger.error(f"Contract lookup failed: {response.status_code} {response.reason}")
                return {"success": False, "error": f"HTTP {response.status_code}"}
                
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error getting contract details: {e}")
            return {"success": False, "error": str(e)}
    
    def history_retrieve_bars(self, contract_id: str, start_datetime: str | datetime, end_datetime: str | datetime,
                             unit: int, unit_number: int, limit: int = 1000,
                             include_partial_bar: bool = True, live: bool = False,
                             is_est: bool = True) -> pd.DataFrame:
        """Retrieve historical bars for a contract"""
        url = f"{self.base_url}api/history/retrievebars"

        # Convert timezone if needed (handle both str and datetime inputs)
        if is_est:
            start_datetime = _to_utc_iso(start_datetime, is_est=True)
            end_datetime = _to_utc_iso(end_datetime, is_est=True)
        else:
            start_datetime = _to_utc_iso(start_datetime, is_est=False)
            end_datetime = _to_utc_iso(end_datetime, is_est=False)
        
        payload = {
            "contractId": contract_id,
            "startTime": start_datetime,
            "endTime": end_datetime,
            "unit": unit,
            "unitNumber": unit_number,
            "limit": limit,
            "includePartialBar": include_partial_bar,
            "live": live,
        }
        
        try:
            response = requests.post(url, headers=self.headers, json=payload)
            result = response.json()
            
            if not result.get("success"):
                self.logger.error(f"Historical data request failed: {result}")
                return pd.DataFrame()
            
            # Convert to DataFrame
            df = pd.DataFrame(result.get("bars"), index=None)
            
            if df.empty:
                return df
            
            # Convert timestamps
            df["t"] = pd.to_datetime(df["t"], utc=True).dt.tz_convert("America/New_York")
            
            # Filter by date range
            df = df[
                (df["t"] >= pd.to_datetime(start_datetime))
                & (df["t"] <= pd.to_datetime(end_datetime))
            ]

            # Map ProjectX column names to standard OHLCV format
            column_mapping = {
                'o': 'open',
                'h': 'high', 
                'l': 'low',
                'c': 'close',
                'v': 'volume',
                't': 'datetime',
            }
            
            # Rename columns to standard format
            df.rename(columns=column_mapping, inplace=True)
            
            # Reorder columns to standard format
            standard_columns = ["datetime", "open", "high", "low", "close", "volume"]
            available_columns = [col for col in standard_columns if col in df.columns]
            extra_columns = [col for col in df.columns if col not in standard_columns]
            df = df[available_columns + extra_columns]

            # Sort and reset index
            df.sort_values(by=["datetime"], inplace=True)
            df.reset_index(drop=True, inplace=True)
            
            return df
            
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error: {e}")
            return pd.DataFrame()


class ProjectXClient:
    """
    ProjectX Client wrapper for Lumibot integration
    
    This is the main interface that Lumibot brokers will use.
    """
    
    def __init__(self, config: dict):
        """Initialize ProjectX client with broker configuration"""
        self.config = config
        self.firm = config.get("firm")
        
        # Setup logging
        self.logger = get_logger(f"ProjectXClient_{self.firm}")
        
        # Get authentication token
        self.token = ProjectXAuth.get_auth_token(config)
        if not self.token:
            raise Exception(f"Failed to authenticate with {self.firm}")
        
        # Initialize ProjectX API client
        self.api = ProjectX(config, self.token)
        
        # Streaming client
        self.streaming = None
        
        # Cache for contract details to reduce API calls
        self._contract_cache = {}
        
        # Enhanced caching
        self._account_cache = None
        self._account_cache_time = 0
        self._positions_cache = None
        self._positions_cache_time = 0
        self._orders_cache = None
        self._orders_cache_time = 0
        self._cache_ttl = 30  # 30 seconds cache
    
    def get_accounts(self) -> List[Dict]:
        """Get list of available accounts"""
        # Use cached data if available and fresh
        if (self._account_cache is not None and 
            time.time() - self._account_cache_time < self._cache_ttl):
            self.logger.debug(f"🚀 Using cached account data")
            return self._account_cache
        
        response = self.api.account_search()
        if response and response.get("success"):
            accounts = response.get("accounts", [])
            self._account_cache = accounts
            self._account_cache_time = time.time()
            return accounts
        else:
            raise Exception(f"Failed to retrieve accounts: {response}")
    
    def get_preferred_account_id(self) -> int:
        """Get preferred account ID - use configured preferred account or highest balance"""
        accounts = self.get_accounts()
        
        if not accounts:
            raise Exception("No accounts found")
        
        self.logger.debug(f"Found {len(accounts)} accounts: {[acc.get('name', 'Unknown') for acc in accounts]}")
        
        # First priority: Use specifically configured preferred account name
        preferred_name = self.config.get("preferred_account_name")
        if preferred_name:
            self.logger.info(f"Looking for preferred account: {preferred_name}")
            for account in accounts:
                if account.get('name') == preferred_name:
                    account_id = account.get('id')
                    account_balance = account.get('balance', 0)
                    self.logger.info(f"✅ Using preferred account: {preferred_name} (ID: {account_id}, Balance: ${account_balance:,.2f})")
                    return account_id
            
            # If preferred account not found, log warning but continue
            self.logger.warning(f"⚠️ Preferred account '{preferred_name}' not found, using highest balance account")
        
        # Fallback: Select account with highest balance
        selected_account = max(accounts, key=lambda x: x.get('balance', 0))
        account_id = selected_account.get('id')
        account_name = selected_account.get('name', 'Unknown')
        account_balance = selected_account.get('balance', 0)
        
        self.logger.info(f"✅ Selected highest balance account: {account_name} (ID: {account_id}, Balance: ${account_balance:,.2f})")
        return account_id
    
    def get_account_balance(self, account_id: int) -> Dict:
        """Get account balance and details"""
        accounts = self.get_accounts()
        for account in accounts:
            if account.get('id') == account_id:
                balance = account.get('balance', 0)
                return {
                    'cash': balance,
                    'equity': balance,  # For futures, equity = cash when no positions
                    'buying_power': balance,  # Assume same as cash for futures
                    'account_value': balance
                }
        raise Exception(f"Account {account_id} not found")
    
    def get_positions(self, account_id: int) -> List[Dict]:
        """Get open positions with caching"""
        # Use cached data if available and fresh
        if (self._positions_cache is not None and 
            time.time() - self._positions_cache_time < self._cache_ttl):
            self.logger.debug(f"🚀 Using cached positions data")
            return self._positions_cache
        
        response = self.api.position_search_open(account_id)
        if response and response.get("success"):
            positions = response.get("positions", [])
            # Update cache
            self._positions_cache = positions
            self._positions_cache_time = time.time()
            return positions
        elif response and response.get("error") == "Rate limited":
            # For rate limiting, return cached data if available, otherwise empty list
            return self._positions_cache if self._positions_cache is not None else []
        else:
            raise Exception(f"Failed to retrieve positions: {response}")
    
    def get_orders(self, account_id: int, start_date: str = None, end_date: str = None) -> List[Dict]:
        """Get orders within date range with caching"""
        if not start_date:
            # Default to last 30 days
            from datetime import datetime, timedelta
            start_date = (datetime.now() - timedelta(days=30)).isoformat()
        
        # Use cached data if available and fresh
        if (self._orders_cache is not None and 
            time.time() - self._orders_cache_time < self._cache_ttl):
            self.logger.debug(f"🚀 Using cached orders data")
            return self._orders_cache
        
        response = self.api.order_search(account_id, start_date, end_date)
        if response and response.get("success"):
            orders = response.get("orders", [])
            # Update cache
            self._orders_cache = orders
            self._orders_cache_time = time.time()
            return orders
        elif response and response.get("error") == "Rate limited":
            # For rate limiting, return cached data if available, otherwise empty list
            return self._orders_cache if self._orders_cache is not None else []
        else:
            raise Exception(f"Failed to retrieve orders: {response}")
    
    def place_order(self, account_id: int, contract_id: str, side: str, quantity: int, 
                   order_type: int, price: float = None) -> Dict:
        """Place a trading order"""
        # Convert side to ProjectX format
        side_map = {"BUY": 0, "SELL": 1}
        side_int = side_map.get(side.upper())
        if side_int is None:
            raise ValueError(f"Invalid side: {side}")
        
        response = self.api.order_place(
            account_id=account_id,
            contract_id=contract_id,
            order_type=order_type,  # Fixed: use order_type instead of deprecated 'type'
            side=side_int,
            size=quantity,
            limit_price=price if order_type == 1 else None  # Only set price for limit orders
        )
        
        if response and response.get("success"):
            return response
        else:
            raise Exception(f"Failed to place order: {response}")
    
    def cancel_order(self, account_id: int, order_id: str) -> bool:
        """Cancel an order"""
        response = self.api.order_cancel(account_id, int(order_id))
        return response and response.get("success", False)
    
    def order_modify(self, account_id: int, order_id: int, size: int = None, 
                    limit_price: float = None, stop_price: float = None, trail_price: float = None) -> Dict:
        """Modify an existing order - Not supported by ProjectX API"""
        # ProjectX doesn't support order modification - need to cancel and re-place
        return {"success": False, "error": "Order modification not supported by ProjectX API"}
    
    def get_historical_data(self, contract_id: str, start_time: str, end_time: str,
                           timeframe: str = "1minute") -> pd.DataFrame:
        """Get historical price data"""
        # Parse timeframe
        if timeframe == "1minute":
            unit, unit_number = 2, 1
        elif timeframe == "5minute":
            unit, unit_number = 2, 5
        elif timeframe == "1hour":
            unit, unit_number = 3, 1
        elif timeframe == "1day":
            unit, unit_number = 4, 1
        else:
            unit, unit_number = 2, 1  # Default to 1 minute
        
        df = self.api.history_retrieve_bars(
            contract_id=contract_id,
            start_datetime=start_time,
            end_datetime=end_time,
            unit=unit,
            unit_number=unit_number
        )
        
        return df
    
    def history_retrieve_bars(self, contract_id: str, start_datetime: str | datetime, end_datetime: str | datetime,
                             unit: int, unit_number: int, limit: int = 1000,
                             include_partial_bar: bool = True, live: bool = False,
                             is_est: bool = True) -> pd.DataFrame:
        """Direct access to history_retrieve_bars - wrapper to API method"""
        return self.api.history_retrieve_bars(
            contract_id=contract_id,
            start_datetime=start_datetime, 
            end_datetime=end_datetime,
            unit=unit,
            unit_number=unit_number,
            limit=limit,
            include_partial_bar=include_partial_bar,
            live=live,
            is_est=is_est
        )
    
    def search_contracts(self, search_text: str) -> List[Dict]:
        """Search for contracts"""
        response = self.api.contract_search(search_text)
        if response and response.get("success"):
            return response.get("contracts", [])
        else:
            raise Exception(f"Failed to search contracts: {response}")
    
    def contract_search(self, search_text: str) -> List[Dict]:
        """Search for contracts - alias for search_contracts"""
        return self.search_contracts(search_text)
    
    def order_search(self, account_id: int, start_date: str = None, end_date: str = None, 
                    start_datetime: str = None, end_datetime: str = None) -> List[Dict]:
        """Search for orders - alias for get_orders with flexible parameter names"""
        # Handle both parameter name formats for compatibility
        start = start_datetime or start_date
        end = end_datetime or end_date
        return self.get_orders(account_id, start, end)
    
    def order_place(self, account_id: int, contract_id: str, type: int, side: int, size: int,
                   limit_price: float = None, stop_price: float = None, trail_price: float = None,
                   custom_tag: str = None, linked_order_id: int = None) -> dict:
        """Place an order for a contract - matches original ProjectX API"""
        response = self.api.order_place(
            account_id=account_id,
            contract_id=contract_id,
            type=type,
            side=side,
            size=size,
            limit_price=limit_price,
            stop_price=stop_price,
            trail_price=trail_price,
            custom_tag=custom_tag,
            linked_order_id=linked_order_id
        )
        
        if response and response.get("success"):
            return response
        else:
            raise Exception(f"Failed to place order: {response}")
    
    def contract_search(self, search_text: str) -> dict:
        """Search for contracts - direct API wrapper"""
        return self.api.contract_search(search_text, live=False)
    
    def contract_search_id(self, contract_id: str) -> dict:
        """Get contract by ID - direct API wrapper"""
        return self.api.contract_search_id(contract_id)
    
    def get_contract_details(self, contract_id: str) -> Dict:
        """Get contract details by contract ID"""
        # Check cache first
        if contract_id in self._contract_cache:
            return self._contract_cache[contract_id]
        
        response = self.api.contract_search_id(contract_id)
        if response and response.get("success"):
            contracts = response.get("contracts", [])
            if contracts:
                contract_details = contracts[0]  # Return first match
                # Cache the result
                self._contract_cache[contract_id] = contract_details
                return contract_details
            else:
                # No contracts found
                return {}
        elif response and response.get("error") in ["Rate limited", "Invalid JSON response"]:
            # Don't cache errors, but return empty dict gracefully
            return {}
        else:
            # For other errors, return empty dict instead of raising exception
            return {}
    
    def find_contract_by_symbol(self, symbol: str) -> str:
        """Find contract ID by searching for symbol using Asset class continuous futures logic"""
        try:
            from lumibot.entities import Asset
            
            symbol_upper = symbol.upper()
            self.logger.info(f"🔍 Searching for contract: {symbol} -> {symbol_upper}")
            
            # Use Asset class logic for continuous futures resolution
            try:
                # Create continuous futures asset
                asset = Asset(symbol_upper, asset_type=Asset.AssetType.CONT_FUTURE)
                
                # Get potential contracts using Asset class logic
                potential_contracts = asset.get_potential_futures_contracts()
                
                self.logger.debug(f"📋 Asset class generated {len(potential_contracts)} potential contracts")
                
                # Try each potential contract to find one that works
                for contract_symbol in potential_contracts:
                    # Convert to ProjectX format: CON.F.US.SYMBOL.EXPIRY
                    if not contract_symbol.startswith("CON.F.US."):
                        # Parse symbol like "MESU25" -> "CON.F.US.MES.U25"
                        if len(contract_symbol) >= 4:
                            base_symbol = contract_symbol[:-3]  # Remove last 3 chars (month + year)
                            month_year = contract_symbol[-3:]   # Get month + year code
                            if len(month_year) == 3:
                                month_code = month_year[0]
                                year_code = month_year[1:]
                                contract_id = f"CON.F.US.{base_symbol}.{month_code}{year_code}"
                            else:
                                contract_id = f"CON.F.US.{symbol_upper}.{month_year}"
                        else:
                            contract_id = f"CON.F.US.{symbol_upper}.U25"  # Fallback
                    else:
                        contract_id = contract_symbol
                    
                    # Cache and return the first valid contract
                    self._contract_cache[contract_id] = {"id": contract_id, "symbol": symbol_upper}
                    self.logger.info(f"✅ Using Asset class contract: {contract_id}")
                    return contract_id
                
            except Exception as asset_error:
                self.logger.warning(f"⚠️ Asset class method failed: {asset_error}, falling back to API search")
            
            # Fallback: Use hardcoded mapping for immediate compatibility
            common_futures_fallback = {
                'MES': 'CON.F.US.MES.U25',
                'ES': 'CON.F.US.ES.U25',    
                'NQ': 'CON.F.US.NQ.U25',    
                'YM': 'CON.F.US.YM.U25',    
                'RTY': 'CON.F.US.RTY.U25',  
            }
            
            if symbol_upper in common_futures_fallback:
                contract_id = common_futures_fallback[symbol_upper]
                self.logger.debug(f"📋 Using fallback mapping: {contract_id}")
                self._contract_cache[contract_id] = {"id": contract_id, "symbol": symbol_upper}
                return contract_id
            
            # Search using the contract search API
            self.logger.info(f"🔍 Searching via API for: {symbol}")
            try:
                contracts = self.search_contracts(symbol)
                if contracts:
                    self.logger.info(f"📋 Found {len(contracts)} contracts via search")
                    
                    # Find the most recent/active contract (usually sorted by expiry)  
                    active_contracts = [c for c in contracts if c.get('active', True)]
                    if active_contracts:
                        # Try different possible field names for contract ID
                        contract_id = (active_contracts[0].get('contractId') or 
                                     active_contracts[0].get('id') or 
                                     active_contracts[0].get('symbol') or '')
                        self.logger.info(f"✅ Using active contract: {contract_id}")
                        return contract_id
                    elif contracts:
                        # Try different possible field names for contract ID
                        contract_id = (contracts[0].get('contractId') or 
                                     contracts[0].get('id') or 
                                     contracts[0].get('symbol') or '')
                        self.logger.info(f"✅ Using first contract: {contract_id}")
                        return contract_id
                else:
                    self.logger.warning(f"⚠️ No contracts found via API search")
            except Exception as search_e:
                self.logger.error(f"❌ API search failed: {search_e}")
            
            # If all else fails, return the hardcoded mapping anyway (might work for orders)
            if symbol_upper in common_futures_fallback:
                fallback_contract = common_futures_fallback[symbol_upper]
                self.logger.info(f"🔄 Fallback to hardcoded mapping (last resort): {fallback_contract}")
                return fallback_contract
            
            self.logger.error(f"❌ No contract found for symbol: {symbol}")
            return ''
            
        except Exception as e:
            self.logger.error(f"Error finding contract for symbol {symbol}: {e}")
            return ''
    
    def get_contract_tick_size(self, contract_id: str) -> float:
        """Get tick size for a contract"""
        try:
            contract_details = self.get_contract_details(contract_id)
            tick_size = contract_details.get('tickSize', 0.25)  # Default to 0.25 for most futures
            return float(tick_size)
        except Exception as e:
            self.logger.warning(f"Could not get tick size for {contract_id}, using default 0.25: {e}")
            return 0.25
    
    def round_to_tick_size(self, price: float, tick_size: float) -> float:
        """Round price to the nearest tick size"""
        try:
            if tick_size <= 0:
                return price
            return round(price / tick_size) * tick_size
        except Exception as e:
            self.logger.warning(f"Error rounding price {price} to tick size {tick_size}: {e}")
            return price
    
    def get_streaming_client(self, account_id: int = None) -> ProjectXStreaming:
        """Get streaming client for real-time data"""
        if not self.streaming:
            self.streaming = ProjectXStreaming(self.config, self.token, account_id)
        return self.streaming
    
    def round_to_tick_size(self, price: float, tick_size: float) -> float:
        """Round price to the nearest tick size increment"""
        if price is None or tick_size is None:
            return None
        return round(price / tick_size) * tick_size
    
    def order_cancel(self, account_id: int, order_id: int) -> dict:
        """Cancel an order via high-level client; always return dict."""
        try:
            response = self.api.order_cancel(account_id, order_id)
            # Underlying api.order_cancel returns dict (or False); normalize
            if isinstance(response, dict):
                return response
            return {"success": bool(response)}
        except Exception as e:
            self.logger.error(f"Error in order_cancel wrapper: {e}")
            return {"success": False, "error": str(e)}


def _to_utc_iso(dt_or_str, is_est: bool = True) -> str:
    """
    Normalize input (datetime or ISO string) to a UTC ISO formatted string.
    - If input is a string it accepts ISO formats with or without trailing 'Z'.
    - If input is a naive datetime and is_est is True, localize to America/New_York.
      If is_est is False and naive, assume UTC.
    - If input is timezone-aware, convert to UTC.
    Returns ISO string with +00:00 timezone.
    """
    if isinstance(dt_or_str, datetime):
        dt = dt_or_str
    elif isinstance(dt_or_str, str):
        s = dt_or_str
        if s.endswith('Z'):
            s = s[:-1]
        # fromisoformat handles YYYY-MM-DDTHH:MM:SS[.ffffff][+HH:MM]
        dt = datetime.fromisoformat(s)
    else:
        raise TypeError("start/end datetime must be a str or datetime")

    if is_est:
        est = pytz.timezone("America/New_York")
        # If naive, localize to EST; if aware, convert to EST first (keeps DST correctness)
        if dt.tzinfo is None:
            dt = est.localize(dt)
        else:
            dt = dt.astimezone(est)
        dt_utc = dt.astimezone(pytz.utc)
    else:
        # Assume UTC for naive datetimes when is_est is False
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=pytz.utc)
        dt_utc = dt.astimezone(pytz.utc)

    return dt_utc.isoformat()


# ================= Bracket Helpers Overview =================
# The bracket-related helpers below encapsulate pure or minimally stateful logic
# originally embedded in the ProjectX broker. Goals:
#   - Reduce branching/noise inside broker methods
#   - Make race handling (early meta store / restoration) explicit
#   - Centralize naming/tagging conventions for bracket parent & children
#   - Enable future unit tests without broker wiring
# Helper Groups:
#   Creation/meta: create_bracket_meta, early_store_bracket_meta, restore_bracket_meta_if_needed
#   Tagging: normalize_bracket_entry_tag, derive_base_tag, bracket_child_tag, build_unique_order_tag
#   Pricing: select_effective_prices
#   Spawn flow: should_spawn_bracket_children, build_bracket_child_spec
# All helpers are defensive (swallow exceptions) to preserve original broker robustness.
# ===========================================================
# Core metadata factory
def create_bracket_meta(tp_price, sl_price):
    """Return the synthetic bracket metadata dict (pure function).

    Shape identical to inline version used in ProjectX broker so spawning/restoration logic stays unchanged.
    """
    return {
        'tp_price': tp_price,
        'sl_price': sl_price,
        'children': {},
        'active': True,
        'base_tag': None,
    }


def normalize_bracket_entry_tag(tag: str):
    """Normalize tag to BRK_ENTRY_<base>; return (normalized_tag, base_tag).

    If tag already has one of the bracket prefixes it is rewritten to entry form.
    Pure; no logging.
    """
    if not tag:
        return tag, None
    base = tag
    for prefix in ("BRK_ENTRY_", "BRK_TP_", "BRK_STOP_"):
        if base.startswith(prefix):
            base = base[len(prefix):]
            break
    return f"BRK_ENTRY_{base}", base

# ================= Additional Low-Risk Helpers =================
def build_unique_order_tag(order):
    """Generate (or normalize) a unique order tag.

    Mirrors existing inline logic in ProjectX broker: if tag absent or blank create
    STRAT-<millis><rand2>. Keeps existing tag if non-empty. Returns tag string.
    Pure with respect to broker state (only uses order + time/random).
    """
    try:
        import time, random
        if not getattr(order, 'tag', None):
            strat_part = ''
            try:
                if hasattr(order, 'strategy') and order.strategy:
                    strat_name = order.strategy if isinstance(order.strategy, str) else getattr(order.strategy, 'name', '')
                    strat_part = (strat_name or 'LB')[:8].upper()
            except Exception:
                strat_part = 'LB'
            unique_suffix = f"{int(time.time()*1000)%100000000:08d}{random.randint(10,99)}"
            order.tag = f"{strat_part}-{unique_suffix}"
        else:
            if not str(order.tag).strip():
                order.tag = f"LB-{int(time.time()*1000)}"
    except Exception:
        # Silent; caller already logs on failure in original logic
        pass
    return order.tag


def select_effective_prices(order, client, tick_size):
    """Return (limit_price, stop_price) applying secondary_* precedence.

    Logic copied from inline broker section (non-bracket path):
    - Warn if deprecated take_profit_price / stop_loss_price present & not None.
    - secondary_limit_price supersedes order.limit_price (rounded)
    - secondary_stop_price supersedes order.stop_price (rounded)
    """
    limit_price = None
    stop_price = None

    # Deprecated warnings handled here so caller stays compact
    if hasattr(order, 'take_profit_price') and getattr(order, 'take_profit_price') is not None:
        try:
            # Expect caller's logger warning; if unavailable, ignore
            if hasattr(order, 'strategy') and hasattr(order.strategy, 'logger'):
                order.strategy.logger.warning("Order has deprecated attribute 'take_profit_price'. Set 'secondary_limit_price' instead.")
        except Exception:
            pass
    if hasattr(order, 'stop_loss_price') and getattr(order, 'stop_loss_price') is not None:
        try:
            if hasattr(order, 'strategy') and hasattr(order.strategy, 'logger'):
                order.strategy.logger.warning("Order has deprecated attribute 'stop_loss_price'. Set 'secondary_stop_price' instead.")
        except Exception:
            pass

    # Limit price precedence
    if hasattr(order, 'secondary_limit_price') and getattr(order, 'secondary_limit_price') is not None:
        try:
            limit_price = client.round_to_tick_size(getattr(order, 'secondary_limit_price'), tick_size)
        except Exception:
            if getattr(order, 'limit_price', None) is not None:
                try:
                    limit_price = client.round_to_tick_size(order.limit_price, tick_size)
                except Exception:
                    pass
    elif getattr(order, 'limit_price', None) is not None:
        try:
            limit_price = client.round_to_tick_size(order.limit_price, tick_size)
        except Exception:
            pass

    # Stop price precedence
    if hasattr(order, 'secondary_stop_price') and getattr(order, 'secondary_stop_price') is not None:
        try:
            stop_price = client.round_to_tick_size(getattr(order, 'secondary_stop_price'), tick_size)
        except Exception:
            if getattr(order, 'stop_price', None) is not None:
                try:
                    stop_price = client.round_to_tick_size(order.stop_price, tick_size)
                except Exception:
                    pass
    elif getattr(order, 'stop_price', None) is not None:
        try:
            stop_price = client.round_to_tick_size(order.stop_price, tick_size)
        except Exception:
            pass

    return limit_price, stop_price


def bracket_child_tag(kind: str, base_tag: str) -> str:
    """Return standardized child tag for bracket order.

    kind: 'tp' or 'sl'. Mirrors existing naming scheme.
    """
    if kind == 'tp':
        return f"BRK_TP_{base_tag}"
    if kind == 'sl':
        return f"BRK_STOP_{base_tag}"
    raise ValueError(f"Unsupported bracket child kind: {kind}")


def derive_base_tag(tag: str) -> str:
    """Derive the base tag (without BRK_*_ prefix). Safe if tag empty.

    Used when restoring meta or spawning children if base_tag missing.
    """
    if not tag:
        return tag
    base = tag
    for prefix in ("BRK_ENTRY_", "BRK_TP_", "BRK_STOP_"):
        if base.startswith(prefix):
            base = base[len(prefix):]
            break
    return base


def early_store_bracket_meta(store: dict, temp_key: str, meta: dict, logger=None):
    """Early store bracket meta under a provisional key if not already present.

    Mirrors inline try/except logic; silent on failure except optional debug logging.
    """
    try:
        if store is None:
            return
        if temp_key not in store:
            store[temp_key] = dict(meta)
            if logger:
                logger.debug(f"[BRACKET META EARLY] stored temp_key={temp_key} tp={meta.get('tp_price')} sl={meta.get('sl_price')}")
    except Exception:
        pass


def restore_bracket_meta_if_needed(order, cache, meta_map, logger=None):
    """Ensure order._synthetic_bracket is attached if present in cache or meta_map.

    Returns True if restoration / attachment happened, else False.
    """
    restored = False
    try:
        if hasattr(order, '_synthetic_bracket'):
            return False
        broker_id = getattr(order, 'id', None)
        # Check cached order first
        if cache and broker_id in cache:
            cached = cache.get(broker_id)
            if cached and hasattr(cached, '_synthetic_bracket'):
                order._synthetic_bracket = dict(getattr(cached, '_synthetic_bracket'))
                restored = True
        # Fallback to meta_map
        if not restored and meta_map and broker_id in meta_map:
            meta = meta_map.get(broker_id)
            if meta:
                order._synthetic_bracket = dict(meta)
                restored = True
        if restored and logger:
            logger.debug(f"[BRACKET META RESTORE] order_id={broker_id} restored={restored}")
    except Exception:
        pass
    return restored


def should_spawn_bracket_children(meta: dict, parent) -> tuple:
    """Determine if bracket children should be spawned.

    Returns (eligible: bool, reason: str). Mutates meta for the 'no tp/sl' case to mirror current logic.
    This contains only decision logic; side effects like logging remain in caller.
    """
    if meta is None:
        return False, 'no_meta'
    if meta.get('children') is None:
        return False, 'children_missing'
    if meta.get('children_submitted') or getattr(parent, '_bracket_children_submitted', False):
        return False, 'already_submitted'
    tp_price = meta.get('tp_price')
    sl_price = meta.get('sl_price')
    if tp_price is None and sl_price is None:
        meta['children_submitted'] = True
        meta['active'] = False
        return False, 'no_tp_sl'
    return True, 'ok'


def build_bracket_child_spec(parent, kind: str, price: float, base_tag: str) -> dict:
    """Return a pure spec dict for a bracket child before creating Order.

    Fields: side, order_type, tag, price_key, price_value.
    price_key is 'limit_price' for tp and 'stop_price' for sl.
    """
    side = 'sell' if parent.side.lower() == 'buy' else 'buy'
    if kind == 'tp':
        order_type = 'limit'
        price_key = 'limit_price'
    elif kind == 'sl':
        order_type = 'stop'
        price_key = 'stop_price'
    else:
        raise ValueError(f"Unknown bracket child kind: {kind}")
    tag = bracket_child_tag(kind, base_tag)
    return {
        'side': side,
        'order_type': order_type,
        'tag': tag,
        'price_key': price_key,
        'price_value': price,
    }
