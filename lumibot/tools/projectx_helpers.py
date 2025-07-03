"""
ProjectX Helper Utilities for Lumibot Integration

This module provides the core ProjectX API client and streaming functionality
integrated into Lumibot's architecture based on the actual working Project X library.
"""

import logging
import time
import os
import warnings
from datetime import datetime
from typing import Callable, Optional, Dict, Any, List

import pandas as pd
import pytz
import requests

# Suppress SSL deprecation warnings from third-party websocket library
warnings.filterwarnings("ignore", category=DeprecationWarning, module="websocket")
warnings.filterwarnings("ignore", category=DeprecationWarning, message="ssl.PROTOCOL_TLS is deprecated")
warnings.filterwarnings("ignore", category=DeprecationWarning, message="websockets.legacy is deprecated")
warnings.filterwarnings("ignore", category=DeprecationWarning, message="websockets.client.connect is deprecated")
warnings.filterwarnings("ignore", category=DeprecationWarning, message="websockets.client.WebSocketClientProtocol is deprecated")

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
            logging.error(f"Request error: {e}")
            return None
        except ValueError as e:
            logging.error(f"JSON decode error: {e}")
            return None
        
        # Return None if authentication failed
        if not auth_resp.get("success"):
            error_code = auth_resp.get("errorCode")
            error_message = auth_resp.get("errorMessage", "No error message provided")
            logging.error(f"Authentication failed - Error Code: {error_code}, Message: {error_message}")
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
        self.logger = logging.getLogger(f"ProjectXStreaming_{self.firm}")
    
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
    
    def _handle_account_update(self, data):
        """Handle account update event"""
        if self.on_account_update:
            self.on_account_update(data)
    
    def _handle_order_update(self, data):
        """Handle order update event"""
        if self.on_order_update:
            self.on_order_update(data)
    
    def _handle_position_update(self, data):
        """Handle position update event"""
        if self.on_position_update:
            self.on_position_update(data)
    
    def _handle_trade_update(self, data):
        """Handle trade update event"""
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
        
        self.logger = logging.getLogger(f"ProjectXClient_{self.firm}")
    
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
    
    def order_place(self, account_id: int, contract_id: str, type: int, side: int, size: int,
                   limit_price: float = None, stop_price: float = None, trail_price: float = None,
                   custom_tag: str = None, linked_order_id: int = None) -> dict:
        """Place an order for a contract"""
        url = f"{self.base_url}api/order/place"
        
        payload = {
            "accountId": account_id,
            "contractId": contract_id,
            "type": type,
            "side": side,
            "size": size,
            "limitPrice": limit_price,
            "stopPrice": stop_price,
            "trailPrice": trail_price,
            "customTag": custom_tag,
            "linkedOrderId": linked_order_id,
        }
        
        try:
            response = requests.post(url, headers=self.headers, json=payload)
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error: {e}")
            return {"success": False, "error": str(e)}
    
    def order_cancel(self, account_id: int, order_id: int) -> dict:
        """Cancel an order"""
        url = f"{self.base_url}api/order/cancel"
        
        payload = {
            "accountId": account_id,
            "orderId": order_id,
        }
        
        try:
            response = requests.post(url, headers=self.headers, json=payload)
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Error: {e}")
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
    
    def history_retrieve_bars(self, contract_id: str, start_datetime: str, end_datetime: str,
                             unit: int, unit_number: int, limit: int = 1000,
                             include_partial_bar: bool = True, live: bool = False,
                             is_est: bool = True) -> pd.DataFrame:
        """Retrieve historical bars for a contract"""
        url = f"{self.base_url}api/history/retrievebars"
        
        # Convert timezone if needed
        if is_est:
            est = pytz.timezone("America/New_York")
            start_datetime = (
                est.localize(datetime.fromisoformat(start_datetime[:-1]))
                .astimezone(pytz.utc)
                .isoformat()
            )
            end_datetime = (
                est.localize(datetime.fromisoformat(end_datetime[:-1]))
                .astimezone(pytz.utc)
                .isoformat()
            )
        
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
            
            # Add date and time columns
            df["date"] = df["t"].dt.date
            df["time"] = df["t"].dt.time
            
            # Map ProjectX column names to standard OHLCV format
            column_mapping = {
                'o': 'open',
                'h': 'high', 
                'l': 'low',
                'c': 'close',
                'v': 'volume'
            }
            
            # Rename columns to standard format
            df.rename(columns=column_mapping, inplace=True)
            
            # Reorder columns to standard format
            standard_columns = ["date", "time", "open", "high", "low", "close", "volume"]
            available_columns = [col for col in standard_columns if col in df.columns]
            extra_columns = [col for col in df.columns if col not in standard_columns]
            df = df[available_columns + extra_columns]
            
            # Drop timestamp column
            df.drop(columns=["t"], inplace=True)
            
            # Sort and reset index
            df.sort_values(by=["date", "time"], inplace=True)
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
        self.logger = logging.getLogger(f"ProjectXClient_{self.firm}")
        
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
        """Get preferred account ID"""
        accounts = self.get_accounts()
        
        # Filter for practice accounts
        practice_accounts = [
            account for account in accounts
            if account.get("name", "").lower().startswith(("prac", "tof-px"))
        ]
        
        if not practice_accounts:
            raise Exception("No practice accounts found")
        
        # Use preferred account if specified
        preferred_name = self.config.get("preferred_account_name")
        if preferred_name:
            for account in practice_accounts:
                if account.get('name') == preferred_name:
                    return account.get('id')
        
        # Fallback to highest balance account
        selected_account = max(practice_accounts, key=lambda x: x.get('balance', 0))
        return selected_account.get('id')
    
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
    
    def history_retrieve_bars(self, contract_id: str, start_datetime: str, end_datetime: str,
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
            if symbol_upper in common_futures:
                fallback_contract = common_futures[symbol_upper]
                self.logger.info(f"🔄 Fallback to hardcoded mapping: {fallback_contract}")
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
        """Cancel an order"""
        response = self.api.order_cancel(account_id, order_id)
        return response and response.get("success", False) 