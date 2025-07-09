"""
ProjectX Broker Implementation for Lumibot

Provides futures trading functionality through ProjectX broker integration.
Supports multiple underlying brokers (TSX, TOPONE, etc.) via ProjectX gateway.
"""

import asyncio
import logging
import math
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Union

import pandas as pd
from lumibot.brokers.broker import Broker
from lumibot.data_sources import DataSource
from lumibot.entities import Asset, Order, Position
from lumibot.tools.projectx_helpers import ProjectXClient
# Import moved to avoid circular dependency
# from lumibot.credentials import PROJECTX_CONFIG


class ProjectX(Broker):
    """
    ProjectX broker implementation for futures trading.
    
    Supports multiple underlying brokers through ProjectX gateway.
    Base URLs are provided automatically for all supported firms.
    
    Required Configuration:
    - PROJECTX_{FIRM}_API_KEY: API key for the broker
    - PROJECTX_{FIRM}_USERNAME: Username for the broker  
    - PROJECTX_{FIRM}_PREFERRED_ACCOUNT_NAME: Account name for trading
    
    Optional Configuration:
    - PROJECTX_FIRM: Explicitly specify firm (auto-detected if not set)
    - PROJECTX_{FIRM}_BASE_URL: Override default API URL
    - PROJECTX_{FIRM}_STREAMING_BASE_URL: Override default streaming URL
    
    Supported firms: topstepx, topone, tickticktrader, alphaticks,
    aquafutures, blueguardianfutures, blusky, bulenox, e8x, fundingfutures,
    thefuturesdesk, futureselite, fxifyfutures, goatfundedfutures, holaprime,
    nexgen, tx3funding, demo, daytraders
    """
    
    # Order type mappings (ProjectX values - CORRECTED from real Project X library)
    ORDER_TYPE_MAPPING = {
        "limit": 1,       # Limit order
        "market": 2,      # Market order  
        "stop": 4,        # Stop order
        "trail": 5,       # Trailing stop order
        "join_bid": 6,    # Join bid order
        "join_ask": 7,    # Join ask order
    }
    
    # Order side mappings (ProjectX values - CORRECTED from real Project X library)
    ORDER_SIDE_MAPPING = {
        "buy": 0,   # Buy/Long
        "sell": 1,  # Sell/Short
    }
    
    # ProjectX order status to Lumibot status mapping (CORRECTED based on real data analysis)
    ORDER_STATUS_MAPPING = {
        1: "new",              # New/Pending
        2: "submitted",        # Submitted to exchange
        3: "open",             # Open/Active on exchange (NOT partially filled!)
        4: "filled",           # Completely filled
        5: "cancelled",        # Cancelled
        6: "rejected",         # Rejected by exchange
        7: "expired",          # Order expired
        8: "replaced",         # Order replaced/modified
        9: "pending_cancel",   # Cancel request pending
        10: "pending_replace", # Replace request pending
        11: "partially_filled",# Actually partially filled (if this status exists)
        12: "suspended",       # Order suspended
        13: "triggered",       # Stop/conditional order triggered
        # Add fallback for unknown statuses
    }
    
    def __init__(self, config: dict = None, data_source: DataSource = None, 
                 connect_stream: bool = True, max_workers: int = 20, firm: str = None):
        """
        Initialize ProjectX broker.
        
        Args:
            config: Configuration dictionary (optional, auto-detected from environment)
            data_source: Data source for market data
            connect_stream: Whether to connect to streaming data
            max_workers: Maximum worker threads for async operations
            firm: Specific firm to use (auto-detected if not provided)
        """
        # Use environment config if not provided
        if config is None:
            from lumibot.credentials import get_projectx_config
            config = get_projectx_config(firm)
        
        # Validate required configuration
        required_fields = ["api_key", "username", "base_url"]
        missing_fields = [field for field in required_fields if not config.get(field)]
        
        if missing_fields:
            firm_name = config.get("firm", "unknown")
            raise ValueError(
                f"Missing required ProjectX configuration for {firm_name}: {', '.join(missing_fields)}. "
                f"Please set: PROJECTX_{firm_name}_API_KEY, PROJECTX_{firm_name}_USERNAME. "
                f"Base URL should be provided automatically for supported firms."
            )
        
        # Warning if no preferred account name is set
        if not config.get("preferred_account_name"):
            firm_name = config.get("firm", "unknown")
            self.logger = logging.getLogger(f"ProjectXBroker_{firm_name}")
            self.logger.warning(
                f"No preferred account name set for {firm_name}. "
                f"Consider setting PROJECTX_{firm_name}_PREFERRED_ACCOUNT_NAME for better account selection."
            )
        
        self.config = config
        self.firm = config.get("firm")
        
        # Initialize ProjectX client
        self.client = ProjectXClient(config)
        
        # Account management
        self.account_id = None
        self.account_info = {}
        
        # Streaming connection
        self.connect_stream = connect_stream
        self.streaming_client = None
        
        # Order tracking
        self._orders_cache = {}  # Store orders by their IDs
        self._positions_cache = {}  # Store positions
        
        # Thread management
        self.max_workers = max_workers
        
        # Setup logging
        self.logger = logging.getLogger(f"ProjectXBroker_{self.firm}")
        
        # Initialize parent class
        super().__init__(
            name=f"ProjectX_{self.firm}",
            data_source=data_source,
            connect_stream=connect_stream,
            max_workers=max_workers
        )
        
        self.logger.debug(f"ProjectX broker initialized for {self.firm}")
        self.logger.debug(f"Data source: {data_source.__class__.__name__ if data_source else 'None'}")
        self.logger.debug(f"Streaming enabled: {connect_stream}")
        
        # Check if we should auto-connect
        if not self.account_id:
            self.logger.debug(f"Account ID not set, will connect when needed")
    
    def connect(self):
        """Connect to ProjectX broker and set up account."""
        try:
            self.logger.debug(f"Connecting to ProjectX broker: {self.firm}")
            
            # Get account information
            self.logger.debug(f"Getting preferred account ID")
            self.account_id = self.client.get_preferred_account_id()
            
            if not self.account_id:
                self.logger.error(f"❌ No suitable account found")
                raise Exception("No suitable account found")
            
            self.logger.info(f"✅ Connected with account ID: {self.account_id}")
            
            # Set up streaming if enabled
            if self.connect_stream:
                self.logger.debug(f"Setting up streaming connection")
                self._setup_streaming()
            
            # Sync existing orders into tracking system for strategy compatibility
            self._sync_existing_orders_to_tracking()
            
            self.logger.debug("ProjectX broker connection complete")
            return True
            
        except Exception as e:
            self.logger.error(f"❌ Failed to connect to ProjectX: {e}")
            return False
    
    def _setup_streaming(self):
        """Set up streaming data connection."""
        try:
            self.streaming_client = self.client.get_streaming_client(self.account_id)
            
            # Set up event handlers
            self.streaming_client.on_order_update = self._handle_order_update
            self.streaming_client.on_position_update = self._handle_position_update
            self.streaming_client.on_trade_update = self._handle_trade_update
            self.streaming_client.on_account_update = self._handle_account_update
            
            # Start streaming connection
            if self.streaming_client.start_user_hub():
                self.streaming_client.subscribe_all(self.account_id)
                self.logger.debug("Streaming connection established")
            else:
                self.logger.warning("Failed to establish streaming connection")
                
        except Exception as e:
            self.logger.error(f"Failed to setup streaming: {e}")
    
    def disconnect(self):
        """Disconnect from ProjectX broker."""
        try:
            if self.streaming_client:
                self.streaming_client.stop()
            self.logger.info("Disconnected from ProjectX broker")
        except Exception as e:
            self.logger.error(f"Error disconnecting: {e}")
    
    # ========== Required Broker Methods ==========
    
    def cancel_order(self, order: Order) -> bool:
        """Cancel an order at the broker."""
        try:
            if not order.id:
                self.logger.error("Cannot cancel order without ID")
                return False
            
            response = self.client.order_cancel(self.account_id, int(order.id))
            
            if response and response.get("success"):
                # Update order status
                order.status = "cancelled"
                self.logger.info(f"Order {order.id} cancelled successfully")
                return True
            else:
                error_msg = response.get("errorMessage", "Unknown error") if response else "No response"
                self.logger.error(f"Failed to cancel order {order.id}: {error_msg}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error cancelling order {order.id}: {e}")
            return False
    
    def _modify_order(self, order: Order, limit_price: float = None, 
                     stop_price: float = None) -> bool:
        """Modify an existing order."""
        try:
            if not order.id:
                self.logger.error("Cannot modify order without ID")
                return False
            
            # Get contract tick size for price rounding
            contract_id = self._get_contract_id_from_asset(order.asset)
            if contract_id:
                tick_size = self.client.get_contract_tick_size(contract_id)
                
                # Round prices to tick size
                if limit_price is not None:
                    limit_price = self.client.round_to_tick_size(limit_price, tick_size)
                if stop_price is not None:
                    stop_price = self.client.round_to_tick_size(stop_price, tick_size)
            
            response = self.client.order_modify(
                account_id=self.account_id,
                order_id=int(order.id),
                size=order.quantity,
                limit_price=limit_price,
                stop_price=stop_price
            )
            
            if response and response.get("success"):
                # Update order prices
                if limit_price is not None:
                    order.limit_price = limit_price
                if stop_price is not None:
                    order.stop_price = stop_price
                
                self.logger.info(f"Order {order.id} modified successfully")
                return True
            else:
                error_msg = response.get("errorMessage", "Unknown error") if response else "No response"
                self.logger.error(f"Failed to modify order {order.id}: {error_msg}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error modifying order {order.id}: {e}")
            return False
    
    def _submit_order(self, order: Order) -> Order:
        """Submit a new order to the broker."""
        try:
            # Get contract ID from asset
            contract_id = self._get_contract_id_from_asset(order.asset)
            if not contract_id:
                order.status = "rejected"
                order.error = f"Could not find contract for {order.asset.symbol}"
                return order
            
            # Get contract tick size for price rounding
            tick_size = self.client.get_contract_tick_size(contract_id)
            
            # Map order type and side
            order_type = self.ORDER_TYPE_MAPPING.get(order.order_type.lower())
            if order_type is None:
                order.status = "rejected"
                order.error = f"Unsupported order type: {order.order_type}"
                return order
            
            order_side = self.ORDER_SIDE_MAPPING.get(order.side.lower())
            if order_side is None:
                order.status = "rejected"
                order.error = f"Unsupported order side: {order.side}"
                return order
            
            # Round prices to tick size
            limit_price = None
            stop_price = None
            
            if order.limit_price is not None:
                limit_price = self.client.round_to_tick_size(order.limit_price, tick_size)
            if order.stop_price is not None:
                stop_price = self.client.round_to_tick_size(order.stop_price, tick_size)
            
            # Submit order
            response = self.client.order_place(
                account_id=self.account_id,
                contract_id=contract_id,
                order_type=order_type,  # Fixed: use order_type instead of deprecated 'type'
                side=order_side,
                size=order.quantity,
                limit_price=limit_price,
                stop_price=stop_price,
                custom_tag=order.tag
            )
            
            if response and response.get("success"):
                # Update order with response data
                order.id = str(response.get("orderId"))
                order.status = "submitted"
                order.limit_price = limit_price
                order.stop_price = stop_price
                
                # Cache the order
                self._orders_cache[order.id] = order
                
                self.logger.info(f"Order submitted successfully with ID: {order.id}")
            else:
                error_msg = response.get("errorMessage", "Unknown error") if response else "No response"
                order.status = "rejected"
                order.error = error_msg
                self.logger.error(f"Failed to submit order: {error_msg}")
            
            return order
            
        except Exception as e:
            order.status = "rejected"
            order.error = str(e)
            self.logger.error(f"Error submitting order: {e}")
            return order
    
    def _get_balances_at_broker(self, quote_asset: Asset, strategy) -> tuple:
        """Get account balances from the broker.
        
        Returns:
            tuple: (cash_value, positions_value, total_liquidation_value)
            
        Raises:
            Exception: If unable to retrieve balance data (instead of returning misleading 0.0 values)
        """
        try:
            # Ensure we have an account_id
            if not self.account_id:
                self.logger.debug(f"Auto-connecting to get account ID")
                if not self.connect():
                    raise Exception("Failed to auto-connect for balance retrieval - no account ID available")
            
            # Use cached account balance method
            balance_data = self.client.get_account_balance(self.account_id)
            
            # Extract balance information
            cash_balance = float(balance_data.get("cash", 0.0))
            equity = float(balance_data.get("equity", 0.0))
            
            # For futures, typically:
            # - cash = available cash
            # - equity = total account value (cash + unrealized PnL)
            # - positions_value = unrealized PnL
            positions_value = equity - cash_balance
            total_liquidation_value = equity
            
            self.logger.debug(f"Account balance - Cash: ${cash_balance:.2f}, Positions Value: ${positions_value:.2f}, Total: ${total_liquidation_value:.2f}")
            
            return (cash_balance, positions_value, total_liquidation_value)
            
        except Exception as e:
            self.logger.error(f"❌ Failed to retrieve account balance: {e}")
            # DO NOT return (0.0, 0.0, 0.0) as it's misleading - $0 is a valid account balance!
            # Instead, raise an exception to indicate the API failure
            raise Exception(f"Unable to retrieve account balance from ProjectX: {e}")
    
    def _get_orders_at_broker(self) -> List[Order]:
        """Get all orders from the broker with performance optimization."""
        try:
            # Ensure we have an account_id
            if not self.account_id:
                self.logger.debug(f"Auto-connecting to get account ID")
                if not self.connect():
                    raise Exception("Failed to auto-connect for order retrieval - no account ID available")
            
            # Get orders from last 30 days to catch filled/cancelled orders
            # Note: Orders may disappear quickly after being filled
            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)
            
            orders_data = self.client.get_orders(
                account_id=self.account_id,
                start_date=start_date.isoformat(),
                end_date=end_date.isoformat()
            )
            
            self.logger.debug(f"Retrieved {len(orders_data)} orders from broker")
            
            # Convert recent orders with detailed logging
            recent_orders = []
            conversion_attempts = 0
            conversion_successes = 0
            
            # Process the most recent 50 orders with detailed logging
            for broker_order in orders_data[:50]:  
                conversion_attempts += 1
                try:
                    order_id = broker_order.get('id', 'unknown')
                    status = broker_order.get('status', 'unknown')
                    order_type = broker_order.get('type', 'unknown')
                    contract_id = broker_order.get('contractId', 'unknown')
                    
                    # Minimal logging - only log issues, not every successful conversion
                    order = self._convert_broker_order_to_lumibot_order(broker_order)
                    if order is not None:
                        recent_orders.append(order)
                        self._orders_cache[order.id] = order
                        conversion_successes += 1
                    else:
                        self.logger.debug(f"❌ Order {order_id} conversion returned None")
                        
                except Exception as e:
                    self.logger.error(f"❌ Failed to convert order {broker_order.get('id', 'unknown')}: {e}")
                    import traceback
                    self.logger.error(f"Full traceback: {traceback.format_exc()}")
                    continue
            
            self.logger.debug(f"Order conversion results: {conversion_successes}/{conversion_attempts} successful")
            
            # Final safety check - filter out any None orders that might have slipped through
            orders = [order for order in recent_orders if order is not None]
            
            self.logger.debug(f"✅ Processed {len(orders)} recent/active orders from {len(orders_data)} total")
            return orders
            
        except Exception as e:
            self.logger.error(f"❌ Error getting orders: {e}")
            # Don't raise exception - return empty list for graceful degradation
            return []
    
    def _get_positions_at_broker(self) -> List[Position]:
        """Get all positions from the broker."""
        try:
            # Ensure we have an account_id
            if not self.account_id:
                self.logger.debug(f"Auto-connecting to get account ID")
                if not self.connect():
                    raise Exception("Failed to auto-connect for position retrieval - no account ID available")
            
            positions_data = self.client.get_positions(self.account_id)
            
            self.logger.debug(f"Retrieved {len(positions_data)} positions from broker")
            
            positions = []
            
            for broker_position in positions_data:
                try:
                    position = self._convert_broker_position_to_lumibot_position(broker_position)
                    if position is not None:
                        positions.append(position)
                        # Update cache
                        self._positions_cache[position.asset.symbol] = position
                    else:
                        contract_id = broker_position.get('contractId', 'unknown')
                        self.logger.warning(f"❌ Position {contract_id} conversion returned None")
                except Exception as e:
                    self.logger.error(f"❌ Failed to convert position {broker_position.get('contractId', 'unknown')}: {e}")
                    continue
            
            # Final safety check - filter out any None positions
            positions = [position for position in positions if position is not None]
            
            # Log summary of converted positions
            if positions:
                position_summary = ", ".join([f"{pos.quantity} {pos.asset.symbol}" for pos in positions])
                self.logger.debug(f"Converted {len(positions)} positions: {position_summary}")
            else:
                self.logger.info("✅ No positions found")
            
            return positions
            
        except Exception as e:
            self.logger.error(f"❌ Error getting positions: {e}")
            raise Exception(f"Unable to retrieve positions from ProjectX: {e}")
    
    def get_chains(self, asset: Asset) -> Dict:
        """
        Get options chains for an asset.
        
        ProjectX is a futures broker, so this method is not applicable.
        Raises NotImplementedError as futures don't have options chains.
        """
        raise NotImplementedError("ProjectX is a futures broker - options chains are not supported")
    
    def get_historical_account_value(self) -> dict:
        """Get the historical account value of the account."""
        # ProjectX doesn't provide historical account value endpoint
        # Return empty dict for now
        return {}
    
    def _get_stream_object(self):
        """Get the broker stream connection."""
        # ProjectX uses SignalR streaming which is handled in the streaming_client
        return self.streaming_client
    
    def _register_stream_events(self):
        """Register the function on_trade_event to be executed on each trade_update event."""
        # Events are already registered in _setup_streaming()
        pass
    
    def _run_stream(self):
        """Run the stream."""
        # SignalR stream is already running via streaming_client
        pass
    
    def _pull_positions(self, strategy) -> List[Position]:
        """Get the account positions from the broker."""
        return self._get_positions_at_broker()
    
    def _pull_position(self, strategy, asset: Asset) -> Position:
        """Pull a single position from the broker that matches the asset."""
        positions = self._get_positions_at_broker()
        for position in positions:
            if position.asset == asset:
                return position
        return None
    
    def _parse_broker_order(self, response: dict, strategy_name: str, strategy_object=None) -> Order:
        """Parse a broker order representation to an order object."""
        return self._convert_broker_order_to_lumibot_order(response)
    
    def _pull_broker_order(self, identifier: str) -> Order:
        """Get a broker order representation by its id."""
        # ProjectX doesn't have a single order endpoint, use cached orders
        if identifier in self._orders_cache:
            order = self._orders_cache[identifier]
            # Safety check to ensure we don't return None orders
            if order is not None:
                return order
        return None
    
    def _pull_broker_all_orders(self) -> List[dict]:
        """Get the broker open orders."""
        try:
            # Get orders from last 30 days
            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)
            
            orders = self.client.order_search(
                account_id=self.account_id,
                start_datetime=start_date.isoformat(),
                end_datetime=end_date.isoformat()
            )
            
            # order_search returns a list directly, not a dict with orders key
            if isinstance(orders, list):
                return orders
            elif isinstance(orders, dict) and orders.get("success"):
                return orders.get("orders", [])
            
            return []
            
        except Exception as e:
            self.logger.error(f"Error getting all orders: {e}")
            return []
    
    # ========== Helper Methods ==========
    
    def _get_contract_id_from_asset(self, asset: Asset) -> str:
        """Get ProjectX contract ID from Lumibot asset."""
        try:
            symbol = asset.symbol
            
            # Handle continuous futures using Asset class logic
            if asset.asset_type == Asset.AssetType.CONT_FUTURE:
                self.logger.debug(f"Converting continuous future {symbol} to specific contract")
                
                try:
                    # Use Asset class method to resolve continuous futures
                    potential_contracts = asset.get_potential_futures_contracts()
                    
                    for contract_symbol in potential_contracts:
                        # Convert to ProjectX format if needed
                        if not contract_symbol.startswith("CON.F.US."):
                            # Parse symbol like "MESU25" -> "CON.F.US.MES.U25"
                            if len(contract_symbol) >= 4:
                                base_symbol = contract_symbol[:-3]  # Remove last 3 chars
                                month_year = contract_symbol[-3:]   # Get month + year code
                                if len(month_year) == 3:
                                    month_code = month_year[0]
                                    year_code = month_year[1:]
                                    contract_id = f"CON.F.US.{base_symbol}.{month_code}{year_code}"
                                else:
                                    contract_id = f"CON.F.US.{symbol}.{month_year}"
                            else:
                                contract_id = f"CON.F.US.{symbol}.U25"  # Fallback
                        else:
                            contract_id = contract_symbol
                        
                        self.logger.debug(f"✅ Using Asset class contract: {contract_id}")
                        return contract_id
                        
                except Exception as asset_error:
                    self.logger.warning(f"⚠️ Asset class resolution failed, falling back to client method: {asset_error}")
            
            # For non-continuous futures or fallback, use client method
            contract_id = self.client.find_contract_by_symbol(symbol)
            
            if not contract_id:
                self.logger.error(f"Contract not found for asset: {asset.symbol} (type: {asset.asset_type})")
                return ""
            
            self.logger.debug(f"✅ Found contract ID: {contract_id} for {symbol}")
            return contract_id
            
        except Exception as e:
            self.logger.error(f"Error getting contract ID for {asset.symbol}: {e}")
            return ""
    
    def _convert_broker_order_to_lumibot_order(self, broker_order: dict) -> Order:
        """Convert ProjectX order to Lumibot Order object."""
        try:
            # Get asset from contract - use efficient lookup
            contract_id = broker_order.get("contractId")
            asset = self._get_asset_from_contract_id_cached(contract_id)
            
            if not asset:
                # Don't log warnings for every failed conversion - too noisy
                return None
            
            # Convert order type
            order_type_id = broker_order.get("type")
            order_type = self._get_order_type_from_id(order_type_id)
            
            # Convert order side (corrected mapping)
            side_id = broker_order.get("side")
            side = "buy" if side_id == 0 else "sell" if side_id == 1 else "unknown"
            
            # Convert status with detailed logging
            status_id = broker_order.get("status")
            status = self.ORDER_STATUS_MAPPING.get(status_id, f"unknown_status_{status_id}")
            
            if status.startswith("unknown_status_"):
                self.logger.warning(f"Unknown order status ID: {status_id} for order {broker_order.get('id')}")
            
            # Create Order object
            order = Order(
                strategy="",  # Will be set by strategy when needed
                asset=asset,
                quantity=broker_order.get("size", 0),
                side=side,
                order_type=order_type  # Use order_type instead of deprecated 'type'
            )
            
            # Set additional properties
            order.id = str(broker_order.get("id"))
            order.status = status
            order.limit_price = broker_order.get("limitPrice")
            order.stop_price = broker_order.get("stopPrice")
            order.filled_quantity = broker_order.get("filledSize", 0)
            order.avg_fill_price = broker_order.get("avgFillPrice")
            order.tag = broker_order.get("customTag")
            
            # Set timestamps
            if broker_order.get("createdDateTime"):
                order.created_at = pd.to_datetime(broker_order["createdDateTime"])
            if broker_order.get("updatedDateTime"):
                order.updated_at = pd.to_datetime(broker_order["updatedDateTime"])
            
            return order
            
        except Exception as e:
            self.logger.error(f"Error converting broker order: {e}")
            return None
    
    def _convert_broker_position_to_lumibot_position(self, broker_position: dict) -> Position:
        """Convert ProjectX position to Lumibot Position object."""
        try:
            # Get asset from contract - use efficient cached lookup
            contract_id = broker_position.get("contractId")
            asset = self._get_asset_from_contract_id_cached(contract_id)
            
            if not asset:
                return None
            
            # Create Position object
            quantity = broker_position.get("size", 0)
            # Try both field names: avgPrice and averagePrice
            avg_price = broker_position.get("avgPrice") or broker_position.get("averagePrice", 0.0)
            
            position = Position(
                strategy="",  # Will be set by strategy
                asset=asset,
                quantity=quantity,
                avg_fill_price=avg_price
            )
            
            # Set additional properties
            position.unrealized_pnl = broker_position.get("unrealizedPnl", 0.0)
            position.realized_pnl = broker_position.get("realizedPnl", 0.0)
            
            return position
            
        except Exception as e:
            self.logger.error(f"Error converting broker position: {e}")
            return None
    
    def _get_asset_from_contract_id(self, contract_id: str) -> Asset:
        """Get Lumibot Asset from ProjectX contract ID using cached contract details."""
        try:
            contract = self.client.get_contract_details(contract_id)
            
            symbol = contract.get("symbol")
            if symbol:
                # Create continuous futures asset to match strategy expectations
                return Asset(symbol, asset_type=Asset.AssetType.CONT_FUTURE)
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting asset from contract ID {contract_id}: {e}")
            return None
    
    def _get_asset_from_contract_id_cached(self, contract_id: str) -> Asset:
        """Get Asset from contract ID with efficient caching to avoid excessive API calls."""
        if not contract_id:
            return None
        
        # Check if we already have this in our asset cache
        if hasattr(self, '_asset_cache') and contract_id in self._asset_cache:
            return self._asset_cache[contract_id]
        
        # Initialize asset cache if not exists
        if not hasattr(self, '_asset_cache'):
            self._asset_cache = {}
        
        try:
            # Try to extract symbol from common contract ID patterns
            asset = self._extract_asset_from_contract_pattern(contract_id)
            if asset:
                self._asset_cache[contract_id] = asset
                return asset
            
            # For unknown contracts, try one API call and cache the result
            contract = self.client.get_contract_details(contract_id)
            symbol = contract.get("symbol")
            if symbol:
                # Use continuous futures to match strategy expectations
                asset = Asset(symbol, asset_type=Asset.AssetType.CONT_FUTURE)
                self._asset_cache[contract_id] = asset
                return asset
            
            # Cache the failure to avoid repeat lookups
            self._asset_cache[contract_id] = None
            return None
            
        except Exception as e:
            # Cache the failure and return None to avoid repeat API calls
            self._asset_cache[contract_id] = None
            return None
    
    def _extract_asset_from_contract_pattern(self, contract_id: str) -> Asset:
        """Extract asset symbol from common contract ID patterns to avoid API calls."""
        try:
            # Common pattern: CON.F.US.SYMBOL.EXPIRY (e.g., CON.F.US.MES.U25)
            if contract_id.startswith("CON.F.US."):
                parts = contract_id.split(".")
                if len(parts) >= 5:
                    symbol = parts[3]  # Extract the symbol part
                    expiry_code = parts[4]  # Extract expiry (e.g., U25)
                    
                    # For continuous futures, use the base symbol as cont_future
                    # This matches what the strategy expects
                    return Asset(symbol, asset_type=Asset.AssetType.CONT_FUTURE)
            
            # Add other pattern extractions as needed
            # Pattern: SYMBOL-EXPIRY (e.g., MES-MAR25)
            if "-" in contract_id:
                symbol = contract_id.split("-")[0]
                return Asset(symbol, asset_type=Asset.AssetType.CONT_FUTURE)
            
            return None
            
        except Exception:
            return None
    
    def _get_order_type_from_id(self, type_id: int) -> str:
        """Convert ProjectX order type ID to string."""
        reverse_mapping = {v: k for k, v in self.ORDER_TYPE_MAPPING.items()}
        order_type = reverse_mapping.get(type_id, "market")
        
        # Fix ProjectX to Lumibot order type mapping
        if order_type == "trail":
            return "trailing_stop"
        
        return order_type
    
    def _update_orders_cache(self):
        """Update the orders cache with latest data."""
        try:
            orders = self._get_orders_at_broker()
            for order in orders:
                if order is not None:  # Defensive check
                    self._orders_cache[order.id] = order
        except Exception as e:
            self.logger.error(f"Error updating orders cache: {e}")
    
    def _update_positions_cache(self):
        """Update the positions cache with latest data."""
        try:
            positions = self._get_positions_at_broker()
            for position in positions:
                if position is not None:  # Defensive check
                    self._positions_cache[position.asset.symbol] = position
        except Exception as e:
            self.logger.error(f"Error updating positions cache: {e}")
    
    # ========== Streaming Event Handlers ==========
    
    def _handle_order_update(self, data):
        """Handle order update from streaming."""
        try:
            order = self._convert_broker_order_to_lumibot_order(data)
            if order is not None:
                self._orders_cache[order.id] = order
                self.logger.debug(f"Order update received: {order.id} - {order.status}")
        except Exception as e:
            self.logger.error(f"Error handling order update: {e}")
    
    def _handle_position_update(self, data):
        """Handle position update from streaming."""
        try:
            position = self._convert_broker_position_to_lumibot_position(data)
            if position is not None:
                self._positions_cache[position.asset.symbol] = position
                self.logger.debug(f"Position update received: {position.asset.symbol}")
        except Exception as e:
            self.logger.error(f"Error handling position update: {e}")
    
    def _handle_trade_update(self, data):
        """Handle trade update from streaming."""
        try:
            self.logger.debug(f"Trade update received: {data}")
            # Trade updates can trigger order and position cache updates
            self._update_orders_cache()
            self._update_positions_cache()
        except Exception as e:
            self.logger.error(f"Error handling trade update: {e}")
    
    def _handle_account_update(self, data):
        """Handle account update from streaming."""
        try:
            self.logger.debug(f"Account update received: {data}")
            self.account_info = data
        except Exception as e:
            self.logger.error(f"Error handling account update: {e}")
    
    def _sync_existing_orders_to_tracking(self):
        """Sync existing ACTIVE orders from broker into the tracking system for strategy compatibility."""
        try:
            self.logger.debug("Syncing active orders into tracking system")
            
            # CRITICAL: Clear old orders from tracking system to prevent mixing with fresh data
            # This prevents old canceled orders from previous sessions showing up
            strategy_name = None
            if self._subscribers:
                strategy_name = self._subscribers[0].name
                self.logger.debug(f"Clearing old orders for strategy: {strategy_name}")
                
                # Remove old orders for this strategy from all tracking lists
                old_count = 0
                for order_list in [self._new_orders, self._partially_filled_orders, self._filled_orders, 
                                 self._canceled_orders, self._error_orders, self._unprocessed_orders]:
                    orders_to_remove = [order for order in order_list.get_list() if order.strategy == strategy_name]
                    for order in orders_to_remove:
                        order_list.remove(order.id, key="id")
                        old_count += 1
                
                self.logger.debug(f"Cleared {old_count} old orders from tracking system")
            
            # Get existing orders from broker 
            all_orders = self._get_orders_at_broker()
            
            # Filter to only ACTIVE orders to prevent spam from old canceled orders
            active_orders = [
                order for order in all_orders 
                if order.status in ["new", "submitted", "open", "partially_filled", "partial_filled"]
            ]
            
            self.logger.debug(f"Found {len(active_orders)} active orders to sync (filtered from {len(all_orders)} total)")
            
            # Only sync if we have active orders - skip if all are canceled/filled
            if not active_orders:
                self.logger.debug("No active orders to sync - all orders are completed/canceled")
                return
            
            self.logger.debug(f"Assigning active orders to strategy: {strategy_name}")
            
            synced_count = 0
            for order in active_orders:
                try:
                    # Assign strategy name if available and not already set
                    if strategy_name:
                        order.strategy = strategy_name
                        self.logger.debug(f"Assigned strategy '{strategy_name}' to order {order.id}")
                    
                    # Mark as synced from broker to prevent auto-cancellation during validation
                    order._synced_from_broker = True
                    
                    # Add orders to appropriate tracking lists based on status
                    if order.status in ["new", "submitted", "open"]:
                        self._new_orders.append(order)
                        synced_count += 1
                    elif order.status in ["partially_filled", "partial_filled"]:
                        self._partially_filled_orders.append(order)
                        synced_count += 1
                
                except Exception as e:
                    self.logger.error(f"Failed to sync order {order.id}: {e}")
                    continue
            
            self.logger.debug(f"Successfully synced {synced_count} active orders into tracking system")
            
            # Log summary instead of individual orders
            if synced_count > 0:
                status_summary = {}
                for order in active_orders:
                    status_summary[order.status] = status_summary.get(order.status, 0) + 1
                status_text = ", ".join([f"{count} {status}" for status, count in status_summary.items()])
                self.logger.debug(f"Order status breakdown: {status_text}")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to sync existing orders: {e}")
            # Continue without failing - this is not critical for basic functionality
    
    def _add_subscriber(self, subscriber):
        """Override to sync orders when a strategy is added."""
        super()._add_subscriber(subscriber)
        
        # Sync existing orders to this strategy
        try:
            self.logger.debug(f"Strategy '{subscriber.name}' added - syncing existing orders")
            self._sync_existing_orders_to_tracking()
        except Exception as e:
            self.logger.error(f"Failed to sync orders for new strategy {subscriber.name}: {e}") 