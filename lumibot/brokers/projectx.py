"""
ProjectX Broker Implementation for Lumibot

Provides futures trading functionality through ProjectX broker integration.
Supports multiple underlying brokers (TSX, TOPONE, etc.) via ProjectX gateway.
"""

from datetime import datetime, timedelta
from typing import Dict, List

import pandas as pd

from lumibot.brokers.broker import Broker
from lumibot.data_sources import DataSource
from lumibot.entities import Asset, Order, Position
from lumibot.tools.lumibot_logger import get_logger
from lumibot.tools.projectx_helpers import (
    ProjectXClient,
    create_bracket_meta,
    normalize_bracket_entry_tag,
    build_unique_order_tag,
    select_effective_prices,
    bracket_child_tag,
    derive_base_tag,
    early_store_bracket_meta,
    restore_bracket_meta_if_needed,
    should_spawn_bracket_children,
    build_bracket_child_spec,
)
from termcolor import colored
# PollingStream usage was removed to align with centralized lifecycle in core Broker
import traceback

# Import moved to avoid circular dependency
# from lumibot.credentials import PROJECTX_CONFIG

logger = get_logger(__name__)


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

    # ProjectX order status to Lumibot status mapping (FIXED based on actual ProjectX API documentation)
    # Source: ProjectX API docs show OrderStatus enum: Open=1, Filled=2, Cancelled=3, Expired=4, Rejected=5, Pending=6
    ORDER_STATUS_MAPPING = {
        1: "open",             # Open (active order on exchange)
        2: "filled",           # Filled (completely executed)
        3: "cancelled",        # Cancelled
        4: "expired",          # Expired (map to cancelled for Lumibot)
        5: "rejected",         # Rejected (will be aliased to "error")
        6: "new",              # Pending (new order, not yet on exchange)
        # Extended statuses that may exist in some ProjectX implementations:
        7: "partially_filled", # Partially filled (if supported)
        8: "replaced",         # Order replaced/modified
        9: "pending_cancel",   # Cancel request pending
        10: "pending_replace", # Replace request pending
        11: "suspended",       # Order suspended
        12: "triggered",       # Stop/conditional order triggered
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
            self.logger = get_logger(f"ProjectXBroker_{firm_name}")
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

        # Order/position caches
        self._orders_cache = {}  # Store orders by their IDs
        self._positions_cache = {}  # Store positions
        # Bracket tracking maps (synthetic implementation)
        self._bracket_parent_by_child_id = {}
        self._bracket_meta = {}  # parent_id -> meta dict (persistent across conversions)

        # Thread management
        self.max_workers = max_workers

        # Setup logging
        self.logger = get_logger(f"ProjectXBroker_{self.firm}")

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
            self.logger.debug("Account ID not set, will connect when needed")

    # (dedupe set will be lazily created in _on_new_order override)

    def connect(self):
        """Connect to ProjectX broker and set up account."""
        try:
            self.logger.debug(f"Connecting to ProjectX broker: {self.firm}")

            # Get account information
            self.logger.debug("Getting preferred account ID")
            self.account_id = self.client.get_preferred_account_id()

            if not self.account_id:
                self.logger.error("❌ No suitable account found")
                raise Exception("No suitable account found")

            self.logger.info(f"✅ Connected with account ID: {self.account_id}")

            # Set up streaming if enabled
            if self.connect_stream:
                self.logger.debug("Setting up streaming connection")
                self._setup_streaming()

            # No adapter-level sync; rely on core Broker's first-iteration handling

            self.logger.debug("ProjectX broker connection complete")
            return True

        except Exception as e:
            self.logger.error(f"❌ Failed to connect to ProjectX: {e}")
            return False

    def _setup_streaming(self):
        """Set up streaming data connection."""
        try:
            self.logger.debug(f"Setting up streaming for account ID: {self.account_id}")
            self.streaming_client = self.client.get_streaming_client(self.account_id)

            # Set up event handlers
            self.streaming_client.on_order_update = self._handle_order_update
            self.streaming_client.on_position_update = self._handle_position_update
            self.streaming_client.on_trade_update = self._handle_trade_update
            self.streaming_client.on_account_update = self._handle_account_update

            # Start streaming connection
            if self.streaming_client.start_user_hub():
                self.streaming_client.subscribe_all(self.account_id)
                self.logger.debug(f"Subscribed to all streams for account {self.account_id}")
                # KEY FIX: Complete stream handshake for Lumibot integration
                self._stream_established()
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

    # --- Logging dedupe override ---
    def _on_new_order(self, order):  # override to suppress duplicates
        if not hasattr(self, '_creation_log_ids'):
            self._creation_log_ids = set()
        oid = getattr(order, 'identifier', None) or getattr(order, 'id', None) or id(order)
        if oid not in self._creation_log_ids:
            self._creation_log_ids.add(oid)
            # replicate original log format
            self.logger.info(colored(f"New order was created: {order}", color="green"))
        else:
            # silent or debug-level suppression; keep a trace
            self.logger.debug(f"[dedupe] suppressed duplicate creation log for order {oid}")
        # Continue normal event dispatch via base behavior (manual copy since not calling super())
        payload = dict(order=order)
        subscriber = self._get_subscriber(order.strategy)
        if subscriber:
            subscriber.add_event(subscriber.NEW_ORDER, payload)

    # ========== Required Broker Methods ==========

    def cancel_order(self, order: Order) -> bool:
        """Cancel an order at the broker."""
        try:
            if not order.id:
                self.logger.error("Cannot cancel order without ID")
                return False

            response = self.client.order_cancel(self.account_id, int(order.id))
            success = False
            if isinstance(response, dict):
                success = response.get("success") is True
            elif isinstance(response, bool):
                success = response

            if success:
                # Update order status
                order.status = "cancelled"
                # Ensure identifier present; lifecycle routing handled centrally
                if not getattr(order, "identifier", None):
                    order.identifier = order.id
                # Downgrade adapter-level logs; centralized system handles lifecycle
                self.logger.debug(f"Order {order.id} cancelled successfully")
                return True
            else:
                error_msg = None
                if isinstance(response, dict):
                    error_msg = response.get("error") or response.get("errorMessage")
                if not error_msg:
                    error_msg = "Unknown cancel failure"
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

                # Adapter should not emit high-level lifecycle logs
                self.logger.debug(f"Order {order.id} modified successfully")
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

            # Log order submission details
            self.logger.debug(f"Submitting order: {order.asset.symbol}, qty={order.quantity}, "
                            f"side={order.side}, type={order.order_type}")

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

            # Detect synthetic bracket parent (do NOT apply secondary prices to entry)
            is_bracket_parent = (
                getattr(order, 'order_class', None) == getattr(Order.OrderClass, 'BRACKET', None)
                and not getattr(order, '_is_bracket_child', False)
            )

            if is_bracket_parent:
                self.logger.debug(
                    f"[BRACKET DETECT] parent candidate id(temp)={getattr(order,'id',None)} tag={getattr(order,'tag',None)} "
                    f"order_class={getattr(order,'order_class',None)} sec_limit={getattr(order,'secondary_limit_price',None)} "
                    f"sec_stop={getattr(order,'secondary_stop_price',None)} limit={getattr(order,'limit_price',None)} stop={getattr(order,'stop_price',None)}"
                )
                # Capture intended TP/SL from secondary_* without sending them in parent
                tp_price = getattr(order, 'secondary_limit_price', None)
                sl_price = getattr(order, 'secondary_stop_price', None)
                # Use helper to build synthetic bracket metadata (pure, low-risk extraction)
                order._synthetic_bracket = create_bracket_meta(tp_price, sl_price)
                order._is_bracket_parent = True
                # Early store meta with a temporary key (will update key once broker id returned)
                # Early provisional meta store under temp key
                temp_key = getattr(order, 'id', None) or f"temp_{id(order)}"
                if not hasattr(self, '_bracket_meta'):
                    self._bracket_meta = {}
                early_store_bracket_meta(self._bracket_meta, temp_key, order._synthetic_bracket, self.logger)
                # Entry prices: only use primary limit/stop (rare) else None
                limit_price = self.client.round_to_tick_size(order.limit_price, tick_size) if getattr(order, 'limit_price', None) is not None else None
                stop_price = self.client.round_to_tick_size(order.stop_price, tick_size) if getattr(order, 'stop_price', None) is not None else None
            else:
                # Use extracted pure helper for non-bracket price selection
                limit_price, stop_price = select_effective_prices(order, self.client, tick_size)

            # Submit order
            # NOTE: ProjectXClient.order_place expects parameter name 'type', not 'order_type'
            # Passing 'order_type' caused: unexpected keyword argument 'order_type'
            # Ensure we supply a UNIQUE custom tag (ProjectX requires uniqueness per account)
            # Unique tag generation via helper (mirrors prior logic)
            previous_tag = getattr(order, 'tag', None)
            build_unique_order_tag(order)
            if previous_tag != getattr(order, 'tag', None):
                self.logger.debug(f"Auto-assigned order tag {order.tag}")

            # Apply bracket parent tag normalization AFTER tag generation
            if is_bracket_parent:
                # Normalize entry tag using helper (pure, preserves prior behavior)
                normalized_tag, base_tag = normalize_bracket_entry_tag(order.tag)
                if normalized_tag:
                    order.tag = normalized_tag
                if base_tag and hasattr(order, '_synthetic_bracket'):
                    order._synthetic_bracket['base_tag'] = base_tag

            response = self.client.order_place(
                account_id=self.account_id,
                contract_id=contract_id,
                type=order_type,
                side=order_side,
                size=order.quantity,
                limit_price=limit_price,
                stop_price=stop_price,
                custom_tag=order.tag
            )

            # Log response details
            self.logger.debug(f"Order response: success={response.get('success') if response else 'None'}, "
                            f"orderId={response.get('orderId') if response else 'None'}")
            
            if response and response.get("success"):
                # Step 1: Update order with broker's ID (matching Alpaca/Tradier pattern)
                order.id = str(response.get("orderId"))
                order.identifier = order.id  # Critical: Update identifier BEFORE tracking
                if is_bracket_parent and hasattr(order, '_synthetic_bracket'):
                    # If we previously stored under a temp key, migrate
                    try:
                        # Remove any temp_* entries that match tp/sl pair to prevent duplicate meta
                        for k in list(self._bracket_meta.keys()):
                            if k.startswith('temp_'):
                                temp_meta = self._bracket_meta.get(k)
                                if temp_meta and temp_meta.get('tp_price') == order._synthetic_bracket.get('tp_price') and temp_meta.get('sl_price') == order._synthetic_bracket.get('sl_price'):
                                    self._bracket_meta.pop(k, None)
                    except Exception:
                        pass
                    order._synthetic_bracket['parent_id'] = order.id
                    # Persist meta map for conversions
                    try:
                        self._bracket_meta[order.id] = dict(order._synthetic_bracket)
                        self.logger.debug(
                            f"[BRACKET META STORE] parent_id={order.id} tp={order._synthetic_bracket.get('tp_price')} "
                            f"sl={order._synthetic_bracket.get('sl_price')} tag={order.tag}"
                        )
                        # Ultra-fast fill race: fill events may have arrived before meta store; attempt spawn now if children not yet submitted.
                        try:
                            if not order._synthetic_bracket.get('children_submitted'):
                                # Ensure cache copy gets meta for subsequent events
                                cached = self._orders_cache.get(order.id)
                                if cached and not hasattr(cached, '_synthetic_bracket'):
                                    cached._synthetic_bracket = order._synthetic_bracket
                                    cached._is_bracket_parent = True
                                self.logger.debug(f"[BRACKET SPAWN IMMEDIATE] parent_id={order.id} status={order.status}")
                                self._maybe_spawn_bracket_children(order)
                        except Exception as ie:
                            self.logger.error(f"[BRACKET SPAWN IMMEDIATE ERROR] parent_id={order.id} err={ie}")
                    except Exception:
                        pass
                    self.logger.debug(f"[BRACKET DETECT CONFIRMED] parent_id={order.id} tp={order._synthetic_bracket.get('tp_price')} sl={order._synthetic_bracket.get('sl_price')}")
                
                self.logger.debug(f"Order submitted: id={order.id}, status=submitted")
                
                # Step 2: Set initial status and prices
                order.status = "submitted"
                order.limit_price = limit_price
                order.stop_price = stop_price
                
                # Step 3: Add to _unprocessed_orders FIRST (following gold standard pattern)
                # This is CRITICAL - must happen before _process_trade_event
                self._unprocessed_orders.append(order)
                
                # Step 4: Cache for quick lookups (optional optimization)
                self._orders_cache[order.id] = order
                
                # Step 5: Process the NEW_ORDER event (moves from unprocessed to new)
                try:
                    self._process_trade_event(order, self.NEW_ORDER)
                    self.logger.debug(f"Order submitted successfully with ID: {order.id} - NEW_ORDER event dispatched")
                except Exception as e:
                    self.logger.error(f"Error dispatching NEW_ORDER event for {order.id}: {e}")
                    # Continue even if event dispatch fails
                    self.logger.debug(f"Order submitted successfully with ID: {order.id} (event dispatch failed)")

                # Note: children will be spawned upon fill event
            else:
                error_msg = response.get("errorMessage", "Unknown error") if response else "No response"
                # Map specific broker error codes/messages to standardized handling
                lowered = error_msg.lower()
                if "maximum position exceeded" in lowered:
                    # Treat as risk/size violation rather than generic reject, so strategy can downsize
                    order.status = "rejected"
                    order.error = "max_position_exceeded"
                    self.logger.warning(
                        f"ProjectX risk check: maximum position would be exceeded by this order (qty={order.quantity}). "
                        f"Broker message: {error_msg}"
                    )
                else:
                    order.status = "rejected"
                    order.error = error_msg
                    # Keep error log for submit failures
                    self.logger.error(f"Failed to submit order: {error_msg}")

            return order

        except Exception as e:
            order.status = "rejected"
            order.error = str(e)
            # Keep error log for visibility
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
                self.logger.debug("Auto-connecting to get account ID")
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
                self.logger.debug("Auto-connecting to get account ID")
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
                    # Guard against unexpected payload shapes (e.g., list)
                    if not isinstance(broker_order, dict):
                        self.logger.debug(f"Skipping non-dict order payload: {type(broker_order)}")
                        continue
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
                    # Guard against non-dict in error path
                    order_id = broker_order.get('id', 'unknown') if isinstance(broker_order, dict) else 'unknown'
                    self.logger.error(f"❌ Failed to convert order {order_id}: {e}")
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
                self.logger.debug("Auto-connecting to get account ID")
                if not self.connect():
                    raise Exception("Failed to auto-connect for position retrieval - no account ID available")

            positions_data = self.client.get_positions(self.account_id)

            self.logger.debug(f"Retrieved {len(positions_data)} positions from broker")

            positions = []

            for broker_position in positions_data:
                try:
                    if not isinstance(broker_position, dict):
                        self.logger.debug(f"Skipping non-dict position payload: {type(broker_position)}")
                        continue
                    position = self._convert_broker_position_to_lumibot_position(broker_position)
                    if position is not None:
                        positions.append(position)
                        # Update cache
                        self._positions_cache[position.asset.symbol] = position
                    else:
                        contract_id = broker_position.get('contractId', 'unknown') if isinstance(broker_position, dict) else 'unknown'
                        self.logger.debug(f"Position {contract_id} conversion returned None")
                except Exception as e:
                    contract_id = broker_position.get('contractId', 'unknown') if isinstance(broker_position, dict) else 'unknown'
                    self.logger.error(f"❌ Failed to convert position {contract_id}: {e}")
                    continue

            # Final safety check - filter out any None positions
            positions = [position for position in positions if position is not None]

            # Log summary of converted positions
            if positions:
                position_summary = ", ".join([f"{pos.quantity} {pos.asset.symbol}" for pos in positions])
                self.logger.debug(f"Converted {len(positions)} positions: {position_summary}")
            else:
                # Downgraded to debug to avoid repetitive noise
                self.logger.debug("No positions found")

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
        """No-op: adapter doesn't emit lifecycle; core Broker manages routing."""
        return

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
        """Get all orders from broker, including recently filled ones via trades."""
        try:
            # Use a tighter time window around "now" as recommended by the other AI
            # This helps catch recently placed orders and avoids missing them
            end_date = datetime.now() + timedelta(seconds=30)  # Look slightly ahead for clock skew
            start_date = datetime.now() - timedelta(minutes=5)  # Look back 5 minutes for recent orders
            
            self.logger.debug(f"Searching orders: account={self.account_id}, "
                            f"start={start_date.isoformat()}, end={end_date.isoformat()}")

            # Search for orders
            orders = self.client.order_search(
                account_id=self.account_id,
                start_datetime=start_date.isoformat(),
                end_datetime=end_date.isoformat()
            )

            # Process order search results
            order_list = []
            if isinstance(orders, list):
                order_list = orders
                self.logger.debug(f"API returned {len(orders)} orders")
            elif isinstance(orders, dict) and orders.get("success"):
                order_list = orders.get("orders", [])
                self.logger.debug(f"API returned {len(order_list)} orders")
            
            # Also search for recent trades to catch filled market orders
            # Trades are the ground truth for fills according to the other AI
            try:
                trades = self._search_recent_trades(start_date, end_date)
                self.logger.debug(f"Found {len(trades)} recent trades")
                
                # For each trade, ensure we have the corresponding order
                for trade in trades:
                    order_id = str(trade.get('orderId'))
                    # Check if we already have this order
                    found = False
                    for order in order_list:
                        if str(order.get('id')) == order_id:
                            found = True
                            # Update order with fill info from trade
                            if trade.get('price'):
                                order['filledPrice'] = trade.get('price')
                            if trade.get('size'):
                                order['fillVolume'] = trade.get('size')
                            break
                    
                    if not found:
                        # Create a synthetic order record from trade data
                        # This helps catch market orders that filled instantly
                        synthetic_order = {
                            'id': order_id,
                            'accountId': trade.get('accountId'),
                            'contractId': trade.get('contractId'),
                            'status': 2,  # Filled
                            'fillVolume': trade.get('size'),
                            'filledPrice': trade.get('price'),
                            'side': trade.get('side'),
                            # We don't know the original order type, assume market for instant fills
                            'type': 2  # Market
                        }
                        order_list.append(synthetic_order)
                        self.logger.debug(f"Added synthetic order from trade: {order_id}")
            except Exception as trade_e:
                self.logger.debug(f"Could not search trades: {trade_e}")
            
            return order_list

        except Exception as e:
            self.logger.error(f"Error getting all orders: {e}")
            return []
    
    def _search_recent_trades(self, start_date, end_date) -> List[dict]:
        """Search for recent trades to identify filled orders."""
        try:
            # Use the ProjectX trade search API
            response = self.client.api.trade_search(
                account_id=self.account_id,
                start_timestamp=start_date.isoformat(),
                end_timestamp=end_date.isoformat()
            )
            
            if response and response.get("success"):
                trades = response.get("trades", [])
                return trades
            return []
        except Exception as e:
            self.logger.debug(f"Error searching trades: {e}")
            return []

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
            # Ignore unexpected payloads
            if not isinstance(broker_order, dict):
                return None
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
                # Downgrade to debug to avoid adapter-level noise
                self.logger.debug(f"Unknown order status ID: {status_id} for order {broker_order.get('id')}")

            # Get the broker's order ID
            broker_order_id = str(broker_order.get("id"))
            # If we have a cached order, reuse critical lifecycle info & prevent status downgrade
            cached_order = self._orders_cache.get(broker_order_id) if hasattr(self, '_orders_cache') else None
            if cached_order:
                # Prevent status regression (e.g., filled -> new) from out-of-order stream messages
                terminal_statuses = {"fill", "filled", "canceled", "cancelled", "error"}
                if cached_order.status and cached_order.status.lower() in terminal_statuses:
                    # If incoming status is earlier lifecycle, keep terminal info
                    if status in ("new", "open"):
                        return cached_order  # Keep existing terminal order unchanged

            # Create Order object with broker's ID as identifier from the start
            order = Order(
                strategy="",  # Will be set by strategy when needed
                asset=asset,
                quantity=broker_order.get("size", 0),
                side=side,
                order_type=order_type,  # Use order_type instead of deprecated 'type'
                identifier=broker_order_id  # Set identifier to broker's ID right away
            )

            # Set additional properties
            order.id = broker_order_id
            order.status = status
            order.limit_price = broker_order.get("limitPrice")
            order.stop_price = broker_order.get("stopPrice")
            # Quantity & fill info
            order.filled_quantity = broker_order.get("filledSize", broker_order.get("filledQty", 0))
            # Robust fill price extraction across possible field names
            for price_key in ("avgFillPrice", "averagePrice", "avgPrice", "fillPrice", "price", "lastFillPrice"):
                if broker_order.get(price_key) is not None:
                    order.avg_fill_price = broker_order.get(price_key)
                    break
            order.tag = broker_order.get("customTag")

            # If cached order exists, inherit strategy & any previously known fill data
            if cached_order:
                if getattr(cached_order, 'strategy', None):
                    order.strategy = cached_order.strategy
                # Propagate synthetic bracket metadata & flags
                restore_bracket_meta_if_needed(order, {broker_order_id: cached_order} if cached_order else {}, getattr(self, '_bracket_meta', {}), self.logger)
                if getattr(cached_order, '_is_bracket_parent', False):
                    order._is_bracket_parent = True
                if getattr(cached_order, '_bracket_children_submitted', False):
                    order._bracket_children_submitted = True
                if getattr(cached_order, '_is_bracket_child', False):
                    order._is_bracket_child = True
                if hasattr(cached_order, '_bracket_parent_id') and not hasattr(order, '_bracket_parent_id'):
                    order._bracket_parent_id = getattr(cached_order, '_bracket_parent_id')
                # Fallback fill price if still missing
                if not getattr(order, 'avg_fill_price', None) and getattr(cached_order, 'avg_fill_price', None):
                    order.avg_fill_price = cached_order.avg_fill_price
                # Fallback filled quantity
                if (getattr(order, 'filled_quantity', None) in (None, 0)) and getattr(cached_order, 'filled_quantity', None):
                    order.filled_quantity = cached_order.filled_quantity
                # Preserve previously known fill info if new payload omits it
                prev_avg = getattr(cached_order, 'avg_fill_price', None)
                if prev_avg is not None and not order.avg_fill_price:
                    order.avg_fill_price = prev_avg
                prev_filled_qty = getattr(cached_order, 'filled_quantity', None)
                if prev_filled_qty is not None and not order.filled_quantity:
                    order.filled_quantity = prev_filled_qty

            # Attempt to resolve strategy when missing (prevents 'Subscriber  not found')
            if not getattr(order, 'strategy', None):
                # 1. Tag prefix heuristic (tag generated like STRATNAME-XXXXXXXX)
                if order.tag and hasattr(self, '_subscribers') and self._subscribers:
                    tag_prefix = order.tag.split('-')[0].upper()
                    try:
                        for sub in self._subscribers:
                            sub_name = getattr(sub, 'name', '') or str(sub)
                            if sub_name.upper().startswith(tag_prefix):
                                order.strategy = sub_name
                                break
                    except Exception:
                        pass
                # 2. Single subscriber shortcut
                if not getattr(order, 'strategy', None) and hasattr(self, '_subscribers') and len(self._subscribers) == 1:
                    only_sub = self._subscribers[0]
                    order.strategy = getattr(only_sub, 'name', '') or str(only_sub)
                # 3. Fallback to cached order even if strategy empty string above
                if (not getattr(order, 'strategy', None)) and cached_order and getattr(cached_order, 'strategy', None):
                    order.strategy = cached_order.strategy

            # Set timestamps
            if broker_order.get("createdDateTime"):
                order.created_at = pd.to_datetime(broker_order["createdDateTime"])
            if broker_order.get("updatedDateTime"):
                order.updated_at = pd.to_datetime(broker_order["updatedDateTime"])

            # Restore bracket meta from persistent map if not already attached
            if restore_bracket_meta_if_needed(order, self._orders_cache if hasattr(self, '_orders_cache') else {}, getattr(self, '_bracket_meta', {}), self.logger):
                if getattr(order, '_synthetic_bracket', None):
                    order._is_bracket_parent = True

            return order

        except Exception as e:
            # Keep error log; avoid crashing conversion loop
            self.logger.error(f"Error converting broker order: {e}")
            return None

    # ================= Lifecycle Processing Helpers ==================
    # Adapter-level lifecycle routing removed; rely on core Broker's centralized processors

    def _convert_broker_position_to_lumibot_position(self, broker_position: dict) -> Position:
        """Convert ProjectX position to Lumibot Position object."""
        try:
            # Ignore unexpected payloads
            if not isinstance(broker_position, dict):
                return None
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
            # Keep error for debugging; no adapter-level lifecycle logs
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

        except Exception:
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

    # ========== Event Dispatch Methods ==========

    def _detect_and_dispatch_order_changes(self, new_order):
        """Detect status changes and dispatch appropriate events."""
        try:
            if new_order.id in self._orders_cache:
                cached_order = self._orders_cache[new_order.id]
                
                # Preserve strategy name from cached order
                if not new_order.strategy and cached_order.strategy:
                    new_order.strategy = cached_order.strategy
                    
                if cached_order.status != new_order.status:
                    self.logger.debug(f"Order status change detected: {new_order.id} {cached_order.status} -> {new_order.status}")
                    self._dispatch_status_change(cached_order, new_order)
            else:
                # First time seeing this order
                if new_order.status == "new" or new_order.status == "open":
                    # New order being tracked for first time
                    self._process_trade_event(new_order, self.NEW_ORDER)
                else:
                    # Order was created before strategy started - handle initial state
                    self._handle_pre_existing_order(new_order)
        except Exception as e:
            self.logger.error(f"Error detecting order changes for {new_order.id}: {e}")

    def _dispatch_status_change(self, cached_order, new_order):
        """Dispatch appropriate event based on status change."""
        try:
            status = new_order.status.lower()
            
            # Map Project X statuses to Lumibot events - After STATUS_ALIAS_MAP normalization
            # Note: statuses have already been normalized through STATUS_ALIAS_MAP in Order class
            if status == "new" or status == "open":
                # New or Open orders trigger NEW_ORDER event
                self._process_trade_event(new_order, self.NEW_ORDER)
            elif status in ("fill", "filled"):
                # Filled orders (status=2 becomes "filled" then aliased to "fill")
                # Ensure bracket metadata is preserved
                if getattr(cached_order, '_is_bracket_parent', False) and not getattr(new_order, '_is_bracket_parent', False):
                    new_order._is_bracket_parent = True
                restore_bracket_meta_if_needed(new_order, {getattr(cached_order,'id',None): cached_order} if cached_order else {}, getattr(self, '_bracket_meta', {}), self.logger)
                if getattr(cached_order, '_is_bracket_child', False) and not getattr(new_order, '_is_bracket_child', False):
                    new_order._is_bracket_child = True
                    if hasattr(cached_order, '_bracket_parent_id'):
                        new_order._bracket_parent_id = getattr(cached_order, '_bracket_parent_id')

                price = getattr(new_order, 'avg_fill_price', None)
                if price is None:
                    price = getattr(cached_order, 'avg_fill_price', None)
                if price is None:
                    price = getattr(new_order, 'limit_price', None) or getattr(new_order, 'stop_price', None)
                if price is None:
                    price = getattr(cached_order, 'limit_price', None) or getattr(cached_order, 'stop_price', None)
                quantity = getattr(new_order, 'filled_quantity', None)
                if (quantity is None or quantity == 0):
                    quantity = getattr(cached_order, 'filled_quantity', None)
                if (quantity is None or quantity == 0):
                    quantity = getattr(new_order, 'quantity', None) or getattr(cached_order, 'quantity', None)
                
                if price is None:
                    self.logger.debug(f"[FILL PRICE MISSING] Using 0.0 placeholder for order {new_order.id}")
                    price = 0.0
                if quantity is None:
                    quantity = getattr(new_order, 'quantity', None) or getattr(cached_order, 'quantity', 0)
                self._process_trade_event(
                    new_order, 
                    self.FILLED_ORDER, 
                    price=price, 
                    filled_quantity=quantity,
                    multiplier=new_order.asset.multiplier if new_order.asset else 1
                )
                # Bracket parent: spawn children after processing fill event (even if price fallback)
                # Use helper _is_bracket_parent to fall back on stored meta map if attribute missing
                if self._is_bracket_parent(new_order) and not getattr(new_order, '_bracket_children_submitted', False):
                    self.logger.debug(f"[BRACKET SPAWN CHECK] parent_id={new_order.id} has_meta={hasattr(new_order,'_synthetic_bracket')} meta={getattr(new_order,'_synthetic_bracket',None)}")
                    try:
                        self._maybe_spawn_bracket_children(new_order)
                    except Exception as be:
                        self.logger.error(f"Bracket child spawn failed for parent {new_order.id}: {be}")
                # Bracket child: handle sibling cancellation
                if getattr(new_order, '_is_bracket_child', False):
                    try:
                        self._handle_bracket_child_fill(new_order)
                    except Exception as ce:
                        self.logger.error(f"Error handling bracket child fill {new_order.id}: {ce}")
            elif status == "canceled":
                # Cancelled orders (status=3 becomes "cancelled" then aliased to "canceled")
                # Also handles expired (status=4 becomes "expired" then aliased to "canceled")
                self._process_trade_event(new_order, self.CANCELED_ORDER)
            elif status == "error":
                # Rejected orders (status=5 becomes "rejected" then aliased to "error")
                self._process_trade_event(new_order, self.ERROR_ORDER)
                # If bracket child errors, deactivate bracket
                if getattr(new_order, '_is_bracket_child', False):
                    parent_id = self._bracket_parent_by_child_id.get(new_order.id)
                    parent = self._orders_cache.get(parent_id) if parent_id else None
                    if parent and getattr(parent, '_synthetic_bracket', None):
                        parent._synthetic_bracket['active'] = False
            elif status == "partial_fill":
                # Partially filled orders (status=7 if supported)
                price = getattr(new_order, 'avg_fill_price', None) or getattr(new_order, 'limit_price', None)
                quantity = getattr(new_order, 'filled_quantity', None) or getattr(new_order, 'quantity', None)
                
                if price is not None and quantity is not None:
                    self._process_trade_event(
                        new_order,
                        self.PARTIALLY_FILLED_ORDER,
                        price=price,
                        filled_quantity=quantity, 
                        multiplier=new_order.asset.multiplier if new_order.asset else 1
                    )
                else:
                    self.logger.warning(f"Partial fill event missing price ({price}) or quantity ({quantity}) data for order {new_order.id}")
            else:
                self.logger.debug(f"Unknown or unhandled order status for event dispatch: {status}")
                
        except Exception as e:
            self.logger.error(f"Error dispatching status change for order {new_order.id}: {e}")

    # ======== Synthetic Bracket Helpers =========
    # A BRACKET parent order (order_class=BRACKET) carries only the entry details; TP/SL are
    # supplied via secondary_limit_price / secondary_stop_price and captured into synthetic
    # metadata (no native broker OCO linkage). On parent submission we:
    #   1. Build meta (tp/sl/base_tag/children flags) and early-store under temp key.
    #   2. After broker assigns real order id, meta is migrated & children may spawn immediately
    #      (handles ultra-fast fills racing earlier implementations).
    #   3. On fill/status events, _maybe_spawn_bracket_children re-validates meta & spawns
    #      one limit (TP) and/or one stop (SL) child, tagging them BRK_TP_/BRK_STOP_ + base.
    #   4. Child fills invoke sibling cancellation for synthetic OCO behavior.
    # Resilience:
    #   - Meta restoration helper reattaches state if cache entries lost or events reorder.
    #   - Spawn predicate centralizes gate conditions; reasons logged at DEBUG for diagnostics.
    # Helpers live in projectx_helpers.py to keep this broker lean and testable.
    def _is_bracket_parent(self, order: Order) -> bool:
        if getattr(order, '_is_bracket_parent', False):
            return True
        return bool(getattr(self, '_bracket_meta', {}).get(getattr(order, 'id', None)))

    def _is_bracket_child(self, order: Order) -> bool:
        return getattr(order, '_is_bracket_child', False)

    def _maybe_spawn_bracket_children(self, parent: Order):
        """Spawn TP/SL child orders for a filled bracket parent."""
        try:
            self.logger.debug(f"[BRACKET SPAWN ENTER] parent={getattr(parent,'id',None)} is_parent={self._is_bracket_parent(parent)} meta_attached={hasattr(parent,'_synthetic_bracket')}")
            if not self._is_bracket_parent(parent):
                if parent.id not in self._bracket_meta:
                    self.logger.debug(f"[BRACKET SPAWN ABORT] not parent and no meta parent={getattr(parent,'id',None)}")
                    return
            meta = getattr(parent, '_synthetic_bracket', None)
            if not meta:
                meta = self._bracket_meta.get(parent.id)
                if not meta:
                    self.logger.debug(f"[BRACKET SPAWN ABORT] no meta found parent={parent.id}")
                    return
                else:
                    self.logger.debug(f"[BRACKET SPAWN META RESTORE] parent={parent.id} restored_from_map=True")
                parent._synthetic_bracket = meta
            eligible, reason = should_spawn_bracket_children(meta, parent)
            if not eligible:
                self.logger.debug(f"[BRACKET SPAWN ABORT] parent={parent.id} reason={reason}")
                return
            tp_price = meta.get('tp_price')
            sl_price = meta.get('sl_price')
            meta['children_submitted'] = True
            parent._bracket_children_submitted = True
            base_tag = meta.get('base_tag') or derive_base_tag(parent.tag or '')
            if tp_price is not None:
                try:
                    self.logger.debug(f"[BRACKET SPAWN] creating TP child parent={parent.id} price={tp_price}")
                    tp_child = self._create_bracket_child(parent, kind='tp', price=tp_price, base_tag=base_tag)
                    if tp_child and tp_child.id:
                        meta['children']['tp'] = tp_child.id
                        self._bracket_parent_by_child_id[tp_child.id] = parent.id
                except Exception as e:
                    self.logger.error(f"Failed to submit TP child for parent {parent.id}: {e}")
            if sl_price is not None:
                try:
                    self.logger.debug(f"[BRACKET SPAWN] creating SL child parent={parent.id} price={sl_price}")
                    sl_child = self._create_bracket_child(parent, kind='sl', price=sl_price, base_tag=base_tag)
                    if sl_child and sl_child.id:
                        meta['children']['sl'] = sl_child.id
                        self._bracket_parent_by_child_id[sl_child.id] = parent.id
                except Exception as e:
                    self.logger.error(f"Failed to submit SL child for parent {parent.id}: {e}")
            self.logger.debug(f"[BRACKET SPAWN COMPLETE] parent={parent.id} tp_child={meta.get('children',{}).get('tp')} sl_child={meta.get('children',{}).get('sl')}")
        except Exception as e:
            self.logger.error(f"[BRACKET SPAWN ERROR] parent={getattr(parent,'id',None)} error={e}")

    def _create_bracket_child(self, parent: Order, kind: str, price: float, base_tag: str) -> Order:
        """Create and submit a single bracket child (tp or sl)."""
        spec = build_bracket_child_spec(parent, kind, price, base_tag)
        child = Order(
            strategy=parent.strategy,
            asset=parent.asset,
            quantity=parent.quantity,
            side=spec['side'],
            order_type=spec['order_type'],
            identifier=None
        )
        # Mark as child to bypass bracket detection
        child._is_bracket_child = True
        child._bracket_parent_id = parent.id
        try:
            from datetime import datetime
            child.created_at = datetime.now()
        except Exception:
            pass
        child.tag = spec['tag']
        # Attach lightweight meta pointer for diagnostics (not full meta copy to avoid divergence)
        try:
            child._synthetic_bracket_child = True
        except Exception:
            pass
        # Assign prices
        if spec['price_key'] == 'limit_price':
            child.limit_price = spec['price_value']
        else:
            child.stop_price = spec['price_value']
        # Submit
        submitted = self._submit_order(child)
        if not submitted or not getattr(submitted, 'id', None):
            self.logger.error(f"Bracket child submission failed (kind={kind}) for parent {parent.id}")
        else:
            self.logger.debug(f"Bracket child submitted: parent={parent.id} kind={kind} id={submitted.id} price={price}")
        return submitted

    def _handle_bracket_child_fill(self, child: Order):
        """Cancel sibling when one bracket child fills."""
        parent_id = self._bracket_parent_by_child_id.get(child.id)
        if not parent_id:
            return
        parent = self._orders_cache.get(parent_id)
        if not parent or not getattr(parent, '_synthetic_bracket', None):
            return
        meta = parent._synthetic_bracket
        if not meta.get('active', False):
            return
        # Determine sibling
        siblings = meta.get('children', {})
        sibling_id = None
        for k, v in siblings.items():
            if v != child.id:
                sibling_id = v
                break
        if sibling_id and sibling_id in self._orders_cache:
            sibling_order = self._orders_cache[sibling_id]
            # Cancel only if not terminal already
            sibling_status = (getattr(sibling_order, 'status', '') or '').lower()
            if sibling_status not in {"fill", "filled", "canceled", "cancelled", "error"}:
                try:
                    self.cancel_order(sibling_order)
                    self.logger.debug(f"[BRACKET SIBLING CANCEL] canceled sibling={sibling_id} after child_fill={child.id}")
                except Exception as e:
                    self.logger.error(f"Failed cancel sibling {sibling_id} for parent {parent_id}: {e}")
        # Deactivate bracket
        meta['active'] = False
            
    def _handle_pre_existing_order(self, order):
        """Handle orders that existed before strategy started."""
        try:
            # Process as new order first, then handle final state
            if self._first_iteration:
                if order.status.lower() == "fill":
                    self._process_trade_event(order, self.NEW_ORDER)
                    price = getattr(order, 'avg_fill_price', None) or getattr(order, 'limit_price', None)
                    quantity = getattr(order, 'filled_quantity', None) or getattr(order, 'quantity', None)
                    if price and quantity:
                        self._process_trade_event(order, self.FILLED_ORDER, price=price, filled_quantity=quantity, multiplier=order.asset.multiplier if order.asset else 1)
                elif order.status.lower() == "canceled":
                    self._process_trade_event(order, self.NEW_ORDER)
                    self._process_trade_event(order, self.CANCELED_ORDER)
                elif order.status.lower() == "error":
                    self._process_trade_event(order, self.NEW_ORDER) 
                    self._process_trade_event(order, self.ERROR_ORDER)
                else:
                    # Just process as new
                    self._process_trade_event(order, self.NEW_ORDER)
            else:
                # Not first iteration, just add as new
                self._process_new_order(order)
        except Exception as e:
            self.logger.error(f"Error handling pre-existing order {order.id}: {e}")

    # ========== Streaming Event Handlers ==========

    def _handle_order_update(self, data):
        """Handle order update from streaming."""
        try:
            # Process streaming order updates
            
            # Stream can deliver a single dict or a list of dicts
            payloads = data if isinstance(data, list) else [data]
            for item in payloads:
                # Check if item is actually a dict
                if not isinstance(item, dict):
                    self.logger.debug(f"Unexpected order item type: {type(item)}")
                    continue
                
                # Extract the actual order data from the wrapper
                # Format is {'action': 1, 'data': {...actual order data...}}
                order_data = item.get('data', item)  # Use item itself if no 'data' key
                    
                # Process order data from streaming
                
                order = self._convert_broker_order_to_lumibot_order(order_data)
                if order is not None:
                    # KEY FIX: Detect status changes and dispatch lifecycle events
                    self._detect_and_dispatch_order_changes(order)
                    
                    # Update cache after processing events
                    self._orders_cache[order.id] = order
                    self.logger.debug(f"Order update processed: {order.id} -> {order.status}")
        except Exception as e:
            self.logger.error(f"Error handling order update: {e}", exc_info=True)

    def _handle_position_update(self, data):
        """Handle position update from streaming."""
        try:
            payloads = data if isinstance(data, list) else [data]
            for item in payloads:
                position = self._convert_broker_position_to_lumibot_position(item)
                if position is not None:
                    self._positions_cache[position.asset.symbol] = position
                    self.logger.debug(f"Position update received: {position.asset.symbol}")
        except Exception as e:
            self.logger.error(f"Error handling position update: {e}")

    def _handle_trade_update(self, data):
        """Handle trade update from streaming - trades are ground truth for fills."""
        try:
            # Process streaming trade updates
            
            # Process trade events to detect fills
            payloads = data if isinstance(data, list) else [data]
            for item in payloads:
                # Check if item is actually a dict
                if not isinstance(item, dict):
                    self.logger.debug(f"Unexpected trade item type: {type(item)}")
                    continue
                
                # Extract the actual trade data from the wrapper
                # Format is {'action': 0, 'data': {...actual trade data...}}
                trade_data = item.get('data', item)  # Use item itself if no 'data' key
                    
                # Process trade data from streaming
                
                # Extract order ID from trade - trades use 'orderId' to reference the order
                order_id = str(trade_data.get("orderId")) if trade_data.get("orderId") else None
                
                if order_id and order_id in self._orders_cache:
                    order = self._orders_cache[order_id]
                    
                    # Update order with fill information from trade
                    fill_price = trade_data.get("price")
                    fill_size = trade_data.get("size")
                    
                    if fill_price and fill_size:
                        # Mark order as filled based on trade data
                        order.status = "filled"
                        order.filled_quantity = fill_size
                        order.avg_fill_price = fill_price
                        
                        # Dispatch fill event - pass same order twice since it's the updated version
                        self._dispatch_status_change(order, order)
                        
                        self.logger.debug(f"Trade fill processed for order {order_id}: "
                                        f"{fill_size} @ {fill_price}")
                elif order_id:
                    self.logger.debug(f"Trade for unknown order {order_id} - might be pre-existing")
            
            # Trade updates can trigger order and position cache updates
            self._update_orders_cache()
            self._update_positions_cache()
        except Exception as e:
            self.logger.error(f"Error handling trade update: {e}", exc_info=True)

    def _handle_account_update(self, data):
        """Handle account update from streaming."""
        try:
            # Account updates may be dict or list; keep last dict seen
            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        self.account_info = item
            elif isinstance(data, dict):
                self.account_info = data
            self.logger.debug("Account update received")
        except Exception as e:
            self.logger.error(f"Error handling account update: {e}")

    def _add_subscriber(self, subscriber):
        """Override to sync orders when a strategy is added."""
        super()._add_subscriber(subscriber)
    # No adapter-level sync; core Broker handles first-iteration lifecycle
