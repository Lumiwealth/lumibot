"""
ProjectX Broker Implementation for Lumibot

Provides futures trading functionality through ProjectX broker integration.
Supports multiple underlying brokers (TSX, TOPONE, etc.) via ProjectX gateway.
"""

from __future__ import annotations

import threading

from lumibot._lazy_imports import LazyLogger, LazyModule, LazyPytzTimezoneRef, lazy_class
from lumibot.brokers.broker import Broker
# PollingStream usage was removed to align with centralized lifecycle in core Broker

# Import moved to avoid circular dependency
# from lumibot.credentials import PROJECTX_CONFIG

logger = LazyLogger(__name__)
TYPE_CHECKING = False
datetime = lazy_class("datetime", "datetime")
timedelta = lazy_class("datetime", "timedelta")
_DEFAULT_PYTZ = LazyPytzTimezoneRef("America/New_York")
Asset = lazy_class("lumibot.entities", "Asset")
DataSource = lazy_class("lumibot.data_sources", "DataSource")
Order = lazy_class("lumibot.entities", "Order")
Position = lazy_class("lumibot.entities", "Position")
ProjectXClient = None

if TYPE_CHECKING:
    from lumibot.data_sources import DataSource
    from lumibot.entities import Position


def _default_pytz():
    return _DEFAULT_PYTZ._load()


def __getattr__(name):
    if name == "LUMIBOT_DEFAULT_PYTZ":
        return _default_pytz()
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def _get_projectx_client_class():
    global ProjectXClient
    if ProjectXClient is None:
        from lumibot.tools.projectx_helpers import ProjectXClient
    return ProjectXClient


def _position_class():
    from lumibot.entities import Position

    return Position


def colored(*args, **kwargs):
    from termcolor import colored as _colored

    return _colored(*args, **kwargs)


class _LazyProjectXClient:
    def __init__(self, config, factory=None):
        self._config = config
        self._factory = factory
        self._client = None

    def _load(self):
        if self._client is None:
            factory = self._factory or _get_projectx_client_class()
            self._client = factory(self._config)
        return self._client

    def __getattr__(self, name):
        return getattr(self._load(), name)

    def __setattr__(self, name, value):
        if name in {"_config", "_factory", "_client"}:
            object.__setattr__(self, name, value)
            return
        setattr(self._load(), name, value)


def create_bracket_meta(*args, **kwargs):
    from lumibot.tools.projectx_helpers import create_bracket_meta as _create_bracket_meta

    return _create_bracket_meta(*args, **kwargs)


def normalize_bracket_entry_tag(*args, **kwargs):
    from lumibot.tools.projectx_helpers import normalize_bracket_entry_tag as _normalize_bracket_entry_tag

    return _normalize_bracket_entry_tag(*args, **kwargs)


def build_unique_order_tag(*args, **kwargs):
    from lumibot.tools.projectx_helpers import build_unique_order_tag as _build_unique_order_tag

    return _build_unique_order_tag(*args, **kwargs)


def select_effective_prices(*args, **kwargs):
    from lumibot.tools.projectx_helpers import select_effective_prices as _select_effective_prices

    return _select_effective_prices(*args, **kwargs)


def bracket_child_tag(*args, **kwargs):
    from lumibot.tools.projectx_helpers import bracket_child_tag as _bracket_child_tag

    return _bracket_child_tag(*args, **kwargs)


def derive_base_tag(*args, **kwargs):
    from lumibot.tools.projectx_helpers import derive_base_tag as _derive_base_tag

    return _derive_base_tag(*args, **kwargs)


def early_store_bracket_meta(*args, **kwargs):
    from lumibot.tools.projectx_helpers import early_store_bracket_meta as _early_store_bracket_meta

    return _early_store_bracket_meta(*args, **kwargs)


def restore_bracket_meta_if_needed(*args, **kwargs):
    from lumibot.tools.projectx_helpers import restore_bracket_meta_if_needed as _restore_bracket_meta_if_needed

    return _restore_bracket_meta_if_needed(*args, **kwargs)


def should_spawn_bracket_children(*args, **kwargs):
    from lumibot.tools.projectx_helpers import should_spawn_bracket_children as _should_spawn_bracket_children

    return _should_spawn_bracket_children(*args, **kwargs)


def build_bracket_child_spec(*args, **kwargs):
    from lumibot.tools.projectx_helpers import build_bracket_child_spec as _build_bracket_child_spec

    return _build_bracket_child_spec(*args, **kwargs)


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
        4: "expired",          # Expired (mapped to canceled in adapter conversion)
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

    POSITION_TYPE_MAPPING = {
        1: "long",
        2: "short",
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
            self.logger = LazyLogger(f"ProjectXBroker_{firm_name}")
            self.logger.warning(
                f"No preferred account name set for {firm_name}. "
                f"Consider setting PROJECTX_{firm_name}_PREFERRED_ACCOUNT_NAME for better account selection."
            )

        self.config = config
        self.firm = config.get("firm")

        # The default broker and its market-data source share authentication and hubs.
        from lumibot.data_sources.projectx_data import ProjectXData
        if data_source is None:
            data_source = ProjectXData(config)
            self.client = _LazyProjectXClient(config, ProjectXClient)
            data_source.client = self.client
        elif (isinstance(ProjectXData, type) and isinstance(data_source, ProjectXData)
              and getattr(data_source, "config", None) == config):
            self.client = data_source.client
        else:
            self.client = _LazyProjectXClient(config, ProjectXClient)

        # Account management
        self.account_id = None
        self.account_info = {}

        # Streaming connection
        self.connect_stream = connect_stream
        self.streaming_client = None

        # Order/position caches
        self._positions_cache = {}  # Store positions
        # Bracket tracking maps (synthetic implementation)
        self._bracket_parent_by_child_id = {}
        self._bracket_meta = {}  # parent_id -> meta dict (persistent across conversions)
        # Asset cache for contract ID lookups
        self._asset_cache = {}
        self._contract_cache = {}

        # Lock to serialize order-update event dispatch and prevent race conditions
        # when "fill" and "new" websocket events arrive concurrently for the same order
        # (e.g. instantly-filling market orders). Use RLock so bracket-spawn callbacks
        # that re-enter dispatch don't deadlock.
        self._order_update_lock = threading.RLock()
        self._execution_reports = {}

        # Orders configuration
        self.order_lookback_days = 30  # Look back period for fetching historical orders

        # Thread management
        self.max_workers = max_workers

        # Setup logging
        self.logger = LazyLogger(f"ProjectXBroker_{self.firm}")

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
                if not self._setup_streaming():
                    return False

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
            if (self.streaming_client.start_user_hub()
                    and self.streaming_client.subscribe_all(self.account_id)):
                self.logger.debug(f"Subscribed to all streams for account {self.account_id}")
                # KEY FIX: Complete stream handshake for Lumibot integration
                self._stream_established()
                return True
            else:
                self.logger.warning("Failed to establish streaming connection")

        except Exception as e:
            self.logger.error(f"Failed to setup streaming: {e}")
        return False

    def disconnect(self):
        """Disconnect from ProjectX broker."""
        try:
            if self.streaming_client:
                self.streaming_client.stop()
            self.logger.info("Disconnected from ProjectX broker")
        except Exception as e:
            self.logger.error(f"Error disconnecting: {e}")

    # --- Logging dedupe override ---
    @classmethod
    def strategy_tag_matches(cls, tag, strategy_name):
        if super().strategy_tag_matches(tag, strategy_name):
            return True
        base = derive_base_tag(str(tag or ''))
        if len(base) == 54 and all(c in 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-' for c in base):
            from lumibot.tools.projectx_helpers import projectx_strategy_identity
            return base[:32] == projectx_strategy_identity(strategy_name)
        name, separator, suffix = base.rpartition('-')
        if not separator:
            return False
        if len(suffix) == 32 and all(c in '0123456789abcdef' for c in suffix):
            return super().strategy_tag_matches(name, strategy_name)
        # Older tags retained up to eight upper-case characters. Only exact
        # complete names are identifiable; a truncated shared prefix is ambiguous.
        return (len(suffix) == 10 and suffix.isdigit() and len(str(strategy_name)) <= 8
                and name == str(strategy_name).upper())

    def _reconcile_protective_fill(self, order, terminal):
        order.filled_quantity = sum(float(tx.quantity) for tx in order.transactions)
        if self._is_bracket_parent(order):
            self._maybe_spawn_bracket_children(order)
        if terminal and self._is_bracket_child(order):
            self._handle_bracket_child_fill(order)

    def _on_filled_order(self, position, order, price, quantity, multiplier):
        self._reconcile_protective_fill(order, terminal=True)
        super()._on_filled_order(position, order, price, quantity, multiplier)

    def _on_partially_filled_order(self, position, order, price, quantity, multiplier):
        self._reconcile_protective_fill(order, terminal=False)
        super()._on_partially_filled_order(position, order, price, quantity, multiplier)

    def _on_new_order(self, order):  # override to suppress duplicates
        if not hasattr(self, '_creation_log_ids'):
            self._creation_log_ids = set()
        oid = str(getattr(order, 'identifier', None) or getattr(order, 'id', None) or id(order))
        if oid not in self._creation_log_ids:
            self._creation_log_ids.add(oid)
            # replicate original log format
            self.logger.info(colored(f"New order was created: {order}", color="green"))
        else:
            # silent or debug-level suppression; keep a trace
            self.logger.debug(f"[dedupe] suppressed duplicate creation log for order {oid}")
            return
        # Continue normal event dispatch via base behavior (manual copy since not calling super())
        payload = dict(order=order)
        subscriber = self._get_subscriber(order.strategy)
        if subscriber:
            subscriber.add_event(subscriber.NEW_ORDER, payload)

    # ========== Required Broker Methods ==========

    def cancel_order(self, order: Order) -> bool:
        """Cancel an order at the broker."""
        try:
            if not order.identifier:
                self.logger.error("Cannot cancel order without ID")
                return False

            response = self.client.order_cancel(self.account_id, int(order.identifier))
            success = False
            if isinstance(response, dict):
                success = response.get("success") is True
            elif isinstance(response, bool):
                success = response

            if success:
                # Update order status
                # order.status = "cancelled"
                # Downgrade adapter-level logs; centralized system handles lifecycle
                self.logger.debug(f"Order {order.identifier} cancelled successfully")
                return True
            else:
                error_msg = None
                if isinstance(response, dict):
                    error_msg = response.get("error") or response.get("errorMessage")
                if not error_msg:
                    error_msg = "Unknown cancel failure"
                self.logger.error(f"Failed to cancel order {order.identifier}: {error_msg}")
                return False

        except Exception as e:
            self.logger.error(f"Error cancelling order {order.identifier}: {e}")
            return False

    def _modify_order(self, order: Order, limit_price: float = None,
                     stop_price: float = None) -> bool:
        """Modify an existing order."""
        try:
            if not order.identifier:
                self.logger.error("Cannot modify order without ID")
                return False

            # Get contract tick size for price rounding
            # contract_id = self._get_contract_id_from_asset(order.asset)
            contract_id = self.data_source._get_contract_id_from_asset(order.asset)
            if contract_id:
                tick_size = self.client.get_contract_tick_size(contract_id)

                # Round prices to tick size and handle Decimal case that can't be serialized
                if limit_price is not None:
                    limit_price = self.client.round_to_tick_size(float(limit_price), tick_size)
                if stop_price is not None:
                    stop_price = self.client.round_to_tick_size(float(stop_price), tick_size)

            response = self.client.order_modify(
                account_id=self.account_id,
                order_id=int(order.identifier),
                size=int(order.quantity),
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
                self.logger.debug(f"Order {order.identifier} modified successfully")
                return True
            else:
                error_msg = response.get("errorMessage", "Unknown error") if response else "No response"
                self.logger.error(f"Failed to modify order {order.identifier}: {error_msg}")
                return False

        except Exception as e:
            self.logger.error(f"Error modifying order {order.identifier}: {e}")
            return False

    def _submit_order(self, order: Order) -> Order:
        """Submit a new order to the broker."""
        try:
            from decimal import Decimal
            size = Decimal(str(order.quantity))
            if not size.is_finite() or size <= 0 or size != size.to_integral_value():
                raise ValueError("ProjectX futures quantities must be positive whole contracts")
            # Get contract ID from asset
            contract_id = self.data_source._get_contract_id_from_asset(order.asset)
            if not contract_id:
                order.status = "rejected"
                order.error = f"Could not find contract for {order.asset.symbol}"
                return order

            self._asset_cache[contract_id] = order.asset

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

            side_t = "buy" if order.is_buy_order() else "sell" if order.is_sell_order() else "unknown"
            order_side = self.ORDER_SIDE_MAPPING.get(side_t, None)
            if order_side is None:
                order.status = "rejected"
                order.error = f"Unsupported order side: {order.side}"
                return order

            # Detect synthetic bracket parent (do NOT apply secondary prices to entry)
            is_bracket_parent = (
                order.order_class == Order.OrderClass.BRACKET
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
                order.identifier = str(response.get("orderId"))  # Critical: Update identifier BEFORE tracking
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
                    order._synthetic_bracket['parent_id'] = order.identifier
                    # Persist meta map for conversions
                    try:
                        self._bracket_meta[order.identifier] = dict(order._synthetic_bracket)
                        self.logger.debug(
                            f"[BRACKET META STORE] parent_id={order.identifier} "
                            f"tp={order._synthetic_bracket.get('tp_price')} "
                            f"sl={order._synthetic_bracket.get('sl_price')} tag={order.tag}"
                        )
                        # Ultra-fast fill race: fill events may have arrived before meta store; attempt spawn now if children not yet submitted.
                        try:
                            if not order._synthetic_bracket.get('children_submitted'):
                                # Ensure cache copy gets meta for subsequent events
                                cached = self.get_tracked_order(order.identifier)
                                if cached and not hasattr(cached, '_synthetic_bracket'):
                                    cached._synthetic_bracket = order._synthetic_bracket
                                    cached._is_bracket_parent = True
                                self.logger.debug(f"[BRACKET SPAWN IMMEDIATE] "
                                                  f"parent_id={order.identifier} status={order.status}")
                                self._maybe_spawn_bracket_children(cached or order)
                        except Exception as ie:
                            self.logger.error(f"[BRACKET SPAWN IMMEDIATE ERROR] parent_id={order.identifier} err={ie}")
                    except Exception:
                        pass
                    self.logger.debug(f"[BRACKET DETECT CONFIRMED] parent_id={order.identifier} tp={order._synthetic_bracket.get('tp_price')} sl={order._synthetic_bracket.get('sl_price')}")
                
                self.logger.debug(f"Order submitted: id={order.identifier}, status=submitted")
                
                # Step 2: Set initial status and prices
                order.status = "submitted"
                order.limit_price = limit_price
                order.stop_price = stop_price
                
                # Step 3: Add to _unprocessed_orders FIRST (following gold standard pattern)
                # This is CRITICAL - must happen before _process_trade_event
                # Also caches the order for quick lookup during event processing
                # Locking is required because instantly-filling orders may trigger _handle_order_update before
                # _submit_order finishes, causing race conditions and duplicate orders
                with self._order_update_lock:
                    existing_order = self.get_tracked_order(order.identifier)
                    if existing_order:
                        self.logger.debug(f"Order {order.identifier} already in tracking cache, skipping unprocessed "
                                          f"append")
                    else:
                        self._unprocessed_orders.append(order)

                    # An execution may have reached the canonical tracked order
                    # before the REST acknowledgement. Do not reopen it.
                    try:
                        if not existing_order or str(existing_order.status).lower() in {'unprocessed', 'submitted'}:
                            self._process_trade_event(order, self.NEW_ORDER)
                        self.logger.debug(f"Order submitted successfully with ID: {order.identifier} - NEW_ORDER event dispatched")
                    except Exception as e:
                        self.logger.error(f"Error dispatching NEW_ORDER event for {order.identifier}: {e}")
                        # Continue even if event dispatch fails
                        self.logger.debug(f"Order submitted successfully with ID: {order.identifier} (event dispatch failed)")

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
            end_date = datetime.now(_default_pytz()).replace(hour=23, minute=59, second=59)
            start_date = end_date.replace(hour=0, minute=0, second=0) - timedelta(days=self.order_lookback_days)

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

            # Preserve every provider-returned order, including older working exits.
            for broker_order in orders_data:
                conversion_attempts += 1
                try:
                    # Guard against unexpected payload shapes (e.g., list)
                    if not isinstance(broker_order, dict):
                        self.logger.debug(f"Skipping non-dict order payload: {type(broker_order)}")
                        continue
                    order_id = broker_order.get('id', 'unknown')

                    # Minimal logging - only log issues, not every successful conversion
                    order = self._convert_broker_order_to_lumibot_order(broker_order)
                    if order is not None:
                        recent_orders.append(order)
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
            self._restore_bracket_links(orders)
            self.logger.debug(f"✅ Processed {len(orders)} recent/active orders from {len(orders_data)} total")
            return orders

        except Exception as e:
            self.logger.error(f"❌ Error getting orders: {e}")
            raise RuntimeError("ProjectX order state is unavailable") from e

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
                        self._positions_cache[position.asset] = position
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
        cached_order = self.get_tracked_order(identifier)
        # Safety check to ensure we don't return None orders
        if cached_order is not None:
            return cached_order
        return None

    def _pull_broker_all_orders(self) -> List[dict]:
        """Get all orders from broker, including recently filled ones via trades."""
        try:
            # Get all orders from today to catch any recent activity
            end_date = datetime.now(_default_pytz()).replace(hour=23, minute=59, second=59)
            start_date = end_date.replace(hour=0, minute=0, second=0)

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
            
            # Recover execution totals only for orders whose original terms are known.
            # A trade alone cannot reveal the original order's size/type or final state.
            trades_by_order = {}
            try:
                for trade in self._search_recent_trades(start_date, end_date):
                    if not trade.get("voided") and trade.get("id") is not None:
                        trades_by_order.setdefault(str(trade.get("orderId")), {})[trade["id"]] = trade
                for order in order_list:
                    trades = list(trades_by_order.get(str(order.get("id")), {}).values())
                    if not trades:
                        continue
                    quantity = sum(float(row.get("size") or 0) for row in trades)
                    notional = sum(float(row.get("size") or 0) * float(row["price"])
                                   for row in trades if row.get("price") is not None)
                    if quantity > float(order.get("fillVolume") or 0):
                        order["fillVolume"] = quantity
                        order["filledPrice"] = notional / quantity
            except Exception as exc:
                self.logger.warning(f"Execution history recovery unavailable: {type(exc).__name__}")

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
        """Get ProjectX contract ID from Lumibot asset. DEPRICATED: use data_source method."""
        self.logger.warning(f"⚠️ _get_contract_id_from_asset is deprecated in broker; use data_source method instead.")
        try:
            symbol = asset.symbol
            cache_key = f"{symbol}_{asset.asset_type}"
            if cache_key in self._contract_cache:
                return self._contract_cache[cache_key]

            # Start with the API client lookup because ProjectX only allows a single contract per symbol and
            # near the end of the quarter it is hard to predict which contract will be active. Result will be cached
            # to avoid repeated API lookups.
            contract_id = self.client.find_contract_by_symbol(symbol)

            # Handle continuous futures using Asset class logic
            if not contract_id and asset.asset_type == Asset.AssetType.CONT_FUTURE:
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

                except Exception as asset_error:
                    self.logger.warning(f"⚠️ Asset class resolution failed, falling back to client method: {asset_error}")

            if not contract_id:
                self.logger.error(f"Contract not found for asset: {asset.symbol} (type: {asset.asset_type})")
                return ""

            # Update the cache
            self._contract_cache[cache_key] = contract_id

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
            tracked = self.get_tracked_order(str(broker_order.get("id")))
            asset = tracked.asset if tracked is not None else self._get_asset_from_contract_id_cached(contract_id)

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
            cached_order = self.get_tracked_order(broker_order_id)
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
            order.identifier = broker_order_id
            order.status = status
            order.limit_price = broker_order.get("limitPrice")
            order.stop_price = broker_order.get("stopPrice")
            order.tag = broker_order.get("customTag")

            # Helper to return first non-None value from broker_order for given keys
            def _first_non_none(keys, default=None):
                for key in keys:
                    if key in broker_order and broker_order.get(key) is not None:
                        return broker_order.get(key)
                return default

            # Quantity & fill info
            order.filled_quantity = _first_non_none(("fillVolume", "filledSize", "filledQty"), 0)
            order.avg_fill_price = _first_non_none(("filledPrice", "avgFillPrice", "averagePrice", "avgPrice",
                                                    "fillPrice", "price", "lastFillPrice"), None)

            if status in {"new", "open"} and 0 < float(order.filled_quantity or 0) < float(order.quantity):
                order.status = "partial_fill"

            # Set timestamps using the helper to avoid KeyError if only one field exists
            order.broker_create_date = _first_non_none(("creationTimestamp", "createdDateTime"), None)
            order.created_at = order.broker_create_date
            order.broker_update_date = _first_non_none(("updateTimestamp", "updatedDateTime"), None)

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
            # Attempt to resolve strategy when missing (prevents 'Subscriber  not found')
            if not getattr(order, 'strategy', None):
                # 1. Tag prefix heuristic (tag generated like STRATNAME-XXXXXXXX)
                if order.tag and hasattr(self, '_subscribers') and self._subscribers:
                    try:
                        owners = [getattr(sub, 'name', '') or str(sub) for sub in self._subscribers]
                        owners = [name for name in owners if self.strategy_tag_matches(order.tag, name)]
                        if len(owners) == 1:
                            order.strategy = owners[0]
                    except Exception:
                        pass
                # 2. Single subscriber shortcut
                if not order.tag and not getattr(order, 'strategy', None) and hasattr(self, '_subscribers') and len(self._subscribers) == 1:
                    only_sub = self._subscribers[0]
                    order.strategy = getattr(only_sub, 'name', '') or str(only_sub)
                # 3. Fallback to cached order even if strategy empty string above
                if (not getattr(order, 'strategy', None)) and cached_order and getattr(cached_order, 'strategy', None):
                    order.strategy = cached_order.strategy

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
            Position = _position_class()
            # Ignore unexpected payloads
            if not isinstance(broker_position, dict):
                return None
            # Get asset from contract - use efficient cached lookup
            contract_id = broker_position.get("contractId")
            asset = self._get_asset_from_contract_id_cached(contract_id)

            if not asset:
                return None

            # Create Position object. Lumibot uses positive quantity for long, negative for short
            direction = self.POSITION_TYPE_MAPPING.get(broker_position.get("type"), "")
            quantity = broker_position.get("size", 0) if direction != "short" else -broker_position.get("size", 0)

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
        from lumibot.tools.projectx_helpers import asset_from_contract_details
        return asset_from_contract_details(self.client.get_contract_details(contract_id))

    def _get_asset_from_contract_id_cached(self, contract_id: str) -> Asset:
        if not contract_id:
            return None
        if contract_id in self._asset_cache:
            return self._asset_cache[contract_id]
        # Preserve a caller's dated contract identity through order/position recovery.
        for key, value in getattr(self.data_source, "_contract_cache", {}).items():
            if value == contract_id and isinstance(key, tuple):
                symbol, asset_type, expiration = key
                asset = Asset(symbol, asset_type=asset_type, expiration=expiration)
                from lumibot.tools.projectx_helpers import contract_price_multiplier
                multiplier = contract_price_multiplier(self.client.get_contract_details(contract_id))
                if multiplier is not None:
                    asset.multiplier = multiplier
                self._asset_cache[contract_id] = asset
                return asset
        try:
            asset = self._get_asset_from_contract_id(contract_id)
            if asset is not None:
                self._asset_cache[contract_id] = asset
            return asset
        except Exception as exc:
            self.logger.warning(f"Could not read contract metadata for {contract_id}: {type(exc).__name__}")
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
        """
        Update the orders cache with latest data.
        DEPRICATION WARNING: This method is no longer needed with using stored broker orders since double caching
        caused more issues than it solved. The core Broker's get_tracked_order directly returns the latest broker
        order data, so this cache update is redundant and can be removed in future versions. Keeping it here for now
        to avoid breaking changes, but it will no longer be called or maintained.
        """
        try:
            orders = self._get_orders_at_broker()
            for order in orders:
                if order is not None:  # Defensive check
                    self._orders_cache[order.identifier] = order
        except Exception as e:
            self.logger.error(f"Error updating orders cache: {e}")

    def _update_positions_cache(self):
        """Update the positions cache with latest data."""
        try:
            positions = self._get_positions_at_broker()
            for position in positions:
                if position is not None:  # Defensive check
                    self._positions_cache[position.asset] = position
        except Exception as e:
            self.logger.error(f"Error updating positions cache: {e}")

    # ========== Event Dispatch Methods ==========

    def _detect_and_dispatch_order_changes(self, new_order):
        """Detect status changes and dispatch appropriate events.

        This method acquires ``_order_update_lock`` to serialize concurrent
        websocket callbacks (e.g. a "fill" and a "new" event arriving at the
        same time for a market order that fills instantly).  Using an RLock
        allows bracket-spawn helpers that re-enter dispatch to proceed without
        deadlocking.
        """
        try:
            with self._order_update_lock:
                cached_order = self.get_tracked_order(new_order.identifier)
                if cached_order:
                    # Preserve strategy name from cached order
                    if not new_order.strategy and cached_order.strategy:
                        new_order.strategy = cached_order.strategy

                    if (cached_order.status != new_order.status or
                            float(getattr(new_order, "filled_quantity", 0) or 0) >
                            sum(float(tx.quantity) for tx in cached_order.transactions)):
                        self.logger.debug(f"Order status change detected: {new_order.identifier} {cached_order.status} -> {new_order.status}")
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
            self.logger.error(f"Error detecting order changes for {new_order.identifier}: {e}")

    def _dispatch_status_change(self, cached_order, new_order):
        """Apply cumulative provider fills once, then preserve any terminal state."""
        status = str(new_order.status).lower()
        previous_status = str(cached_order.status).lower()
        quantity = getattr(new_order, "filled_quantity", None)
        if not quantity and status in {"fill", "filled"}:
            quantity = new_order.quantity
        accounted = sum(float(tx.quantity) for tx in cached_order.transactions)
        total = float(quantity or 0)
        delta = total - accounted
        if delta > 0:
            price = getattr(new_order, "avg_fill_price", None)
            if price is None:
                self.logger.warning(f"Fill price missing for order {new_order.identifier}; awaiting execution data")
            else:
                event = self.FILLED_ORDER if total >= float(cached_order.quantity) else self.PARTIALLY_FILLED_ORDER
                if not self._reconcile_cumulative_execution(cached_order, total, price,
                        terminal=event == self.FILLED_ORDER):
                    self.logger.warning(f"Invalid or incomplete execution data for order {new_order.identifier}")
                    return
                cached_order.filled_quantity = total
                if previous_status in {"canceled", "error"} and event == self.PARTIALLY_FILLED_ORDER:
                    cached_order.status = previous_status
        if status in {"fill", "filled", "partial_fill", "new", "open"}:
            return
        if str(cached_order.status).lower() == status:
            return
        if status == "canceled":
            self._process_trade_event(cached_order, self.CANCELED_ORDER)
        elif status == "error":
            self._process_trade_event(cached_order, self.ERROR_ORDER)

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
    def _restore_bracket_links(self, orders):
        """Reattach already accepted protective orders using their persisted tags."""
        groups = {}
        for order in orders:
            tag = str(order.tag or "")
            for prefix, kind in (("BRK_ENTRY_", "entry"), ("BRK_TP_", "tp"), ("BRK_STOP_", "sl")):
                if tag.startswith(prefix):
                    groups.setdefault(tag[len(prefix):], {})[kind] = order
                    break
        for base, group in groups.items():
            parent = group.get("entry")
            children = {kind: group[kind] for kind in ("tp", "sl") if kind in group}
            if parent is None or not children:
                continue
            meta = self._bracket_meta.get(parent.identifier)
            if meta is None:
                meta = create_bracket_meta(getattr(group.get("tp"), "limit_price", None),
                                           getattr(group.get("sl"), "stop_price", None))
                meta.update(base_tag=base, children_submitted=True,
                            protected_quantity=float(getattr(parent, "filled_quantity", 0) or parent.quantity))
                self._bracket_meta[parent.identifier] = meta
            parent._synthetic_bracket = meta
            parent._is_bracket_parent = True
            parent._bracket_children_submitted = True
            for kind, child in children.items():
                meta["children"][kind] = child.identifier
                child._is_bracket_child = True
                child._bracket_parent_id = parent.identifier
                self._bracket_parent_by_child_id[child.identifier] = parent.identifier

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
                if parent.identifier not in self._bracket_meta:
                    self.logger.debug(f"[BRACKET SPAWN ABORT] not parent and no meta parent={getattr(parent,'id',None)}")
                    return
            meta = getattr(parent, '_synthetic_bracket', None)
            if not meta:
                meta = self._bracket_meta.get(parent.identifier)
                if not meta:
                    self.logger.debug(f"[BRACKET SPAWN ABORT] no meta found parent={parent.identifier}")
                    return
                else:
                    self.logger.debug(f"[BRACKET SPAWN META RESTORE] parent={parent.identifier} restored_from_map=True")
                parent._synthetic_bracket = meta
            eligible, reason = should_spawn_bracket_children(meta, parent)
            if not eligible:
                self.logger.debug(f"[BRACKET SPAWN ABORT] parent={parent.identifier} reason={reason}")
                return
            target = float(getattr(parent, 'filled_quantity', 0) or parent.quantity)
            previous = meta.get('protected_quantity', target)
            resized = True
            for kind, child_id in meta['children'].items():
                if target <= previous:
                    continue
                response = self.client.order_modify(account_id=self.account_id,
                    order_id=int(child_id), size=int(target))
                if not response or not response.get('success'):
                    resized = False
                    self.logger.error(f"Could not resize protective order {child_id}")
                else:
                    child = self.get_tracked_order(child_id)
                    if child is not None:
                        child.quantity = target
            tp_price = meta.get('tp_price')
            sl_price = meta.get('sl_price')
            base_tag = meta.get('base_tag') or derive_base_tag(parent.tag or '')
            if tp_price is not None and not meta['children'].get('tp'):
                try:
                    self.logger.debug(f"[BRACKET SPAWN] creating TP child parent={parent.identifier} price={tp_price}")
                    tp_child = self._create_bracket_child(parent, kind='tp', price=tp_price, base_tag=base_tag)
                    if tp_child and tp_child.identifier and str(tp_child.status).lower() not in {'error', 'rejected'}:
                        meta['children']['tp'] = tp_child.identifier
                        self._bracket_parent_by_child_id[tp_child.identifier] = parent.identifier
                except Exception as e:
                    self.logger.error(f"Failed to submit TP child for parent {parent.identifier}: {e}")
            if sl_price is not None and not meta['children'].get('sl'):
                try:
                    self.logger.debug(f"[BRACKET SPAWN] creating SL child parent={parent.identifier} price={sl_price}")
                    sl_child = self._create_bracket_child(parent, kind='sl', price=sl_price, base_tag=base_tag)
                    if sl_child and sl_child.identifier and str(sl_child.status).lower() not in {'error', 'rejected'}:
                        meta['children']['sl'] = sl_child.identifier
                        self._bracket_parent_by_child_id[sl_child.identifier] = parent.identifier
                except Exception as e:
                    self.logger.error(f"Failed to submit SL child for parent {parent.identifier}: {e}")
            complete = ((tp_price is None or bool(meta['children'].get('tp')))
                        and (sl_price is None or bool(meta['children'].get('sl'))))
            complete = complete and resized
            meta['children_submitted'] = complete
            if complete:
                meta['protected_quantity'] = target
            parent._bracket_children_submitted = complete
            self.logger.debug(f"[BRACKET SPAWN COMPLETE] parent={parent.identifier} tp_child={meta.get('children',{}).get('tp')} sl_child={meta.get('children',{}).get('sl')}")
        except Exception as e:
            self.logger.error(f"[BRACKET SPAWN ERROR] parent={getattr(parent,'id',None)} error={e}")

    def _create_bracket_child(self, parent: Order, kind: str, price: float, base_tag: str) -> Order:
        """Create and submit a single bracket child (tp or sl)."""
        spec = build_bracket_child_spec(parent, kind, price, base_tag)
        child = Order(
            strategy=parent.strategy,
            asset=parent.asset,
            quantity=getattr(parent, "filled_quantity", None) or parent.quantity,
            side=spec['side'],
            order_type=spec['order_type'],
            identifier=None
        )
        # Mark as child to bypass bracket detection
        child._is_bracket_child = True
        child._bracket_parent_id = parent.identifier
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
            self.logger.error(f"Bracket child submission failed (kind={kind}) for parent {parent.identifier}")
        else:
            self.logger.debug(f"Bracket child submitted: parent={parent.identifier} kind={kind} id={submitted.identifier} price={price}")
        return submitted

    def _handle_bracket_child_fill(self, child: Order):
        """Cancel sibling when one bracket child fills."""
        parent_id = self._bracket_parent_by_child_id.get(child.identifier)
        if not parent_id:
            return
        parent = self.get_tracked_order(parent_id)
        if not parent or not getattr(parent, '_synthetic_bracket', None):
            return
        meta = parent._synthetic_bracket
        if not meta.get('active', False):
            return
        # Determine sibling
        siblings = meta.get('children', {})
        sibling_id = None
        for k, v in siblings.items():
            if v != child.identifier:
                sibling_id = v
                break

        sibling_order = self.get_tracked_order(sibling_id)
        if sibling_order:
            # Cancel only if not terminal already
            sibling_status = (getattr(sibling_order, 'status', '') or '').lower()
            if sibling_status not in {"fill", "filled", "canceled", "cancelled", "error"}:
                try:
                    sibling_order.status = "cancelling"
                    self.cancel_order(sibling_order)
                    self.logger.debug(f"[BRACKET SIBLING CANCEL] canceled sibling={sibling_id} after child_fill={child.identifier}")
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
                    price = getattr(order, 'avg_fill_price', None)
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
            self.logger.error(f"Error handling pre-existing order {order.identifier}: {e}")

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
                if order_data.get("accountId") not in (None, self.account_id):
                    continue

                # Process order data from streaming
                order = self._convert_broker_order_to_lumibot_order(order_data)
                if order is not None:
                    # KEY FIX: Detect status changes and dispatch lifecycle events
                    self._detect_and_dispatch_order_changes(order)
                    self.logger.debug(f"Order update processed: {order.identifier} -> {order.status}")
        except Exception as e:
            self.logger.error(f"Error handling order update: {e}", exc_info=True)

    def _handle_position_update(self, data):
        """Cache account totals; strategy positions remain owned by core fill/sync logic."""
        payloads = data if isinstance(data, list) else [data]
        for item in payloads:
            if not isinstance(item, dict):
                continue
            item = item.get("data", item)
            if item.get("accountId") not in (None, self.account_id):
                continue
            position = self._convert_broker_position_to_lumibot_position(item)
            if position is None:
                continue
            if not position.quantity:
                self._positions_cache.pop(position.asset, None)
            else:
                self._positions_cache[position.asset] = position

    def _handle_trade_update(self, data):
        """Reconcile unique provider executions with cumulative order updates."""
        from types import SimpleNamespace
        payloads = data if isinstance(data, list) else [data]
        for item in payloads:
            if not isinstance(item, dict):
                continue
            trade = item.get("data", item)
            if trade.get("voided"):
                self.logger.warning("Provider reported a voided execution; account reconciliation required")
                continue
            if trade.get("accountId") not in (None, self.account_id):
                continue
            order_id = str(trade.get("orderId"))
            with self._order_update_lock:
                order = self.get_tracked_order(order_id)
                if order is None or trade.get("id") is None:
                    continue
                price, size = trade.get("price"), trade.get("size")
                if price is None or size is None or float(size) <= 0:
                    continue
                reports = self._execution_reports.setdefault(order_id, {})
                if trade["id"] in reports:
                    continue
                reports[trade["id"]] = trade
                total = sum(float(row["size"]) for row in reports.values())
                notional = sum(float(row["size"]) * float(row["price"]) for row in reports.values())
                # Fees remain provider facts even when an order update arrived first.
                order.trade_cost = sum(float(row.get("fees") or 0) for row in reports.values())
                snapshot = SimpleNamespace(identifier=order_id, filled_quantity=total,
                    quantity=order.quantity, avg_fill_price=notional / total,
                    status="fill" if total >= float(order.quantity) else "partial_fill")
                self._dispatch_status_change(order, snapshot)
        self._update_positions_cache()

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
