import logging, time, traceback
import os
from decimal import Decimal
from typing import Optional, List, Dict, Tuple, Any
import pandas as pd

from lumibot.trading_builtins import PollingStream

from lumibot.data_sources.bitunix_data import BitunixData
from lumibot.brokers import Broker, LumibotBrokerAPIError
from lumibot.entities import Asset, Order, Position
from lumibot.tools.bitunix_helpers import BitUnixClient

logger = logging.getLogger(__name__)

class Bitunix(Broker):
    """
    A broker class that connects to the Bitunix exchange for crypto futures trading.

    This broker is designed specifically for Bitunix's perpetual futures API. It supports submitting, tracking, and closing positions for crypto futures contracts (e.g., BTCUSDT perpetual). The broker uses Bitunix's REST API for all trading operations.

    Key Features:
    - Only supports crypto futures (TRADING_MODE must be "FUTURES").
    - Uses Bitunix's "flash close" endpoint to close open futures positions instantly at market price.
    - All positions and orders are managed using Bitunix's API conventions.
    - Not suitable for spot trading or non-futures assets.

    Notes:
    - The `close_position` method will use Bitunix's flash close endpoint, which is faster and more reliable for closing futures positions than submitting a regular market order.
    - All asset symbols should be the full Bitunix symbol (e.g., "BTCUSDT").
    - Leverage and margin settings are managed per-symbol as needed.
    """

    ASSET_TYPE_MAP = dict(
        stock=[],
        option=[],
        future=["future"],
        crypto_future=["future"],    # new crypto‐futures
        forex=[],
        crypto=["crypto"],
    )
    
    # Default quote asset for crypto transactions
    LUMIBOT_DEFAULT_QUOTE_ASSET = Asset("USDT", Asset.AssetType.CRYPTO)

    DEFAULT_POLL_INTERVAL = 5  # seconds between polling cycles

    def __init__(self, config, max_workers: int = 1, chunk_size: int = 100, connect_stream: bool = True, poll_interval: Optional[float] = None, data_source=None):
        # --- Bitunix trading mode check ---
        trading_mode = None
        if isinstance(config, dict):
            trading_mode = config.get("TRADING_MODE", "FUTURES")
        else:
            trading_mode = getattr(config, "TRADING_MODE", "FUTURES")
        if str(trading_mode).upper() != "FUTURES":
            print(f"Bitunix TRADING_MODE '{trading_mode}' is not supported yet. Please use another broker for spot trading.")

        # Ensure _stream_loop exists before calling super, so _launch_stream doesn't error
        self._stream_loop = None
        if isinstance(config, dict):
            api_key = config.get("API_KEY")
            api_secret = config.get("API_SECRET")
        else:
            api_key = getattr(config, "API_KEY", None)
            api_secret = getattr(config, "API_SECRET", None)
        
        # Track current leverage per symbol to avoid redundant API calls
        self.current_leverage: Dict[str, int] = {}
        # Override default market setting for to be 24/7, but still respect config/env if set
        self.market = (config.get("MARKET") if config else None) or os.environ.get("MARKET") or "24/7"

        if not api_key or not api_secret:
            raise ValueError("API_KEY and API_SECRET must be provided in config")

        # Initialize API client and WS attributes BEFORE calling super().__init__
        self.api = BitUnixClient(api_key=api_key, secret_key=api_secret)
        self.api_secret = api_secret  # needed for signing
        # Private-channel URL per BitUnix docs (kept for reference, but not used for polling)
        self.ws_url = "wss://fapi.bitunix.com/private/"
        '''
        # Set default futures position mode to hedge
        try:
            response = self.api.change_position_mode("HEDGE")
            # Check response code for success
            if response and response.get("code") == 0:
                logger.info(
                    f"Default position mode set to {response.get('data', [{}])[0].get('positionMode')}"
                )
            else:
                # Log specific error if code is not 0
                logger.warning(
                    f"Failed to set default position mode to HEDGE. API Response: {response}"
                )
        except Exception as e:
            # Log exception details
            logger.warning(f"Failed to set default position mode to HEDGE due to an exception: {e}")
            logger.debug(traceback.format_exc()) # Add debug level traceback for more detail if needed
        '''
        if not data_source:
            data_source = BitunixData(config, max_workers=max_workers, chunk_size=chunk_size)
            # Share the client instance with the data source if it was just created
            data_source.client = self.api
            # Share the client_symbols set with the broker for WebSocket subscriptions
            self.client_symbols = data_source.client_symbols

        self.poll_interval = poll_interval or self.DEFAULT_POLL_INTERVAL
        super().__init__(
            name="bitunix",
            connect_stream=connect_stream, # Use connect_stream to enable _run_stream thread
            data_source=data_source,
            config=config,
            max_workers=max_workers,
        )

    def get_quote_asset(self):
        # Only clear and set quote_assets if USDT is not the only asset
        if not (len(self.quote_assets) == 1 and Asset("USDT", Asset.AssetType.CRYPTO) in self.quote_assets):
            self.quote_assets.clear()
            self.quote_assets.add(Asset("USDT", Asset.AssetType.CRYPTO))
        
        return Asset("USDT", Asset.AssetType.CRYPTO)  

    def get_timestamp(self):
        return time.time()

    def is_market_open(self):
        return True

    def get_time_to_open(self):
        return 0

    def get_time_to_close(self):
        return float("inf")
    
    def _get_balances_at_broker(self, quote_asset: Asset, strategy) -> Optional[Tuple[float, float, float]]:
        """
        Returns (cash, positions_value, total_liquidation_value)
        """
        # ---------- FUTURES wallet only ----------
        # Force margin_coin to USDT
        fut_resp = self.api.get_account(margin_coin="USDT")
        try:
            data = fut_resp.get("data", {})

            # cash components
            available = Decimal(data.get("available", "0") or "0")
            frozen = Decimal(data.get("frozen", "0") or "0")
            margin = Decimal(data.get("margin", "0") or "0")
            cross_pnl = Decimal(data.get("crossUnrealizedPNL", "0") or "0")
            iso_pnl = Decimal(data.get("isolationUnrealizedPNL", "0") or "0")

            # equity / net liquidation
            fut_equity = available + frozen + margin + cross_pnl + iso_pnl
            net_liquidation = float(fut_equity)

        except Exception:
            logger.warning("Unexpected futures account response: %s", fut_resp)
            available = frozen = margin = cross_pnl = iso_pnl = Decimal("0")
            net_liquidation = 0.0

        cash = float(available)

        # Compute total notional of open futures positions
        positions_value = 0.0
        for pos in self._pull_positions(strategy):
            if pos.avg_fill_price:
                # Convert both quantity and avg_fill_price to float before multiplication
                positions_value += float(abs(pos.quantity)) * float(pos.avg_fill_price)

        return cash, positions_value, net_liquidation

    def _pull_positions(self, strategy) -> List[Position]:
        """
        Retrieves FUTURES positions.
        Futures positions are fetched from the open positions endpoint.
        """
        positions = []
        strategy_name = strategy.name if strategy else ""

        try:
            resp = self.api.get_positions()
            if resp and resp.get("code") == 0:
                for p in resp.get("data", []):
                    sym = p.get("symbol", "")
                    # qty is now under "qty"
                    qty = Decimal(str(p.get("qty", "0")))
                    # Bitunix now uses "BUY"/"SELL"
                    side = p.get("side", "").upper()
                    if side == "SELL":
                        qty = -abs(qty)
                    else:
                        qty = abs(qty)
                    # entry price is avgOpenPrice (fallback to entryValue)
                    entry = Decimal(str(p.get("avgOpenPrice", p.get("entryValue", "0"))))
                    if qty != 0 and sym:
                        asset = Asset(sym, Asset.AssetType.CRYPTO_FUTURE)
                        pos = Position(strategy_name, asset, qty)
                        pos.avg_fill_price = entry
                        pos._raw = p
                        positions.append(pos)
        except Exception as e:
            logger.warning("Error fetching futures positions: %s", e)
            logger.debug(traceback.format_exc())
        return positions

    def _map_side_to_bitunix(self, side: Order.OrderSide) -> str:
        """Map Lumibot order side to BitUnix side."""
        return "BUY" if side == Order.OrderSide.BUY else "SELL"
    
    def _map_type_to_bitunix(self, order_type: Order.OrderType) -> str:
        """Map Lumibot order type to BitUnix order type."""
        if order_type == Order.OrderType.LIMIT:
            return "LIMIT"
        elif order_type == Order.OrderType.MARKET:
            return "MARKET"
        elif order_type == Order.OrderType.STOP:
            return "STOP"
        elif order_type == Order.OrderType.STOP_LIMIT:
            return "STOP_LIMIT"
        else:
            return "MARKET"  # Default to MARKET for unknown types

    # --- Multi-leg, OCO, OTO, Bracket, Trailing Stop ---
    def _submit_orders(self, orders, is_multileg=False, order_type=None, duration="day", price=None):
        """
        Submit multiple orders. Bitunix does not support multi-leg, OCO, OTO, Bracket, or trailing stop natively.
        """
        if is_multileg or (orders and getattr(orders[0], "order_class", None) in [
            Order.OrderClass.MULTILEG, Order.OrderClass.OCO, Order.OrderClass.OTO, Order.OrderClass.BRACKET
        ]):
            raise NotImplementedError("Bitunix does not support multi-leg, OCO, OTO, or Bracket orders natively.")
        return [self._submit_order(order) for order in orders]

    def _submit_order(self, order: Order) -> Order:
        """
        Submits an order to BitUnix exchange.
        Handles FUTURES orders.
        """
        # Flag set by close_position() – when True we send a reduce‑only order
        reduce_only = getattr(order, "reduce_only", False)


        # Determine symbol format based on asset type
        if order.asset.asset_type in (Asset.AssetType.CRYPTO_FUTURE):
            symbol = order.asset.symbol
        else:
            error_msg = f"Invalid asset type: asset can only be CRYPTO_FUTURE"
            order.set_error(LumibotBrokerAPIError(error_msg))
            order.status = Order.OrderStatus.ERROR  # ensure status is enum
            return order

        # Prepare quantity and price
        quantity = abs(float(order.quantity))
        price = float(order.limit_price) if order.limit_price else None
        
        # Generate a client order ID for tracking
        client_order_id = f"lmbot_{int(time.time() * 1000)}_{hash(str(order)) % 10000}"

        try:
            # Ensure desired leverage is set
            leverage = order.asset.leverage
            try:
                if self.current_leverage.get(symbol) != leverage:
                    lev_resp = self.api.change_leverage(symbol=symbol, leverage=leverage, margin_coin=self.get_quote_asset().symbol) # Use quote_asset.symbol
                    if not lev_resp or lev_resp.get("code") != 0:
                        logger.warning(f"Failed to set leverage for {symbol} to {leverage}x: {lev_resp}")
                    else:
                        logger.info(f"Set leverage for {symbol} to {leverage}x")
                        self.current_leverage[symbol] = leverage
            except Exception as e:
                logger.warning(f"Error setting leverage for {symbol} to {leverage}: {e}")
            # FUTURES order
            params = {
                "symbol": symbol,
                "side": self._map_side_to_bitunix(order.side),
                "orderType": self._map_type_to_bitunix(order.order_type),
                "qty": quantity,
                "clientId": client_order_id,
                **({"reduceOnly": True} if reduce_only else {}),
            }
            if price is not None:
                params["price"] = price
            
            # TP/SL
            tp = getattr(order, "secondary_limit_price", None) or getattr(order, "take_profit_price", None)
            sl = getattr(order, "secondary_stop_price", None) or getattr(order, "stop_loss_price", None)

            if tp is not None:
                params["take_profit_price"] = float(tp)
            
            if sl is not None:
                params["stop_loss_price"] = float(sl)

            # Submit order
            response = self.api.place_order(**params)
            self.logger.info(response)
            # Immediately handle any non-zero API codes as errors
            code = response.get("code") if isinstance(response, dict) else None
            if code is None or code != 0:
                err_msg = (
                    f"Error placing order: code={code}, "
                    f"msg={response.get('msg') if isinstance(response, dict) else response}, "
                    f"data={response.get('data') if isinstance(response, dict) else None}"
                )
                order.set_error(LumibotBrokerAPIError(err_msg))
                order.status = Order.OrderStatus.ERROR  # ensure status is enum
                # Attach full response for debugging
                order.update_raw(response)
                self.stream.dispatch(self.ERROR_ORDER, order=order, error_msg=err_msg)
                return order
            # Success handling only
            data = response.get("data", {})
            order_id = data.get("orderId")
            if order_id:
                order.identifier = order_id
                order.status = Order.OrderStatus.SUBMITTED  # ensure status is enum
                order.update_raw(response)
                self._unprocessed_orders.append(order)
                self._process_trade_event(order, self.NEW_ORDER)
            else:
                error_msg = f"No order ID in response: {response}"
                order.set_error(LumibotBrokerAPIError(error_msg))
                order.status = Order.OrderStatus.ERROR  # ensure status is enum
                self.stream.dispatch(self.ERROR_ORDER, order=order, error_msg=error_msg)                
        except Exception as e:
            error_msg = f"Exception placing order: {str(e)}"
            order.set_error(LumibotBrokerAPIError(error_msg))
            order.status = Order.OrderStatus.ERROR  # ensure status is enum
            self.stream.dispatch(self.ERROR_ORDER, order=order, error_msg=error_msg)
        return order

    # ------------------------------------------------------------------
    # Position‑closing helper using Bitunix reduce‑only orders
    # ------------------------------------------------------------------
    def close_position(self, strategy_name: str, asset: Asset, fraction: float = 1.0):
        """
        Close all or part of an existing position using a reduce‑only
        market order (tradeSide="CLOSE").

        Parameters
        ----------
        strategy_name : str
            Name of the strategy that owns the position.
        asset : Asset
            The asset whose position should be closed.
        fraction : float, optional
            Fraction of the position to close (0 < fraction ≤ 1). Defaults to 1 (full close).

        Returns
        -------
        Optional[Order]
            The submitted order, or ``None`` if no position exists.
        """
        # Retrieve the current tracked position
        position = self.get_tracked_position(strategy_name, asset)
        if not position or position.quantity == 0:
            return None
        
        # Ensure fraction is between 0 and 1
        quantity = abs(position.quantity)
        
        # Create the order object
        order = Order(strategy_name, asset, quantity * fraction)

        # Reverse the side for closing
        if position.quantity > 0:
            order.side = Order.OrderSide.SELL
        elif position.quantity < 0:
            order.side = Order.OrderSide.BUY

        # Mark as reduce‑only so `_submit_order` will send tradeSide="CLOSE"
        setattr(order, "reduce_only", True)

        return self.submit_order(order)

    def cancel_order(self, order: Order):
        """
        Cancels a FUTURES order.
        """
        if not order.identifier:
            self.logger.warning("Specified order doesn't exist")
            return
            
        try:
            # Retry up to 5 times on network error
            response = self.api.cancel_order(order_id=order.identifier)

            # Check response
            if response and response.get("code") == 0:
                # Use self._process_trade_event for consistency
                self._process_trade_event(order, self.CANCELED_ORDER)
            else:
                # Log error but don't raise, let polling handle final state
                logger.error(f"Failed to cancel order {order.identifier}: {response}")
                # Dispatch an error event if immediate feedback is needed
                self._process_trade_event(order, self.ERROR_ORDER, error=LumibotBrokerAPIError(f"Failed to cancel order: {response}"))
        except Exception as e:
             # Log error but don't raise, let polling handle final state
            logger.error(f"Error canceling order {order.identifier}: {str(e)}")
            # Dispatch an error event
            self._process_trade_event(order, self.ERROR_ORDER, error=LumibotBrokerAPIError(f"Error canceling order: {str(e)}"))
            pass

    def _pull_broker_order(self, identifier: str, asset_type: Asset.AssetType=Asset.AssetType.CRYPTO) -> Optional[Dict]:
        """
        Fetches a single order by ID from BitUnix.
        """
        try:
            response = self.api.get_order_detail(order_id=identifier)
            if response and response.get("code") == 0:
                return response.get("data")
            return None
        except Exception:
            logger.error(f"Error getting order details for {identifier}")
            return None

    def _pull_broker_all_orders(self, symbol: Optional[str] = None, status: Optional[str] = None) -> List[Dict]:
        all_orders = []
        # Fetch FUTURES open orders
        try:
            fut_open_resp = self.api.get_pending_orders(symbol=symbol)
            if fut_open_resp and fut_open_resp.get("code") == 0:

                data = fut_open_resp.get("data") or {}
                fut_orders = data.get("orderList", [])
                if len(fut_orders) > 0:
                    all_orders.extend(fut_orders)
        except Exception:
            logger.warning("Error fetching futures open orders")
        return all_orders

    def _map_status_from_bitunix(self, broker_status) -> Order.OrderStatus:
        """Maps BitUnix order status string to Lumibot OrderStatus enum."""
        # Ensure broker_status is a string before uppercasing and strip trailing underscores and whitespace
        status_str = str(broker_status).upper().rstrip('_').strip()

        status_map = {
            "NEW":               Order.OrderStatus.SUBMITTED,
            "PARTIALLY_FILLED":  Order.OrderStatus.PARTIALLY_FILLED,
            "FILLED":            Order.OrderStatus.FILLED,
            "CANCELED":          Order.OrderStatus.CANCELED,
            "REJECTED":          Order.OrderStatus.ERROR,
            "EXPIRED":           Order.OrderStatus.CANCELED,
            "PENDING_CANCEL":    Order.OrderStatus.CANCELED,    # mapped to CANCELED since PENDING_CANCEL isn't defined
        }
        mapped_status = status_map.get(status_str)

        if mapped_status is None:
            logger.warning(f"Unmapped Bitunix order status received: '{broker_status}' (processed as '{status_str}'). Defaulting to ERROR.")
            # Return ERROR status for unrecognized states
            return Order.OrderStatus.ERROR
        return mapped_status

    def _parse_broker_order(
        self, response: Dict, strategy_name: str, strategy_object: Any = None
    ) -> Optional[Order]:
        """Converts BitUnix order response to Lumibot Order object."""
        if not response:
            return None
        
        try:
            # Extract order details
            order_id = response.get("orderId")
            symbol = response.get("symbol", "")
            status = response.get("status", "")
            side_raw = response.get("side", "")
            order_type = response.get("orderType", "")
            
            # Extract quantities and prices
            # fields use 'qty'/'tradeQty' for BitUnix
            qty_original = Decimal(str(response.get("qty",     "0")))
            qty_executed = Decimal(str(response.get("tradeQty","0")))
            # parse limit price only if numeric
            price_limit = None
            ps = response.get("price")
            if ps and isinstance(ps, str) and order_type != "MARKET":
                price_limit = Decimal(ps)
            # parse average price
            price_avg = None
            ap = response.get("avgPrice")
            if ap:
                price_avg = Decimal(str(ap))
            
            leverage=int(str(response.get("leverage", "1")))
                        
            asset = Asset(symbol, Asset.AssetType.CRYPTO_FUTURE, leverage=leverage)
            quote = None
            
            # Map order side
            side = Order.OrderSide.BUY if side_raw.upper() == "BUY" else Order.OrderSide.SELL
            
            # Map order type
            if order_type.upper() == "LIMIT":
                order_type_enum = Order.OrderType.LIMIT
            elif order_type.upper() == "MARKET":
                order_type_enum = Order.OrderType.MARKET
            elif order_type.upper() == "STOP":
                order_type_enum = Order.OrderType.STOP
            elif order_type.upper() == "STOP_LIMIT":
                order_type_enum = Order.OrderType.STOP_LIMIT
            else:
                order_type_enum = Order.OrderType.MARKET  # Default
            
            # Map order status
            order_status = self._map_status_from_bitunix(status)
            
            # Create Lumibot Order object (use keywords to avoid duplicate positional limit_price)
            order = Order(
                strategy=strategy_name,
                asset=asset,
                quantity=qty_original,
                side=side,
                order_type=order_type_enum,
                limit_price=price_limit,
                status=order_status,
                identifier=order_id,
                quote=quote
            )

            # Set filled info
            order.filled_quantity = qty_executed
            order.avg_fill_price = price_avg
            
            # Set creation time if available
            create_time = response.get("time") or response.get("createTime")
            if create_time:
                order.broker_create_date = pd.to_datetime(create_time, unit='ms', utc=True)
            
            # Store raw response for reference
            order.update_raw(response)
            return order
            
        except Exception as e:
            logger.error(f"Error parsing order: {str(e)}")
            logger.error(traceback.format_exc())
            return None

    # --- Polling-based stream implementation ---

    def do_polling(self):
        """
        Polls Bitunix for order status updates and dispatches events.
        """
        # Pull current positions and sync with Lumibot's positions
        self.sync_positions(None)

        # Get all open orders from Bitunix and dispatch them to the stream for processing
        raw_orders = self._pull_broker_all_orders()
        stored_orders = {x.identifier: x for x in self.get_all_orders()}
        for order_row in raw_orders:
            order = self._parse_broker_order(order_row, strategy_name=self._strategy_name)
            if not order:
                continue
            # Process all orders
            if order.identifier not in stored_orders:
                if self._first_iteration:
                    if order.status == Order.OrderStatus.FILLED:
                        self._process_trade_event(order, self.FILLED_ORDER, price=order.avg_fill_price, filled_quantity=order.quantity)
                    elif order.status == Order.OrderStatus.CANCELED:
                        self._process_trade_event(order, self.CANCELED_ORDER)
                    elif order.status == Order.OrderStatus.PARTIALLY_FILLED:
                        self._process_trade_event(order, self.PARTIALLY_FILLED_ORDER, price=order.avg_fill_price, filled_quantity=order.quantity)
                    elif order.status == Order.OrderStatus.SUBMITTED:
                        self._process_trade_event(order, self.NEW_ORDER)
                    elif order.status == Order.OrderStatus.ERROR:
                        self._process_trade_event(order, self.ERROR_ORDER, error=order.error_message)
                else:
                    self._process_trade_event(order, self.NEW_ORDER)
            else:
                stored_order = stored_orders[order.identifier]
                stored_order.quantity = order.quantity
                stored_order.broker_create_date = order.broker_create_date
                stored_order.broker_update_date = order.broker_update_date
                if order.avg_fill_price:
                    stored_order.avg_fill_price = order.avg_fill_price
                # Status has changed since last time we saw it, dispatch the new status.
                if not order.equivalent_status(stored_order):
                    if order.status == Order.OrderStatus.SUBMITTED:
                        self.stream.dispatch(self.NEW_ORDER, order=stored_order)
                    elif order.status == Order.OrderStatus.PARTIALLY_FILLED:
                        self.stream.dispatch(self.PARTIALLY_FILLED_ORDER, order=stored_order, price=order.avg_fill_price, filled_quantity=order.quantity)
                    elif order.status == Order.OrderStatus.FILLED:
                        self.stream.dispatch(self.FILLED_ORDER, order=stored_order, price=order.avg_fill_price, filled_quantity=order.quantity)
                    elif order.status == Order.OrderStatus.CANCELED:
                        self.stream.dispatch(self.CANCELED_ORDER, order=stored_order)
                    elif order.status == Order.OrderStatus.ERROR:
                        msg = order_row.get("msg", f"{self.name} encountered an error with order {order.identifier} | {order}")
                        self.stream.dispatch(self.ERROR_ORDER, order=stored_order, error_msg=msg)
                else:
                    stored_order.status = order.status

        # See if there are any tracked (active) orders that are no longer in the broker's list, dispatch as cancelled
        tracked_orders = {x.identifier: x for x in self.get_tracked_orders()}
        broker_ids = [o.get("orderId") for o in raw_orders if "orderId" in o]
        for order_id, order in tracked_orders.items():
            if order_id not in broker_ids:
                logger.debug(
                    f"Poll Update: {self.name} no longer has order {order}, but Lumibot does. Dispatching as cancelled."
                )
                if order.is_active():
                    self.stream.dispatch(self.CANCELED_ORDER, order=order)

    def _get_stream_object(self):
        """Returns the polling stream object."""
        return PollingStream(self.poll_interval)

    def _register_stream_events(self):
        """Register polling event for Bitunix."""
        broker = self

        @broker.stream.add_action(PollingStream.POLL_EVENT)
        def on_trade_event_poll():
            self.do_polling()

        @broker.stream.add_action(broker.NEW_ORDER)
        def on_trade_event_new(order):
            logger.info(f"Processing action for new order {order}")
            try:
                broker._process_trade_event(order, broker.NEW_ORDER)
                return True
            except Exception:
                logger.error(traceback.format_exc())

        @broker.stream.add_action(broker.FILLED_ORDER)
        def on_trade_event_fill(order, price, filled_quantity):
            logger.info(f"Processing action for filled order {order} | {price} | {filled_quantity}")
            try:
                broker._process_trade_event(order, broker.FILLED_ORDER, price=price, filled_quantity=filled_quantity)
                return True
            except Exception:
                logger.error(traceback.format_exc())

        @broker.stream.add_action(broker.CANCELED_ORDER)
        def on_trade_event_cancel(order):
            logger.info(f"Processing action for cancelled order {order}")
            try:
                broker._process_trade_event(order, broker.CANCELED_ORDER)
            except Exception:
                logger.error(traceback.format_exc())

        @broker.stream.add_action(broker.ERROR_ORDER)
        def on_trade_event_error(order, error_msg):
            logger.error(f"Processing action for error order {order} | {error_msg}")
            try:
                if order.is_active():
                    broker._process_trade_event(order, broker.ERROR_ORDER)
                logger.error(error_msg)
                order.set_error(error_msg)
            except Exception:
                logger.error(traceback.format_exc())

    def _run_stream(self):
        self._stream_established()
        try:
            self.stream._run()
        except Exception as e:
            logger.error(f"Error running Bitunix polling stream: {e}")

    # ...existing code...

    def _modify_order(self, order: Order, price: float = None, quantity: float = None):
        """
        Modifies an existing order (if supported by BitUnix).
        """
        if not order.identifier:
            raise LumibotBrokerAPIError("Cannot modify order without order ID")
            
        try:
            # Determine symbol format based on asset type
            if order.asset.asset_type == Asset.AssetType.CRYPTO_FUTURE:
                symbol = order.asset.symbol
            else:
                logger.error(f"Cannot modify order for asset type {order.asset.asset_type}")
                return order
                
            # Prepare modification parameters
            params = {
                "orderId": order.identifier,
                "symbol": symbol
            }
            
            # Add optional modifications
            if price is not None:
                params["price"] = str(price)
            if quantity is not None:
                params["quantity"] = str(quantity)
                
            # Send modification request
            response = self.api.modify_order(**params)
            
            # Process response
            if response and response.get("code") == 0:
                # Update the order object with new values
                if price is not None:
                    order.limit_price = Decimal(str(price))
                if quantity is not None:
                    order.quantity = Decimal(str(quantity))
                    
                # Update raw data
                order.update_raw(response)
                return order
            else:
                raise LumibotBrokerAPIError(f"Failed to modify order: {response}")
        except Exception as e:
            raise LumibotBrokerAPIError(f"Error modifying order: {str(e)}")

    def _pull_position(self, strategy, asset: Asset) -> Optional[Position]:
        """
        Fetch a single position by asset.
        """
        # Reuse the multi-position pull and filter by asset
        positions = self._pull_positions(strategy)
        for pos in positions:
            if pos.asset == asset:
                return pos
        return None
    
    def get_historical_account_value(self, start_date=None, end_date=None, frequency=None) -> dict:
        """
        Not implemented: Bitunix does not support historical account value retrieval.
        """
        self.logger.error("get_historical_account_value is not implemented for Bitunix broker.")
        return {}

    def sell_all(self, strategy_name, cancel_open_orders: bool = True, strategy=None, is_multileg: bool = False):
        """Override sell_all to use flash_close_position for futures."""
        # Optional: cancel any open orders first
        if cancel_open_orders:
            super().sell_all(strategy_name, cancel_open_orders=True, strategy=strategy, is_multileg=is_multileg)
        # Fetch current positions
        positions = self._pull_positions(strategy)
        # Flash-close all futures positions
        for pos in positions:
            if pos.asset.asset_type in (Asset.AssetType.CRYPTO_FUTURE) and pos.quantity != 0:
                raw = getattr(pos, "_raw", {})
                position_id = raw.get("positionId")
                if not position_id:
                    logger.warning("No positionId for %s, skipping flash close", pos.asset.symbol)
                    continue
                side = "SELL" if pos.quantity > 0 else "BUY"
                resp = self.api.flash_close_position(position_id=position_id, side=side)
                if not resp or resp.get("code") != 0:
                    logger.warning("Failed to flash close position %s: %s", position_id, resp)
                else:
                    logger.info("Flash-closed position %s (%s)", position_id, pos.asset.symbol)

    def _parse_source_timestep(self, timestep: str) -> str:
        """
        Convert Lumibot timestep to BitUnix interval format.
        Delegates to the data source's implementation.
        """
        if hasattr(self.data_source, '_parse_source_timestep'):
            return self.data_source._parse_source_timestep(timestep)
        
        # Fallback implementation if data source doesn't have the method
        normalized = timestep.lower().strip()
        
        timestep_map = {
            "1m": "1m", "minute": "1m",
            "3m": "3m", 
            "5m": "5m",
            "15m": "15m",
            "30m": "30m", 
            "1h": "1h", "hour": "1h",
            "2h": "2h",
            "4h": "4h", 
            "1d": "1d", "day": "1d", "d": "1d"
        }
        
        return timestep_map.get(normalized, "1m")  # Default to 1m if unknown
