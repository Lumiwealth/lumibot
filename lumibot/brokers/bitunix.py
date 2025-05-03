import logging, time, traceback
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
    """A broker class that connects to the Bitunix exchange."""

    ASSET_TYPE_MAP = dict(
        stock=[],
        option=[],
        future=["future"],
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

        # Ensure _stream_loop exists before calling super, so _launch_stream doesn’t error
        self._stream_loop = None
        if isinstance(config, dict):
            api_key = config.get("API_KEY")
            api_secret = config.get("API_SECRET")
        else:
            api_key = getattr(config, "API_KEY", None)
            api_secret = getattr(config, "API_SECRET", None)
        
        # Track current leverage per symbol to avoid redundant API calls
        self.current_leverage: Dict[str, int] = {}

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
        Fetches FUTURES balances and returns a tuple of (total_cash, gross_spot_value, net_liquidation_value)
        Only the available quote asset is used for cash.
        """
        total_cash = gross_spot = net_liquidation = 0.0

        # ---------- FUTURES wallet only ----------
        # Force margin_coin to USDT as it's the most common for futures balances
        # and might avoid errors like "This futures does not allow trading" if another asset is used.
        fut_resp = self.api.get_account(margin_coin="USDT") # Use "USDT" explicitly
        # {'code': 0, 'data': {'marginCoin': 'USDT', 'available': '-187.067355065', 'frozen': '0', 'margin': '186.683', 'transfer': '0', 'positionMode': 'HEDGE', 'crossUnrealizedPNL': '-3.663', 'isolationUnrealizedPNL': '0', 'bonus': '0'}, 'msg': 'result.success'}
        try:
            data = fut_resp.get("data", {})

            # Compute equity
            total_cash     = Decimal(data.get("available", "0") or "0")
            frozen        = Decimal(data.get("frozen",    "0") or "0")
            margin        = Decimal(data.get("margin",    "0") or "0")
            cross_pnl     = Decimal(data.get("crossUnrealizedPNL", "0") or "0")
            isolation_pnl = Decimal(data.get("isolationUnrealizedPNL", "0") or "0")
            fut_equity    = total_cash + frozen + margin + cross_pnl + isolation_pnl
            net_liquidation = float(fut_equity) # Assuming balance is already in USDT
        except Exception as e:
            logger.warning("Unexpected futures account response type: %s", type(fut_resp))
            logger.warning(e)

        total_cash = float(total_cash)
        return total_cash, gross_spot, net_liquidation

    def _pull_positions(self, strategy) -> List[Position]:
        """
        Retrieves both SPOT and FUTURES positions.
        Spot positions are inferred from the main account balances.
        Futures positions are fetched from the open positions endpoint.
        """
        positions = []
        strategy_name = strategy.name if strategy else ""

        # FUTURES positions from open positions endpoint
        try:
            resp = self.api.get_positions(margin_coin=self.get_quote_asset().symbol) # Use first get_quote_asset().symbol
            if resp and resp.get("code") == 0:
                for p in resp.get("data", []):
                    sym = p.get("symbol", "")
                    # Determine quantity: use side to sign if provided, else assume positive
                    qty = Decimal(str(p.get("positionAmt", p.get("maxQty", "0"))))
                    side = p.get("side", "").upper()
                    if side == "SHORT" or qty < 0:
                        qty = -abs(qty)
                    else:
                        qty = abs(qty)
                    entry = Decimal(str(p.get("entryPrice", "0")))
                    if qty != 0 and sym:
                        asset = Asset(sym, Asset.AssetType.FUTURE)
                        pos = Position(strategy_name, asset, qty)
                        pos.average_entry_price = entry
                        positions.append(pos)
        except Exception as e:
            logger.warning("Error fetching futures positions: %s", e)
            logger.warning(traceback.format_exc())

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

    # --- Order conformance logic ---
    def _conform_order(self, order):
        """
        Conform an order to Bitunix requirements (e.g., rounding, min/max checks).
        """
        # Example: round price to 2 decimals for futures, 6 for spot
        if order.limit_price is not None:
            if order.asset.asset_type == Asset.AssetType.FUTURE:
                order.limit_price = round(float(order.limit_price), 2)
            elif order.asset.asset_type == Asset.AssetType.CRYPTO:
                order.limit_price = round(float(order.limit_price), 6)
        if order.quantity is not None:
            order.quantity = round(float(order.quantity), 6)
        # Add more checks as needed (min qty, etc.)

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
        Handles both SPOT and FUTURES orders.
        """
        if order.asset.asset_type not in (Asset.AssetType.CRYPTO, Asset.AssetType.FUTURE):
            error_msg = f"Asset type {order.asset.asset_type} not supported by BitUnix"
            order.set_error(LumibotBrokerAPIError(error_msg))
            return order

        # Determine symbol format based on asset type
        if order.asset.asset_type == Asset.AssetType.FUTURE:
            symbol = order.asset.symbol
            quote_symbol = self.get_quote_asset().symbol
            # Ensure symbol ends with quote_symbol (e.g., BTCUSDT)
            if not symbol.endswith(quote_symbol):
                symbol = f"{symbol}{quote_symbol}"
            if symbol == quote_symbol:
                error_msg = f"Invalid symbol: symbol cannot be the same as quote asset ({quote_symbol})"
                order.set_error(LumibotBrokerAPIError(error_msg))
                return order
        else:
            # Spot trading pair
            symbol = f"{order.asset.symbol}{order.quote.symbol}" if order.quote else order.asset.symbol

        # Prepare quantity and price
        quantity = abs(float(order.quantity))
        price = float(order.limit_price) if order.limit_price else None
        
        # Generate a client order ID for tracking
        client_order_id = f"lmbot_{int(time.time() * 1000)}_{hash(str(order)) % 10000}"

        # Conform the order to Bitunix requirements
        self._conform_order(order)

        try:
            if order.asset.asset_type == Asset.AssetType.FUTURE:
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
                }
                if price is not None:
                    params["price"] = price
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
                    # Attach full response for debugging
                    order.update_raw(response)
                    self.stream.dispatch(self.ERROR_ORDER, order=order, error_msg=err_msg)
                    return order
                # Success handling only
                data = response.get("data", {})
                order_id = data.get("orderId")
                if order_id:
                    order.identifier = order_id
                    order.status = Order.OrderStatus.SUBMITTED
                    order.update_raw(response)
                    self._unprocessed_orders.append(order)
                    self._process_trade_event(order, self.NEW_ORDER)
                else:
                    error_msg = f"No order ID in response: {response}"
                    order.set_error(LumibotBrokerAPIError(error_msg))
                    self.stream.dispatch(self.ERROR_ORDER, order=order, error_msg=error_msg)
            else:
                # SPOT order
                side_code = 2 if order.side == Order.OrderSide.BUY else 1   # 2 = buy, 1 = sell
                type_code = 1 if order.order_type == Order.OrderType.LIMIT else 2  # 1 = limit, 2 = market
                params = {
                    "symbol": symbol,
                    "side": side_code,
                    "type": type_code,
                    "volume": quantity,
                }
                if price is not None:
                    params["price"] = price

                response = self.api.place_spot_order(**params)
                # Process response
                if response and response.get("code") == 0:
                    data = response.get("data", {})
                    order_id = data.get("orderId")
                    if order_id:
                        order.identifier = order_id
                        order.status = Order.OrderStatus.SUBMITTED
                        order.update_raw(response)
                        self._unprocessed_orders.append(order)
                        self._process_trade_event(order, self.NEW_ORDER)
                    else:
                        error_msg = f"No order ID in response: {response}"
                        order.set_error(LumibotBrokerAPIError(error_msg))
                        self.stream.dispatch(self.ERROR_ORDER, order=order, error_msg=error_msg)
                else:
                    error_msg = f"Error placing order: {response}"
                    order.set_error(LumibotBrokerAPIError(error_msg))
                    # Attach full response for debugging
                    order.update_raw(response)
                    self.stream.dispatch(self.ERROR_ORDER, order=order, error_msg=error_msg)
        except Exception as e:
            error_msg = f"Exception placing order: {str(e)}"
            order.set_error(e)
            self.stream.dispatch(self.ERROR_ORDER, order=order, error_msg=error_msg)
        return order

    def cancel_order(self, order: Order):
        """
        Cancels a SPOT or FUTURES order.
        """
        if not order.identifier or order.is_final():
            return
            
        try:
            # Determine symbol format based on asset type
            if order.asset.asset_type == Asset.AssetType.FUTURE:
                response = self.api.cancel_order(order_id=order.identifier)
            else:
                symbol = f"{order.asset.symbol}{order.quote.symbol}" if order.quote else order.asset.symbol
                response = self.api.cancel_spot_order(
                    order_id=order.identifier,
                    symbol=symbol
                )
            # Check response
            if response and response.get("code") == 0:
                # Use self._process_trade_event for consistency
                self._process_trade_event(order, self.CANCELED_ORDER)
            else:
                # Log error but don't raise, let polling handle final state
                logger.error(f"Failed to cancel order {order.identifier}: {response}")
                # Optionally dispatch an error event if immediate feedback is needed
                # self._process_trade_event(order, self.ERROR_ORDER, error=LumibotBrokerAPIError(f"Failed to cancel order: {response}"))
        except Exception as e:
             # Log error but don't raise, let polling handle final state
            logger.error(f"Error canceling order {order.identifier}: {str(e)}")
            # Optionally dispatch an error event
            # self._process_trade_event(order, self.ERROR_ORDER, error=LumibotBrokerAPIError(f"Error canceling order: {str(e)}"))
            pass # Don't raise, rely on polling

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

        # Fetch SPOT open orders
        try:
            spot_open_resp = self.api.get_pending_orders(symbol=symbol)
            if spot_open_resp and spot_open_resp.get("code") == 0:
                data = spot_open_resp.get("data") or {}
                spot_orders = data.get("orderList", [])

                if len(spot_orders) > 0:
                    all_orders.extend(spot_orders)
        except Exception:
            logger.warning("Error fetching spot open orders")
        
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
        # Ensure broker_status is a string before uppercasing
        status_str = str(broker_status).upper()

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
            order_type = response.get("type", "")
            
            # Extract quantities and prices
            # fields use 'qty'/'tradeQty' for BitUnix
            qty_original = Decimal(str(response.get("qty",     "0")))
            qty_executed = Decimal(str(response.get("tradeQty","0")))
            # parse limit price only if numeric
            price_limit = None
            ps = response.get("price")
            if ps and isinstance(ps, str) and ps.upper() != "MARKET":
                price_limit = Decimal(ps)
            # parse average price
            price_avg = None
            ap = response.get("avgPrice")
            if ap:
                price_avg = Decimal(str(ap))
                        
            # Determine if this is a futures or spot order based on symbol format
            is_future = False
            if symbol.endswith("PERP") or "-" in symbol:
                is_future = True
                asset = Asset(symbol, Asset.AssetType.FUTURE)
                quote = None
            else:
                # For spot, extract base and quote from symbol
                # Assuming USDT is the quote currency - adjust if needed
                base_symbol = symbol
                quote_symbol = "USDT"
                
                # Try to extract base/quote if a common quote currency is found
                for common_quote in ["USDT", "BTC", "BUSD", "ETH"]:
                    if symbol.endswith(common_quote):
                        base_symbol = symbol[:-len(common_quote)]
                        quote_symbol = common_quote
                        break
                
                asset = Asset(base_symbol, Asset.AssetType.CRYPTO)
                quote = Asset(quote_symbol, Asset.AssetType.CRYPTO)
            
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
            if order.asset.asset_type == Asset.AssetType.FUTURE:
                symbol = order.asset.symbol
            else:
                symbol = f"{order.asset.symbol}{order.quote.symbol}" if order.quote else order.asset.symbol
                
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
        all_positions = self._pull_positions(strategy)
        for pos in all_positions:
            if pos.asset == asset:
                return pos
        return None
    
    def get_historical_account_value(self, start_date=None, end_date=None, frequency=None) -> dict:
        """
        Not implemented: Bitunix does not support historical account value retrieval.
        """
        self.logger.error("get_historical_account_value is not implemented for Bitunix broker.")
        return {}
    