import logging, time, traceback
import decimal
from decimal import Decimal
from typing import Optional, List, Dict, Tuple, Any
import pandas as pd
import asyncio
import websockets
import hashlib
import json
import os
import requests

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
    DEFAULT_MARGIN_COIN = "USDT" # Default margin coin for futures
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
            # Get margin coin from config, default to USDT
            self.margin_coin = config.get("MARGIN_COIN", self.DEFAULT_MARGIN_COIN)
        else:
            api_key = getattr(config, "API_KEY", None)
            api_secret = getattr(config, "API_SECRET", None)
            self.margin_coin = getattr(config, "MARGIN_COIN", self.DEFAULT_MARGIN_COIN)

        if not api_key or not api_secret:
            raise ValueError("API_KEY and API_SECRET must be provided in config")

        # Initialize API client and WS attributes BEFORE calling super().__init__
        self.api = BitUnixClient(api_key=api_key, secret_key=api_secret)
        self.api_secret = api_secret  # needed for signing
        # Private-channel URL per BitUnix docs (kept for reference, but not used for polling)
        self.ws_url = "wss://fapi.bitunix.com/private/"

        if not data_source:
            data_source = BitunixData(config, max_workers=max_workers, chunk_size=chunk_size)
            # Share the client instance with the data source if it was just created
            data_source.client = self.api

        self.poll_interval = poll_interval or self.DEFAULT_POLL_INTERVAL
        super().__init__(
            name="bitunix",
            connect_stream=connect_stream, # Use connect_stream to enable _run_stream thread
            data_source=data_source,
            config=config,
            max_workers=max_workers,
        )
        # Removed asyncio task creation: self._poll_task = asyncio.get_event_loop().create_task(self._poll_loop())

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
        Only the available margin coin is used for cash.
        """
        total_cash = gross_spot = net_liquidation = 0.0
        futures_margin_coin_symbol = self.margin_coin
        primary_quote_asset = self.LUMIBOT_DEFAULT_QUOTE_ASSET

        # ---------- FUTURES wallet only ----------
        fut_resp = self.api.get_account(margin_coin=futures_margin_coin_symbol)
        if isinstance(fut_resp, dict):
            data = fut_resp.get("data", {})

            # Compute equity
            total_cash     = Decimal(data.get("available", "0") or "0")
            frozen        = Decimal(data.get("frozen",    "0") or "0")
            margin        = Decimal(data.get("margin",    "0") or "0")
            cross_pnl     = Decimal(data.get("crossUnrealizedPNL", "0") or "0")
            isolation_pnl = Decimal(data.get("isolationUnrealizedPNL", "0") or "0")
            fut_equity    = total_cash + frozen + margin + cross_pnl + isolation_pnl
            if futures_margin_coin_symbol == primary_quote_asset.symbol:
                net_liquidation = float(fut_equity)
            else:
                conv_price = self.data_source.get_last_price(
                    Asset(futures_margin_coin_symbol, Asset.AssetType.CRYPTO),
                    primary_quote_asset
                )
                if conv_price:
                    net_liquidation = float(fut_equity * conv_price)
        else:
            logger.warning("Unexpected futures account response type: %s", type(fut_resp))

        # Spot wallet is not used in futures-only mode

        return (total_cash, gross_spot, net_liquidation)

    def _pull_positions(self, strategy) -> List[Position]:
        """
        Retrieves both SPOT and FUTURES positions.
        Spot positions are inferred from the main account balances.
        Futures positions are fetched from the open positions endpoint.
        """
        positions = []
        strategy_name = strategy.name if strategy else ""

        # SPOT positions from balances (inferred from main account endpoint)
        try:
            # Use the main account endpoint
            resp = self.api.get_account(margin_coin=self.LUMIBOT_DEFAULT_QUOTE_ASSET.symbol) # Use default quote
            if resp and resp.get("code") == 0:
                # Check if 'balances' key exists for spot-like assets
                balances = resp.get("data", {}).get("balances", [])
                if balances:
                    for balance in balances:
                        asset_symbol = balance.get("asset", "")
                        free_amount = Decimal(balance.get("free", "0"))
                        locked_amount = Decimal(balance.get("locked", "0"))
                        total_amount = free_amount + locked_amount

                        # Only create positions for non-zero, non-quote assets
                        if total_amount > 0 and asset_symbol != self.LUMIBOT_DEFAULT_QUOTE_ASSET.symbol:
                            # Assume these are spot crypto assets
                            asset = Asset(asset_symbol, Asset.AssetType.CRYPTO)
                            position = Position(strategy_name, asset, total_amount)
                            positions.append(position)
                else:
                    #logger.info("No 'balances' key in account response, cannot infer spot positions.")
                    pass
        except Exception:
            logger.error("Error fetching spot positions from account balance")
            logger.error(traceback.format_exc())

        # FUTURES positions from open positions endpoint
        try:
            resp = self.api.get_positions(margin_coin=self.margin_coin)
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
            # Use configured margin coin
            margin_coin = self.margin_coin 
        else:
            # Spot trading pair
            symbol = f"{order.asset.symbol}{order.quote.symbol}" if order.quote else order.asset.symbol

        # Prepare quantity and price
        quantity = abs(float(order.quantity))
        price = float(order.limit_price) if order.limit_price else None
        
        # Generate a client order ID for tracking
        client_order_id = f"lmbot_{int(time.time() * 1000)}_{hash(str(order)) % 10000}"

        try:
            if order.asset.asset_type == Asset.AssetType.FUTURE:
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

    def _get_ws_url(self) -> str:
        """Return the WebSocket URL for BitUnix streams."""
        return self.ws_url

    async def _authenticate_ws(self, ws: websockets.WebSocketClientProtocol):
        """Authenticate on the private WebSocket channel using API key and secret."""
        timestamp = str(int(time.time()))
        nonce = str(int(time.time() * 1_000_000))
        digest = hashlib.sha256((nonce + timestamp + self.api.api_key).encode()).hexdigest()
        sign = hashlib.sha256((digest + self.api_secret).encode()).hexdigest()
        await ws.send(json.dumps({
            "op": "login",
            "args": [{
                "apiKey": self.api.api_key,
                "timestamp": timestamp,
                "nonce": nonce,
                "sign": sign
            }]
        }))
        # Loop until login response arrives
        while True:
            raw = await ws.recv()
            msg = json.loads(raw)
            if msg.get("op") == "connect":
                continue
            if msg.get("op") == "login":
                if msg.get("code") != 0:
                    raise Exception(f"WebSocket auth failed: {msg}")
                break

    # Subscription logic is now handled in _run_stream per new spec

    def _get_stream_object(self):
        """Returns the stream object for polling."""
        return PollingStream(self)

    def _register_stream_events(self):
        """No-op: WS messages are handled directly in _handle_stream_message."""
        pass

    async def _handle_stream_message(self, message: str):
        """Parse incoming WS messages and dispatch Lumibot events."""
        try:
            msg = json.loads(message)
            # Auto‑reply to server ping
            if msg.get("op") == "ping" and "ping" in msg:
                await self._ws.send(json.dumps({"op": "ping", "pong": msg["ping"], "ping": int(time.time())}))
                return
            channel = msg.get('ch') or msg.get('method')
            data = msg.get('data', {})
            
            # Process order updates
            if channel and 'order' in channel:
                # Try to parse order update
                order_update = self._parse_broker_order(data, self._strategy_name)
                if not order_update:
                    return
                    
                # Determine appropriate event based on order status
                status = order_update.status
                if status == Order.OrderStatus.SUBMITTED:
                    event = self.NEW_ORDER
                elif status == Order.OrderStatus.PARTIALLY_FILLED:
                    event = self.PARTIALLY_FILLED_ORDER
                elif status == Order.OrderStatus.FILLED:
                    event = self.FILLED_ORDER
                elif status == Order.OrderStatus.CANCELED:
                    event = self.CANCELED_ORDER
                elif status == Order.OrderStatus.ERROR:
                    event = self.ERROR_ORDER
                else:
                    # Unrecognized status, skip processing
                    return
                    
                # Calculate filled data for trade events
                price = float(order_update.avg_fill_price) if order_update.avg_fill_price else None
                qty = float(order_update.filled_quantity) if order_update.filled_quantity else 0
                
                # Dispatch event via broker's trade event processor
                self._process_trade_event(order_update, event, price=price, filled_quantity=qty)
        except Exception as e:
            logger.error(f"Error processing WebSocket message: {str(e)}")
            logger.error(traceback.format_exc())

    def _run_stream(self):
        """
        Run the polling loop in the stream thread.
        Polls BitUnix REST endpoints for order and position updates.
        """
        # Signal that the "stream" (polling loop) is established
        self._stream_established()
        logger.info(f"Bitunix polling loop started with interval: {self.poll_interval}s")

        while True:
            try:
                # 1) Poll open orders and dispatch new/cancel/filled events
                # Fetch all potentially relevant orders (open and recently closed might be needed)
                # Using get_history_orders might be more comprehensive than get_pending_orders
                # Adjust status filter as needed, e.g., fetch NEW, PARTIALLY_FILLED
                all_orders_raw = self._pull_broker_all_orders(status=None) # Fetch all states initially
                
                processed_ids = set() # Keep track of orders processed in this cycle

                for od in all_orders_raw:
                    order_obj = self._parse_broker_order(od, self._strategy_name)
                    if order_obj:
                        processed_ids.add(order_obj.identifier)
                        # Check against tracked orders to detect changes
                        tracked_order = self.get_tracked_order(order_obj.identifier)

                        # If untracked or status changed, process the update
                        if tracked_order is None or tracked_order.status != order_obj.status:
                            status = order_obj.status
                            event = None
                            if status == Order.OrderStatus.SUBMITTED:
                                event = self.NEW_ORDER
                            elif status == Order.OrderStatus.PARTIALLY_FILLED:
                                event = self.PARTIALLY_FILLED_ORDER
                            elif status == Order.OrderStatus.FILLED:
                                event = self.FILLED_ORDER
                            elif status == Order.OrderStatus.CANCELED:
                                event = self.CANCELED_ORDER
                            elif status == Order.OrderStatus.ERROR:
                                event = self.ERROR_ORDER
                            
                            if event:
                                # Calculate filled data for trade events
                                price = float(order_obj.avg_fill_price) if order_obj.avg_fill_price else None
                                qty = float(order_obj.filled_quantity) if order_obj.filled_quantity else 0
                                self._process_trade_event(order_obj, event, price=price, filled_quantity=qty)
                        elif tracked_order: # Ensure tracked_order is not None
                             # Order exists and status hasn't changed, update raw data if needed
                             tracked_order.update_raw(od) # Use the raw dict 'od' directly

                # Check for orders that were tracked but disappeared (e.g., fully filled/canceled between polls)
                # This part might be complex and depends on how reliably _pull_broker_all_orders works
                # For simplicity, we rely on the polled data containing final states.

                # 2) Poll positions to update any position-based logic if needed
                # This might be less critical if position updates are derived from fills
                # positions = self._pull_positions(getattr(self, "_strategy", None))
                # for pos in positions:
                #     # Dispatching position updates frequently might be noisy
                #     # Consider dispatching only on significant changes if needed
                #     # self.stream.dispatch(self.UPDATED_POSITION, position=pos)
                #     pass # Position polling might be optional depending on strategy needs

            except requests.exceptions.RequestException as e:
                 logger.warning(f"Bitunix polling network error: {e}")
            except Exception as e:
                logger.error("Bitunix polling error: %s", e)
                logger.error(traceback.format_exc())

            # Wait for the next polling interval
            time.sleep(self.poll_interval)

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

    def _pull_position(self, strategy) -> Optional[Position]:
        """Single-position fetch not directly supported, use _pull_positions instead."""
        return None

    def get_historical_account_value(self, start_date=None, end_date=None, frequency=None) -> list:
        """Historical account value fetching not currently supported by BitUnix."""
        return []