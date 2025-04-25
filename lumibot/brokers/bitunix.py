import logging, time, traceback
from decimal import Decimal
from typing import Optional, List, Dict, Tuple, Any
import pandas as pd
import asyncio
import websockets
import hashlib
import json
import os
import requests

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

    def __init__(self, config, max_workers: int = 1, chunk_size: int = 100, connect_stream: bool = False, data_source=None):
        if isinstance(config, dict):
            api_key = config.get("API_KEY")
            api_secret = config.get("API_SECRET")
        else:
            api_key = getattr(config, "API_KEY", None)
            api_secret = getattr(config, "API_SECRET", None)
        if not api_key or not api_secret:
            raise ValueError("API_KEY and API_SECRET must be provided in config")
        if not data_source:
            data_source = BitunixData(config, max_workers=max_workers, chunk_size=chunk_size)
        super().__init__(
            name="bitunix",
            connect_stream=connect_stream,
            data_source=data_source,
            config=config,
            max_workers=max_workers,
        )
        # WebSocket streaming configuration
        self.api = BitUnixClient(api_key=api_key, secret_key=api_secret)
        self.api_secret = api_secret  # needed for signing
        self.ws_url = "wss://openapi.bitunix.com:443/ws-api/v1"
        self._stream_loop = None

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
        Fetches SPOT balances and FUTURES balances.
        Returns tuple of (total_cash, gross_spot_value, net_liquidation_value)
        """
        total_cash = gross_spot = net_liquidation = 0.0
        quote_symbol = quote_asset.symbol

        # SPOT balances
        try:
            # Using the account balance endpoint for spot assets
            resp = self.api.get_account()
            self.logger.info(resp)
            # Check if the response is valid
            if resp and resp.get("code") == 0:
                balances = resp.get("data", {}).get("balances", [])
                for balance in balances:
                    asset_symbol = balance.get("asset", "")
                    free_amount = Decimal(balance.get("free", "0"))
                    locked_amount = Decimal(balance.get("locked", "0"))
                    total_amount = free_amount + locked_amount
                    
                    if total_amount > 0:
                        if asset_symbol == quote_symbol:
                            # This is the quote currency, add to cash
                            total_cash += float(total_amount)
                            net_liquidation += float(total_amount)
                        else:
                            # For other assets, get market value in quote currency
                            try:
                                price = self.data_source.get_last_price(
                                    Asset(asset_symbol, Asset.AssetType.CRYPTO),
                                    quote_asset
                                )
                                if price:
                                    value = float(total_amount * price)
                                    gross_spot += value
                                    net_liquidation += value
                            except Exception:
                                logger.warning(f"Could not get price for {asset_symbol}")
        except Exception:
            logger.error("Error fetching spot balances")
            logger.error(traceback.format_exc())

        # FUTURES balances and positions
        try:
            fut_resp = self.api.get_account(margin_coin=quote_symbol)
            if fut_resp and fut_resp.get("code") == 0:
                data = fut_resp.get("data") or {}
                margin_balance = Decimal(data.get("marginBalance", "0"))
                unrealized_pnl  = Decimal(data.get("unrealizedPnl",    "0"))
                net_liquidation += float(margin_balance + unrealized_pnl)
        except Exception:
            logger.warning("Error fetching futures balances, skipping")
            logger.warning(traceback.format_exc())
        
        return (total_cash, gross_spot, net_liquidation)

    def _pull_positions(self, strategy) -> List[Position]:
        """
        Retrieves both SPOT and FUTURES positions.
        """
        positions=[]
        strategy_name = strategy.name if strategy else ""

        # SPOT positions from balances
        try:
            resp = self.api.get_account()
            if resp and resp.get("code") == 0:
                balances = resp.get("data", {}).get("balances", [])
                
                for balance in balances:
                    asset_symbol = balance.get("asset", "")
                    free_amount = Decimal(balance.get("free", "0"))
                    locked_amount = Decimal(balance.get("locked", "0"))
                    total_amount = free_amount + locked_amount
                    
                    # Only create positions for non-zero, non-quote assets
                    if total_amount > 0 and asset_symbol != self.LUMIBOT_DEFAULT_QUOTE_ASSET.symbol:
                        asset = Asset(asset_symbol, Asset.AssetType.CRYPTO)
                        position = Position(strategy_name, asset, total_amount)
                        positions.append(position)
        except Exception:
            logger.error("Error fetching spot positions")
            logger.error(traceback.format_exc())

        # FUTURES positions (use history endpoint)
        try:
            resp = self.api.get_history_positions(symbol=None, skip=0, limit=100)
            if resp and resp.get("code") == 0:
                quote_sym = self.LUMIBOT_DEFAULT_QUOTE_ASSET.symbol
                for p in resp.get("data", {}).get("positionList", []):
                    sym = p.get("symbol","")
                    # skip the margin/quote currency
                    if sym == quote_sym:
                        continue
                    qty = Decimal(p.get("maxQty","0"))
                    if p.get("side","").upper()=="SHORT":
                        qty = -qty
                    entry = Decimal(p.get("entryPrice","0"))
                    if qty != 0 and sym:
                        asset = Asset(sym, Asset.AssetType.FUTURE)
                        pos = Position(strategy_name, asset, qty)
                        pos.average_entry_price = entry
                        # pos.update_raw(p)  # Position has no update_raw()
                        positions.append(pos)
        except Exception:
            logger.warning("Error fetching futures positions, skipping")
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
            margin_coin = order.quote.symbol if order.quote else "USDT"
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
                    "quantity": quantity,
                    "marginCoin": margin_coin,
                    "clientOrderId": client_order_id,
                }
                
                # Add optional parameters
                if price is not None:
                    params["price"] = price
                
                # Add leverage if specified
                if hasattr(order, "leverage") and order.leverage:
                    params["leverage"] = order.leverage
                
                # Determine if this is opening or closing a position
                position_side = "OPEN"
                if hasattr(order, "reduce_only") and order.reduce_only:
                    position_side = "CLOSE"
                params["tradeSide"] = position_side
                
                # Submit order
                response = self.api.place_order(**params)
            else:
                # SPOT order
                params = {
                    "symbol": symbol,
                    "side": self._map_side_to_bitunix(order.side),
                    "type": self._map_type_to_bitunix(order.order_type),
                    "quantity": quantity,
                    "clientOrderId": client_order_id,
                }
                
                # Add price for limit orders
                if price is not None:
                    params["price"] = price
                
                # Submit order
                response = self.api.place_order(**params)
            
            # Process response
            if response and response.get("code") == 0:
                data = response.get("data", {})
                order_id = data.get("orderId")
                
                if order_id:
                    order.identifier = order_id
                    order.status = Order.OrderStatus.SUBMITTED
                    order.update_raw(response)
                    self._unprocessed_orders.append(order)
                    self.stream.dispatch(self.NEW_ORDER, order=order)
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
                symbol = order.asset.symbol
                margin_coin = order.quote.symbol if order.quote else "USDT"
                
                # Cancel futures order
                response = self.api.cancel_order(
                    order_id=order.identifier,
                    symbol=symbol,
                    margin_coin=margin_coin
                )
            else:
                # Spot order
                symbol = f"{order.asset.symbol}{order.quote.symbol}" if order.quote else order.asset.symbol
                
                # Cancel spot order
                response = self.api.cancel_order(
                    order_id=order.identifier,
                    symbol=symbol
                )
                
            # Check response
            if response and response.get("code") == 0:
                order.status = Order.OrderStatus.CANCELED
                self.stream.dispatch(self.CANCELED_ORDER, order=order)
            else:
                raise LumibotBrokerAPIError(f"Failed to cancel order: {response}")
                
        except Exception as e:
            raise LumibotBrokerAPIError(f"Error canceling order: {str(e)}")

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
        
        # If we need historical orders (not just open orders)
        if status != "OPEN":
            try:
                hist_resp = self.api.get_history_orders(symbol=symbol, status=status)
                if hist_resp and hist_resp.get("code") == 0:
                    data = hist_resp.get("data") or {}
                    # extract list, handle both dict-with-orderList or direct list
                    hist_list = data.get("orderList") if isinstance(data, dict) else data
                    all_orders.extend(hist_list or [])
            except Exception:
                logger.warning("Error fetching historical orders")
            
        return all_orders

    def _map_status_from_bitunix(self, broker_status) -> Order.OrderStatus:
        """Maps BitUnix order status to Lumibot OrderStatus."""
        status_map = {
            "NEW":               Order.OrderStatus.SUBMITTED,
            "PARTIALLY_FILLED":  Order.OrderStatus.PARTIALLY_FILLED,
            "FILLED":            Order.OrderStatus.FILLED,
            "CANCELED":          Order.OrderStatus.CANCELED,
            "REJECTED":          Order.OrderStatus.ERROR,
            "EXPIRED":           Order.OrderStatus.CANCELED,
            "PENDING_CANCEL":    Order.OrderStatus.CANCELED,    # mapped to CANCELED since PENDING_CANCEL isn't defined
        }
        return status_map.get(str(broker_status).upper())

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
        timestamp = str(int(time.time() * 1000))
        nonce = str(int(time.time() * 1000000))
        
        # Create signature according to BitUnix WebSocket API docs
        digest = hashlib.sha256((nonce + timestamp + self.api.api_key).encode()).hexdigest()
        sign = hashlib.sha256((digest + self.api_secret).encode()).hexdigest()
        
        auth_payload = {
            "op": "login", 
            "args": [{
                "apiKey": self.api.api_key,
                "timestamp": timestamp,
                "nonce": nonce,
                "sign": sign
            }]
        }
        await ws.send(json.dumps(auth_payload))
        
        # Wait for auth response
        response = await ws.recv()
        auth_resp = json.loads(response)
        
        if auth_resp.get("code") != 0:
            logger.error(f"WebSocket authentication failed: {auth_resp}")
            raise Exception(f"WebSocket authentication failed: {auth_resp}")

    async def _subscribe_channels(self, ws: websockets.WebSocketClientProtocol):
        """Subscribe to necessary public and private channels."""
        # Get tracked symbols from data source
        symbols = getattr(self.data_source, "client_symbols", set())
        
        # Create subscription arguments
        args = []
        for symbol in symbols:
            # Market data channels
            args.extend([
                {"ch": "market.kline.1m", "symbol": symbol},
                {"ch": "market.depth", "symbol": symbol},
                {"ch": "market.ticker", "symbol": symbol}
            ])
        
        # Add private channels for all symbols
        args.extend([
            {"ch": "user.order"},
            {"ch": "user.trade"},
            {"ch": "user.position"}
        ])
        
        # Subscribe
        subscribe_payload = {"op": "subscribe", "args": args}
        await ws.send(json.dumps(subscribe_payload))
        
        # Wait for subscription response
        response = await ws.recv()
        sub_resp = json.loads(response)
        
        if sub_resp.get("code") != 0:
            logger.warning(f"Some WebSocket subscriptions may have failed: {sub_resp}")

    def _get_stream_object(self):
        """Use the broker itself as the stream handler."""
        return self

    def _register_stream_events(self):
        """No-op: WS messages are handled directly in _handle_stream_message."""
        pass

    async def _handle_stream_message(self, message: str):
        """Parse incoming WS messages and dispatch Lumibot events."""
        try:
            msg = json.loads(message)
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
        """Start the WebSocket loop and feed messages into the handler."""
        if self._stream_loop and not self._stream_loop.is_closed():
            return
        self._stream_loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self._stream_loop)

        async def _keep_running():
            backoff = 1
            while True:
                try:
                    async with websockets.connect(self._get_ws_url()) as ws:
                        # Authenticate
                        await self._authenticate_ws(ws)
                        # Subscribe to channels
                        await self._subscribe_channels(ws)
                        # Mark stream as established
                        self._stream_established()
                        logger.info("BitUnix WebSocket connected and subscribed")
                        
                        # Process messages
                        async for message in ws:
                            await self._handle_stream_message(message)
                except Exception as e:
                    logger.warning(f"BitUnix WS disconnected: {str(e)}, reconnecting in {backoff} seconds")
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, 60)  # Exponential backoff with 60-second max

        self._stream_loop.run_until_complete(_keep_running())

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