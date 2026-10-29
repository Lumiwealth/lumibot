"""MetaMask Agent Wallet broker backed by the optional local Node service."""

from decimal import Decimal
from threading import RLock
from uuid import uuid4

import requests

from lumibot.brokers.broker import Broker
from lumibot.data_sources.metamask_data import MetaMaskData
from lumibot.entities import Asset, Order, Position
from lumibot.tools.metamask_client import MetaMaskAPIError, MetaMaskClient, decimal_value, token_payload
from lumibot.trading_builtins.custom_stream import PollingStream


class MetaMask(Broker):
    """One broker instance binds to one dedicated agent wallet and one market type.

    ``MODE='perp'`` routes Hyperliquid orders. ``MODE='spot'`` exposes exact-input
    swaps via :meth:`swap`; token buys cannot promise an exact output quantity.
    Owner credentials and browser wallet access remain outside the strategy.
    """

    IS_BACKTESTING_BROKER = False

    def __init__(self, config=None, data_source=None, connect_stream=True, **kwargs):
        config = dict(config or {})
        self.mode = config.get("MODE", "perp")
        if self.mode not in ("perp", "spot"):
            raise ValueError("MetaMask MODE must be 'perp' or 'spot'")
        self.client = MetaMaskClient(config)
        self._observations = {}
        self._event_lock = RLock()
        self._poll_interval = float(config.get("POLL_INTERVAL", 5))
        data_source = data_source or MetaMaskData(config, client=self.client)
        super().__init__(
            name="metamask", config=config, data_source=data_source, connect_stream=connect_stream, **kwargs
        )
        self.market = "24/7"

    def _submit_order(self, order):
        if self.mode != "perp" or order.asset.asset_type != "crypto_future":
            raise ValueError("Use swap(input_asset, output_asset, input_amount) for exact-input spot swaps")
        if order.order_type not in (Order.OrderType.MARKET, Order.OrderType.LIMIT):
            raise ValueError("MetaMask supports market and limit orders; conditional orders are not emulated")
        if order.order_class not in (None, "", Order.OrderClass.SIMPLE):
            raise ValueError("MetaMask does not support bracket or multi-leg orders")
        if order.quote is not None and order.quote.symbol != "USDC":
            raise ValueError("Hyperliquid perpetuals settle in USDC")
        if order.asset.multiplier != 1:
            raise ValueError("Hyperliquid quantities are base units with multiplier=1")
        if order.side not in (Order.OrderSide.BUY, Order.OrderSide.SELL):
            raise ValueError("Use buy/sell with custom_params={'reduce_only': True} to reduce a position")
        params = order.custom_params or {}
        extra = set(params) - {"reduce_only", "leverage"}
        if extra:
            raise ValueError(f"Unsupported MetaMask order parameters: {sorted(extra)}")
        tif = order.time_in_force or "gtc"
        if order.order_type == Order.OrderType.MARKET and tif == "day":
            tif = "ioc"
        if tif not in ("gtc", "ioc", "alo"):
            raise ValueError("MetaMask time_in_force must be gtc, ioc, or alo")
        request = {
            "kind": "perp",
            "id": str(order.identifier),
            "symbol": order.asset.symbol,
            "side": str(order.side),
            "quantity": format(decimal_value(order.quantity), "f"),
            "orderType": str(order.order_type),
            "leverage": params.get("leverage", order.asset.leverage),
            "reduceOnly": params.get("reduce_only", False),
            "tif": tif,
        }
        if order.order_type == Order.OrderType.LIMIT:
            request["limitPrice"] = format(decimal_value(order.limit_price), "f")
        self.client.verify()
        try:
            response = self.client.request("POST", "/v1/orders", payload=request)
        except (requests.Timeout, requests.ConnectionError):
            response = {
                "id": str(order.identifier),
                "request": request,
                "state": "unknown",
                "filledQuantity": "0",
                "message": "Submission outcome unknown; poll the same ID. Do not create a replacement order.",
            }
        with self._event_lock:
            if str(order.identifier) not in self._observations and self.get_tracked_order(order.identifier) is None:
                self._unprocessed_orders.append(order)
            self._observe(order, response)
        return order

    def _submit_orders(self, orders, **kwargs):
        if kwargs.pop("is_multileg", False) or kwargs:
            raise NotImplementedError("MetaMask batch submission accepts independent orders only")
        # A wallet has one signing operation in flight at a time.
        return [self.submit_order(order) for order in orders]

    def swap(self, input_asset, output_asset, input_amount, *, slippage_bps=50, identifier=None):
        """Submit a same-chain exact-input swap; return its durable operation.

        This does not create an exact-output LumiBot buy order. Poll using
        ``get_swap(identifier)``. Reuse the ID after transport uncertainty.
        """
        if self.mode != "spot":
            raise ValueError("Set MODE='spot' for onchain swaps")
        request = {
            "kind": "swap",
            "id": identifier or uuid4().hex,
            "input": token_payload(input_asset),
            "output": token_payload(output_asset),
            "inputAmount": format(decimal_value(input_amount), "f"),
            "slippageBps": slippage_bps,
        }
        try:
            return self.client.call("POST", "/v1/orders", payload=request)
        except (requests.Timeout, requests.ConnectionError):
            return {
                "id": request["id"],
                "request": request,
                "state": "unknown",
                "message": "Poll this ID; submission may have reached MetaMask",
            }

    def get_swap(self, identifier):
        return next(
            (
                op
                for op in self.client.call("GET", "/v1/orders")
                if op["id"] == identifier and op["request"]["kind"] == "swap"
            ),
            None,
        )

    def _pull_broker_all_orders(self):
        return [op for op in self.client.call("GET", "/v1/orders") if op["request"]["kind"] == "perp"]

    def _pull_broker_order(self, identifier):
        return next((op for op in self._pull_broker_all_orders() if op["id"] == str(identifier)), None)

    def _parse_broker_order(self, response, strategy_name, strategy_object=None):
        request = response["request"]
        if request["kind"] != "perp":
            raise ValueError("Swap operations are retrieved with get_swap(), not exact-output orders")
        order = Order(
            strategy_name,
            Asset(request["symbol"], asset_type="crypto_future", leverage=request["leverage"]),
            decimal_value(request["quantity"]),
            request["side"],
            order_type=request["orderType"],
            limit_price=request.get("limitPrice"),
            time_in_force=request["tif"],
            quote=Asset("USDC", asset_type="crypto"),
            identifier=response["id"],
        )
        order.update_raw(response)
        order.status = {
            "filled": "fill",
            "canceled": "canceled",
            "rejected": "error",
            "partial_fill": "partial_fill",
            "open": "open",
        }.get(response["state"], "submitted")
        if response.get("averagePrice"):
            order.avg_fill_price = float(decimal_value(response["averagePrice"]))
        quantity = decimal_value(response["filledQuantity"])
        if quantity and response.get("averagePrice"):
            price = decimal_value(response["averagePrice"])
            order.add_transaction(price, quantity)
            self._observations[str(order.identifier)] = (quantity, quantity * price, response["state"])
        return order

    def _flatten_order(self, order):
        return [order]

    def _modify_order(self, order, **kwargs):
        raise NotImplementedError("Cancel and confirm cancellation before placing a replacement order")

    def cancel_order(self, order):
        response = self.client.call("POST", "/v1/cancel", payload={"id": str(order.identifier)})
        self._observe(order, response)
        return order

    def get_historical_account_value(self):
        raise NotImplementedError("MetaMask does not expose historical account equity through this adapter")

    def get_funding_payments(self, start_time, end_time, symbol=None):
        """Read funding ledger (Unix milliseconds); balances already include these cash flows."""
        if self.mode != "perp":
            raise ValueError("Funding payments apply to perpetuals only")
        params = {"startTime": start_time, "endTime": end_time}
        if symbol is not None:
            params["symbol"] = symbol
        return self.client.call("GET", "/v1/funding", params=params)

    def _get_balances_at_broker(self, quote_asset, strategy):
        balance = self.client.call("GET", "/v1/account", params={"kind": self.mode})
        if quote_asset is not None and quote_asset.symbol != balance["currency"]:
            raise ValueError(f"MetaMask account valuation is denominated in {balance['currency']}")
        return tuple(float(decimal_value(balance[k])) for k in ("cash", "positionsValue", "equity"))

    def _pull_positions(self, strategy):
        balance = self.client.call("GET", "/v1/account", params={"kind": self.mode})
        result = []
        for row in balance["positions"]:
            if self.mode == "perp":
                asset = Asset(row["symbol"], asset_type="crypto_future")
            else:
                token = row.get("asset")
                if token is None:
                    raise MetaMaskAPIError(
                        "UNBOUND_ASSET", "Wallet holds a token absent from the approved asset registry"
                    )
                asset = Asset(
                    token["symbol"],
                    asset_type="crypto",
                    chain_id=token["chainId"],
                    token_address=token["address"],
                    decimals=token["decimals"],
                )
            position = Position(
                strategy,
                asset,
                decimal_value(row["quantity"]),
                avg_fill_price=float(decimal_value(row["entryPrice"])) if row.get("entryPrice") is not None else None,
            )
            position.market_value = float(decimal_value(row["marketValue"]))
            position.pnl = float(decimal_value(row["unrealizedPnl"])) if row.get("unrealizedPnl") is not None else None
            result.append(position)
        return result

    def _pull_position(self, strategy, asset):
        return next((p for p in self._pull_positions(strategy) if p.asset == asset), None)

    def _get_stream_object(self):
        return PollingStream(self._poll_interval)

    def _register_stream_events(self):
        self.stream.add_action(self.stream.POLL_EVENT)(self.do_polling)

    def _run_stream(self):
        self._stream_established()
        self.stream._run()

    def do_polling(self):
        for response in self._pull_broker_all_orders():
            if response["request"]["kind"] != "perp":
                continue
            order = self.get_tracked_order(response["id"])
            if order is not None:
                self._observe(order, response)

    def _observe(self, order, response):
        with self._event_lock:
            state = response["state"]
            previous = self._observations.get(str(order.identifier), (Decimal(0), Decimal(0), None))
            quantity = decimal_value(response["filledQuantity"])
            if quantity < previous[0]:
                raise MetaMaskAPIError(
                    "FILL_REGRESSION", "Provider fill history is incomplete; refusing to overwrite known fills"
                )
            if previous[2] in ("filled", "canceled", "rejected"):
                return
            value = quantity * decimal_value(response.get("averagePrice", "0"))
            if quantity > previous[0]:
                delta = quantity - previous[0]
                price = (value - previous[1]) / delta
                if price <= 0:
                    raise MetaMaskAPIError("INVALID_FILL", "Fill price must be positive")
                event = self.FILLED_ORDER if state == "filled" else self.PARTIALLY_FILLED_ORDER
                self._process_trade_event(order, event, price=price, filled_quantity=delta)
            elif state == "filled" and previous[2] == "partial_fill" and quantity > 0:
                self._process_trade_event(
                    order, self.FILLED_ORDER, price=decimal_value(response["averagePrice"]), filled_quantity=0
                )
            elif state == "open" and previous[2] in (None, "submitted", "awaiting_approval", "unknown"):
                self._process_trade_event(order, self.NEW_ORDER)
            if state == "canceled":
                self._process_trade_event(order, self.CANCELED_ORDER)
            elif state == "rejected":
                self._process_trade_event(
                    order,
                    self.ERROR_ORDER,
                    error=MetaMaskAPIError("REJECTED", response.get("message", "Provider rejected order")),
                )
            order.update_raw(response)
            self._observations[str(order.identifier)] = (quantity, value, state)
