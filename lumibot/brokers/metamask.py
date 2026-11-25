"""Native Python MetaMask managed-wallet broker."""

import os
from decimal import Context, Decimal
from math import isfinite
from threading import Event, RLock, current_thread
from uuid import uuid4

import requests

from lumibot.brokers.broker import ORDER_NATIVE, ORDER_UNSUPPORTED, Broker, OrderSupport
from lumibot.data_sources.metamask_data import MetaMaskData
from lumibot.entities import Asset, Order, Position
from lumibot.tools.metamask_client import MetaMaskAPIError, MetaMaskClient, decimal_value, token_payload
from lumibot.trading_builtins.custom_stream import PollingStream

_SIDE_MAP = {
    "buy": ("buy", False),
    "buy_to_open": ("buy", False),
    "buy_to_close": ("buy", True),
    "buy_to_cover": ("buy", True),
    "sell": ("sell", False),
    "sell_to_open": ("sell", False),
    "sell_short": ("sell", False),
    "sell_to_close": ("sell", True),
}

_STATUS_MAP = {
    "filled": "fill",
    "canceled": "canceled",
    "rejected": "error",
    "partial_fill": "partial_fill",
    "open": "open",
    "waiting_for_fill": "open",
    "awaiting_approval": "awaiting_approval",
}


class MetaMask(Broker):
    """One broker instance binds to one dedicated agent wallet and one market type.

    ``MODE='perp'`` routes Hyperliquid orders (market, limit, stop, stop-limit,
    bracket, OTO, and OCO). ``MODE='spot'`` exposes exact-input swaps via
    :meth:`swap`; token buys cannot promise an exact output quantity.
    MetaMask retains wallet keys. Local limits are cooperative process controls;
    provider Guard Mode enforces the remote wallet policy.
    """

    IS_BACKTESTING_BROKER = False

    ORDER_SUPPORT = {
        "market": ORDER_NATIVE,
        "limit": ORDER_NATIVE,
        "stop": ORDER_NATIVE,
        "stop_limit": ORDER_NATIVE,
        "trailing_stop": (
            ORDER_UNSUPPORTED,
            "Trailing stop orders are not supported by MetaMask. Use a stop or stop-limit order instead.",
        ),
        "bracket": ORDER_NATIVE,
        "oto": ORDER_NATIVE,
        "oco": ORDER_NATIVE,
        "trailing_leg": (
            ORDER_UNSUPPORTED,
            "Trailing stop legs are not supported by MetaMask. Use a fixed stop or stop-limit price instead.",
        ),
        "stop_limit_leg": ORDER_NATIVE,
        "advanced_entry_types": {"market", "limit"},
    }

    def __init__(self, config=None, data_source=None, connect_stream=True, **kwargs):
        config = dict(config or {})
        self.mode = config.get("MODE", "perp")
        if self.mode not in ("perp", "spot"):
            raise ValueError("MetaMask MODE must be 'perp' or 'spot'")
        self.client = MetaMaskClient(config)
        self._observations = {}
        self._event_lock = RLock()
        self._close_lock = RLock()
        self._closed = False
        self._stream_thread = None
        self._stream_ready = Event()
        if not connect_stream:
            self._stream_ready.set()
        self._poll_interval = float(config.get("POLL_INTERVAL", 10))
        data_source = data_source or MetaMaskData(config, client=self.client)
        super().__init__(
            name="metamask", config=config, data_source=data_source, connect_stream=connect_stream, **kwargs
        )
        self.market = config.get("MARKET") or os.environ.get("MARKET") or "24/7"

    @property
    def polling_interval(self) -> float:
        return self._poll_interval

    def get_contract_spec(self, asset, quote=None):
        """Return Hyperliquid linear USDC perpetual contract terms in base units."""
        from lumibot.entities import ContractSpec

        if getattr(asset, "contract_spec", None) is not None:
            return asset.contract_spec
        if self.mode != "perp" or getattr(asset, "asset_type", None) != "crypto_future":
            raise ValueError("Contract specifications are only defined for Hyperliquid crypto_future assets")
        if quote is not None and getattr(quote, "symbol", quote) != "USDC":
            raise ValueError("Hyperliquid perpetuals settle in USDC")
        market = self.client.market(asset.symbol)
        spec = ContractSpec(
            base=market.symbol,
            quote="USDC",
            settle="USDC",
            contract_size=Decimal("1"),
            inverse=False,
            quantity_unit="base",
        )
        if isinstance(asset, Asset):
            asset._resolved_contract_spec = spec
        return spec

    def get_order_size_rules(self, asset, quote=None):
        """Return Hyperliquid lot step derived from szDecimals.

        Ordinary Hyperliquid perpetual orders enforce a price-dependent $10 minimum
        notional rather than a fixed base-unit minimum, so ``min_quantity`` is
        reported as ``None`` while ``quantity_step`` reports the exact lot step.
        """
        if self.mode != "perp" or getattr(asset, "asset_type", None) != "crypto_future":
            return None
        if quote is not None and getattr(quote, "symbol", quote) != "USDC":
            raise ValueError("Hyperliquid perpetuals settle in USDC")
        market = self.client.market(asset.symbol)
        step = Decimal(1).scaleb(-market.size_decimals)
        return {
            "min_quantity": None,
            "quantity_step": step,
            "contract_size": Decimal("1"),
            "quantity_unit": "base",
        }

    def supports_order(
        self,
        order=None,
        *,
        order_type=None,
        order_class=None,
        asset_type=None,
        trailing_leg=None,
        stop_limit_leg=None,
    ) -> OrderSupport:
        if getattr(self, "mode", "perp") != "perp":
            return OrderSupport(
                ORDER_UNSUPPORTED,
                "Spot orders are not supported by MetaMask; use swap(input_asset, output_asset, input_amount) "
                "for exact-input spot swaps.",
            )
        eff_asset_type = getattr(getattr(order, "asset", None), "asset_type", None) if order is not None else asset_type
        if eff_asset_type is not None and str(eff_asset_type).lower() != "crypto_future":
            return OrderSupport(
                ORDER_UNSUPPORTED,
                f"Asset type '{eff_asset_type}' is not supported by MetaMask perp mode; use crypto_future or "
                "swap(input_asset, output_asset, input_amount) for exact-input spot swaps.",
            )
        eff_class = (
            str(order.order_class or Order.OrderClass.SIMPLE).lower()
            if order is not None
            else str(order_class or Order.OrderClass.SIMPLE).lower()
        )
        if eff_class not in ("", "simple", "bracket", "oto", "oco"):
            return OrderSupport(
                ORDER_UNSUPPORTED,
                f"Order class '{eff_class}' is not supported by MetaMask.",
            )
        return super().supports_order(
            order,
            order_type=order_type,
            order_class=order_class,
            asset_type=asset_type,
            trailing_leg=trailing_leg,
            stop_limit_leg=stop_limit_leg,
        )

    def close(self):
        """Release this broker after its Strategy/Trader execution has stopped.

        Stop dispatch, wait for owned polling/submission workers, then release
        the native session and journal. Never close from an owned worker.
        """
        workers = (getattr(self, "_orders_thread", None), self._stream_thread)
        if current_thread() in workers:
            raise RuntimeError("Close MetaMask from its owner after stopping Strategy/Trader execution")
        with self._close_lock:
            if self._closed:
                return
            self.cleanup_streams()
            self._stream_ready.wait()
            for worker in (getattr(self, "_orders_thread", None), self._stream_thread):
                if worker is not None:
                    worker.join()
            self.client.close()
            self._closed = True

    @staticmethod
    def _normalize_side(side):
        key = str(side).lower()
        mapped = _SIDE_MAP.get(key)
        if mapped is None:
            raise ValueError(f"Unsupported MetaMask order side: {side}")
        return mapped

    def _build_child_payload(self, parent_order, child):
        if child.asset != parent_order.asset:
            raise ValueError("Grouped order children must use the same asset as the parent order")
        child_side, _ = self._normalize_side(child.side)
        child_type = str(child.order_type).lower()
        if child_type not in ("limit", "stop", "stop_limit"):
            raise ValueError(f"Child order type '{child_type}' is not supported by MetaMask")
        tpsl = "tp" if child_type == "limit" else "sl"
        child.parent_identifier = str(parent_order.identifier)
        child.reduce_only = True
        payload = {
            "id": str(child.identifier),
            "side": child_side,
            "quantity": format(decimal_value(child.quantity), "f"),
            "orderType": child_type,
            "tpsl": tpsl,
            "reduceOnly": True,
        }
        if child_type == "limit":
            payload["limitPrice"] = format(decimal_value(child.limit_price), "f")
        elif child_type == "stop":
            payload["stopPrice"] = format(decimal_value(child.stop_price), "f")
        else:
            limit_px = child.stop_limit_price if child.stop_limit_price is not None else child.limit_price
            payload["stopPrice"] = format(decimal_value(child.stop_price), "f")
            payload["limitPrice"] = format(decimal_value(limit_px), "f")
        return payload

    def _submit_order(self, order):
        support = self.supports_order(order)
        if support.mode == ORDER_UNSUPPORTED:
            raise ValueError(support.reason)
        if order.quote is not None and order.quote.symbol != "USDC":
            raise ValueError("Hyperliquid perpetuals settle in USDC")
        if order.asset.multiplier != 1:
            raise ValueError("Hyperliquid quantities are base units with multiplier=1")
        side, implied_reduce_only = self._normalize_side(order.side)
        params = order.custom_params or {}
        extra = set(params) - {"reduce_only", "leverage", "tpsl"}
        if extra:
            raise ValueError(f"Unsupported MetaMask order parameters: {sorted(extra)}")
        order_class = str(order.order_class or Order.OrderClass.SIMPLE).lower()
        reduce_only = bool(order.reduce_only or params.get("reduce_only", False) or implied_reduce_only)
        leverage = params.get("leverage", order.asset.leverage)

        if order_class == "simple":
            order_type = str(order.order_type).lower()
            if order_type not in ("market", "limit", "stop", "stop_limit"):
                raise ValueError(f"Order type '{order_type}' is not supported by MetaMask")
            tif = (order.time_in_force or "gtc").lower()
            if order_type in ("stop", "stop_limit"):
                if tif == "day":
                    tif = "gtc"
                if tif != "gtc":
                    raise ValueError("MetaMask trigger orders support time_in_force='gtc' only")
            else:
                if order_type == "market" and tif == "day":
                    tif = "ioc"
                if tif not in ("gtc", "ioc", "alo"):
                    raise ValueError("MetaMask time_in_force must be gtc, ioc, or alo")
            request = {
                "kind": "perp",
                "id": str(order.identifier),
                "symbol": order.asset.symbol,
                "side": side,
                "quantity": format(decimal_value(order.quantity), "f"),
                "orderType": order_type,
                "leverage": leverage,
                "reduceOnly": reduce_only,
                "tif": tif,
            }
            if order_type == "limit":
                request["limitPrice"] = format(decimal_value(order.limit_price), "f")
            elif order_type == "stop":
                request["stopPrice"] = format(decimal_value(order.stop_price), "f")
                if params.get("tpsl") is not None:
                    request["tpsl"] = params["tpsl"]
            elif order_type == "stop_limit":
                limit_px = order.stop_limit_price if order.stop_limit_price is not None else order.limit_price
                request["stopPrice"] = format(decimal_value(order.stop_price), "f")
                request["limitPrice"] = format(decimal_value(limit_px), "f")
                if params.get("tpsl") is not None:
                    request["tpsl"] = params["tpsl"]
        else:
            children = list(order.child_orders or [])
            if not children:
                raise ValueError(f"{order_class.upper()} order requires child orders")
            children_payload = [self._build_child_payload(order, child) for child in children]
            if order_class == "oco":
                order.reduce_only = True
                request = {
                    "kind": "perp",
                    "id": str(order.identifier),
                    "symbol": order.asset.symbol,
                    "side": side,
                    "quantity": format(decimal_value(order.quantity), "f"),
                    "orderType": "limit",
                    "orderClass": "oco",
                    "leverage": leverage,
                    "reduceOnly": True,
                    "tif": "gtc",
                    "children": children_payload,
                }
            else:
                entry_type = str(order.order_type).lower()
                if entry_type not in ("market", "limit"):
                    raise ValueError(f"Entry order type '{entry_type}' is not supported for {order_class} orders")
                tif = (order.time_in_force or "gtc").lower()
                if entry_type == "market" and tif == "day":
                    tif = "ioc"
                if tif not in ("gtc", "ioc", "alo"):
                    raise ValueError("MetaMask time_in_force must be gtc, ioc, or alo")
                request = {
                    "kind": "perp",
                    "id": str(order.identifier),
                    "symbol": order.asset.symbol,
                    "side": side,
                    "quantity": format(decimal_value(order.quantity), "f"),
                    "orderType": entry_type,
                    "orderClass": order_class,
                    "leverage": leverage,
                    "reduceOnly": reduce_only,
                    "tif": tif,
                    "children": children_payload,
                }
                if entry_type == "limit":
                    request["limitPrice"] = format(decimal_value(order.limit_price), "f")

        self.client.verify()
        try:
            response = self.client.submit(request)
        except (requests.Timeout, requests.ConnectionError):
            response = {
                "id": str(order.identifier),
                "request": request,
                "state": "unknown",
                "filledQuantity": "0",
                "message": "Submission outcome unknown; poll the same ID. Do not create a replacement order.",
            }
            if order.child_orders:
                response["childOperations"] = [
                    {
                        "id": str(child.identifier),
                        "parentId": str(order.identifier),
                        "state": "unknown",
                        "filledQuantity": "0",
                    }
                    for child in order.child_orders
                ]
        with self._event_lock:
            if str(order.identifier) not in self._observations and self.get_tracked_order(order.identifier) is None:
                self._unprocessed_orders.append(order)
            for child in order.child_orders or []:
                if str(child.identifier) not in self._observations and self.get_tracked_order(child.identifier) is None:
                    self._unprocessed_orders.append(child)
            self._observe(order, response)
        return self.get_tracked_order(order.identifier) or order

    def _submit_orders(self, orders, **kwargs):
        if kwargs.pop("is_multileg", False) or kwargs:
            raise NotImplementedError("MetaMask batch submission accepts independent orders only")
        # A wallet has one signing operation in flight at a time.
        return [self.submit_order(order) for order in orders]

    def close_position(
        self, strategy_name: str, asset: Asset, fraction: float = 1.00, position_side=None, position_id=None
    ):
        if self.mode != "perp":
            raise ValueError("close_position is only supported in MODE='perp'; use swap() for exact-input spot swaps")
        return super().close_position(
            strategy_name, asset, fraction=fraction, position_side=position_side, position_id=position_id
        )

    def sell_all(self, strategy_name, cancel_open_orders=True, strategy=None, is_multileg=False):
        if self.mode != "perp":
            raise ValueError("sell_all is only supported in MODE='perp'; use swap() for exact-input spot swaps")
        return super().sell_all(
            strategy_name, cancel_open_orders=cancel_open_orders, strategy=strategy, is_multileg=is_multileg
        )

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
            return self.client.submit(request)
        except (requests.Timeout, requests.ConnectionError):
            return {
                "id": request["id"],
                "request": request,
                "state": "unknown",
                "message": "Poll this ID; submission may have reached MetaMask",
            }

    def get_swap(self, identifier):
        return next(
            (op for op in self.client.orders() if op["id"] == identifier and op["request"]["kind"] == "swap"),
            None,
        )

    def _pull_broker_all_orders(self):
        return [op for op in self.client.orders() if op["request"]["kind"] == "perp"]

    def _pull_broker_order(self, identifier):
        target = str(identifier)
        for op in self._pull_broker_all_orders():
            if op["id"] == target:
                return op
            for child in op.get("childOperations") or []:
                if isinstance(child, dict) and child.get("id") == target:
                    return self._child_observation(op, child)
        return None

    @staticmethod
    def _child_observation(parent_response, child_response):
        parent_req = parent_response.get("request") or {}
        child_req = child_response.get("request")
        if not isinstance(child_req, dict):
            child_req = {
                "kind": "perp",
                "id": child_response["id"],
                "symbol": parent_req.get("symbol"),
                "side": child_response.get("side"),
                "quantity": child_response.get("quantity"),
                "orderType": child_response.get("orderType"),
                "tpsl": child_response.get("tpsl"),
                "leverage": parent_req.get("leverage", 1),
                "reduceOnly": True,
                "tif": "gtc",
            }
            if child_response.get("limitPrice") is not None:
                child_req["limitPrice"] = child_response["limitPrice"]
            if child_response.get("stopPrice") is not None:
                child_req["stopPrice"] = child_response["stopPrice"]
        elif "leverage" not in child_req and "leverage" in parent_req:
            child_req = {**child_req, "leverage": parent_req["leverage"]}
        obs = {
            **child_response,
            "parentId": parent_response.get("id"),
            "symbol": parent_req.get("symbol"),
            "request": child_req,
            "filledQuantity": child_response.get("filledQuantity", "0"),
        }
        if "message" not in obs:
            if child_response.get("rejectionReason"):
                obs["message"] = child_response["rejectionReason"]
            elif parent_response.get("message"):
                obs["message"] = parent_response["message"]
        if (
            "orderRelease" not in obs
            and child_response.get("state") == "canceled"
            and isinstance(parent_response.get("orderRelease"), dict)
            and parent_response["orderRelease"].get("state") == "not_released"
        ):
            child_cloid = child_response.get("clientOrderId")
            if isinstance(child_cloid, str):
                obs["orderRelease"] = {
                    **parent_response["orderRelease"],
                    "clientOrderId": child_cloid,
                }
        return obs

    def _hydrate_parsed_order(self, order, response):
        order.update_raw(response)
        self._set_trade_cost(order, response)
        self._set_submission_outcome(order, response)
        order.status = _STATUS_MAP.get(response["state"], "submitted")
        if response["state"] == "rejected":
            order.error_message = response.get("message", "Provider rejected order")
        if response.get("averagePrice"):
            order.avg_fill_price = float(decimal_value(response["averagePrice"]))
        quantity = decimal_value(response.get("filledQuantity", "0"))
        if quantity and response.get("averagePrice"):
            price = decimal_value(response["averagePrice"])
            order.add_transaction(price, quantity)
        with self._event_lock:
            identifier = str(order.identifier)
            if identifier not in self._observations and self.get_tracked_order(identifier) is None:
                # Seed restored history once. Parsing a newer snapshot must not consume pending callbacks.
                value = (
                    Context(prec=160).multiply(quantity, decimal_value(response.get("averagePrice")))
                    if quantity > 0
                    else Decimal(0)
                )
                self._observations[identifier] = (quantity, value, response["state"])
                # Register cumulative snapshots before shared executor synchronization
                # can treat them as new fills and add the full order size again.
                if self._first_iteration or order.is_active():
                    self._process_broker_synced_order(order)
        return order

    def _parse_broker_order(self, response, strategy_name, strategy_object=None):
        request = response["request"]
        if request["kind"] != "perp":
            raise ValueError("Swap operations are retrieved with get_swap(), not exact-output orders")
        order_class = request.get("orderClass", "simple")
        order_type = request["orderType"]
        asset = Asset(request["symbol"], asset_type="crypto_future", leverage=request.get("leverage", 1))
        quote = Asset("USDC", asset_type="crypto")

        child_orders = []
        raw_children = response.get("childOperations")
        if isinstance(raw_children, list) and raw_children:
            for child_raw in raw_children:
                if not isinstance(child_raw, dict):
                    continue
                child_obs = self._child_observation(response, child_raw)
                child_req = child_obs["request"]
                c_type = child_req["orderType"]
                c_limit = child_req.get("limitPrice")
                c_stop = child_req.get("stopPrice")
                child = Order(
                    strategy_name,
                    asset,
                    decimal_value(child_req["quantity"]),
                    child_req["side"],
                    order_type=c_type,
                    limit_price=c_limit if c_type != "stop_limit" else None,
                    stop_price=c_stop,
                    stop_limit_price=c_limit if c_type == "stop_limit" else None,
                    time_in_force=child_req.get("tif", "gtc"),
                    quote=quote,
                    identifier=child_obs["id"],
                    reduce_only=True,
                )
                child.parent_identifier = str(response["id"])
                self._hydrate_parsed_order(child, child_obs)
                child_orders.append(child)
            if order_class in ("oco", "bracket") and len(child_orders) == 2:
                child_orders[0].dependent_order = child_orders[1]
                child_orders[1].dependent_order = child_orders[0]

        limit_px = request.get("limitPrice")
        stop_px = request.get("stopPrice")
        order = Order(
            strategy_name,
            asset,
            decimal_value(request["quantity"]),
            request["side"],
            order_type=order_type,
            order_class=order_class if order_class != "simple" else None,
            limit_price=limit_px if order_type != "stop_limit" else None,
            stop_price=stop_px,
            stop_limit_price=limit_px if order_type == "stop_limit" else None,
            time_in_force=request.get("tif", "gtc"),
            quote=quote,
            identifier=response["id"],
            reduce_only=request.get("reduceOnly", False),
            child_orders=child_orders or None,
        )
        if response.get("parentId"):
            order.parent_identifier = str(response["parentId"])
        return self._hydrate_parsed_order(order, response)

    def _flatten_order(self, order):
        return [o for o in (order, *list(order.child_orders or [])) if self.get_tracked_order(o.identifier) is None]

    def _apply_direct_order_observation(self, order_lumi, matched_order):
        with self._event_lock:
            previous = self._observations.get(str(order_lumi.identifier))
            previous_children = {
                str(child.identifier): self._observations.get(str(child.identifier))
                for child in (order_lumi.child_orders or [])
            }
            self._observe(order_lumi, matched_order._raw)
            if previous != self._observations.get(str(order_lumi.identifier)):
                return True
            return any(prev != self._observations.get(cid) for cid, prev in previous_children.items())

    def _process_broker_synced_order(self, order):
        """Restore a cumulative snapshot without replaying fills into account state."""
        with self._lock:
            if self.get_tracked_order(order.identifier) is not None:
                return
            order.set_new()
            if order.is_filled():
                order.set_filled()
                tracker = self._filled_orders
            elif order.status == Order.OrderStatus.ERROR:
                order.set_error(order.error_message or "Provider rejected order")
                tracker = self._error_orders
            elif order.is_canceled():
                order.set_canceled()
                tracker = self._canceled_orders
            elif order.status == Order.OrderStatus.PARTIALLY_FILLED:
                order.set_partially_filled()
                tracker = self._partially_filled_orders
            elif order.status == Order.OrderStatus.AWAITING_APPROVAL:
                # Keep pre-release approval orders in unprocessed so approval -> open emits NEW_ORDER cleanly.
                tracker = self._unprocessed_orders
            else:
                tracker = self._new_orders if order.is_active() else self._unprocessed_orders
            tracker.append(order)
            self._invalidate_order_caches()

    def _modify_order(self, order, **kwargs):
        raise NotImplementedError("Cancel and confirm cancellation before placing a replacement order")

    def cancel_order(self, order):
        try:
            response = self.client.cancel(str(order.identifier))
        except (requests.Timeout, requests.ConnectionError):
            self.logger.warning("MetaMask cancel timed out for order %s; poll to confirm status", order.identifier)
            return order
        except MetaMaskAPIError as error:
            if error.code in {"REQUEST_OUTCOME_UNKNOWN", "VENUE_OUTCOME_UNKNOWN"}:
                self.logger.warning(
                    "MetaMask cancel outcome unknown for order %s (%s); poll to confirm status",
                    order.identifier,
                    error.code,
                )
                return order
            raise
        self._observe(order, response)
        return order

    def cancel_orders(self, orders):
        """Cancel orders sequentially, parents before children, skipping legs already terminalized."""
        ordered = sorted(
            orders,
            key=lambda item: 1 if getattr(item, "parent_identifier", None) is not None else 0,
        )
        for item in ordered:
            tracked = self.get_tracked_order(item.identifier) or item
            if tracked.is_canceled() or tracked.is_filled() or tracked.status == Order.OrderStatus.ERROR:
                continue
            try:
                self.cancel_order(tracked)
            except Exception as error:
                self.logger.error(
                    "MetaMask failed to cancel order %s during bulk cancel: %s",
                    tracked.identifier,
                    error,
                )

    def get_historical_account_value(self):
        raise NotImplementedError("MetaMask does not expose historical account equity through this adapter")

    def get_cash_event_sync(self, *, since=None, limit=5000):
        """Read a bounded USDC window with explicit account and coverage evidence."""
        from lumibot.tools.metamask.activity import cash_event_sync

        return cash_event_sync(self.client, self.mode, since=since, limit=limit)

    def get_cash_events(self, *, since=None, limit=100):
        """Return complete normalized history, or refuse an ambiguous/truncated list."""
        result = self.get_cash_event_sync(since=since, limit=limit)
        if result.state != "complete":
            raise MetaMaskAPIError("CASH_HISTORY_INCOMPLETE", result.reason_code)
        return list(result.events)

    def get_funding_payments(self, start_time, end_time, symbol=None):
        """Read funding ledger (Unix milliseconds); balances already include these cash flows."""
        if self.mode != "perp":
            raise ValueError("Funding payments apply to perpetuals only")
        return self.client.funding(start_time, end_time, symbol)

    def _get_balances_at_broker(self, quote_asset, strategy):
        balance = self.client.account(self.mode)
        if quote_asset is not None and quote_asset.symbol != balance["currency"]:
            raise ValueError(f"MetaMask account valuation is denominated in {balance['currency']}")
        return tuple(float(decimal_value(balance[k])) for k in ("cash", "positionsValue", "equity"))

    def _pull_positions(self, strategy):
        balance = self.client.account(self.mode)
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
        self._stream_thread = current_thread()
        try:
            self._stream_ready.set()
            self._stream_established()
            self.stream._run()
        finally:
            self._stream_ready.set()

    def do_polling(self):
        if self.mode != "perp":
            return
        try:
            orders = self._pull_broker_all_orders()
        except Exception:
            self._record_order_feed(False)
            raise
        first_error = None
        for response in orders:
            if response["request"]["kind"] != "perp":
                continue
            order = self.get_tracked_order(response["id"])
            if order is not None:
                try:
                    self._observe(order, response)
                except Exception as error:
                    self.logger.error("MetaMask order observation failed for %s: %s", response["id"], error)
                    if first_error is None:
                        first_error = error
            elif isinstance(response.get("childOperations"), list):
                for child_resp in response["childOperations"]:
                    if not isinstance(child_resp, dict) or not child_resp.get("id"):
                        continue
                    child_order = self.get_tracked_order(child_resp["id"])
                    if child_order is not None:
                        try:
                            self._observe(child_order, self._child_observation(response, child_resp))
                        except Exception as error:
                            self.logger.error(
                                "MetaMask child order observation failed for %s: %s", child_resp["id"], error
                            )
                            if first_error is None:
                                first_error = error
        if first_error is not None:
            self._record_order_feed(False)
            raise first_error
        self._record_order_feed(True)

    def _process_partially_filled_order(self, order, price, quantity):
        with self._lock:
            terminal_status = order.status if order.is_canceled() or order.status == self.ERROR_ORDER else None
            result = super()._process_partially_filled_order(order, price, quantity)
            if terminal_status is not None:
                # Late execution evidence must reach callbacks without reopening the order.
                order.status = terminal_status
                self._clean_order_trackers(order)
            return result

    @staticmethod
    def _set_submission_outcome(order, response):
        state = response["state"]
        certainty = (
            "confirmed_submitted"
            if state in {"open", "waiting_for_fill", "partial_fill", "filled"}
            else "confirmed_rejected"
            if state == "rejected"
            else "unknown"
        )
        previous = getattr(order, "submission_outcome", None)
        previous = previous if isinstance(previous, dict) else {}
        release = response.get("orderRelease")
        if release is None and "orderRelease" not in response and state == "canceled":
            release = previous.get("order_release")
        venue_id = response.get("providerOrderId")
        valid_venue_id = (type(venue_id) is int and venue_id > 0) or (
            isinstance(venue_id, str) and venue_id.isdecimal() and int(venue_id) > 0
        )
        venue_evidence = (
            valid_venue_id
            or decimal_value(response.get("filledQuantity", "0")) > 0
            or bool(order.transactions)
            or previous.get("state") in {"open", "waiting_for_fill", "partial_fill", "filled"}
            or previous.get("venue_acceptance_observed") is True
        )
        if state == "canceled" and venue_evidence:
            certainty = "confirmed_submitted"
        if state == "canceled" and (release is not None or "orderRelease" in response) and not venue_evidence:
            client_id = release.get("clientOrderId") if isinstance(release, dict) else None
            proven = (
                isinstance(release, dict)
                and release.get("state") == "not_released"
                and release.get("source") == "native_plan_local_cancel"
                and isinstance(client_id, str)
                and len(client_id) == 34
                and client_id.startswith("0x")
                and all(character in "0123456789abcdef" for character in client_id[2:])
                and response.get("providerOrderId") is None
                and not response.get("fees")
            )
            certainty = "confirmed_not_submitted" if proven else "unknown"
        order.submission_outcome = {
            "broker_state_certainty": certainty,
            "reconciliation_required": certainty == "unknown" or bool(response.get("reconciliationErrorCode")),
            "state": state,
        }
        if state == "awaiting_approval" and isinstance(response.get("approvalId"), str) and response["approvalId"]:
            order.submission_outcome["approval_id"] = response["approvalId"]
        if state in {"open", "waiting_for_fill", "partial_fill", "filled"} or venue_evidence:
            order.submission_outcome["venue_acceptance_observed"] = True
        if certainty == "confirmed_not_submitted":
            order.submission_outcome["order_release"] = dict(release)

    @staticmethod
    def _set_trade_cost(order, response):
        if "fees" not in response:
            return
        fees = response["fees"]
        if not isinstance(fees, list):
            raise MetaMaskAPIError("INVALID_FEE", "Order fees must be a list of currency and amount records")
        money, total, supported = Context(prec=160), Decimal(0), True
        for fee in fees:
            if not isinstance(fee, dict) or not isinstance(fee.get("asset"), str) or not fee["asset"]:
                raise MetaMaskAPIError("INVALID_FEE", "Order fee currency is missing")
            amount = decimal_value(fee.get("amount"))
            supported = supported and fee["asset"] == "USDC"
            if fee["asset"] == "USDC":
                total = money.add(total, amount)
        cost = float(total) if supported else None
        if cost is not None and not isfinite(cost):
            raise MetaMaskAPIError("INVALID_FEE", "Order fee total exceeds the supported reporting range")
        order.trade_cost = cost

    def _observe_children(self, order, response):
        raw_children = response.get("childOperations")
        if not isinstance(raw_children, list) or not raw_children:
            return
        child_by_id = {str(child.identifier): child for child in (order.child_orders or [])}
        first_error = None
        for child_resp in raw_children:
            if not isinstance(child_resp, dict) or not child_resp.get("id"):
                continue
            cid = str(child_resp["id"])
            child_order = self.get_tracked_order(cid) or child_by_id.get(cid)
            if child_order is not None:
                try:
                    self._observe(child_order, self._child_observation(response, child_resp))
                except Exception as error:
                    self.logger.error("MetaMask child order observation failed for %s: %s", cid, error)
                    if first_error is None:
                        first_error = error
        if first_error is not None:
            raise first_error

    def _observe(self, order, response):
        with self._event_lock:
            order = self.get_tracked_order(order.identifier) or order
            req = response.get("request") if isinstance(response.get("request"), dict) else {}
            is_oco_container = str(order.order_class or "").lower() == "oco" or req.get("orderClass") == "oco"
            if is_oco_container:
                child_error = None
                try:
                    self._observe_children(order, response)
                except Exception as error:
                    child_error = error
                state = response["state"]
                previous = self._observations.get(str(order.identifier), (Decimal(0), Decimal(0), None))
                if previous[2] in ("filled", "canceled", "rejected"):
                    state = previous[2]
                    response = {**response, "state": state}
                    order.update_raw(response)
                    self._set_submission_outcome(order, response)
                    if child_error is not None:
                        raise child_error
                    return
                if state in ("open", "partial_fill") and previous[2] in (
                    None,
                    "submitted",
                    "awaiting_approval",
                    "unknown",
                    "prepared",
                ):
                    self._process_trade_event(order, self.NEW_ORDER)
                elif state == "awaiting_approval":
                    order.status = Order.OrderStatus.AWAITING_APPROVAL
                elif state == "submitted" and order.status == Order.OrderStatus.AWAITING_APPROVAL:
                    order.status = Order.OrderStatus.SUBMITTED
                if state == "partial_fill" and previous[2] != "partial_fill":
                    with self._lock:
                        order.status = Order.OrderStatus.PARTIALLY_FILLED
                        order.set_partially_filled()
                        self._clean_order_trackers(order)
                        self._invalidate_order_caches()
                elif state == "filled" and previous[2] != "filled":
                    # Child leg already emitted the authoritative fill; retire container without double-counting.
                    with self._lock:
                        order.status = self.FILLED_ORDER
                        order.set_filled()
                        self._clean_order_trackers(order)
                        self._invalidate_order_caches()
                elif state == "canceled" and previous[2] != "canceled":
                    self._process_trade_event(order, self.CANCELED_ORDER)
                elif state == "rejected" and previous[2] != "rejected":
                    self._process_trade_event(
                        order,
                        self.ERROR_ORDER,
                        error=MetaMaskAPIError("REJECTED", response.get("message", "Provider rejected order")),
                    )
                order.update_raw(response)
                self._set_submission_outcome(order, response)
                self._observations[str(order.identifier)] = (Decimal(0), Decimal(0), state)
                if child_error is not None:
                    raise child_error
                return

            money = Context(prec=160)
            state = response["state"]
            previous = self._observations.get(str(order.identifier), (Decimal(0), Decimal(0), None))
            parent_error = None
            terminal_unchanged = False
            try:
                quantity = decimal_value(response.get("filledQuantity", "0"))
                if quantity < previous[0]:
                    raise MetaMaskAPIError(
                        "FILL_REGRESSION", "Provider fill history is incomplete; refusing to overwrite known fills"
                    )
                if quantity > decimal_value(order.quantity):
                    raise MetaMaskAPIError("INVALID_FILL", "Cumulative fills exceed the original order quantity")
                self._set_trade_cost(order, response)
                if previous[2] in ("filled", "canceled", "rejected"):
                    state = previous[2]
                    response = {**response, "state": state}
                    if quantity == previous[0]:
                        order.update_raw(response)
                        # Preserve proven terminal identity despite a stale unknown reply.
                        self._set_submission_outcome(order, response)
                        terminal_unchanged = True
                if not terminal_unchanged:
                    value = (
                        money.multiply(quantity, decimal_value(response.get("averagePrice")))
                        if quantity > 0
                        else Decimal(0)
                    )
                    if quantity > previous[0]:
                        delta = money.subtract(quantity, previous[0])
                        price = money.divide(money.subtract(value, previous[1]), delta)
                        if price <= 0:
                            raise MetaMaskAPIError("INVALID_FILL", "Fill price must be positive")
                        event = self.FILLED_ORDER if state == "filled" else self.PARTIALLY_FILLED_ORDER
                        self._process_trade_event(order, event, price=price, filled_quantity=delta)
                    elif state == "filled" and previous[2] == "partial_fill" and quantity > 0:
                        self._process_trade_event(
                            order, self.FILLED_ORDER, price=decimal_value(response["averagePrice"]), filled_quantity=0
                        )
                    elif state in ("open", "waiting_for_fill") and previous[2] in (
                        None,
                        "submitted",
                        "awaiting_approval",
                        "unknown",
                        "prepared",
                    ):
                        self._process_trade_event(order, self.NEW_ORDER)
                    elif state == "awaiting_approval":
                        order.status = Order.OrderStatus.AWAITING_APPROVAL
                    elif state == "submitted" and order.status == Order.OrderStatus.AWAITING_APPROVAL:
                        order.status = Order.OrderStatus.SUBMITTED
                    if state == "canceled" and previous[2] != "canceled":
                        self._process_trade_event(order, self.CANCELED_ORDER)
                    elif state == "rejected" and previous[2] != "rejected":
                        self._process_trade_event(
                            order,
                            self.ERROR_ORDER,
                            error=MetaMaskAPIError("REJECTED", response.get("message", "Provider rejected order")),
                        )
                    order.update_raw(response)
                    self._set_submission_outcome(order, response)
                    self._observations[str(order.identifier)] = (quantity, value, state)
            except Exception as error:
                parent_error = error
            try:
                self._observe_children(order, response)
            except Exception as child_error:
                if parent_error is None:
                    raise
                self.logger.error("MetaMask child observation also failed for %s: %s", order.identifier, child_error)
            if parent_error is not None:
                raise parent_error
