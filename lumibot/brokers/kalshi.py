"""Kalshi live broker using LumiBot orders, positions and lifecycle dispatch."""

from __future__ import annotations

import json
import queue
import random
import threading
import time
import uuid
from datetime import datetime
from decimal import Decimal

from lumibot.brokers.broker import Broker
from lumibot.data_sources.kalshi_data import KalshiData
from lumibot.entities import Asset, Order, Position
from lumibot.tools.kalshi_client import KalshiAPIError, KalshiClient, decimal_value
from lumibot.trading_builtins import CustomStream


class KalshiStream(CustomStream):
    """Existing queue dispatcher, with authenticated WS wakeups and REST repair.

    Order/fill notifications wake reconciliation on this stream's dispatcher.
    REST cumulative fills are authoritative, so dropped, duplicated or reordered
    WebSocket messages cannot double-count executions.
    """

    UPDATE = "kalshi_update"

    def __init__(self, broker, *, websocket_factory=None):
        super().__init__()
        self.broker = broker
        self._websocket_factory = websocket_factory
        self._reader = None
        self._socket = None
        self._subscriptions = set()
        self._repair = threading.Event()
        self._repair.set()

    def _receive(self, message):
        kind = message.get("type")
        if kind == "error":
            raise KalshiAPIError("Kalshi WebSocket rejected a subscription")
        if kind == "subscribed":
            self._repair.set()
            self._subscriptions.add((message.get("msg") or {}).get("channel"))
            if {"user_orders", "fill"} <= self._subscriptions:
                self.broker._stream_established()
        if kind in ("user_order", "fill"):
            row = message.get("msg") or {}
            if row.get("order_id"):
                try:
                    self._queue.put_nowait((self.UPDATE, {"identifier": row["order_id"]}))
                except queue.Full:
                    self._repair.set()

    def _read_websocket(self):
        from websockets.sync.client import connect

        factory = self._websocket_factory or connect
        attempt = 0
        while not self._stop_event.is_set():
            try:
                client = self.broker._client
                with factory(
                    client.ws_url,
                    additional_headers=client.auth_headers("GET", client.WS_PATH),
                    open_timeout=10,
                    close_timeout=2,
                    ping_interval=20,
                    ping_timeout=20,
                ) as socket:
                    self._socket = socket
                    self._subscriptions.clear()
                    socket.send(
                        json.dumps({"id": 1, "cmd": "subscribe", "params": {"channels": ["user_orders", "fill"]}})
                    )
                    self._repair.set()
                    while not self._stop_event.is_set():
                        try:
                            message = socket.recv(timeout=1)
                        except TimeoutError:
                            continue
                        self._receive(json.loads(message))
                        attempt = 0
            except Exception:
                if not self._stop_event.is_set():
                    # Exceptions from websocket libraries can contain signed
                    # handshake headers. Log no exception text or traceback.
                    self.broker.logger.warning("Kalshi stream disconnected; reconciling and reconnecting")
                    self._repair.set()
                    self._stop_event.wait(min(30, 0.5 * 2 ** min(attempt, 6)) + random.random() * 0.25)
                    attempt += 1
            finally:
                self._socket = None
                self.broker._is_stream_subscribed = False

    def _run(self):
        self._thread = threading.current_thread()
        self._reader = threading.Thread(target=self._read_websocket, name="Kalshi-websocket", daemon=True)
        self._reader.start()
        last_poll = 0.0
        while not self._stop_event.is_set():
            try:
                event, payload = self._queue.get(timeout=0.2)
                try:
                    self._process_queue_event(event, payload)
                finally:
                    self._queue.task_done()
            except queue.Empty:
                pass
            except Exception:
                self._repair.set()
                self.broker.logger.warning("Kalshi order update deferred to REST reconciliation")
            now = time.monotonic()
            if self._repair.is_set() or now - last_poll >= self.broker.polling_interval:
                self._repair.clear()
                last_poll = now
                try:
                    if self.broker._strategy_name:
                        self.broker.sync_orders(self.broker._strategy_name)
                        self.broker.sync_positions(self.broker._strategy_name)
                except Exception:
                    self.broker.logger.warning("Kalshi REST reconciliation failed; retrying next polling interval")

    def stop(self):
        self._stop_event.set()
        if self._socket is not None:
            self._socket.close()
        if self._reader and self._reader is not threading.current_thread():
            self._reader.join(timeout=3)
        if self._thread is not threading.current_thread():
            super().stop()


class Kalshi(Broker):
    """Live Kalshi event markets. All prices and order sides refer to YES."""

    NAME = "Kalshi"
    _TIF = {
        "gtc": "good_till_canceled",
        "gtd": "good_till_canceled",
        "ioc": "immediate_or_cancel",
        "fok": "fill_or_kill",
    }

    def __init__(
        self, config=None, data_source=None, *, connect_stream=True, max_workers=4, polling_interval=5, client=None
    ):
        config = config or {}
        subaccount = config.get("SUBACCOUNT", config.get("KALSHI_SUBACCOUNT", 0))
        if isinstance(subaccount, bool) or str(subaccount) not in {str(n) for n in range(64)}:
            raise ValueError("KALSHI_SUBACCOUNT must be an integer from 0 to 63")
        self.subaccount = int(subaccount)
        self.polling_interval = max(1.0, float(polling_interval))
        self._client = client if client is not None else KalshiClient(config, require_auth=True)
        self._owns_client = client is None
        self._reconcile_lock = threading.RLock()
        self._pending_submissions = {}
        data_source = data_source or KalshiData(client=self._client)
        self._kalshi_data = data_source if isinstance(data_source, KalshiData) else KalshiData(client=self._client)
        if isinstance(data_source, KalshiData) and data_source._client.is_demo != self._client.is_demo:
            raise ValueError("Kalshi broker and data source must use the same environment")
        # Pass no credential dictionary into Broker's generic config/logging path.
        super().__init__(
            name=self.NAME,
            data_source=data_source,
            config={"MARKET": "24/7"},
            connect_stream=connect_stream,
            max_workers=max_workers,
            start_orders_thread=False,
        )

    def _get_balances_at_broker(self, quote_asset, strategy):
        if quote_asset is not None and quote_asset.symbol != "USD":
            raise ValueError("Kalshi account balances are denominated in USD")
        row = self._client.request("GET", "/portfolio/balance", params={"subaccount": self.subaccount})
        cash = (
            decimal_value(row["balance_dollars"], "balance")
            if row.get("balance_dollars") is not None
            else decimal_value(row["balance"], "balance") / 100
        )
        positions = decimal_value(row["portfolio_value"], "portfolio_value") / 100
        return float(cash), float(positions), float(cash + positions)

    def get_historical_account_value(self):
        """Kalshi does not expose historical equity through this broker contract."""
        return {}

    def _pull_positions(self, strategy):
        name = self._strategy_name_from_input(strategy)
        rows = self._client.pages(
            "/portfolio/positions",
            "market_positions",
            params={"subaccount": self.subaccount, "limit": 1000, "count_filter": "position"},
        )
        positions = []
        for row in rows:
            quantity = decimal_value(row.get("position_fp", row.get("position")), "position")
            if quantity == 0:
                continue
            asset = Asset(row["ticker"], asset_type=Asset.AssetType.PREDICTION_CONTRACT, precision="0.01")
            exposure = row.get("market_exposure_dollars")
            avg = decimal_value(exposure) / abs(quantity) if exposure is not None else None
            # Preserve signed YES inventory; NO contracts are negative. Cost and
            # value of a NO holding use its complementary price, not -YES price.
            position = Position(name, asset, quantity, avg_fill_price=float(avg) if avg is not None else None)
            position._raw = row
            positions.append(position)
        return positions

    def _pull_position(self, strategy, asset):
        KalshiData._validate_asset(asset)
        return next((position for position in self._pull_positions(strategy) if position.asset == asset), None)

    def _pull_broker_all_orders(self):
        return list(
            self._client.pages("/portfolio/orders", "orders", params={"subaccount": self.subaccount, "limit": 1000})
        )

    def _pull_broker_order(self, identifier):
        try:
            row = self._client.request("GET", f"/portfolio/orders/{self._client.path_id(identifier)}")["order"]
        except KalshiAPIError as exc:
            if exc.status_code != 404:
                raise
            # The historical API has no order-ID filter; explicit old-ID reads
            # must paginate history. Normal synchronization stays on live data.
            row = next(
                (
                    r
                    for r in self._client.pages("/historical/orders", "orders", params={"limit": 1000})
                    if str(r.get("order_id")) == str(identifier)
                ),
                None,
            )
        if row is not None and row.get("subaccount_number", 0) != self.subaccount:
            return None
        return row

    @staticmethod
    def _side(row):
        book_side = row.get("book_side")
        if book_side is None:
            outcome = row.get("outcome_side")
            if outcome in ("yes", "no"):
                book_side = "bid" if outcome == "yes" else "ask"
            elif row.get("side") in ("yes", "no") and row.get("action") in ("buy", "sell"):
                book_side = "bid" if (row["side"] == "yes") == (row["action"] == "buy") else "ask"
        if book_side not in ("bid", "ask"):
            raise KalshiAPIError("Kalshi order has an unknown direction")
        return Order.OrderSide.BUY if book_side == "bid" else Order.OrderSide.SELL

    @staticmethod
    def _counts(row):
        filled = decimal_value(row.get("fill_count_fp", row.get("fill_count", 0)), "fill count")
        remaining = decimal_value(row.get("remaining_count_fp", row.get("remaining_count", 0)), "remaining count")
        initial = decimal_value(
            row.get("initial_count_fp", row.get("initial_count", filled + remaining)), "initial count"
        )
        if min(filled, remaining, initial) < 0 or filled > initial or filled + remaining > initial:
            raise KalshiAPIError("Kalshi returned inconsistent order quantities")
        return filled, remaining, initial

    def _parse_broker_order(self, response, strategy_name, strategy_object=None):
        filled, remaining, initial = self._counts(response)
        state = response.get("status")
        status = {
            "resting": Order.OrderStatus.NEW,
            "executed": Order.OrderStatus.FILLED,
            "canceled": Order.OrderStatus.CANCELED,
            "rejected": Order.OrderStatus.ERROR,
        }.get(state, Order.OrderStatus.UNKNOWN)
        if state == "resting" and filled:
            status = Order.OrderStatus.PARTIALLY_FILLED
        price = KalshiData._number(response, "yes_price_dollars", legacy="yes_price", scale=100)
        order = Order(
            strategy_name,
            Asset(response["ticker"], asset_type=Asset.AssetType.PREDICTION_CONTRACT, precision="0.01"),
            initial,
            self._side(response),
            order_type=response.get("type", "limit"),
            limit_price=price,
            identifier=str(response["order_id"]),
            status=status,
            time_in_force={v: k for k, v in self._TIF.items() if k != "gtd"}.get(response.get("time_in_force"), "gtc"),
        )
        order.update_raw(response)
        order.broker_create_date = response.get("created_time")
        order.broker_update_date = response.get("last_update_time")
        order._kalshi_client_order_id = response.get("client_order_id")
        return order

    def _order_payload(self, order):
        KalshiData._validate_asset(order.asset, order.quote, order.exchange)
        if order.order_class not in (None, "", Order.OrderClass.SIMPLE) or order.child_orders:
            raise ValueError(
                "Kalshi supports simple orders only; bracket, OCO, OTO and multileg orders are unsupported"
            )
        if order.order_type != Order.OrderType.LIMIT:
            raise ValueError("Kalshi supports limit orders only; use an explicit limit price and GTC, IOC, FOK or GTD")
        side = {Order.OrderSide.BUY: "bid", Order.OrderSide.SELL: "ask"}.get(order.side)
        if side is None:
            raise ValueError("Kalshi order side must be 'buy' or 'sell' in YES-contract terms")
        quantity = decimal_value(order.quantity, "quantity")
        price = decimal_value(order.limit_price, "limit price")
        if quantity <= 0 or quantity % Decimal("0.01"):
            raise ValueError("Kalshi quantity must be positive, in increments of 0.01 contracts")
        if not 0 < price < 1 or price % Decimal("0.0001"):
            raise ValueError("Kalshi limit price must be between 0 and 1 USD with at most four decimal places")
        tif = str(order.time_in_force).lower()
        if tif not in self._TIF:
            raise ValueError("Kalshi time_in_force must be gtc, ioc, fok or gtd; day is unsupported")
        if order.custom_params:
            raise ValueError("Kalshi custom order parameters are unsupported in this integration")
        payload = {
            "ticker": order.asset.symbol,
            "side": side,
            "count": f"{quantity:.2f}",
            "price": f"{price:.4f}",
            "time_in_force": self._TIF[tif],
            "self_trade_prevention_type": "taker_at_cross",
            "subaccount": self.subaccount,
        }
        if tif == "gtd":
            expiration = order.good_till_date
            if not isinstance(expiration, datetime) or expiration.tzinfo is None:
                raise ValueError("Kalshi GTD requires a timezone-aware good_till_date")
            if expiration.timestamp() <= time.time():
                raise ValueError("Kalshi good_till_date must be in the future")
            payload["expiration_time"] = int(expiration.timestamp())
        elif order.good_till_date is not None:
            raise ValueError("Kalshi good_till_date requires time_in_force='gtd'")
        market = self._kalshi_data._market(order.asset)
        if market.get("market_type") != "binary" or market.get("mve_collection_ticker"):
            raise ValueError("Kalshi V1 supports binary, non-multivariate markets only")
        if market.get("status") not in ("active", "open"):
            raise ValueError("Kalshi market is not open for trading")
        ranges = market.get("price_ranges", [])
        valid = False
        for band in ranges:
            start, end, step = (decimal_value(band[k], "price range") for k in ("start", "end", "step"))
            if step > 0 and start <= price <= end and (price - start) % step == 0:
                valid = True
                break
        if not valid:
            raise ValueError("Kalshi limit price does not match the market's published price ranges/tick size")
        return payload

    def _submit_order(self, order):
        if order.was_transmitted():
            raise ValueError("Kalshi order is already submitted; use modify_order or create a new order")
        payload = self._order_payload(order)
        # Retain this ID on an uncertain outcome. Retrying the same Order object
        # cannot accidentally invent a second idempotency key.
        client_id = getattr(order, "_kalshi_client_order_id", None) or str(uuid.uuid4())
        order._kalshi_client_order_id = client_id
        payload["client_order_id"] = client_id
        with self._reconcile_lock:
            self._pending_submissions[client_id] = order
            try:
                response = self._client.request("POST", "/portfolio/events/orders", json=payload)
            except KalshiAPIError as exc:
                if exc.status_code is not None and 400 <= exc.status_code < 500 and exc.status_code != 409:
                    self._pending_submissions.pop(client_id, None)
                    self._process_trade_event(order, self.ERROR_ORDER, error=exc)
                else:
                    order.status = Order.OrderStatus.UNKNOWN
                    order.error_message = "Kalshi submission outcome unknown; reconcile client_order_id before retrying"
                raise
            order.identifier = str(response["order_id"])
            order.update_raw(response)
            self._pending_submissions.pop(client_id, None)
            order._kalshi_fill_count = Decimal(0)
            order._kalshi_fill_notional = Decimal(0)
            self._process_trade_event(order, self.NEW_ORDER)
            self._invalidate_order_caches()
        # A failed follow-up read must not turn an accepted submit into a failed
        # submit that a caller might retry. Periodic/WS reconciliation repairs it.
        self._refresh_after_mutation(order)
        return order

    def _refresh_after_mutation(self, order):
        try:
            self._reconcile_order(order.identifier)
        except Exception:
            self.logger.warning("Kalshi accepted order mutation; status refresh deferred to reconciliation")

    def cancel_order(self, order):
        if not order.identifier:
            raise ValueError("Kalshi cancellation requires a broker order identifier")
        self._client.request(
            "DELETE",
            f"/portfolio/events/orders/{self._client.path_id(order.identifier)}",
            params={"subaccount": self.subaccount, "market_ticker": order.asset.symbol},
        )
        # Local CANCELLING is not terminal and must never suppress this request.
        self._refresh_after_mutation(order)

    def _modify_order(self, order, limit_price=None, stop_price=None):
        if stop_price is not None or limit_price is None:
            raise ValueError("Kalshi modify_order supports limit_price only")
        with self._reconcile_lock:
            snapshot = self._pull_broker_order(order.identifier)
            if snapshot is None:
                raise KalshiAPIError("Kalshi order was not found for modification")
            candidate = self._parse_broker_order(snapshot, order.strategy)
            candidate.limit_price = limit_price
            candidate.time_in_force = order.time_in_force
            candidate.good_till_date = order.good_till_date
            validated = self._order_payload(candidate)
            updated_id = str(uuid.uuid4())
            payload = {key: validated[key] for key in ("ticker", "side", "price", "count")}
            payload.update(client_order_id=snapshot.get("client_order_id"), updated_client_order_id=updated_id)
            self._client.request(
                "POST",
                f"/portfolio/events/orders/{self._client.path_id(order.identifier)}/amend",
                params={"subaccount": self.subaccount},
                json=payload,
            )
            order.limit_price = limit_price
            order._kalshi_client_order_id = updated_id
            self._process_trade_event(order, self.MODIFIED_ORDER)
        self._refresh_after_mutation(order)
        return order

    def _fill_totals(self, row):
        identifier = row["order_id"]
        fills = list(
            self._client.pages(
                "/portfolio/fills",
                "fills",
                params={"order_id": identifier, "subaccount": self.subaccount, "limit": 1000},
            )
        )
        expected, _, _ = self._counts(row)
        if sum(decimal_value(f.get("count_fp", f.get("count", 0))) for f in fills) < expected:
            fills.extend(
                f
                for f in self._client.pages(
                    "/historical/fills", "fills", params={"ticker": row["ticker"], "limit": 1000}
                )
                if f.get("order_id") == identifier and f.get("subaccount_number", 0) == self.subaccount
            )
        unique = {}
        for fill in fills:
            if fill.get("order_id") != identifier:
                raise KalshiAPIError("Kalshi fill did not match the requested order")
            key = fill.get("fill_id", fill.get("trade_id"))
            if not key:
                raise KalshiAPIError("Kalshi fill is missing its identifier")
            unique[key] = fill
        count, notional, fees = Decimal(0), Decimal(0), Decimal(0)
        for fill in unique.values():
            quantity = decimal_value(fill.get("count_fp", fill.get("count")), "fill count")
            price = decimal_value(fill["yes_price_dollars"], "fill price")
            if quantity <= 0 or not 0 <= price <= 1:
                raise KalshiAPIError("Kalshi returned an invalid fill")
            count += quantity
            notional += quantity * price
            fees += decimal_value(fill.get("fee_cost", 0), "fee")
        # Either REST view can lead the other. Applying fills newer than the
        # order snapshot can prematurely complete an order or seed imported
        # history with a fill checkpoint ahead of its lifecycle state.
        if count != expected:
            raise KalshiAPIError("Kalshi fills are not yet consistent with the order; retry reconciliation")
        return count, notional, fees

    def _apply_snapshot(self, row, strategy_name):
        parsed = self._parse_broker_order(row, strategy_name)
        filled, _, initial = self._counts(row)
        tracked = self.get_tracked_order(parsed.identifier)
        if tracked is None:
            tracked = self._pending_submissions.pop(row.get("client_order_id"), None)
            if tracked is not None:
                tracked.identifier = parsed.identifier
                tracked.update_raw(row)
                tracked._kalshi_fill_count = Decimal(0)
                tracked._kalshi_fill_notional = Decimal(0)
                self._process_trade_event(tracked, self.NEW_ORDER)
        if tracked is None:
            # Existing account history is imported without announcing historical
            # fills as new executions to today's strategy.
            tracked = parsed
            parsed_status = parsed.status
            count, notional, fees = self._fill_totals(row) if filled else (Decimal(0), Decimal(0), Decimal(0))
            self._process_new_order(tracked)
            if filled:
                tracked.add_transaction(float(notional / count), count)
                tracked._avg_fill_price = float(notional / count)
            tracked._kalshi_fill_count, tracked._kalshi_fill_notional = count, notional
            tracked.trade_cost = float(fees)
            self._new_orders.remove(tracked.identifier, key="identifier")
            tracked.status = parsed_status
            if row.get("status") == "executed":
                tracked.set_filled()
                self._filled_orders.append(tracked)
            elif row.get("status") == "canceled":
                tracked.set_canceled()
                self._canceled_orders.append(tracked)
            elif row.get("status") == "rejected":
                tracked.set_error(KalshiAPIError("Kalshi rejected the order"))
                self._error_orders.append(tracked)
            elif filled:
                tracked.set_partially_filled()
                self._partially_filled_orders.append(tracked)
            else:
                self._new_orders.append(tracked)
            self._invalidate_order_caches()
            return tracked
        old_count = getattr(tracked, "_kalshi_fill_count", Decimal(0))
        if filled < old_count:
            return tracked  # Stale REST/WS snapshot; never roll back fills.
        tracked.quantity = initial
        tracked.limit_price = parsed.limit_price
        tracked.update_raw(row)
        if filled > old_count:
            count, notional, fees = self._fill_totals(row)
            if count > initial:
                raise KalshiAPIError("Kalshi fills exceed order size")
            delta = count - old_count
            price = (notional - getattr(tracked, "_kalshi_fill_notional", Decimal(0))) / delta
            event = self.FILLED_ORDER if count == initial else self.PARTIALLY_FILLED_ORDER
            # The generic average-price setter rounds to cents. Kalshi supports
            # sub-cent prices; retain the provider's exact average on this Order.
            tracked._avg_fill_price = float(notional / count)
            tracked.trade_cost = float(fees)
            tracked._kalshi_fill_count, tracked._kalshi_fill_notional = count, notional
            self._process_trade_event(tracked, event, price=price, filled_quantity=delta)
            self.sync_positions(tracked.strategy)
        terminal = getattr(tracked, "_kalshi_terminal_event", None)
        if (
            row.get("status") == "canceled"
            and not tracked.is_filled()
            and tracked.status != self.CANCELED_ORDER
            and terminal != self.CANCELED_ORDER
        ):
            tracked._kalshi_terminal_event = self.CANCELED_ORDER
            self._process_trade_event(tracked, self.CANCELED_ORDER)
        elif row.get("status") == "rejected" and tracked.status != self.ERROR_ORDER and terminal != self.ERROR_ORDER:
            tracked._kalshi_terminal_event = self.ERROR_ORDER
            self._process_trade_event(tracked, self.ERROR_ORDER, error=KalshiAPIError("Kalshi rejected the order"))
        elif parsed.status == Order.OrderStatus.UNKNOWN:
            tracked.status = Order.OrderStatus.UNKNOWN
            self.logger.warning("Kalshi returned an unknown order status")
        self._invalidate_order_caches()
        return tracked

    def _reconcile_order(self, identifier):
        with self._reconcile_lock:
            row = self._pull_broker_order(identifier)
            if row is None:
                return None
            tracked = self.get_tracked_order(identifier)
            name = tracked.strategy if tracked is not None else self._strategy_name
            if not name:
                return None
            return self._apply_snapshot(row, name)

    def sync_orders(self, strategy):
        """Override the existing sync hook so REST repair emits fill deltas."""
        name = self._strategy_name_from_input(strategy)
        with self._reconcile_lock:
            rows = self._pull_broker_all_orders()
            identifiers = set()
            for row in rows:
                identifiers.add(str(row["order_id"]))
                tracked = self.get_tracked_order(str(row["order_id"]))
                self._apply_snapshot(row, tracked.strategy if tracked else name)
            for order in list(self.get_active_tracked_orders(name)):
                if order.identifier not in identifiers:
                    self._reconcile_order(order.identifier)

    def _get_stream_object(self):
        return KalshiStream(self)

    def _register_stream_events(self):
        @self.stream.add_action(KalshiStream.UPDATE)
        def update(identifier):
            self._reconcile_order(identifier)

    def _run_stream(self):
        self.stream.run(self.name)

    def cleanup_streams(self):
        super().cleanup_streams()
        if getattr(self, "_owns_client", False):
            self._client.close()
