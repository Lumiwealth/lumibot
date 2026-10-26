"""Kalshi selected-contract trading through the existing LumiBot broker hooks."""

import uuid
from datetime import datetime, timedelta
from datetime import time as day_time
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal
from threading import RLock
from time import monotonic

from zoneinfo import ZoneInfo

from lumibot.brokers.broker import Broker
from lumibot.data_sources.kalshi_data import KalshiData
from lumibot.entities import Asset, Order, Position
from lumibot.tools.kalshi_client import KalshiAPIError, KalshiClient, decimal_value
from lumibot.trading_builtins.custom_stream import PollingStream


def _parse_balances(row):
    """Parse cash, holdings, total from the currently observed API contract.

    Authenticated demo evidence on 2026-10-26 reports portfolio_value=0 for
    an independently verified funded-flat account. It is holdings value, not
    cash-inclusive equity; the older changelog describes a different meaning.
    Cash preserves fixed-point precision; holdings use the cent-valued field.
    """
    if not isinstance(row, dict):
        raise KalshiAPIError("Kalshi returned an invalid balance response")
    try:
        cash = (
            decimal_value(row["balance_dollars"], "balance")
            if row.get("balance_dollars") is not None
            else decimal_value(row["balance"], "balance") / Decimal(100)
        )
        holdings = decimal_value(row["portfolio_value"], "portfolio_value") / Decimal(100)
    except (KeyError, TypeError, ValueError):
        raise KalshiAPIError("Kalshi balance response is missing valid cash or portfolio value") from None
    return cash, holdings, cash + holdings


class Kalshi(Broker):
    """USD prediction markets with explicit YES/NO asset identity.

    Orders retain their selected outcome; the venue nets YES against NO.
    REST polling uses LumiBot's existing dispatcher and lifecycle events.
    """

    NAME = "Kalshi"

    def __init__(
        self, config=None, data_source=None, *, client=None, connect_stream=True, max_workers=4, polling_interval=5
    ):
        config = config or {}
        configured = config.get("CLIENT", config.get("KALSHI_CLIENT"))
        self._client = client if client is not None else configured
        self._owns_client = self._client is None
        if self._owns_client:
            self._client = KalshiClient(config, require_auth=True)
        # Omission preserves a restricted key's own subaccount binding. An
        # explicitly selected zero remains zero; do not substitute a default.
        self._account_params = {}
        for key, field in (("SUBACCOUNT", "subaccount"), ("EXCHANGE_INDEX", "exchange_index")):
            value = config.get(key, config.get("KALSHI_" + key))
            if value is not None:
                self._account_params[field] = value
        self.polling_interval = float(polling_interval)
        self._reconcile_lock = RLock()
        self._pending_submissions = {}
        self._balance_lock = RLock()
        self._balance_snapshot = None
        self._balance_read_at = None
        self.balance_updated_ts = None
        self._closed = False
        self._owns_data_source = data_source is None
        source = data_source if data_source is not None else KalshiData(client=self._client)
        self._kalshi_data = source if isinstance(source, KalshiData) else KalshiData(client=self._client)
        super().__init__(
            name=self.NAME,
            data_source=source,
            config={"MARKET": "24/7"},
            connect_stream=connect_stream,
            start_orders_thread=False,
            max_workers=max_workers,
        )

    def _get_balances_at_broker(self, quote_asset, strategy):
        if quote_asset is not None and (quote_asset.symbol != "USD" or quote_asset.asset_type != "forex"):
            raise ValueError("Kalshi account balances are denominated in USD")
        # The balance endpoint has a separate 1/s Basic/Advanced bucket. Share
        # consecutive cash/equity reads; failures never populate this cache.
        with self._balance_lock:
            if self._balance_read_at is None or monotonic() - self._balance_read_at >= 1:
                row = self._client.request("GET", "/portfolio/balance", params=dict(self._account_params))
                parsed = _parse_balances(row)
                self.balance_updated_ts = row.get("updated_ts")
                self._balance_snapshot = tuple(float(value) for value in parsed)
                self._balance_read_at = monotonic()
            return self._balance_snapshot

    def close(self):
        if self._closed:
            return
        self.cleanup_streams()
        if self._owns_data_source:
            self.data_source.shutdown()
        if self._kalshi_data is not self.data_source:
            self._kalshi_data.shutdown()
        if self._owns_client:
            self._client.close()
        self._closed = True

    def _invalidate_balance(self):
        with self._balance_lock:
            self._balance_read_at = None

    def _get_stream_object(self):
        return PollingStream(polling_interval=self.polling_interval)

    def _register_stream_events(self):
        @self.stream.add_action(PollingStream.POLL_EVENT)
        def poll():
            if not self._strategy_name:
                return
            for synchronize in (self.sync_orders, self.sync_positions):
                try:
                    synchronize(self._strategy_name)
                except Exception:
                    self.logger.warning("Kalshi reconciliation incomplete; retrying on the next poll")

    def _run_stream(self):
        self._stream_established()
        self.stream.run(self.name)

    @staticmethod
    def _state(order):
        if order.custom_params is None:
            order.custom_params = {}
        # The existing Order serializer persists custom_params. Decimal strings
        # retain checkpoints without a second persistence owner or cent rounding.
        return order.custom_params.setdefault("_kalshi", {})

    def _route(self, market=None):
        route = dict(self._account_params)
        if market is not None and market.get("exchange_index") is not None:
            index = market["exchange_index"]
            if "exchange_index" in route and int(route["exchange_index"]) != int(index):
                raise ValueError("Kalshi market belongs to a different exchange_index than the selected fund pool")
            route["exchange_index"] = index
        return route

    def _order_payload(self, order):
        self._kalshi_data._validate_asset(order.asset, order.quote, order.exchange)
        if order.order_class not in (None, "", Order.OrderClass.SIMPLE) or order.child_orders:
            raise ValueError("Kalshi has no native bracket, OCO, OTO or multileg order for this endpoint")
        if order.order_type not in (Order.OrderType.LIMIT, Order.OrderType.MARKET):
            raise ValueError("Kalshi has no native stop or trailing order for this endpoint")
        buy = order.side == Order.OrderSide.BUY
        if order.side not in (Order.OrderSide.BUY, Order.OrderSide.SELL):
            raise ValueError("Kalshi uses the normal buy/sell side on a selected YES/NO Asset")
        quantity = decimal_value(order.quantity, "quantity")
        if quantity <= 0 or quantity % Decimal("0.01"):
            raise ValueError("Kalshi quantity must be positive in increments of 0.01 contracts")
        market = self._kalshi_data._market(order.asset)
        if market.get("status") not in ("active", "open"):
            raise ValueError("Kalshi market is not open for trading")
        ranges = []
        for band in market.get("price_ranges", []):
            start, end, step = (decimal_value(band[key], "price range") for key in ("start", "end", "step"))
            if step <= 0 or start > end:
                raise KalshiAPIError("Kalshi returned an invalid price range")
            ranges.append((start, end, step))
        if not ranges:
            raise KalshiAPIError("Kalshi market is missing its published price grid")
        side = "bid" if buy == (order.asset.outcome == "yes") else "ask"
        tif = str(order.time_in_force).lower()
        if order.order_type == Order.OrderType.MARKET:
            # Kalshi has no unpriced order: cross all valid grid levels with an
            # IOC limit, without representing an unfilled remainder as filled.
            ticks = []
            for start, end, step in ranges:
                lower = (
                    start
                    + max(0, ((Decimal("0.0001") - start) / step).to_integral_value(rounding=ROUND_CEILING)) * step
                )
                upper = (
                    start
                    + ((min(end, Decimal("0.9999")) - start) / step).to_integral_value(rounding=ROUND_FLOOR) * step
                )
                if 0 < lower <= upper < 1:
                    ticks.extend((lower, upper))
            if not ticks:
                raise KalshiAPIError("Kalshi market has no executable prediction-price grid")
            price = max(ticks) if side == "bid" else min(ticks)
            tif = "ioc"
        else:
            selected = decimal_value(order.limit_price, "limit price")
            price = selected if order.asset.outcome == "yes" else Decimal(1) - selected
            if not 0 < price < 1 or price % Decimal("0.0001"):
                raise ValueError("Kalshi price must be between zero and one USD with at most four decimal places")
            if not any(start <= price <= end and (price - start) % step == 0 for start, end, step in ranges):
                raise ValueError("Kalshi limit price does not match the market's published price grid")
        time_in_force = {
            "gtc": "good_till_canceled",
            "gtd": "good_till_canceled",
            "day": "good_till_canceled",
            "ioc": "immediate_or_cancel",
            "fok": "fill_or_kill",
        }.get(tif)
        if time_in_force is None:
            raise ValueError("Kalshi time_in_force must be day, gtc, gtd, ioc or fok")
        payload = {
            "ticker": order.asset.symbol,
            "side": side,
            "price": f"{price:.4f}",
            "count": f"{quantity:.2f}",
            "time_in_force": time_in_force,
            "self_trade_prevention_type": "taker_at_cross",
            **self._route(market),
        }
        if tif == "gtd":
            expiration = order.good_till_date
            if (
                not isinstance(expiration, datetime)
                or expiration.tzinfo is None
                or expiration.timestamp() <= datetime.now().timestamp()
            ):
                raise ValueError("Kalshi GTD requires a future timezone-aware good_till_date")
            payload["expiration_time"] = int(expiration.timestamp())
        elif tif == "day":
            now = datetime.now(ZoneInfo("America/New_York"))
            payload["expiration_time"] = int(
                datetime.combine(now.date() + timedelta(days=1), day_time(), now.tzinfo).timestamp()
            )
        elif order.good_till_date is not None:
            raise ValueError("Kalshi good_till_date requires time_in_force='gtd'")
        options = {key: value for key, value in (order.custom_params or {}).items() if key != "_kalshi"}
        protected = (set(payload) - {"self_trade_prevention_type"}) | {
            "client_order_id",
            "subaccount",
            "exchange_index",
            "expiration_time",
        }
        if protected & options.keys():
            raise ValueError(
                "Kalshi custom_params cannot override order identity, price, quantity, time or account routing"
            )
        if options.get("reduce_only") and tif != "ioc":
            raise ValueError("Kalshi reduce_only requires time_in_force='ioc'")
        payload.update(options)
        return payload

    def _submit_order(self, order):
        with self._reconcile_lock:
            state = self._state(order)
            if state.get("client_order_id") or order.was_transmitted():
                # Never replay a possibly accepted write, including a restored
                # Order whose _transmitted flag was intentionally not serialized.
                raise ValueError("Kalshi order has already been attempted; reconcile it before submitting a new order")
            payload = self._order_payload(order)
            client_id = str(uuid.uuid4())
            state.update(client_order_id=client_id, fill_count="0", fill_notional="0")
            payload["client_order_id"] = client_id
            self._pending_submissions[client_id] = order
            try:
                response = self._client.request("POST", "/portfolio/events/orders", json=payload)
                if not response.get("order_id"):
                    raise KalshiAPIError(
                        "Kalshi submission response is missing the order identifier; reconcile client_order_id"
                    )
            except KalshiAPIError as exc:
                definite = (
                    exc.status_code is not None and 400 <= exc.status_code < 500 and exc.status_code not in (408, 409)
                )
                definite = definite or (
                    exc.status_code == 409 and exc.error_code == KalshiAPIError.FOK_INSUFFICIENT_VOLUME
                )
                if definite:
                    self._pending_submissions.pop(client_id, None)
                    self._process_trade_event(order, self.ERROR_ORDER, error=exc)
                else:
                    order.status = Order.OrderStatus.UNKNOWN
                    order.error_message = (
                        "Kalshi submission outcome unknown; reconcile client_order_id before a new order"
                    )
                raise
            order.identifier = str(response["order_id"])
            order.update_raw(response)
            self._pending_submissions.pop(client_id, None)
            # The shared sync window may hold NEW while priority fills continue.
            # Register the original object before either path can observe it.
            self._unprocessed_orders.append(order)
            self._process_trade_event(order, self.NEW_ORDER)
            self._invalidate_order_caches()
            self._invalidate_balance()
        self._refresh_after_mutation(order)
        return order

    def _refresh_after_mutation(self, order):
        try:
            self._pull_order(order.identifier, order.strategy)
        except Exception:
            self.logger.warning("Kalshi accepted the mutation; follow-up status refresh deferred to reconciliation")

    def cancel_order(self, order):
        with self._reconcile_lock:
            market = self._kalshi_data._market(order.asset)
            self._client.request(
                "DELETE",
                f"/portfolio/events/orders/{KalshiClient.path_id(order.identifier)}",
                params={"market_ticker": order.asset.symbol, **self._route(market)},
            )
            self._invalidate_balance()
        self._refresh_after_mutation(order)
        return order

    def _modify_order(self, order, limit_price=None, stop_price=None):
        if stop_price is not None or limit_price is None:
            raise ValueError("Kalshi modify_order supports limit_price; native stop orders are unavailable")
        with self._reconcile_lock:
            row = self._pull_broker_order(order.identifier)
            state = self._state(order)
            if row is not None:
                self._apply_snapshot(order, row)
            elif not order.was_transmitted() or not state.get("client_order_id"):
                raise KalshiAPIError("Kalshi order was not found for modification")
            if not order.is_active():
                raise ValueError("Kalshi order is no longer active")
            candidate = Order(
                order.strategy,
                order.asset,
                order.quantity,
                order.side,
                limit_price=limit_price,
                time_in_force=order.time_in_force,
                good_till_date=order.good_till_date,
            )
            validated = self._order_payload(candidate)
            payload = {key: validated[key] for key in ("ticker", "side", "price", "count")}
            if "exchange_index" in validated:
                payload["exchange_index"] = validated["exchange_index"]
            updated_id = str(uuid.uuid4())
            payload.update(client_order_id=state["client_order_id"], updated_client_order_id=updated_id)
            response = self._client.request(
                "POST",
                f"/portfolio/events/orders/{KalshiClient.path_id(order.identifier)}/amend",
                params={key: value for key, value in self._account_params.items() if key == "subaccount"},
                json=payload,
            )
            # Amend may change a provider identifier. Preserve the same selected
            # Asset, action, Order object, and cumulative fill checkpoint.
            if response.get("order_id") and str(response["order_id"]) != str(order.identifier):
                raise KalshiAPIError(
                    "Kalshi amend returned a different order identifier; reconcile before further mutation"
                )
            order.limit_price = limit_price
            self._state(order)["client_order_id"] = updated_id
            self._invalidate_balance()
            self._process_trade_event(order, self.MODIFIED_ORDER)
        self._refresh_after_mutation(order)
        return order

    def _pull_positions(self, strategy):
        name = self._strategy_name_from_input(strategy)
        rows = self._client.pages(
            "/portfolio/positions",
            "market_positions",
            params={**self._account_params, "limit": 1000, "count_filter": "position"},
        )
        positions = []
        for row in rows:
            quantity = decimal_value(row.get("position_fp", row.get("position")), "position")
            if not quantity:
                continue
            asset = Asset(
                row["ticker"], "prediction_contract", outcome="yes" if quantity > 0 else "no", precision="0.01"
            )
            exposure = row.get("market_exposure_dollars")
            avg = decimal_value(exposure, "market exposure") / abs(quantity) if exposure is not None else None
            position = Position(name, asset, abs(quantity), avg_fill_price=float(avg) if avg is not None else None)
            # Expose the real net holding, never a simultaneous synthetic pair.
            position._raw = row
            positions.append(position)
        return positions

    def _pull_position(self, strategy, asset):
        self._kalshi_data._validate_asset(asset)
        return next((p for p in self._pull_positions(strategy) if p.asset == asset), None)

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

    @staticmethod
    def _book_side(row):
        side = row.get("book_side")
        if side is None and row.get("outcome_side") in ("yes", "no"):
            side = "bid" if row["outcome_side"] == "yes" else "ask"
        if side is None and row.get("side") in ("yes", "no") and row.get("action") in ("buy", "sell"):
            side = "bid" if (row["side"] == "yes") == (row["action"] == "buy") else "ask"
        if side not in ("bid", "ask"):
            raise KalshiAPIError("Kalshi order has an unknown direction")
        return side

    def _parse_broker_order(self, response, strategy_name, strategy_object=None):
        filled, remaining, initial = self._counts(response)
        status = {
            "resting": Order.OrderStatus.PARTIALLY_FILLED if filled else Order.OrderStatus.NEW,
            "executed": Order.OrderStatus.FILLED if filled == initial else Order.OrderStatus.CANCELED,
            "canceled": Order.OrderStatus.CANCELED,
            "rejected": Order.OrderStatus.ERROR,
        }.get(response.get("status"), Order.OrderStatus.UNKNOWN)
        tracked = self.get_tracked_order(str(response["order_id"])) or self._pending_submissions.get(
            response.get("client_order_id")
        )
        asset = (
            tracked.asset
            if tracked
            else Asset(response["ticker"], "prediction_contract", outcome="yes", precision="0.01")
        )
        yes_price = decimal_value(response["yes_price_dollars"], "order price")
        selected_price = yes_price if asset.outcome == "yes" else Decimal(1) - yes_price
        order = Order(
            strategy_name,
            asset,
            initial,
            tracked.side if tracked else ("buy" if self._book_side(response) == "bid" else "sell"),
            order_type=tracked.order_type if tracked else "limit",
            limit_price=float(selected_price),
            identifier=str(response["order_id"]),
            status=status,
            time_in_force=tracked.time_in_force if tracked else "gtc",
        )
        order.update_raw(response)
        order.broker_create_date = response.get("created_time")
        order.broker_update_date = response.get("last_update_time")
        return order

    def _pull_broker_all_orders(self):
        return list(self._client.pages("/portfolio/orders", "orders", params={**self._account_params, "limit": 1000}))

    def _parse_broker_orders(self, broker_orders, strategy_name, strategy_object=None):
        # An uncertain write may already be terminal when first visible. Link
        # our client ID before the shared sync intentionally skips unknown
        # terminal account history; otherwise an owned fill could be lost.
        for row in broker_orders or []:
            if row.get("client_order_id") in self._pending_submissions:
                self._process_broker_synced_order(self._parse_broker_order(row, strategy_name))
        return super()._parse_broker_orders(broker_orders, strategy_name, strategy_object)

    def _pull_broker_order(self, identifier):
        try:
            row = self._client.request(
                "GET", f"/portfolio/orders/{KalshiClient.path_id(identifier)}", params=dict(self._account_params)
            )["order"]
        except KalshiAPIError as exc:
            if exc.status_code != 404:
                raise
            row = next(
                (
                    row
                    for row in self._client.pages(
                        "/historical/orders", "orders", params={**self._account_params, "limit": 1000}
                    )
                    if str(row.get("order_id")) == str(identifier)
                ),
                None,
            )
        if row is not None:
            if str(row.get("order_id")) != str(identifier):
                raise KalshiAPIError("Kalshi direct lookup returned a different order")
            selected = self._account_params.get("subaccount")
            if selected is not None and int(row.get("subaccount_number", 0)) != int(selected):
                return None
        return row

    def _fill_totals(self, row, outcome):
        fills = list(
            self._client.pages(
                "/portfolio/fills", "fills", params={**self._account_params, "order_id": row["order_id"], "limit": 1000}
            )
        )
        expected, _, _ = self._counts(row)
        if sum(decimal_value(f.get("count_fp", f.get("count", 0)), "fill count") for f in fills) < expected:
            fills.extend(
                f
                for f in self._client.pages(
                    "/historical/fills",
                    "fills",
                    params={**self._account_params, "ticker": row["ticker"], "limit": 1000},
                )
                if str(f.get("order_id")) == str(row["order_id"])
            )
        unique = {}
        for fill in fills:
            if str(fill.get("order_id")) != str(row["order_id"]) or fill.get("ticker") != row["ticker"]:
                raise KalshiAPIError("Kalshi fill did not match the requested order and market")
            key = fill.get("fill_id", fill.get("trade_id"))
            if not key:
                raise KalshiAPIError("Kalshi fill is missing its identifier")
            if key in unique and unique[key] != fill:
                raise KalshiAPIError("Kalshi returned conflicting duplicate fills")
            unique[key] = fill
        count, notional, fees = Decimal(0), Decimal(0), Decimal(0)
        for fill in unique.values():
            quantity = decimal_value(fill.get("count_fp", fill.get("count")), "fill count")
            yes_price = decimal_value(fill["yes_price_dollars"], "fill price")
            if quantity <= 0 or not 0 <= yes_price <= 1:
                raise KalshiAPIError("Kalshi returned an invalid fill")
            count += quantity
            notional += quantity * (yes_price if outcome == "yes" else Decimal(1) - yes_price)
            fees += decimal_value(fill.get("fee_cost", 0), "fill fee")
        if count != expected:
            raise KalshiAPIError("Kalshi fills and order snapshot are not yet consistent; retry reconciliation")
        return count, notional, fees

    def _process_broker_synced_order(self, order):
        pending = self._pending_submissions.pop(order._raw.get("client_order_id"), None)
        if pending is not None:
            pending.identifier = order.identifier
            pending.update_raw(order._raw)
            self._unprocessed_orders.append(pending)
            self._process_trade_event(pending, self.NEW_ORDER)
            return self._apply_snapshot(pending, order._raw)
        # Import existing account history without replaying historical fills.
        status = order.status
        count, notional, fees = (
            self._fill_totals(order._raw, order.asset.outcome)
            if self._counts(order._raw)[0]
            else (Decimal(0), Decimal(0), Decimal(0))
        )
        self._state(order).update(
            client_order_id=order._raw.get("client_order_id"), fill_count=str(count), fill_notional=str(notional)
        )
        order.trade_cost = float(fees)
        if count:
            order.avg_fill_price = float(notional / count)
        self._process_new_order(order)
        self._new_orders.remove(order.identifier, key="identifier")
        order.status = status
        bucket = {
            Order.OrderStatus.FILLED: self._filled_orders,
            Order.OrderStatus.CANCELED: self._canceled_orders,
            Order.OrderStatus.ERROR: self._error_orders,
            Order.OrderStatus.PARTIALLY_FILLED: self._partially_filled_orders,
        }.get(status, self._new_orders)
        bucket.append(order)
        self._invalidate_order_caches()
        return order

    def _apply_snapshot(self, tracked, row):
        parsed = self._parse_broker_order(row, tracked.strategy)
        if parsed.asset.symbol != tracked.asset.symbol:
            raise KalshiAPIError("Kalshi order snapshot changed market identity")
        state = self._state(tracked)
        filled, _, initial = self._counts(row)
        old_count = decimal_value(state.get("fill_count", 0), "fill checkpoint")
        if filled < old_count:
            return tracked
        if (
            tracked.status in (self.FILLED_ORDER, self.CANCELED_ORDER, self.ERROR_ORDER) or state.get("terminal_event")
        ) and parsed.is_active():
            return tracked
        tracked.quantity = initial
        if tracked.order_type == Order.OrderType.LIMIT:
            tracked.limit_price = parsed.limit_price
        tracked.update_raw(row)
        if row.get("client_order_id"):
            state["client_order_id"] = row["client_order_id"]
        tracked.broker_update_date = row.get("last_update_time")
        if filled > old_count:
            count, notional, fees = self._fill_totals(row, tracked.asset.outcome)
            delta = count - old_count
            price = (notional - decimal_value(state.get("fill_notional", 0))) / delta
            # Refresh net inventory BEFORE queueing the standard fill callback.
            # A buy-NO execution can reduce YES without creating a NO holding.
            self.sync_positions(tracked.strategy)
            self._invalidate_balance()
            tracked.avg_fill_price = float(notional / count)
            tracked.trade_cost = float(fees)
            state.update(fill_count=str(count), fill_notional=str(notional))
            event = self.FILLED_ORDER if count == initial else self.PARTIALLY_FILLED_ORDER
            self._process_trade_event(tracked, event, price=price, filled_quantity=delta)
        terminal = state.get("terminal_event")
        if (
            parsed.status == self.CANCELED_ORDER
            and not tracked.is_filled()
            and tracked.status != self.CANCELED_ORDER
            and terminal != self.CANCELED_ORDER
        ):
            state["terminal_event"] = self.CANCELED_ORDER
            self._process_trade_event(tracked, self.CANCELED_ORDER)
        elif parsed.status == self.ERROR_ORDER and tracked.status != self.ERROR_ORDER and terminal != self.ERROR_ORDER:
            state["terminal_event"] = self.ERROR_ORDER
            self._process_trade_event(tracked, self.ERROR_ORDER, error=KalshiAPIError("Kalshi rejected the order"))
        elif parsed.status == Order.OrderStatus.UNKNOWN:
            tracked.status = Order.OrderStatus.UNKNOWN
            self.logger.warning("Kalshi returned an unknown order state; reconciliation remains incomplete")
        self._invalidate_order_caches()
        return tracked

    def _process_new_order(self, order):
        existing = self.get_tracked_order(order.identifier)
        if existing is not None and existing.status in (self.FILLED_ORDER, self.CANCELED_ORDER, self.ERROR_ORDER):
            # A queued NEW must not announce or reopen an order after its
            # priority fill/terminal event has already been processed.
            return None
        return super()._process_new_order(order)

    def _process_filled_order(self, order, price, quantity):
        super()._process_filled_order(order, price, quantity)
        return self.get_tracked_position(order.strategy, order.asset) or Position(
            order.strategy, order.asset, 0, orders=[order]
        )

    def _process_partially_filled_order(self, order, price, quantity):
        super()._process_partially_filled_order(order, price, quantity)
        return order, self.get_tracked_position(order.strategy, order.asset) or Position(
            order.strategy, order.asset, 0, orders=[order]
        )

    def _apply_direct_order_observation(self, order_lumi, matched_order):
        with self._reconcile_lock:
            before = (order_lumi.status, self._state(order_lumi).get("fill_count"), order_lumi.limit_price)
            self._apply_snapshot(order_lumi, matched_order._raw)
            return before != (order_lumi.status, self._state(order_lumi).get("fill_count"), order_lumi.limit_price)

    def _pull_order(self, identifier, strategy_name):
        with self._reconcile_lock:
            tracked = self.get_tracked_order(str(identifier))
            if tracked is not None and tracked.strategy != strategy_name:
                return None
            row = self._pull_broker_order(identifier)
            if row is None:
                return None
            if tracked is not None:
                return self._apply_snapshot(tracked, row)
            return self._process_broker_synced_order(self._parse_broker_order(row, strategy_name))

    def sync_orders(self, strategy):
        with self._reconcile_lock:
            return super().sync_orders(strategy)

    def _refresh_missing_active_order_from_broker(self, order_lumi, strategy_name, **kwargs):
        # The V2 write acknowledgement can lead both live and historical GETs.
        # A transient 404 never proves that an IOC/market order was canceled.
        kwargs["terminalize_missing"] = False
        return super()._refresh_missing_active_order_from_broker(order_lumi, strategy_name, **kwargs)

    def get_historical_account_value(self):
        raise NotImplementedError("Kalshi historical account values are not implemented yet")
