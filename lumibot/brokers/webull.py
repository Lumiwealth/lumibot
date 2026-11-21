"""Webull's Individual OpenAPI trading adapter."""
from datetime import date
from threading import Lock, Thread
from uuid import uuid4

from lumibot.brokers.broker import Broker
from lumibot.entities import Asset, Order, Position
from lumibot.tools.helpers import get_multileg_quantity_units
from lumibot.tools.webull_helpers import (
    WebullAPIError,
    make_client,
    paper_mode,
    positive_value,
    request_json,
    require_number,
)
from lumibot.trading_builtins import PollingStream

_TYPES = {
    Order.OrderType.MARKET: "MARKET",
    Order.OrderType.LIMIT: "LIMIT",
    Order.OrderType.STOP: "STOP_LOSS",
    Order.OrderType.STOP_LIMIT: "STOP_LOSS_LIMIT",
    Order.OrderType.TRAIL: "TRAILING_STOP_LOSS",
}
_SIDES = {
    Order.OrderSide.BUY: "BUY",
    Order.OrderSide.SELL: "SELL",
    Order.OrderSide.SELL_SHORT: "SELL_SHORT",
    Order.OrderSide.BUY_TO_COVER: "BUY_TO_COVER",
    Order.OrderSide.BUY_TO_OPEN: "BUY",
    Order.OrderSide.BUY_TO_CLOSE: "BUY",
    Order.OrderSide.SELL_TO_OPEN: "SELL",
    Order.OrderSide.SELL_TO_CLOSE: "SELL",
}
_STATUS = {
    "PENDING": "submitted", "SUBMITTING": "submitted", "SUBMITTED": "submitted",
    "WORKING": "open", "OPEN": "open", "FILLED": "fill", "FINAL_FILLED": "fill",
    "PARTIALLY_FILLED": "partial_fill", "PARTIAL_FILLED": "partial_fill",
    "CANCELLING": "cancelling", "CANCELING": "cancelling", "PENDING_CANCEL": "cancelling",
    "CANCELLED": "canceled", "CANCELED": "canceled", "CANCEL": "canceled",
    "FAILED": "error", "REJECTED": "error", "EXPIRED": "expired",
}


class Webull(Broker):
    """Connect one explicitly selected Webull account to a LumiBot strategy."""

    def __init__(self, config, data_source=None, connect_stream=True, *,
                 trading_client=None, start_orders_thread=True, option_source=None, **kwargs):
        self._webull_config = dict(config)
        self.is_paper = paper_mode(self._webull_config.get("PAPER"))
        self.account_id = str(self._webull_config.get("ACCOUNT_ID") or "").strip()
        if not self.account_id:
            raise ValueError("Select a Webull account using ACCOUNT_ID")
        self._client = trading_client
        self._client_lock = Lock()
        self._account_verified = False
        self._account_class = None
        self._events = None
        self._events_thread = None
        self.order_event_state = "disabled"
        self._poll_offset = 0
        from lumibot.data_sources import WebullData

        self._external_data_source = data_source
        self._webull_data = data_source if isinstance(data_source, WebullData) else WebullData(config)
        super().__init__("Webull", connect_stream=connect_stream, data_source=data_source or self._webull_data,
                         option_source=option_source, start_orders_thread=start_orders_thread, **kwargs)

    @property
    def trading_client(self):
        with self._client_lock:
            if self._client is None:
                self._client = make_client(self._webull_config, trading=True)
            return self._client

    def _selected_account(self):
        client = self.trading_client
        with self._client_lock:
            if not self._account_verified:
                accounts = request_json("account discovery", client.account_v2.get_account_list)
                matches = [row for row in accounts if row.get("account_id") == self.account_id]
                if len(matches) != 1:
                    raise ValueError("The selected Webull account was not found in this environment")
                self._account_class = matches[0].get("account_class")
                self._account_verified = True
        return self.account_id

    def _get_balances_at_broker(self, quote_asset, strategy):
        balance = request_json("balance read", self.trading_client.account_v2.get_account_balance,
                               self._selected_account())
        if quote_asset is not None and balance.get("total_asset_currency") != quote_asset.symbol:
            raise ValueError("Webull balance currency does not match the requested quote asset")
        cash = require_number(balance.get("total_cash_balance"), "total_cash_balance")
        equity = require_number(balance.get("total_net_liquidation_value"), "total_net_liquidation_value")
        market_value = balance.get("total_market_value")
        if self._account_class == "FUTURES" and market_value in (None, ""):
            # Like the existing futures brokers, carry the mark-to-market
            # contribution to equity, not the gross notional exposure.
            positions = equity - cash
        else:
            positions = require_number(market_value, "total_market_value")
        return float(cash), float(positions), float(equity)

    def get_buying_power(self, currency="USD"):
        """Read Webull buying power separately from the actual cash and equity fields."""
        balance = request_json("balance read", self.trading_client.account_v2.get_account_balance,
                               self._selected_account())
        row = next((row for row in balance["account_currency_assets"] if row["currency"] == currency), None)
        if row is None:
            raise ValueError("The Webull account did not report buying power in the requested currency")
        return require_number(row.get("buying_power"), "buying power")

    def get_historical_account_value(self):
        raise NotImplementedError("Webull Individual OpenAPI has no verified historical account value endpoint")

    def _pull_positions(self, strategy):
        rows = request_json("position read", self.trading_client.account_v2.get_account_position,
                            self._selected_account())
        name = self._strategy_name_from_input(strategy)
        return [Position(name, self._read_asset(row), require_number(row["quantity"], "position quantity"))
                for row in rows]

    def _pull_position(self, strategy, asset):
        return next((position for position in self._pull_positions(strategy) if position.asset == asset), None)

    def _read_asset(self, row):
        instrument = row.get("instrument_type")
        if instrument == "EQUITY":
            return Asset(row["symbol"])
        if instrument == "OPTION":
            leg = row.get("legs") or [row]
            if len(leg) != 1:
                raise ValueError("A single option needs exactly one provider leg")
            item = leg[0]
            expiration = item["option_expire_date"]
            strike = item.get("strike_price") or item.get("option_exercise_price")
            return Asset(row["symbol"], asset_type="option", expiration=date.fromisoformat(expiration),
                         strike=float(require_number(strike, "option strike")), right=item["option_type"],
                         multiplier=float(require_number(item["option_contract_multiplier"], "option multiplier")))
        if instrument == "FUTURES":
            terms = self._webull_data.futures_contract(row["symbol"])
            return Asset(row["symbol"], asset_type="future", multiplier=terms["size"])
        if instrument == "CRYPTO":
            currency = row.get("currency", "USD")
            symbol = row["symbol"]
            return Asset(symbol.removesuffix(currency), asset_type="crypto")
        if instrument == "EVENT":
            return Asset(row["symbol"], asset_type="prediction_contract", outcome=row["event_outcome"].lower())
        raise ValueError("Webull returned an unmapped instrument type")

    @staticmethod
    def _option_leg(order, quantity=None):
        asset = order.asset
        leg = dict(side=_SIDES[order.side], quantity=positive_value(
            order.quantity if quantity is None else quantity, "leg quantity"),
            symbol=asset.symbol, market="US", instrument_type="EQUITY")
        if asset.asset_type == Asset.AssetType.OPTION:
            if not asset.right or asset.expiration is None:
                raise ValueError("Webull options require a right and expiry")
            leg.update(instrument_type="OPTION", option_type=str(asset.right).upper(),
                       option_expire_date=asset.expiration.isoformat(),
                       strike_price=positive_value(asset.strike, "option strike"))
        return leg

    def _order_row(self, order, *, combo_type="NORMAL", equity_session=None):
        asset_type = order.asset.asset_type
        wire_type = {"stock": "EQUITY", "option": "OPTION", "future": "FUTURES", "crypto": "CRYPTO",
                     "prediction_contract": "EVENT"}.get(asset_type)
        if wire_type is None:
            raise ValueError("Webull requires a supported, tradeable instrument")
        params = order.custom_params or {}
        session = equity_session or str(params.get("support_trading_session", "CORE")).upper()
        duration = str(order.time_in_force).upper()
        if session not in {"CORE", "ALL", "NIGHT"}:
            raise ValueError("Webull equity session must be CORE, ALL or NIGHT")
        allowed = {"IOC"} if asset_type == "crypto" and order.order_type == Order.OrderType.MARKET else {"DAY", "GTC"}
        if duration not in allowed:
            raise ValueError("Webull order duration is not supported for this order type")
        order_type = _TYPES.get(order.order_type)
        side = _SIDES.get(order.side)
        if asset_type == "stock" and combo_type != "NORMAL":
            side = "SELL" if order.is_sell_order() else "BUY"
        symbol = order.asset.symbol
        if asset_type == "future":
            config = getattr(order.asset, "contract_spec", None)
            symbol = getattr(config, "symbol", None) or symbol
            if not any(character.isdigit() for character in symbol):
                raise ValueError("Resolve the actual dated Webull futures symbol before trading")
        if asset_type == "crypto":
            if order.quote is None:
                raise ValueError("Webull crypto orders require an explicit quote currency")
            symbol += order.quote.symbol
        if order_type is None or side is None:
            raise ValueError("Webull does not recognize this equity order type or side")
        quantity = positive_value(order.quantity, "quantity")
        row = dict(client_order_id=str(order.identifier), combo_type=combo_type, instrument_type=wire_type,
                   symbol=symbol, market="US", side=side, entrust_type="QTY",
                   quantity=quantity, order_type=order_type, time_in_force=duration)
        if asset_type == "stock" or "support_trading_session" in params:
            row["support_trading_session"] = session
        if asset_type == "option":
            row.update(option_strategy="SINGLE", legs=[self._option_leg(order)])
            if combo_type in {"NORMAL", "MASTER"}:
                row["position_intent"] = str(order.side).upper()
            for key in ("leg_in_or_out", "leg_in_strategy"):
                if key in params:
                    row[key] = params[key]
            if order.position_id is not None:
                row["position_id"] = order.position_id
        if asset_type == "prediction_contract":
            if order.order_type != Order.OrderType.LIMIT:
                raise ValueError("Webull event contracts use the LIMIT order endpoint")
            row["event_outcome"] = order.asset.outcome
            if params.get("event_trade_mode"):
                row["event_trade_mode"] = params["event_trade_mode"]
        if order.order_type == Order.OrderType.LIMIT:
            row["limit_price"] = positive_value(order.limit_price, "limit price")
        elif order.order_type in (Order.OrderType.STOP, Order.OrderType.STOP_LIMIT):
            row["stop_price"] = positive_value(order.stop_price, "stop price")
            if order.order_type == Order.OrderType.STOP_LIMIT:
                row["limit_price"] = positive_value(order.stop_limit_price, "stop limit price")
        elif order.order_type == Order.OrderType.TRAIL:
            if order.trail_price is not None:
                trailing_type, step = "AMOUNT", order.trail_price
            else:
                trailing_type, step = "PERCENTAGE", require_number(order.trail_percent, "trailing percent") * 100
            row.update(trailing_type=trailing_type, trailing_stop_step=positive_value(step, "trailing step"))
        return row

    def _submit_order(self, order):
        if not order.child_orders:
            return self._submit_rows(order, [self._order_row(order)])

        group = order.order_class
        if group not in {Order.OrderClass.BRACKET, Order.OrderClass.OTO, Order.OrderClass.OCO}:
            raise ValueError("Use Webull's native multi-leg submission for option packages")
        if order.asset.asset_type not in {Asset.AssetType.STOCK, Asset.AssetType.OPTION}:
            raise ValueError("Webull has no documented native linked order for this asset")
        rows = [] if group == Order.OrderClass.OCO else [self._order_row(order, combo_type="MASTER")]
        for child in order.child_orders:
            if order.asset.asset_type == Asset.AssetType.OPTION:
                child_type = "STOP_PROFIT" if child.order_type == Order.OrderType.LIMIT else "STOP_LOSS"
            else:
                child_type = {Order.OrderClass.BRACKET: "OTOCO", Order.OrderClass.OTO: "OTO",
                              Order.OrderClass.OCO: "OCO"}[group]
            session = (order.custom_params or {}).get("support_trading_session")
            rows.append(self._order_row(child, combo_type=child_type, equity_session=session))
        return self._submit_rows(order, rows, client_combo_id=str(order.identifier)
                                 if group == Order.OrderClass.OCO else uuid4().hex)

    def _submit_orders(self, orders, is_multileg=False, order_type=None, duration="day", price=None, **kwargs):
        if not is_multileg:
            return [self.submit_order(order) for order in orders]
        if not orders:
            return []
        for leg in orders:
            self.resolve_option_order_intent(leg)
        contracts = [leg.asset for leg in orders if leg.asset.asset_type == Asset.AssetType.OPTION]
        if not contracts or len({leg.asset.symbol for leg in orders}) != 1:
            raise ValueError("Webull option packages require one shared underlying")
        package_quantity, ratios = get_multileg_quantity_units(leg.quantity for leg in orders)
        shape = (orders[0].custom_params or {}).get("option_strategy")
        if shape is None and len(contracts) == len(orders) == 2 and ratios == [1, 1]:
            if contracts[0].right == contracts[1].right and contracts[0].expiration == contracts[1].expiration:
                shape = "VERTICAL"
            elif contracts[0].right == contracts[1].right and contracts[0].strike == contracts[1].strike:
                shape = "CALENDAR"
            elif contracts[0].expiration == contracts[1].expiration and contracts[0].right != contracts[1].right:
                shape = "STRADDLE" if contracts[0].strike == contracts[1].strike else "STRANGLE"
        if not shape:
            raise ValueError("Supply Webull's documented option_strategy for this package")
        if order_type in {"debit", "credit", "even"}:
            pay = order_type != "credit"
        elif order_type == "limit" and price is not None and float(price) != 0:
            pay = float(price) > 0
        else:
            raise ValueError("Webull option packages require an explicit net limit and debit/credit intent")
        limit = require_number(price or 0, "package net price").copy_abs()
        side = "buy" if pay else "sell"
        parent = Order(orders[0].strategy, Asset(orders[0].asset.symbol, asset_type="multileg"),
                       package_quantity, side, order_type="limit", order_class="multileg",
                       child_orders=orders, limit_price=limit, time_in_force=duration, tag=orders[0].tag)
        row = dict(client_order_id=parent.identifier, combo_type="NORMAL", instrument_type="OPTION",
                   market="US", symbol=parent.asset.symbol, side=side.upper(), order_type="LIMIT",
                   time_in_force=str(duration).upper(), entrust_type="QTY", option_strategy=shape,
                   quantity=positive_value(package_quantity, "package quantity"), limit_price=format(limit, "f"),
                   legs=[self._option_leg(leg) for leg in orders])
        return [self._submit_rows(parent, [row])]

    def _submit_rows(self, order, rows, *, client_combo_id=None):
        account = self._selected_account()
        with self._lock:
            if self.get_tracked_order(order.identifier):
                raise ValueError("This Webull client order ID is already tracked; reconcile it before submitting")
            self._unprocessed_orders.append(order)
        try:
            kwargs = {"client_combo_order_id": client_combo_id} if client_combo_id else {}
            receipt = request_json("placement", self.trading_client.order_v3.place_order, account, rows, **kwargs)
            if not isinstance(receipt, dict) or not any(
                receipt.get(field) for field in ("order_id", "client_order_id", "combo_order_id")
            ):
                raise WebullAPIError("placement acknowledgement")
        except WebullAPIError as error:
            if not error.write_outcome_unknown:
                order.set_error(error)
                self._process_trade_event(order, self.ERROR_ORDER, error=str(error))
                return order
            # Keep the original identifier and reconcile. Do not retry placement.
            try:
                observed = self._pull_order(order.identifier, order.strategy)
            except (WebullAPIError, ValueError):
                observed = None
            if observed is None:
                order.status = Order.OrderStatus.SUBMITTED
                raise WebullAPIError("placement outcome is unknown; look up the original client order ID") from None
            self._process_new_order(order)
            self._apply_direct_order_observation(order, observed)
            return order

        order.update_raw({key: receipt[key] for key in ("client_order_id", "order_id", "combo_order_id") if key in receipt})
        self._process_new_order(order)
        return order

    def cancel_order(self, order):
        return request_json("cancellation", self.trading_client.order_v3.cancel_order,
                            self._selected_account(), str(order.identifier))

    def _modify_order(self, order, limit_price=None, stop_price=None, *, quantity=None):
        if order.asset.asset_type == Asset.AssetType.CRYPTO:
            raise ValueError("Webull does not offer native crypto replacement")
        row = self._order_row(order)
        modify = {
            "client_order_id": row["client_order_id"], "order_type": row["order_type"],
            "time_in_force": row["time_in_force"], "quantity": row["quantity"],
        }
        for field in ("stop_price", "limit_price", "trailing_type", "trailing_stop_step"):
            if field in row:
                modify[field] = row[field]
        if quantity is not None:
            filled = require_number(getattr(order, "filled_quantity", None) or 0, "filled quantity")
            if require_number(quantity, "quantity") <= filled:
                raise ValueError("Webull replacement quantity must exceed completed fills")
            modify["quantity"] = positive_value(quantity, "quantity")
        if limit_price is not None:
            modify["limit_price"] = positive_value(limit_price, "limit price")
        if stop_price is not None:
            modify["stop_price"] = positive_value(stop_price, "stop price")
        return request_json("replacement", self.trading_client.order_v3.replace_order,
                            self._selected_account(), [modify])

    def _pull_broker_order(self, identifier):
        result = request_json("order detail", self.trading_client.order_v3.get_order_detail,
                              self._selected_account(), str(identifier))
        if not isinstance(result, dict):
            return None
        rows = result.get("orders") or []
        if rows and result.get("client_order_id") == str(identifier):
            return result
        for row in rows:
            if row.get("client_order_id") == str(identifier):
                return {**result, "orders": [row]}
        return None

    def _pull_broker_all_orders(self):
        account = self._selected_account()
        open_orders = self.trading_client.order_v3.list_order_open
        seen_cursors = set()
        cursor = None
        orders = []
        while True:
            kwargs = {} if cursor is None else {"pagination_key": cursor}
            page = request_json("open orders", open_orders, account, **kwargs)
            orders.extend(page["data"])
            cursor = page.get("pagination_key")
            if not cursor:
                return orders
            if cursor in seen_cursors:
                raise WebullAPIError("open orders returned a repeated cursor")
            seen_cursors.add(cursor)

    def _parse_broker_order(self, response, strategy_name, strategy_object=None):
        if not response:
            return None
        rows = response.get("orders") or []
        if not rows:
            return None
        if len(rows) == 1:
            return self._parse_order_row(rows[0], strategy_name)
        parent_id = response.get("client_order_id")
        root = next((row for row in rows if row["client_order_id"] == parent_id), None)
        if root is None:
            raise ValueError("Webull linked order parent cannot be matched to its client order ID")
        children = [self._parse_order_row(row, strategy_name) for row in rows if row is not root]
        group = Order.OrderClass.BRACKET if len(children) > 1 else Order.OrderClass.OTO
        return self._parse_order_row(root, strategy_name, order_class=group, child_orders=children)

    def _parse_order_row(self, row, strategy_name, *, order_class=None, child_orders=None):
        if row.get("option_strategy") not in ("", "SINGLE", None) and len(row.get("legs") or []) > 1:
            return self._parse_option_package(row, strategy_name)
        asset = self._read_asset(row)
        order_type = next((kind for kind, wire in _TYPES.items() if wire == row.get("order_type")), "unknown")
        side = str(row.get("position_intent") or "").lower() or next(
            (kind for kind, wire in _SIDES.items() if wire == row.get("side")), "unknown"
        )
        status = _STATUS.get(row.get("status"), "unknown")
        prices = {}
        for key in ("limit_price", "stop_price"):
            if row.get(key) not in ("", None):
                value = require_number(row[key], key)
                if value > 0 or (key == "limit_price" and row.get("option_strategy") not in (None, "", "SINGLE")):
                    prices[key] = float(value)
        if order_type == Order.OrderType.STOP_LIMIT and "limit_price" in prices:
            prices["stop_limit_price"] = prices.pop("limit_price")
        avg_fill = row.get("filled_price")
        order = Order(strategy_name, asset, require_number(row.get("total_quantity"), "order quantity"),
                      side, order_type=order_type, status=status, identifier=row["client_order_id"],
                      order_class=order_class, child_orders=child_orders,
                      time_in_force=row["time_in_force"].lower(),
                      avg_fill_price=float(require_number(avg_fill, "fill price")) if avg_fill not in (None, "") else None,
                      custom_params={"support_trading_session": row["support_trading_session"]}
                      if row.get("support_trading_session") else None, **prices)
        if row.get("filled_quantity") not in ("", None):
            order.filled_quantity = require_number(row["filled_quantity"], "filled quantity")
        order.broker_status = row.get("status")
        if row.get("filled_time_at"):
            order.broker_update_date = row["filled_time_at"]
        elif row.get("place_time_at"):
            order.broker_update_date = row["place_time_at"]
        order.update_raw({key: row[key] for key in ("client_order_id", "order_id") if key in row})
        return order

    def _parse_option_package(self, row, strategy_name):
        children = []
        for item in row["legs"]:
            # The native package is the only executable order. These are
            # contract terms for accounting, without fabricated execution IDs.
            leg_asset = self._read_asset({
                "symbol": row["symbol"], "instrument_type": item.get("instrument_type", "OPTION"),
                "legs": [item],
            })
            children.append(Order(
                strategy_name, leg_asset, require_number(item["quantity"], "leg quantity"),
                item["side"].lower(), order_type="market", identifier=item.get("id"),
            ))
        order = Order(
            strategy_name, Asset(row["symbol"], asset_type="multileg"),
            require_number(row["total_quantity"], "package quantity"), row["side"].lower(),
            order_type=str(row["order_type"]).lower(), order_class="multileg",
            limit_price=row.get("limit_price"), time_in_force=row["time_in_force"].lower(),
            status=_STATUS.get(row.get("status"), "unknown"), child_orders=children,
            identifier=row["client_order_id"], avg_fill_price=row.get("filled_price") or None,
        )
        if row.get("filled_quantity") not in ("", None):
            order.filled_quantity = require_number(row["filled_quantity"], "package filled quantity")
        order.update_raw({key: row[key] for key in ("client_order_id", "order_id") if key in row})
        return order

    def _get_stream_object(self):
        return PollingStream(polling_interval=5)

    def _register_stream_events(self):
        self.stream.add_action(self.stream.POLL_EVENT)(self._poll_tracked_orders)
        self.stream.add_action("webull_order_observed")(self._poll_signaled_order)

    def _poll_signaled_order(self, order_id):
        order = self.get_tracked_order(order_id)
        if order is not None:
            self._refresh_tracked_order(order)

    def _poll_tracked_orders(self):
        orders = [order for order in self.get_all_orders() if order.is_active()]
        if not orders:
            return
        # Rotate bounded targeted requests so one busy account cannot starve an
        # older order or exceed the documented detail limit.
        index = self._poll_offset % len(orders)
        selected = (orders[index:] + orders[:index])[:2]
        self._poll_offset += len(selected)
        for order in selected:
            self._refresh_tracked_order(order)

    def _refresh_tracked_order(self, order):
        observed = self._pull_order(order.identifier, order.strategy)
        if observed is not None and observed.broker_status in _STATUS:
            with self._lock:
                return self._apply_direct_order_observation(order, observed)
        return False

    def _on_order_observation(self, payload):
        # Full detail is authoritative. The push signals that an order changed,
        # and polling recovers it even when the bounded queue drops that signal.
        if payload.get("account_id") not in (None, self.account_id):
            return
        order_id = payload.get("client_order_id")
        if order_id and self.get_tracked_order(order_id) is not None:
            self.stream.dispatch("webull_order_observed", order_id=order_id)

    def _set_event_state(self, state):
        self.order_event_state = state

    def _start_order_events(self):
        try:
            self._selected_account()
            from lumibot.brokers.webull_events import WebullOrderEvents

            self._events = WebullOrderEvents(
                self._webull_config["APP_KEY"], self._webull_config["APP_SECRET"],
                self.account_id, paper=self.is_paper,
            )
            if not self._stop_event.is_set():
                self._events.run(self._on_order_observation, self._set_event_state)
        except (WebullAPIError, ImportError, KeyError, ValueError):
            self.order_event_state = "polling"

    def _run_stream(self):
        self._stream_established()
        self._events_thread = Thread(target=self._start_order_events, daemon=True, name="webull-order-events")
        self._events_thread.start()
        self.stream._run()

    def _start_realtime_bars(self, asset, keep_bars=30):
        self.data_source.start_realtime_bars(asset, keep_bars)

    def _get_realtime_bars(self, asset):
        return self.data_source.get_realtime_bars(asset)

    def _cancel_realtime_bars(self, asset):
        self.data_source.cancel_realtime_bars(asset)

    def cleanup_streams(self):
        super().cleanup_streams()
        webull_data = getattr(self, "_webull_data", None)
        if webull_data is not None and webull_data is not getattr(self, "_external_data_source", None):
            webull_data.shutdown()
        events = getattr(self, "_events", None)
        if events is not None:
            events.close()
        thread = getattr(self, "_events_thread", None)
        if thread is not None and thread.is_alive():
            thread.join(timeout=2)
