"""Webull's Individual OpenAPI trading adapter."""
from datetime import date, datetime, timedelta, timezone
from logging import getLogger
from threading import Lock, Thread
from uuid import NAMESPACE_URL, uuid4, uuid5

from lumibot.brokers.broker import Broker
from lumibot.entities import Asset, Order, Position
from lumibot.tools.helpers import get_multileg_quantity_units
from lumibot.tools.webull_helpers import (
    WebullAPIError,
    make_client,
    paper_mode,
    positive_value,
    read_json,
    request_json,
    require_number,
)
from lumibot.trading_builtins import PollingStream

logger = getLogger(__name__)

_ALGO_FIELDS = ("algo_type", "algo_start_time", "algo_end_time", "max_target_percent", "target_vol_percent")

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
    Order.OrderSide.SELL_SHORT: "SHORT",
    Order.OrderSide.BUY_TO_COVER: "BUY",
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


def _client_identifier(value):
    value = str(value)
    if len(value.encode("utf-8")) <= 32:
        return value
    return uuid5(NAMESPACE_URL, f"webull:order:{value}").hex


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
        self._order_read_markets = None
        self._closed_order_read_days = None
        from lumibot.data_sources import WebullData

        self._external_data_source = data_source
        self._webull_data = data_source if isinstance(data_source, WebullData) else WebullData(config)
        super().__init__("Webull", connect_stream=connect_stream, data_source=data_source or self._webull_data,
                         option_source=option_source, start_orders_thread=start_orders_thread, **kwargs)

    def _order_checkpoint_account_identity(self):
        return {"account_id": self.account_id, "environment": "paper" if self.is_paper else "live"}

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
                accounts = read_json("account discovery", client.account_v2.get_account_list)
                matches = [row for row in accounts if row.get("account_id") == self.account_id]
                if len(matches) != 1:
                    raise ValueError("The selected Webull account was not found in this environment")
                self._account_class = matches[0].get("account_class")
                self._account_verified = True
        return self.account_id

    def _get_balances_at_broker(self, quote_asset, strategy):
        balance = read_json("balance read", self.trading_client.account_v2.get_account_balance,
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
        balance = read_json("balance read", self.trading_client.account_v2.get_account_balance,
                               self._selected_account())
        row = next((row for row in balance["account_currency_assets"] if row["currency"] == currency), None)
        if row is None:
            raise ValueError("The Webull account did not report buying power in the requested currency")
        return require_number(row.get("buying_power"), "buying power")

    def get_historical_account_value(self):
        raise NotImplementedError("Webull Individual OpenAPI has no verified historical account value endpoint")

    def _pull_positions(self, strategy):
        rows = read_json("position read", self.trading_client.account_v2.get_account_position,
                            self._selected_account())
        name = self._strategy_name_from_input(strategy)
        holdings = {}
        for row in rows:
            quantity = require_number(row["quantity"], "position quantity")
            if row.get("instrument_type") == "OPTION" and len(row.get("legs", ())) > 1:
                parts = self._option_position_legs(row, quantity)
            else:
                parts = ((self._read_asset(row), quantity),)
            for asset, amount in parts:
                holdings[asset] = holdings.get(asset, 0) + amount
        return [Position(name, asset, quantity) for asset, quantity in holdings.items() if quantity]

    def _option_position_legs(self, row, quantity):
        # The US endpoint supplies a signed package quantity. Its per-leg
        # "proportion" is the portfolio weight, not the number of contracts.
        shape = row.get("option_strategy")
        count = {"VERTICAL": 2, "BUTTERFLY": 3, "STRADDLE": 2, "STRANGLE": 2,
                 "COVERED_STOCK": 2, "COLLAR_WITH_STOCK": 3,
                 "CALENDAR": 2, "DIAGONAL": 2, "CONDOR": 4, "IRON_CONDOR": 4, "IRON_BUTTERFLY": 4}.get(shape)
        if count is None or len(row["legs"]) != count:
            raise ValueError(f"Unrecognized Webull grouped option holding: {shape}")
        if any(leg.get("symbol", row["symbol"]) != row["symbol"] for leg in row["legs"]):
            raise ValueError("Unexpected underlying in a grouped Webull holding")
        if shape in {"COVERED_STOCK", "COLLAR_WITH_STOCK"}:
            shares = [leg for leg in row["legs"] if leg.get("instrument_type") == "EQUITY"]
            options = [leg for leg in row["legs"] if leg.get("instrument_type") == "OPTION"]
            rights = {"CALL", "PUT"} if shape == "COLLAR_WITH_STOCK" else {"CALL"}
            if len(shares) != 1 or {leg.get("option_type") for leg in options} != rights:
                raise ValueError("Inconsistent equity and option legs in a grouped Webull holding")
            deliverables = {require_number(leg.get("option_contract_deliverable"), "share deliverable")
                            for leg in options}
            if len(deliverables) != 1 or min(deliverables) <= 0:
                raise ValueError("Inconsistent share deliverables in a grouped Webull holding")
            holdings = [(Asset(row["symbol"]), quantity * deliverables.pop())]
            for leg in options:
                asset = self._read_asset({**row, "legs": [leg]})
                holdings.append((asset, -quantity if asset.right == "CALL" else quantity))
            return holdings
        assets = sorted((self._read_asset({**row, "legs": [leg]}) for leg in row["legs"]), key=lambda a: a.strike)
        if shape in {"CALENDAR", "DIAGONAL"}:
            near, far = sorted(assets, key=lambda asset: asset.expiration)
            if (near.expiration == far.expiration or near.right != far.right or near.multiplier != far.multiplier
                    or (near.strike == far.strike) != (shape == "CALENDAR")):
                raise ValueError("Inconsistent contract terms in a grouped Webull holding")
            return [(near, -quantity), (far, quantity)]

        if shape in {"STRADDLE", "STRANGLE"}:
            first, second = assets
            same_series = (first.expiration, first.multiplier) == (second.expiration, second.multiplier)
            same_strike = first.strike == second.strike
            if (not same_series or {first.right, second.right} != {"CALL", "PUT"}
                    or same_strike != (shape == "STRADDLE")):
                raise ValueError("Inconsistent contract terms in a grouped Webull holding")
            return [(asset, quantity) for asset in assets]

        if shape in {"IRON_CONDOR", "IRON_BUTTERFLY"}:
            puts = [asset for asset in assets if asset.right == "PUT"]
            calls = [asset for asset in assets if asset.right == "CALL"]
            series = {(asset.expiration, asset.multiplier) for asset in assets}
            if len(puts) != 2 or len(calls) != 2 or len(series) != 1:
                raise ValueError("Inconsistent contract terms in a grouped Webull holding")
            low, short_put, short_call, high = (*puts, *calls)
            center_matches = (short_put.strike == short_call.strike if shape == "IRON_BUTTERFLY"
                              else short_put.strike < short_call.strike)
            if not center_matches or not (low.strike < short_put.strike and short_call.strike < high.strike):
                raise ValueError("Inconsistent contract terms in a grouped Webull holding")
            # The provider reported credit openings with a negative package
            # quantity. Use put/call identity to disambiguate the same strike.
            return [(low, -quantity), (short_put, quantity), (short_call, quantity), (high, -quantity)]

        series = {(asset.right, asset.expiration, asset.multiplier) for asset in assets}
        if len(series) != 1 or len({asset.strike for asset in assets}) != count:
            raise ValueError("Inconsistent contract terms in a grouped Webull holding")
        if shape == "BUTTERFLY":
            ratios = (1, -2, 1)
        elif shape == "CONDOR":
            ratios = (1, -1, -1, 1)
        else:
            # A long call vertical buys the low strike. A long put vertical
            # buys the high strike. A negative package quantity inverts both.
            ratios = (1, -1) if assets[0].right == "CALL" else (-1, 1)
        return [(asset, quantity * ratio) for asset, ratio in zip(assets, ratios)]

    @staticmethod
    def _matches_position_asset(requested, held):
        if requested == held:
            return True
        if requested.asset_type != Asset.AssetType.FUTURE or held.asset_type != Asset.AssetType.FUTURE:
            return False
        symbol = getattr(requested.contract_spec, "symbol", None) or requested.symbol
        return symbol == held.symbol and requested.expiration in (None, held.expiration)

    def get_tracked_position(self, strategy, asset, position_side=None, position_id=None):
        if getattr(asset, "asset_type", None) == Asset.AssetType.FUTURE:
            # Webull identifies each delivery month by its own symbol. A strategy
            # may omit the date when looking up the same fully qualified contract.
            for position in self.get_tracked_positions(strategy):
                if self._matches_position_asset(asset, position.asset):
                    asset = position.asset
                    break
        return super().get_tracked_position(strategy, asset, position_side, position_id)

    def _pull_position(self, strategy, asset):
        return next((position for position in self._pull_positions(strategy)
                     if self._matches_position_asset(asset, position.asset)), None)

    def _dated_future(self, asset):
        symbol = getattr(asset.contract_spec, "symbol", None) or asset.symbol
        if not any(character.isdigit() for character in symbol):
            raise ValueError("Resolve the actual dated Webull futures symbol before trading")
        terms = self._webull_data.futures_contract(symbol)
        expiry = date.fromisoformat(terms["last_trading_date"])
        if asset.expiration is not None and asset.expiration != expiry:
            raise ValueError("Webull contract last trading date differs from the requested expiration")
        # Rebuild rather than mutate the asset: its identity and hash are cached.
        return Asset.from_dict({**asset.to_dict(), "symbol": symbol, "expiration": expiry,
                                "multiplier": float(require_number(terms["size"], "contract multiplier"))})

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
            return self._dated_future(Asset(row["symbol"], asset_type="future"))
        if instrument == "CRYPTO":
            currency = row.get("currency") or "USD"
            symbol = row["symbol"]
            return Asset(symbol.removesuffix(currency), asset_type="crypto")
        if instrument == "EVENT":
            return Asset(row["symbol"], asset_type="prediction_contract", outcome=row["event_outcome"].lower())
        raise ValueError("Webull returned an unmapped instrument type")

    @staticmethod
    def _option_leg(order, quantity=None):
        asset = order.asset
        if asset.asset_type not in (Asset.AssetType.STOCK, Asset.AssetType.OPTION):
            raise ValueError("Webull option packages accept only stock and option legs")
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

    def accepts_cash_amount_order(self, order):
        params = order.custom_params or {}
        if "total_cash_amount" not in params or order.side != Order.OrderSide.BUY or order.child_orders:
            return False
        duration = str(order.time_in_force).upper()
        if order.asset.asset_type == Asset.AssetType.STOCK:
            return order.order_type == Order.OrderType.MARKET and duration == "DAY"
        return (order.asset.asset_type == Asset.AssetType.PREDICTION_CONTRACT
                and order.order_type == Order.OrderType.LIMIT and duration == "FOK"
                and params.get("event_trade_mode") in (None, "TRADE_IN_AMOUNT"))

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
        if asset_type == "prediction_contract":
            allowed = {"DAY", "GTC", "IOC", "FOK", "GTD"}
        if duration not in allowed:
            raise ValueError("Webull order duration is not supported for this order type")
        order_type = _TYPES.get(order.order_type)
        side = _SIDES.get(order.side)
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
        row = dict(client_order_id=str(order.identifier), combo_type=combo_type, instrument_type=wire_type,
                   symbol=symbol, market="US", side=side, order_type=order_type, time_in_force=duration)
        algorithm = {key: params[key] for key in _ALGO_FIELDS if key in params}
        if algorithm:
            if (algorithm.get("algo_type") not in {"TWAP", "VWAP", "POV"} or asset_type != "stock"
                    or order_type not in {"MARKET", "LIMIT"} or duration != "DAY"
                    or combo_type != "NORMAL" or order.child_orders):
                raise ValueError("Webull equity algorithms require a standalone DAY market or limit order")
            row.update(algorithm)
        if "total_cash_amount" in params:
            if not self.accepts_cash_amount_order(order) or combo_type != "NORMAL" or order.quantity != 0:
                raise ValueError("Webull cash-amount orders require a standalone DAY stock market buy or FOK event limit buy with zero quantity")
            row.update(entrust_type="AMOUNT", total_cash_amount=positive_value(params["total_cash_amount"], "cash amount"))
        else:
            row.update(entrust_type="QTY", quantity=positive_value(order.quantity, "quantity"))
        if asset_type == "stock" or "support_trading_session" in params:
            row["support_trading_session"] = session
        if asset_type == "option":
            row.update(option_strategy="SINGLE", legs=[self._option_leg(order)])
            if combo_type in {"NORMAL", "MASTER"} and not params.get("leg_in_or_out"):
                row["position_intent"] = str(order.side).upper()
            for key in ("leg_in_or_out", "leg_in_strategy"):
                if key in params:
                    row[key] = params[key]
            if order.position_id is not None:
                row["position_id"] = order.position_id
        if asset_type == "prediction_contract":
            if order.order_type != Order.OrderType.LIMIT:
                raise ValueError("Webull event contracts use the LIMIT order endpoint")
            if order.asset.outcome not in {"yes", "no"}:
                raise ValueError("Webull event orders require a yes or no outcome")
            row["event_outcome"] = order.asset.outcome
            mode = params.get("event_trade_mode")
            if mode is not None:
                expected = "TRADE_IN_AMOUNT" if row["entrust_type"] == "AMOUNT" else "TRADE_IN_CONTRACT"
                if mode != expected:
                    raise ValueError("Webull event trade mode must match the requested amount or contract quantity")
                if order.limit_price is not None:
                    raise ValueError("A Webull best-price event order cannot enforce a limit price")
                row["event_trade_mode"] = mode
            if duration == "GTD":
                expiry = order.good_till_date
                if not isinstance(expiry, date):
                    raise ValueError("Webull GTD event orders require good_till_date")
                if isinstance(expiry, datetime):
                    expiry = (expiry.astimezone(timezone.utc) if expiry.tzinfo else expiry).date()
                row["expire_date"] = expiry.isoformat()
        if order.order_type == Order.OrderType.LIMIT and not row.get("event_trade_mode"):
            row["limit_price"] = positive_value(order.limit_price, "limit price")
        elif order.order_type in (Order.OrderType.STOP, Order.OrderType.STOP_LIMIT):
            row["stop_price"] = positive_value(order.stop_price, "stop price")
            if order.order_type == Order.OrderType.STOP_LIMIT:
                row["limit_price"] = positive_value(order.stop_limit_price, "stop limit price")
        elif order.order_type == Order.OrderType.TRAIL:
            if order.trail_price is not None:
                trailing_type, step = "AMOUNT", order.trail_price
            else:
                trailing_type, step = "PERCENTAGE", order.trail_percent
            row.update(trailing_type=trailing_type, trailing_stop_step=positive_value(step, "trailing step"))
        return row

    def _submit_order(self, order):
        key = (order.custom_params or {}).get("client_order_id")
        with self._lock:
            if self.get_tracked_order(order.identifier) is not None:
                raise ValueError("This Webull order is already tracked; reconcile it before submitting")
            if key:
                order.identifier = _client_identifier(key)
        if not order.child_orders:
            if order.asset.asset_type == Asset.AssetType.FUTURE:
                order.asset = self._dated_future(order.asset)
                order.symbol = order.asset.symbol
            return self._submit_rows(order, [self._order_row(order)])

        group = order.order_class
        if group not in {Order.OrderClass.BRACKET, Order.OrderClass.OTO, Order.OrderClass.OCO}:
            raise ValueError("Use Webull's native multi-leg submission for option packages")
        if order.asset.asset_type not in {Asset.AssetType.STOCK, Asset.AssetType.OPTION}:
            raise ValueError("Webull has no documented native linked order for this asset")
        rows = [] if group == Order.OrderClass.OCO else [self._order_row(order, combo_type="MASTER")]
        for index, child in enumerate(order.child_orders):
            if key:
                child.identifier = uuid5(NAMESPACE_URL, f"webull:leg:{order.identifier}:{index}").hex
            if order.asset.asset_type == Asset.AssetType.OPTION:
                child_type = "STOP_PROFIT" if child.order_type == Order.OrderType.LIMIT else "STOP_LOSS"
            else:
                child_type = {Order.OrderClass.BRACKET: "OTOCO", Order.OrderClass.OTO: "OTO",
                              Order.OrderClass.OCO: "OCO"}[group]
            session = (order.custom_params or {}).get("support_trading_session")
            rows.append(self._order_row(child, combo_type=child_type, equity_session=session))
        if group == Order.OrderClass.OCO:
            combo_id = str(order.identifier)
        elif key:
            combo_id = uuid5(NAMESPACE_URL, f"webull:combo:{order.identifier}").hex
        else:
            combo_id = uuid4().hex
        return self._submit_rows(order, rows, client_combo_id=combo_id)

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
            if price is None and order_type != "even":
                raise ValueError("Webull debit and credit packages require an explicit net limit")
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
        keys = {(leg.custom_params or {}).get("client_order_id") for leg in orders}
        package_key = kwargs.get("client_order_id") or (keys.pop() if len(keys) == 1 else None)
        if package_key:
            parent.identifier = _client_identifier(package_key)
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
            initial_status = order.status
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
                sent = {row["client_order_id"] for row in rows}
                with self._lock:
                    confirmed = order.status != initial_status
                    if not confirmed:
                        order.status = Order.OrderStatus.SUBMITTED
                    for child in order.child_orders:
                        if str(child.identifier) in sent and self.get_tracked_order(child.identifier) is None:
                            child.parent_identifier = order.identifier
                            child.status = Order.OrderStatus.SUBMITTED
                            self._unprocessed_orders.append(child)
                if confirmed:
                    return order
                raise WebullAPIError("placement outcome is unknown; look up the original client order ID") from None
            self._track_submitted_rows(order, rows)
            self._apply_direct_order_observation(order, observed)
            return order

        order.update_raw({key: receipt[key] for key in ("client_order_id", "order_id", "combo_order_id") if key in receipt})
        self._track_submitted_rows(order, rows)
        return order

    def _track_submitted_rows(self, order, rows):
        # Webull sends private events and order detail for each executable
        # bracket member. Option package legs are terms of one executable row.
        sent = {row["client_order_id"] for row in rows}
        with self._lock:
            if order.order_class == Order.OrderClass.OCO:
                self._process_placeholder_order(order)
            else:
                self._process_new_order(order)
            for child in order.child_orders:
                if str(child.identifier) in sent:
                    child.parent_identifier = order.identifier
                    self._process_new_order(child)

    def cancel_order(self, order):
        return request_json("cancellation", self.trading_client.order_v3.cancel_order,
                            self._selected_account(), str(order.identifier))

    def _modify_order(self, order, limit_price=None, stop_price=None, *, quantity=None):
        params = order.custom_params or {}
        if "total_cash_amount" in params or params.get("entrust_type") == "AMOUNT":
            raise ValueError("Webull share-quantity replacement cannot amend a cash-amount market order")
        if order.asset.asset_type == Asset.AssetType.CRYPTO:
            raise ValueError("Webull does not offer native crypto replacement")
        if order.asset.asset_type in (Asset.AssetType.OPTION, Asset.AssetType.MULTILEG):
            modify = self._option_replacement(order, quantity)
        else:
            modify = self._standard_replacement(order, quantity)
        if limit_price is not None:
            modify["limit_price"] = positive_value(limit_price, "limit price")
        if stop_price is not None:
            modify["stop_price"] = positive_value(stop_price, "stop price")
        return request_json("replacement", self.trading_client.order_v3.replace_order,
                            self._selected_account(), [modify])

    def _option_replacement(self, order, quantity):
        # Webull assigns leg IDs after placement. The parent client ID alone
        # cannot amend a single option or a spread through the v3 endpoint.
        group = self._pull_broker_order(order.identifier)
        row = next((item for item in (group or {}).get("orders", [])
                    if item.get("client_order_id") == str(order.identifier)), None)
        if row is None or row.get("instrument_type") != "OPTION":
            raise ValueError("Webull did not return the option order to replace")
        total = require_number(row.get("total_quantity"), "option quantity")
        target = total if quantity is None else require_number(quantity, "replacement quantity")
        filled = require_number(row.get("filled_quantity") or 0, "filled quantity")
        if total <= 0 or target <= filled or target != target.to_integral_value():
            raise ValueError("Webull replacement quantity must be in whole contracts and exceed completed fills")
        legs = []
        seen = set()
        for leg in row.get("legs") or []:
            leg_id = leg.get("id")
            if not leg_id or leg_id in seen:
                raise ValueError("Webull did not return a unique option leg ID")
            seen.add(leg_id)
            size = require_number(leg.get("quantity"), "option leg quantity") * target / total
            if size != size.to_integral_value():
                raise ValueError("Webull replacement quantity must preserve whole-contract leg ratios")
            legs.append({"id": leg_id, "quantity": positive_value(size, "option leg quantity")})
        if not legs:
            raise ValueError("Webull did not return any option leg IDs")
        return {"client_order_id": str(order.identifier), "quantity": positive_value(target, "quantity"),
                "legs": legs}

    def _standard_replacement(self, order, quantity):
        row = self._order_row(order)
        modify = {
            "client_order_id": row["client_order_id"], "order_type": row["order_type"],
            "time_in_force": row["time_in_force"],
        }
        for field in ("stop_price", "limit_price", "trailing_type", "trailing_stop_step"):
            if field in row:
                modify[field] = row[field]
        if quantity is not None:
            filled = require_number(getattr(order, "filled_quantity", None) or 0, "filled quantity")
            if require_number(quantity, "quantity") <= filled:
                raise ValueError("Webull replacement quantity must exceed completed fills")
            modify["quantity"] = positive_value(quantity, "quantity")
        return modify

    def _pull_broker_order(self, identifier):
        identifier = _client_identifier(identifier)
        result = read_json("order detail", self.trading_client.order_v3.get_order_detail,
                           self._selected_account(), identifier)
        if not isinstance(result, dict):
            return None
        rows = result.get("orders") or []
        if rows and result.get("client_order_id") == str(identifier):
            return result
        for row in rows:
            if row.get("client_order_id") == str(identifier):
                return {**result, "orders": [row]}
        return None

    def set_order_read_markets(self, symbols):
        """Limit an explicit order-list read to instruments or their underlying symbols."""
        self._order_read_markets = None if symbols is None else {str(s).upper() for s in symbols}

    def set_closed_order_read_window(self, days):
        """Include historical orders from the preceding days in an explicit list read."""
        self._closed_order_read_days = days

    def _order_pages(self, operation, method, account, **params):
        seen_cursors = set()
        while True:
            page = read_json(operation, method, account, **params)
            yield from page["data"]
            cursor = page.get("pagination_key")
            if not cursor:
                return
            if cursor in seen_cursors:
                raise WebullAPIError(f"{operation} returned a repeated cursor")
            seen_cursors.add(cursor)
            params["pagination_key"] = cursor

    def _matches_order_read_market(self, row):
        markets = self._order_read_markets
        if row.get("instrument_type") == "CRYPTO":
            base = self._read_asset(row).symbol
            if base in markets or f"{base}/{row.get('currency') or 'USD'}" in markets:
                return True
        return any(item.get("symbol") in markets or item.get("underlying_symbol") in markets
                   for item in (row, *(row.get("legs") or [])))

    def _pull_broker_all_orders(self):
        account = self._selected_account()
        api = self.trading_client.order_v3
        groups = list(self._order_pages("open orders", api.list_order_open, account))
        if self._closed_order_read_days:
            now = datetime.now(timezone.utc)
            start = now - timedelta(days=self._closed_order_read_days)
            # Supply both UTC boundaries. An isolated start is rejected by the venue.
            groups.extend(self._order_pages(
                "order history", api.list_order_history, account,
                start_time=start.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
                end_time=now.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            ))
        selected = {}
        for group in groups:
            members = group.get("orders") or []
            if self._order_read_markets is not None and not any(
                self._matches_order_read_market(row) for row in members
            ):
                continue
            # Open-list observations are fresher than historical snapshots of the same group.
            group_id = group.get("client_order_id") or group.get("combo_order_id")
            if not group_id and members:
                group_id = members[0]["client_order_id"]
            if not group_id:
                raise WebullAPIError("Webull order list returned a group without an identity")
            selected.setdefault(group_id, group)
        return list(selected.values())

    def _orders_for_strategy_sync(self, orders, strategy):
        # Account reads return all venue orders. Only local or restored native
        # identities establish ownership for strategy sync and callbacks.
        strategy_name = self._strategy_name_from_input(strategy)
        owned = []
        for observed in orders:
            tracked = self.get_tracked_order(observed.identifier, use_placeholders=True)
            if tracked is not None and tracked.strategy == strategy_name:
                owned.append(observed)
        return owned

    def _iter_executable_order_tree(self, orders):
        for order in orders:
            if order.order_class == Order.OrderClass.MULTILEG:
                # Webull assigns IDs to option terms for amendments. Only the package executes.
                yield order
            else:
                yield from self._iter_order_tree([order])

    def _parse_broker_order(self, response, strategy_name, strategy_object=None):
        if not response:
            return None
        rows = response.get("orders") or []
        if not rows or any(not row.get("status") for row in rows):
            # PaperTrade can return the ID before it publishes the new state.
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
        if order_type == Order.OrderType.TRAIL and row.get("trailing_stop_step") not in (None, ""):
            field = {"PERCENTAGE": "trail_percent", "AMOUNT": "trail_price"}.get(row.get("trailing_type"))
            if field is None:
                raise ValueError("Webull returned an unrecognized trailing stop unit")
            prices[field] = float(require_number(row["trailing_stop_step"], "trailing stop step"))
        avg_fill = row.get("filled_price")
        quantity = row.get("total_quantity")
        params = {}
        for key in ("support_trading_session", "event_trade_mode", *_ALGO_FIELDS):
            if row.get(key):
                params[key] = row[key]
        if row.get("expire_date") and row["time_in_force"] == "GTD":
            prices["good_till_date"] = datetime.fromisoformat(row["expire_date"]).replace(tzinfo=timezone.utc)
        if row.get("entrust_type") == "AMOUNT":
            params["entrust_type"] = "AMOUNT"
            if row.get("total_cash_amount") not in ("", None):
                params["total_cash_amount"] = row["total_cash_amount"]
            # A cash-amount order has no assigned share quantity before execution.
            if quantity in (None, ""):
                quantity = 0
        order = Order(strategy_name, asset, require_number(quantity, "order quantity"),
                      side, order_type=order_type, status=status, identifier=row["client_order_id"],
                      order_class=order_class, child_orders=child_orders,
                      quote=Asset(row.get("currency") or "USD", "forex") if row["instrument_type"] == "CRYPTO" else None,
                      time_in_force=row["time_in_force"].lower(),
                      avg_fill_price=float(require_number(avg_fill, "fill price")) if avg_fill not in (None, "") else None,
                      custom_params=params or None, **prices)
        return self._read_order_execution(order, row)

    @staticmethod
    def _read_order_execution(order, row):
        if row.get("filled_quantity") not in ("", None):
            order.filled_quantity = require_number(row["filled_quantity"], "filled quantity")
        order.broker_status = row.get("status")
        def timestamp(field):
            value = row.get(field)
            if not value:
                return None
            parsed = datetime.fromisoformat(value.removesuffix("Z") + "+00:00" if value.endswith("Z") else value)
            return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)

        order.broker_create_date = timestamp("place_time_at")
        order.broker_update_date = timestamp("filled_time_at") or order.broker_create_date
        order.update_raw({key: row[key] for key in ("client_order_id", "order_id") if key in row})
        return order

    def _parse_option_package(self, row, strategy_name):
        children = []
        for item in row["legs"]:
            # The native package is the only executable order. These are
            # contract terms for accounting, without fabricated execution IDs.
            instrument = item.get("instrument_type")
            if not instrument:
                # Historical mixed packages can omit instrument_type on all legs.
                # Their sole equity leg carries no option contract terms.
                mixed = row["option_strategy"] in {"COVERED_STOCK", "COLLAR_WITH_STOCK"}
                has_terms = any(item.get(key) for key in ("option_type", "strike_price", "option_expire_date"))
                instrument = "EQUITY" if mixed and not has_terms else "OPTION"
            leg_asset = self._read_asset({
                "symbol": item.get("symbol") or row["symbol"], "instrument_type": instrument,
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
        return self._read_order_execution(order, row)

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
        orders = [order for order in self.get_all_orders() if order.is_active()
                  and not order.is_filled() and not order.is_canceled()
                  and order.order_class != Order.OrderClass.OCO]
        if not orders:
            return
        # Rotate bounded targeted requests so one busy account cannot starve an
        # older order or exceed the documented detail limit.
        index = self._poll_offset % len(orders)
        selected = (orders[index:] + orders[:index])[:2]
        self._poll_offset += len(selected)
        for order in selected:
            try:
                self._refresh_tracked_order(order)
            except (WebullAPIError, ValueError, ArithmeticError) as exc:
                logger.warning("Webull order observation failed (%s); will retry on a future poll", type(exc).__name__)

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
