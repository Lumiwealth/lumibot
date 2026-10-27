from __future__ import annotations

import os
import time
from collections import OrderedDict
from threading import Lock

from lumibot._lazy_imports import LazyLogger, LazyModule, lazy_class

from .broker import Broker

logger = LazyLogger(__name__)
TYPE_CHECKING = False
datetime = LazyModule("datetime")
Asset = lazy_class("lumibot.entities", "Asset")
Order = lazy_class("lumibot.entities", "Order")
Decimal = lazy_class("decimal", "Decimal")

if TYPE_CHECKING:
    from lumibot.entities import Position


def colored(*args, **kwargs):
    from termcolor import colored as _colored

    return _colored(*args, **kwargs)


def _position_class():
    from lumibot.entities import Position

    return Position


class Ccxt(Broker):
    """
    Crypto broker using CCXT.
    """

    def __init__(self, config, data_source: CcxtData = None, max_workers=20, chunk_size=100, **kwargs):
        from lumibot.data_sources import CcxtData

        if data_source is None:
            data_source = CcxtData(config, max_workers=max_workers, chunk_size=chunk_size)
        if not isinstance(data_source, CcxtData):
            raise ValueError(f"Ccxt Broker's Data Source must be of type {CcxtData}")
        self.api = data_source.api
        self._contract_accounts_lock = Lock()
        self._contract_accounts_cache = None
        self._stream_orders = OrderedDict()
        self._stream_order_scopes = {}
        self._submitted_orders = {}
        self._pending_client_orders = {}
        super().__init__(name="ccxt", config=config, data_source=data_source, max_workers=max_workers, **kwargs)

        # Override default market setting for crypto to be 24/7, but still respect config/env if set
        self.market = (config.get("MARKET") if config else None) or os.environ.get("MARKET") or "24/7"
        self.fetch_open_orders_last_request_time = None
        self.binance_all_orders_rate_limit = 5
        if not isinstance(self.data_source, CcxtData):
            raise ValueError(f"Ccxt Broker's Data Source must be of type {CcxtData}")
        self.api = self.data_source.api

    def _ensure_markets_loaded(self):
        ensure = getattr(self.data_source, "_ensure_markets_loaded", None)
        if callable(ensure):
            ensure()

    def get_market(self, asset, quote=None):
        return self.data_source.get_market(asset, quote)

    def get_asset(self, symbol):
        """Build an asset from provider metadata; contract quantities are contracts."""
        market = self.get_market(symbol)
        if market.get("option"):
            # CCXT options map to LumiBot option assets (base, expiry, strike, right).
            expiry = datetime.datetime.fromtimestamp(market["expiry"] / 1000, tz=datetime.timezone.utc).date()
            strike = market["strike"]
            strike = int(strike) if float(strike).is_integer() else float(strike)
            return Asset(market["base"], asset_type="option", expiration=expiry, strike=strike,
                         right=str(market["optionType"]).upper(), multiplier=market.get("contractSize") or 1)
        if market.get("contract"):
            return Asset(market["symbol"], asset_type="crypto_future", multiplier=market["contractSize"])
        return Asset(market["base"], asset_type="crypto")

    def get_contract_exposure(self, asset, quantity, price):
        market = self.get_market(asset)
        contracts, size, price = Decimal(str(quantity)), Decimal(str(market["contractSize"])), Decimal(str(price))
        if market.get("inverse"):
            quote = contracts * size
            base = quote / price
        else:
            base = contracts * size
            quote = base * price
        return {"base": base, "quote": quote, "settle": market["settle"], "contract_size": size}

    @staticmethod
    def _position_side(response):
        """Hedge leg named by a unified or native exchange field, else None (netted)."""
        info = response.get("info") or {}
        for side in (response.get("positionSide"), info.get("posSide"), info.get("positionSide"),
                     info.get("holdSide")):
            if side is not None and str(side).lower() in ("long", "short"):
                return str(side).lower()
        # Position index: 1 = buy-side hedge leg, 2 = sell-side hedge leg, 0 = one-way.
        index = str(info.get("positionIdx", "")).strip()
        if index in ("1", "2"):
            return "long" if index == "1" else "short"
        side = str(response.get("side") or "").lower()
        # Unified positions name their leg long/short; orders say buy/sell, which is not a leg.
        return side if response.get("hedged") and side in ("long", "short") else None

    @staticmethod
    def _phemex_uses_real_values(market):
        return (market.get("info") or {}).get("type") == "PerpetualV2"

    def _phemex_contract_accounts(self):
        """Read every metadata-discovered settlement wallet and its positions.

        Phemex V2 uses the G endpoints and unscaled Rv values. Legacy contracts
        use the original endpoints and currency-specific Ev scaling. Routing by
        product metadata also covers newly listed settlement currencies.
        """
        self._ensure_markets_loaded()
        with self._contract_accounts_lock:
            if self._contract_accounts_cache and time.monotonic() - self._contract_accounts_cache[0] < 1:
                return self._contract_accounts_cache[1]
            groups = {}
            for market in self.api.markets.values():
                if market.get("contract"):
                    groups[(market["settle"], self._phemex_uses_real_values(market))] = market
            result = {"positions": [], "balances": {}}
            for (settle, real), market in sorted(groups.items()):
                currency = self.api.currency(settle)
                request = {"currency": currency["id"]}
                endpoint = self.api.privateGetGAccountsAccountPositions if real else self.api.privateGetAccountsAccountPositions
                response = endpoint(request)
                data = response.get("data") or {}
                account = data.get("account") or {}
                values = {}
                for name, key in (("total", "accountBalance"), ("used", "totalUsedBalance")):
                    if key + "Rv" in account:
                        values[name] = Decimal(str(account[key + "Rv"]))
                    elif key + "Ev" in account:
                        values[name] = Decimal(str(account[key + "Ev"])) / Decimal(10) ** int(currency.get("valueScale", 8))
                    else:
                        raise ValueError(f"Phemex {settle} account omitted {key}")
                values["free"] = values["total"] - values["used"]
                result["balances"][settle] = values
                for raw in data.get("positions") or []:
                    contract = next((item for item in self.api.markets.values()
                                     if item.get("contract") and item["id"] == raw.get("symbol")
                                     and item["settle"] == settle), None)
                    result["positions"].append(self.api.parse_position(raw, contract))
            self._contract_accounts_cache = (time.monotonic(), result)
            return result

    def _fetch_derivative_positions(self):
        if self.api.id == "phemex":
            return self._phemex_contract_accounts()["positions"]
        # One call per contract type the exchange actually lists (swap first, as before),
        # so dated futures and options positions are read too.
        present = {m.get("type") for m in self.api.markets.values() if m.get("contract")}
        types = [t for t in ("swap", "future", "option") if t in present] or ["swap"]
        positions = []
        for market_type in types:
            try:
                positions.extend(self.api.fetch_positions(params={"type": market_type}) or [])
            except Exception as exc:
                if market_type == types[0]:
                    raise
                logger.warning("CCXT %s positions for %s could not be read: %s",
                               market_type, self.api.id, type(exc).__name__)
        return positions

    def set_position_mode(self, hedged, asset, params=None):
        market = self.get_market(asset)
        if self.api.id == "phemex" and self._phemex_uses_real_values(market):
            return self.api.privatePutGPositionsSwitchPosModeSync({
                "symbol": market["id"], "targetPosMode": "Hedged" if hedged else "OneWay", **(params or {})})
        return self.api.set_position_mode(hedged, market["symbol"], params or {})

    def set_leverage(self, leverage, asset, params=None):
        return self.api.set_leverage(leverage, self.get_market(asset)["symbol"], params or {})

    def set_margin_mode(self, margin_mode, asset, params=None):
        return self.api.set_margin_mode(margin_mode, self.get_market(asset)["symbol"], params or {})

    def fetch_funding_rate(self, asset, params=None):
        return self.api.fetch_funding_rate(self.get_market(asset)["symbol"], params or {})

    # =========Clock functions=====================

    def get_timestamp(self):
        """Returns the current UNIX timestamp representation from CCXT"""
        logger.warning("The method 'get_time_to_close' is not applicable with Crypto 24/7 markets.")
        return self.api.microseconds() / 1000000

    def is_market_open(self):
        """The market is always open for Crypto.

        Returns
        -------
        True
        """
        return True

    def get_time_to_open(self):
        """Not applicable with Crypto 24/7 markets.

        Returns
        -------
        None
        """
        logger.warning("The method 'get_time_to_open' is not applicable with Crypto 24/7 markets.")
        return None

    def get_time_to_close(self):
        """Not applicable with Crypto 24/7 markets.

        Returns
        -------
        None
        """
        logger.debug("The method 'get_time_to_close' is not applicable with Crypto 24/7 markets.")
        return None

    def is_margin_enabled(self):
        """Check if the broker is using margin trading"""
        return "margin" in self._config and self._config["margin"]

    def _fetch_balance(self):
        params = {"type": "spot"} if self.api.id == "phemex" else {}

        if self.is_margin_enabled():
            params["type"] = "margin"

        return self.api.fetch_balance(params)

    # =========Positions functions==================
    @staticmethod
    def _balance_rows(balances):
        """Read CCXT's unified currency maps, including older per-currency results."""
        reserved = {"total", "free", "used", "info", "timestamp", "datetime", "debt"}
        totals = balances.get("total") or {}
        currencies = set(totals)
        currencies.update(key for key, value in balances.items()
                          if key not in reserved and isinstance(value, dict) and "total" in value)
        for currency in sorted(currencies):
            row = balances.get(currency) or {}
            total = totals.get(currency, row.get("total"))
            if total is None or Decimal(str(total)) == 0:
                continue
            free = (balances.get("free") or {}).get(currency, row.get("free"))
            used = (balances.get("used") or {}).get(currency, row.get("used"))
            if free is None and used is not None:
                free = Decimal(str(total)) - Decimal(str(used))
            if used is None and free is not None:
                used = Decimal(str(total)) - Decimal(str(free))
            yield {"currency": currency, "total": total, "free": free, "used": used}

    def _currency_value(self, currency, units, quote_asset):
        if currency == quote_asset.symbol:
            return units
        direct = f"{currency}/{quote_asset.symbol}"
        inverse = f"{quote_asset.symbol}/{currency}"
        symbol = direct if direct in self.api.markets else inverse
        if symbol not in self.api.markets:
            logger.warning("No %s valuation market for %s; excluded from CCXT equity", quote_asset, currency)
            return Decimal("0")
        price = self.api.fetch_ticker(symbol).get("last")
        if price is None or Decimal(str(price)) <= 0:
            raise ValueError(f"No usable CCXT valuation price for {symbol}")
        price = Decimal(str(price))
        return units * price if symbol == direct else units / price

    def _get_balances_at_broker(self, quote_asset, strategy):
        """Return cash, non-cash holdings plus PnL, and equity in quote units."""
        cash = Decimal("0")
        positions_value = Decimal("0")
        self._ensure_markets_loaded()
        rows = list(self._balance_rows(self._fetch_balance()))
        contracts = None
        if self.api.id == "phemex" and any(m.get("contract") for m in self.api.markets.values()):
            contracts = self._phemex_contract_accounts()
            rows.extend({"currency": currency, **balance} for currency, balance in contracts["balances"].items())
        for row in rows:
            currency, units = row["currency"], Decimal(str(row["total"]))
            if currency == quote_asset.symbol:
                cash += units
            elif units:
                positions_value += self._currency_value(currency, units, quote_asset)
        if contracts:
            for position in contracts["positions"]:
                pnl = position.get("unrealizedPnl")
                if pnl is None and Decimal(str(position.get("contracts") or 0)) != 0:
                    raise ValueError(f"Phemex omitted unrealized PnL for {position['symbol']}")
                if pnl:
                    settlement = self.get_market(position["symbol"])["settle"]
                    positions_value += self._currency_value(settlement, Decimal(str(pnl)), quote_asset)
        return float(cash), float(positions_value), float(cash + positions_value)

    def _parse_broker_position(self, position, strategy, orders=None):
        """parse a broker position representation
        into a position object"""
        Position = _position_class()

        if "contracts" in position:
            market = self.get_market(position["symbol"])
            quantity = Decimal(str(position.get("contracts") or 0))
            if position.get("side") == "short":
                quantity = -abs(quantity)
            position_id = position.get("id") if self.api.id != "phemex" else None
            result = Position(strategy, self.get_asset(market["symbol"]), quantity, orders=orders,
                              avg_fill_price=position.get("entryPrice"),
                              position_side=self._position_side(position), position_id=position_id)
            result.current_price = position.get("markPrice")
            result.pnl = position.get("unrealizedPnl")
            result.currency = market["settle"]
            result.exchange = self.api.id
            return result

        symbol = position["currency"]
        hold = position["used"]
        available = position["free"]
        quantity = Decimal(str(position["total"]))

        self._ensure_markets_loaded()
        currency = self.api.currencies.get(symbol) or {}
        precision = currency.get("precision")
        if precision is not None:
            import ccxt

            if self.api.precisionMode == ccxt.DECIMAL_PLACES:
                precision = Decimal("10") ** -precision
            elif self.api.precisionMode == ccxt.SIGNIFICANT_DIGITS:
                # Significant digits cannot be represented as a fixed coin increment.
                precision = None

        asset = Asset(
            symbol=symbol,
            asset_type="crypto",
            precision=precision,
        )

        position_return = Position(strategy, asset, quantity, hold=hold, available=available, orders=orders)
        return position_return

    def _pull_broker_position(self, asset, position_side=None):
        market = self.get_market(asset) if asset.asset_type == "crypto_future" else None
        matches = [row for row in self._pull_broker_positions()
                   if (row.get("symbol") == market["symbol"] if market else row.get("currency") == asset.symbol)
                   and (position_side is None or self._position_side(row) == position_side)]
        if len(matches) > 1:
            raise ValueError(f"Multiple positions for {asset.symbol}; specify position_side")
        return matches[0] if matches else None

    def _pull_broker_positions(self, strategy=None):
        """Read spot holdings using the unified balance contract on every exchange."""
        quote = strategy.quote_asset.symbol if strategy is not None else None
        rows = [row for row in self._balance_rows(self._fetch_balance()) if row["currency"] != quote]
        self._ensure_markets_loaded()
        if self.api.has.get("fetchPositions") and any(m.get("contract") for m in self.api.markets.values()):
            seen = set()
            for position in self._fetch_derivative_positions():
                key = (position["symbol"], self._position_side(position),
                       position.get("id") if self.api.id != "phemex" else None)
                if key not in seen and Decimal(str(position.get("contracts") or 0)) != 0:
                    rows.append(position)
                    seen.add(key)
        return rows

    def _parse_broker_positions(self, broker_positions, strategy):
        """parse a list of broker positions into a
        list of position objects"""
        result = []
        for broker_position in broker_positions:
            new_pos = self._parse_broker_position(broker_position, strategy)

            # Check if the position is not None
            if new_pos is not None:
                result.append(new_pos)

        return result

    def _pull_positions(self, strategy):
        """Get the account positions. return a list of
        position objects"""
        response = self._pull_broker_positions(strategy)
        result = self._parse_broker_positions(response, strategy.name)
        return result

    def _pull_position(self, strategy, asset, position_side=None):
        """
        Pull a single position from the broker that matches the asset and strategy. If no position is found, None is
        returned.

        Parameters
        ----------
        strategy: Strategy
            The strategy object that placed the order to pull
        asset: Asset
            The asset to pull the position for

        Returns
        -------
        Position
            The position object for the asset and strategy if found, otherwise None
        """
        response = self._pull_broker_position(asset, position_side=position_side)
        return self._parse_broker_position(response, getattr(strategy, "name", strategy)) if response else None

    # =======Orders and assets functions=========
    def _parse_broker_order(self, response, strategy_name, strategy_object=None):
        """parse a broker order representation
        to an order object"""
        market = self.get_market(response["symbol"])
        asset = self.get_asset(market["symbol"])
        quote = market["quote"]
        order_type = response.get("type") or "market"
        trigger = response.get("triggerPrice") or response.get("stopPrice")
        if trigger is not None and order_type in ("market", "limit"):
            order_type = "stop" if order_type == "market" else "stop_limit"
        order = Order(
            strategy_name, asset, response["amount"], response["side"],
            limit_price=response.get("price") if order_type in ("limit", "stop_limit") else None,
            stop_price=trigger,
            time_in_force=(response.get("timeInForce") or "gtc").lower(),
            quote=Asset(quote, asset_type="crypto"), order_type=order_type,
            avg_fill_price=response.get("average"),
            position_side=self._position_side(response),
        )
        order.reduce_only = response.get("reduceOnly") or (response.get("info") or {}).get("reduceOnly", False)
        order.set_identifier(response["id"])
        order.status = response.get("status") or "new"
        order.update_raw(response)
        return order

    def _pull_broker_order(self, identifier):
        order = self.get_tracked_order(identifier) or self._submitted_orders.get(str(identifier))
        if order is not None and self.api.has.get("fetchOrder"):
            pair = self.get_market(order.pair or order.asset, order.quote)["symbol"]
            params = self._position_request_params(order)
            return self.api.fetch_order(identifier, pair, **({"params": params} if params else {}))
        if str(identifier) in self._stream_orders:
            return self._stream_orders[str(identifier)]
        if self.api.id == "phemex":
            raise ValueError("Phemex order lookup requires a known order market")
        orders = self._pull_broker_all_orders() + self._pull_broker_closed_orders()
        return next((item for item in orders if str(item["id"]) == str(identifier)), None)

    def _pull_broker_closed_orders(self):
        params = {}
        if self.is_margin_enabled() and self.api.id == "kucoin":
            params["tradeType"] = "MARGIN_TRADE"
        return self.api.fetch_closed_orders(params=params)

    def _pull_broker_all_orders(self):
        """Get the broker open orders"""
        if self.api.id == "phemex":
            self._ensure_markets_loaded()
            if self.stream is not None:
                scopes = []
                if any(m.get("spot") for m in self.api.markets.values()):
                    scopes.append({"type": "spot"})
                contracts = [m for m in self.api.markets.values() if m.get("contract")]
                if any(not self._phemex_uses_real_values(m) for m in contracts):
                    scopes.append({"type": "swap"})
                if any(self._phemex_uses_real_values(m) for m in contracts):
                    scopes.append({"type": "swap", "settle": "USDT"})
                for scope in scopes:
                    self.stream.subscribe("watch_balance", params=scope)
                    self.stream.snapshot("watch_orders", params=scope, max_age=None, require_snapshot=True)
                with self._lock:
                    return [dict(order) for order in self._stream_orders.values()
                            if order.get("status") in ("open", "new", "pending")]
            # Phemex REST requires a market symbol. A stream-disabled account
            # needs the full discovery scan to avoid losing external orders.
            orders = []
            for market in self.api.markets.values():
                if market.get("active") is not False:
                    orders.extend(self.api.fetch_open_orders(market["symbol"]))
            return orders

        if self.stream is not None and self.api.apiKey:
            self.stream.subscribe("watch_orders")
            self.stream.subscribe("watch_balance")
        # For binance api rate limit on calling all orders at once.
        if self.api.exchangeId == "binance":
            self.api.options["warnOnFetchOpenOrdersWithoutSymbol"] = False
            if self.fetch_open_orders_last_request_time is not None:
                net_rate_limit = (
                    self.binance_all_orders_rate_limit
                    - (datetime.datetime.now() - self.fetch_open_orders_last_request_time).seconds
                )
                if net_rate_limit > 0:
                    logger.info(
                        f"Binance all order rate limit is being exceeded, bot sleeping for "
                        f"{net_rate_limit} seconds."
                    )
                    self.sleep(net_rate_limit)
            self.fetch_open_orders_last_request_time = datetime.datetime.now()

        params = {}

        if self.is_margin_enabled() and self.api.exchangeId != "binance":
            params["tradeType"] = "MARGIN_TRADE"

        orders = self.api.fetch_open_orders(params=params)
        return orders

    def _flatten_order(self, order):
        # A REST acknowledgement can register or finish an order before the
        # optional submission worker gets here. Do not enqueue it a second time.
        return [] if order._new_event.is_set() or order._closed_event.is_set() else [order]

    def _submit_order(self, order):
        """Submit through CCXT's unified order API, preserving native parameters."""
        if order.order_class not in (None, "simple"):
            order.set_error("CCXT compound order lifecycle is not implemented; use native attached-order parameters")
            return order
        if order.quantity is None or order.quantity <= 0:
            order.set_error("Order quantity must be greater than zero")
            return order
        self._ensure_markets_loaded()
        try:
            market = self.get_market(order.pair or order.asset, order.quote)
        except ValueError as exc:
            order.set_error(exc)
            return order
        pair = market["symbol"]
        order.pair = pair
        submission_started = False
        accepted_response = None
        client_id = None
        if market.get("contract"):
            order.asset.multiplier = market["contractSize"]
        try:
            order.quantity = Decimal(self.api.amount_to_precision(pair, order.quantity))
            for field in ("limit_price", "stop_price"):
                price = getattr(order, field, None)
                if price is not None:
                    setattr(order, field, Decimal(self.api.price_to_precision(pair, price)))
            limits = self.api.markets[pair].get("limits") or {}
            for price in (order.limit_price, order.stop_price):
                if price is not None:
                    self._check_market_limit("price", price, limits)
            if order.limit_price is not None:
                cost = order.limit_price * order.quantity
                if market.get("contract"):
                    cost = self.get_contract_exposure(order.asset, order.quantity, order.limit_price)["quote"]
                self._check_market_limit("cost", cost, limits)
            params = {}
            if self.is_margin_enabled() and self.api.id == "kucoin":
                params["tradeType"] = "MARGIN_TRADE"
            if order.stop_price is not None:
                params["triggerPrice"] = str(order.stop_price)
            if order.time_in_force and order.time_in_force.lower() != "day":
                params["timeInForce"] = order.time_in_force.upper()
            if getattr(order, "reduce_only", False):
                params["reduceOnly"] = True
            if order.position_side is not None:
                params["hedged"] = True
                if self.api.id in ("phemex", "okx"):
                    params["posSide"] = order.position_side.capitalize() if self.api.id == "phemex" else order.position_side
                if order.side != ("buy" if order.position_side == "long" else "sell"):
                    params["reduceOnly"] = True
            params.update(order.custom_params or {})
            if self.api.id == "phemex":
                params.setdefault("clientOrderId", str(order.identifier))
            if market.get("contract") and not market.get("option") and not params.get("reduceOnly"):
                self._apply_asset_leverage(order.asset, pair)
            client_id = params.get("clientOrderId")
            if client_id:
                self._pending_client_orders[str(client_id)] = order
            args = self.create_order_args(order)
            submission_started = True
            response = self.api.create_order(*args, params=params)
            accepted_response = response
            with self._lock:
                if response.get("id") is None:
                    order.status = "unknown"
                    order.error_message = "Submission outcome unknown: exchange returned no order ID"
                    self._unprocessed_orders.append(order)
                    return order
                order.set_identifier(response["id"])
                self._submitted_orders[str(order.identifier)] = order
                self._apply_ccxt_order_observation(order, response)
                queued = self._stream_orders.get(str(order.identifier))
                if queued is not None:
                    self._apply_ccxt_order_observation(order, queued)
                if client_id:
                    self._pending_client_orders.pop(str(client_id), None)
        except Exception as exc:
            import ccxt

            if accepted_response is not None and accepted_response.get("id") is not None:
                order.error_message = f"Accepted order observation could not be applied: {type(exc).__name__}; reconcile"
                order.update_raw(accepted_response)
                if not order._new_event.is_set():
                    order.status = "unknown"
                    self._unprocessed_orders.append(order)
            elif submission_started and isinstance(exc, ccxt.NetworkError):
                order.status = "unknown"
                order.error_message = f"Submission outcome unknown after {type(exc).__name__}; reconcile before retrying"
                self._unprocessed_orders.append(order)
            else:
                if client_id:
                    self._pending_client_orders.pop(str(client_id), None)
                order.set_error(exc)
            logger.error("CCXT order submission for %s: %s", order.pair, type(exc).__name__)
        return order

    def _apply_asset_leverage(self, asset, pair):
        """Set ``asset.leverage`` on opening contract orders, as the Bitunix broker does.

        Only a leverage above 1 is applied: ``Asset`` defaults to 1, and treating that
        default as an instruction would reset leverage a user set with ``set_leverage``.
        A failure is logged and the order still goes, matching Bitunix.
        """
        leverage = getattr(asset, "leverage", 1) or 1
        if leverage <= 1:
            return
        applied = self.__dict__.setdefault("_applied_leverage", {})
        if applied.get(pair) == leverage:
            return
        try:
            self.api.set_leverage(leverage, pair, {})
            applied[pair] = leverage
            logger.info("Set leverage for %s to %sx", pair, leverage)
        except Exception as exc:
            logger.warning("Could not set leverage for %s to %sx: %s", pair, leverage, exc)

    @staticmethod
    def _check_market_limit(field, value, limits):
        for bound, limit in (limits.get(field) or {}).items():
            if limit is None:
                continue
            limit = Decimal(str(limit))
            if (bound == "min" and value < limit) or (bound == "max" and value > limit):
                raise ValueError(f"CCXT {field} {value} exceeds market {bound} {limit}")

    def create_order_args(self, order):
        """Build the unified symbol, type, side, coin quantity and optional price."""
        order_type = {"stop": "market", "stop_limit": "limit"}.get(order.order_type, order.order_type)
        price = str(order.limit_price) if order.limit_price is not None else None
        if order_type == "market" and order.side == "buy" and self.api.options.get("createMarketBuyOrderRequiresPrice"):
            price = self.data_source.get_last_price(order.asset, quote=order.quote)
            if price is None:
                raise ValueError(f"A price is required to calculate market-buy cost for {order.pair}")
            price = str(price)
        return [order.pair, order_type, order.side, str(order.quantity), price]

    def cancel_order(self, order):
        """Cancel using the unified market identity; only confirm terminal responses."""
        pair = self.get_market(order.pair or order.asset, order.quote)["symbol"]
        params = self._position_request_params(order)
        response = self.api.cancel_order(order.identifier, pair, **({"params": params} if params else {}))
        if str(response.get("id")) == str(order.identifier) and response.get("status") in ("canceled", "cancelled", "expired"):
            with self._lock:
                self._apply_ccxt_order_observation(order, response)
        return response

    def _position_request_params(self, order):
        if order.position_side is None:
            return {}
        if self.api.id == "phemex":
            return {"posSide": order.position_side.capitalize()}
        if self.api.id == "okx":
            return {"posSide": order.position_side}
        return {}

    def cancel_open_orders(self, strategy, orders: list[Order] | None = None):
        """Cancel the supplied orders or active orders owned by the strategy."""
        strategy_name = getattr(strategy, "name", strategy)
        candidates = orders if orders is not None else self.get_tracked_orders(strategy_name)
        for order in candidates:
            if order.strategy == strategy_name and order.is_active():
                self.cancel_order(order)

    def _modify_order(self, order, limit_price=None, stop_price=None):
        if not order.is_active():
            return None
        pair = self.get_market(order.pair or order.asset, order.quote)["symbol"]
        price = order.limit_price if limit_price is None else limit_price
        trigger = order.stop_price if stop_price is None else stop_price
        if price is not None:
            price = self.api.price_to_precision(pair, price)
        params = {**(order.custom_params or {}), **self._position_request_params(order)}
        if trigger is not None:
            params["triggerPrice"] = self.api.price_to_precision(pair, trigger)
        order_type = {"stop": "market", "stop_limit": "limit"}.get(order.order_type, order.order_type)
        response = self.api.edit_order(order.identifier, pair, order_type, order.side,
                                       self.api.amount_to_precision(pair, order.quantity), price, params=params)
        if limit_price is not None:
            order.limit_price = Decimal(str(price))
        if stop_price is not None:
            order.stop_price = Decimal(str(params["triggerPrice"]))
        with self._lock:
            self._apply_ccxt_order_observation(order, response)
        return order

    def get_historical_account_value(self):
        logger.error("The function get_historical_account_value is not " "implemented yet for CCXT.")
        return {"hourly": None, "daily": None}

    def wait_for_order_registrations(self, orders):
        return self.wait_for_orders_registration(orders)

    def wait_for_order_executions(self, orders):
        return self.wait_for_orders_execution(orders)

    def _get_stream_object(self):
        from lumibot.trading_builtins.ccxt_stream import CcxtStream

        stream = CcxtStream(self.api, self._config, on_orders=self._on_ccxt_orders,
                            on_balance=self._on_ccxt_balance)
        self.data_source._stream = stream
        return stream

    def _launch_stream(self):
        # Pro construction, network activity and authentication stay lazy.
        # A subscription starts the one managed event loop when needed.
        self._register_stream_events()

    def _register_stream_events(self):
        return

    def _run_stream(self):
        self.stream.start()

    def _on_ccxt_balance(self, balance):
        self._contract_accounts_cache = None
        self._stream_established()

    def _on_ccxt_orders(self, orders, scope, snapshot=False):
        scope_key = tuple(sorted(scope.items()))
        with self._lock:
            if snapshot:
                for identifier in list(self._stream_orders):
                    if self._stream_order_scopes.get(identifier) == scope_key:
                        del self._stream_orders[identifier]
                        self._stream_order_scopes.pop(identifier, None)
            for response in orders:
                identifier = str(response["id"])
                local = self.get_tracked_order(identifier) or self._submitted_orders.get(identifier)
                if local is None and response.get("clientOrderId"):
                    local = self._pending_client_orders.get(str(response["clientOrderId"]))
                    if local is not None:
                        local.set_identifier(response["id"])
                        self._invalidate_order_caches()
                if local is not None and local._closed_event.is_set() and response.get("status") in ("open", "new", "pending"):
                    continue
                previous = self._stream_orders.get(identifier) or {}
                if response.get("filled") is not None and previous.get("filled") is not None:
                    if Decimal(str(response["filled"])) < Decimal(str(previous["filled"])):
                        continue
                self._stream_orders[identifier] = dict(response)
                self._stream_order_scopes[identifier] = scope_key
                if local is not None:
                    self._apply_ccxt_order_observation(local, response)
            # Retain all open orders, with a bounded terminal replay cache.
            terminal = [key for key, value in self._stream_orders.items()
                        if value.get("status") not in ("open", "new", "pending")]
            for identifier in terminal[:-1000]:
                del self._stream_orders[identifier]
                self._stream_order_scopes.pop(identifier, None)
        self._stream_established()

    def _apply_ccxt_order_observation(self, order, response):
        """Reduce cumulative REST/WS observations through the shared lifecycle."""
        status = response.get("status")
        already = sum((Decimal(str(tx.quantity)) for tx in order.transactions), Decimal("0"))
        reported = response.get("filled")
        filled = Decimal(str(reported)) if reported is not None else already
        if filled < already or (order.is_filled() and status in ("open", "new", "pending")):
            return False
        delta = filled - already
        changed = False
        if not order._new_event.is_set() and status not in ("rejected", "error"):
            self._process_trade_event(order, self.NEW_ORDER)
            changed = True
        if delta > 0:
            market = self.api.markets.get(order.pair or order.asset.symbol) or {}
            size = Decimal(str(market.get("contractSize") or 1))
            inverse = market.get("inverse", False)
            previous_cost = sum((Decimal(str(tx.quantity)) * size / Decimal(str(tx.price)) if inverse
                                 else Decimal(str(tx.quantity)) * size * Decimal(str(tx.price))
                                 for tx in order.transactions), Decimal("0"))
            cost = response.get("cost")
            average = response.get("average")
            if cost is None and average is not None:
                average = Decimal(str(average))
                cost = filled * size / average if inverse else filled * size * average
            if cost is None:
                raise ValueError(f"CCXT execution omitted price for order {order.identifier}")
            delta_cost = Decimal(str(cost)) - previous_cost
            if delta_cost <= 0:
                raise ValueError(f"CCXT execution returned invalid cumulative cost for order {order.identifier}")
            price = delta * size / delta_cost if inverse else delta_cost / (delta * size)
            if price <= 0:
                raise ValueError(f"CCXT execution returned an invalid price for order {order.identifier}")
            event = self.FILLED_ORDER if status == "closed" or filled >= Decimal(str(order.quantity)) else self.PARTIALLY_FILLED_ORDER
            self._process_trade_event(order, event, price=float(price), filled_quantity=float(delta),
                                      multiplier=order.asset.multiplier)
            changed = True
        if status in ("canceled", "cancelled", "expired") and not order.is_canceled():
            self._process_trade_event(order, self.CANCELED_ORDER)
            changed = True
        elif status in ("rejected", "error") and order.status != "error":
            self._process_trade_event(order, self.ERROR_ORDER, error=response.get("error") or "Exchange rejected order")
            changed = True
        elif status in ("open", "new") and order.status in ("unprocessed", "submitted") and not filled:
            self._process_trade_event(order, self.NEW_ORDER)
            changed = True
        if response.get("average") is not None and filled:
            order.avg_fill_price = response["average"]
        fees = response.get("fees") or ([response["fee"]] if response.get("fee") else [])
        if not fees and self.api.id == "phemex":
            info = response.get("info") or {}
            market = self.api.markets.get(order.pair or order.asset.symbol) or {}
            currency = info.get("feeCurrency") or market.get("settle")
            if currency and ("cumFeeRv" in info or "cumFeeEv" in info):
                if "cumFeeRv" in info:
                    fee = Decimal(str(info["cumFeeRv"]))
                else:
                    scale = (self.api.currency(currency) or {}).get("valueScale", 8)
                    fee = Decimal(str(info["cumFeeEv"])) / Decimal(10) ** int(scale)
                fees = [{"currency": currency, "cost": float(fee)}]
        if fees:
            order.fees = fees
        order.update_raw(response)
        if order._closed_event.is_set():
            self._submitted_orders.pop(str(order.identifier), None)
            for client_id, pending in list(self._pending_client_orders.items()):
                if pending is order:
                    del self._pending_client_orders[client_id]
        return changed

    def _apply_direct_order_observation(self, order_lumi, matched_order):
        with self._lock:
            return self._apply_ccxt_order_observation(order_lumi, matched_order._raw)
