from __future__ import annotations

import os

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
        params = {}

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

    def _get_balances_at_broker(self, quote_asset, strategy):
        """Return cash, non-cash holdings value, and equity in the quote currency."""
        cash = Decimal("0")
        positions_value = Decimal("0")
        self._ensure_markets_loaded()
        for row in self._balance_rows(self._fetch_balance()):
            currency, units = row["currency"], Decimal(str(row["total"]))
            if currency == quote_asset.symbol:
                cash = units
                continue
            direct = f"{currency}/{quote_asset.symbol}"
            inverse = f"{quote_asset.symbol}/{currency}"
            symbol = direct if direct in self.api.markets else inverse
            if symbol not in self.api.markets:
                logger.warning("No %s valuation market for %s; excluded from CCXT equity", quote_asset, currency)
                continue
            price = self.api.fetch_ticker(symbol).get("last")
            if price is None or Decimal(str(price)) <= 0:
                raise ValueError(f"No usable CCXT valuation price for {symbol}")
            price = Decimal(str(price))
            positions_value += units * price if symbol == direct else units / price
        return float(cash), float(positions_value), float(cash + positions_value)

    def _parse_broker_position(self, position, strategy, orders=None):
        """parse a broker position representation
        into a position object"""
        Position = _position_class()

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

    def _pull_broker_position(self, asset):
        return next((row for row in self._pull_broker_positions()
                     if row["currency"] == asset.symbol), None)

    def _pull_broker_positions(self, strategy=None):
        """Read spot holdings using the unified balance contract on every exchange."""
        quote = strategy.quote_asset.symbol if strategy is not None else None
        return [row for row in self._balance_rows(self._fetch_balance()) if row["currency"] != quote]

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

    def _pull_position(self, strategy, asset):
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
        response = self._pull_broker_position(asset)
        return self._parse_broker_position(response, getattr(strategy, "name", strategy)) if response else None

    # =======Orders and assets functions=========
    def _parse_broker_order(self, response, strategy_name, strategy_object=None):
        """parse a broker order representation
        to an order object"""
        base, quote = response["symbol"].split("/", 1)
        order_type = response.get("type") or "market"
        trigger = response.get("triggerPrice") or response.get("stopPrice")
        if trigger is not None and order_type in ("market", "limit"):
            order_type = "stop" if order_type == "market" else "stop_limit"
        order = Order(
            strategy_name, Asset(base, asset_type="crypto"), response["amount"], response["side"],
            limit_price=response.get("price") if order_type in ("limit", "stop_limit") else None,
            stop_price=trigger,
            time_in_force=(response.get("timeInForce") or "gtc").lower(),
            quote=Asset(quote, asset_type="crypto"), order_type=order_type,
            avg_fill_price=response.get("average"),
        )
        order.set_identifier(response["id"])
        order.status = response.get("status") or "new"
        order.update_raw(response)
        return order

    def _pull_broker_order(self, identifier):
        """Get a broker order representation by its id"""
        open_orders = self._pull_broker_all_orders()
        closed_orders = self._pull_broker_closed_orders()
        all_orders = open_orders + closed_orders

        response = [order for order in all_orders if order["id"] == identifier]

        return response[0] if len(response) > 0 else None

    def _pull_broker_closed_orders(self):
        params = {}
        if self.is_margin_enabled() and self.api.id == "kucoin":
            params["tradeType"] = "MARGIN_TRADE"
        return self.api.fetch_closed_orders(params=params)

    def _pull_broker_all_orders(self):
        """Get the broker open orders"""
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
        """Some submitted orders may trigger other orders.
        _flatten_order returns a list containing the main order
        and all the derived ones"""
        orders = [order]

        if self.api.exchangeId == "binance":
            return orders

        if "legs" in order._raw and order._raw.legs:
            strategy_name = order.strategy
            for json_sub_order in order._raw.legs:
                sub_order = self._parse_broker_order(json_sub_order, strategy_name)
                orders.append(sub_order)

        return orders

    def _submit_order(self, order):
        """Submit through CCXT's unified order API, preserving native parameters."""
        if order.order_class not in (None, "simple"):
            order.set_error("CCXT compound order lifecycle is not implemented; use native attached-order parameters")
            return order
        if order.quantity is None or order.quantity <= 0:
            order.set_error("Order quantity must be greater than zero")
            return order
        self._ensure_markets_loaded()
        from lumibot.tools.symbol_normalization import resolve_ccxt_market_symbol

        pair = resolve_ccxt_market_symbol(self.api.markets, order.pair, exchange_id=self.api.id)
        if pair is None:
            order.set_error(f"No CCXT market for {order.pair}")
            return order
        order.pair = pair
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
                self._check_market_limit("cost", order.limit_price * order.quantity, limits)
            params = {}
            if self.is_margin_enabled() and self.api.id == "kucoin":
                params["tradeType"] = "MARGIN_TRADE"
            if order.stop_price is not None:
                params["triggerPrice"] = str(order.stop_price)
            if order.time_in_force and order.time_in_force.lower() != "day":
                params["timeInForce"] = order.time_in_force.upper()
            params.update(order.custom_params or {})
            response = self.api.create_order(*self.create_order_args(order), params=params)
            order.set_identifier(response["id"])
            order.status = response.get("status") or "new"
            order.update_raw(response)
        except Exception as exc:
            order.set_error(exc)
            logger.error("CCXT order submission failed for %s: %s", order.pair, type(exc).__name__)
        return order

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
        response = self.api.cancel_order(order.identifier, order.pair)
        if str(response.get("id")) == str(order.identifier) and response.get("status") in ("canceled", "cancelled", "expired"):
            order.status = response["status"]
            order.set_canceled()
        return response

    def cancel_open_orders(self, strategy, orders: list[Order] | None = None):
        """Cancel all open orders at the broker."""
        for order in self._pull_broker_all_orders():
            self.api.cancel_order(order["id"], symbol=order["symbol"])

    def _modify_order(self, order: Order, limit_price: Union[float, None] = None,
                      stop_price: Union[float, None] = None):
        """
        Modify an order at the broker. Nothing will be done for orders that are already cancelled or filled. You are
        only allowed to change the limit price and/or stop price. If you want to change the quantity,
        you must cancel the order and submit a new one.
        """
        raise NotImplementedError("CCXTBroker modify order is not implemented.")

    def get_historical_account_value(self):
        logger.error("The function get_historical_account_value is not " "implemented yet for CCXT.")
        return {"hourly": None, "daily": None}

    def wait_for_order_registration(self, order):
        """Wait for the registration of the orders with the broker.

        Not yet implemented, requires streaming.
        """
        raise NotImplementedError(
            "Waiting for an order registration is not yet implemented in Crypto, "
            "requires streaming. Check the order status at each interval."
        )

    def wait_for_order_registrations(self, orders):
        """Wait for the registration of the order with the broker.

        Not yet implemented, requires streaming.
        """
        raise NotImplementedError(
            "Waiting for an order registration is not yet implemented in Crypto, "
            "requires streaming. Check the order status at each interval."
        )

    def wait_for_order_execution(self, order):
        """Wait for order to fill.

        Not yet implemented, requires streaming.
        """
        raise NotImplementedError(
            "Waiting for an order execution is not yet implemented in Crypto, "
            "requires streaming. Check the order status at each interval."
        )

    def wait_for_order_executions(self, order):
        """Wait for orders to fill.

        Not yet implemented, requires streaming.
        """
        raise NotImplementedError(
            "Waiting for an order execution is not yet implemented in Crypto, "
            "requires streaming. Check the order status at each interval."
        )

    def _get_stream_object(self):
        pass

    def _register_stream_events(self):
        pass

    def _run_stream(self):
        pass
