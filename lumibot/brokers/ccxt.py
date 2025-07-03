import datetime
import logging
import os
from decimal import ROUND_DOWN, Decimal, getcontext
from typing import Union

from lumibot.data_sources import CcxtData
from lumibot.entities import Asset, Order, Position
from termcolor import colored

from .broker import Broker


class Ccxt(Broker):
    """
    Crypto broker using CCXT.
    """

    def __init__(self, config, data_source: CcxtData = None, max_workers=20, chunk_size=100, **kwargs):
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

    # =========Clock functions=====================

    def get_timestamp(self):
        """Returns the current UNIX timestamp representation from CCXT"""
        logging.warning("The method 'get_time_to_close' is not applicable with Crypto 24/7 markets.")
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
        logging.warning("The method 'get_time_to_open' is not applicable with Crypto 24/7 markets.")
        return None

    def get_time_to_close(self):
        """Not applicable with Crypto 24/7 markets.

        Returns
        -------
        None
        """
        logging.debug("The method 'get_time_to_close' is not applicable with Crypto 24/7 markets.")
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
    def _get_balances_at_broker(self, quote_asset, strategy):
        """Get's the current actual cash, positions value, and total
        liquidation value from ccxt.

        This method will get the current actual values from ccxt broker
        for the actual cash, positions value, and total liquidation.

        Best attempts will be made to use USD as a base currency.

        Returns
        -------
        tuple of float
            (cash, positions_value, total_liquidation_value)
        """
        total_cash_value = 0
        positions_value = 0
        # Get the market values for each pair held.
        balances = self._fetch_balance()

        currency_key = "currency"
        if self.api.exchangeId in ["coinbasepro", "kucoin", "kraken", "coinbase", "binance", "bitmex"]:
            balances_info = []
            reserved_keys = ["total", "free", "used", "info", "timestamp", "datetime", "debt"]
            for key in balances:
                if key in reserved_keys:
                    continue
                bal = balances[key]["total"]
                if Decimal(bal) != Decimal("0"):
                    balances_info.append({"currency": key, "balance": bal})
        else:
            raise NotImplementedError(f"{self.api.exchangeId} not implemented yet.")

        no_valuation = []
        for currency_info in balances_info:
            currency = currency_info[currency_key]

            if currency == quote_asset.symbol:
                total_cash_value = Decimal(currency_info["balance"])
                continue

            # Check for three USD markets.
            market_string = f"{currency}/{quote_asset.symbol}"
            market = None
            if market_string in self.api.markets:
                market = market_string

            if market is None:
                no_valuation.append(currency)
                continue

            precision_amount = self.api.markets[market]["precision"]["amount"]
            precision_price = self.api.markets[market]["precision"]["price"]

            if self.api.exchangeId == "binance":
                precision_amount = 10**-precision_amount
                precision_price = 10**-precision_price

            total_balance = currency_info["balance"]

            units = Decimal(total_balance)

            attempts = 0
            max_attempts = 3
            while attempts < max_attempts:
                last_price = self.api.fetch_ticker(market)["last"]
                if last_price is None:
                    attempts += 1
                else:
                    attempts = max_attempts

            if last_price is None:
                last_price = 0

            price = Decimal(last_price)

            value = units * price
            positions_value += value

        if len(no_valuation) > 0:
            logging.info(
                f"The coins {no_valuation} have no valuation in {quote_asset} "
                f"and therefore could not be added to the portfolio when calculating "
                f"the total value of the holdings."
            )

        total_cash_value = float(total_cash_value)
        gross_positions_value = float(positions_value) + total_cash_value
        net_liquidation_value = float(positions_value) + total_cash_value

        return (total_cash_value, gross_positions_value, net_liquidation_value)

    def _parse_broker_position(self, position, strategy, orders=None):
        """parse a broker position representation
        into a position object"""

        symbol = position["currency"]
        hold = position["used"]
        available = position["free"]
        quantity = Decimal(position["total"])

        # Check if symbol is in the currencies list
        if symbol not in self.api.currencies:
            logging.error(
                f"The symbol {symbol} is not in the currencies list. "
                f"Please check the symbol and the exchange currencies list."
            )
            precision = None

        elif self.api.exchangeId == "binance":
            precision = str(10 ** -self.api.currencies[symbol]["precision"])

        else:
            precision = str(self.api.currencies[symbol]["precision"])

        asset = Asset(
            symbol=symbol,
            asset_type="crypto",
            precision=precision,
        )

        position_return = Position(strategy, asset, quantity, hold=hold, available=available, orders=orders)
        return position_return

    def _pull_broker_position(self, asset):
        """Given a asset, get the broker representation
        of the corresponding asset"""

        position = None
        if self.api.exchangeId == "binance":
            for position in self._pull_broker_positions():
                if position["currency"] == asset.symbol:
                    return position
        else:
            position = self._pull_broker_positions()["info"][asset.symbol]
        return position

    def _pull_broker_positions(self, strategy=None):
        """Get the broker representation of all positions"""
        response = self._fetch_balance()

        if self.api.exchangeId in ["kraken", "kucoin", "coinbasepro", "coinbase", "binance", "bitmex"]:
            balances_info = []
            reserved_keys = [
                "total",
                "free",
                "used",
                "info",
                "timestamp",
                "datetime",
                "debt",
                strategy.quote_asset.symbol if strategy else None,
            ]
            for key in response:
                if key in reserved_keys:
                    continue
                bals = response[key]
                if Decimal(bals["total"]) != Decimal("0"):
                    bals["currency"] = key
                    balances_info.append(bals)

            return balances_info
        else:
            raise NotImplementedError(
                f"{self.api.exchangeId} not implemented yet. \
                                      If you think this is incorrect, then please check \
                                      the exact spelling of the exchangeId."
            )

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
        result = self._parse_broker_position(response, strategy)
        return result

    # =======Orders and assets functions=========
    def _parse_broker_order(self, response, strategy_name, strategy_object=None):
        """parse a broker order representation
        to an order object"""
        pair = response["symbol"].split("/")
        order = Order(
            strategy_name,
            Asset(
                symbol=pair[0],
                asset_type="crypto",
            ),
            response["amount"],
            response["side"],
            limit_price=response["price"],
            stop_price=response["stopPrice"],
            time_in_force=response["timeInForce"].lower() if response["timeInForce"] else None,
            quote=Asset(
                symbol=pair[1],
                asset_type="crypto",
            ),
            order_type=response["type"] if "type" in response else None,
        )
        order.set_identifier(response["id"])
        order.status = response["status"]
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

        if self.api.id == "kraken":  # Check if the exchange is Kraken
            logging.info("Detected Kraken exchange. Not sending params for closed orders.")
            params = None  # Ensure no parameters are sent
        elif self.is_margin_enabled():
            params["tradeType"] = "MARGIN_TRADE"

        closed_orders = self.api.fetch_closed_orders(params)

        return closed_orders

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
                    logging.info(
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
        """Submit an order for an asset"""

        # Check if order has a quantity
        if not hasattr(order, "quantity") or order.quantity is None:
            raise ValueError(f"Order {order} does not have a quantity.")

        # Check that order quantity is a numeric type
        if not isinstance(order.quantity, (int, float, Decimal)):
            raise ValueError(f"Order quantity must be a numeric type, not {type(order.quantity)}")

        # Check if order quantity is greater than 0.
        if order.quantity <= 0:
            logging.warning(f"The order {order} was rejected as the order quantity is 0 or less.")
            return

        # Orders limited.
        order_class = None
        order_types = ["market", "limit", "stop_limit"]
        # TODO: Is this actually true?? Try testing this with a bunch of different exchanges.
        markets_error_message = "Only `market`, `limit`, or `stop_limit` orders work with crypto currency markets."

        if order.order_class != order_class:
            logging.error(f"A compound order of {order.order_class} was entered. " f"{markets_error_message}")
            return

        if order.order_type not in order_types:
            logging.error(f"An order type of {order.order_type} was entered which is not " f"valid. {markets_error_message}")
            return

        # Check order within limits.
        market = self.api.markets.get(order.pair, None)
        if market is None:
            logging.error(f"An order for {order.pair} was submitted. The market for that pair does not exist")
            order.set_error("No market for pair.")
            return order

        limits = market["limits"]
        precision = market["precision"]
        if self.api.exchangeId in ["binance", "kucoin"]:
            precision_amount = Decimal(str(10 ** -precision["amount"]))
        elif self.api.exchangeId == "kraken":
            initial_precision_amount = Decimal(str(precision["amount"]))

            # Remove a few decimal places because Kraken precision amount is wrong and it's causing orders to fail.
            precision_exp_modifier = 2
            initial_precision_exp = abs(initial_precision_amount.as_tuple().exponent)
            new_precision_exp = initial_precision_exp - precision_exp_modifier
            factor = 10**new_precision_exp
            precision_amount = Decimal(1) / Decimal(factor)
        else:
            # Set the precision for the Decimal context
            getcontext().prec = 8
            getcontext().rounding = ROUND_DOWN
            decimal_value = Decimal(precision["amount"])
            precision_amount = decimal_value.quantize(Decimal("1e-{0}".format(8)), rounding=ROUND_DOWN)

        # Convert the amount to Decimal.
        if hasattr(order, "quantity") and getattr(order, "quantity") is not None:
            qty = Decimal(getattr(order, "quantity"))

            # Calculate the precision factor as the reciprocal of precision_amount
            precision_factor = Decimal("1") / precision_amount

            new_qty = (qty * precision_factor).to_integral_value(rounding="ROUND_DOWN") / precision_factor

            if new_qty <= Decimal(0):
                logging.warning(
                    f"The order {order} was rejected as the order quantity is 0 or less after rounding down to the exchange minimum precision amount of {precision_amount}."
                )
                return

            setattr(
                order,
                "quantity",
                new_qty,
            )

            # TODO: Remove this if really not needed by several brokers (keeping for now because it's a big change and need to monitor first).
            # try:
            #     if limits["amount"]["min"] is not None:
            #         assert Decimal(order.quantity) >= Decimal(limits["amount"]["min"])
            # except AssertionError:
            #     logging.warning(
            #         f"\nThe order {order} was rejected as the order quantity \n"
            #         f"was less then the minimum allowed for {order.pair}. The minimum order quantity is {limits['amount']['min']} \n"
            #         f"The quantity for this order was {order.quantity} \n"
            #     )
            #     return

            # try:
            #     if limits["amount"]["max"] is not None:
            #         assert order.quantity <= limits["amount"]["max"]
            # except AssertionError:
            #     logging.warning(
            #         f"\nThe order {order} was rejected as the order quantity \n"
            #         f"was greater then the maximum allowed for {order.pair}. The maximum order "
            #         f"quantity is {limits['amount']['max']} \n"
            #         f"The quantity for this order was {order.quantity} \n"
            #     )
            #     return

        # Convert the price to Decimal.
        for price_type in [
            "limit_price",
            "stop_price",
        ]:
            if hasattr(order, price_type) and getattr(order, price_type) is not None:
                precision_price = Decimal(str(10 ** -precision["price"])
                                          ) if self.api.exchangeId == "binance" else precision["price"]
                setattr(
                    order,
                    price_type,
                    Decimal(getattr(order, price_type)).quantize(Decimal(str(precision_price))),
                )
            else:
                continue

            try:
                if limits["price"]["min"] is not None:
                    assert getattr(order, price_type) >= limits["price"]["min"]
            except AssertionError:
                logging.warning(
                    f"\nThe order {order} was rejected as the order {price_type} \n"
                    f"was less then the minimum allowed for {order.pair}. The minimum price "
                    f"is {limits['price']['min']} \n"
                    f"The price for this order was {getattr(order, price_type):4.9f} \n"
                )
                return

            try:
                if limits["price"]["max"] is not None:
                    assert getattr(order, price_type) <= limits["price"]["max"]
            except AssertionError:
                logging.warning(
                    f"\nThe order {order} was rejected as the order {price_type} \n"
                    f"was greater then the maximum allowed for {order.pair}. The maximum price "
                    f"is {limits['price']['max']} \n"
                    f"The price for this order was {getattr(order, price_type):4.9f} \n"
                )
                return

            try:
                if limits["cost"]["min"] is not None:
                    price = Decimal(getattr(order, price_type))
                    qty = Decimal(order.quantity)
                    limit_min = Decimal(limits["cost"]["min"])
                    assert (price * qty) >= limit_min
            except AssertionError:
                logging.warning(
                    f"\nThe order {order} was rejected as the order total cost \n"
                    f"was less then the minimum allowed for {order.pair}. The minimum cost "
                    f"is {limits['cost']['min']} \n"
                    f"The cost for this order was only {(getattr(order, price_type) * order.quantity):4.9f} \n"
                )
                return

            try:
                if limits["cost"]["max"] is not None:
                    assert getattr(order, price_type) * order.quantity <= Decimal(limits["cost"]["max"])
            except AssertionError:
                logging.warning(
                    f"\nThe order {order} was rejected as the order total cost \n"
                    f"was greater then the maximum allowed for {order.pair}. The maximum cost "
                    f"is {limits['cost']['max']} \n"
                    f"The cost for this order was {(getattr(order, price_type) * order.quantity):4.9f} \n"
                )
                return
        args = self.create_order_args(order)

        params = {}

        if self.is_margin_enabled() and self.api.exchangeId != "binance":
            params["tradeType"] = "MARGIN_TRADE"

        # if self.api.exchangeId == "coinbase" and order.order_type == "market":
        #     params["createMarketBuyOrderRequiresPrice"] = False

        try:
            # Add order.custom_params to params
            if hasattr(order, "custom_params") and order.custom_params is not None:
                params.update(order.custom_params)

            response = self.api.create_order(*args, params=params)
            order.set_identifier(response["id"])
            order.status = response["status"]
            order.update_raw(response)

        except Exception as e:
            order.set_error(e)
            message = str(e)
            full_message = f"{order} did not go through. The following error occurred: {message}"
            logging.info(colored(full_message, "red"))

        return order

    def create_order_args(self, order):
        """Will create the args for the ccxt `create_order` submission.

        Creating the order args. There are only a few acceptable lumibot
        orders. These are:
            market, limit, stop_limit

        There is no stop or trailing orders. Also combo orders do not
        work in crypto. So no bracket or oco orders.

        The args are complicated and will vary for each broker. All new
        broker conditions should be fully documented as below.

        The main arguments for `api.create_order()` are:
            symbol: always the pairing symbol.
            type: order types vary with broker, see below.
            side: buy or sell
            amount: string quantity to buy or sell
            price=None: Optional, use for limit pricing.
            {params}: custom parameters.

        For Binance the args are as follows:
            Allowable orders:
                orderTypes:
                    'MARKET'
                    'LIMIT',
                    'STOP_LOSS_LIMIT'
                custom parameters dict:
                    {'stopPrice': string price}

            Examples of the args for binance are as follows:

            Market Order
            api.create_order("BTC/USDT", "MARKET", "buy", "0.0999")

            Limit Order
            api.create_order("BTC/USDT", "LIMIT", "buy", "0.0888", "40000")
            api.create_order("BTC/USDT", "LIMIT", "sell", "0.0777", "40000")

            Stop Entry Limit Order
            api.create_order(
                "BTC/USDT",
                "TAKE_PROFIT_LIMIT",
                "sell",
                ".0777",
                40100,
                {"stopPrice": 40000},
            )

            Stop Loss Limit Order
            api.create_order(
                "BTC/USDT",
                "STOP_LOSS_LIMIT",
                "sell",
                "0.0666",
                40100,
                {"stopPrice": 40000}
            )

        For Coinbase the args are as follows:
            Allowable orders:
                orderTypes:
                    `market`
                    `limit`
                custom parameters dict:
                    {
                    `stop`: `loss` or `entry`
                    `stop_price`: string price
                    }

            Examples of the args for coinbase are as follows:

            Market Order
            api.create_order("BTC/USD", "market", side, amount)

            Limit Order
            api.create_order("BTC/USD", "limit", side, amount, "40000")
            api.create_order("BTC/USD", "limit", side, amount, "40000")

            Buy Stop Entry
            api.create_order(
                "BTC/USD",
                "limit",
                "buy",
                "0.123",
                "40100",
                {"stop": "entry", "stop_price": "40000"},
            )

            Sell Stop Loss
            order = api.create_order(
                "BTC/USD",
                "limit",
                "sell",
                "0.111",
                "40100",
                {"stop": "loss", "stop_price": "40000"},
            )


        Parameters
        ----------
        order

        Returns
        -------
        create_order api arguments : dict

        """
        broker = self.api.exchangeId
        if broker == "binance":
            params = {}
            if order.order_type in ["stop_limit"]:
                params = {
                    "stopPrice": str(order.stop_price),
                }
                # Remove items with None values
                params = {k: v for k, v in params.items() if v}

            order_type_map = dict(market="MARKET", limit="LIMIT", stop_limit="STOP_LOSS_LIMIT")

            args = [
                order.pair,
                order_type_map[order.order_type],
                order.side,
                str(order.quantity),
            ]
            if order.order_type in ["limit", "stop_limit"]:
                args.append(str(order.limit_price))

            if len(params) > 0:
                args.append(params)

            return args

        elif broker in ["kraken", "kucoin", "coinbasepro", "coinbase"]:
            params = {}
            if order.order_type in ["stop_limit"]:
                params = {
                    "stop": "entry" if order.side == "buy" else "loss",
                    "stop_price": str(order.stop_price),
                }

                # Remove items with None values
                params = {k: v for k, v in params.items() if v}

            order_type_map = dict(
                market="market",
                stop="market",
                limit="limit",
                stop_limit="limit",
            )

            args = [
                order.pair,
                order_type_map[order.order_type],
                order.side,
                str(order.quantity),  # check this with coinbase.
            ]
            if order_type_map[order.order_type] == "limit":
                args.append(str(order.limit_price))

            # If coinbase, you need to pass the price even with a market order
            if broker == "coinbase" and order_type_map[order.order_type] == "market":
                price = self.data_source.get_last_price(order.asset, quote=order.quote)
                args.append(str(price))

            if len(params) > 0:
                args.append(params)

            return args

        else:
            raise ValueError(
                f"An attempt was made to use the broker {broker} which is "
                f"not an approved broker. Please refer to the Lumibot docs"
                f"to get a list of currently approved brokers."
            )

    def cancel_order(self, order):
        """Cancel an order"""
        if self.api.exchangeId == "binance":
            response = self.api.cancel_order(order.identifier, order.pair)
            if order.identifier == response["id"]:
                order.set_canceled()
        else:
            response = self.api.cancel_order(order.identifier, order.symbol)
            if order.identifier == response:
                order.set_canceled()

    def cancel_open_orders(self, strategy):
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
        logging.error("The function get_historical_account_value is not " "implemented yet for CCXT.")
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
