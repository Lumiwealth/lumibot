import datetime
import logging
# from asyncio import CancelledError
from decimal import ROUND_DOWN, Decimal

from lumibot.data_sources import CcxtData
from lumibot.entities import Asset, Order, Position
from termcolor import colored

from .broker import Broker


class Ccxt(CcxtData, Broker):
    """Inherit CcxtData first and all the price market
    methods than inherits broker

    """

    def __init__(self, config, max_workers=20, chunk_size=100, connect_stream=False):
        # Calling init methods
        CcxtData.__init__(self, config, max_workers=max_workers, chunk_size=chunk_size)
        Broker.__init__(self, name="ccxt", connect_stream=connect_stream)

        self.market = "24/7"
        self.fetch_open_orders_last_request_time = None
        self.binance_all_orders_rate_limit = 5

    # =========Clock functions=====================

    def get_timestamp(self):
        """Returns the current UNIX timestamp representation from CCXT.

        Parameters
        ----------
        None
        """
        logging.warning(
            "The method 'get_time_to_close' is not applicable with Crypto 24/7 markets."
        )
        return self.api.microseconds() / 1000000

    def is_market_open(self):
        """Not applicable with Crypto 24/7 markets.

        Returns
        -------
        None
        """
        logging.warning(
            "The method 'is_market_open' is not applicable with Crypto 24/7 markets."
        )
        return None

    def get_time_to_open(self):
        """Not applicable with Crypto 24/7 markets.

        Returns
        -------
        None
        """
        logging.warning(
            "The method 'get_time_to_open' is not applicable with Crypto 24/7 markets."
        )
        return None

    def get_time_to_close(self):
        """Not applicable with Crypto 24/7 markets.

        Returns
        -------
        None
        """
        logging.warning(
            "The method 'get_time_to_close' is not applicable with Crypto 24/7 markets."
        )
        return None

    def is_margin_enabled(self):
        """Check if the broker is using margin trading"""
        return "margin" in self.api_keys and self.api_keys["margin"] == True

    def _fetch_balance(self):
        params = {}

        if self.is_margin_enabled():
            params["type"] = "margin"

        return self.api.fetch_balance(params)

    # =========Positions functions==================
    def _get_balances_at_broker(self, quote_asset):
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
        if self.api.exchangeId in ["coinbasepro", "kucoin", "kraken", "coinbase"]:
            balances_info = []
            reserved_keys = ["total", "free", "used", "info", "timestamp", "datetime"]
            for key in balances:
                if key in reserved_keys:
                    continue
                bal = balances[key]["total"]
                if Decimal(bal) != Decimal("0"):
                    balances_info.append({"currency": key, "balance": bal})

        # TODO: Test binance and switch it to the way we do it for coinbasepro and others if possible
        elif self.api.exchangeId == "binance":
            balances_info = []
            for bal in balances["info"]["balances"]:
                if (Decimal(bal["free"]) + Decimal(bal["locked"])) != Decimal("0"):
                    balances_info.append(bal)
            currency_key = "asset"
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
                precision_amount = 10 ** -precision_amount
                precision_price = 10 ** -precision_price

            # Binance only has `free` and `locked`.
            if self.api.exchangeId == "binance":
                total_balance = Decimal(currency_info["free"]) + Decimal(
                    currency_info["locked"]
                )
            else:
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

        gross_positions_value = float(positions_value) + float(total_cash_value)
        net_liquidation_value = float(positions_value) + float(total_cash_value)

        return (total_cash_value, gross_positions_value, net_liquidation_value)

    def _parse_broker_position(self, position, strategy, orders=None):
        """parse a broker position representation
        into a position object"""

        if self.api.exchangeId == "binance":
            symbol = position["asset"]
            precision = str(10 ** -self.api.currencies[symbol]["precision"])
            quantity = Decimal(position["free"]) + Decimal(position["locked"])
            hold = position["locked"]
            available = position["free"]
        else:
            symbol = position["currency"]
            precision = str(self.api.currencies[symbol]["precision"])
            quantity = Decimal(position["total"])
            hold = position["used"]
            available = position["free"]

        asset = Asset(
            symbol=symbol,
            asset_type="crypto",
            precision=precision,
        )

        position_return = Position(
            strategy, asset, quantity, hold=hold, available=available, orders=orders
        )
        return position_return

    def _pull_broker_position(self, asset):
        """Given a asset, get the broker representation
        of the corresponding asset"""
        response = self._pull_broker_positions()["info"][asset.symbol]
        return response

    def _pull_broker_positions(self, strategy=None):
        """Get the broker representation of all positions"""
        response = self._fetch_balance()

        if self.api.exchangeId == "binance":
            return response["info"]["balances"]
        elif self.api.exchangeId in ["kraken", "kucoin", "coinbasepro", "coinbase"]:
            balances_info = []
            reserved_keys = [
                "total",
                "free",
                "used",
                "info",
                "timestamp",
                "datetime",
                strategy.quote_asset.symbol,
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
            time_in_force=response["timeInForce"].lower(),
            quote=Asset(
                symbol=pair[1],
                asset_type="crypto",
            ),
            type=response["type"] if "type" in response else None,
        )
        order.set_identifier(response["id"])
        order.update_status(response["status"])
        order.update_raw(response)
        return order

    def _pull_broker_order(self, id):
        """Get a broker order representation by its id"""
        open_orders = self._pull_broker_open_orders()
        closed_orders = self._pull_broker_closed_orders()
        all_orders = open_orders + closed_orders

        response = [order for order in all_orders if order["id"] == id]

        return response[0] if len(response) > 0 else None

    def _pull_broker_closed_orders(self):
        params = {}
        if self.is_margin_enabled():
            params["tradeType"] = "MARGIN_TRADE"

        closed_orders = self.api.fetch_closed_orders(params)

        return closed_orders

    def _pull_broker_open_orders(self):
        """Get the broker open orders"""
        # For binance api rate limit on calling all orders at once.
        if self.api.exchangeId == "binance":
            self.api.options["warnOnFetchOpenOrdersWithoutSymbol"] = False
            if self.fetch_open_orders_last_request_time is not None:
                net_rate_limit = (
                    self.binance_all_orders_rate_limit
                    - (
                        datetime.datetime.now()
                        - self.fetch_open_orders_last_request_time
                    ).seconds
                )
                if net_rate_limit > 0:
                    logging.info(
                        f"Binance all order rate limit is being exceeded, bot sleeping for "
                        f"{net_rate_limit} seconds."
                    )
                    self.sleep(net_rate_limit)
            self.fetch_open_orders_last_request_time = datetime.datetime.now()

        params = {}
        if self.is_margin_enabled():
            params["tradeType"] = "MARGIN_TRADE"

        orders = self.api.fetch_open_orders(params=params)
        return orders

    def _flatten_order(self, order):
        """Some submitted orders may trigger other orders.
        _flatten_order returns a list containing the main order
        and all the derived ones"""
        orders = [order]
        if "legs" in order._raw and order._raw.legs:
            strategy_name = order.strategy
            for json_sub_order in order._raw.legs:
                sub_order = self._parse_broker_order(json_sub_order, strategy_name)
                orders.append(sub_order)

        return orders

    def _submit_order(self, order):
        """Submit an order for an asset"""

        # Orders limited.
        order_class = ""
        order_types = ["market", "limit", "stop_limit"]
        # TODO: Is this actually true?? Try testing this with a bunch of different exchanges.
        markets_error_message = (
            f"Only `market`, `limit`, or `stop_limit` orders work "
            f"with crypto currency markets."
        )

        if order.order_class != order_class:
            logging.error(
                f"A compound order of {order.order_class} was entered. "
                f"{markets_error_message}"
            )
            return

        if order.type not in order_types:
            logging.error(
                f"An order type of {order.type} was entered which is not "
                f"valid. {markets_error_message}"
            )
            return

        # Check order within limits.
        market = self.api.markets.get(order.pair, None)
        if market is None:
            logging.error(
                f"An order for {order.pair} was submitted. The market for that pair does not exist"
            )
            order.set_error("No market for pair.")
            return order

        limits = market["limits"]
        precision = market["precision"]
        if self.api.exchangeId in ["binance", "kucoin"]:
            precision_amount = str(10 ** -precision["amount"])
        elif self.api.exchangeId == "kraken":
            initial_precision_amount = Decimal(str(precision["amount"]))

            # Remove a few decimal places because Kraken precision amount is wrong and it's causing orders to fail.
            precision_exp_modifier = 2
            initial_precision_exp = abs(initial_precision_amount.as_tuple().exponent)
            new_precision_exp = initial_precision_exp - precision_exp_modifier
            factor = 10 ** new_precision_exp
            precision_amount = Decimal(1) / Decimal(factor)
        else:
            precision_amount = str(precision["amount"])

        # Convert the amount to Decimal.
        if hasattr(order, "quantity") and getattr(order, "quantity") is not None:
            qty = Decimal(getattr(order, "quantity"))
            new_qty = qty.quantize(precision_amount, rounding=ROUND_DOWN)

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
                setattr(
                    order,
                    price_type,
                    Decimal(getattr(order, price_type)).quantize(
                        Decimal(str(precision["price"]))
                    ),
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
                    assert getattr(order, price_type) * order.quantity <= Decimal(
                        limits["cost"]["max"]
                    )
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
        if self.is_margin_enabled():
            params["tradeType"] = "MARGIN_TRADE"

        # if self.api.exchangeId == "coinbase" and order.type == "market":
        #     params["createMarketBuyOrderRequiresPrice"] = False

        try:
            # Add order.custom_params to params
            if hasattr(order, "custom_params") and order.custom_params is not None:
                params.update(order.custom_params)

            response = self.api.create_order(*args, params=params)
            order.set_identifier(response["id"])
            order.update_status(response["status"])
            order.update_raw(response)

        except Exception as e:
            order.set_error(e)
            message = str(e)
            full_message = (
                f"{order} did not go through. The following error occurred: {message}"
            )
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
            if order.type in ["stop_limit"]:
                params = {
                    "stopPrice": str(order.stop_price),
                }
                # Remove items with None values
                params = {k: v for k, v in params.items() if v}

            order_type_map = dict(
                market="MARKET", limit="LIMIT", stop_limit="STOP_LOSS_LIMIT"
            )

            args = [
                order.pair,
                order_type_map[order.type],
                order.side,
                str(order.quantity),
            ]
            if order.type in ["limit", "stop_limit"]:
                args.append(str(order.limit_price))

            if len(params) > 0:
                args.append(params)

            return args

        elif broker in ["kraken", "kucoin", "coinbasepro", "coinbase"]:
            params = {}
            if order.type in ["stop_limit"]:
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
                order_type_map[order.type],
                order.side,
                str(order.quantity),  # check this with coinbase.
            ]
            if order_type_map[order.type] == "limit":
                args.append(str(order.limit_price))

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
        response = self.api.cancel_order(order.identifier, order.symbol)
        if order.identifier == response:
            order.set_canceled()

    def cancel_open_orders(self, strategy):
        """Cancel all open orders at the broker."""
        for order in self._pull_broker_open_orders():
            self.api.cancel_order(order["id"], symbol=order["symbol"])

    def get_historical_account_value(self):
        logging.error(
            "The function get_historical_account_value is not "
            "implemented yet for CCXT."
        )
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
