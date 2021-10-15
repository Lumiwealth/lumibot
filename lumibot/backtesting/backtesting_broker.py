import logging
import traceback
from datetime import datetime, timedelta
from functools import wraps
from secrets import token_hex

from lumibot.brokers import Broker
from lumibot.entities import Order, Position
from lumibot.tools import get_trading_days
from lumibot.trading_builtins import CustomStream


class BacktestingBroker(Broker):
    # Metainfo
    IS_BACKTESTING_BROKER = True

    CannotPredictFuture = ValueError(
        "Cannot predict the future. Backtesting datetime already in the present. "
        "Check if your backtesting end time is set after available data."
    )

    def __init__(self, data_source, connect_stream=True, max_workers=20):
        # Calling init methods
        self.name = "backtesting"
        self.max_workers = max_workers

        if not data_source.IS_BACKTESTING_DATA_SOURCE:
            raise ValueError(
                "object %r is not a backteesting data_source" % data_source
            )
        self._data_source = data_source
        if data_source.SOURCE != "PANDAS":
            self._trading_days = get_trading_days()

        Broker.__init__(self, name=self.name, connect_stream=connect_stream)

    def __getattribute__(self, name):
        attr = object.__getattribute__(self, name)

        if name == "submit_order":

            broker = self

            @wraps(attr)
            def new_func(order, *args, **kwargs):
                result = attr(order, *args, **kwargs)
                if (
                    result.was_transmitted()
                    and result.order_class
                    and result.order_class == "oco"
                ):
                    orders = broker._flatten_order(result)
                    for order in orders:
                        logging.info("%r was sent to broker %s" % (order, self.name))
                        broker._new_orders.append(order)
                else:
                    broker._new_orders.append(order)
                return result

            return new_func
        else:
            return attr

    @property
    def datetime(self):
        return self._data_source._datetime

    # =========Internal functions==================

    def _update_datetime(self, input):
        """Works with either timedelta or datetime input"""
        if isinstance(input, timedelta):
            new_datetime = self.datetime + input
        elif isinstance(input, int) or isinstance(input, float):
            new_datetime = self.datetime + timedelta(seconds=input)
        else:
            new_datetime = input

        self._data_source._update_datetime(new_datetime)
        logging.info(f"Current backtesting datetime {self.datetime}")

    # =========Clock functions=====================

    def should_continue(self):
        """In production mode always returns True.
        Needs to be overloaded for backtesting to
        check if the limit datetime was reached"""
        if self.datetime >= self._data_source.datetime_end:
            return False
        return True

    def is_market_open(self):
        """return True if market is open else false"""
        now = self.datetime
        return (
            (now >= self._trading_days.market_open)
            & (now < self._trading_days.market_close)
        ).any()

    def _get_next_trading_day(self):
        now = self.datetime
        search = self._trading_days[now < self._trading_days.market_open]
        if search.empty:
            raise self.CannotPredictFuture

        return search.market_open[0].to_pydatetime()

    def get_time_to_open(self):
        """Return the remaining time for the market to open in seconds"""
        now = self.datetime

        search = self._trading_days[now < self._trading_days.market_close]
        if search.empty:
            raise self.CannotPredictFuture

        trading_day = search.iloc[0]
        if now >= trading_day.market_open:
            return 0

        delta = trading_day.market_open - now
        return delta.total_seconds()

    def get_time_to_close(self):
        """Return the remaining time for the market to close in seconds"""
        now = self.datetime
        search = self._trading_days[now < self._trading_days.market_close]
        if search.empty:
            raise self.CannotPredictFuture

        trading_day = search.iloc[0]
        if now < trading_day.market_open:
            return 0

        delta = trading_day.market_close - now
        return delta.total_seconds()

    def _await_market_to_open(self, timedelta=None):
        if (
            self._data_source.SOURCE == "PANDAS"
            and self._data_source._timestep == "day"
        ):
            return
        time_to_open = self.get_time_to_open()
        if timedelta is not None:
            time_to_open -= 60 * timedelta
        self._update_datetime(time_to_open)

    def _await_market_to_close(self, timedelta=None):
        if (
            self._data_source.SOURCE == "PANDAS"
            and self._data_source._timestep == "day"
        ):
            return
        time_to_close = self.get_time_to_close()
        if timedelta is not None:
            time_to_close -= 60 * timedelta
        self._update_datetime(time_to_close)

    # =========Positions functions==================
    def _pull_broker_position(self, asset):
        """Given an asset, get the broker representation
        of the corresponding asset"""
        orders = []
        quantity = 0
        for position in self._filled_positions:
            if position.asset == asset:
                orders.extend(position.orders)
                quantity += position.quantity

        response = Position("", asset, quantity, orders=orders)
        return response

    def _pull_broker_positions(self):
        """Get the broker representation of all positions"""
        response = self._filled_positions.__items
        return response

    def _parse_broker_position(self, broker_position, strategy, orders=None):
        """parse a broker position representation
        into a position object"""
        broker_position.strategy = strategy
        return broker_position

    # =======Orders and assets functions=========

    def _parse_broker_order(self, response, strategy):
        """parse a broker order representation
        to an order object"""
        order = response
        return order

    def _pull_broker_order(self, id):
        """Get a broker order representation by its id"""
        for order in self._tracked_orders:
            if order.id == id:
                return order
        return None

    def _pull_broker_open_orders(self):
        """Get the broker open orders"""
        orders = self._new_orders.__items
        return orders

    def _flatten_order(self, order):
        """Some submitted orders may triggers other orders.
        _flatten_order returns a list containing the derived orders"""

        orders = []
        if order.order_class == "":
            orders.append(order)
            if order.stop_price:
                stop_loss_order = Order(
                    order.strategy,
                    order.asset,
                    order.quantity,
                    order.side,
                    stop_price=order.stop_price,
                )
                stop_loss_order = self._parse_broker_order(
                    stop_loss_order, order.strategy
                )
                orders.append(stop_loss_order)

        elif order.order_class == "oco":
            stop_loss_order = Order(
                order.strategy,
                order.asset,
                order.quantity,
                order.side,
                stop_price=order.stop_loss_price,
            )
            orders.append(stop_loss_order)

            limit_order = Order(
                order.strategy,
                order.asset,
                order.quantity,
                order.side,
                limit_price=order.take_profit_price,
            )
            orders.append(limit_order)

            stop_loss_order.dependent_order = limit_order
            limit_order.dependent_order = stop_loss_order

        elif order.order_class in ["bracket", "oto"]:
            side = "sell" if order.side == "buy" else "buy"
            if order.order_class == "bracket" or (
                order.order_class == "oto" and order.stop_loss_price
            ):
                stop_loss_order = Order(
                    order.strategy,
                    order.asset,
                    order.quantity,
                    side,
                    stop_price=order.stop_loss_price,
                    limit_price=order.stop_loss_limit_price,
                )
                orders.append(stop_loss_order)

            if order.order_class == "bracket" or (
                order.order_class == "oto" and order.take_profit_price
            ):
                limit_order = Order(
                    order.strategy,
                    order.asset,
                    order.quantity,
                    side,
                    limit_price=order.take_profit_price,
                )
                orders.append(limit_order)

            if order.order_class == "bracket":
                stop_loss_order.dependent_order = limit_order
                limit_order.dependent_order = stop_loss_order

        return orders

    def submit_order(self, order):
        """Submit an order for an asset"""
        order.set_identifier(token_hex(16))
        order.update_raw(order)
        return order

    def submit_orders(self, orders):
        results = []
        for order in orders:
            results.append(self.submit_order(order))
        return results

    def cancel_order(self, order):
        """Cancel an order"""
        self.stream.dispatch(
            self.CANCELED_ORDER,
            order=order,
        )

    def expired_contracts(self, strategy):
        """Checks if options or futures contracts have expried and converts
        to cash.

        Parameters
        ----------
        strategy : str
            Strategy object name.

        Returns
        --------
            List of orders
        """
        if self._data_source.SOURCE != "PANDAS":
            return []

        orders_closing_contracts = []
        for tracked_position in self.get_tracked_positions(strategy):
            if tracked_position.asset.expiration == self.datetime.date():
                orders_closing_contracts.append(tracked_position.get_selling_order())

        return orders_closing_contracts

    def process_pending_orders(self, strategy):
        """Used to evaluate and execute open orders in backtesting.

        This method will evaluate the open orders at the beginning of every new bar to
        determine if any of the open orders should have been filled. This method will
        execute order events as needed, mostly fill events.

        Parameters
        ----------
        strategy : Strategy object

        """

        # Process expired contracts.
        pending_orders = self.expired_contracts(strategy)

        pending_orders += [
            order
            for order in self.get_tracked_orders(strategy)
            if order.status in ["unprocessed", "new"]
        ]

        if len(pending_orders) == 0:
            return

        for order in pending_orders:
            if order.dependent_order_filled:
                continue

            # Check validity if current date > valid date, cancel order. todo valid date
            price = 0
            filled_quantity = order.quantity

            if self._data_source.SOURCE == "YAHOO":
                ohlc = self.get_last_bar(order.asset)
                dt = ohlc.df.index[-1]
                open = ohlc.df.open[-1]
                high = ohlc.df.high[-1]
                low = ohlc.df.low[-1]
                close = ohlc.df.close[-1]
                volume = ohlc.df.volume[-1]

            elif self._data_source.SOURCE == "PANDAS":
                ohlc = self._data_source._data_store[order.asset]._get_bars_dict(
                    self.datetime, length=1
                )
                if ohlc is None:
                    self.cancel_order(order)
                    continue
                dt = ohlc["datetime"][-1]
                open = ohlc["open"][-1]
                high = ohlc["high"][-1]
                low = ohlc["low"][-1]
                close = ohlc["close"][-1]
                volume = ohlc["volume"][-1]

            # Determine transaction price.
            if order.type == "market":
                price = open
            elif order.type == "limit":
                price = self.limit_order(order.limit_price, order.side, open, high, low)
            elif order.type == "stop":
                price = self.stop_order(order.stop_price, order.side, open, high, low)
            elif order.type == "stop_limit":
                if not order.price_triggered:
                    price = self.stop_order(
                        order.stop_price, order.side, open, high, low
                    )
                    if price != 0:
                        price = self.limit_order(
                            order.limit_price, order.side, price, high, low
                        )
                        order.price_triggered = True
                elif order.price_triggered:
                    price = self.limit_order(
                        order.limit_price, order.side, open, high, low
                    )
            else:
                raise ValueError(
                    f"Order type {order.type} is not allowable in backtesting."
                )

            if price != 0:
                if order.dependent_order:
                    self.cancel_order(order.dependent_order)

                if order.order_class in ["bracket", "oto"]:
                    orders = self._flatten_order(order)
                    for flat_order in orders:
                        logging.info(
                            "%r was sent to broker %s" % (flat_order, self.name)
                        )
                        self._new_orders.append(flat_order)

                self.stream.dispatch(
                    self.FILLED_ORDER,
                    order=order,
                    price=price,
                    filled_quantity=filled_quantity,
                )
            else:
                continue

    def limit_order(self, limit_price, side, open, high, low):
        """Limit order logic. """
        if side == "buy":
            if limit_price >= open:
                return open
            elif limit_price < open and limit_price >= low:
                return limit_price
            elif limit_price < low:
                return 0
        elif side == "sell":
            if limit_price <= open:
                return open
            elif limit_price > open and limit_price <= high:
                return limit_price
            elif limit_price > high:
                return 0

    def stop_order(self, stop_price, side, open, high, low):
        """Stop order logic. """
        if side == "buy":
            if stop_price <= open:
                return open
            elif stop_price > open and stop_price <= high:
                return stop_price
            elif stop_price > high:
                return 0
        elif side == "sell":
            if stop_price >= open:
                return open
            elif stop_price < open and stop_price >= low:
                return stop_price
            elif stop_price < low:
                return 0

    # =========Market functions=======================
    def get_last_price(self, asset):
        """Takes an asset asset and returns the last known price"""
        return self._data_source.get_last_price(asset)

    def get_last_prices(self, symbols):
        """Takes a list of symbols and returns the last known prices"""
        return self._data_source.get_last_prices(symbols)

    def get_last_bar(self, asset):
        """Returns OHLCV dictionary for last bar of the asset. """
        return self._data_source.get_symbol_bars(asset, 1)

    def get_chains(self, asset):
        return self._data_source.get_chains(asset)

    def get_chain(self, chains, exchange="SMART"):
        """Returns option chain for a particular exchange."""
        for x, p in chains.items():
            if x == exchange:
                return p

    def get_expiration(self, chains, exchange="SMART"):
        """Returns expirations and strikes high/low of target price."""
        return sorted(list(self.get_chain(chains, exchange=exchange)["Expirations"]))

    def get_multiplier(self, chains, exchange="SMART"):
        """Returns the multiplier"""
        return self.get_chain(chains, exchange)["Multiplier"]

    def get_strikes(self, asset):
        """Returns the strikes for an option asset with right and
        expiry."""
        return self._data_source.get_strikes(asset)

    # ==========Processing streams data=======================

    def _get_stream_object(self):
        """get the broker stream connection"""
        stream = CustomStream()
        return stream

    def _register_stream_events(self):
        """Register the function on_trade_event
        to be executed on each trade_update event"""
        broker = self

        @broker.stream.add_action(broker.FILLED_ORDER)
        def on_trade_event(order, price, filled_quantity):
            try:
                broker._process_trade_event(
                    order,
                    broker.FILLED_ORDER,
                    price=price,
                    filled_quantity=filled_quantity,
                    multiplier=order.asset.multiplier,
                )
                return True
            except:
                logging.error(traceback.format_exc())

        @broker.stream.add_action(broker.CANCELED_ORDER)
        def on_trade_event(order):
            try:
                broker._process_trade_event(
                    order,
                    broker.CANCELED_ORDER,
                )
                return True
            except:
                logging.error(traceback.format_exc())

    def _run_stream(self):
        self._stream_established()
        self.stream._run()
