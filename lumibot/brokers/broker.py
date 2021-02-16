import logging
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from functools import wraps
from threading import RLock, Thread

from lumibot.trading_builtins import SafeList


class Broker:

    # Metainfo
    IS_BACKTESTING_BROKER = False

    # Trading events flags
    NEW_ORDER = "new"
    CANCELED_ORDER = "canceled"
    FILLED_ORDER = "fill"
    PARTIALLY_FILLED_ORDER = "partial_fill"

    def __init__(self, connect_stream=True):
        """Broker constructor"""
        # Shared Variables between threads
        self.name = ""
        self._lock = RLock()
        self._unprocessed_orders = SafeList(self._lock)
        self._new_orders = SafeList(self._lock)
        self._canceled_orders = SafeList(self._lock)
        self._partially_filled_orders = SafeList(self._lock)
        self._filled_positions = SafeList(self._lock)
        self._subscribers = SafeList(self._lock)
        self._is_stream_subscribed = False

        # setting the stream object
        self.stream = self._get_stream_object()
        if connect_stream:
            self._launch_stream()

    @property
    def _tracked_orders(self):
        return (
            self._unprocessed_orders + self._new_orders + self._partially_filled_orders
        )

    def __getattribute__(self, name):
        attr = object.__getattribute__(self, name)
        if name != "submit_order":
            return attr

        broker = self

        @wraps(attr)
        def new_func(order, *args, **kwargs):
            result = attr(order, *args, **kwargs)
            if result.was_transmitted():
                orders = broker._flatten_order(result)
                for order in orders:
                    logging.info("%r was sent to broker %s" % (order, broker.name))
                    broker._unprocessed_orders.append(order)

            return result

        return new_func

    # =========Internal functions==============

    def _process_new_order(self, order):
        logging.info("New %r was submited." % order)
        self._unprocessed_orders.remove(order.identifier, key="identifier")
        order.update_status(self.NEW_ORDER)
        self._new_orders.append(order)
        return order

    def _process_canceled_order(self, order):
        logging.info("%r was canceled." % order)
        self._new_orders.remove(order.identifier, key="identifier")
        self._partially_filled_orders.remove(order.identifier, key="identifier")
        order.update_status(self.CANCELED_ORDER)
        self._canceled_orders.append(order)
        return order

    def _process_partially_filled_order(self, order, price, quantity):
        logging.info(
            "New transaction: %s %d of %s at %s$ per share"
            % (order.side, quantity, order.symbol, price)
        )
        logging.info("%r was partially filled" % order)
        self._new_orders.remove(order.identifier, key="identifier")

        order.add_transaction(price, quantity)
        order.update_status(self.PARTIALLY_FILLED_ORDER)
        self._partially_filled_orders.append(order)
        return order

    def _process_filled_order(self, order, price, quantity):
        logging.info(
            "New transaction: %s %d of %s at %s$ per share"
            % (order.side, quantity, order.symbol, price)
        )
        logging.info("%r was filled" % order)
        self._new_orders.remove(order.identifier, key="identifier")
        self._partially_filled_orders.remove(order.identifier, key="identifier")

        order.add_transaction(price, quantity)
        order.update_status(self.FILLED_ORDER)

        position = self.get_tracked_position(order.strategy, order.symbol)
        if position is None:
            # Create new position for this given strategy,symbol
            position = order.to_position()
            self._filled_positions.append(position)
        else:
            # Add the order to the already existing position
            position.add_order(order)
            if position.quantity == 0:
                logging.info("Position %r liquidated" % position)
                self._filled_positions.remove(position)

        return position

    # =========Clock functions=====================

    def should_continue(self):
        """In production mode always returns True.
        Needs to be overloaded for backtesting to
        check if the limit timestamp was reached"""
        return True

    def is_market_open(self):
        """return True if market is open else false"""
        pass

    def get_time_to_open(self):
        """Return the remaining time for the market to open in seconds"""
        pass

    def get_time_to_close(self):
        """Return the remaining time for the market to close in seconds"""
        pass

    def await_market_to_open(self):
        """Executes infinite loop until market opens"""
        isOpen = self.is_market_open()
        while not isOpen:
            time_to_open = self.get_time_to_open()
            if time_to_open > 60 * 60:
                delta = timedelta(seconds=time_to_open)
                logging.info("Market will open in %s." % str(delta))
                time.sleep(60 * 60)
            elif time_to_open > 60:
                logging.info("%d minutes til market open." % int(time_to_open / 60))
                time.sleep(60)
            else:
                logging.info("%d seconds til market open." % time_to_open)
                time.sleep(time_to_open)

            isOpen = self.is_market_open()

    def await_market_to_close(self):
        """Sleep until market closes"""
        isOpen = self.is_market_open()
        if isOpen:
            time_to_close = self.get_time_to_close()
            sleeptime = max(0, time_to_close)
            logging.info("Sleeping until the market closes")
            time.sleep(sleeptime)

    # =========Positions functions==================

    def get_tracked_position(self, strategy, symbol):
        """get a tracked position given a symbol and
        a strategy"""
        for position in self._filled_positions:
            if position.symbol == symbol and position.strategy == strategy:
                return position
        return None

    def get_tracked_positions(self, strategy):
        """get all tracked positions for a given strategy"""
        result = [
            position
            for position in self._filled_positions
            if position.strategy == strategy
        ]
        return result

    def _parse_broker_position(self, broker_position, strategy, orders=None):
        """parse a broker position representation
        into a position object"""
        pass

    def _parse_broker_positions(self, broker_positions, strategy):
        """parse a list of broker positions into a
        list of position objects"""
        result = []
        for broker_position in broker_positions:
            result.append(self._parse_broker_position(broker_position, strategy))

        return result

    def _pull_broker_position(self, symbol):
        """Given a symbol, get the broker representation
        of the corresponding symbol"""
        pass

    def _pull_broker_positions(self):
        """Get the broker representation of all positions"""
        pass

    def _pull_positions(self, strategy):
        """Get the account positions. return a list of
        position objects"""
        response = self._pull_broker_positions()
        result = self._parse_broker_positions(response, strategy)
        return result

    def _pull_position(self, strategy, symbol):
        """Get the account position for a given symbol.
        return a position object"""
        response = self._pull_broker_position(symbol)
        result = self._parse_broker_position(response, strategy)
        return result

    # =========Orders and assets functions=================

    def get_tracked_order(self, identifier):
        """get a tracked order given an identifier"""
        for order in self._tracked_orders:
            if order.identifier == identifier:
                return order
        return None

    def get_tracked_orders(self, strategy, symbol=None):
        """get all tracked orders for a given strategy"""
        result = []
        for order in self._tracked_orders:
            if order.strategy == strategy and (
                symbol is None or order.symbol == symbol
            ):
                result.append(order)

        return result

    def get_tracked_assets(self, strategy):
        """Get the list of symbols for positions
        and open orders for a given strategy"""
        orders = self.get_tracked_orders(strategy)
        positions = self.get_tracked_positions(strategy)
        result = [o.symbol for o in orders] + [p.symbol for p in positions]
        return list(set(result))

    def get_asset_potential_total(self, strategy, symbol):
        """given a strategy and a symbol, check the ongoing
        position and the tracked order and returns the total
        number of shares provided all orders went through"""
        quantity = 0
        position = self.get_tracked_position(strategy, symbol)
        if position is not None:
            quantity = position.quantity
        orders = self.get_tracked_orders(strategy, symbol)
        for order in orders:
            quantity += order.get_increment()
        return quantity

    def _parse_broker_order(self, response, strategy):
        """parse a broker order representation
        to an order object"""
        pass

    def _parse_broker_orders(self, broker_orders, strategy):
        """parse a list of broker orders into a
        list of order objects"""
        result = []
        for broker_order in broker_orders:
            result.append(self._parse_broker_order(broker_order, strategy))

        return result

    def _pull_broker_order(self, id):
        """Get a broker order representation by its id"""
        pass

    def _pull_order(self, identifier, strategy):
        """pull and parse a broker order by id"""
        response = self._pull_broker_order(identifier)
        if response:
            order = self._parse_broker_order(response, strategy)
            return order
        return None

    def _pull_broker_open_orders(self):
        """Get the broker open orders"""
        pass

    def _pull_open_orders(self, strategy):
        """Get a list of order objects representing the open
        orders"""
        response = self._pull_broker_open_orders()
        result = self._parse_broker_orders(response, strategy)
        return result

    def submit_order(self, order):
        """Submit an order for an asset"""
        pass

    def submit_orders(self, orders):
        """submit orders"""
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            tasks = []
            for order in orders:
                tasks.append(executor.submit(self.submit_order, order))

    def cancel_order(self, order):
        """Cancel an order"""
        pass

    def cancel_orders(self, orders):
        """cancel orders"""
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            tasks = []
            for order in orders:
                tasks.append(executor.submit(self.cancel_order, order))

    def cancel_open_orders(self, strategy):
        """cancel all open orders for a given strategy"""
        orders = self.get_tracked_orders(strategy)
        self.cancel_orders(orders)

    def sell_all(self, strategy, cancel_open_orders=True):
        """sell all positions"""
        logging.warning("Strategy %s: sell all" % strategy)
        if cancel_open_orders:
            self.cancel_open_orders(strategy)

        orders = []
        positions = self.get_tracked_positions(strategy)
        for position in positions:
            order = position.get_selling_order()
            orders.append(order)
        self.submit_orders(orders)

    # =========Market functions=======================

    def get_last_price(self, symbol):
        """Takes an asset symbol and returns the last known price"""
        pass

    def get_last_prices(self, symbols):
        """Takes a list of symbols and returns the last known prices"""
        pass

    def get_tradable_assets(self, easy_to_borrow=None, filter_func=None):
        """Get the list of all tradable assets from the market"""
        pass

    # =========Subscribers/Strategies functions==============

    def _add_subscriber(self, subscriber):
        """Adding a new strategy as a subscriber for thes broker"""
        self._subscribers.append(subscriber)

    def _get_subscriber(self, name):
        """get a subscriber/strategy by name"""
        for subscriber in self._subscribers:
            if subscriber._name == name:
                return subscriber

        return None

    def _on_new_order(self, order):
        """notify relevant subscriber/strategy about
        new order event"""
        subscriber = self._get_subscriber(order.strategy)
        subscriber.on_new_order(order)

    def _on_canceled_order(self, order):
        """notify relevant subscriber/strategy about
        canceled order event"""
        subscriber = self._get_subscriber(order.strategy)
        subscriber.on_canceled_order(order)

    def _on_partially_filled_order(self, order):
        """notify relevant subscriber/strategy about
        partially filled order event"""
        subscriber = self._get_subscriber(order.strategy)
        subscriber.on_partially_filled_order(order)

    def _on_filled_order(self, position, order):
        """notify relevant subscriber/strategy about
        filled order event"""
        subscriber = self._get_subscriber(order.strategy)
        subscriber.on_filled_order(position, order)

    def _update_subscriber_unspent_money(self, name, side, quantity, price):
        """update the corresponding strategy unspent_money"""
        subscriber = self._get_subscriber(name)
        subscriber._update_unspent_money(side, quantity, price)

    # ==========Processing streams data=======================

    def _get_stream_object(self):
        """get the broker stream connection"""
        pass

    def _stream_established(self):
        self._is_stream_subscribed = True

    def _process_trade_event(
        self, stored_order, type_event, price=None, filled_quantity=None
    ):
        """process an occured trading event and update the
        corresponding order"""
        # for fill and partial_fill events, price and filled_quantity must be specified
        strategy = stored_order.strategy
        subscriber = self._get_subscriber(strategy)
        with subscriber._lock:
            if type_event in [self.FILLED_ORDER, self.PARTIALLY_FILLED_ORDER] and (
                price is None or filled_quantity is None
            ):
                raise ValueError(
                    """For filled_order and partially_filled_order event,
                    price and filled_quantity must be specified.
                    Received respectively %r and %r"""
                    % (price, filled_quantity)
                )

            if filled_quantity is not None:
                error = ValueError(
                    "filled_quantity must be a positive integer, received %r instead"
                    % filled_quantity
                )
                try:
                    filled_quantity = int(filled_quantity)
                    if filled_quantity < 0:
                        raise error
                except:
                    raise error

            if price is not None:
                error = ValueError(
                    "price must be a positive float, received %r instead" % price
                )
                try:
                    price = float(price)
                    if price < 0:
                        raise error
                except:
                    raise error

            if type_event == self.NEW_ORDER:
                stored_order = self._process_new_order(stored_order)
                self._on_new_order(stored_order)
            elif type_event == self.CANCELED_ORDER:
                stored_order = self._process_canceled_order(stored_order)
                self._on_canceled_order(stored_order)
            elif type_event == self.PARTIALLY_FILLED_ORDER:
                self._update_subscriber_unspent_money(
                    stored_order.strategy, stored_order.side, filled_quantity, price
                )
                stored_order = self._process_partially_filled_order(
                    stored_order, price, filled_quantity
                )
                self._on_partially_filled_order(stored_order)
            elif type_event == self.FILLED_ORDER:
                self._update_subscriber_unspent_money(
                    stored_order.strategy, stored_order.side, filled_quantity, price
                )
                position = self._process_filled_order(
                    stored_order, price, filled_quantity
                )
                self._on_filled_order(position, stored_order)
            else:
                logging.info(
                    "Unhandled type event %s for %r" % (type_event, stored_order)
                )

        return

    def _register_stream_events(self):
        """Register the function on_trade_event
        to be executed on each trade_update event"""
        pass

    def _run_stream(self):
        pass

    def _launch_stream(self):
        """Set the asynchronous actions to be executed after
        when events are sent via socket streams"""
        self._register_stream_events()
        t = Thread(
            target=self._run_stream, daemon=True, name=f"broker_{self.name}_thread"
        )
        t.start()
        if not self.IS_BACKTESTING_BROKER:
            logging.info(
                """Waiting for the socket stream connection to be established, 
                method _stream_established must be called"""
            )
            while True:
                if self._is_stream_subscribed is True:
                    break
        return

    def _poll(self):
        """Check every minute orders in the '_new_orders' and
        '_partially_filled' lists and update their status
        if necessary"""
        for order in self._tracked_orders:
            old_status = order.status
            order_updated = self._pull_order(order.identifier, order.strategy)
            if order_updated is None:
                raise ValueError(
                    "No trace of previous order with id %s found by the broker %s"
                    % (order.identifier, self.name)
                )

            new_status = order_updated.status
            if old_status != new_status:
                type_event = new_status
                price = None
                filled_quantity = None  #'filled_qty': '0',
                self._process_trade_event(
                    order_updated,
                    type_event,
                    price=price,
                    filled_quantity=filled_quantity,
                )
