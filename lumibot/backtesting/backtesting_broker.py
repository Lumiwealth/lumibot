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
        "Cannot predict the future. Backtesting datetime already in the present"
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
        self._trading_days = get_trading_days()

        Broker.__init__(self, name=self.name, connect_stream=connect_stream)

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
                    logging.info("%r was sent to broker %s" % (order, self.name))
                    broker._new_orders.append(order)

                broker.stream.dispatch(broker.FILLED_ORDER, order=order)
            return result

        return new_func

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
        now = self._data_source.localize_datetime(self.datetime)
        return (
            (now >= self._trading_days.market_open)
            & (now < self._trading_days.market_close)
        ).any()

    def _get_next_trading_day(self):
        now = self._data_source.localize_datetime(self.datetime)
        search = self._trading_days[now < self._trading_days.market_open]
        if search.empty:
            raise self.CannotPredictFuture

        return search.market_open[0].to_pydatetime()

    def get_time_to_open(self):
        """Return the remaining time for the market to open in seconds"""
        now = self._data_source.localize_datetime(self.datetime)
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
        now = self._data_source.localize_datetime(self.datetime)
        search = self._trading_days[now < self._trading_days.market_close]
        if search.empty:
            raise self.CannotPredictFuture

        trading_day = search.iloc[0]
        if now < trading_day.market_open:
            return 0

        delta = trading_day.market_close - now
        return delta.total_seconds()

    def _await_market_to_open(self, timedelta=None):
        time_to_open = self.get_time_to_open()
        if timedelta is not None:
            time_to_open -= 60 * timedelta
        self._update_datetime(time_to_open)

    def _await_market_to_close(self, timedelta=None):
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
            if position.asset.same_as(asset):
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
        _flatten_order returns a list containing the main order
        and all the derived ones"""
        orders = [order]
        if order.stop_price:
            stop_loss_order = Order(
                order.strategy,
                order.asset,
                order.quantity,
                "sell",
                stop_price=order.stop_price,
            )
            stop_loss_order = self._parse_broker_order(stop_loss_order, order.strategy)
            orders.append(stop_loss_order)

        return orders

    def submit_order(self, order):
        """Submit an order for an asset"""
        if order.order_class or order.type != "market":
            if order.order_class:
                logging.warning(
                    "Backtest executes Bracket, OTO and OCO orders as simple orders"
                )
            else:
                logging.warning(
                    "Backtest executes limit, stop, stop_limit and trailing orders as market orders"
                )

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
        pass

    # =========Market functions=======================

    def get_last_price(self, asset):
        """Takes an asset asset and returns the last known price"""
        return self._data_source.get_last_price(asset)

    def get_last_prices(self, symbols):
        """Takes a list of symbols and returns the last known prices"""
        return self._data_source.get_last_prices(symbols)

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
        def on_trade_event(order):
            try:
                identifier = order.identifier
                asset = order.asset
                stored_order = broker.get_tracked_order(identifier)
                filled_quantity = stored_order.quantity
                price = broker.get_last_price(asset)
                broker._process_trade_event(
                    stored_order,
                    broker.FILLED_ORDER,
                    price=price,
                    filled_quantity=filled_quantity,
                )
                return True
            except:
                logging.error(traceback.format_exc())

    def _run_stream(self):
        self._stream_established()
        self.stream._run()
