import logging
from functools import wraps


class Broker:
    def __init__(self, debug=False):
        self.name = ""
        self.orders = []
        self.new_orders = []
        self.canceled_orders = []
        self.partially_filled_orders = []
        self.filled_orders = []
        self.debug = debug

    def __getattribute__(self, name):
        attr = object.__getattribute__(self, name)
        if name != "submit_order":
            return attr

        @wraps(attr)
        def new_func(order, *args, **kwargs):
            result = attr(order, *args, **kwargs)
            if not result.is_rejected():
                self.add_order(result)

        return new_func

    def get_order(self, identifier):
        order = [o for o in self.orders if o.identifier == identifier]
        if order:
            return order[0]
        return None

    def add_order(self, order):
        self.orders.append(order)

    def move_order_to_new(self, order):
        logging.info("New %r submited." % order)
        order.status = "new"
        self.new_orders.append(order)

    def move_order_to_canceled(self, order):
        logging.info("%r canceled." % order)
        if order in self.new_orders:
            self.new_orders.remove(order)
        elif order in self.partially_filled_orders:
            self.partially_filled_orders.remove(order)

        order.status = "canceled"
        self.canceled_orders.append(order)

    def move_order_to_filled(self, order, price, quantity):
        logging.info(
            "New transaction: %s %d of %s at %s$ per share"
            % (order.side, quantity, order.symbol, price)
        )
        logging.info("%r filled")
        if order in self.new_orders:
            self.new_orders.remove(order)
        elif order in self.partially_filled_orders:
            self.partially_filled_orders.remove(order)

        order.add_transaction(price, quantity)
        order.status = "filled"
        self.filled_orders.append(order)

    def move_order_to_partially_filled(self, order, price, quantity):
        logging.info(
            "New transaction: %s %d of %s at %s$ per share"
            % (order.side, quantity, order.symbol, price)
        )
        logging.info("%r partially filled")
        if order in self.new_orders:
            self.new_orders.remove(order)

        order.add_transaction(price, quantity)
        order.status = "partially_filled"
        self.partially_filled_orders.append(order)
