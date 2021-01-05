from collections import namedtuple

import lumibot.entities as entities


class Order:
    Transaction = namedtuple("Transaction", ["quantity", "price"])

    SELL = "sell"
    BUY = "buy"

    def __init__(
        self,
        strategy,
        symbol,
        quantity,
        side,
        limit_price=None,
        stop_price=None,
        time_in_force="day",
    ):
        # Initialization default values
        self.strategy = strategy
        self.symbol = symbol
        self.quantity = None
        self.limit_price = None
        self.stop_price = None
        self.side = None
        self.type = "market"
        self.time_in_force = time_in_force
        self.order_class = None
        self.identifier = None
        self.status = "unprocessed"
        self.transactions = []

        # setting internal variables
        self._raw = None
        self._transmitted = False
        self._error = None
        self._error_message = None

        # setting the quantity
        error = ValueError(
            "Quantity must be a positive integer, got %r instead" % quantity
        )
        try:
            quantity = int(quantity)
            if quantity <= 0:
                raise error
            self.quantity = quantity
        except ValueError:
            raise error

        # setting the side
        if side not in [self.BUY, self.SELL]:
            raise ValueError("Side must be either sell or buy, got %r instead" % side)
        self.side = side

        # setting the limit_price
        self.limit_price = None
        if limit_price is not None:
            error = ValueError(
                "limit_price must be a positive float, got %r instead" % limit_price
            )
            try:
                limit_price = float(limit_price)
                if limit_price < 0:
                    raise error
                self.limit_price = limit_price
                self.type = "limit"
            except ValueError:
                raise error

        # setting the stop price
        self.stop_price = None
        if stop_price is not None:
            error = ValueError(
                "stop_price must be a positive float, got %r instead" % stop_price
            )
            try:
                stop_price = float(stop_price)
                if stop_price < 0:
                    raise error
                self.stop_price = stop_price
                self.order_class = "oto"
            except:
                raise error

    def __repr__(self):
        repr = "%s order of | %d %s %s |" % (
            self.type,
            self.quantity,
            self.symbol,
            self.side,
        )
        if self.order_class:
            repr = "%s of class %s" % (repr, self.order_class)
        repr = "%s with status %s" % (repr, self.status)
        return repr

    def set_identifier(self, identifier):
        self.identifier = identifier

    def add_transaction(self, price, quantity):
        transaction = self.Transaction(price=price, quantity=quantity)
        self.transactions.append(transaction)

    def update_status(self, status):
        self.status = status

    def set_error(self, error):
        self.status = "error"
        self._error = error
        self._error_message = str(error)

    def was_transmitted(self):
        return self._transmitted is True

    def update_raw(self, raw):
        if raw is not None:
            self._transmitted = True
            self._raw = raw

    def to_position(self):
        position = entities.Position(
            self.strategy, self.symbol, self.quantity, orders=[self]
        )
        return position

    def get_increment(self):
        increment = self.quantity
        if self.side == self.SELL:
            increment = -increment
        return increment
