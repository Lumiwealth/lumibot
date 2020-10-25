from collections import namedtuple


class Order:
    Transaction = namedtuple("Transaction", ["quantity", "price"])

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
        self.type = "market"
        self.order_class = None
        self.status = "unsubmitted"
        self.transactions = []

        # setting the strategy executing the order
        self.strategy = strategy

        # setting the symbol
        self.symbol = symbol

        # setting the time in force
        self.time_in_force = time_in_force

        # setting the quantity
        if not isinstance(quantity, int) or quantity <= 0:
            raise ValueError(
                "Quantity must be a positive integer, got %r instead" % quantity
            )
        self.quantity = quantity

        # setting the side
        if side not in ["buy", "sell"]:
            raise ValueError("Side must be either sell or buy, got %r instead" % side)
        self.side = side

        # setting the limit_price
        self.limit_price = None
        if limit_price is not None:
            if not isinstance(limit_price, float) or limit_price < 0:
                raise ValueError(
                    "limit_price must be a positive float, got %r instead" % limit_price
                )
            else:
                self.limit_price = limit_price
                self.type = "limit"

        # setting the stop price
        self.stop_price = None
        if stop_price is not None:
            if not isinstance(stop_price, float) or stop_price < 0:
                raise ValueError(
                    "stop_price must be a positive float, got %r instead" % stop_price
                )
            else:
                self.stop_price = stop_price
                self.order_class = "oto"

        # setting internal variables
        self._identifier = None
        self._raw = None
        self._rejected = None
        self._error = None
        self._error_message = None

    def __repr__(self):
        repr = "%s order of | %d %s %s |" % (
            self.type,
            self.quantity,
            self.symbol,
            self.side,
        )
        if self.order_class:
            repr = "%s of class %s" % (repr, self.order_class)
        return repr

    def set_identifier(self, identifier):
        self._identifier = identifier

    def update_raw(self, raw):
        self._rejected = False
        self._raw = raw
        self.status = "new"

    def set_error(self, error):
        self.status = "rejected"
        self._rejected = True
        self._error = error
        self._error_message = str(error)

    def is_rejected(self):
        return self._rejected

    def add_transaction(self, price, quantity):
        transaction = self.Transaction(price=price, quantity=quantity)
        self.transactions.add(transaction)
