import lumibot.entities as entities


class Position:
    def __init__(self, strategy, asset, quantity, orders=None):
        self.strategy = strategy
        self.asset = asset
        self.symbol = self.asset.symbol
        self.quantity = None
        self.orders = None

        # internal variables
        self._raw = None

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

        if orders is not None and not isinstance(orders, list):
            raise ValueError(
                "orders parameter must be a list of orders. received type %s"
                % type(orders)
            )
        if orders is None:
            self.orders = []
        else:
            for order in orders:
                if not isinstance(order, entities.Order):
                    raise ValueError(
                        "orders must be a list of Order object, found %s object."
                        % type(order)
                    )
            self.orders = orders

    def __repr__(self):
        repr = "%d shares of %s" % (self.quantity, self.symbol)
        return repr

    def update_raw(self, raw):
        self._raw = raw

    def get_selling_order(self):
        order = entities.Order(self.strategy, self.asset, self.quantity, "sell")
        return order

    def add_order(self, order, quantity):
        increment = quantity if order.side == order.BUY else -quantity
        self.quantity += increment
        if order not in self.orders:
            self.orders.append(order)
