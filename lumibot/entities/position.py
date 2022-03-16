import lumibot.entities as entities
from decimal import Decimal, getcontext

class Position:
    def __init__(self, strategy, asset, quantity, orders=None, hold=0, available=0):
        self.strategy = strategy
        self.asset = asset
        self.symbol = self.asset.symbol
        self.orders = None

        # Quantity is the total number of shares/units owned in the position.
        # setting the quantity
        self.quantity = quantity

        # Hold are the assets that are not free in the portfolio. (Crypto: only)
        # Available are the assets that are free in the portfolio. (Crypto: only)
        self.hold = hold
        self.available = available

        # internal variables
        self._raw = None


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
        repr = "%f shares of %s" % (self.quantity, self.symbol)
        return repr

    @property
    def quantity(self):
        if self.asset.asset_type == "crypto":
            return self._quantity
        return int(self._quantity)

    @quantity.setter
    def quantity(self, value):
        self._quantity = Decimal(value)

    @property
    def hold(self):
        return self._hold

    @hold.setter
    def hold(self, value):
        self._hold = self.value_type(value)

    @hold.deleter
    def hold(self):
        if self.asset.asset_type != 'crypto':
            return 0
        else:
            self._available = Decimal('0')

    @property
    def available(self):
        return self._available

    @available.setter
    def available(self, value):
        self._available = self.value_type(value)

    @available.deleter
    def available(self):
        if self.asset.asset_type != 'crypto':
            return 0
        else:
            self._available = Decimal('0')

    def value_type(self, value):
        # Used to check the number types for hold and available.
        if self.asset.asset_type != 'crypto':
            return 0

        default_precision = 8
        precision = self.asset.precision if hasattr(self, "asset.precision") else default_precision
        if isinstance(value, Decimal):
            return value.quantize(Decimal(precision))
        elif isinstance(value, (int, float, str,)):
            return Decimal(str(value)).quantize(Decimal(precision))

    def get_selling_order(self):
        """Returns an order that can be used to sell this position.

        Parameters
        ----------
        None

        Returns
        -------
        order : Order
            An order that can be used to sell this position.

        """
        order = None
        if self.quantity < 0:
            order = entities.Order(self.strategy, self.asset, abs(self.quantity), "buy")
        else:
            order = entities.Order(self.strategy, self.asset, self.quantity, "sell")
        return order

    def add_order(self, order, quantity):
        increment = quantity if order.side == order.BUY else -quantity
        self.quantity += increment
        if order not in self.orders:
            self.orders.append(order)
