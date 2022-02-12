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
        repr = "%d shares of %s" % (self.quantity, self.symbol)
        return repr

    @property
    def quantity(self):
        return self._quantity

    @quantity.setter
    def quantity(self, value):
        self._quantity = int(value) if not isinstance(value, Decimal) else value

    @property
    def hold(self):
        return self._hold

    @hold.setter
    def hold(self, value):
        if self.asset.asset_type != 'crypto':
            return 0

        if isinstance(value, Decimal):
            self._hold = value.quantize(Decimal(self.asset.precision))
        elif isinstance(value, (int, float, str,)):
            self._hold = Decimal(str(value)).quantize(Decimal(self.asset.precision))

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
        if self.asset.asset_type != 'crypto':
            return 0

        if isinstance(value, Decimal):
            self._available = value.quantize(Decimal(self.asset.precision))
        elif isinstance(value, (int, float, str)):
            self._available = Decimal(str(value)).quantize(Decimal(self.asset.precision))

    @available.deleter
    def available(self):
        if self.asset.asset_type != 'crypto':
            return 0
        else:
            self._available = Decimal('0')

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
