from decimal import Decimal, getcontext
import lumibot.entities as entities

from collections import deque
from dataclasses import dataclass

@dataclass
class MutableTrans:
    '''
    This is just a convenience class to use in the cost_basis_calculation. It contains the same data as the
    Transactions named tuple from order.py, but it is mutable which makes it easier to use.
    '''
    quantity: float
    price: float


class Position:
    """
    This is a Position object. It is used to keep track of the quantity of an asset owned in a strategy.
    Position objects are retreived from the broker using the get_positions() or get_position() methods.

    Attributes
    ----------
    strategy : str
        The strategy that owns this position.
    asset : Asset
        The asset that this position is for.
    symbol : str
        The symbol of the asset. e.g. AAPL for Apple stock.
    quantity : float
        The quantity of the asset owned.
    orders : list of Order
        The orders that have been executed for this position.
    """

    def __init__(self, strategy, asset, quantity, orders=None, hold=0, available=0, cost_basis=0.0):
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

        # cost_basis is the amount of money it took to aquire this position
        # it can be derived from summarising all orders, or can be aquired from Alpaca
        self.cost_basis = cost_basis
        self.calculate_cost_basis_from_orders = False

        # If we didn't receive cost basis from the broker, try to calculate it from the orders.
        if self.cost_basis == 0.0:
            self.calculate_cost_basis_from_orders = True
            self.update_cost_basis_from_orders()
            
    def __repr__(self):
        repr = "%f shares of %s" % (self.quantity, self.asset)
        return repr

    @property
    def quantity(self):
        result = float(self._quantity)

        # If result is less than 0.000001, return 0.0 to avoid rounding errors.
        if abs(result) < 0.000001:
            return 0.0

        return result

    @quantity.setter
    def quantity(self, value):
        self._quantity = Decimal(value)

    @property
    def avg_entry_price(self):
        return self.cost_basis / self.quantity if self.quantity else 0.0

    @property
    def hold(self):
        return self._hold

    @hold.setter
    def hold(self, value):
        self._hold = self.value_type(value)

    @hold.deleter
    def hold(self):
        if self.asset.asset_type != "crypto":
            return 0
        else:
            self._available = Decimal("0")

    @property
    def available(self):
        return self._available

    @available.setter
    def available(self, value):
        self._available = self.value_type(value)

    @available.deleter
    def available(self):
        if self.asset.asset_type != "crypto":
            return 0
        else:
            self._available = Decimal("0")

    def value_type(self, value):
        # Used to check the number types for hold and available.
        if self.asset.asset_type != "crypto":
            return 0

        default_precision = 8
        precision = (
            self.asset.precision
            if hasattr(self, "asset.precision")
            else default_precision
        )
        if isinstance(value, Decimal):
            return value.quantize(Decimal(precision))
        elif isinstance(
            value,
            (
                int,
                float,
                str,
            ),
        ):
            return Decimal(str(value)).quantize(Decimal(precision))

    def get_selling_order(self, quote_asset=None):
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
            order = entities.Order(
                self.strategy, self.asset, abs(self.quantity), "buy", quote=quote_asset
            )
        else:
            order = entities.Order(
                self.strategy, self.asset, self.quantity, "sell", quote=quote_asset
            )
        return order

    def add_order(self, order: entities.Order, quantity: Decimal):
        increment = quantity if order.side == order.BUY else -quantity
        self._quantity += Decimal(increment)
        if order not in self.orders:
            self.orders.append(order)
            
        # Update cost_basis to include this order as well
        if self.calculate_cost_basis_from_orders:
            self.update_cost_basis_from_orders()


    def update_cost_basis_from_orders(self):
        ''' Update positions cost_basis based on available orders and their transactions. '''

        # Separate all transactions in buys and sells
        buys = deque()
        sells = deque()
        for order in self.orders:
            for transaction in order.transactions:
                qty = float(transaction.quantity)
                qty = qty if order.side == 'buy' else -qty
                print(f'Cost_basis {order.asset}: {order} - qty: {qty} price: {transaction.price}')
                if qty > 0.0:
                    buys.append(MutableTrans(quantity=qty, price=transaction.price))
                elif qty < 0.0:
                    sells.append(MutableTrans(quantity=qty, price=transaction.price))

        # Emulate FIFO to determine cost basis
        # Loop all buys/sells until one of the lists run out
        while True:
            if len(buys) == 0 or len(sells) == 0:
                break
            diff = buys[0].quantity - abs(sells[0].quantity)
            if diff > 0.0:
                sells.popleft()
                buys[0].quantity = diff
            elif diff < 0.0:
                buys.popleft()
                sells[0].quantity = diff
            else:
                sells.popleft()
                buys.popleft()

        # After FIFOing all transactions, what we have left are the shares that makes up the cost basis.
        cost_price = 0.0
        total_qty = 0.0
        for transaction in list(buys + sells):
            cost_price += transaction.quantity * transaction.price
            total_qty += transaction.quantity

        self.cost_basis = cost_price
        print(f'Cost_basis updated: {cost_price}')