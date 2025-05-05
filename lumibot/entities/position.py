from decimal import Decimal, getcontext

import lumibot.entities as entities


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
    hold : float
        The assets that are not free in the portfolio. (Crypto: only)
    available : float
        The assets that are free in the portfolio. (Crypto: only)
    avg_fill_price : float
        The average fill price of the position.
    """

    def __init__(
            self, 
            strategy, 
            asset, 
            quantity, 
            orders=None, 
            hold=0, 
            available=0, 
            avg_fill_price=None
        ):
        self.strategy = strategy
        self.asset = asset
        self.symbol = self.asset.symbol
        self.orders = None
        self.avg_fill_price = avg_fill_price

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
        return f"{self.strategy} Position: {self.quantity} shares of {self.asset} ({len(self.orders)} orders)"

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
        # Prevent use for crypto futures
        if getattr(self.asset, "asset_type", None) == "crypto_future":
            import logging
            logger = logging.getLogger(__name__)
            logger.warning("get_selling_order is not supported for crypto futures. Use the broker's close_position method instead.")
            return None
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

    def add_order(self, order: entities.Order, quantity: Decimal = Decimal(0)):
        increment = quantity if order.side == "buy" else -quantity
        self._quantity += Decimal(increment)
        if order not in self.orders:
            self.orders.append(order)

    # ========= Serialization methods ===========
    def to_dict(self):
        return {
            "strategy": self.strategy,
            "asset": self.asset.to_dict(),
            "quantity": float(self.quantity),
            "orders": [order.to_dict() for order in self.orders],
            "hold": self.hold,
            "available": float(self.available) if self.available else None,
            "avg_fill_price": float(self.avg_fill_price) if self.avg_fill_price else None,
        }
    
    @classmethod
    def from_dict(cls, data):
        asset = entities.Asset.from_dict(data["asset"])
        return cls(
            strategy=data["strategy"],
            asset=asset,
            quantity=Decimal(data["quantity"]),
            orders=[entities.Order.from_dict(order) for order in data["orders"]],
            hold=Decimal(data["hold"]),
            available=Decimal(data["available"]),
            avg_fill_price=Decimal(data["avg_fill_price"]),
        )