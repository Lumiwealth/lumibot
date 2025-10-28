from decimal import Decimal

import lumibot.entities as entities
from lumibot.entities.asset import StrEnum #todo: this should be centralized, and not repeated in Asset and Position


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
    current_price : float
        The current price of the asset.
    market_value : float
        The market value of the position.
    pnl : float
        The profit and loss of the position.
    pnl_percent : float
        The profit and loss of the position as a percentage of the average fill price.
    asset_type : str
        The type of the asset.
    exchange : str
        The exchange that the position is on.
    currency : str
        The currency that the position is denominated in.
    multiplier : float
        The multiplier of the asset.
    expiration : datetime.date
        The expiration of the asset. (Options and futures: only). Probably better to use on position.asset
    strike : float
        The strike price of the asset. (Options: only). Probably better to use on position.asset
    option_type : str
        The type of the option. (Options: only). Probably better to use on position.asset
    side : PositionSide
        The side of the position (LONG or SHORT)
    """

    class PositionSide(StrEnum):
        LONG = "LONG"
        SHORT = "SHORT"

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
        """Creates a position.

        NOTE: There are some properties that can be assigned to a position entity outside of the constructor (pnl, current_price, etc)

        """
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
            from lumibot.tools.lumibot_logger import get_logger
            logger = get_logger(__name__)
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
        """
        Convert position to dictionary for serialization.

        NOTE: We explicitly exclude internal Python fields and large data fields
        that can cause DynamoDB 400KB limit errors:
        - _bars: Historical bar data (can be 1.8MB+)
        - _raw: Raw broker response data (can be 22KB+)
        - _asset: Duplicate asset data (5KB+)
        - Any field starting with underscore (Python internals)

        We ONLY return the essential fields needed for portfolio tracking.
        """

        # Only return the essential fields - no dynamic attributes
        # This is a WHITELIST approach - only include what we explicitly want
        result = {
            "strategy": self.strategy,
            "asset": self.asset.to_dict() if self.asset else None,
            "symbol": self.symbol,  # Added symbol field
            "quantity": float(self.quantity),
            "orders": [],  # We'll handle orders specially below
            "hold": self.hold,
            "available": float(self.available) if self.available else None,
            "avg_fill_price": float(self.avg_fill_price) if self.avg_fill_price else None,
        }

        # Add dynamically set fields if they exist (from broker)
        if hasattr(self, 'current_price'):
            result['current_price'] = float(self.current_price) if self.current_price else None
        if hasattr(self, 'market_value'):
            result['market_value'] = float(self.market_value) if self.market_value else None
        if hasattr(self, 'pnl'):
            result['pnl'] = float(self.pnl) if self.pnl else None
        if hasattr(self, 'pnl_percent'):
            result['pnl_percent'] = float(self.pnl_percent) if self.pnl_percent else None
        if hasattr(self, 'asset_type'):
            result['asset_type'] = self.asset_type
        if hasattr(self, 'exchange'):
            result['exchange'] = self.exchange
        if hasattr(self, 'currency'):
            result['currency'] = self.currency
        if hasattr(self, 'multiplier'):
            result['multiplier'] = self.multiplier
        if hasattr(self, 'expiration'):  #should probably use position.asset instead
            result['expiration'] = str(self.expiration) if self.expiration else None
        if hasattr(self, 'strike'): #should probably use position.asset instead
            result['strike'] = float(self.strike) if self.strike else None
        if hasattr(self, 'option_type'): #should probably use position.asset instead
            result['option_type'] = self.option_type
        if hasattr(self, 'underlying_symbol'): #should probably use position.asset instead
            result['underlying_symbol'] = self.underlying_symbol

        # Handle orders carefully - ensure to_dict() is called properly
        if self.orders:
            result["orders"] = [order.to_dict() for order in self.orders]

        # DEFENSIVE: Double-check we're not including any underscore fields
        # This shouldn't be necessary with the whitelist approach, but being safe
        keys_to_remove = [k for k in result.keys() if k.startswith('_')]
        for key in keys_to_remove:
            del result[key]

        return result

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
