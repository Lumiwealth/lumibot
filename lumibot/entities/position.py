from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, Any, Self, cast

from lumibot.entities.asset import Asset, StrEnum  # todo: this should be centralized, and not repeated in Asset and Position

if TYPE_CHECKING:
    from lumibot.entities.order import Order


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

    strategy: Any
    asset: Asset
    symbol: str | None
    orders: list[Order]
    avg_fill_price: Any | None
    _quantity: Decimal
    _quantity_float: float
    _hold: Decimal | int | None
    _available: Decimal | int | None
    _raw: Any | None

    def __init__(
        self,
        strategy: Any,
        asset: Asset,
        quantity: Any,
        orders: Any | None = None,
        hold: Any = 0,
        available: Any = 0,
        avg_fill_price: Any | None = None,
    ) -> None:
        """Creates a position.

        NOTE: There are some properties that can be assigned to a position entity outside of the constructor (pnl, current_price, etc)

        """
        self.strategy = strategy
        self.asset = asset
        self.symbol = self.asset.symbol
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
            raise ValueError(f"orders parameter must be a list of orders. received type {type(orders)}")
        if orders is None:
            self.orders = []
        else:
            from lumibot.entities.order import Order

            validated_orders: list[Order] = []
            for order in cast(list[Any], orders):
                if not isinstance(order, Order):
                    raise ValueError(f"orders must be a list of Order object, found {type(order)} object.")
                validated_orders.append(order)
            self.orders = validated_orders

    @classmethod
    def simple_backtest(
        cls,
        strategy: Any,
        asset: Asset,
        quantity: Any,
        order: Order,
        avg_fill_price: Any | None = None,
        quantity_float: Any | None = None,
    ) -> Self:
        """Create a Position for the validated simple backtest fill hot path."""
        position = cls.__new__(cls)
        position.strategy = strategy
        position.asset = asset
        position.symbol = asset.symbol
        position.orders = [order]
        position.avg_fill_price = avg_fill_price
        position._quantity = quantity if type(quantity) is Decimal else Decimal(quantity)
        position._quantity_float = float(position._quantity) if quantity_float is None else float(quantity_float)
        try:
            asset_type_value = str(getattr(order, "_simple_asset_type_value"))  # noqa: B009 - avoids protected-member access noise.
        except AttributeError:
            asset_type = getattr(asset, "asset_type", None)
            asset_type_value = str.__str__(asset_type) if isinstance(asset_type, str) else str(asset_type or "")
        if asset_type_value == "crypto":
            position._hold = Decimal("0")
            position._available = Decimal("0")
        else:
            position._hold = 0
            position._available = 0
        position._raw = None
        return position

    def __repr__(self) -> str:
        return f"{self.strategy} Position: {self.quantity} shares of {self.asset} ({len(self.orders)} orders)"

    @property
    def quantity(self) -> float:
        result = self._quantity_float

        # If result is less than 0.000001, return 0.0 to avoid rounding errors.
        if abs(result) < 0.000001:
            return 0.0

        return result

    @quantity.setter
    def quantity(self, value: Any) -> None:
        self._quantity = Decimal(value)
        self._quantity_float = float(self._quantity)

    @property
    def hold(self) -> Decimal | int | None:
        return self._hold

    @hold.setter
    def hold(self, value: Any) -> None:
        self._hold = self.value_type(value)

    @hold.deleter
    def hold(self) -> int | None:
        if self.asset.asset_type != "crypto":
            return 0
        else:
            self._available = Decimal("0")

    @property
    def available(self) -> Decimal | int | None:
        return self._available

    @available.setter
    def available(self, value: Any) -> None:
        self._available = self.value_type(value)

    @available.deleter
    def available(self) -> int | None:
        if self.asset.asset_type != "crypto":
            return 0
        else:
            self._available = Decimal("0")

    def value_type(self, value: Any) -> Decimal | int | None:
        # Used to check the number types for hold and available.
        if self.asset.asset_type != "crypto":
            return 0

        default_precision = 8
        precision = self.asset.precision or default_precision
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

    def get_selling_order(self, quote_asset: Asset | None = None) -> Order | None:
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
            logger.warning(
                "get_selling_order is not supported for crypto futures. Use the broker's close_position method instead."
            )
            return None
        order = None
        if self.quantity < 0:
            from lumibot.entities.order import Order

            order = Order(self.strategy, self.asset, abs(self.quantity), "buy", quote=quote_asset)
        else:
            from lumibot.entities.order import Order

            order = Order(self.strategy, self.asset, self.quantity, "sell", quote=quote_asset)
        return order

    def add_order(self, order: Order, quantity: Decimal = Decimal(0)) -> None:
        qty = Decimal(quantity)

        if order.is_buy_order():
            increment = qty
        elif order.is_sell_order():
            increment = -qty
        else:
            increment = qty

        self._quantity += increment
        self._quantity_float = float(self._quantity)
        if order not in self.orders:
            self.orders.append(order)

    def add_simple_order(self, order: Order, quantity: Decimal, is_buy: bool) -> None:
        qty = Decimal(quantity)
        self._quantity += qty if is_buy else -qty
        self._quantity_float = float(self._quantity)
        self.orders.append(order)

    # ========= Serialization methods ===========
    def to_minimal_dict(self) -> dict[str, Any]:
        """
        Return a minimal dictionary representation of the position for progress logging.

        This creates a lightweight representation suitable for real-time progress updates,
        containing only the essential fields needed to display the position.

        Returns
        -------
        dict
            A minimal dictionary with keys:
            - asset: Minimal asset dict (from asset.to_minimal_dict())
            - qty: Position quantity
            - val: Market value (rounded to 2 decimal places)
            - pnl: Unrealized P&L (rounded to 2 decimal places)

        Example
        -------
        >>> position = Position(strategy="MyStrategy", asset=Asset("AAPL"), quantity=100)
        >>> position.to_minimal_dict()
        {'asset': {'symbol': 'AAPL', 'type': 'stock'}, 'qty': 100, 'val': 15000.00, 'pnl': 500.00}
        """
        # Get market value
        market_value = 0.0
        market_value_raw = getattr(self, "market_value", None)
        if market_value_raw is not None:
            try:
                market_value = float(market_value_raw)
            except (TypeError, ValueError):
                pass

        # Get unrealized P&L
        pnl = 0.0
        pnl_raw = getattr(self, "pnl", None)
        if pnl_raw is not None:
            try:
                pnl = float(pnl_raw)
            except (TypeError, ValueError):
                pass

        # Build minimal dict
        result: dict[str, Any] = {
            "asset": self.asset.to_minimal_dict()
            if self.asset and hasattr(self.asset, "to_minimal_dict")
            else {"symbol": str(self.symbol)},
            "qty": self.quantity if self.quantity else 0,
            "val": round(market_value, 2),
            "pnl": round(pnl, 2),
        }

        return result

    def to_dict(self) -> dict[str, Any]:
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
        orders_payload: list[Any] = []
        result: dict[str, Any] = {
            "strategy": self.strategy,
            "asset": self.asset.to_dict() if self.asset else None,
            "symbol": self.symbol,  # Added symbol field
            "quantity": float(self.quantity),
            "orders": orders_payload,  # We'll handle orders specially below
            "hold": self.hold,
            "available": float(self.available) if self.available else None,
            "avg_fill_price": float(self.avg_fill_price) if self.avg_fill_price else None,
        }

        # Add dynamically set fields if they exist (from broker)
        current_price = getattr(self, "current_price", None)
        if current_price is not None:
            result["current_price"] = float(current_price)
        market_value = getattr(self, "market_value", None)
        if market_value is not None:
            result["market_value"] = float(market_value)
        pnl = getattr(self, "pnl", None)
        if pnl is not None:
            result["pnl"] = float(pnl)
        pnl_percent = getattr(self, "pnl_percent", None)
        if pnl_percent is not None:
            result["pnl_percent"] = float(pnl_percent)
        for key in (
            "asset_type",
            "exchange",
            "currency",
            "multiplier",
            "option_type",
            "underlying_symbol",
        ):
            value = getattr(self, key, None)
            if value is not None:
                result[key] = value
        expiration = getattr(self, "expiration", None)
        if expiration is not None:  # should probably use position.asset instead
            result["expiration"] = str(expiration)
        strike = getattr(self, "strike", None)
        if strike is not None:  # should probably use position.asset instead
            result["strike"] = float(strike)

        # Handle orders carefully - ensure to_dict() is called properly
        if self.orders:
            result["orders"] = [order.to_dict() for order in self.orders]

        # DEFENSIVE: Double-check we're not including any underscore fields
        # This shouldn't be necessary with the whitelist approach, but being safe
        keys_to_remove = [k for k in result.keys() if k.startswith("_")]
        for key in keys_to_remove:
            del result[key]

        return result

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        from lumibot.entities.order import Order

        asset = Asset.from_dict(data["asset"])
        return cls(
            strategy=data["strategy"],
            asset=asset,
            quantity=Decimal(data["quantity"]),
            orders=[Order.from_dict(order) for order in data["orders"]],
            hold=Decimal(data["hold"]),
            available=Decimal(data["available"]),
            avg_fill_price=Decimal(data["avg_fill_price"]),
        )
