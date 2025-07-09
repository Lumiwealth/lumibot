import logging
import uuid
from collections import namedtuple
from decimal import Decimal
from enum import Enum
from threading import Event
import datetime
from typing import Union, TYPE_CHECKING

if TYPE_CHECKING:
    from lumibot.entities.asset import Asset

import lumibot.entities as entities
from lumibot.tools.types import check_positive, check_price


# Set up module-specific logger
logger = logging.getLogger(__name__)


# Custom string enum implementation for Python 3.9 compatibility
class StrEnum(str, Enum):
    """
    A string enum implementation that works with Python 3.9+
    
    This class extends str and Enum to create string enums that:
    1. Can be used like strings (string methods, comparison)
    2. Are hashable (for use in dictionaries, sets, etc.)
    3. Can be used in string comparisons without explicit conversion
    """
    def __str__(self):
        return self.value
        
    def __eq__(self, other):
        if isinstance(other, str):
            return self.value == other
        return super().__eq__(other)
    
    def __hash__(self):
        # Use the hash of the enum member, not the string value
        # This ensures proper hashability while maintaining enum identity
        return super().__hash__()


SELL = "sell"
BUY = "buy"

VALID_STATUS = ["unprocessed", "new", "open", "submitted", "fill", "partial_fill", "cancelling", "canceled", "error", "cash_settled"]
STATUS_ALIAS_MAP = {
    "cancelled": "canceled",
    "cancel": "canceled",
    "cash": "cash_settled",
    "expired": "canceled",  # Alpaca/Tradier status
    "filled": "fill",  # IBKR/Alpaca/Tradier status
    "partially_filled": "partial_filled",  # Alpaca/Tradier status
    "pending": "open",  # Tradier status
    "presubmitted": "new",  # IBKR status
    "apicancelled": "canceled",  # IBKR status
    "pendingcancel": "cancelling",  # IBKR status
    "inactive": "error",  # IBKR status
    "pendingsubmit": "new",  # IBKR status
    "apipending": "new",  # IBKR status
    "rejected": "error",  # Tradier status
    "submit": "submitted",
    "done_for_day": "canceled",  # Alpaca status
    "replaced": "canceled",  # Alpaca status
    "stopped": "canceled",  # Alpaca status
    "suspended": "canceled",  # Alpaca status
    "pending_cancel": "canceled",  # Alpaca status
    "pending_new": "new",  # Alpaca status
    "pending_replace": "canceled",  # Alpaca status
    "pending_review": "open",  # Alpaca status
    "accepted": "open",  # Alpaca status
    "calculated": "open",  # Alpaca status
    "accepted_for_bidding": "open",  # Alpaca status
    "held": "open",  # Alpaca status
}

NONE_TYPE = type(None)  # Order is shadowing 'type' parameter, this is a workaround to still access type(None)

class Order:
    Transaction = namedtuple("Transaction", ["quantity", "price"])

    class OrderClass(StrEnum):
        SIMPLE = "simple"
        BRACKET = "bracket"
        OCO = "oco"
        OTO = "oto"
        MULTILEG = "multileg"

    class OrderType(StrEnum):
        MARKET = "market"
        LIMIT = "limit"
        STOP = "stop"
        STOP_LIMIT = "stop_limit"
        TRAIL = "trailing_stop"

    class OrderSide(StrEnum):
        BUY = "buy"
        SELL = "sell"
        BUY_TO_COVER = "buy_to_cover"
        SELL_SHORT = "sell_short"
        BUY_TO_OPEN = "buy_to_open"
        BUY_TO_CLOSE = "buy_to_close"
        SELL_TO_OPEN = "sell_to_open"
        SELL_TO_CLOSE = "sell_to_close"

    class OrderStatus(StrEnum):
        UNPROCESSED = "unprocessed"
        SUBMITTED = "submitted"
        OPEN = "open"
        NEW = "new"
        CANCELLING = "cancelling"
        CANCELED = "canceled"
        FILLED = "fill"
        PARTIALLY_FILLED = "partial_fill"
        CASH_SETTLED = "cash_settled"
        ERROR = "error"
        EXPIRED = "expired"

    def __init__(
        self,
        strategy,
        asset: Union[str, "Asset"] = None,
        quantity: float = None,
        side: OrderSide = None,
        limit_price: float = None,
        stop_price: float = None,
        stop_limit_price: float = None,
        trail_price: float = None,
        trail_percent: float = None,
        secondary_limit_price: float = None,
        secondary_stop_price: float = None,
        secondary_stop_limit_price: float = None,
        secondary_trail_price: float = None,
        secondary_trail_percent: float = None,
        take_profit_price: float = None,  # Deprecated
        stop_loss_price: float = None,  # Deprecated
        stop_loss_limit_price: float = None,  # Deprecated
        time_in_force: str = "day",
        good_till_date: datetime.datetime = None,
        exchange: str = None,
        position_filled: bool = False,
        quote: "Asset" = None,
        pair: str = None,
        date_created: datetime.datetime = None,
        type: Union[OrderType, None] = None,  # Deprecated, use 'order_type' instead
        order_type: Union[OrderType, None] = None,
        order_class: Union[OrderClass, None] = OrderClass.SIMPLE,
        trade_cost: float = None,
        custom_params: dict = None,
        identifier: str = None,
        avg_fill_price: float = None,
        error_message: str = None,
        child_orders: Union[list, None] = None,
        tag: str = "",
        status: OrderStatus = "unprocessed",
    ):
        """Order class for managing individual orders.

        Order class for creating order objects that will track details
        of each order through the lifecycle of the order from creation
        through cancel, fill or closed for other reasons.

        Each order must be tied to one asset only. The order will carry
        instructions for quanity, side (buy/sell), pricing information,
        order types, valid through period, etc.

        Parameters
        ----------
        strategy : str
            The strategy that created the order.
        asset : Asset
            The asset that will be traded. While it is possible to
            create a string asset when trading stocks in the strategy
            script, all string stocks are converted to `Asset` inside
            Lumibot before creating the `Order` object. Therefore, all
            `Order` objects will only have an `Asset` object.

            If trading cryptocurrency, this asset will be the base
            of the trading pair. For example: if trading `BTC/ETH`, then
            asset will be for `BTC`.

            For cryptocurrencies, it is also possible to enter this as
            a tuple containing `(base, quote)
        quantity : float
            The number of shares or units to trade.
        side : str
            Whether the order is `buy` or `sell`.
        limit_price : float
            A Limit order is an order to buy or sell at a specified
            price or better. The Limit order ensures that if the
            order fills, it will not fill at a price less favorable
            than your limit price, but it does not guarantee a fill.
        stop_price : float
            A Stop order is an instruction to submit a buy or sell
            market order if and when the user-specified stop trigger
            price is attained or penetrated.

            In cryptocurrencies, if the order has both a `stop_price`
            and a `limit_price`, then once the stop price is met, a
            limit order will become active with the `limit_price'.
        stop_limit_price : float
            Stop loss with limit price used to ensure a specific fill price
            when the stop price is reached. For more information, visit:
            https://www.investopedia.com/terms/s/stop-limitorder.asp
        secondary_limit_price : float
            Limit price used for child orders of Advanced Orders like
            Bracket Orders and One Triggers Other (OTO) orders. One Cancels
            Other (OCO) orders do not use this field as the primary prices
            can specify all info needed to execute the OCO (because there is
            no corresponding Entry Order).
        secondary_stop_price : float
            Stop price used for child orders of Advanced Orders like
            Bracket Orders and One Triggers Other (OTO) orders. One Cancels
            Other (OCO) orders do not use this field as the primary prices
            can specify all info needed to execute the OCO (because there is
            no corresponding Entry Order).
        secondary_stop_limit_price : float
            Stop limit price used for child orders of Advanced Orders like
            Bracket Orders and One Triggers Other (OTO) orders. One Cancels
            Other (OCO) orders do not use this field as the primary prices
            can specify all info needed to execute the OCO (because there is
            no corresponding Entry Order).
        time_in_force : str
            Amount of time the order is in force. Order types include:
                - `day` Orders valid for the remainder of the day.
                - 'gtc' Good until cancelled.
                - 'gtd' Good until date. (IB only)
                - 'ioc' Immediate or cancelled.
            (Default: 'day')
        good_till_date : datetime.datetime
            This is the time order is valid for Good Though Date orders.
        take_profit_price : float
            Limit price used for bracket orders and one cancels other
            orders. (Deprecated, use 'secondary_limit_price' instead)
        stop_loss_price : float
            Stop price used for bracket orders and one cancels other
            orders. (Deprecated, use 'secondary_stop_price' instead)
        stop_loss_limit_price : float
            Stop loss with limit price used to ensure a specific fill price.
            (Deprecated, use 'secondary_stop_limit_price' instead)
        trail_price : float
            Trailing stop orders allow you to continuously and
            automatically keep updating the stop price threshold based
            on the stock price movement. `trail_price` sets the
            trailing price in dollars.
        trail_percent : float
            Trailing stop orders allow you to continuously and
            automatically keep updating the stop price threshold based
            on the stock price movement. `trail_percent` sets the
            trailing price in percent.
        secondary_trail_price : float
            Trailing stop price for child orders of Advanced Orders like Bracket or OTO orders.
        secondary_trail_percent : float
            Trailing stop percent for child orders of Advanced Orders like Bracket or OTO orders.
        position_filled : bool
            The order has been filled.
        exchange : str
            The exchange where the order will be placed.
            Default = None
        quote : Asset
            This is the base cryptocurrency. For example, if trading
            `BTC/ETH` this parameter will be 'ETH' (as an Asset object).
        pair : str
            A string representation of the trading pair. eg: `BTC/USD`.
        date_created : datetime.datetime
            The date the order was created.
        type : str or Order.OrderType
            The type of order. Possible values are: `market`, `limit`, `stop`, `stop_limit`, `trail`, `trail_limit`.
            (Deprecated, use 'order_type' instead)
        order_type : str or Order.OrderType
            The type of order. Possible values are: `market`, `limit`, `stop`, `stop_limit`, `trail`, `trail_limit`.
        order_class : str
            The order class. Possible values are: `simple`, `bracket`, `oco`, `oto`, `multileg`.
        trade_cost : float
            The cost of this order in the quote currency.
        custom_params : dict
            A dictionary of custom parameters that can be used to pass additional information to the broker. This is useful for passing custom parameters to the broker that are not supported by Lumibot.
            Eg. `custom_params={"leverage": 3}` for Kraken margin trading.
        avg_fill_price: float
            The average price that the order was fileld at.
        tag: str
            A tag that can be used to identify the order. This is useful for tracking orders in the broker. Not all
            brokers support this feature and lumibot will simply ignore it for those that don't.
        identifier : str
            A unique identifier for the order. If not provided, a random UUID will be generated.
        error_message : str
            The error message if the order was not processed successfully.
        child_orders : list
            A list of child orders that are associated with this order. This is useful for bracket orders where the
            take profit and stop loss orders are associated with the parent order, or for OCO orders where the two orders
            are associated with each other.
        Examples
        --------
        >>> from lumibot.entities import Asset
        >>> from lumibot.entities import Order
        >>> asset = Asset("MSFT", "stock")
        >>> order = self.create_order(
        ...     asset,
        ...     quantity=100,
        ...     side="buy",
        ...     limit_price=100,
        ...     stop_price=90,
        ...     stop_limit_price=80,
        ... )
        >>> order.asset
        Asset(symbol='MSFT', asset_type='stock')
        >>> order.quantity
        100
        >>> order.side
        'buy'
        >>> order.limit_price
        100
        >>> order.stop_price
        90
        >>> order.stop_limit_price
        80
        >>> order.time_in_force
        'day'
        >>> order.exchange
        'SMART'
        >>> order.position_filled
        False
        >>> order.status
        'open'
        >>> order.order_type
        'limit'
        >>> order.order_class
        'simple'
        >>> order.strategy
        'test'

        """
        # Ensure child_orders is properly initialized
        self.child_orders = child_orders if isinstance(child_orders, list) else []

        if asset == quote and asset is not None:
            logger.error(
                f"When creating an Order, asset and quote must be different. Got asset = {asset} and quote = {quote}"
            )
            return

        if isinstance(asset, str):
            asset = entities.Asset(symbol=asset)

        # Initialization default values
        self.strategy = strategy

        # If quantity is negative, then make sure it is positive
        if quantity is not None and quantity < 0:
            # Warn the user that the quantity is negative
            logger.warning(
                f"Quantity for order {identifier} is negative ({quantity}). Changing to positive because quantity must always be positive for orders."
            )
            quantity = abs(quantity)

        # It is possible for crypto currencies to arrive as a tuple of
        # two assets.
        if isinstance(asset, tuple) and asset[0].asset_type == "crypto":
            self.asset = asset[0]
            self.quote = asset[1]
        else:
            self.asset = asset
            self.quote = quote

        self.symbol = self.asset.symbol if self.asset else None
        self.identifier = identifier if identifier else uuid.uuid4().hex
        self.parent_identifier = None
        self._status = "unprocessed"
        self._date_created = date_created
        self.side = None
        self.time_in_force = time_in_force
        self.good_till_date = good_till_date
        self.position_filled = position_filled
        self.limit_price = None
        self.stop_price = None
        self.stop_limit_price = None
        self.trail_price = None
        self.trail_percent = None
        self.price_triggered = False
        self.take_profit_price = None # Used for bracket, OTO, and OCO orders TODO: Remove this because it is confusing (use child orders instead)
        self.stop_loss_price = None # Used for bracket, OTO, and OCO orders TODO: Remove this because it is confusing (use child orders instead)
        self.stop_loss_limit_price = None
        self.transactions = []
        self.order_class = order_class
        self.dependent_order = None
        self.dependent_order_filled = False
        self.order_type = order_type
        self.trade_cost = trade_cost
        self.custom_params = custom_params
        self._trail_stop_price = None  # Used by backtesting broker to track desired trailing stop price so far
        self.tag = tag
        self._avg_fill_price = avg_fill_price # The weighted average filled price for this order. Calculated if not given by broker
        self.broker_create_date = None  # The datetime the order was created by the broker
        self.broker_update_date = None  # The datetime the order was last updated by the broker
        self.status = status

        # Options:
        self.exchange = exchange

        # Cryptocurrency market.
        if self.asset and self.asset.asset_type == "crypto":
            self.pair = f"{self.asset.symbol}/{self.quote.symbol}"
        else: 
            self.pair = pair

        # setting events
        self._new_event = Event()
        self._canceled_event = Event()
        self._partial_filled_event = Event()
        self._filled_event = Event()
        self._closed_event = Event()

        # setting internal variables
        self._raw = None
        self._transmitted = False
        self._error = None
        self.error_message = error_message

        self._quantity = quantity

        try:
            self.side = side if isinstance(side, (self.OrderSide, NONE_TYPE)) else self.OrderSide(side)
        except ValueError:
            raise ValueError(f"Order: Invalid side {side}. Must be one of:"
                             f" {', '.join([str(s.value) for s in self.OrderSide])}") from None

        try:
            self.order_class = order_class \
                if isinstance(order_class, (self.OrderClass, NONE_TYPE)) else self.OrderClass(order_class)
        except ValueError:
            raise ValueError(f"Order: Invalid order_class '{order_class}'. Must be one of:"
                             f" {', '.join([str(oc.value) for oc in self.OrderClass])}") from None

        # Check - deprecated parameters and inform the user
        deprecated_params = {
            "take_profit_price": "limit_price",
            "stop_loss_price": "stop_price",
            "stop_loss_limit_price": "stop_limit_price",
            "type": "order_type",
        }
        for param, new_param in deprecated_params.items():
            if locals()[param] is not None:
                # Get caller information for better debugging
                import inspect
                frame = inspect.currentframe().f_back
                filename = frame.f_code.co_filename.split('/')[-1]  # Just the filename
                lineno = frame.f_lineno
                function_name = frame.f_code.co_name
                
                logger.warning(f"DEPRECATED in {filename}:{function_name}:{lineno} - "
                             f"Order parameter '{param}' is deprecated. Use '{new_param}' instead.")
                
                if locals()[new_param]:
                    raise ValueError(f"You cannot set both {param} and {new_param}. "
                                   f"This may cause unexpected behavior.")
                locals()[new_param] = locals()[param]

        # TODO: Remove when type//take_profit_price/stop_loss_price/stop_loss_limit_price are finally
        #  deprecated permanently
        secondary_limit_price = secondary_limit_price if secondary_limit_price is not None \
            else take_profit_price if take_profit_price is not None else secondary_limit_price
        secondary_stop_price = secondary_stop_price if secondary_stop_price is not None \
            else stop_loss_price if stop_loss_price is not None else secondary_stop_price
        secondary_stop_limit_price = secondary_stop_limit_price if secondary_stop_limit_price is not None \
            else stop_loss_limit_price if stop_loss_limit_price is not None else secondary_stop_limit_price
        order_type = order_type if order_type is not None else type if type is not None else order_type

        # Check - only provide a single stoploss modifier like trail_price, trail_percent, stop_limit_price, etc.
        unique_sl_modifiers = ["stop_limit_price", "trail_price", "trail_percent"]
        unique_secondary_modifiers = ["secondary_stop_limit_price", "secondary_trail_price", "secondary_trail_percent"]
        local_vars = locals()
        for unique_mods in [unique_sl_modifiers, unique_secondary_modifiers]:
            unique_count = sum([1 for unique_mod in unique_mods
                                if unique_mod in local_vars and local_vars[unique_mod] is not None])
            if unique_count > 1:
                raise ValueError(f"Order: You can only specify one of {', '.join(unique_mods)}. "
                                 f"{unique_count} were given.")

        # Check - Order Class values passed in the 'type' parameter is depricated. OTO/Bracket/etc should
        # be passed in the 'order_class' parameter. The 'type' parameter should only be used for order types like
        # market, limit, stop, etc.
        valid_order_classes = [order_class for order_class in Order.OrderClass]
        valid_order_types = [order_type for order_type in Order.OrderType]
        if order_type in valid_order_classes:
            logger.warning(f"Order: Passing Advanced order class ({self.order_type}) in 'order_type' field is "
                            f"deprecated. Please use 'order_class' instead. "
                            f"Valid Classes: {', '.join(valid_order_classes)} | "
                            f"Valid Types: {', '.join(valid_order_types)}")
            self.order_class = order_type
            self.order_type = None
            order_type = None

        # Check - Order Class values passed in the 'type' parameter is depricated. OTO/Bracket/etc should
        # This is done here so that the older depricated parameters are still accepted for backwards compatibility
        try:
            self.order_type = order_type \
                if isinstance(order_type, (self.OrderType, NONE_TYPE)) else self.OrderType(order_type)
        except ValueError:
            raise ValueError(f"Order: Invalid order_type {order_type}. Must be one of:"
                             f" {', '.join([str(t.value) for t in self.OrderType])}") from None

        self._set_prices(
            limit_price,
            stop_price,
            stop_limit_price,
            trail_price,
            trail_percent,
            secondary_limit_price,
            secondary_stop_price,
            secondary_stop_limit_price,
            secondary_trail_price,
            secondary_trail_percent,
        )

        self._set_type_and_prices(
            limit_price,
            stop_price,
            stop_limit_price,
            trail_price,
            trail_percent,
            position_filled,
        )
        self._set_order_class_children(
            secondary_limit_price,
            secondary_stop_price,
            secondary_stop_limit_price,
            secondary_trail_price,
            secondary_trail_percent,
        )
    def is_advanced_order(self):
        return self.order_class in [self.OrderClass.OCO, self.OrderClass.BRACKET, self.OrderClass.OTO]

    def is_buy_order(self):
        return self.side is not None and (
            self.side == self.OrderSide.BUY or
            self.side == self.OrderSide.BUY_TO_OPEN or
            self.side == self.OrderSide.BUY_TO_COVER or
            self.side == self.OrderSide.BUY_TO_CLOSE
        )

    def is_sell_order(self):
        return self.side is not None and (
            self.side == self.OrderSide.SELL or
            self.side == self.OrderSide.SELL_SHORT or
            self.side == self.OrderSide.SELL_TO_OPEN or
            self.side == self.OrderSide.SELL_TO_CLOSE
        )

    def is_stop_order(self):
        return self.order_type in [self.OrderType.STOP, self.OrderType.STOP_LIMIT, self.OrderType.TRAIL]

    def is_parent(self) -> bool:
        """
        Check if the order is a parent order. Parent orders are typically Multileg orders where the child orders
        do the actual trading and cash settlements and the parent order is a container that holds them all together.
        Lumibot should not update any positions/cash balances when parent orders fill.

        Returns
        -------
        bool
            True if the order is a parent order, False otherwise.
        """
        return bool(self.child_orders)

    def add_child_order(self, o):
        """
        Add a child order to the parent order.

        Parameters
        ----------
        o : Order
            The child order to add to the parent order.
        """
        self.child_orders.append(o)

    def update_trail_stop_price(self, price):
        """Update the trail stop price.
        This will be used to determine if a trailing stop order should be triggered in a backtest.

        Parameters
        ----------

        price : float
            The last price of the asset. For trailing stop orders, this is the price that will be used to update the trail stop price.
        """
        # If the order is not a trailing stop order, then do nothing.
        if self.order_type != self.OrderType.TRAIL:
            return

        # Update the trail stop price if we have a trail_percent
        if self.trail_percent is not None:
            # Get potential trail stop price
            if self.is_buy_order():
                potential_trail_stop_price = price * (1 + self.trail_percent)
            # Buy/Sell are the only valid sides, so we can use else here.
            else:
                potential_trail_stop_price = price * (1 - self.trail_percent)

            # Set the trail stop price if it has not been set yet.
            if self._trail_stop_price is None:
                self._trail_stop_price = potential_trail_stop_price
                return

            # Ratchet down the trail stop price for a buy order if the price has decreased.
            if self.is_sell_order() and potential_trail_stop_price < self._trail_stop_price:
                # Update the trail stop price
                self._trail_stop_price = potential_trail_stop_price

            # Ratchet up the trail stop price for a sell order if the price has increased.
            if self.is_sell_order() and potential_trail_stop_price > self._trail_stop_price:
                # Update the trail stop price
                self._trail_stop_price = potential_trail_stop_price

        # Update the trail stop price if we have a trail_price
        if self.trail_price is not None:
            # Get potential trail stop price
            if self.is_buy_order():
                potential_trail_stop_price = price + self.trail_price
            elif self.is_sell_order():
                potential_trail_stop_price = price - self.trail_price
            else:
                raise ValueError(f"side must be either 'buy' or 'sell'. Got {self.side} instead.")

            # Set the trail stop price if it has not been set yet.
            if self._trail_stop_price is None:
                self._trail_stop_price = potential_trail_stop_price
                return

            # Ratchet down the trail stop price for a buy order if the price has decreased.
            if self.is_buy_order() and potential_trail_stop_price < self._trail_stop_price:
                # Update the trail stop price
                self._trail_stop_price = potential_trail_stop_price

            # Ratchet up the trail stop price for a sell order if the price has increased.
            if self.is_sell_order() and potential_trail_stop_price > self._trail_stop_price:
                # Update the trail stop price
                self._trail_stop_price = potential_trail_stop_price

    def get_current_trail_stop_price(self):
        """
        Get the current trailing stop price. This is the price that the trailing stop order will be triggered at.

        Returns
        -------
        float
            The current trailing stop price.
        """
        return self._trail_stop_price

    def _set_prices(
            self,
            limit_price,
            stop_price,
            stop_limit_price,
            trail_price,
            trail_percent,
            secondary_limit_price,
            secondary_stop_price,
            secondary_stop_limit_price,
            secondary_trail_price,
            secondary_trail_percent,
    ):
        self.limit_price = check_price(limit_price, "limit_price must be float.", nullable=True)
        self.stop_price = check_price(stop_price, "stop_price must be float.", nullable=True)
        self.stop_limit_price = check_price(stop_limit_price, "stop_limit_price must be float.",
                                            nullable=True)
        self.trail_price = check_price(trail_price, "trail_price must be positive float.", nullable=True)
        self.trail_percent = check_positive(trail_percent, float, "trail_percent must be positive float.")
        self.secondary_limit_price = check_price(secondary_limit_price, "secondary_limit_price must be float.",
                                                 nullable=True)
        self.secondary_stop_price = check_price(secondary_stop_price, "secondary_stop_price must be float.",
                                                nullable=True)
        self.secondary_stop_limit_price = check_price(secondary_stop_limit_price,
                                                      "secondary_stop_limit_price must be float.", nullable=True)
        self.secondary_trail_price = check_price(secondary_trail_price, "secondary_trail_price must be positive float.",
                                                 nullable=True)
        self.secondary_trail_percent = check_positive(secondary_trail_percent, float, "secondary_trail_percent must be positive float.")

    def _set_type_and_prices(
        self,
        limit_price,
        stop_price,
        stop_limit_price,
        trail_price,
        trail_percent,
        position_filled,
    ):
        if self.order_type is None:
            # Check if this is a trailing stop order
            if trail_price is not None or trail_percent is not None:
                self.order_type = self.OrderType.TRAIL

            # Check if this is a market order
            elif limit_price is None and stop_price is None:
                self.order_type = self.OrderType.MARKET

            # Check if this is a stop order
            elif limit_price is None and stop_price is not None:
                self.order_type = self.OrderType.STOP if not stop_limit_price else self.OrderType.STOP_LIMIT

            # Check if this is a limit order
            elif limit_price is not None and stop_price is None:
                self.order_type = self.OrderType.LIMIT

            elif self.order_class == self.OrderClass.OCO:
                # This is a "One-Cancel-Other" advanced order. All info needed to calculate the child orders exists
                # so they will be created automatically here unless specified directly by the user. It is expected that
                # the broker will only submit the child orders as active orders, the parent order is just to tie them
                # together.
                self.order_type = self.OrderType.LIMIT

            else:
                raise ValueError(
                    "Order type could not be determined. If you are trying to create an advanced order such \
                                 as a Bracket Order, OCO or OTO, please specify the order_class parameter when \
                                 creating the order."
                )

    def _set_order_class_children(self, secondary_limit_price, secondary_stop_price, secondary_stop_limit_price,
                                  secondary_trail_price, secondary_trail_percent):

        if self.order_class == self.OrderClass.OCO:
            # This is a "One-Cancel-Other" advanced order. All info needed to calculate the child orders exists
            # so they will be created automatically here unless specified directly by the user. It is expected that
            # the broker will only submit the child orders as active orders, the parent order is just to tie them
            # together.
            if not self.child_orders and self.stop_price is not None and self.limit_price is not None:
                # Create the child orders
                limit_order = Order(
                    strategy=self.strategy,
                    asset=self.asset,
                    quantity=self.quantity,
                    side=self.side,
                    limit_price=self.limit_price,
                    order_type=Order.OrderType.LIMIT,
                )
                stop_order = Order(
                    # Stop Type will be filled in automatically for child based on the Stop Modifiers
                    strategy=self.strategy,
                    asset=self.asset,
                    quantity=self.quantity,
                    side=self.side,
                    stop_price=self.stop_price,
                    stop_limit_price=self.stop_limit_price,
                    trail_price=self.trail_price,
                    trail_percent=self.trail_percent,
                )
                # Set dependencies so that the two orders will cancel the other in BackTesting
                limit_order.dependent_order = stop_order
                stop_order.dependent_order = limit_order
                self.child_orders = [limit_order, stop_order]

            elif len(self.child_orders) == 2:
                # Verify that the child orders are valid order objects
                for child_order in self.child_orders:
                    if not isinstance(child_order, Order):
                        raise ValueError("Child orders must be of type Order")
            else:
                raise ValueError("Order class is OCO but child orders are not set and no limit/stop prices have "
                                 "been provided.")

        elif self.order_class == self.OrderClass.BRACKET:
            # This is a "Bracket" advanced order which typically consists of a primary (entry) order and
            # two child orders. The user can provide their own list of child orders to the parent order object to
            # override the defaults. It is expected that the broker object will submit the parent (entry) order as
            # well as the child orders.

            # Create the child orders. There can be one or two child orders depending on what secondary values
            # have been provided.
            if not self.child_orders:
                child_side = self.OrderSide.SELL_TO_CLOSE if self.is_buy_order() else self.OrderSide.BUY_TO_CLOSE
                if secondary_limit_price is not None:
                    self.child_orders.append(
                        Order(
                            strategy=self.strategy,
                            asset=self.asset,
                            quantity=self.quantity,
                            side=child_side,
                            limit_price=secondary_limit_price,
                            order_type=Order.OrderType.LIMIT,
                        )
                    )
                if secondary_stop_price is not None:
                    self.child_orders.append(
                        Order(
                            # Stop Type will be filled in automatically for child based on the Stop Modifiers
                            strategy=self.strategy,
                            asset=self.asset,
                            quantity=self.quantity,
                            side=child_side,
                            stop_price=secondary_stop_price,
                            stop_limit_price=secondary_stop_limit_price,
                            trail_price=secondary_trail_price,
                            trail_percent=secondary_trail_percent,
                        )
                    )

            # Error check that at least 1 child order exists
            if not self.child_orders:
                raise ValueError("Order class is BRACKET but no child orders or secondary limit/stop prices "
                                 "have been provided.")
            elif len(self.child_orders) > 1:
                # Set dependencies so that the two orders will cancel the other in BackTesting
                self.child_orders[0].dependent_order = self.child_orders[1]
                self.child_orders[1].dependent_order = self.child_orders[0]

        elif self.order_class == self.OrderClass.OTO:
            # This is a "One-Triggers-One" advanced order. This order is typically used as a "half-bracket" where the
            # parent order is an entry order and the child order is either the stop or limit order that will be
            # placed if the parent order is filled. It is expected that the broker object will submit the
            # parent (entry) order as well as the child order.
            if secondary_limit_price is not None and secondary_stop_price is not None:
                raise ValueError("Order class is OTO but both secondary limit and stop prices have been provided. "
                                 "OTO only allows for one of these values.")
            if secondary_limit_price is None and secondary_stop_price is None:
                raise ValueError("Order class is OTO but no secondary limit or stop prices have been provided. Must "
                                 "provide exactly one of 'secondary_limit_price' or 'secondary_stop_price'.")

            # Implement child orders: Open Position Order, one Triggered Order (Limit or Stop)
            if not self.child_orders:
                child_side = self.OrderSide.SELL_TO_CLOSE if self.is_buy_order() else self.OrderSide.BUY_TO_CLOSE
                if secondary_limit_price is not None:
                    self.child_orders.append(
                        Order(
                            strategy=self.strategy,
                            asset=self.asset,
                            quantity=self.quantity,
                            side=child_side,
                            limit_price=secondary_limit_price,
                            order_type=Order.OrderType.LIMIT,
                        )
                    )
                elif secondary_stop_price is not None:
                    self.child_orders.append(
                        Order(
                            # Stop Type will be filled in automatically for child based on the Stop Modifiers
                            strategy=self.strategy,
                            asset=self.asset,
                            quantity=self.quantity,
                            side=child_side,
                            stop_price=secondary_stop_price,
                            stop_limit_price=secondary_stop_limit_price,
                            trail_price=secondary_trail_price,
                            trail_percent=secondary_trail_percent,
                        )
                    )

            # Error check that only 1 child order exists
            if len(self.child_orders) != 1:
                raise ValueError(f"Order class is OTO found {len(self.child_orders)} child orders. OTO requires exactly"
                                 f" one child order.")

    @property
    def avg_fill_price(self):
        return self._avg_fill_price

    @avg_fill_price.setter
    def avg_fill_price(self, value):
        self._avg_fill_price = round(float(value), 2) if value is not None else None

    @property
    def identifier(self):
        return self._identifier

    @identifier.setter
    def identifier(self, value):
        self._identifier = value
        if self.is_parent():
            for child_order in self.child_orders:
                child_order.parent_identifier = value

    @property
    def status(self):
        return self._status

    @status.setter
    def status(self, value):
        if value and isinstance(value, str):
            if value.lower() in VALID_STATUS:
                self._status = value.lower()
            elif value.lower() in STATUS_ALIAS_MAP:
                self._status = STATUS_ALIAS_MAP[value.lower()]
            else:
                self._status = value.lower()
                # Log an error
                logger.error(f"Invalid order status: {value}")

    @property
    def quantity(self):
        return self._quantity

    @quantity.setter
    def quantity(self, value):
        # All non-crypto assets must be of type 'int'.
        if not isinstance(value, Decimal):
            if isinstance(value, float):
                value = Decimal(str(value))

        quantity = Decimal(value)
        self._quantity = quantity

        # Update the quantity for all child orders
        for child_order in self.child_orders:
            child_order.quantity = quantity

    def __hash__(self):
        return hash(self.identifier)

    # Compares two order objects to see if they are the same.
    def __eq__(self, other):
        # If the other object is not an Order object, then they are not equal.
        if not isinstance(other, Order):
            return False

        # If the other object is an Order object, then compare the identifier.
        return (
            self.identifier == other.identifier
            and self.asset == other.asset
            and self.quantity == other.quantity
            and self.side == other.side
        )

    def __repr__(self):
        if self.asset is None:
            self.rep_asset = self.symbol
        else:
            if self.asset.asset_type == "crypto":
                self.rep_asset = f"{self.pair}"
            elif self.asset.asset_type == "future":
                self.rep_asset = f"{self.symbol} {self.asset.expiration}"
            elif self.asset.asset_type == "option":
                self.rep_asset = f"{self.symbol} {self.asset.expiration} " f"{self.asset.right} {self.asset.strike}"
            else:
                self.rep_asset = self.symbol

        price = None
        for attribute in ["limit_price", "stop_price", "take_profit_price"]:
            if getattr(self, attribute) is not None:
                price = getattr(self, attribute)
                break
        if self.is_filled():
            price = self.get_fill_price()

        # If there are child orders, list them in the repr
        if self.child_orders:
            # If there is an order class, use that in the repr instead of the type
            if self.order_class:
                repr_str = f"{self.order_class} {self.quantity} order |"
            else:
                repr_str = f"{self.order_type} {self.quantity} order |"

            # Add the child orders to the repr
            for child_order in self.child_orders:
                child_str = str(child_order).replace('|', '')
                repr_str = f"{repr_str} child {child_str} |"
        else:
            repr_str = f"{self.order_type} order of | {self.quantity} {self.rep_asset} {self.side} |"
        if price:
            repr_str = f"{repr_str} @ ${price}"

        repr_str = f"{repr_str} {self.status}"
        return repr_str

    def set_identifier(self, identifier):
        self.identifier = identifier

    def add_transaction(self, price, quantity):
        transaction = self.Transaction(price=price, quantity=quantity)
        self.transactions.append(transaction)

    def cash_pending(self, strategy):
        # Returns the impact to cash of any unfilled shares.
        quantity_unfilled = self.quantity - sum([transaction.quantity for transaction in self.transactions])
        if quantity_unfilled == 0:
            return 0
        elif len(self.transactions) == 0:
            cash_value = self.quantity * strategy.get_last_price(self.asset)
        else:
            cash_value = quantity_unfilled * self.transactions[-1].price
        if self.side == SELL:
            return cash_value
        else:
            return -cash_value

    def get_fill_price(self):
        """
        Get the weighted average filled price for this order. Option contracts often encounter partial fills,
        so the weighted average is the only valid price that can be used for PnL calculations.

        Returns
        -------
        float
            The weighted average filled price for this order. 0.0 will be returned if the order
            has not yet been filled.
        """
        # Average price is set directly by the broker for parent orders
        if self.avg_fill_price is not None:
            return self.avg_fill_price

        # Only calculate on filled orders
        if not self.transactions or not self.quantity:
            return None

        # Check if x.price is None
        if any(x.price is None for x in self.transactions):
            return None

        # calculate the weighted average filled price since options often encounter partial fills
        # Some Backtest runs are using a Decimal for the Transaction quantity, so we need to convert to float
        return round(sum([float(x.price) * float(x.quantity) for x in self.transactions]) / float(self.quantity), 2)

    def is_active(self):
        """
        Returns whether this order is active.
        Returns
        -------
        bool
            True if the order is active, False otherwise.
        """
        active_children = any([child for child in self.child_orders if child.is_active()])
        return not self.is_filled() and not self.is_canceled() or active_children

    def is_canceled(self):
        """
        Returns whether this order has been cancelled.

        Returns
        -------
        bool
            True if the order has been cancelled, False otherwise.
        """
        return self.status.lower() in ["cancelled", "canceled", "cancel", "error"]

    def is_filled(self):
        """
        Returns whether this order has been filled.

        Returns
        -------
        bool
            True if the order has been filled, False otherwise.
        """
        if self.position_filled:
            return True
        elif self.status.lower() in ["filled", "fill", "cash_settled"]:
            return True
        else:
            return False

    def equivalent_status(self, status) -> bool:
        """Returns if the status is equivalent to the order status."""
        status = status.status if isinstance(status, Order) else status

        if not status:
            return False
        elif self.status.lower() in [status.lower(), STATUS_ALIAS_MAP.get(status.lower(), "")]:
            return True
        # open/new status is equivalent
        elif {self.status.lower(), status.lower()}.issubset({"open", "new"}):
            return True
        else:
            return False

    @classmethod
    def is_equivalent_status(cls, status1, status2) -> bool:
        """Returns if the 2 statuses passed are equivalent."""

        if not status1 or not status2:
            return False
        elif status1.lower() == status2.lower():  # Direct match check
            return True
        elif status1.lower() in STATUS_ALIAS_MAP.get(status2.lower(), []):
            return True
        elif status2.lower() in STATUS_ALIAS_MAP.get(status1.lower(), []):
            # Bidirectional alias check
            return True
        elif {status1.lower(), status2.lower()}.issubset({"open", "new"}):
            return True
        else:
            return False

    def set_error(self, error):
        self.status = "error"
        self._error = error
        self.error_message = str(error)
        self._closed_event.set()

    def was_transmitted(self):
        return self._transmitted

    def update_raw(self, raw):
        if raw is not None:
            self._transmitted = True
            self._raw = raw

    def to_position(self, quantity):
        position_qty = quantity
        if self.side == SELL:
            position_qty = -quantity

        position = entities.Position(
            self.strategy,
            self.asset,
            position_qty,
            orders=[self],
            avg_fill_price=self.avg_fill_price
        )
        return position

    def get_increment(self):
        increment = self.quantity
        if self.side == SELL:
            if not self.is_option():
                increment = -increment
        if self.side == BUY:
            if self.is_option():
                increment = -increment
        return float(increment)

    def is_option(self):
        """Return true if this order is an option."""
        return True if self.asset.asset_type == "option" else False

    # ======Setting the events methods===========

    def set_new(self):
        self._new_event.set()

    def set_canceled(self):
        self._canceled_event.set()
        self._closed_event.set()

    def set_partially_filled(self):
        self._partial_filled_event.set()

    def set_filled(self):
        self._filled_event.set()
        self._closed_event.set()

    # =========Waiting methods==================

    def wait_to_be_registered(self):
        logging.info("Waiting for order %r to be registered" % self)
        self._new_event.wait()
        logging.info("Order %r registered" % self)

    def wait_to_be_closed(self):
        logging.info("Waiting for broker to execute order %r" % self)
        self._closed_event.wait()
        logging.info("Order %r executed by broker" % self)

    # ========= Serialization methods ===========

    def to_dict(self):
        # Initialize an empty dictionary for serializable attributes
        order_dict = {}

        # List of non-serializable keys (thread locks, events, etc.)
        non_serializable_keys = [
            "_new_event", "_canceled_event", "_partial_filled_event", "_filled_event", "_closed_event"
        ]

        # Iterate through all attributes in the object's __dict__
        for key, value in self.__dict__.items():
            # Skip known non-serializable attributes by name
            if key in non_serializable_keys:
                continue

            # Convert datetime objects to ISO format for JSON serialization
            if isinstance(value, datetime.datetime):
                order_dict[key] = value.isoformat()

            # If it is a Decimal object, convert it to a float
            elif isinstance(value, Decimal):
                order_dict[key] = float(value)

            # Recursively handle objects that have their own to_dict method (like asset, quote, etc.)
            elif hasattr(value, "to_dict"):
                order_dict[key] = value.to_dict()

            # Handle lists of objects, ensuring to call to_dict on each if applicable
            elif isinstance(value, list):
                order_dict[key] = [item.to_dict() if hasattr(item, "to_dict") else item for item in value]

            # Add serializable attributes directly
            else:
                order_dict[key] = value

        return order_dict
    
    @classmethod
    def from_dict(cls, order_dict):
        # Extract the core essential arguments to pass to __init__
        asset_data = order_dict.get('asset')
        asset_obj = None
        if asset_data and isinstance(asset_data, dict):
            # Assuming Asset has its own from_dict method
            asset_obj = entities.Asset.from_dict(asset_data)
        
        # Extract essential arguments, using None if the values are missing
        strategy = order_dict.get('strategy', None)
        side = order_dict.get('side', None)  # Default to None if side is missing
        quantity = order_dict.get('quantity', None)
        
        # Create the initial object using the essential arguments
        obj = cls(
            strategy=strategy,
            side=side,
            asset=asset_obj,  # Pass the constructed asset object
            quantity=quantity
        )

        # List of non-serializable keys (thread locks, events, etc.)
        non_serializable_keys = [
            "_new_event", "_canceled_event", "_partial_filled_event", "_filled_event", "_closed_event"
        ]

        # Handle additional fields directly after the instance is created
        for key, value in order_dict.items():
            if key not in ['strategy', 'side', 'asset', 'quantity'] and key not in non_serializable_keys:
                
                # Convert datetime strings back to datetime objects
                if isinstance(value, str) and "T" in value:
                    try:
                        setattr(obj, key, datetime.datetime.fromisoformat(value))
                    except ValueError:
                        setattr(obj, key, value)
                
                # Recursively convert nested objects using from_dict (for objects like quote)
                elif isinstance(value, dict) and hasattr(cls, key) and hasattr(getattr(cls, key), 'from_dict'):
                    nested_class = getattr(cls, key)
                    setattr(obj, key, nested_class.from_dict(value))
                
                # Handle list of orders (child_orders)
                elif isinstance(value, list) and key == 'child_orders':
                    child_orders = [cls.from_dict(item) for item in value]  # Recursively create Order objects
                    setattr(obj, key, child_orders)
                
                # Set simple values directly
                else:
                    setattr(obj, key, value)

        return obj
