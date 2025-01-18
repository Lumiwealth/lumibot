import logging
import uuid
from collections import namedtuple
from decimal import Decimal
from threading import Event
import datetime
from typing import Union

import lumibot.entities as entities
from lumibot.tools.types import check_positive, check_price

SELL = "sell"
BUY = "buy"

VALID_STATUS = ["unprocessed", "new", "open", "submitted", "fill", "partial_fill", "cancelling", "canceled", "error", "cash_settled"]
STATUS_ALIAS_MAP = {
    "cancelled": "canceled",
    "cancel": "canceled", 
    "cash": "cash_settled",
    "expired": "canceled",  # Alpaca/Tradier status
    "filled": "fill",  # Alpaca/Tradier status
    "partially_filled": "partial_filled",  # Alpaca/Tradier status
    "pending": "open",  # Tradier status
    "presubmitted": "new",  # IBKR status
    "filled": "fill",  # IBKR status
    "cancelled": "canceled",  # IBKR status
    "apicancelled": "canceled",  # IBKR status
    "pendingcancel": "cancelling",  # IBKR status
    "inactive": "error",  # IBKR status
    "pendingsubmit": "new",  # IBKR status 
    "presubmitted": "new",  # IBKR status
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
    "expired": "canceled",  # Tradier status
}

class Order:
    Transaction = namedtuple("Transaction", ["quantity", "price"])

    class OrderClass:
        BRACKET = "bracket"
        OCO = "oco"
        OTO = "oto"
        MULTILEG = "multileg"

    class OrderType:
        MARKET = "market"
        LIMIT = "limit"
        STOP = "stop"
        STOP_LIMIT = "stop_limit"
        TRAIL = "trailing_stop"
        BRACKET = "bracket"
        OCO = "oco"
        OTO = "oto"

    class OrderSide:
        BUY = "buy"
        SELL = "sell"
        BUY_TO_COVER = "buy_to_cover"
        SELL_SHORT = "sell_short"
        BUY_TO_OPEN = "buy_to_open"
        BUY_TO_CLOSE = "buy_to_close"
        SELL_TO_OPEN = "sell_to_open"
        SELL_TO_CLOSE = "sell_to_close"

    class OrderStatus:
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
        take_profit_price: float = None,
        stop_loss_price: float = None,
        stop_loss_limit_price: float = None,
        trail_price: float = None,
        trail_percent: float = None,
        time_in_force: str = "day",
        good_till_date: datetime.datetime = None,
        exchange: str = None,
        position_filled: bool = False,
        quote: "Asset" = None,
        pair: str = None,
        date_created: datetime.datetime = None,
        type: OrderType = None,
        order_class: OrderClass = None,
        trade_cost: float = None,
        custom_params: dict = {},
        identifier: str = None,
        avg_fill_price: float = None,
        error_message: str = None,
        child_orders: list = None,
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
            orders.
        stop_loss_price : float
            Stop price used for bracket orders and one cancels other
            orders.
        stop_loss_limit_price : float
            Stop loss with limit price used for bracket orders and one
            cancels other orders.
        trail_price : float
            Trailing stop orders allow you to continuously and
            automatically keep updating the stop price threshold based
            on the stock price movement. `trail_price` sets the
            trailing price in dollars.
        trail_percent : float
            Trailing stop orders allow you to continuously and
            automatically keep updating the stop price threshold based
            on the stock price movement. `trail_price` sets the
            trailing price in percent.
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
        type : str
            The type of order. Possible values are: `market`, `limit`, `stop`, `stop_limit`, `trail`, `trail_limit`, `bracket`, `bracket_limit`, `bracket_stop`, `bracket_stop_limit`.
        order_class : str
            The order class. Possible values are: `bracket`, `oco`, `oto`, `multileg`.
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
        ...     take_profit_price=110,
        ...     stop_loss_price=90,
        ...     stop_loss_limit_price=80,
        ... )
        >>> order.asset
        Asset(symbol='MSFT', asset_type='stock')
        >>> order.quantity
        100
        >>> order.side
        'buy'
        >>> order.limit_price
        100
        >>> order.take_profit_price
        110
        >>> order.stop_loss_price
        90
        >>> order.stop_loss_limit_price
        80
        >>> order.time_in_force
        'day'
        >>> order.exchange
        'SMART'
        >>> order.position_filled
        False
        >>> order.status
        'open'
        >>> order.type
        'limit'
        >>> order.order_class
        'bracket'
        >>> order.strategy
        'test'

        """
        # Ensure child_orders is properly initialized
        self.child_orders = child_orders if isinstance(child_orders, list) else []

        if asset == quote and asset is not None:
            logging.error(
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
            logging.warning(
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
        self.type = type
        self.trade_cost = trade_cost
        self.custom_params = custom_params
        self._trail_stop_price = None
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

        self.side = side

        self._set_type(
            limit_price,
            stop_price,
            take_profit_price,
            stop_loss_price,
            stop_loss_limit_price,
            trail_price,
            trail_percent,
            position_filled,
        )

    def is_buy_order(self):
        return self.side is not None and (
            self.side.lower() == self.OrderSide.BUY or
            self.side.lower() == self.OrderSide.BUY_TO_OPEN or
            self.side.lower() == self.OrderSide.BUY_TO_COVER or
            self.side.lower() == self.OrderSide.BUY_TO_CLOSE
        )
    
    def is_sell_order(self):
        return self.side is not None and (
            self.side.lower() == self.OrderSide.SELL or
            self.side.lower() == self.OrderSide.SELL_SHORT or
            self.side.lower() == self.OrderSide.SELL_TO_OPEN or
            self.side.lower() == self.OrderSide.SELL_TO_CLOSE
        )

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
        if self.type != "trailing_stop":
            return

        # Update the trail stop price if we have a trail_percent
        if self.trail_percent is not None:
            # Get potential trail stop price
            if self.side == "buy":
                potential_trail_stop_price = price * (1 + self.trail_percent)
            # Buy/Sell are the only valid sides, so we can use else here.
            else:
                potential_trail_stop_price = price * (1 - self.trail_percent)

            # Set the trail stop price if it has not been set yet.
            if self._trail_stop_price is None:
                self._trail_stop_price = potential_trail_stop_price
                return

            # Ratchet down the trail stop price for a buy order if the price has decreased.
            if self.side == "buy" and potential_trail_stop_price < self._trail_stop_price:
                # Update the trail stop price
                self._trail_stop_price = potential_trail_stop_price

            # Ratchet up the trail stop price for a sell order if the price has increased.
            if self.side == "sell" and potential_trail_stop_price > self._trail_stop_price:
                # Update the trail stop price
                self._trail_stop_price = potential_trail_stop_price

        # Update the trail stop price if we have a trail_price
        if self.trail_price is not None:
            # Get potential trail stop price
            if self.side == "buy":
                potential_trail_stop_price = price + self.trail_price
            elif self.side == "sell":
                potential_trail_stop_price = price - self.trail_price
            else:
                raise ValueError(f"side must be either 'buy' or 'sell'. Got {self.side} instead.")

            # Set the trail stop price if it has not been set yet.
            if self._trail_stop_price is None:
                self._trail_stop_price = potential_trail_stop_price
                return

            # Ratchet down the trail stop price for a buy order if the price has decreased.
            if self.side == "buy" and potential_trail_stop_price < self._trail_stop_price:
                # Update the trail stop price
                self._trail_stop_price = potential_trail_stop_price

            # Ratchet up the trail stop price for a sell order if the price has increased.
            if self.side == "sell" and potential_trail_stop_price > self._trail_stop_price:
                # Update the trail stop price
                self._trail_stop_price = potential_trail_stop_price

    def _set_type(
        self,
        limit_price,
        stop_price,
        take_profit_price,
        stop_loss_price,
        stop_loss_limit_price,
        trail_price,
        trail_percent,
        position_filled,
    ):
        if self.type is None:
            # Check if this is a trailing stop order
            if trail_price is not None or trail_percent is not None:
                self.type = self.OrderType.TRAIL

            # Check if this is a market order
            elif limit_price is None and stop_price is None:
                self.type = self.OrderType.MARKET

            # Check if this is a stop order
            elif limit_price is None and stop_price is not None:
                self.type = self.OrderType.STOP

            # Check if this is a limit order
            elif limit_price is not None and stop_price is None:
                self.type = self.OrderType.LIMIT

            # Check if this is a stop limit order
            elif limit_price is not None and stop_price is not None:
                self.type = self.OrderType.STOP_LIMIT

            else:
                raise ValueError(
                    "Order type could not be determined. If you are trying to create an advanced order such \
                                 as a Bracket Order, OCO or OTO, please specify the type parameter when creating the order."
                )

        if self.type == "oco":
            # This is a "One-Cancel-Other" advanced order
            self.order_class = self.OrderType.OCO
            self.type = self.OrderType.OCO
            if stop_loss_price is not None and take_profit_price is not None:
                self.take_profit_price = check_price(take_profit_price, "take_profit_price must be float.")
                self.stop_loss_price = check_price(stop_loss_price, "stop_loss_price must be float.")
                self.stop_loss_limit_price = check_price(
                    stop_loss_limit_price,
                    "stop_loss_limit_price must be float.",
                    nullable=True,
                )

                # Create the child orders
                self.child_orders = [
                    Order(
                        strategy=self.strategy,
                        asset=self.asset,
                        quantity=self.quantity,
                        side=self.side,
                        limit_price=self.take_profit_price,
                        type="limit",
                        position_filled=True,
                    ),
                    Order(
                        strategy=self.strategy,
                        asset=self.asset,
                        quantity=self.quantity,
                        side=self.side,
                        limit_price=self.stop_loss_limit_price,
                        stop_price=self.stop_loss_price,
                        type="stop_limit",
                        position_filled=True,
                    ),
                ]

            elif len(self.child_orders) == 2:
                # Verify that the child orders are valid order objects
                for child_order in self.child_orders:
                    if not isinstance(child_order, Order):
                        raise ValueError("Child orders must be of type Order")

        elif self.type == "bracket":
            # This is a "One-Cancel-Other" advanced order
            self.order_class = "bracket"

            # If limit_price is set then the initial order is a limit order
            if limit_price is not None:
                self.type = "limit"
                self.limit_price = check_price(limit_price, "limit_price must be float.")
            # If stop_price is set then the initial order is a stop order
            elif stop_price is not None:
                self.type = "stop"
                self.stop_price = check_price(stop_price, "stop_price must be float.")
            # If neither limit_price nor stop_price is set then the initial order is a market order
            else:
                self.type = "market"

            self.take_profit_price = check_price(take_profit_price, "take_profit_price must be float.")
            self.stop_loss_price = check_price(stop_loss_price, "stop_loss_price must be float.")
            self.stop_loss_limit_price = check_price(
                stop_loss_limit_price,
                "stop_loss_limit_price must be float.",
                nullable=True,
            )

        elif self.type == "oto":
            # This is a "One-Triggers-One" advanced order
            self.order_class = "oto"

            # If limit_price is set then the initial order is a limit order
            if limit_price is not None:
                self.type = "limit"
                self.limit_price = check_price(limit_price, "limit_price must be float.")
            # If stop_price is set then the initial order is a stop order
            elif stop_price is not None:
                self.type = "stop"
                self.stop_price = check_price(stop_price, "stop_price must be float.")
            # If neither limit_price nor stop_price is set then the initial order is a market order
            else:
                self.type = "market"

            if take_profit_price is not None:
                self.take_profit_price = check_price(
                    take_profit_price,
                    "take_profit_price must be float.",
                )
            else:
                self.stop_loss_price = check_price(stop_loss_price, "stop_loss_price must be float.")
                self.stop_loss_limit_price = check_price(
                    stop_loss_limit_price,
                    "stop_loss_limit_price must be float.",
                    nullable=True,
                )

        # If it's a trailing stop order, then make sure the trailing price is set
        elif self.type == "trailing_stop":
            if trail_price is not None:
                self.trail_price = check_price(trail_price, "trail_price must be positive float.", allow_negative=False)
            else:
                self.trail_percent = check_positive(
                    trail_percent,
                    float,
                    "trail_percent must be positive float.",
                )

        # If it's a stop order, then make sure the stop price is set
        elif self.type == "stop":
            self.stop_price = check_price(stop_price, "stop_price must be float.")

        # If it's a limit order, then make sure the limit price is set
        elif self.type == "limit":
            self.limit_price = check_price(limit_price, "limit_price must be float.")

        # If it's a stop limit order, then make sure the stop and limit prices are set
        elif self.type == "stop_limit":
            self.limit_price = check_price(limit_price, "limit_price must be float.")
            self.stop_price = check_price(stop_price, "stop_price must be float.")

    @property
    def avg_fill_price(self):
        return self._avg_fill_price

    @avg_fill_price.setter
    def avg_fill_price(self, value):
        self._avg_fill_price = round(float(value), 2) if value is not None else None

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
                logging.error(f"Invalid order status: {value}")

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
                repr_str = f"{self.order_class} order |"
            else:
                repr_str = f"{self.type} order |"

            # Add the child orders to the repr
            for child_order in self.child_orders:
                repr_str = f"{repr_str} {child_order.type} {child_order.side} {child_order.quantity} {child_order.asset} |"
        else:
            repr_str = f"{self.type} order of | {self.quantity} {self.rep_asset} {self.side} |"
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
        return not self.is_filled() and not self.is_canceled()

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
        elif status1.lower() in [status2.lower(), STATUS_ALIAS_MAP.get(status2.lower(), "")]:
            return True
        # open/new status is equivalent
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
        )
        return position

    def get_increment(self):
        increment = self.quantity
        if self.side == SELL:
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
