import logging
from collections import namedtuple
from decimal import Decimal
from threading import Event

import lumibot.entities as entities
from lumibot.tools.types import check_positive, check_price, check_quantity


class Order:
    Transaction = namedtuple("Transaction", ["quantity", "price"])

    SELL = "sell"
    BUY = "buy"

    def __init__(
        self,
        strategy,
        asset,
        quantity,
        side,
        limit_price=None,
        stop_price=None,
        take_profit_price=None,
        stop_loss_price=None,
        stop_loss_limit_price=None,
        trail_price=None,
        trail_percent=None,
        time_in_force="day",
        good_till_date=None,
        sec_type=None,
        exchange=None,
        position_filled=False,
        quote=None,
        pair=None,
        date_created=None,
        type=None,
        trade_cost: float = None,
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
        trade_cost : float
            The cost of this order in the quote currency.
        Examples
        --------
        >>> from lumibot.entities import Asset
        >>> from lumibot.order import Order
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
        if asset == quote:
            logging.error(
                f"When creating an Order, asset and quote must be different. Got asset = {asset} and quote = {quote}"
            )
            return

        if isinstance(asset, str):
            asset = entities.Asset(symbol=asset)

        if sec_type is None:
            sec_type = asset.asset_type

        # Initialization default values
        self.strategy = strategy

        # It is possible for crypto currencies to arrive as a tuple of
        # two assets.
        if isinstance(asset, tuple) and asset[0].asset_type == "crypto":
            self.asset = asset[0]
            self.quote = asset[1]
        else:
            self.asset = asset
            self.quote = quote

        self.symbol = self.asset.symbol
        self.identifier = None
        self.status = "unprocessed"
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
        self.take_profit_price = None
        self.stop_loss_price = None
        self.stop_loss_limit_price = None
        self.transactions = []
        self.order_class = None
        self.dependent_order = None
        self.dependent_order_filled = False
        self.type = type
        self.trade_cost = trade_cost

        # Options:
        self.exchange = exchange
        self.sec_type = sec_type

        # Cryptocurrency market.
        self.pair = (
            f"{self.asset.symbol}/{self.quote.symbol}"
            if self.asset.asset_type == "crypto"
            else pair
        )

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
        self._error_message = None

        self.quantity = quantity

        # setting the side
        if side not in [self.BUY, self.SELL]:
            raise ValueError("Side must be either sell or buy, got %r instead" % side)
        self.side = side

        if self.type is None:
            self.type = "market"
            if position_filled:
                # This is a "One-Cancel-Other" advanced order
                # with the entry order already filled
                self.order_class = "oco"
                self.type = "limit"
                if stop_loss_price is None or take_profit_price is None:
                    raise ValueError(
                        "stop_loss_price and take_profit_loss must be defined for oco class orders"
                    )
                else:
                    self.take_profit_price = check_price(
                        take_profit_price, "take_profit_price must be positive float."
                    )
                    self.stop_loss_price = check_price(
                        stop_loss_price, "stop_loss_price must be positive float."
                    )
                    self.stop_loss_limit_price = check_price(
                        stop_loss_limit_price,
                        "stop_loss_limit_price must be positive float.",
                        nullable=True,
                    )
            else:
                # This is either a simple order, bracket order or One-Triggers-Other order
                if take_profit_price is not None and stop_loss_price is not None:
                    # Both take_profit_price and stop_loss_price are defined
                    # so this is a bracket order
                    self.order_class = "bracket"
                    self.take_profit_price = check_price(
                        take_profit_price, "take_profit_price must be positive float."
                    )
                    self.stop_loss_price = check_price(
                        stop_loss_price, "stop_loss_price must be positive float."
                    )
                    self.stop_loss_limit_price = check_price(
                        stop_loss_limit_price,
                        "stop_loss_limit_price must be positive float.",
                        nullable=True,
                    )
                elif take_profit_price is not None or stop_loss_price is not None:
                    # Only one of take_profit_price and stop_loss_price are defined
                    # so this is a One-Triggers-Other order
                    self.order_class = "oto"
                    if take_profit_price is not None:
                        self.take_profit_price = check_price(
                            take_profit_price,
                            "take_profit_price must be positive float.",
                        )
                    else:
                        self.stop_loss_price = check_price(
                            stop_loss_price, "stop_loss_price must be positive float."
                        )
                        self.stop_loss_limit_price = check_price(
                            stop_loss_limit_price,
                            "stop_loss_limit_price must be positive float.",
                            nullable=True,
                        )
                else:
                    # This is a simple order with no legs
                    self.order_class = ""

                # Set pricing of entry order.
                if trail_price is not None or trail_percent is not None:
                    self.type = "trailing_stop"
                    if trail_price is not None:
                        self.trail_price = check_price(
                            trail_price, "trail_price must be positive float."
                        )
                    else:
                        self.trail_percent = check_positive(
                            trail_percent,
                            float,
                            "trail_percent must be positive float.",
                        )
                elif limit_price is None and stop_price is None:
                    self.type = "market"
                elif limit_price is None and stop_price is not None:
                    self.type = "stop"
                    self.stop_price = check_price(
                        stop_price, "stop_price must be positive float."
                    )
                elif limit_price is not None and stop_price is None:
                    self.type = "limit"
                    self.limit_price = check_price(
                        limit_price, "limit_price must be positive float."
                    )
                elif limit_price is not None and stop_price is not None:
                    self.type = "stop_limit"
                    self.limit_price = check_price(
                        limit_price, "limit_price must be positive float."
                    )
                    self.stop_price = check_price(
                        stop_price, "stop_price must be positive float."
                    )

    @property
    def quantity(self):
        if self.asset.asset_type == "crypto":
            return self._quantity
        return int(self._quantity)

    @quantity.setter
    def quantity(self, value):
        # All non-crypto assets must be of type 'int'.
        error_msg = (
            f"Quantity for {self.asset} which is a "
            f"{self.asset.asset_type}, must be of type 'int'."
            f"The value {value} was entered which is a {type(value)}."
        )

        if not isinstance(value, Decimal):
            if isinstance(value, float):
                value = Decimal(str(value))

        quantity = Decimal(value)
        self._quantity = check_quantity(
            quantity, "Order quantity must be a positive Decimal"
        )

    def __repr__(self):
        self.rep_asset = self.symbol
        if self.asset.asset_type == "crypto":
            self.rep_asset = f"{self.pair}"
        elif self.asset.asset_type == "future":
            self.rep_asset = f"{self.symbol} {self.asset.expiration}"
        elif self.asset.asset_type == "option":
            self.rep_asset = (
                f"{self.symbol} {self.asset.expiration} "
                f"{self.asset.right} {self.asset.strike}"
            )
        repr = f"{self.type} order of | {self.quantity} {self.rep_asset} {self.side} |"
        if self.order_class:
            repr = "%s of class %s" % (repr, self.order_class)
        repr = "%s with status %s" % (repr, self.status)
        return repr

    def set_identifier(self, identifier):
        self.identifier = identifier

    def add_transaction(self, price, quantity):
        transaction = self.Transaction(price=price, quantity=quantity)
        self.transactions.append(transaction)

    def cash_pending(self, strategy):
        # Returns the impact to cash of any unfilled shares.
        quantity_unfilled = self.quantity - sum(
            [transaction.quantity for transaction in self.transactions]
        )
        if quantity_unfilled == 0:
            return 0
        elif len(self.transactions) == 0:
            cash_value = self.quantity * strategy.get_last_price(self.asset)
        else:
            cash_value = quantity_unfilled * self.transactions[-1].price
        if self.side == self.SELL:
            return cash_value
        else:
            return -cash_value

    def update_status(self, status):
        self.status = status

    def set_error(self, error):
        self.status = "error"
        self._error = error
        self._error_message = str(error)
        self._closed_event.set()

    def was_transmitted(self):
        return self._transmitted is True

    def update_raw(self, raw):
        if raw is not None:
            self._transmitted = True
            self._raw = raw

    def to_position(self, quantity):
        position_qty = quantity
        if self.side == self.SELL:
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
        if self.side == self.SELL:
            increment = -increment
        return increment

    def is_option(self):
        # Return true if this order is an option.
        if self.sec_type == "OPT":
            return True
        else:
            return False

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
