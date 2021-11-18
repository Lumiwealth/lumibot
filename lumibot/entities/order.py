import logging
from collections import namedtuple
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
        sec_type="STK",
        exchange="SMART",
        position_filled=False,
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
            Lumibot before creating the `Order` object. Therefore all
            `Order` objects will only have an `Asset` object.
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
        time_in_force : str (in development)
            Amount of time the order is in force. Default: 'day'
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
            Default = `SMART`

        """
        if isinstance(asset, str):
            asset = entities.Asset(symbol=asset)

        # Initialization default values
        self.strategy = strategy
        self.asset = asset
        self.symbol = self.asset.symbol
        self.identifier = None
        self.status = "unprocessed"
        self.side = None
        self.time_in_force = time_in_force
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
        self.type = "market"
        self.dependent_order = None
        self.dependent_order_filled = False

        # Options:
        self.exchange = exchange
        self.sec_type = sec_type

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

        # setting the quantity
        self.quantity = check_quantity(
            quantity, "Order quantity must be a positive integer"
        )

        # setting the side
        if side not in [self.BUY, self.SELL]:
            raise ValueError("Side must be either sell or buy, got %r instead" % side)
        self.side = side

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
                        take_profit_price, "take_profit_price must be positive float."
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
                        trail_percent, float, "trail_percent must be positive float."
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

    def __repr__(self):
        self.rep_asset = self.symbol
        if self.asset.asset_type == "future":
            self.rep_asset = f"{self.symbol} {self.asset.expiration}"
        elif self.asset.asset_type == "option":
            self.rep_asset = (
                f"{self.symbol} {self.asset.expiration} "
                f"{self.asset.right} {self.asset.strike}"
            )
        repr = "%s order of | %d %s %s |" % (
            self.type,
            self.quantity,
            self.rep_asset,
            self.side,
        )
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
