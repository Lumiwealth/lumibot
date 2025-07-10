import datetime
import os
import time
import uuid
from asyncio.log import logger
from decimal import Decimal
from typing import Union, List, Type, Callable

import jsonpickle
import matplotlib
import numpy as np
import pandas as pd
import pandas_market_calendars as mcal
from termcolor import colored
from apscheduler.triggers.cron import CronTrigger

from ..entities import Asset, Order, Position, Data, TradingFee, Quote
from ..tools import get_risk_free_rate
from ..traders import Trader
from ..data_sources import DataSource

from ._strategy import _Strategy

matplotlib.use("Agg")

class Strategy(_Strategy):
    @property
    def name(self):
        """Returns the name of the strategy.

        Returns:
            str: The name of the strategy.

        Example
        -------
        >>> self.log_message(f'Strategy name: {self.name}')
        """
        return self._name

    @property
    def initial_budget(self):
        """Returns the initial budget for the strategy.

        Returns:
            float: The initial budget for the strategy.

        Example
        -------
        >>> self.log_message(f'Strategy initial budget: {self.initial_budget}')
        """
        return self._initial_budget

    @property
    def quote_asset(self):
        """Returns the quote asset for the strategy. The quote asset is what is considered
        "cash" (as in `self.cash`), and it is the currency that `self.portfolio_value` uses.

        Returns:
            Asset: The quote asset for the strategy

        Example
        -------
        >>> self.log_message(f"The quote asset for this strategy is {self.quote_asset}")
        """
        return self._quote_asset

    @quote_asset.setter
    def quote_asset(self, value):
        self._quote_asset = value
        self.broker.quote_assets.add(value)

    @property
    def last_on_trading_iteration_datetime(self):
        """Returns the datetime of the last iteration.

        Returns:
            datetime: The datetime of the last iteration.

        Example
        -------
        >>> self.log_message(f'The last trading iteration happened at: {self.last_on_trading_iteration_datetime}')
        """
        return self._last_on_trading_iteration_datetime

    @property
    def minutes_before_opening(self):
        # noinspection PyShadowingNames
        """
        Get or set the number of minutes that the strategy will start executing before the market opens.
        The lifecycle method before_market_opens is executed minutes_before_opening minutes before the market opens.
        By default, equals to 60 minutes.

        Returns
        -------
        int
            The number of minutes before the market opens.

        Example
        -------
        >>> # Set the number of minutes before the market opens
        >>> self.minutes_before_opening = 10

        >>> # Set the number of minutes before the market opens to 0 in the initialize method
        >>> def initialize(self):
        >>>     self.minutes_before_opening = 0

        """
        return self._minutes_before_opening

    @minutes_before_opening.setter
    def minutes_before_opening(self, value):
        self._minutes_before_opening = value

    @property
    def minutes_before_closing(self):
        """Get or set the number of minutes that the strategy will stop executing before market closes.

        The lifecycle method on_trading_iteration is executed inside a loop that stops only when there is only minutes_before_closing minutes remaining before market closes. By default equals to 5 minutes.

        Returns
        -------
        minutes_before_closing : int
            The number of minutes before market closes that the strategy will stop executing.

        Example
        -------
        >>> # Set the minutes before closing to 5
        >>> self.minutes_before_closing = 5

        >>> # Get the minutes before closing
        >>> self.log_message(self.minutes_before_closing)

        >>> # Set the minutes before closing to 10 in the initialize method
        >>> def initialize(self):
        >>>     self.minutes_before_closing = 10
        """
        return self._minutes_before_closing

    @minutes_before_closing.setter
    def minutes_before_closing(self, value):
        self._minutes_before_closing = value

    @property
    def minutes_after_closing(self):
        """Get or set the number of minutes that the strategy will continue executing after market closes.

        The lifecycle method after_market_closes is executed minutes_after_closing minutes after the market closes. By default, equals to 0 minutes.

        Returns
        -------
        int
            The number of minutes after the market closes that the strategy will continue executing.

        Example
        -------
        >>> # Set the number of minutes after the market closes
        >>> self.minutes_after_closing = 10

        >>> # Set the number of minutes after the market closes to 0 in the initialize method
        >>> def initialize(self):
        >>>     self.minutes_after_closing = 0

        """
        return self._minutes_after_closing

    @minutes_after_closing.setter
    def minutes_after_closing(self, value):
        self._minutes_after_closing = value

    @property
    def sleeptime(self):
        """Get or set the current sleep time for the strategy.

        Sleep time is the time the program will pause between executions of on_trading_iteration and trace_stats.
        This is used to control the speed of the program.

        By default, equals 1 minute. You can set the sleep time as an integer which will be interpreted as
        minutes. eg: sleeptime = 50 would be 50 minutes. Conversely, you can enter the time as a string with
        the duration numbers first, followed by the time units: 'M' for minutes, 'S' for seconds
        eg: '300S' is 300 seconds, '10M' is 10 minutes.

        Returns
        -------
        sleeptime : int or str
            Sleep time in minutes or a string with the duration numbers first, followed by the time
            units: `S` for seconds, `M` for minutes, `H` for hours' or `D` for days.

        Example
        -------
        >>> # This is usually used in the initialize method

        >>> # Set the sleep time to 5 minutes in the initialize method
        >>> def initialize(self): # Your initialize lifecycle method
        >>>     self.sleeptime = '5M'

        >>> # Set the sleep time to 10 seconds in the initialize method
        >>> def initialize(self): # Your initialize lifecycle method
        >>>     self.sleeptime = '10S'

        >>> # Set the sleeptime to 10 minutes
        >>> self.sleeptime = 10

        >>> # Set the sleeptime to 300 seconds
        >>> self.sleeptime = "300S"

        >>> # Set the sleep time to 5 minutes
        >>> self.sleeptime = 5

        >>> # Set the sleep time to 5 seconds
        >>> self.sleeptime = "5S"

        >>> # Set the sleep time to 2 hours
        >>> self.sleeptime = "2H"

        >>> # Set the sleep time to 2 days
        >>> self.sleeptime = "2D"
        """
        return self._sleeptime

    @sleeptime.setter
    def sleeptime(self, value):
        self._sleeptime = value

    @property
    def backtesting_start(self):
        return self._backtesting_start

    @property
    def backtesting_end(self):
        return self._backtesting_end

    @property
    def unspent_money(self):
        """Deprecated, will be removed in the future. Please use `self.cash` instead."""
        return self.cash

    @property
    def portfolio_value(self):
        """Returns the current portfolio value (cash + positions value).

        Returns the portfolio value of positions plus cash in US dollars.

        Crypto markets will attempt to resove to US dollars as a quote
        currency.

        Returns
        -------
        portfolio_value : float
            The current portfolio value. Includes the actual values of shares held by the current strategy plus the total cash.

        Example
        -------
        >>> # Get the current portfolio value
        >>> self.log_message(self.portfolio_value)

        """

        self.update_broker_balances(force_update=False)

        return self._portfolio_value

    @property
    def cash(self):
        """Returns the current cash. This is the money that is not used for positions or
        orders (in other words, the money that is available to buy new assets, or cash).

        This property is updated whenever a transaction was filled by the broker or when dividends
        are paid.

        Crypto currencies are a form of cash. Therefore cash will always be zero.

        Returns
        -------
        cash : float
            The current cash.

        Example
        -------
        >>> # Get the current cash available in the account
        >>> self.log_message(self.cash)
        """

        self.update_broker_balances(force_update=False)

        cash_position = self.get_position(self.quote_asset)
        quantity = cash_position.quantity if cash_position else None

        # This is not really true:
        # if quantity is None:
        #     self._set_cash_position(0)
        #     quantity = 0

        if type(quantity) is Decimal:
            quantity = float(quantity)
        elif quantity is None:
            quantity = 0.0

        return quantity

    @property
    def first_iteration(self):
        """Returns True if this is the first iteration of the strategy (is True if the lifecycle
        method on_trading_iteration is being excuted for the first time).

        Returns
        -------
        first_iteration : bool
            True if this is the first iteration of the strategy.

        Example
        -------
        >>> # Check if this is the first iteration
        >>> if self.first_iteration:
        >>>     self.log_message("This is the first iteration")
        """
        return self._first_iteration

    @property
    def stats_file(self):
        return self._stats_file

    @property
    def stats(self):
        return self._stats

    @property
    def analysis(self):
        return self._analysis

    @property
    def risk_free_rate(self) -> float:
        if self._risk_free_rate is not None:
            return self._risk_free_rate
        else:
            # Use the yahoo data to get the risk free rate, or 0 if None is returned
            now = self.get_datetime()
            return get_risk_free_rate(now) or 0.0

    # ======= Helper Methods =======================

    def log_message(self, message: str, color: str = None, broadcast: bool = False):
        """Logs an info message prefixed with the strategy name.

        Uses python logging to log the message at the `info` level.
        Logging goes to the logging file, not the console.

        Parameters
        ----------
        message : str
            String message for logging.

        color : str
            Color of the message. Eg. `"red"` or `"green"`.

        broadcast : bool
            If True, the message will be broadcasted to any connected message services.

        Returns
        -------
        message : str
            Strategy name plus the original message.

        Example
        --------
        >>> self.log_message('Sending a buy order')
        """

        if broadcast:
            # Send the message to Discord
            self.send_discord_message(message)

        # If we are backtesting and we don't want to save the logfile, don't log (they're not displayed in the console anyway)
        if not self.save_logfile and self.is_backtesting:
            return

        if color:
            colored_message = colored(message, color)
            self.logger.info(colored_message)
        else:
            self.logger.info(message)

        return message

    # ====== Order Methods ===============

    def create_order(
        self,
        asset: Union[str, Asset],
        quantity: Union[int, str, Decimal],
        side: str,
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
        time_in_force: str = "gtc",
        good_till_date: datetime.datetime = None,
        take_profit_price: float = None,  # Deprecated, use 'secondary_limit_price' instead
        stop_loss_price: float = None,  # Deprecated, use 'secondary_stop_price' instead
        stop_loss_limit_price: float = None,  # Deprecated, use 'secondary_stop_limit_price' instead
        position_filled: float = None,
        exchange: str = None,
        quote: Asset = None,
        pair: str = None,
        order_type: Union[Order.OrderType, None] = None,
        order_class: Union[Order.OrderClass, None] = None,
        type: Union[Order.OrderType, None] = None,  # Deprecated, use 'order_type' instead
        custom_params: dict = None,
    ):
        # noinspection PyShadowingNames,PyUnresolvedReferences
        """Creates a new order for this specific strategy. Once created, an order must still be submitted.

        Some notes on Crypto markets:

        Crypto markets require both a base currency and a quote currency to create an order. For example, use the quote parameter.:

            >>> from lumibot.entities import Asset
            >>>
            >>> self.create_order(
            >>>     Asset(symbol='BTC', asset_type=Asset.AssetType.CRYPTO),
            >>>     .50,
            >>>     'buy',
            >>>     quote=Asset(symbol='USDT', asset_type='crypto'),
            >>> )

        Orders for crypto markets are restriced to: ``market``, ``limit``, ``stop_limit``.

        Crypto markets' orders are simple. There are no compound orders such
        ``oco`` or ``bracket``. Also, duration of orders are all GTC.

        Parameters
        ----------
        asset : str or Asset
            The asset that will be traded. If this is just a stock, then
            ``str`` is sufficient. However, all assets other than stocks
            must use ``Asset``.
        quantity : int string Decimal (float will deprecate)
            The number of shares or units to trade. One may enter an
            int, a string number eg: "3.213", or a Decimal obect,
            eg: Decimal("3.213"). Internally all will convert to Decimal.
        side : str
            Whether the order is ``buy`` or ``sell``.
        order_type : Order.OrderType
            The type of order. Order types include: ``'market'``, ``'limit'``, ``'stop'``, ``'stop_limit'``,
            ``trailing_stop``
            We will try to determine the order type if you do not specify it.
        order_class: Order.OrderClass
            The class of the order. Order classes include: ``'simple'``, ``'bracket'``, ``'oco'``, ``'oto'``,
            ``'multileg'``
        limit_price : float
            A Limit order is an order to buy or sell at a specified
            price or better. The Limit order ensures that if the
            order fills, it will not fill at a price less favorable
            than your limit price, but it does not guarantee a fill.
        stop_price : float
            A Stop order is an instruction to submit a buy or sell
            market order if and when the user-specified stop trigger
            price is attained or penetrated.
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
        stop_loss_limit_price : float
            Stop loss with limit price used for bracket orders and one
            cancels other orders. (Depricated, use 'stop_limit_price` instead)
        time_in_force : str
            Amount of time the order is in force. Order types include:
                - ``'day'`` Orders valid for the remainder of the day.
                - ``'gtc'`` Good until cancelled.
                - ``'gtd'`` Good until date.
            (Default: 'day')
        good_till_date : datetime.datetime
            This is the time order is valid for Good Though Date orders.
        take_profit_price : float
            Limit price used for bracket orders and one cancels other
            orders. (Depricated, use 'secondary_limit_price` instead)
        stop_loss_price : float
            Stop price used for bracket orders and one cancels other
            orders. (Depricated, use 'secondary_stop_price` instead)
        trail_price : float
            Trailing stop orders allow you to continuously and
            automatically keep updating the stop price threshold based
            on the stock price movement. `trail_price` sets the
            trailing price in dollars.
        trail_percent : float
            Trailing stop orders allow you to continuously and
            automatically keep updating the stop price threshold based
            on the stock price movement. E.g. 0.05 would be a 5% trailing stop.
            `trail_percent` sets the trailing price in percent.
        secondary_trail_price : float
            Trailing stop price for child orders of Advanced Orders like Bracket or OTO orders.
        secondary_trail_percent : float
            Trailing stop percent for child orders of Advanced Orders like Bracket or OTO orders.
        exchange : str
            The exchange where the order will be placed.
            ``Default = 'SMART'``
        quote : Asset
            This is the currency that the main coin being bought or sold
            will exchange in. For example, if trading ``BTC/ETH`` this
            parameter will be 'ETH' (as an Asset object).
        custom_params : dict
            A dictionary of custom parameters that can be used to pass additional information to the broker. This is useful for passing custom parameters to the broker that are not supported by Lumibot.
            E.g. `custom_params={"leverage": 3}` for Kraken margin trading.
        type : Order.OrderType
            Deprecated, use 'order_type' instead

        Further Reading
        -------
        - Although the term "stop_limit" can be confusing, it is important to remember that this is a StopLoss modifier
            not a true limit price nor bracket-style order. For more information, visit:
            https://www.investopedia.com/terms/s/stop-limitorder.asp

        Returns
        -------
        Order
            Order object ready to be submitted for trading.

        Examples
        -------
        >>> # For a market buy order
        >>> order = self.create_order("SPY", 100, "buy")
        >>> self.submit_order(order)

        >>> # For a limit order where limit price = 100
        >>> limit_order = self.create_order("SPY", 1, "buy", limit_price=100)
        >>> self.submit_order(limit_order)

        >>> # Sell 100 shares of TLT
        >>> order = self.create_order("TLT", 100, "sell")
        >>> self.submit_order(order)

        >>> # For a stop loss order
        >>> order = self.create_order("SPY", 100, "buy", stop_price=100.00)
        >>> self.submit_order(order)

        >>> # For a stop limit order
        >>> order = self.create_order("SPY", 100, "buy", stop_price=100.00, stop_limit_price=99.95)
        >>> self.submit_order(order)

        >>> # For a market sell order
        >>> order = self.create_order("SPY", 100, "sell")
        >>> self.submit_order(order)

        >>> # For a limit sell order
        >>> order = self.create_order("SPY", 100, "sell", limit_price=100.00)
        >>> self.submit_order(order)

        >>> # For an order with a trailing stop
        >>> order = self.create_order("SPY", 100, "buy", trail_price=100.00)
        >>> self.submit_order(order)

        >>> # For an OCO order - No entry order specified, only exit order info.
        >>> order = self.create_order(
        >>>                "SPY",
        >>>                100,
        >>>                "sell",
        >>>                limit_price=limit,  # Exit Profit point
        >>>                stop_price=stop_loss,  # Exit Loss point
        >>>                stop_limit_price=stop_loss_limit,  # Stop loss modifier (optional)
        >>>                order_class=Order.OrderClass.OCO,
        >>>            )

        >>> # For a bracket order - One Entry order with a Profit and Loss exit orders (2 child orders).
        >>> order = self.create_order(
        >>>                "SPY",
        >>>                100,
        >>>                "buy",
        >>>                limit_price=limit,  # When the Entry order will execute
        >>>                secondary_limit_price=sec_limit,  # When the child Profit Exit order will execute
        >>>                secondary_stop_price=stop_loss,  # When the child Loss Exit order will execute
        >>>                secondary_stop_limit_price=stop_loss_limit,  # Child loss modifier (optional)
        >>>                order_class=Order.OrderClass.BRACKET,
        >>>            )

        >>> # For a bracket order with a trailing stop
        >>> order = self.create_order(
        >>>                "SPY",
        >>>                100,
        >>>                "buy",
        >>>                limit_price=limit,  # When to Enter
        >>>                secondary_limit_price=sec_limit,  # When to Exit Profit
        >>>                secondary_stop_price=stop_loss,  # When to Exit Loss
        >>>                secondary_trail_percent=trail_percent,  # Exit stop modifier (optional)
        >>>                order_class=Order.OrderClass.BRACKET,
        >>>            )

        >>> # For an OTO order - One Entry, only a single Exit criteria is allowed (1/2 of a bracket order)
        >>> order = self.create_order(
        >>>                "SPY",
        >>>                100,
        >>>                "buy",
        >>>                limit_price=limit,  # When to Enter
        >>>                secondary_stop_price=stop_loss,  # When to Exit
        >>>                order_class=Order.OrderClass.OTO,
        >>>            )

        >>> # For a futures order
        >>> from lumibot.entities import Asset
        >>>
        >>> asset = Asset("ES", asset_type=Asset.AssetType.FUTURE, expiration="2019-01-01")
        >>> order = self.create_order(asset, 100, "buy", limit_price=100.00)
        >>> self.submit_order(order)

        >>> # For a futures order with a trailing stop
        >>> from lumibot.entities import Asset
        >>>
        >>> asset = Asset("ES", asset_type=Asset.AssetType.FUTURE, expiration="2019-01-01")
        >>> order = self.create_order(
        >>>                asset,
        >>>                100,
        >>>                "buy",
        >>>                limit_price=limit,  # When to Enter
        >>>                secondary_stop_price=stop_loss,  # When to Exit
        >>>                secondary_trail_percent=trail_percent,  # Exit modifier (optional)
        >>>                order_class=Order.OrderClass.OTO,
        >>>            )
        >>> self.submit_order(order)

        >>> # For an option order
        >>> from lumibot.entities import Asset
        >>>
        >>> asset = Asset("SPY", asset_type=Asset.AssetType.OPTION, expiration="2019-01-01", strike=100.00)
        >>> order = self.create_order(asset, 100, "buy", limit_price=100.00)
        >>> self.submit_order(order)

        >>> # For an option order with a trailing stop
        >>> from lumibot.entities import Asset
        >>>
        >>> asset = Asset("SPY", asset_type=Asset.AssetType.OPTION, expiration="2019-01-01", strike=100.00)
        >>> order = self.create_order(
        >>>                asset,
        >>>                100,
        >>>                "buy",
        >>>                limit_price=limit,
        >>>                secondary_stop_price=stop_loss,
        >>>                secondary_trail_percent=trail_percent,
        >>>                order_class=Order.OrderClass.BRACKET,
        >>>            )
        >>> self.submit_order(order)

        >>> # For a FOREX order
        >>> from lumibot.entities import Asset
        >>>
        >>> asset = Asset(
        >>>    symbol="CHF",
        >>>    currency="EUR",
        >>>    asset_type=Asset.AssetType.FOREX,
        >>>  )
        >>> order = self.create_order(asset, 100, "buy", limit_price=100.00)
        >>> self.submit_order(order)

        >>> # For a options order with a limit price
        >>> from lumibot.entities import Asset
        >>>
        >>> asset = Asset("SPY", asset_type=Asset.AssetType.OPTION, expiration="2019-01-01", strike=100.00)
        >>> order = self.create_order(asset, 100, "buy", limit_price=100.00)
        >>> self.submit_order(order)

        >>> # For a options order with a trailing stop
        >>> from lumibot.entities import Asset
        >>>
        >>> asset = Asset("SPY", asset_type=Asset.AssetType.OPTION, expiration="2019-01-01", strike=100.00)
        >>> order = self.create_order(
        >>>                asset,
        >>>                100,
        >>>                "buy",
        >>>                limit_price=limit,  # When to Enter
        >>>                secondary_stop_price=stop_loss,  # When to Exit
        >>>                secondary_trail_percent=trail_percent,  # Exit modifier (optional)
        >>>                order_class=Order.OrderClass.OTO,
        >>>            )
        >>> self.submit_order(order)

        >>> # For a cryptocurrency order with a market price
        >>> from lumibot.entities import Asset
        >>>
        >>> base = Asset("BTC", asset_type=Asset.AssetType.CRYPTO)
        >>> quote = Asset("USD", asset_type=Asset.AssetType.CRYPTO)
        >>> order = self.create_order(base, 0.05, "buy", quote=quote)
        >>> self.submit_order(order)

        >>> # Placing a limit order with a quote asset for cryptocurrencies
        >>> from lumibot.entities import Asset
        >>>
        >>> base = Asset("BTC", asset_type=Aset.AssetType.CRYPTO)
        >>> quote = Asset("USD", asset_type=Asset.AssetType.CRYPTO)
        >>> order = self.create_order(base, 0.05, "buy", limit_price=41000,  quote=quote)
        >>> self.submit_order(order)
        """

        if quote is None:
            quote = self.quote_asset

        asset = self._sanitize_user_asset(asset)
        order = Order(
            self.name,
            asset,
            quantity,
            side,
            limit_price=limit_price,
            stop_price=stop_price,
            stop_limit_price=stop_limit_price,
            secondary_limit_price=secondary_limit_price,
            secondary_stop_price=secondary_stop_price,
            secondary_stop_limit_price=secondary_stop_limit_price,
            time_in_force=time_in_force,
            good_till_date=good_till_date,
            take_profit_price=take_profit_price,  # Depricated, use 'secondary_limit_price' instead
            stop_loss_price=stop_loss_price,  # Depricated, use 'secondary_stop_price' instead
            stop_loss_limit_price=stop_loss_limit_price,  # Depricated, use 'secondary_stop_limit_price' instead
            trail_price=trail_price,
            trail_percent=trail_percent,
            secondary_trail_price=secondary_trail_price,
            secondary_trail_percent=secondary_trail_percent,
            exchange=exchange,
            position_filled=position_filled,
            date_created=self.get_datetime(),
            quote=quote,
            pair=pair,
            type=type,
            order_type=order_type,
            order_class=order_class,
            custom_params=custom_params,
        )

        # Add debug logging for custom_params
        if custom_params:
            self.log_message(f"🔧 ORDER CREATED with custom_params: {custom_params} for {asset} {side} {quantity}")

        return order

    # ======= Broker Methods ============

    def sleep(self, sleeptime: float, process_pending_orders: bool = True):
        """Sleep for sleeptime seconds.

        Use to pause the execution of the program. This should be used instead of `time.sleep` within the strategy. Also processes pending orders in the meantime.

        Parameters
        ----------
        sleeptime : float
            Time in seconds the program will be paused.
        process_pending_orders : bool
            If True, the broker will process any pending orders.

        Returns
        -------
        None

        Example
        -------
        >>> # Sleep for 5 seconds
        >>> self.sleep(5)
        """

        if not self.is_backtesting:
            # Sleep for the sleeptime in seconds.
            time.sleep(sleeptime)

        return self.broker.sleep(sleeptime)

    def get_selling_order(self, position: Position):
        """Get the selling order for a position.

        Parameters
        -----------
        position : Position
            The position to get the selling order for.

        Returns
        -------
        Order or None

        Example
        -------
        >>> # Get the selling order for a position
        >>> position = self.get_position("SPY")
        >>> order = self.get_selling_order(position)
        >>> self.submit_order(order)

        >>> # Sell all positions owned by the account
        >>> for position in self.get_positions():
        >>>    order = self.get_selling_order(position)
        >>>    self.submit_order(order)
        """
        if position.asset != self.quote_asset:
            selling_order = self.create_order(position.asset, position.quantity, "sell", quote=self.quote_asset)
            return selling_order
        else:
            return None

    def set_market(self, market: str):
        """Set the market for trading hours.

        Setting the market will determine the trading hours for live
        trading and for Yahoo backtesting. Not applicable to Pandas
        backtesting.

        Crypto markets are always 24/7.
        `NASDAQ` is default.

        Parameters
        ----------
        market : str

            Short form for the markets.
            List of markets available are:

            "MarketCalendar", "ASX", "BMF", "CFE", "NYSE", "stock",
            "NASDAQ", "BATS", "CME_Equity", "CBOT_Equity",
            "CME_Agriculture", "CBOT_Agriculture", "COMEX_Agriculture",
            "NYMEX_Agriculture", "CME_Rate", "CBOT_Rate",
            "CME_InterestRate", "CBOT_InterestRate", "CME_Bond",
            "CBOT_Bond", "EUREX", "HKEX", "ICE", "ICEUS", "NYFE", "JPX",
            "LSE", "OSE", "SIX", "SSE", "TSX", "TSXV", "BSE", "TASE",
            "TradingCalendar", "ASEX", "BVMF", "CMES", "IEPA", "XAMS",
            "XASX", "XBKK", "XBOG", "XBOM", "XBRU", "XBUD", "XBUE",
            "XCBF", "XCSE", "XDUB", "XFRA", "XETR", "XHEL", "XHKG",
            "XICE", "XIDX", "XIST", "XJSE", "XKAR", "XKLS", "XKRX",
            "XLIM", "XLIS", "XLON", "XMAD", "XMEX", "XMIL", "XMOS",
            "XNYS", "XNZE", "XOSL", "XPAR", "XPHS", "XPRA", "XSES",
            "XSGO", "XSHG", "XSTO", "XSWX", "XTAE", "XTAI", "XTKS",
            "XTSE", "XWAR", "XWBO", "us_futures", "24/7", "24/5",

            (default: `NASDAQ`)

            The market to set.

        Returns
        -------
        None


        Example
        -------
        >>> # Set the market to 24/7
        >>> def initialize(self):
        >>>    # Set the market to 24/7
        >>>    self.set_market('24/7')

        >>> # Set the market to NASDAQ
        >>> self.set_market('NASDAQ')

        >>> # Set the market to NYSE
        >>> self.set_market('NYSE')

        >>> # Set the market to 24/5
        >>> self.set_market('24/5')

        >>> # Set the market to us_futures
        >>> self.set_market('us_futures')

        >>> # Set the market to stock
        >>> self.set_market('stock')

        >>> # Set the market to BATS
        >>> self.set_market('BATS')

        >>> # Set the market to CME_Equity
        >>> self.set_market('CME_Equity')
        """
        markets = [
            "MarketCalendar",
            "ASX",
            "BMF",
            "CFE",
            "NYSE",
            "stock",
            "NASDAQ",
            "BATS",
            "CME_Equity",
            "CBOT_Equity",
            "CME_Agriculture",
            "CBOT_Agriculture",
            "COMEX_Agriculture",
            "NYMEX_Agriculture",
            "CME_Rate",
            "CBOT_Rate",
            "CME_InterestRate",
            "CBOT_InterestRate",
            "CME_Bond",
            "CBOT_Bond",
            "EUREX",
            "HKEX",
            "ICE",
            "ICEUS",
            "NYFE",
            "JPX",
            "LSE",
            "OSE",
            "SIX",
            "SSE",
            "TSX",
            "TSXV",
            "BSE",
            "TASE",
            "TradingCalendar",
            "ASEX",
            "BVMF",
            "CMES",
            "IEPA",
            "XAMS",
            "XASX",
            "XBKK",
            "XBOG",
            "XBOM",
            "XBRU",
            "XBUD",
            "XBUE",
            "XCBF",
            "XCSE",
            "XDUB",
            "XFRA",
            "XETR",
            "XHEL",
            "XHKG",
            "XICE",
            "XIDX",
            "XIST",
            "XJSE",
            "XKAR",
            "XKLS",
            "XKRX",
            "XLIM",
            "XLIS",
            "XLON",
            "XMAD",
            "XMEX",
            "XMIL",
            "XMOS",
            "XNYS",
            "XNZE",
            "XOSL",
            "XPAR",
            "XPHS",
            "XPRA",
            "XSES",
            "XSGO",
            "XSHG",
            "XSTO",
            "XSWX",
            "XTAE",
            "XTAI",
            "XTKS",
            "XTSE",
            "XWAR",
            "XWBO",
            "us_futures",
            "24/7",
            "24/5",
        ]

        if market not in markets:
            raise ValueError(f"Valid market entries are: {markets}. You entered {market}. Please adjust.")

        # Check if broker is None before setting market
        if self.broker is None:
            from termcolor import colored
            error_msg = colored(
                "No broker is set. Cannot set market. Please set a broker using environment variables, "
                "secrets or by passing it as an argument to the strategy constructor.", 
                "red"
            )
            self.logger.error(error_msg)
            raise ValueError(
                "No broker is set. Cannot set market. Please ensure your broker credentials are properly "
                "configured in environment variables or passed to the strategy constructor."
            )

        self.broker.market = market

    def await_market_to_open(self, timedelta: int = None):
        """Executes infinite loop until market opens

        If the market is closed, pauses code execution until
        self.minutes_before_opening minutes before market opens again.
        If an input (float) is passed as parameter, pauses code
        execution until input minutes before market opens again.

        Parameters
        ---------
        timedelta : int
            Time in minutes before market will open to pause to.
            Overrides the `self.minutes_before_opening`.

        Returns
        -------
        None

        Example
        -------
        >>> # Await market to open (on_trading_iteration will stop running until the market opens)
        >>> self.await_market_to_open()

        """
        if self.broker.market == "24/7":
            return None
        if timedelta is None:
            timedelta = self.minutes_before_opening
        return self.broker._await_market_to_open(timedelta, strategy=self)

    def await_market_to_close(self, timedelta: int = None):
        """Sleep until market closes.

        If the market is open, pauses code execution until market is
        closed. If an input (float) is passed as parameter, pauses code
        execution starting input minutes before market closes.

        Parameters
        ---------
        timedelta : int
           Time in minutes before market closes to pause.
           Overrides the `self.minutes_before_closing`.

        Returns
        -------
        None

        Example
        -------
        >>> # Sleep until market closes (on_trading_iteration will stop running until the market closes)
        >>> self.await_market_to_close()
        """
        if hasattr(self.broker, "market") and self.broker.market == "24/7":
            return None
        if timedelta is None:
            timedelta = self.minutes_before_closing
        return self.broker._await_market_to_close(timedelta, strategy=self)

    @staticmethod
    def crypto_assets_to_tuple(base, quote: Asset):
        """Check for crypto quote, convert to tuple"""
        if isinstance(base, Asset) and base.asset_type == "crypto" and isinstance(quote, Asset):
            return (base, quote)
        return base

    def get_tracked_position(self, asset: Union[str, Asset]):
        """Deprecated, will be removed in the future. Please use `get_position()` instead."""

        self.log_message("Warning: get_tracked_position() is deprecated, please use get_position() instead.")
        self.get_position(asset)

    def get_position(self, asset: Union[str, Asset]):
        """Get a tracked position given an asset for the current
        strategy.

        Seeks out and returns the position object for the given asset
        in the current strategy.

        Parameters
        ----------
        asset : Asset or str
            Asset object who's traded positions is sought.

        Returns
        -------
        Position or None
            A position object for the assset if there is a tracked
            position or returns None to indicate no tracked position.

        Example
        -------
        >>> # Get the position for the TLT asset
        >>> position = self.get_position("TLT")
        >>> # Show the quantity of the TLT position
        >>> self.log_message(position.quantity)

        """

        # Check if asset is an Asset object or a string
        if not (isinstance(asset, Asset) or isinstance(asset, str)):
            logger.error(f"Asset in get_position() must be an Asset object or a string. You entered {asset}.")
            return None

        asset = self._sanitize_user_asset(asset)
        return self.broker.get_tracked_position(self.name, asset)

    def get_tracked_positions(self):
        """Deprecated, will be removed in the future. Please use `get_positions()` instead."""

        self.log_message("Warning: get_tracked_positions() is deprecated, please use get_positions() instead.")
        return self.get_positions()

    def get_portfolio_value(self):
        """Get the current portfolio value (cash + net equity).

        Parameters
        ----------
        None

        Returns
        -------
        float
            The current portfolio value, which is the sum of the cash and net equity. This is the total value of your account, which is the amount of money you would have if you sold all your assets and closed all your positions. For crypto assets, this is the total value of your account in the quote asset (eg. USDT if that is your quote asset).
        """
        return self._portfolio_value

    def get_cash(self):
        """Get the current cash value in your account.

        Parameters
        ----------
        None

        Returns
        -------
        float
            The current cash value. This is the amount of cash you have in your account, which is the amount of money you can use to buy assets. For crypto assets, this is the amount of the quote asset you have in your account (eg. USDT if that is your quote asset).
        """
        return self.cash

    def get_positions(self, include_cash_positions: bool = False):
        """Get all positions for the account.

        Parameters
        ----------
        include_cash_positions : bool
            If True, include cash positions in the returned list. If False, exclude cash positions.

        Returns
        -------
        list
            A list of Position objects for the strategy if there are tracked
            positions or returns and empty list to indicate no tracked
            position.

        Example
        -------
        >>> # Get all tracked positions
        >>> positions = self.get_positions()
        >>> for position in positions:
        >>>     # Show the quantity of each position
        >>>     self.log_message(position.quantity)
        >>>     # Show the asset of each position
        >>>     self.log_message(position.asset)

        """
        include_cash = include_cash_positions or self.include_cash_positions
        tracked_positions = self.broker.get_tracked_positions(self.name)

        # Remove the quote asset from the positions list if it is there
        clean_positions = []
        for position in tracked_positions:
            if position.asset != self.quote_asset or include_cash:
                clean_positions.append(position)

        return clean_positions

    def get_historical_bot_stats(self):
        """Get the historical account value.

        Returns
        -------
        pandas.DataFrame
            The historical bot stats.

        Example
        -------
        >>> # Get the historical bot stats
        >>> bot_stats = self.get_historical_bot_stats()
        >>> # Show the historical bot stats
        >>> self.log_message(account_value)
        """
        return self.stats.set_index("datetime")

    @property
    def positions(self):
        return self.get_positions()

    def _get_contract_details(self, asset: Asset):
        """Convert an asset into a IB Contract.

        Used internally to create an IB Contract from an asset. Used
        only with Interactive Brokers.

        Parameters
        ----------
        asset : Asset
            Asset to be converted into and Interactive Brokers contract.

        Returns
        -------
        list of ContractDetails
            ContractDetails is a complete contract definition with
            Interactive Brokers.
        """

        asset = self._sanitize_user_asset(asset)
        return self.broker.get_contract_details(asset)

    def get_tracked_order(self, identifier: str):
        """Deprecated, will be removed in the future. Please use `get_order()` instead."""

        self.log_message("Warning: get_tracked_order() is deprecated, please use get_order() instead.")
        return self.get_order(identifier)

    def get_order(self, identifier: str):
        """Get a tracked order given an identifier. Check the details of the order including status, etc.

        Returns
        -------
        Order or None
            An order objects for the identifier

        Example
        -------
        >>> # Get the order object for the order id
        >>> order = self.get_order(order_id)
        >>> # Show the status of the order
        >>> self.log_message(order.status)
        """
        order = self.broker.get_tracked_order(identifier)
        if order is not None and order.strategy == self.name:
            return order
        return None

    def get_tracked_orders(self):
        """Deprecated, will be removed in the future. Please use `get_orders()` instead."""

        self.log_message("Warning: get_tracked_orders() is deprecated, please use get_orders() instead.")
        return self.get_orders()

    def get_orders(self):
        """Get all the current open orders.

        Returns
        -------
        list of Order objects
            Order objects for the strategy if there are tracked

        Example
        -------
        >>> # Get all tracked orders
        >>> orders = self.get_orders()
        >>> for order in orders:
        >>>     # Show the status of each order
        >>>     self.log_message(order.status)

        >>> # Get all open orders
        >>> orders = self.get_tracked_orders()
        >>> for order in orders:
        >>>     # Show the status of each order
        >>>     self.log_message(order.status)
        >>>     # Check if the order is open
        >>>     if order.status == "open":
        >>>         # Cancel the order
        >>>         self.cancel_order(order)

        """
        return self.broker.get_tracked_orders(self.name)

    def get_tracked_assets(self):
        """Get the list of assets for positions
        and open orders for the current strategy

        Returns
        -------
        list
            A list of assets for the strategy if there are tracked positions or returns and empty list to indicate no tracked.

        Example
        -------
        >>> # Get all tracked assets
        >>> assets = self.get_tracked_assets()
        >>> for asset in assets:
        >>>     # Show the asset name
        >>>     self.log_message(asset.symbol)
        >>>     # Show the quantity of the asset
        >>>     self.log_message(asset.quantity)


        """
        return self.broker.get_tracked_assets(self.name)

    def get_asset_potential_total(self, asset: Asset):
        """Get the potential total for the asset (orders + positions).

        Parameters
        ----------
        asset : Asset
            Asset object who's potential total is sought.

        Returns
        -------
        int, float or Decimal
            The potential total for the asset. Decimals are automatically
            returned as floats if less than 4 decimal points

        Example
        -------
        >>> # Get the potential total for the TLT asset
        >>> total = self.get_asset_potential_total("TLT")
        >>> self.log_message(total)

        >>> # Show the potential total for an asset
        >>> asset = Asset("TLT")
        >>> total = self.get_asset_potential_total(asset)
        >>> self.log_message(total)

        >>> # Show the potential total for an asset
        >>> asset = Asset("ES", asset_type="future", expiration_date="2020-01-01")
        >>> total = self.get_asset_potential_total(asset)
        """
        asset = self._sanitize_user_asset(asset)
        return self.broker.get_asset_potential_total(self.name, asset)

    def submit_order(self, order: Order, **kwargs):
        """Submit an order or a list of orders for assets

        Submits an order or a list of orders for processing by the active broker.

        Parameters
        ---------
        order : Order object or list of Order objects
            Order object or a list of order objects containing the asset and instructions for executing the order.
        is_multileg : bool
            Tradier only.
            A boolean value to indicate if the orders are part of one multileg order.
            Currently, this is only available for Tradier.
        order_type : str
            Tradier only.
            The order type for the multileg order. Possible values are:
            ('market', 'debit', 'credit', 'even'). Default is 'market'.
        duration : str
            Tradier only.
            The duration for the multileg order. Possible values are:
            ('day', 'gtc', 'pre', 'post'). Default is 'day'.
        price : float
            Tradier only.
            The limit price for the multileg order. Required for 'debit' and 'credit' order types.
        tag : str
            Tradier only.
            A tag for the multileg order.

        Returns
        -------
        Order object or list of Order objects
            Processed order object(s).

        Examples
        --------
        Submitting a single order:

        >>> # For a market buy order
        >>> order = self.create_order("SPY", 100, "buy")
        >>> self.submit_order(order)

        >>> # For a limit buy order
        >>> order = self.create_order("SPY", 100, "buy", limit_price=100.00)
        >>> self.submit_order(order)

        >>> # For a stop loss order
        >>> order = self.create_order("SPY", 100, "buy", stop_price=100.00)
        >>> self.submit_order(order)

        >>> # For a stop limit order
        >>> order = self.create_order("SPY", 100, "buy", limit_price=100.00, stop_price=100.00)
        >>> self.submit_order(order)

        >>> # For a market sell order
        >>> order = self.create_order("SPY", 100, "sell")
        >>> self.submit_order(order)

        >>> # For buying a crypto with a market price
        >>> from lumibot.entities import Asset
        >>>
        >>> asset_base = Asset(
        >>>    "BTC",
        >>>    asset_type=Asset.AssetType.CRYPTO,
        >>> )
        >>> asset_quote = Asset(
        >>>    "USD",
        >>>    asset_type=Asset.AssetType.CRYPTO,
        >>> )
        >>> order = self.create_order(asset_base, 0.1, "buy", quote=asset_quote)
        >>> self.submit_order(order)
        >>> # or...
        >>> order = self.create_order((asset_base, asset_quote), 0.1, "buy")
        >>> self.submit_order(order)

        Submitting multiple orders:

        >>> # For 2 market buy orders
        >>> order1 = self.create_order("SPY", 100, "buy")
        >>> order2 = self.create_order("TLT", 200, "buy")
        >>> self.submit_order([order1, order2])

        >>> # For 2 limit buy orders
        >>> order1 = self.create_order("SPY", 100, "buy", limit_price=100.00)
        >>> order2 = self.create_order("TLT", 200, "buy", limit_price=100.00)
        >>> self.submit_order([order1, order2])

        >>> # For 2 CRYPTO buy orders
        >>> from lumibot.entities import Asset
        >>>
        >>> asset_BTC = Asset(
        >>>    "BTC",
        >>>    asset_type=Asset.AssetType.CRYPTO,
        >>> )
        >>> asset_ETH = Asset(
        >>>    "ETH",
        >>>    asset_type=Asset.AssetType.CRYPTO,
        >>> )
        >>> asset_quote = Asset(
        >>>    "USD",
        >>>    asset_type=Asset.AssetType.FOREX,
        >>> )
        >>> order1 = self.create_order(asset_BTC, 0.1, "buy", quote=asset_quote)
        >>> order2 = self.create_order(asset_ETH, 10, "buy", quote=asset_quote)
        >>> self.submit_order([order1, order2])
        >>> # or...
        >>> order1 = self.create_order((asset_BTC, asset_quote), 0.1, "buy")
        >>> order2 = self.create_order((asset_ETH, asset_quote), 10, "buy")
        >>> self.submit_order([order1, order2])
        """

        if isinstance(order, list):
            # Submit multiple orders
            # Validate orders
            default_multileg = True

            for o in order:
                if not self._validate_order(o):
                    return

                if o.asset.asset_type != "option":
                    default_multileg = False

            if 'is_multileg' not in kwargs:
                kwargs['is_multileg'] = default_multileg

            return self.broker.submit_orders(order, **kwargs)

        else:
            # Submit single order
            if not self._validate_order(order):
                return

            return self.broker.submit_order(order)

    def submit_orders(self, orders: List[Order], **kwargs):
        """[Deprecated] Submit a list of orders

        This method is deprecated and will be removed in future versions.
        Please use `submit_order` instead.

        Submits a list of orders for processing by the active broker.

        Parameters
        ----------
        orders : list of orders
            A list of order objects containing the asset and instructions for the orders.
        is_multileg : bool
            Tradier only.
            A boolean value to indicate if the orders are part of one multileg order.
            Currently, this is only available for Tradier.
        order_type : str
            Tradier only.
            The order type for the multileg order. Possible values are:
            ('market', 'debit', 'credit', 'even'). Default is 'market'.
        duration : str
            Tradier only.
            The duration for the multileg order. Possible values are:
            ('day', 'gtc', 'pre', 'post'). Default is 'day'.
        price : float
            Tradier only.
            The limit price for the multileg order. Required for 'debit' and 'credit' order types.
        tag : str
            Tradier only.
            A tag for the multileg order.

        Returns
        -------
        list of Order objects
            List of processed order objects.

        Examples
        --------
        >>> # For 2 market buy orders
        >>> order1 = self.create_order("SPY", 100, "buy")
        >>> order2 = self.create_order("TLT", 200, "buy")
        >>> self.submit_orders([order1, order2])

        >>> # For 2 limit buy orders
        >>> order1 = self.create_order("SPY", 100, "buy", limit_price=100.00)
        >>> order2 = self.create_order("TLT", 200, "buy", limit_price=100.00)
        >>> self.submit_orders([order1, order2])

        >>> # For 2 CRYPTO buy orders
        >>> from lumibot.entities import Asset
        >>>
        >>> asset_BTC = Asset(
        >>>    "BTC",
        >>>    asset_type=Asset.AssetType.CRYPTO,
        >>> )
        >>> asset_ETH = Asset(
        >>>    "ETH",
        >>>    asset_type=Asset.AssetType.CRYPTO,
        >>> )
        >>> asset_quote = Asset(
        >>>    "USD",
        >>>    asset_type=Asset.AssetType.FOREX,
        >>> )
        >>> order1 = self.create_order(asset_BTC, 0.1, "buy", quote=asset_quote)
        >>> order2 = self.create_order(asset_ETH, 10, "buy", quote=asset_quote)
        >>> self.submit_orders([order1, order2])
        >>> # or...
        >>> order1 = self.create_order((asset_BTC, asset_quote), 0.1, "buy")
        >>> order2 = self.create_order((asset_ETH, asset_quote), 10, "buy")
        >>> self.submit_orders([order1, order2])
        """
        #self.log_message("Warning: `submit_orders` is deprecated, please use `submit_order` instead.")
        return self.submit_order(orders, **kwargs)

    def wait_for_order_registration(self, order: Order):
        """Wait for the order to be registered by the broker

        Parameters
        ----------
        order : Order object
            Order object to be registered by the broker.

        Returns
        -------
        Order object

        Example
        -------
        >>> # For a market buy order
        >>> order = self.create_order("SPY", 100, "buy")
        >>> self.submit_order(order)
        >>> self.wait_for_order_registration(order)

        >>> # For a limit buy order
        >>> order = self.create_order("SPY", 100, "buy", limit_price=100.00)
        >>> self.submit_order(order)
        >>> self.wait_for_order_registration(order)


        """
        return self.broker.wait_for_order_registration(order)

    def wait_for_order_execution(self, order: Order):
        """Wait for one specific order to be executed or canceled by the broker

        Parameters
        ----------
        order : Order object
            Order object to be executed by the broker.

        Returns
        -------
        Order object

        Example
        -------
        >>> # For a market buy order
        >>> order = self.create_order("SPY", 100, "buy")
        >>> self.submit_order(order)
        >>> self.wait_for_order_execution(order)


        """
        return self.broker.wait_for_order_execution(order)

    def wait_for_orders_registration(self, orders: List[Order]):
        """Wait for the orders to be registered by the broker

        Parameters
        ----------
        orders : list of orders
            List of order objects to be registered by the broker.

        Returns
        -------
        list of orders

        Example
        -------
        >>> # For 2 market buy orders
        >>> order1 = self.create_order("SPY", 100, "buy")
        >>> order2 = self.create_order("TLT", 200, "buy")
        >>> self.submit_orders([order1, order2])
        >>> self.wait_for_orders_registration([order1, order2])
        """
        return self.broker.wait_for_orders_registration(orders)

    def wait_for_orders_execution(self, orders: List[Order]):
        """Wait for a list of orders to be executed or canceled by the broker

        Parameters
        ----------
        orders : list of orders
            List of order objects to be executed by the broker.

        Returns
        -------
        list of orders

        Example
        -------
        >>> # For 2 market buy orders
        >>> order1 = self.create_order("SPY", 100, "buy")
        >>> order2 = self.create_order("TLT", 200, "buy")
        >>> self.submit_orders([order1, order2])
        >>> self.wait_for_orders_execution([order1, order2])
        """
        return self.broker.wait_for_orders_execution(orders)

    def cancel_order(self, order: Order):
        """Cancel an order.

        Cancels a single open order provided.

        Parameters
        ---------
        An order object that the user seeks to cancel.

        Returns
        -------
        None

        Example
        -------
        >>> # Create an order then cancel it
        >>> order = self.create_order("SPY", 100, "buy")
        >>> self.submit_order(order)
        >>> self.cancel_order(order)

        """
        # Set the status to CANCELLING
        order.status = Order.OrderStatus.CANCELLING

        # Cancel the order
        return self.broker.cancel_order(order)

    def cancel_orders(self, orders: List[Order]):
        """Cancel orders in all strategies.

        Cancels all open orders provided in any of the running
        strategies.

        Parameters
        ----------
        orders : list of Order objects.

        Returns
        -------
        None

        Example
        -------
        >>> # Create two orders then cancel them
        >>> order1 = self.create_order("IBM", 100, "buy")
        >>> order2 = self.create_order("AAPL", 100, "buy")
        >>> self.submit_orders([order1, order2])
        >>>
        >>> # Cancel all orders
        >>> self.cancel_orders([order1, order2])
        """
        return self.broker.cancel_orders(orders)

    def cancel_open_orders(self):
        """Cancel all the strategy open orders.

        Cancels all orders that are open and awaiting execution within
        a given strategy. If running multiple strategies, will only
        cancel the orders in the current strategy.

        Parameters
        ----------
        None

        Returns
        -------
        None

        Example
        -------
        >>> # Cancel all open orders
        >>> self.cancel_open_orders()

        """
        return self.broker.cancel_open_orders(self.name)

    def modify_order(self, order: Order, limit_price: Union[float, None] = None, stop_price: Union[float, None] = None):
        """Modify an order.

        Modifies a single open order provided.

        Parameters
        ----------
        order : Order object
            Order object to be modified.
        limit_price : float
            New limit price for a limit order. Default is None.
        stop_price : float
            New stop price for a stop order. Default is None.

        Returns
        -------
        None

        Example
        -------
        >>> # Modify an existing order
        >>> order = self.create_order("SPY", 100, "buy", limit_price=100.00)
        >>> self.submit_order(order)
        >>> self.modify_order(order, limit_price=101.00)
        """
        # Check if the order is already cancelled or filled
        if not order.is_active():
            return

        if not order.identifier:
            raise ValueError("Order identifier is not set, unable to modify order. Did you remember to submit it?")

        result = self.broker.modify_order(order, limit_price=limit_price, stop_price=stop_price)
        if limit_price is not None:
            order.limit_price = limit_price
        if stop_price is not None:
            order.stop_price = stop_price
        return result

    def sell_all(self, cancel_open_orders: bool = True, is_multileg: bool = False):
        """Sell all strategy positions.

        The system will generate closing market orders for each open
        position. If `cancel_open_orders` is `True`, then all open
        orders will also be cancelled.

        Open orders are cancelled before the positions are closed.

        Parameters
        ----------
        cancel_open_orders : boolean
            Cancel all order if True, leave all orders in place if
            False. Default is True.
        is_multileg : boolean
            When True, will use multileg orders to close positions.

        Returns
        -------
        None

        Example
        -------
        >>> # Will close all positions for the strategy
        >>> self.sell_all()
        """
        self.broker.sell_all(self.name, cancel_open_orders=cancel_open_orders, strategy=self, is_multileg=is_multileg)

    def close_position(self, asset, fraction: float = 1.0):
        """
        Close a single position for the specified asset.

        This method attempts to close an open position for the given asset. For most brokers, this is done by submitting a market sell order for the open position. For crypto futures brokers (such as Bitunix), this may use a broker-specific fast-close or "flash close" endpoint to close the position immediately at market price.

        Args:
            asset (str or Asset): The symbol or Asset object identifying the position to close.

        Returns:
            Any: The broker.close_position result, or None if no action was taken.

        Notes:
            - For crypto futures (e.g., Bitunix), this will use the broker's flash close endpoint if available.
            - For spot/stock/futures brokers, this will submit a market sell order for the open position.
            - If no open position exists, this method does nothing.
        """
        asset_obj = self._sanitize_user_asset(asset)
        result = self.broker.close_position(self.name, asset_obj, fraction)
        if result is not None:
            return result

    def close_positions(self, assets):
        """
        Close multiple positions for the specified assets.

        Iterates over the provided list of assets and attempts to close each open position. See `close_position` for details on how each position is closed.

        Args:
            assets (list[str or Asset]): Symbols or Asset objects identifying the positions to close.

        Returns:
            list: Results from each `close_position` call, or None if no action was taken.

        Notes:
            - For crypto futures (e.g., Bitunix), this will use the broker's flash close endpoint if available.
            - For spot/stock/futures brokers, this will submit a market sell order for each open position.
        """
        results = []
        for asset in assets:
            results.append(self.close_position(asset))
        return results

    def get_last_price(self, asset: Union[Asset, str], quote=None, exchange=None) -> Union[float, Decimal, None]:
        """Takes an asset and returns the last known price

        Makes an active call to the market to retrieve the last price.
        In backtesting will provide the close of the last complete bar.

        Parameters
        ----------
        asset : Asset object or str
            Asset object for which the last closed price will be
            retrieved.
        quote : Asset object
            Quote asset object for which the last closed price will be
            retrieved. This is required for cryptocurrency pairs.
        exchange : str
            Exchange name for which the last closed price will be
            retrieved. This is required for some cryptocurrency pairs.
        should_use_last_close : bool
            If False, it will make Interactive Brokers only return the
            price of an asset if it has been trading today. Defaults to True.
            TODO: Should this option be depricated? It is now commented out below

        Returns
        -------
        Float or Decimal
            Last closed price as either a float or Decimal object.

        Example
        -------
        >>> # Will return the last price for the asset
        >>> asset = "SPY"
        >>> last_price = self.get_last_price(asset)
        >>> self.log_message(f"Last price for {asset} is {last_price}")

        >>> # Will return the last price for a crypto asset
        >>> base = Asset(symbol="BTC", asset_type="crypto")
        >>> quote = Asset(symbol="USDT", asset_type="crypto")
        >>> last_price = self.get_last_price(base, quote=quote)
        >>> self.log_message(f"Last price for BTC/USDT is {last_price}")

        >>> # Will return the last price for a crypto asset
        >>> base = Asset(symbol="BTC", asset_type="crypto")
        >>> quote = Asset(symbol="USD", asset_type="forex")
        >>> last_price = self.get_last_price(base, quote=quote)
        >>> self.log_message(f"Last price for BTC/USD is {last_price}")

        >>> # Will return the last price for a futures asset
        >>> self.base = Asset(
        >>>     symbol="ES",
        >>>     asset_type="future",
        >>>     expiration=date(2022, 12, 16),
        >>> )
        >>> price = self.get_last_price(asset=self.base, exchange="CME")
        """

        # Check if the asset is valid
        if asset is None or (isinstance(asset, Asset) and not asset.is_valid()):
            self.logger.error(
                f"Asset in get_last_price() must be a valid asset. Got {asset} of type {type(asset)}. You may be missing some of the required parameters for the asset type (eg. strike price for options, expiry for options/futures, etc)."
            )
            return None

        # Check if the Asset object is a string or Asset object
        if not (isinstance(asset, Asset) or isinstance(asset, str) or isinstance(asset, tuple)):
            logger.error(
                f"Asset in get_last_price() must be a string or Asset or tuple object. Got {asset} of type {type(asset)}"
            )
            return None

        asset = self._sanitize_user_asset(asset)

        if quote is None:
            quote_asset = self.quote_asset
        else:
            quote_asset = quote

        try:
            return self.broker.get_last_price(
                asset,
                quote=quote_asset,
                exchange=exchange,
                # should_use_last_close=should_use_last_close,
            )
        except Exception as e:
            self.log_message(f"Could not get last price for {asset}", color="red")
            self.log_message(f"{e}")
            return None

    def get_quote(self, asset: Asset, quote: Asset = None, exchange: str = None) -> Quote:
        """Get a quote for the asset.

        Parameters
        ----------
        asset : Asset object
            The asset for which the quote is needed.
        quote : Asset object, optional
            The quote asset for cryptocurrency pairs.
        exchange : str, optional
            The exchange to get the quote from.

        Returns
        -------
        Quote
            A Quote object with the quote information, eg. bid, ask, etc.
        """

        asset = self._sanitize_user_asset(asset)

        try:
            if self.broker.option_source and asset.asset_type == "option":
                return self.broker.option_source.get_quote(asset, quote=quote, exchange=exchange)
            else:
                return self.broker.data_source.get_quote(asset, quote=quote, exchange=exchange)
        except Exception as e:
            self.log_message(f"Error getting quote from data source: {e}", color="red")
            return Quote(asset=asset)

    def get_tick(self, asset: Union[Asset, str]):
        """Takes an Asset and returns the last known price"""
        # TODO: Should this function be depricated? This appears to be an IBKR-only thing.
        asset = self._sanitize_user_asset(asset)
        return self.broker._get_tick(asset)

    def get_last_prices(self, assets: List[Asset], quote=None, exchange=None):
        """Takes a list of assets and returns the last known prices

        Makes an active call to the market to retrieve the last price. In backtesting will provide the close of the last complete bar.

        Parameters
        ----------
        assets : list of Asset objects
            List of Asset objects for which the last closed price will be retrieved.
        quote : Asset object
            Quote asset object for which the last closed price will be
            retrieved. This is required for cryptocurrency pairs.
        exchange : str
            The exchange to get the prices of.

        Returns
        -------
        list of floats or Decimals
            Last known closing prices as either a list of floats or Decimal objects.

        Example
        -------
        >>> # Will return the last price for the assets
        >>> assets = ["SPY", "TLT"]
        >>> last_prices = self.get_last_prices(assets)
        """
        symbol_asset = isinstance(assets[0], str)
        if symbol_asset:
            assets = [self._sanitize_user_asset(asset) for asset in assets]

        asset_prices = self.broker.get_last_prices(assets, quote=quote, exchange=exchange)

        if symbol_asset:
            return {a.symbol: p for a, p in asset_prices.items()}
        else:
            return asset_prices

    # ======= Broker Methods  ============
    def options_expiry_to_datetime_date(self, date: datetime.date):
        """Converts an IB Options expiry to datetime.date.

        Parameters
        ----------
            date : str
                String in the format of 'YYYYMMDD'

        Returns
        -------
            datetime.date

        Example
        -------
        >>> # Will return the date for the expiry
        >>> date = "20200101"
        >>> expiry_date = self.options_expiry_to_datetime_date(date)
        >>> self.log_message(f"Expiry date for {date} is {expiry_date}")
        """
        return datetime.datetime.strptime(date, "%Y%m%d").date()

    def get_chains(self, asset: Asset):
        """Returns option chains.

        Obtains option chain information for the asset (stock) from each
        of the exchanges the options trade on and returns a dictionary
        for each exchange.

        Parameters
        ----------
        asset : Asset object
            The stock whose option chain is being fetched. Represented
            as an asset object.

        Returns
        -------
        dictionary of dictionary
            Format:
            - `Multiplier` (str) eg: `100`
            - 'Chains' - paired Expiration/Strike info to guarentee that the strikes are valid for the specific
                         expiration date.
                         Format:
                           chains['Chains']['CALL'][exp_date] = [strike1, strike2, ...]
                         Expiration Date Format: 2023-07-31

        Example
        -------
        >>> # Will return the option chains for SPY
        >>> asset = "SPY"
        >>> chains = self.get_chains(asset)
        """
        asset = self._sanitize_user_asset(asset)
        return self.broker.get_chains(asset)

    def get_next_trading_day(self, date: str, exchange="NYSE"):
        """
        Finds the next trading day for the given date and exchange.

        Parameters
        ----------
        date : str
            The date from which to find the next trading day, in 'YYYY-MM-DD' format.
        exchange : str
            The exchange calendar to use, default is 'NYSE'.

        Returns
        -------
            next_trading_day (datetime.date): The next trading day after the given date.
        """

        # Load the specified market calendar
        calendar = mcal.get_calendar(exchange)

        # Convert the input string date to pandas Timestamp
        date_timestamp = pd.Timestamp(date)

        # Get the next trading day. The schedule is inclusive of the start_date when the market is open on this day.
        # Hence, we add 1 day to the start_date to ensure we start checking from the day after.
        schedule = calendar.schedule(start_date=date_timestamp + pd.Timedelta(days=1),
                                     end_date=date_timestamp + pd.Timedelta(days=10))

        # The next trading day is the first entry in the schedule
        next_trading_day = schedule.index[0].date()

        return next_trading_day

    def get_chain(self, chains: dict, exchange: str = "SMART"):
        """Returns option chain for a particular exchange.

        Takes in a full set of chains for all the exchanges and returns
        on chain for a given exchange. The full chains are returned
        from `get_chains` method.

        Parameters
        ----------
        chains : dictionary of dictionaries
            The chains dictionary created by `get_chains` method.

        exchange : str optional
            The exchange such as `SMART`, `CBOE`. Default is `SMART`

        Returns
        -------
        dictionary of dictionary
            Format:
            - `Multiplier` (str) eg: `100`
            - 'Chains' - paired Expiration/Strke info to guarentee that the stikes are valid for the specific
                         expiration date.
                         Format:
                           chains['Chains']['CALL'][exp_date] = [strike1, strike2, ...]
                         Expiration Date Format: 2023-07-31

        Example
        -------
        >>> # Will return the option chains for SPY
        >>> asset = "SPY"
        >>> chain = self.get_chain(asset)
        """
        return self.broker.get_chain(chains)

    def get_chain_full_info(
            self,
            asset: Asset,
            expiry: Union[str, datetime.datetime, datetime.date],
            chains: dict = None,
            underlying_price: float = None,
            risk_free_rate: float = None,
            strike_min: float = None,
            strike_max: float = None
            ) -> pd.DataFrame:
        """Returns full option chain information for a given asset and expiry. This will include all known broker
        option information for each strike price, including: greeks, bid, ask, volume, open_interest etc. Not all
        Lumibot brokers provide all of this data and tick-style data like bid/ask/open_interest are not available
        during BackTesting.

        This method can be quite slow for brokers that do not provide this data natively, as it will need to make
        multiple API calls (1 per strike) to get all the data.  Min/Max strike values can be provided to reduce the
        number of queries made.

        Using the `chains` dictionary obtained from `get_chains` finds
        all the options information for a given asset and expiry.

        Parameters
        ----------
        asset : Asset object
            The asset for which the option chain is being fetched.
        expiry : str | datetime.datetime | datetime.date
            The expiry date for the option chain in the format of
            `2023-07-31`.
        chains : dictionary of dictionaries, optional
            The chains dictionary created by `get_chains` method. If not
            provided, the method will fetch the chains for the asset (if needed). This is
            needed to discover the list of strikes. Some brokers like Tradier or
            Polygon LiveData do not need the strike list.
        underlying_price : float, optional
            The price of the underlying asset. If not provided, the
            method will fetch the price from the broker. Useful to provide to reduce the
            number of API calls.
        risk_free_rate : float, optional
            The risk-free rate to use in the calculations. If not
            provided, the method will use the default risk-free rate.
        strike_min : float, optional
            The minimum strike price to return. If not provided, the
            method will return all strikes.
        strike_max : float, optional
            The maximum strike price to return. If not provided, the
            method will return all strikes.

        Returns
        -------
        pd.DataFrame
            A DataFrame with the option chain information.

        Example
        -------
        >>> # Will return the option chains for SPY
        >>> asset = "SPY"
        >>> expiry = "2023-07-31"
        >>> df = self.get_chain_full_info(asset, expiry)
        >>> print(f"Strike: {df.iloc[0]['strike']}, Delta: {df.iloc[0]['greeks.delta']}")
        """
        asset = self._sanitize_user_asset(asset)
        risk_free_rate = risk_free_rate if risk_free_rate is not None else self.risk_free_rate
        if underlying_price is None:
            underlying_asset = Asset(symbol=asset.symbol, asset_type="stock")
            und_price = self.get_last_price(underlying_asset)
        else:
            und_price = underlying_price

        return self.broker.get_chain_full_info(asset, expiry, chains=chains, underlying_price=und_price,
                                               risk_free_rate=risk_free_rate, strike_min=strike_min,
                                               strike_max=strike_max)

    def get_expiration(self, chains: dict):
        """Returns expiration dates for an option chain for a particular
        exchange.

        Using the `chains` dictionary obtained from `get_chains` finds
        all expiry dates for the option chains on a given
        exchange. The return list is sorted.

        Parameters
        ---------
        chains : dictionary of dictionaries
            The chains dictionary created by `get_chains` method.

        Returns
        -------
        list of datetime.date
            Sorted list of dates in the form of `2022-10-13`.

        Example
        -------
        >>> # Will return the expiry dates for SPY
        >>> asset = "SPY"
        >>> expiry_dates = self.get_expiration(asset)
        """
        return self.broker.get_expiration(chains)

    def get_multiplier(self, chains: dict, exchange: str = "SMART"):
        """Returns option chain for a particular exchange.

        Using the `chains` dictionary obtained from `get_chains` finds
        all the multipliers for the option chains on a given
        exchange.

        Parameters
        ----------
        chains : dictionary of dictionaries
            The chains dictionary created by `get_chains` method.

        exchange : str optional
            The exchange such as `SMART`, `CBOE`. Default is `SMART`

        Returns
        -------
        list of str
            Sorted list of dates in the form of `20221013`.

        Example
        -------
        >>> # Will return the multiplier for SPY
        >>> asset = "SPY"
        >>> multiplier = self.get_multiplier(asset)
        """

        return self.broker.get_multiplier(chains, exchange=exchange)

    def get_strikes(self, asset: Asset, chains: dict = None):
        """Returns a list of strikes for a give underlying asset.

        Using the `chains` dictionary obtained from `get_chains` finds
        all the multiplier for the option chains on a given
        exchange.

        Parameters
        ----------
        asset : Asset
            Asset object as normally used for an option but without
            the strike information. The Asset object must be an option asset type.
        chains : dictionary of dictionaries, optional
            The chains dictionary created by `get_chains` method. If not
            provided, the method will fetch the chains for the asset.

        Returns
        -------
        list of floats
            Sorted list of strikes as floats.

        Example
        -------
        >>> # Will return the strikes for SPY
        >>> asset = "SPY"
        >>> strikes = self.get_strikes(asset)
        """

        asset = self._sanitize_user_asset(asset)
        return self.broker.get_strikes(asset, chains)

    def find_first_friday(self, timestamp: Union[datetime.datetime, pd.Timestamp]):
        """Finds the first Friday of the month for a given timestamp.

        Parameters
        ----------
        timestamp : datetime.datetime | pd.Timestamp
            The timestamp for which the first Friday of the month is
            needed.

        Returns
        -------
        datetime.datetime
            The first Friday of the month.
        """
        # Convert the timestamp to a datetime object if it's not already one
        if isinstance(timestamp, pd.Timestamp):
            timestamp = timestamp.to_pydatetime()

        # Get the day index of the first day of the month (0 is Monday, 1 is Tuesday, etc.)
        day_index = timestamp.weekday()

        # Calculate the number of days to add to reach the first Friday
        days_to_add = (4 - day_index) % 7

        # Create a new datetime object for the first Friday of the month
        first_friday = timestamp + datetime.timedelta(days=days_to_add)

        return first_friday

    def get_option_expiration_after_date(self, dt: datetime.date):
        """Returns the next option expiration date after the given date.

        Parameters
        ----------
        dt : datetime.date
            The date to find the next option expiration date after.

        Returns
        -------
        datetime.date
            The next option expiration date after the given date.

        Example
        -------
        >>> # Will return the next option expiration date after the given date
        >>> dt = datetime.date(2021, 1, 1)
        >>> next_option_expiration = self.get_next_option_expiration(dt)
        """

        tz = self.timezone
        if dt.tzinfo is None:
            dt = pd.Timestamp(dt).tz_localize(tz)
        else:
            dt = pd.Timestamp(dt).tz_convert(tz)

        # Loop over this month and the next month
        for month_increment in [0, 1]:
            # Calculate the first day of the target month
            first_day_of_month = (
                pd.Timestamp(dt.year, dt.month + month_increment, 1)
                if dt.month + month_increment <= 12
                else pd.Timestamp(dt.year + 1, (dt.month + month_increment) % 12, 1)
            )

            # Localize the first day of the month to the timezone
            first_day_of_month = first_day_of_month.tz_localize(tz)

            # Get the first Friday of the month
            first_friday = self.find_first_friday(first_day_of_month)

            # Calculate the third Friday
            third_friday = first_friday + pd.tseries.offsets.Week(2)

            # Check if the third Friday is after the input date
            if third_friday > dt:
                expiration = third_friday
                break

        # Check if the market is open on the expiration date, if not then get the previous open day
        nyse = mcal.get_calendar("NYSE")

        # Get the schedule for all the days that the market is open between the given date and the proposed expiration date
        end_date = expiration + pd.tseries.offsets.BusinessDay(
            2
        )  # Add 2 days to include the expiration date, in case it is a holiday
        schedule = nyse.schedule(start_date=dt, end_date=end_date)

        # Change the schedule index timezone to be the same as the expiration date timezone
        schedule.index = schedule.index.tz_localize(tz)

        # Check if the expiration date is in the schedule, if not then get the previous open day. Make sure they're only comparing dates, not times
        if expiration.date() not in schedule.index.date:
            # Find the date in the schedule that is right before the expiration date
            previous_open_day = schedule.index[schedule.index < expiration][-1]
            return previous_open_day.date()
        else:
            return expiration.date()

    def get_greeks(
        self,
        asset: Asset,
        asset_price: float = None,
        underlying_price: float = None,
        risk_free_rate: float = None,
        query_greeks: bool = False,
    ):
        """Returns the greeks for the option asset at the current
        bar.

        Will return all the greeks available. API Querying for prices
        and rates are expensive, so they should be passed in as arguments
        most of the time.

        Parameters
        ----------
        asset : Asset
            Option asset only for with greeks are desired.
        asset_price : float, optional
            The price of the option asset, by default None
        underlying_price : float, optional
            The price of the underlying asset, by default None
        risk_free_rate : float, optional
            The risk-free rate used in interest calculations, by default None
        query_greeks : bool, optional
            Whether to query the greeks from the broker. By default, the greeks are calculated locally, but if the
            broker supports it, they can be queried instead which could theoretically be more precise.

        Returns
        -------
        Returns a dictionary with greeks as keys and greek values as
        values.

        implied_volatility : float
            The implied volatility.
        delta : float
            The option delta value.
        option_price : float
            The option price.
        pv_dividend : float
            The present value of dividends expected on the option's
            underlying.
        gamma : float
            The option gamma value.
        vega : float
            The option vega value.
        theta : float
            The option theta value.
        underlying_price :
            The price of the underlying.

        Example
        -------
        >>> # Will return the greeks for SPY
        >>> opt_asset = Asset("SPY", expiration=date(2021, 1, 1), strike=100, option_type="call"
        >>> greeks = self.get_greeks(opt_asset)
        >>> implied_volatility = greeks["implied_volatility"]
        >>> delta = greeks["delta"]
        >>> gamma = greeks["gamma"]
        >>> vega = greeks["vega"]
        >>> theta = greeks["theta"]
        """
        if asset.asset_type != "option":
            self.log_message(
                "The greeks method was called using an asset other "
                "than an option. Unable to retrieve greeks for non-"
                "option assest."
            )
            return None

        # Do the expensize API calls here if needed
        opt_price = asset_price if asset_price is not None else self.get_last_price(asset)
        if risk_free_rate is not None:
            risk_free_rate = risk_free_rate
        else:
            risk_free_rate = self.risk_free_rate
        if underlying_price is None:
            underlying_asset = Asset(symbol=asset.symbol, asset_type="stock")
            und_price = self.get_last_price(underlying_asset)
        else:
            und_price = underlying_price

        return self.broker.get_greeks(
            asset,
            asset_price=opt_price,
            underlying_price=und_price,
            risk_free_rate=risk_free_rate,
            query_greeks=query_greeks,
        )

    # ======= Data Source Methods =================

    @property
    def timezone(self):
        """Returns the timezone of the data source. By default, America/New_York.

        Returns
        -------
        str
            The timezone of the data source.

        Example
        -------
        >>> # Will return the timezone of the data source
        >>> timezone = self.timezone
        >>> self.log_message(f"Timezone: {timezone}")
        """
        return self.broker.data_source.DEFAULT_TIMEZONE

    @property
    def pytz(self):
        """Returns the pytz object of the data source. By default, America/New_York.

        Returns
        -------
        pytz.timezone
            The pytz object of the data source.

        Example
        -------
        >>> # Will return the pytz object of the data source
        >>> pytz = self.pytz
        >>> self.log_message(f"pytz: {pytz}")
        """
        return self.broker.data_source.tzinfo

    def get_datetime(self, adjust_for_delay: bool = False):
        """Returns the current datetime according to the data source. In a backtest this will be the current bar's datetime. In live trading this will be the current datetime on the exchange.

        Parameters
        ----------
        adjust_for_delay : bool
            If True, will adjust the datetime for any delay in the data source.

        Returns
        -------
        datetime.datetime
            The current datetime.

        Example
        -------
        >>> # Will return the current datetime
        >>> datetime = self.get_datetime()
        >>> self.log_message(f"The current datetime is {datetime}")
        """
        return self.broker.data_source.get_datetime(adjust_for_delay=adjust_for_delay)

    def get_timestamp(self):
        """Returns the current timestamp according to the data source. In a backtest this will be the current bar's timestamp. In live trading this will be the current timestamp on the exchange.

        Returns
        -------
        int
            The current timestamp.

        Example
        -------
        >>> # Will return the current timestamp
        >>> timestamp = self.get_timestamp()
        >>> self.log_message(f"The current timestamp is {timestamp}")
        """
        return self.broker.data_source.get_timestamp()

    def register_cron_callback(self, cron_schedule: str, callback_function: Callable) -> str:
        """Register a callback function to be executed according to a cron schedule.

        Parameters
        ----------
        cron_schedule : str
            A cron schedule string (e.g., "0 9 * * 1-5" for 9:00 AM Monday through Friday)
        callback_function : callable
            The function to call on the schedule

        Returns
        -------
        str
            The job ID that can be used to remove the job later

        Example
        -------
        >>> self.register_cron_callback("0 9 * * 1-5", self.morning_update)

        Notes
        -----
        This method does nothing in backtesting mode.
        """
        # Generate a unique job ID
        job_id = f"cron_callback_{uuid.uuid4().hex}"

        # Do nothing in backtesting mode
        if self.is_backtesting:
            self.log_message(f"Skipping registration of cron callback {callback_function.__name__} in backtesting mode")
            return job_id

        # Create a CronTrigger from the schedule string using the broker's timezone
        trigger = CronTrigger.from_crontab(cron_schedule, timezone=self.pytz)

        # Add the job to the scheduler
        self._executor.scheduler.add_job(
            callback_function,
            trigger,
            id=job_id,
            name=f"Cron Callback: {callback_function.__name__}",
            jobstore="default"
        )

        self.log_message(f"Registered cron callback {callback_function.__name__} with schedule: {cron_schedule} in {self.timezone} timezone")
        return job_id

    def get_round_minute(self, timeshift: int = 0):
        """Returns the current minute rounded to the nearest minute. In a backtest this will be the current bar's timestamp. In live trading this will be the current timestamp on the exchange.

        Parameters
        ----------
        timeshift : int
            The number of minutes to shift the time.

        Returns
        -------
        int
            The current minute rounded to the nearest minute.

        Example
        -------
        >>> # Will return the current minute rounded to the nearest minute
        >>> round_minute = self.get_round_minute()
        >>> self.log_message(f"The current minute rounded to the nearest minute is {round_minute}")
        """
        return self.broker.data_source.get_round_minute(timeshift=timeshift)

    def get_last_minute(self):
        """Returns the last minute of the current day. In a backtest this will be the current bar's timestamp. In live trading this will be the current timestamp on the exchange.

        Returns
        -------
        int
            The last minute of the current day.

        Example
        -------
        >>> # Will return the last minute of the current day
        >>> last_minute = self.get_last_minute()
        >>> self.log_message(f"The last minute of the current day is {last_minute}")
        """
        return self.broker.data_source.get_last_minute()

    def get_round_day(self, timeshift: int = 0):
        """Returns the current day rounded to the nearest day. In a backtest this will be the current bar's timestamp. In live trading this will be the current timestamp on the exchange.

        Parameters
        ----------
        timeshift : int
            The number of days to shift the time.

        Returns
        -------
        int
            The current day rounded to the nearest day.

        Example
        -------
        >>> # Will return the current day rounded to the nearest day
        >>> round_day = self.get_round_day()
        >>> self.log_message(f"The current day rounded to the nearest day is {round_day}")
        """
        return self.broker.data_source.get_round_day(timeshift=timeshift)

    def get_last_day(self):
        """Returns the last day of the current month. In a backtest this will be the current bar's timestamp. In live trading this will be the current timestamp on the exchange.

        Returns
        -------
        int
            The last day of the current month.

        Example
        -------
        >>> # Will return the last day of the current month
        >>> last_day = self.get_last_day()
        >>> self.log_message(f"The last day of the current month is {last_day}")
        """
        return self.broker.data_source.get_last_day()

    def get_datetime_range(self, length: int, timestep: str = "minute", timeshift: int = None):
        """Returns a list of datetimes for the given length and timestep.

        Parameters
        ----------
        length : int
            The number of datetimes to return.
        timestep : str
            The timestep of the datetimes.
        timeshift : int
            The number of timesteps to shift the datetimes.

        Returns
        -------
        list
            A list of datetimes.

        Example
        -------
        >>> # Will return a list of datetimes for the current day
        >>> datetimes = self.get_datetime_range(length=1, timestep="day")
        >>> self.log_message(f"Datetimes: {datetimes}")
        """
        return self.broker.data_source.get_datetime_range(length, timestep=timestep, timeshift=timeshift)

    def localize_datetime(self, dt: datetime.datetime):
        """Returns a datetime localized to the data source's timezone.

        Parameters
        ----------
        dt : datetime.datetime
            The datetime to localize.

        Returns
        -------
        datetime.datetime
            The localized datetime.

        Example
        -------
        >>> # Will return a datetime localized to the data source's timezone
        >>> localize_datetime = self.localize_datetime(dt=datetime.datetime(2020, 1, 1))
        >>> self.log_message(f"Localized datetime: {localize_datetime}")
        """
        return self.broker.data_source.localize_datetime(dt)

    def to_default_timezone(self, dt: datetime.datetime):
        """Returns a datetime localized to the data source's default timezone.

        Parameters
        ----------
        dt : datetime.datetime
            The datetime to localize.

        Returns
        -------
        datetime.datetime
            The localized datetime.

        Example
        -------
        >>> # Will return a datetime localized to the data source's default timezone
        >>> to_default_timezone = self.to_default_timezone(dt=datetime.datetime(2020, 1, 1))
        >>> self.log_message(f"Localized datetime: {to_default_timezone}")
        """
        return self.broker.data_source.to_default_timezone(dt)

    def load_pandas(self, asset: Union[Asset, str], df: pd.DataFrame):
        asset = self._sanitize_user_asset(asset)
        self.broker.data_source.load_pandas(asset, df)

    def create_asset(
        self,
        symbol: str,
        asset_type: str = "stock",
        expiration: datetime.datetime = None,
        strike: float = None,
        right: str = None,
        multiplier: int = 1,
    ):
        """Creates an asset object. This is used to create an asset object.

        Parameters
        ----------
        symbol : str
            The symbol of the asset.

        asset_type : str
            The type of the asset. Can be either "stock", "option", or "future", "crytpo".

        expiration : datetime.datetime
            The expiration date of the asset (optional, only required for options and futures).

        strike : str
            The strike price of the asset (optional, only required for options).

        right : str
            The right of the option (optional, only required for options).

        multiplier : int
            The multiplier of the asset (optional, only required for options and futures).

        Returns
        -------
        Asset
            The asset object.

        Example
        -------
        >>> # Will create a stock object
        >>> asset = self.create_asset("AAPL", asset_type="stock")

        >>> # Will create an option object
        >>> asset = self.create_asset("AAPL", asset_type="option", expiration=datetime.datetime(2020, 1, 1), strike=100, right="CALL")

        >>> # Will create a future object
        >>> asset = self.create_asset("AAPL", asset_type="future", multiplier=100)

        >>> # Will create a FOREX asset
        >>> asset = self.create_asset(symbol="EUR", asset_type="forex")

        >>> # Will create a CRYPTO asset
        >>> asset = self.create(symbol="BTC", asset_type="crypto"),
        >>> asset = self.create(symbol="USDT", asset_type="crypto"),
        >>> asset = self.create(symbol="EUR", asset_type="crypto"),
        >>> asset = self.create(symbol="ETH", asset_type="crypto"),
        """

        return Asset(
            symbol=symbol,
            asset_type=asset_type,
            expiration=expiration,
            strike=strike,
            right=right,
            multiplier=multiplier,
        )

    def add_marker(
            self,
            name: str,
            value: float = None,
            color: str = "blue",
            symbol: str = "circle",
            size: int = None,
            detail_text: str = None,
            dt: Union[datetime.datetime, pd.Timestamp] = None,
            plot_name: str = "default_plot"
            ):
        """Adds a marker to the indicators plot that loads after a backtest. This can be used to mark important events on the graph, such as price crossing a certain value, marking a support level, marking a resistance level, etc.

        Parameters
        ----------
        name : str
            The name of the marker. This is used to display the name on the graph. Eg. "Overbought", "Oversold", "Stop Loss", "Take Profit", ...
        symbol : str
            The symbol of the marker. Possible values are 'circle', 'circle-open', 'circle-dot', 'circle-open-dot', 'square', 'square-open', 'square-dot', 'square-open-dot', 'diamond', 'diamond-open', 'diamond-dot', 'diamond-open-dot', 'cross', 'cross-open', 'cross-dot', 'cross-open-dot', 'x', 'x-open', 'x-dot', 'x-open-dot', 'triangle-up', 'triangle-up-open', 'triangle-up-dot', 'triangle-up-open-dot', 'triangle-down', 'triangle-down-open', 'triangle-down-dot', 'triangle-down-open-dot', 'triangle-left', 'triangle-left-open', 'triangle-left-dot', 'triangle-left-open-dot', 'triangle-right', 'triangle-right-open', 'triangle-right-dot', 'triangle-right-open-dot', 'triangle-ne', 'triangle-ne-open', 'triangle-ne-dot', 'triangle-ne-open-dot', 'triangle-se', 'triangle-se-open', 'triangle-se-dot', 'triangle-se-open-dot', 'triangle-sw', 'triangle-sw-open', 'triangle-sw-dot', 'triangle-sw-open-dot', 'triangle-nw', 'triangle-nw-open', 'triangle-nw-dot', 'triangle-nw-open-dot', 'pentagon', 'pentagon-open', 'pentagon-dot', 'pentagon-open-dot', 'hexagon', 'hexagon-open', 'hexagon-dot', 'hexagon-open-dot', 'hexagon2', 'hexagon2-open', 'hexagon2-dot', 'hexagon2-open-dot', 'octagon', 'octagon-open', 'octagon-dot', 'octagon-open-dot', 'star', 'star-open', 'star-dot', 'star-open-dot', 'hexagram', 'hexagram-open', 'hexagram-dot', 'hexagram-open-dot', 'star-triangle-up', 'star-triangle-up-open', 'star-triangle-up-dot', 'star-triangle-up-open-dot', 'star-triangle-down', 'star-triangle-down-open', 'star-triangle-down-dot', 'star-triangle-down-open-dot', 'star-square', 'star-square-open', 'star-square-dot', 'star-square-open-dot', 'star-diamond', 'star-diamond-open', 'star-diamond-dot', 'star-diamond-open-dot', 'diamond-tall', 'diamond-tall-open', 'diamond-tall-dot', 'diamond-tall-open-dot', 'diamond-wide', 'diamond-wide-open', 'diamond-wide-dot', 'diamond-wide-open-dot', 'hourglass', 'hourglass-open', 'bowtie', 'bowtie-open', 'circle-cross', 'circle-cross-open', 'circle-x', 'circle-x-open', 'square-cross', 'square-cross-open', 'square-x', 'square-x-open', 'diamond-cross', 'diamond-cross-open', 'diamond-x', 'diamond-x-open', 'cross-thin', 'cross-thin-open', 'x-thin', 'x-thin-open', 'asterisk', 'asterisk-open', 'hash', 'hash-open', 'hash-dot', 'hash-open-dot', 'y-up', 'y-up-open', 'y-down', 'y-down-open', 'y-left', 'y-left-open', 'y-right', 'y-right-open', 'line-ew', 'line-ew-open', 'line-ns', 'line-ns-open', 'line-ne', 'line-ne-open', 'line-nw', 'line-nw-open', 'arrow-up', 'arrow-up-open', 'arrow-down', 'arrow-down-open', 'arrow-left', 'arrow-left-open', 'arrow-right', 'arrow-right-open', 'arrow-bar-up', 'arrow-bar-up-open', 'arrow-bar-down', 'arrow-bar-down-open', 'arrow-bar-left', 'arrow-bar-left-open', 'arrow-bar-right', 'arrow-bar-right-open', 'arrow', 'arrow-open', 'arrow-wide', 'arrow-wide-open'
        value : float or int
            The value of the marker. Default is the current portfolio value.
        color : str
            The color of the marker. Possible values are "red", "green", "blue", "yellow", "orange", "purple", "pink", "brown", "black", and "white".
        size : int
            The size of the marker.
        detail_text : str
            The text to display when the marker is hovered over.
        dt : datetime.datetime or pandas.Timestamp
            The datetime of the marker. Default is the current datetime.
        plot_name : str
            The name of the subplot to add the marker to. If "default_plot" (the default value) or None, the marker will be added to the main plot.

        Example
        -------
        >>> # Will add a marker to the chart
        >>> self.add_chart_marker("Overbought", symbol="circle", color="red", size=10)
        """

        # Check that the parameters are valid
        if not isinstance(name, str):
            raise ValueError(
                f"Invalid name parameter in add_marker() method. Name must be a string but instead got {name}, "
                f"which is a type {type(name)}."
            )

        if not isinstance(symbol, str):
            raise ValueError(
                f"Invalid symbol parameter in add_marker() method. Symbol must be a string but instead got {symbol}, "
                f"which is a type {type(symbol)}."
            )

        if value is not None and not isinstance(value, (float, int, np.float64)):
            raise ValueError(
                f"Invalid value parameter in add_marker() method. Value must be a float or int but instead got {value}, "
                f"which is a type {type(value)}."
            )

        if color is not None and not isinstance(color, str):
            raise ValueError(
                f"Invalid color parameter in add_marker() method. Color must be a string but instead got {color}, "
                f"which is a type {type(color)}."
            )

        if size is not None and not isinstance(size, int):
            raise ValueError(
                f"Invalid size parameter in add_marker() method. Size must be an int but instead got {size}, "
                f"which is a type {type(size)}."
            )

        if detail_text is not None and not isinstance(detail_text, str):
            raise ValueError(
                f"Invalid detail_text parameter in add_marker() method. Detail_text must be a string but instead "
                f"got {detail_text}, which is a type {type(detail_text)}."
            )

        if dt is not None and not isinstance(dt, (datetime.datetime, pd.Timestamp)):
            raise ValueError(
                f"Invalid dt parameter in add_marker() method. Dt must be a datetime.datetime but instead got {dt}, "
                f"which is a type {type(dt)}."
            )

        # If no datetime is specified, use the current datetime
        if dt is None:
            dt = self.get_datetime()

        # If no value is specified, use the current portfolio value
        if value is None:
            value = self.get_portfolio_value()

        # Check for duplicate markers
        if len(self._chart_markers_list) > 0:
            timestamp = dt.timestamp()
            for marker in self._chart_markers_list:
                if (
                        marker["timestamp"] == timestamp
                        and marker["name"] == name
                        and marker["symbol"] == symbol
                        and marker['plot_name'] == plot_name
                ):
                    return None

        new_marker = {
            "datetime": dt,
            "timestamp": dt.timestamp(),  # This is to speed up the process of finding duplicate markers
            "name": name,
            "symbol": symbol,
            "color": color,
            "size": size,
            "value": value,
            "detail_text": detail_text,
            "plot_name": plot_name,
        }

        self._chart_markers_list.append(new_marker)

        return new_marker

    def get_markers_df(self):
        """Returns the markers on the indicator chart as a pandas DataFrame.

        Returns
        -------
        pandas.DataFrame
            The markers on the indicator chart.
        """

        df = pd.DataFrame(self._chart_markers_list)

        return df

    def add_line(
            self,
            name: str,
            value: float,
            color: str = None,
            style: str = "solid",
            width: int = None,
            detail_text: str = None,
            dt: Union[datetime.datetime, pd.Timestamp] = None,
            plot_name: str = "default_plot"
            ):
        """Adds a line data point to the indicator chart. This can be used to add lines such as bollinger bands, prices for specific assets, or any other line you want to add to the chart.

        Parameters
        ----------
        name : str
            The name of the line. This is used to display the name on the graph. Eg. "Overbought", "Oversold", "Stop Loss", "Take Profit", ...
        value : float or int
            The value of the line.
        color : str
            The color of the line. Possible values are "red", "green", "blue", "yellow", "orange", "purple", "pink", "brown", "black", "white", "gray", "lightgray", "darkgray", "lightblue", "darkblue", "lightgreen", "darkgreen", "lightred", "darkred" and any hex color code.
        style : str
            The style of the line. Possible values are "solid", "dotted", and "dashed".
        width : int
            The width of the line.
        detail_text : str
            The text to display when the line is hovered over.
        dt : datetime.datetime or pandas.Timestamp
            The datetime of the line. Default is the current datetime.
        plot_name : str
            The name of the subplot to add the line to. If "default_plot" (the default value) or None, the line will be added to the main plot.

        Example
        -------
        >>> # Will add a line to the chart
        >>> self.add_chart_line("Overbought", value=80, color="red", style="dotted", width=2)
        """

        # Check that the parameters are valid
        if not isinstance(name, str):
            raise ValueError(
                f"Invalid name parameter in add_line() method. Name must be a string but instead got {name}, "
                f"which is a type {type(name)}."
            )

        if not isinstance(value, (float, int, np.float64)):
            raise ValueError(
                f"Invalid value parameter in add_line() method. Value must be a float or int but instead got {value}, "
                f"which is a type {type(value)}."
            )

        if color is not None and not isinstance(color, str):
            raise ValueError(
                f"Invalid color parameter in add_line() method. Color must be a string but instead got {color}, "
                f"which is a type {type(color)}."
            )

        if not isinstance(style, str):
            raise ValueError(
                f"Invalid style parameter in add_line() method. Style must be a string but instead got {style}, "
                f"which is a type {type(style)}."
            )

        if width is not None and not isinstance(width, int):
            raise ValueError(
                f"Invalid width parameter in add_line() method. Width must be an int but instead got {width}, "
                f"which is a type {type(width)}."
            )

        if detail_text is not None and not isinstance(detail_text, str):
            raise ValueError(
                f"Invalid detail_text parameter in add_line() method. Detail_text must be a string but instead got "
                f"{detail_text}, which is a type {type(detail_text)}."
            )

        if dt is not None and not isinstance(dt, (datetime.datetime, pd.Timestamp)):
            raise ValueError(
                f"Invalid dt parameter in add_line() method. Dt must be a datetime.datetime but instead got {dt}, "
                f"which is a type {type(dt)}."
            )

        # If no datetime is specified, use the current datetime
        if dt is None:
            dt = self.get_datetime()

        # Whenever you want to add a new line, use the following code
        self._chart_lines_list.append(
            {
                "datetime": dt,
                "name": name,
                "value": value,
                "color": color,
                "style": style,
                "width": width,
                "detail_text": detail_text,
                "plot_name": plot_name,
            }
        )

    def get_lines_df(self):
        """Returns a dataframe of the lines on the indicator chart.

        Returns
        -------
        pandas.DataFrame
            The lines on the indicator chart.
        """

        df = pd.DataFrame(self._chart_lines_list)

        return df

    def write_backtest_settings(self, settings_file: str):
        """Writes the backtest settings to a file.

        Parameters
        ----------
        settings_file : str
            The file path to write the settings to.

        Returns
        -------
        None

        Example
        -------
        >>> # Will write the backtest settings to a file
        >>> self.write_backtest_settings("backtest_settings.json")

        """
        datasource = self.broker.data_source
        auto_adjust = datasource.auto_adjust if hasattr(datasource, "auto_adjust") else False
        settings = {
            "name": self.name,
            "backtesting_start": str(self.backtesting_start),
            "backtesting_end": str(self.backtesting_end),
            "budget": self.initial_budget,
            "risk_free_rate": float(self.risk_free_rate),
            "minutes_before_closing": self.minutes_before_closing,
            "minutes_before_opening": self.minutes_before_opening,
            "sleeptime": self.sleeptime,
            "auto_adjust": auto_adjust,
            "quote_asset": self.quote_asset,
            "benchmark_asset": self._benchmark_asset,
            "starting_positions": self.starting_positions,
            "parameters": {k: v for k, v in self.parameters.items() if k != 'pandas_data'}
        }
        os.makedirs(os.path.dirname(settings_file), exist_ok=True)
        with open(settings_file, "w") as outfile:
            json = jsonpickle.encode(settings)
            outfile.write(json)

    def get_historical_prices(
        self,
        asset: Union[Asset, str],
        length: int,
        timestep: str = "",
        timeshift: datetime.timedelta = None,
        quote: Asset = None,
        exchange: str = None,
        include_after_hours: bool = True,
    ):
        """Get historical pricing data for a given symbol or asset.

        Return data bars for a given symbol or asset.  Any number of bars can
        be return limited by the data available. This is set with 'length' in
        number of bars. Bars may be returned as daily or by minute. And the
        starting point can be shifted backwards by time or bars.

        Parameters
        ----------
        asset : str or Asset
            The symbol string representation (e.g. AAPL, GOOG, ...) or asset
            object. Cryptocurrencies must also specify the quote currency.
        length : int
            The number of rows (number of timesteps)
        timestep : str
            Either ``"minute"`` for minutes data or ``"day"``
            for days data default value depends on the data_source (minute
            for alpaca, day for yahoo, ...).  If you need, you can specify the width of the bars by adding a number
            before the timestep (e.g. "5 minutes", "15 minutes", "1 day", "2 weeks", "1month", ...)
        timeshift : timedelta
            ``None`` by default. If specified indicates the time shift from
            the present. If  backtesting in Pandas, use integer representing
            number of bars.
        quote : Asset
            The quote currency for crypto currencies (e.g. USD, USDT, EUR, ...).
            Default is the quote asset for the strategy.
        exchange : str
            The exchange to pull the historical data from. Default is None (decided based on the broker)
        include_after_hours : bool
            Whether to include after hours data. Default is True. Currently only works with Interactive Brokers.

        Returns
        -------
        Bars
            The bars object with all the historical pricing data. Please check the ``Entities.Bars``
            object documentation for more details on how to use Bars objects. To get a ``DataFrame``
            from the Bars object, use ``bars.df``.

        Example
        -------
        Extract 2 rows of SPY data with one day timestep between each row
        with the latest data being 24h ago (timedelta(days=1)) (in a backtest)

        >>> # Get the data for SPY for the last 2 days
        >>> bars =  self.get_historical_prices("SPY", 2, "day")
        >>> # To get the DataFrame of SPY data
        >>> df = bars.df
        >>>
        >>> # Then, to get the DataFrame of SPY data
        >>> df = bars.df
        >>> last_ohlc = df.iloc[-1] # Get the last row of the DataFrame (the most recent pricing data we have)
        >>> self.log_message(f"Last price of BTC in USD: {last_ohlc['close']}, and the open price was {last_ohlc['open']}")

        >>> # Get the data for AAPL for the last 30 minutes
        >>> bars =  self.get_historical_prices("AAPL", 30, "minute")
        >>>
        >>> # Then, to get the DataFrame of SPY data
        >>> df = bars.df
        >>> last_ohlc = df.iloc[-1] # Get the last row of the DataFrame (the most recent pricing data we have)
        >>> self.log_message(f"Last price of BTC in USD: {last_ohlc['close']}, and the open price was {last_ohlc['open']}")

        >>> # Get the historical data for an AAPL option for the last 30 minutes
        >>> asset = self.create_asset("AAPL", asset_type="option", expiration=datetime.datetime(2020, 1, 1), strike=100, right="call")
        >>> bars =  self.get_historical_prices(asset, 30, "minute")
        >>>
        >>> # Then, to get the DataFrame of SPY data
        >>> df = bars.df
        >>> last_ohlc = df.iloc[-1] # Get the last row of the DataFrame (the most recent pricing data we have)
        >>> self.log_message(f"Last price of BTC in USD: {last_ohlc['close']}, and the open price was {last_ohlc['open']}")


        >>> # Get the data for BTC in USD  for the last 2 days
        >>> asset_base = self.create(symbol="BTC", asset_type="crypto"),
        >>> asset_quote = self.create(symbol="USDT", asset_type="crypto"),
        >>>
        >>> bars =  self.get_historical_prices(asset_base, 2, "day", quote=asset_quote)
        >>>
        >>> # Then, to get the DataFrame of SPY data
        >>> df = bars.df
        >>> last_ohlc = df.iloc[-1] # Get the last row of the DataFrame (the most recent pricing data we have)
        >>> self.log_message(f"Last price of BTC in USD: {last_ohlc['close']}, and the open price was {last_ohlc['open']}")
        """

        # Get that length is type int and if not try to cast it
        if not isinstance(length, int):
            try:
                length = int(length)
            except Exception as e:
                raise ValueError(
                    f"Invalid length parameter in get_historical_prices() method. Length must be an int but instead got {length}, "
                    f"which is a type {type(length)}."
                )

        if quote is None:
            quote = self.quote_asset

        # Only log once per asset to reduce noise
        asset_key = f"{asset}_{length}_{timestep}"
        if asset_key not in self._logged_get_historical_prices_assets:
            self.logger.info(f"Getting historical prices for {asset}, {length} bars, {timestep}")
            self._logged_get_historical_prices_assets.add(asset_key)

        asset = self._sanitize_user_asset(asset)

        asset = self.crypto_assets_to_tuple(asset, quote)
        if not timestep:
            timestep = self.broker.data_source.get_timestep()
        if self.broker.option_source and asset.asset_type == "option":
            return self.broker.option_source.get_historical_prices(
                asset,
                length,
                timestep=timestep,
                timeshift=timeshift,
                exchange=exchange,
                include_after_hours=include_after_hours,
                quote=quote,
            )
        else:
            return self.broker.data_source.get_historical_prices(
                asset,
                length,
                timestep=timestep,
                timeshift=timeshift,
                exchange=exchange,
                include_after_hours=include_after_hours,
                quote=quote,
            )

    def get_symbol_bars(
        self,
        asset: Union[Asset, str],
        length: int,
        timestep: str = "",
        timeshift: datetime.timedelta = None,
        quote: Asset = None,
        exchange: str = None,
    ):
        """
        This method is deprecated and will be removed in a future version.
        Please use self.get_historical_prices() instead.
        """
        logger.warning(
            "The get_bars method is deprecated and will be removed in a future version. "
            "Please use self.get_historical_prices() instead."
        )

        return self.get_historical_prices(
            asset,
            length,
            timestep=timestep,
            timeshift=timeshift,
            quote=quote,
            exchange=exchange,
        )

    def get_historical_prices_for_assets(
        self,
        assets: List[Asset | str | tuple],
        length: int,
        timestep: str = "minute",
        timeshift: datetime.timedelta = None,
        chunk_size: int = 100,
        max_workers: int = 200,
        exchange: str = None,
        include_after_hours: bool = True,
    ):
        """Get historical pricing data for the list of assets.

        Return data bars for a list of symbols or assets.  Return a dictionary
        of bars for a given list of symbols. Works the same as get_historical_prices
        but take as first parameter a list of assets. Any number of bars can
        be return limited by the data available. This is set with `length` in
        number of bars. Bars may be returned as daily or by minute. And the
        starting point can be shifted backwards by time or bars.

        Parameters
        ----------
        assets : list(str/asset,tuple)
            The symbol string representation (e.g. AAPL, GOOG, ...) or asset
            objects.
            Cryptocurrencies must specify the quote asset. Use tuples with the two asset
            objects, base first, quote second. '(Asset(ETH), Asset(BTC))'
        length : int
            The number of rows (number of timesteps)
        timestep : str
            Either ``"minute"`` for minutes data or ``"day"``
            for days data default value depends on the data_source (minute
            for alpaca, day for yahoo, ...). If you need, you can specify the width of the bars by adding a number
            before the timestep (e.g. "5 minutes", "15 minutes", "1 day", "2 weeks", "1month", ...)
        timeshift : timedelta
            ``None`` by default. If specified indicates the time shift from
            the present. If  backtesting in Pandas, use integer representing
            number of bars.
        include_after_hours : bool
            ``True`` by default. If ``False``, only return bars that are during
            regular trading hours. If ``True``, return all bars. Currently only works for Interactive Brokers.

        Returns
        -------
        dictionary : Asset : bars
            Return a dictionary bars for a given list of symbols. Works the
            same as get_historical_prices take as first parameter a list of symbols.

        Example
        -------

        >>> # Get the data for SPY and TLT for the last 2 days
        >>> bars =  self.get_historical_prices_for_assets(["SPY", "TLT"], 2, "day")
        >>> for asset_bars in bars_list:
        >>>     self.log_message(asset_bars.df)

        >>> # Get the data for AAPL and GOOG for the last 30 minutes
        >>> bars =  self.get_historical_prices_for_assets(["AAPL", "GOOG"], 30, "minute")
        >>> for asset_bars in bars_list:
        >>>     self.log_message(asset_bars.df)

        >>> # Get the price data for EURUSD for the last 2 days
        >>> from lumibot.entities import Asset
        >>> asset_base = Asset(symbol="EUR", asset_type="forex")
        >>> asset_quote = Asset(symbol="USD", asset_type="forex")
        >>> bars =  self.get_historical_prices_for_assets(asset_base, 2, "day", quote=asset_quote)
        >>> df = bars.df
        """

        # Only log once per asset list to reduce noise
        assets_key = f"{assets}_{length}_{timestep}"
        if assets_key not in self._logged_get_historical_prices_assets:
            self.logger.info(f"Getting historical prices for {assets}, {length} bars, {timestep}")
            self._logged_get_historical_prices_assets.add(assets_key)

        assets = [self._sanitize_user_asset(asset) for asset in assets]
        return self.broker.data_source.get_bars(
            assets,
            length,
            timestep=timestep,
            timeshift=timeshift,
            chunk_size=chunk_size,
            max_workers=max_workers,
            exchange=exchange,
        )

    def get_bars(
        self,
        assets: List[Union[Asset, str]],
        length: int,
        timestep: str = "minute",
        timeshift: datetime.timedelta = None,
        chunk_size: int = 100,
        max_workers: int = 200,
        exchange: str = None,
    ):
        """
        This method is deprecated and will be removed in a future version.
        Please use self.get_historical_prices_for_assets() instead."""
        logger.warning(
            "The get_bars method is deprecated and will be removed in a future version. "
            "Please use self.get_historical_prices_for_assets() instead."
        )

        return self.get_historical_prices_for_assets(
            assets,
            length,
            timestep=timestep,
            timeshift=timeshift,
            chunk_size=chunk_size,
            max_workers=max_workers,
            exchange=exchange,
        )

    def start_realtime_bars(self, asset: Asset, keep_bars: int = 30):
        """Starts a real time stream of tickers for Interactive Broker
        only.

        This allows for real time data to stream to the strategy. Bars
        are fixed at every fix seconds.  They will arrive in the strategy
        in the form of a dataframe. The data returned will be:

        - datetime
        - open
        - high
        - low
        - close
        - volume
        - vwap
        - count (trade count)

        Parameters
        ----------
        asset : Asset object
            The asset to stream.

        keep_bars : int
            How many bars/rows to keep of data. If running for an
            extended period of time, it may be desirable to limit the
            size of the data kept.

        Returns
        -------
        None

        """
        self.broker._start_realtime_bars(asset=asset, keep_bars=keep_bars)

    def get_realtime_bars(self, asset: Asset):
        """Retrieve the real time bars as dataframe.

        Returns the current set of real time bars as a dataframe.
        The `datetime` will be in the index. The columns of the
        dataframe are:

        - open
        - high
        - low
        - close
        - volume
        - vwap
        - count (trade count)

        Parameters
        ----------
        asset : Asset object
            The asset that has a stream active.

        Returns
        -------
        dataframe : Pandas Dataframe.
            Dataframe containing the most recent pricing information
            for the asset. The data returned will be the `datetime` in
            the index and the following columns.

            - open
            - high
            - low
            - close
            - volume
            - vwap
            - count (trade count)

            The length of the dataframe will have been set the intial
            start of the real time bars.
        """
        rtb = self.broker._get_realtime_bars(asset)
        if rtb is not None:
            return pd.DataFrame(rtb).set_index("datetime")
        return rtb

    def cancel_realtime_bars(self, asset: Asset):
        """Cancels a stream of real time bars for a given asset.

        Cancels the real time bars for the given asset.

        Parameters
        ----------
        asset : Asset
            Asset object that has streaming data to cancel.

        Returns
        -------
        None

        Example
        -------
        >>> # Cancel the real time bars for SPY
        >>> asset = self.create_asset("SPY")
        >>> self.cancel_realtime_bars(asset)

        """
        self.broker._cancel_realtime_bars(asset)

    def get_yesterday_dividend(self, asset: Asset):
        """Get the dividend for the previous day.

        Parameters
        ----------
        asset : Asset object
            The asset to get the dividend for.

        Returns
        -------
        dividend : float
            The dividend amount.

        Example
        -------
        >>> # Get the dividend for SPY
        >>> asset = self.create_asset("SPY")
        >>> self.get_yesterday_dividend(asset)


        """
        asset = self._sanitize_user_asset(asset)
        return self.broker.data_source.get_yesterday_dividend(asset)

    def get_yesterday_dividends(self, assets: List[Asset]):
        """Get the dividends for the previous day.

        Parameters
        ----------
        assets : list(Asset object)
            The assets to get the dividends for.

        Returns
        -------
        dividends : list(float)
            The dividend amount for each asset.

        Example
        -------
        >>> # Get the dividends for SPY and TLT
        >>> from lumibot.entities import Asset
        >>> assets = [Asset("SPY"), Asset("TLT")]
        >>> self.get_yesterday_dividends(assets)

        """
        assets = [self._sanitize_user_asset(asset) for asset in assets]
        if self.broker and self.broker.data_source:
            return self.broker.data_source.get_yesterday_dividends(assets, quote=self.quote_asset)
        else:
            self.log_message("Broker or data source is not available.")
            return None

    def update_parameters(self, parameters: dict):
        """Update the parameters of the strategy.

        Parameters
        ----------
        parameters : dict
            The parameters to update.

        Returns
        -------
        None
        """
        for key, value in parameters.items():
            self.parameters[key] = value

        self.on_parameters_updated(parameters)

    def get_parameters(self):
        """Get the parameters of the strategy.

        Returns
        -------
        parameters : dict
            The parameters of the strategy.
        """
        return self.parameters

    def set_parameters(self, parameters: dict):
        """Set the default parameters of the strategy.

        Parameters
        ----------
        parameters : dict
            The parameters to set. These new parameters will overwrite
            the existing parameters (including the default settings).

        Returns
        -------
        None
        """
        if parameters is None:
            return None

        for key, value in parameters.items():
            self.parameters[key] = value

        self.on_parameters_updated(parameters)

        return self.parameters

    def set_parameter_defaults(self, parameters: dict):
        """Set the default parameters of the strategy.

        Parameters
        ----------
        parameters : dict
            The parameters to set defaults for. This will not overwrite
            existing parameters if they have already been set.

        Returns
        -------
        None
        """
        if parameters is None:
            return None

        for key, value in parameters.items():
            if key not in self.parameters:
                self.parameters[key] = value

        return self.parameters

    # ======= Lifecycle Methods ====================

    def initialize(self, parameters: dict = None):
        """Initialize the strategy. Use this lifecycle method to initialize parameters.

        This method is called once before the first time the strategy is run.

        Returns
        -------
        None

        Example
        -------
        >>> # Initialize the strategy
        >>> def initialize(self):
        >>>   self.sleeptime = 5
        >>>   self.ticker = "AAPL"
        >>>   self.minutes_before_closing = 5
        >>>   self.max_bars = 100

        >>> # Initialize the strategy
        >>> def initialize(self):
        >>>   # Set the strategy to call on_trading_interation every 2 seconds
        >>>   self.sleeptime = "2S"
        >>>   self.count = 0

        >>> # Initialize the strategy
        >>> def initialize(self):
        >>>   # Set the strategy to call on_trading_interation every 10 minutes
        >>>   self.sleeptime = "10M"
        >>>   self.count = 0

        >>> # Initialize the strategy
        >>> def initialize(self):
        >>>   # Set the strategy to call on_trading_interation every 20 hours
        >>>   self.sleeptime = "20H"
        >>>   self.count = 0

        >>> # Initialize the strategy
        >>> def initialize(self):
        >>>   # Set the strategy to call on_trading_interation every 2 days (48 hours)
        >>>   self.sleeptime = "2D"
        >>>   self.count = 0


        """
        pass

    def before_market_opens(self):
        """Use this lifecycle method to execute code
        self.minutes_before_opening minutes before opening.

        Returns
        -------
        None

        Example
        -------
        >>> # Get the data for SPY and TLT for the last 2 days
        >>> def before_market_opens(self):
        >>>     bars_list =  self.get_historical_prices_for_assets(["SPY", "TLT"], 2, "day")
        >>>     for asset_bars in bars_list:
        >>>         self.log_message(asset_bars.df)

        >
        """
        pass

    def before_starting_trading(self):
        """Lifecycle method executed after the market opens
        and before entering the trading loop. Use this method
        for daily resetting variables

        Returns
        -------
        None

        Example
        -------
        >>> # Get pricing data for the last day
        >>> def before_starting_trading(self):
        >>>     self.get_historical_prices("SPY", 1, "day")


        """
        pass

    def on_trading_iteration(self):
        """Use this lifecycle method for your main trading loop. This method is called every self.sleeptime minutes (or seconds/hours/days if self.sleeptime is "30S", "1H", "1D", etc.).

        Example
        -------
        >>> def on_trading_iteration(self):
        >>>     self.log_message("Hello")
        >>>     order = self.create_order("SPY", 10, "buy")
        >>>     self.submit_order(order)
        """
        pass

    def trace_stats(self, context: dict, snapshot_before: dict):
        """Lifecycle method that will be executed after
        on_trading_iteration. context is a dictionary containing
        on_trading_iteration locals() in last call. Use this
        method to dump stats

        Parameters
        ----------
        context : dict
            Dictionary containing locals() from current call to on_trading_iteration method.

        snapshot_before : dict
            Dictionary containing locals() from last call to on_trading_iteration method.

        Returns
        -------
        dict
            Dictionary containing the stats to be logged.

        Example
        -------
        >>> def trace_stats(self, context, snapshot_before):
        >>>     self.log_message("Trace stats")
        >>>     self.log_message(f"Context: {context}")
        >>>     self.log_message(f"Snapshot before: {snapshot_before}")
        >>>     return {
        >>>         "my_stat": context["my_stat"],
        >>>         "my_other_stat": context["my_other_stat"],
        >>>         "portfolio_value": self.portfolio_value,
        >>>         "cash": self.cash,
        >>>     }
        """
        return {}

    def before_market_closes(self):
        """Use this lifecycle method to execute code before the market closes. You can use self.minutes_before_closing to set the number of minutes before closing

        Parameters
        ----------
        None

        Returns
        -------
        None

        Example
        -------
        >>> # Execute code before market closes
        >>> def before_market_closes(self):
        >>>     self.sell_all()
        """
        pass

    def after_market_closes(self):
        """Use this lifecycle method to execute code
        after market closes. For example dumping stats/reports. This method is called after the last on_trading_iteration.

        Parameters
        ----------
        None

        Returns
        -------
        None

        Example
        -------
        >>> # Dump stats
        >>> def after_market_closes(self):
        >>>     self.log_message("The market is closed")
        >>>     self.log_message(f"The total value of our portfolio is {self.portfolio_value}")
        >>>     self.log_message(f"The amount of cash we have is {self.cash})
        """
        pass

    def on_strategy_end(self):
        """Use this lifecycle method to execute code
        when strategy reached its end. Used to dump
        statistics when backtesting finishes

        Parameters
        ----------
        None

        Returns
        -------
        None

        Example
        -------
        >>> # Log end of strategy
        >>> def on_strategy_end(self):
        >>>     self.log_message("The strategy is complete")
        """
        pass

    # ====== Events Methods ========================

    def on_bot_crash(self, error: Exception):
        """Use this lifecycle event to execute code
        when an exception is raised and the bot crashes

        Parameters
        ----------
        error : Exception
            The exception that was raised.

        Returns
        -------
        None

        Example
        -------
        >>> def on_bot_crash(self, error):
        >>>     self.log_message(error)

        >>> # Sell all assets on crash
        >>> def on_bot_crash(self, error):
        >>>     self.sell_all()

        """
        self.on_abrupt_closing()

    def on_abrupt_closing(self):
        """Use this lifecycle event to execute code
        when the main trader was shut down (Keybord Interuption)

        Parameters
        ----------
        None

        Returns
        -------
        None

        Example
        -------
        >>> def on_abrupt_closing(self):
        >>>     self.log_message("Abrupt closing")
        >>>     self.sell_all()
        """
        pass

    def on_new_order(self, order: Order):
        """Use this lifecycle event to execute code
        when a new order is being processed by the broker

        Parameters
        ----------
        order : Order object
            The order that is being processed.

        Returns
        -------
        None

        Example
        -------
        >>> def on_new_order(self, order):
        >>>     if order.asset == "AAPL":
        >>>         self.log_message("Order for AAPL")
        """
        pass

    def on_canceled_order(self, order: Order):
        """Use this lifecycle event to execute code when an order is canceled.

        Parameters
        ----------
        order : Order object
            The order that is being canceled.

        Returns
        -------
        None

        Example
        -------
        >>> def on_canceled_order(self, order):
        >>>     if order.asset == "AAPL":
        >>>         self.log_message("Order for AAPL canceled")
        """
        pass

    def on_partially_filled_order(
            self,
            position: Position,
            order: Order,
            price: float,
            quantity: Union[float, int],
            multiplier: float
            ):
        """Use this lifecycle event to execute code
        when an order has been partially filled by the broker

        Parameters
        ----------
        position : Position object
            The position that is being filled.

        order : Order object
            The order that is being filled.

        price : float
            The price of the fill.

        quantity : float or int
            The quantity of the fill.

        multiplier : float
            The multiplier of the fill.

        Returns
        -------
        None

        Example
        -------
        >>> def on_partially_filled_order(self, position, order, price, quantity, multiplier):
        >>>     if order.asset == "AAPL":
        >>>         self.log_message(f"{quantity} shares of AAPL partially filled")
        >>>         self.log_message(f"Price: {price}")
        """
        pass

    def on_filled_order(
            self,
            position: Position,
            order: Order,
            price: float,
            quantity: Union[float, int],
            multiplier: float
            ):
        """Use this lifecycle event to execute code when an order has been filled by the broker.

        Parameters
        ----------
        position : Position object
            The position that is being filled.

        order : Order object
            The order that is being filled.

        price : float
            The price of the fill.

        quantity : float or int
            The quantity of the fill.

        multiplier : float
            The multiplier of the fill.

        Returns
        -------
        None

        Example
        -------
        >>> def on_filled_order(self, position, order, price, quantity, multiplier):
        >>>     if order.asset == "AAPL":
        >>>         self.log_message("Order for AAPL filled")
        >>>         self.log_message(f"Price: {price}")

        >>> # Update dictionary with new position
        >>> def on_filled_order(self, position, order, price, quantity, multiplier):
        >>>     if order.asset == "AAPL":
        >>>         self.log_message("Order for AAPL filled")
        >>>         self.log_message(f"Price: {price}")
        >>>         self.positions["AAPL"] = position
        """
        pass

    def on_parameters_updated(self, parameters: dict):
        """Use this lifecycle event to execute code when the parameters are updated.

        Parameters
        ----------
        parameters : dict
            The parameters that are being updated.

        Returns
        -------
        None

        Example
        -------
        >>> def on_parameters_updated(self, parameters):
        >>>     self.log_message(f"Parameters updated: {parameters}")
        """
        pass

    def run_live(self):
        """
        Executes the trading strategy in Live mode

        Returns:
            None
        """
        trader = Trader()

        trader.add_strategy(self)
        trader.run_all()

    @classmethod
    def backtest(
        self,
        datasource_class: Type[DataSource],
        backtesting_start: datetime.datetime = None,
        backtesting_end: datetime.datetime = None,
        minutes_before_closing: int = 1,
        minutes_before_opening: int = 60,
        sleeptime: int = 1,
        stats_file: str = None,
        risk_free_rate: float = None,
        logfile: str = None,
        config: dict = None,
        auto_adjust: bool = False,
        name: str = None,
        budget: float = None,
        benchmark_asset: Union[str, Asset] = "SPY",
        plot_file_html: str = None,
        trades_file: str = None,
        settings_file: str = None,
        pandas_data: List[Data] = None,
        quote_asset: Asset = Asset(symbol="USD", asset_type="forex"),
        starting_positions: dict = None,
        show_plot: bool = True,
        tearsheet_file: str = None,
        save_tearsheet: bool = True,
        show_tearsheet: bool = True,
        parameters: dict = {},
        buy_trading_fees: List[TradingFee] = [],
        sell_trading_fees: List[TradingFee] = [],
        polygon_api_key: str = None,
        indicators_file: str = None,
        show_indicators: bool = True,
        save_logfile: bool = False,
        thetadata_username: str = None,
        thetadata_password: str = None,
        use_quote_data: bool = False,
        show_progress_bar: bool = True,
        quiet_logs: bool = True,
        trader_class: Type[Trader] = Trader,
        save_stats_file: bool = True,
        **kwargs,
    ):
        """Backtest a strategy.

        Parameters
        ----------
        datasource_class : class
            The datasource class to use. For example, if you want to use the yahoo finance datasource, then you
            would pass YahooDataBacktesting as the datasource_class.
        backtesting_start : datetime.datetime
            The start date of the backtesting period.
        backtesting_end : datetime.datetime
            The end date of the backtesting period.
        minutes_before_closing : int
            The number of minutes before closing that the minutes_before_closing strategy method will be called.
        minutes_before_opening : int
            The number of minutes before opening that the minutes_before_opening strategy method will be called.
        sleeptime : int
            The number of seconds to sleep between each iteration of the backtest.
        stats_file : str
            The file to write the stats to.
        risk_free_rate : float
            The risk free rate to use.
        logfile : str
            The file to write the log to.
        config : dict
            The config to use to set up the brokers in live trading.
        auto_adjust : bool
            Whether or not to automatically adjust the strategy.
        name : str
            The name of the strategy.
        budget : float
            The initial budget to use for the backtest.
        benchmark_asset : str or Asset
            The benchmark asset to use for the backtest to compare to. If it is a string then it will be converted
            to a stock Asset object.
        plot_file_html : str
            The file to write the plot html to.
        trades_file : str
            The file to write the trades to.
        settings_file : str
            The file to write the settings to.
        pandas_data : list of Data
            A list of Data objects that are used when the datasource_class object is set to PandasDataBacktesting.
            This contains all the data that will be used in backtesting.
        quote_asset : Asset (crypto)
            An Asset object for the crypto currency that will get used
            as a valuation asset for measuring overall porfolio values.
            Usually USDT, USD, USDC.
        starting_positions : dict
            A dictionary of starting positions for each asset. For example,
            if you want to start with $100 of SPY, and $200 of AAPL, then you
            would pass in starting_positions={'SPY': 100, 'AAPL': 200}.
        show_plot : bool
            Whether to show the plot.
        show_tearsheet : bool
            Whether to show the tearsheet.
        save_tearsheet : bool
            Whether to save the tearsheet.
        parameters : dict
            A dictionary of parameters to pass to the strategy. These parameters
            must be set up within the initialize() method.
        buy_trading_fees : list of TradingFee objects
            A list of TradingFee objects to apply to the buy orders during backtests.
        sell_trading_fees : list of TradingFee objects
            A list of TradingFee objects to apply to the sell orders during backtests.
        polygon_api_key : str
            The polygon api key to use for polygon data. Only required if you are using PolygonDataBacktesting as
            the datasource_class.
        indicators_file : str
            The file to write the indicators to.
        show_indicators : bool
            Whether to show the indicators plot.
        save_logfile : bool
            Whether to save the logs to a file. If True, the logs will be saved to the logs directory. Defaults to False.
            Turning on this option will slow down the backtest.
        thetadata_username : str
            The username to use for the ThetaDataBacktesting datasource. Only required if you are using ThetaDataBacktesting as the datasource_class.
        thetadata_password : str
            The password to use for the ThetaDataBacktesting datasource. Only required if you are using ThetaDataBacktesting as the datasource_class.
        use_quote_data : bool
            Whether to use quote data for the backtest. Defaults to False. If True, the backtest will use quote data for the backtest. (Currently this is specific to ThetaData)
            When set to true this requests Quote data in addition to OHLC which adds time to backtests.
        show_progress_bar : bool
            Whether to show the progress bar. Defaults to True.
        quiet_logs : bool
            Whether to quiet noisy logs by setting the log level to ERROR. Defaults to True.
        trader_class : Trader class
            The trader class to use. Defaults to Trader.

        Returns
        -------
        result : dict
            A dictionary of the backtest results. Eg.

        Examples
        --------

        >>> from datetime import datetime
        >>> from lumibot.backtesting import YahooDataBacktesting
        >>> from lumibot.strategies import Strategy
        >>>
        >>> # A simple strategy that buys AAPL on the first day
        >>> class MyStrategy(Strategy):
        >>>    def on_trading_iteration(self):
        >>>        if self.first_iteration:
        >>>            order = self.create_order("AAPL", quantity=1, side="buy")
        >>>            self.submit_order(order)
        >>>
        >>> # Create a backtest
        >>> backtesting_start = datetime(2018, 1, 1)
        >>> backtesting_end = datetime(2018, 1, 31)
        >>>
        >>> # The benchmark asset to use for the backtest to compare to
        >>> benchmark_asset = Asset(symbol="QQQ", asset_type="stock")
        >>>
        >>> backtest = MyStrategy.backtest(
        >>>     datasource_class=YahooDataBacktesting,
        >>>     backtesting_start=backtesting_start,
        >>>     backtesting_end=backtesting_end,
        >>>     benchmark_asset=benchmark_asset,
        >>> )
        """
        results, strategy = self.run_backtest(
            datasource_class=datasource_class,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            minutes_before_closing=minutes_before_closing,
            minutes_before_opening=minutes_before_opening,
            sleeptime=sleeptime,
            stats_file=stats_file,
            risk_free_rate=risk_free_rate,
            logfile=logfile,
            config=config,
            auto_adjust=auto_adjust,
            name=name,
            budget=budget,
            benchmark_asset=benchmark_asset,
            plot_file_html=plot_file_html,
            trades_file=trades_file,
            settings_file=settings_file,
            pandas_data=pandas_data,
            quote_asset=quote_asset,
            starting_positions=starting_positions,
            show_plot=show_plot,
            tearsheet_file=tearsheet_file,
            save_tearsheet=save_tearsheet,
            show_tearsheet=show_tearsheet,
            parameters=parameters,
            buy_trading_fees=buy_trading_fees,
            sell_trading_fees=sell_trading_fees,
            polygon_api_key=polygon_api_key,
            indicators_file=indicators_file,
            show_indicators=show_indicators,
            save_logfile=save_logfile,
            thetadata_username=thetadata_username,
            thetadata_password=thetadata_password,
            use_quote_data=use_quote_data,
            show_progress_bar=show_progress_bar,
            quiet_logs=quiet_logs,
            trader_class=trader_class,
            save_stats_file=save_stats_file,
            **kwargs,
        )
        return results
