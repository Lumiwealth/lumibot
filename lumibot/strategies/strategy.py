import datetime
import io
import os
import time
import uuid
from asyncio.log import logger
from decimal import Decimal
from typing import Union

import jsonpickle
import matplotlib
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import pandas as pd
import pandas_market_calendars as mcal
import pytz
import requests
from termcolor import colored

from lumibot.entities import Asset, Order
from lumibot.tools import get_risk_free_rate

from ._strategy import _Strategy

matplotlib.use("Agg")

# Set the stats table name for when storing stats in a database, defined by account_history_db_connection_str
STATS_TABLE_NAME = "strategy_tracker"

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

        Parameters
        ----------
        minutes_before_closing : int
            The number of minutes before market closes that the strategy will stop executing.

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
    def sleeptime(self):
        """Get or set the current sleep time for the strategy.

        Sleep time is the time the program will pause between executions of on_trading_iteration and trace_stats.
        This is used to control the speed of the program.

        By default, equals 1 minute. You can set the sleep time as an integer which will be interpreted as
        minutes. eg: sleeptime = 50 would be 50 minutes. Conversely, you can enter the time as a string with
        the duration numbers first, followed by the time units: ‘M’ for minutes, ‘S’ for seconds
        eg: ‘"300S"’ is 300 seconds, ‘"10M"’ is 10 minutes.

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
    def is_backtesting(self):
        """Returns True if the strategy is running in backtesting mode.

        Returns
        -------
        is_backtesting : bool
            True if the strategy is running in backtesting mode.

        Example
        -------
        >>> # Check if the strategy is running in backtesting mode
        >>> if self.is_backtesting:
        >>>     self.log_message("Running in backtesting mode")
        """
        return self._is_backtesting

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
    def risk_free_rate(self):
        # Get the current datetime
        now = self.get_datetime()

        # Use the yahoo data to get the risk free rate
        rfr = get_risk_free_rate(now)
        return rfr

    # ======= Helper Methods =======================

    def log_message(self, message, color=None, broadcast=False):
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
        if color is not None:
            colored_message = colored(message, color)
            self.logger.info(colored_message)
        else:
            self.logger.info(message)

        if broadcast:
            # Send the message to Discord
            self.send_discord_message(message)

        return message

    # ====== Order Methods ===============

    def create_order(
        self,
        asset,
        quantity,
        side,
        limit_price=None,
        stop_price=None,
        time_in_force="gtc",
        good_till_date=None,
        take_profit_price=None,
        stop_loss_price=None,
        stop_loss_limit_price=None,
        trail_price=None,
        trail_percent=None,
        position_filled=False,
        exchange=None,
        quote=None,
        pair=None,
        type=None,
        custom_params={},
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
        type : str
            The type of order. Order types include: ``'market'``, ``'limit'``, ``'stop'``, ``'stop_limit'``,
            ``trailing_stop``, ``'oco'``, ``'bracket'``, ``'oto'``.
            We will try to determine the order type if you do not specify it. It is mandatory to set the type
            for advanced order types such as ``'oco'``, ``'bracket'``, ``'oto'``.
        limit_price : float
            A Limit order is an order to buy or sell at a specified
            price or better. The Limit order ensures that if the
            order fills, it will not fill at a price less favorable
            than your limit price, but it does not guarantee a fill.
        stop_price : float
            A Stop order is an instruction to submit a buy or sell
            market order if and when the user-specified stop trigger
            price is attained or penetrated.
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
            on the stock price movement. E.g. 0.05 would be a 5% trailing stop.
            `trail_percent` sets the trailing price in percent.
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
        >>> order = self.create_order("SPY", 100, "buy", limit_price=100.00, stop_price=100.00)
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

        >>> # For an OCO order
        >>> order = self.create_order(
        >>>                "SPY",
        >>>                100,
        >>>                "sell",
        >>>                take_profit_price=limit,
        >>>                stop_loss_price=stop_loss,
        >>>                type="oco",
        >>>            )

        >>> # For a bracket order
        >>> order = self.create_order(
        >>>                "SPY",
        >>>                100,
        >>>                "sell",
        >>>                take_profit_price=limit,
        >>>                stop_loss_price=stop_loss,
        >>>                stop_loss_limit_price=stop_loss_limit,
        >>>                type="bracket",
        >>>            )

        >>> # For a bracket order with a trailing stop
        >>> order = self.create_order(
        >>>                "SPY",
        >>>                100,
        >>>                "sell",
        >>>                trail_percent=trail_percent,
        >>>                take_profit_price=limit,
        >>>                stop_loss_price=stop_loss,
        >>>                stop_loss_limit_price=stop_loss_limit,
        >>>                type="bracket",
        >>>            )

        >>> # For an OTO order
        >>> order = self.create_order(
        >>>                "SPY",
        >>>                100,
        >>>                "sell",
        >>>                take_profit_price=limit,
        >>>                stop_loss_price=stop_loss,
        >>>                type="oto",
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
        >>>                trail_percent=trail_percent,
        >>>                limit_price=limit,
        >>>                stop_price=stop_loss,
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
        >>>                trail_percent=trail_percent,
        >>>                limit_price=limit,
        >>>                stop_price=stop_loss,
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
        >>>                trail_percent=trail_percent,
        >>>                limit_price=limit,
        >>>                stop_price=stop_loss,
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
            time_in_force=time_in_force,
            good_till_date=good_till_date,
            take_profit_price=take_profit_price,
            stop_loss_price=stop_loss_price,
            stop_loss_limit_price=stop_loss_limit_price,
            trail_price=trail_price,
            trail_percent=trail_percent,
            exchange=exchange,
            position_filled=position_filled,
            date_created=self.get_datetime(),
            quote=quote,
            pair=pair,
            type=type,
            custom_params=custom_params,
        )
        return order

    # ======= Broker Methods ============

    def sleep(self, sleeptime):
        """Sleep for sleeptime seconds.

        Use to pause the execution of the program. This should be used instead of `time.sleep` within the strategy.

        Parameters
        ----------
        sleeptime : float
            Time in seconds the program will be paused.

        Returns
        -------
        None

        Example
        -------
        >>> # Sleep for 5 seconds
        >>> self.sleep(5)
        """
        if not self.is_backtesting:
            # Sleep for the the sleeptime in seconds.
            time.sleep(sleeptime)

        return self.broker.sleep(sleeptime)

    def get_selling_order(self, position):
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

    def set_market(self, market):
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

        self.broker.market = market

    def await_market_to_open(self, timedelta=None):
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

    def await_market_to_close(self, timedelta=None):
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
    def crypto_assets_to_tuple(base, quote):
        """Check for crypto quote, convert to tuple"""
        if isinstance(base, Asset) and base.asset_type == "crypto" and isinstance(quote, Asset):
            return (base, quote)
        return base

    def get_tracked_position(self, asset):
        """Deprecated, will be removed in the future. Please use `get_position()` instead."""

        self.log_message("Warning: get_tracked_position() is deprecated, please use get_position() instead.")
        self.get_position(asset)

    def get_position(self, asset):
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
        return self.portfolio_value

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

    def get_positions(self):
        """Get all positions for the account.

        Parameters
        ----------
        None

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

        return self.broker.get_tracked_positions(self.name)

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
        return self.get_tracked_positions()

    def _get_contract_details(self, asset):
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

    def get_tracked_order(self, identifier):
        """Deprecated, will be removed in the future. Please use `get_order()` instead."""

        self.log_message("Warning: get_tracked_order() is deprecated, please use get_order() instead.")
        return self.get_order(identifier)

    def get_order(self, identifier):
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

    def get_tracked_orders(self, identifier):
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

    def get_asset_potential_total(self, asset):
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

    def submit_order(self, order):
        """Submit an order for an asset

        Submits an order object for processing by the active broker.

        Parameters
        ---------
        order : Order object
            Order object containing the asset and instructions for
            executing the order.

        Returns
        -------
        Order object
            Processed order object.

        Example
        -------
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

        >>> # For a limit sell order
        >>> order = self.create_order("SPY", 100, "sell", limit_price=100.00)
        >>> self.submit_order(order)

        >>> # For buying a future
        >>> from lumibot.entities import Asset
        >>>
        >>> asset = Asset(
        >>>    "ES",
        >>>    asset_type=Asset.AssetType.FUTURE,
        >>>    expiration_date="2020-01-01",
        >>>    multiplier=100)
        >>> order = self.create_order(asset, 100, "buy")
        >>> self.submit_order(order)

        >>> # For selling a future
        >>> from lumibot.entities import Asset
        >>>
        >>> asset = Asset(
        >>>    "ES",
        >>>    asset_type=Asset.AssetType.FUTURE,
        >>>    expiration_date="2020-01-01"
        >>>    multiplier=100)
        >>> order = self.create_order(asset, 100, "sell")
        >>> self.submit_order(order)

        >>> # For buying an option
        >>> from lumibot.entities import Asset
        >>>
        >>> asset = Asset(
        >>>    "SPY",
        >>>    asset_type=Asset.AssetType.OPTION,
        >>>    expiration_date="2020-01-01",
        >>>    strike_price=100.00,
        >>>    right="call")
        >>> order = self.create_order(asset, 10, "buy")
        >>> self.submit_order(order)

        >>> # For selling an option
        >>> from lumibot.entities import Asset
        >>>
        >>> asset = Asset(
        >>>    "SPY",
        >>>    asset_type=Asset.AssetType.OPTION,
        >>>    expiration_date="2020-01-01",
        >>>    strike_price=100.00,
        >>>    right="call")
        >>> order = self.create_order(asset, 10, "sell")
        >>> self.submit_order(order)

        >>> # For buying a stock
        >>> asset = Asset("SPY")
        >>> order = self.create_order(asset, 10, "buy")
        >>> self.submit_order(order)

        >>> # For selling a stock
        >>> asset = Asset("SPY")
        >>> order = self.create_order(asset, 10, "sell")
        >>> self.submit_order(order)

        >>> # For buying a stock with a limit price
        >>> asset = Asset("SPY")
        >>> order = self.create_order(asset, 10, "buy", limit_price=100.00)
        >>> self.submit_order(order)

        >>> # For selling a stock with a limit price
        >>> asset = Asset("SPY")
        >>> order = self.create_order(asset, 10, "sell", limit_price=100.00)
        >>> self.submit_order(order)

        >>> # For buying a stock with a stop price
        >>> asset = Asset("SPY")
        >>> order = self.create_order(asset, 10, "buy", stop_price=100.00)
        >>> self.submit_order(order)

        >>> # For buying FOREX
        >>> from lumibot.entities import Asset
        >>>
        >>> base_asset = Asset(
            symbol="GBP,
            asset_type=Asset.AssetType.FOREX,
        )
        >>> quote_asset = Asset(
            symbol="USD",
            asset_type=Asset.AssetType.FOREX,
        )
        >>> order = self.create_order(asset, 10, "buy", quote=quote_asset)
        >>> self.submit_order(order)

        >>> # For selling FOREX
        >>> from lumibot.entities import Asset
        >>>
        >>> base_asset = Asset(
            symbol="EUR",
            asset_type=Asset.AssetType.FOREX,
        )
        >>> quote_asset = Asset(
            symbol="USD",
            asset_type=Asset.AssetType.FOREX,
        )
        >>> order = self.create_order(asset, 10, "sell", quote=quote_asset)
        >>> self.submit_order(order)

        >>> # For buying an option with a limit price
        >>> from lumibot.entities import Asset
        >>>
        >>> asset = Asset(
        >>>    "SPY",
        >>>    asset_type=Aset.AssetType.OPTION,
        >>>    expiration_date="2020-01-01",
        >>>    strike_price=100.00,
        >>>    right="call")
        >>> order = self.create_order(asset, 10, "buy", limit_price=100.00)
        >>> self.submit_order(order)

        >>> # For selling an option with a limit price
        >>> from lumibot.entities import Asset
        >>>
        >>> asset = Asset(
        >>>    "SPY",
        >>>    asset_type=Asset.AssetType.OPTION,
        >>>    expiration_date="2020-01-01",
        >>>    strike_price=100.00,
        >>>    right="call")
        >>> order = self.create_order(asset, 10, "sell", limit_price=100.00)
        >>> self.submit_order(order)

        >>> # For buying an option with a stop price
        >>> from lumibot.entities import Asset
        >>>
        >>> asset = Asset(
        >>>    "SPY",
        >>>    asset_type=Asset.AssetType.OPTION,
        >>>    expiration_date="2020-01-01",
        >>>    strike_price=100.00,
        >>>    right="call")
        >>> order = self.create_order(asset, 10, "buy", stop_price=100.00)

        >>> # For selling a stock with a stop price
        >>> asset = Asset("SPY")
        >>> order = self.create_order(asset, 10, "sell", stop_price=100.00)
        >>> self.submit_order(order)

        >>> # For buying a stock with a trailing stop price
        >>> asset = Asset("SPY")
        >>> order = self.create_order(asset, 10, "buy", trailing_stop_price=100.00)
        >>> self.submit_order(order)

        >>> # For selling a stock with a trailing stop price
        >>> asset = Asset("SPY")
        >>> order = self.create_order(asset, 10, "sell", trailing_stop_price=100.00)
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
        >>> or...
        >>> order = self.create_order((asset_base, asset_quote), 0.1, "buy")
        >>> self.submit_order(order)

        >>> # For buying a crypto with a limit price
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
        >>> order = self.create_order(asset_base, 0.1, "buy", limit_price="41250", quote=asset_quote)
        >>> or...
        >>> order = self.create_order((asset_base, asset_quote), 0.1, "buy", limit_price="41250")
        >>> self.submit_order(order)

        >>> # For buying a crypto with a stop limit price
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
        >>> order = self.create_order(asset_base, 0.1, "buy", limit_price="41325", stop_price="41300", quote=asset_quote)
        >>> or...
        >>> order = self.create_order((asset_base, asset_quote), 0.1, "buy", limit_price="41325", stop_price="41300",)
        >>> self.submit_order(order)

        >>> # For an OCO order
        >>> order = self.create_order(
        >>>                "SPY",
        >>>                100,
        >>>                "sell",
        >>>                take_profit_price=limit,
        >>>                stop_loss_price=stop_loss,
        >>>                type="oco",
        >>>            )
        >>> self.submit_order(order)

        """

        if order is None:
            self.logger.error(
                "Cannot submit a None order, please check to make sure that you have actually created an order before submitting."
            )
            return

        return self.broker.submit_order(order)

    def submit_orders(self, orders):
        """Submit a list of orders

        Submits a list of orders for processing by the active broker.

        Parameters
        ---------
        orders : list of orders
            A list of order objects containing the asset and
            instructions for the orders.

        Returns
        -------
        list of orders
            List of processed order object.

        Example
        -------
        >>> # For 2 market buy orders
        >>> order1 = self.create_order("SPY", 100, "buy")
        >>> order2 = self.create_order("TLT", 200, "buy")
        >>> self.submit_orders([order1, order2])

        >>> # For 2 limit buy orders
        >>> order1 = self.create_order("SPY", 100, "buy", limit_price=100.00)
        >>> order2 = self.create_order("TLT", 200, "buy", limit_price=100.00)
        >>> self.submit_orders([order1, order2])

        >>> # For 2 stop loss orders
        >>> order1 = self.create_order("SPY", 100, "buy", stop_price=100.00)
        >>> order2 = self.create_order("TLT", 200, "buy", stop_price=100.00)
        >>> self.submit_orders([order1, order2])

        >>> # For 2 stop limit orders
        >>> order1 = self.create_order("SPY", 100, "buy", limit_price=100.00, stop_price=100.00)
        >>> order2 = self.create_order("TLT", 200, "buy", limit_price=100.00, stop_price=100.00)
        >>> self.submit_orders([order1, order2])

        >>> # For 2 market sell orders
        >>> order1 = self.create_order("SPY", 100, "sell")
        >>> order2 = self.create_order("TLT", 200, "sell")
        >>> self.submit_orders([order1, order2])

        >>> # For 2 limit sell orders
        >>> order1 = self.create_order("SPY", 100, "sell", limit_price=100.00)
        >>> order2 = self.create_order("TLT", 200, "sell", limit_price=100.00)
        >>> self.submit_orders([order1, order2])

        >>> # For 2 stop loss orders
        >>> order1 = self.create_order("SPY", 100, "sell", stop_price=100.00)
        >>> order2 = self.create_order("TLT", 200, "sell", stop_price=100.00)
        >>> self.submit_orders([order1, order2])

        >>> # For 2 stop limit orders
        >>> order1 = self.create_order("SPY", 100, "sell", limit_price=100.00, stop_price=100.00)
        >>> order2 = self.create_order("TLT", 200, "sell", limit_price=100.00, stop_price=100.00)
        >>> self.submit_orders([order1, order2])

        >>> # For 2 FOREX buy orders
        >>> from lumibot.entities import Asset
        >>>
        >>> base_asset = Asset(
            symbol="EUR",
            asset_type=Asset.AssetType.FOREX,
        )
        >>> quote_asset = Asset(
            symbol="USD",
            asset_type=Asset.AssetType.FOREX,
        )
        >>> order1 = self.create_order(base_asset, 100, "buy", quote=quote_asset)
        >>> order2 = self.create_order(base_asset, 200, "buy", quote=quote_asset)
        >>> self.submit_orders([order1, order2])

        >>> # For 2 FOREX sell orders
        >>> from lumibot.entities import Asset
        >>>
        >>> base_asset = Asset(
            symbol="EUR",
            asset_type=Asset.AssetType.FOREX,
        )
        >>> quote_asset = Asset(
            symbol="USD",
            asset_type=Asset.AssetType.FOREX,
        )
        >>> order1 = self.create_order(base_asset, 100, "sell", quote=quote_asset)
        >>> order2 = self.create_order(base_asset, 200, "sell", quote=quote_asset)
        >>> self.submit_orders([order1, order2])

        >>> # For 2 CRYPTO buy orders.
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
        >>>    asset_type=Aset.AssetType.FOREX,
        >>> )
        >>> order1 = self.create_order(asset_BTC, 0.1, "buy", quote=asset_quote)
        >>> order2 = self.create_order(asset_ETH, 10, "buy", quote=asset_quote)
        >>> self.submit_order([order1, order2])
        >>> or...
        >>> order1 = self.create_order((asset_BTC, asset_quote), 0.1, "buy")
        >>> order2 = self.create_order((asset_ETH, asset_quote), 10, "buy")
        >>> self.submit_order([order1, order2])
        """
        return self.broker.submit_orders(orders)

    def wait_for_order_registration(self, order):
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

    def wait_for_order_execution(self, order):
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

    def wait_for_orders_registration(self, orders):
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

    def wait_for_orders_execution(self, orders):
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

    def cancel_order(self, order):
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
        return self.broker.cancel_order(order)

    def cancel_orders(self, orders):
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

    def sell_all(self, cancel_open_orders=True):
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

        Returns
        -------
        None

        Example
        -------
        >>> # Will close all positions for the strategy
        >>> self.sell_all()
        """
        self.broker.sell_all(self.name, cancel_open_orders=cancel_open_orders, strategy=self)

    def get_last_price(self, asset, quote=None, exchange=None, should_use_last_close=True):
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
        >>> self.log_message(f"Last price for BTC/USDT is {last_price}")

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

    def get_tick(self, asset):
        """Takes an Asset and returns the last known price"""
        # TODO: Should this function be depricated? This appears to be an IBKR-only thing.
        asset = self._sanitize_user_asset(asset)
        return self.broker._get_tick(asset)

    def get_last_prices(self, assets, quote=None, exchange=None):
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
    def options_expiry_to_datetime_date(self, date):
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

    def get_chains(self, asset):
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

    def get_chain(self, chains, exchange="SMART"):
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

    def get_expiration(self, chains):
        """Returns expiration dates for an option chain for a particular
        exchange.

        Using the `chains` dictionary obtained from `get_chains` finds
        all expiry dates for the option chains on a given
        exchange. The return list is sorted.

        Parameters
        ---------
        chains : dictionary of dictionaries
            The chains dictionary created by `get_chains` method.

        exchange : str optional
            The exchange such as `SMART`, `CBOE`. Default is `SMART`.

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

    def get_multiplier(self, chains, exchange="SMART"):
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

    def get_strikes(self, asset, chains=None):
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

    def find_first_friday(self, timestamp):
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
        asset,
        asset_price=None,
        underlying_price=None,
        risk_free_rate=None,
        query_greeks=False,
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
        return self.broker.data_source.DEFAULT_PYTZ

    def get_datetime(self):
        """Returns the current datetime according to the data source. In a backtest this will be the current bar's datetime. In live trading this will be the current datetime on the exchange.

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
        return self.broker.data_source.get_datetime()

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

    def get_round_minute(self, timeshift=0):
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

    def get_round_day(self, timeshift=0):
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

    def get_datetime_range(self, length, timestep="minute", timeshift=None):
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

    def load_pandas(self, asset, df):
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
        # If backtesting,  return existing asset if in store.
        if self.is_backtesting and asset_type != "crypto":
            # Check for existing asset.
            for asset in self.broker.data_source._data_store:
                is_symbol = asset.symbol == symbol
                is_asset_type = asset.asset_type == asset_type
                is_expiration = asset.expiration == expiration
                if asset.strike != "" and strike != "":
                    is_strike = float(asset.strike) == float(strike)
                else:
                    is_strike = asset.strike == strike
                is_right = asset.right == right
                is_multiplier = asset.multiplier == multiplier
                if asset_type == "stock" and (is_symbol and is_asset_type and is_multiplier):
                    return asset
                elif asset_type == "future" and (is_symbol and is_asset_type and is_expiration and is_multiplier):
                    return asset
                elif asset_type == "option" and (
                    is_symbol and is_asset_type and is_expiration and is_right and is_strike and is_multiplier
                ):
                    return asset
                else:
                    pass

        return Asset(
            symbol=symbol,
            asset_type=asset_type,
            expiration=expiration,
            strike=strike,
            right=right,
            multiplier=multiplier,
        )

    def add_marker(self, name, value=None, color=None, symbol="circle", size=None, detail_text=None, dt=None):
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
                f"Invalid value parameter in add_marker() method. Value must be a float or int but instead "
                f"got {value}, which is a type {type(value)}."
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
                if marker["timestamp"] == timestamp and marker["name"] == name and marker["symbol"] == symbol:
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

    def add_line(self, name, value, color=None, style="solid", width=None, detail_text=None, dt=None):
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

    def write_backtest_settings(self, settings_file):
        datasource = self.broker.data_source
        auto_adjust = datasource.auto_adjust if hasattr(datasource, "auto_adjust") else False
        settings = {
            "name": self.name,
            "backtesting_start": self.backtesting_start,
            "backtesting_end": self.backtesting_end,
            "budget": self.initial_budget,
            "risk_free_rate": self.risk_free_rate,
            "minutes_before_closing": self.minutes_before_closing,
            "minutes_before_opening": self.minutes_before_opening,
            "sleeptime": self.sleeptime,
            "auto_adjust": auto_adjust,
            "quote_asset": self.quote_asset,
            "benchmark_asset": self._benchmark_asset,
            "starting_positions": self.starting_positions,
            "parameters": self.parameters,
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

        if quote is None:
            quote = self.quote_asset

        self.logger.info(f"Getting historical prices for {asset}, {length} bars, {timestep}")

        asset = self._sanitize_user_asset(asset)

        asset = self.crypto_assets_to_tuple(asset, quote)
        if not timestep:
            timestep = self.broker.data_source.MIN_TIMESTEP
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
        asset,
        length,
        timestep="",
        timeshift=None,
        quote=None,
        exchange=None,
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
        assets,
        length,
        timestep="minute",
        timeshift=None,
        chunk_size=100,
        max_workers=200,
        exchange=None,
        include_after_hours=True,
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
        assets : list(str/asset)
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
        >>> for asset in bars:
        >>>     self.log_message(asset.df)

        >>> # Get the data for AAPL and GOOG for the last 30 minutes
        >>> bars =  self.get_historical_prices_for_assets(["AAPL", "GOOG"], 30, "minute")
        >>> for asset in bars:
        >>>     self.log_message(asset.df)

        >>> # Get the price data for EURUSD for the last 2 days
        >>> from lumibot.entities import Asset
        >>> asset_base = Asset(symbol="EUR", asset_type="forex")
        >>> asset_quote = Asset(symbol="USD", asset_type="forex")
        >>> bars =  self.get_historical_prices_for_assets(asset_base, 2, "day", quote=asset_quote)
        >>> df = bars.df
        """

        self.logger.info(f"Getting historical prices for {assets}, {length} bars, {timestep}")

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
        assets,
        length,
        timestep="minute",
        timeshift=None,
        chunk_size=100,
        max_workers=200,
        exchange=None,
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

    def start_realtime_bars(self, asset, keep_bars=30):
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

    def get_realtime_bars(self, asset):
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

    def cancel_realtime_bars(self, asset):
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

    def get_yesterday_dividend(self, asset):
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

    def get_yesterday_dividends(self, assets):
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
        return self.broker.data_source.get_yesterday_dividends(assets, quote=self.quote_asset)

    def update_parameters(self, parameters):
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

    def set_parameters(self, parameters):
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

    def set_parameter_defaults(self, parameters):
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

    def initialize(self, parameters=None):
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
        """Use this lifecycle method to execude code
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

    def trace_stats(self, context, snapshot_before):
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
        """Use this lifecycle method to execude code before the market closes. You can use self.minutes_before_closing to set the number of minutes before closing

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

    def on_bot_crash(self, error):
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

    def on_new_order(self, order):
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

    def on_canceled_order(self, order):
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

    def on_partially_filled_order(self, position, order, price, quantity, multiplier):
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

        quantity : int
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

    def on_filled_order(self, position, order, price, quantity, multiplier):
        """Use this lifecycle event to execute code when an order has been filled by the broker.

        Parameters
        ----------
        position : Position object
            The position that is being filled.

        order : Order object
            The order that is being filled.

        price : float
            The price of the fill.

        quantity : int
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

    def on_parameters_updated(self, parameters):
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

    # ====== Messaging Methods ========================

    def send_discord_message(self, message, image_buf=None, silent=True):
        """
        Sends a message to Discord
        """

        # Check if we are in backtesting mode, if so, don't send the message
        if self.is_backtesting:
            return

        # Check if the message is empty
        if message == "" or message is None:
            # If the message is empty, log and return
            self.logger.debug("The discord message is empty. Please provide a message to send to Discord.")
            return

        # Check if the discord webhook URL is set
        if self.discord_webhook_url is None:
            # If the webhook URL is not set, log and return
            self.logger.debug(
                "The discord webhook URL is not set. Please set the discord_webhook_url parameter in the strategy \
                initialization if you want to send messages to Discord."
            )
            return

        # Remove the extra spaces at the beginning of each line
        message = "\n".join(line.lstrip() for line in message.split("\n"))

        # Get the webhook URL from the environment variables
        webhook_url = self.discord_webhook_url

        # The payload for text content
        payload = {"content": message}

        # If silent is true, set the discord message to be silent
        if silent:
            payload["flags"] = [4096]

        # Check if we have an image
        if image_buf is not None:
            # The files that you want to send
            files = {"file": ("results.png", image_buf, "image/png")}

            # Make a POST request to the webhook URL with the payload and file
            response = requests.post(webhook_url, data=payload, files=files)
        else:
            # Make a POST request to the webhook URL with the payload
            response = requests.post(webhook_url, data=payload)

        # Check if the message was sent successfully
        if response.status_code == 200 or response.status_code == 204:
            self.logger.info("Discord message sent successfully.")
        else:
            self.logger.error(
                f"Failed to send message to Discord. Status code: {response.status_code}, message: {response.text}"
            )

    def send_spark_chart_to_discord(self, stats_df, portfolio_value, now):
        # Check if we are in backtesting mode, if so, don't send the message
        if self.is_backtesting:
            return

        # Only keep the stats for the past week
        stats_df = stats_df.loc[stats_df["datetime"] >= (now - pd.Timedelta(days=7))]

        # Set the default color
        color = "black"

        # Check what return we made over the past week
        if stats_df.shape[0] > 0:
            # Resanple the stats dataframe to daily but keep the datetime column
            stats_df = stats_df.resample("D", on="datetime").last().reset_index()

            # Remove nan values
            stats_df = stats_df.dropna()

            # Get the portfolio value 7 days ago
            portfolio_value_7_days_ago = stats_df.iloc[0]["portfolio_value"]
            # Calculate the return over the past 7 days
            return_7_days = ((portfolio_value / portfolio_value_7_days_ago) - 1) * 100

            # Check if we made a positive return, if so, set the color to green, otherwise set it to red
            if return_7_days > 0:
                color = "green"
            else:
                color = "red"

        # Plotting the DataFrame
        plt.figure()

        # Create an axes instance, setting the facecolor to white
        ax = plt.axes(facecolor="white")

        # Plotting with a thicker line
        ax = stats_df.plot(
            x="datetime",
            y="portfolio_value",
            kind="line",
            linewidth=5,
            color=color,
            # label="Account Value",
            ax=ax,
            legend=False,
        )
        plt.title(f"{self.name} Account Value", fontsize=32, pad=60)
        plt.xlabel("")
        plt.ylabel("")

        # # Increase the font size of the tick labels
        # ax.tick_params(axis="both", which="major", labelsize=18)

        # Use a custom formatter for currency
        formatter = ticker.FuncFormatter(lambda x, pos: "${:1,}".format(int(x)))
        ax.yaxis.set_major_formatter(formatter)

        # Custom formatter function
        def custom_date_formatter(x, pos):
            try:
                date = mdates.num2date(x)
                if pos == 0:  # First tick
                    return date.strftime("%d\n%b\n%Y")
                else:  # Other ticks
                    return date.strftime("%d")
            except Exception:
                return ""

        # Set the locator for the x-axis to automatically find the dates
        locator = mdates.AutoDateLocator(minticks=3, maxticks=7)
        ax.xaxis.set_major_locator(locator)

        # Use custom formatter for the x-axis
        ax.xaxis.set_major_formatter(ticker.FuncFormatter(custom_date_formatter))

        # Use the ConciseDateFormatter to format the x-axis dates
        formatter = mdates.ConciseDateFormatter(locator)

        # Increase the font size of the tick labels
        ax.tick_params(axis="x", which="major", labelsize=18, rotation=0)  # For x-axis
        ax.tick_params(axis="y", which="major", labelsize=18)  # For y-axis

        # Center align x-axis labels
        for label in ax.get_xticklabels():
            label.set_horizontalalignment("center")

        # Save the plot to an in-memory file
        buf = io.BytesIO()
        plt.savefig(buf, format="png", bbox_inches="tight", pad_inches=0.25)
        buf.seek(0)

        # Send the image to Discord
        self.send_discord_message("-----------\n", buf)

    def send_result_text_to_discord(self, returns_text, portfolio_value, cash):
        # Check if we are in backtesting mode, if so, don't send the message
        if self.is_backtesting:
            return

        # Get the current positions
        positions = self.get_positions()

        # Create the positions text
        positions_details_list = []
        for position in positions:
            # Check if the position asset is the quote asset

            if position.asset == self.quote_asset:
                last_price = 1
            else:
                # Get the last price
                last_price = self.get_last_price(position.asset)

            # Make sure last_price is a number
            if last_price is None or not isinstance(last_price, (int, float, Decimal)):
                self.logger.info(f"Last price for {position.asset} is not a number: {last_price}")
                continue

            # Calculate teh value of the position
            position_value = position.quantity * last_price

            # If option, multiply % of portfolio by 100
            if position.asset.asset_type == "option":
                position_value = position_value * 100

            # Calculate the percent of the portfolio that this position represents
            percent_of_portfolio = position_value / portfolio_value

            # Add the position details to the list
            positions_details_list.append(
                {
                    "asset": position.asset,
                    "quantity": position.quantity,
                    "value": position_value,
                    "percent_of_portfolio": percent_of_portfolio,
                }
            )

        # Sort the positions by the percent of the portfolio
        positions_details_list = sorted(positions_details_list, key=lambda x: x["percent_of_portfolio"], reverse=True)

        # Create the positions text
        positions_text = ""
        for position in positions_details_list:
            # positions_text += f"{position.quantity:,.2f} {position.asset} ({percent_of_portfolio:,.0%})\n"
            positions_text += (
                f"{position['quantity']:,.2f} {position['asset']} ({position['percent_of_portfolio']:,.0%})\n"
            )

        # Create a message to send to Discord (round the values to 2 decimal places)
        message = f"""
                **Update for {self.name}**
                **Account Value:** ${portfolio_value:,.2f}
                **Cash:** ${cash:,.2f}
                {returns_text}
                **Positions:**
                {positions_text}
                """

        # Remove any leading whitespace
        # Remove the extra spaces at the beginning of each line
        message = "\n".join(line.lstrip() for line in message.split("\n"))

        # Add self.discord_account_summary_footer to the message
        if hasattr(self, "discord_account_summary_footer") and self.discord_account_summary_footer is not None:
            message += f"{self.discord_account_summary_footer}\n\n"

        # Add powered by Lumiwealth to the message
        message += "[**Powered by 💡 Lumiwealth**](<https://lumiwealth.com>)\n-----------"

        # Send the message to Discord
        self.send_discord_message(message, None)

    def send_account_summary_to_discord(self):
        # Check if we are in backtesting mode, if so, don't send the message
        if self.is_backtesting:
            return

        # Check if last_account_summary_dt has been set, if not, set it to None
        if not hasattr(self, "last_account_summary_dt"):
            self.last_account_summary_dt = None

        # Check if we should send an account summary to Discord
        should_send_account_summary = self.should_send_account_summary_to_discord()
        if not should_send_account_summary:
            return

        # Get the current portfolio value
        portfolio_value = self.get_portfolio_value()

        # Get the current cash
        cash = self.get_cash()

        # # Get the datetime
        now = pd.Timestamp(datetime.datetime.now()).tz_localize("America/New_York")

        # Get the returns
        returns_text, stats_df = self.calculate_returns()

        # Send a spark chart to Discord
        self.send_spark_chart_to_discord(stats_df, portfolio_value, now)

        # Send the results text to Discord
        self.send_result_text_to_discord(returns_text, portfolio_value, cash)

    def get_stats_from_database(self, stats_table_name):
        # Load the stats dataframe from the database
        stats_df = pd.read_sql_table(stats_table_name, self.account_history_db_connection_str)

        return stats_df

    def to_sql(self, stats_df, stats_table_name, connection_string, if_exists, index):
        # Save the stats to the database
        stats_df.to_sql(
            stats_table_name,
            connection_string,
            if_exists=if_exists,
            index=index,
        )

    def calculate_returns(self):
        # Check if we are in backtesting mode, if so, don't send the message
        if self.is_backtesting:
            return

        # Calculate the return over the past 24 hours, 7 days, and 30 days using the stats dataframe

        # Get the current time in New York
        ny_tz = pytz.timezone("America/New_York")

        # Get the datetime
        now = datetime.datetime.now(ny_tz)

        # Load the stats dataframe from the database
        stats_df = self.get_stats_from_database(STATS_TABLE_NAME)

        # Only keep the stats for this strategy ID
        stats_df = stats_df.loc[stats_df["strategy_id"] == self.strategy_id]

        # Convert the datetime column to a datetime
        stats_df["datetime"] = pd.to_datetime(stats_df["datetime"])  # , utc=True)

        # Convert the datetime column to UTC without converting the time data
        stats_df["datetime"] = stats_df["datetime"].dt.tz_convert(None)

        # Explicitly set the time zone to New York without converting the time data
        stats_df["datetime"] = stats_df["datetime"].dt.tz_localize("America/New_York", ambiguous="infer")

        # Get the stats
        stats_new = pd.DataFrame(
            {
                "id": str(uuid.uuid4()),
                "datetime": [now],
                "portfolio_value": [self.get_portfolio_value()],
                "cash": [self.get_cash()],
                "strategy_id": [self.strategy_id],
            }
        )

        # Add the new stats to the existing stats
        stats_df = pd.concat([stats_df, stats_new])

        # # Convert the datetime column to eastern time
        stats_df["datetime"] = stats_df["datetime"].dt.tz_convert("America/New_York")

        # Remove any duplicate rows
        stats_df = stats_df[~stats_df["datetime"].duplicated(keep="last")]

        # Sort the stats by the datetime column
        stats_df = stats_df.sort_values("datetime")

        # Set the strategy ID column to be the strategy ID
        stats_df["strategy_id"] = self.strategy_id

        # Index should be a uuid, fill the index with uuids
        stats_df.loc[pd.isna(stats_df["id"]), "id"] = [
            str(uuid.uuid4()) for _ in range(len(stats_df.loc[pd.isna(stats_df["id"])]))
        ]

        # Check that the stats dataframe has at least 1 row and contains the portfolio_value column
        if stats_df.shape[0] > 0 and "portfolio_value" in stats_df.columns:
            # Save the stats to the database
            self.to_sql(stats_new, STATS_TABLE_NAME, self.account_history_db_connection_str, "append", False)

            # Get the current portfolio value
            portfolio_value = self.get_portfolio_value()

            # Initialize the results
            results_text = ""

            # Add results for the past 24 hours
            # Get the datetime 24 hours ago
            datetime_24_hours_ago = now - pd.Timedelta(days=1)
            # Get the df for the past 24 hours
            stats_past_24_hours = stats_df.loc[stats_df["datetime"] >= datetime_24_hours_ago]
            # Check if there are any stats for the past 24 hours
            if stats_past_24_hours.shape[0] > 0:
                # Get the portfolio value 24 hours ago
                portfolio_value_24_hours_ago = stats_past_24_hours.iloc[0]["portfolio_value"]
                # Calculate the return over the past 24 hours
                return_24_hours = ((portfolio_value / portfolio_value_24_hours_ago) - 1) * 100
                # Add the return to the results
                results_text += f"**24 hour Return:** {return_24_hours:,.2f}% (${(portfolio_value - portfolio_value_24_hours_ago):,.2f} change)\n"

            # Add results for the past 7 days
            # Get the datetime 7 days ago
            datetime_7_days_ago = now - pd.Timedelta(days=7)
            # First check if we have stats that are at least 7 days old
            if stats_df["datetime"].min() < datetime_7_days_ago:
                # Get the df for the past 7 days
                stats_past_7_days = stats_df.loc[stats_df["datetime"] >= datetime_7_days_ago]
                # Check if there are any stats for the past 7 days
                if stats_past_7_days.shape[0] > 0:
                    # Get the portfolio value 7 days ago
                    portfolio_value_7_days_ago = stats_past_7_days.iloc[0]["portfolio_value"]
                    # Calculate the return over the past 7 days
                    return_7_days = ((portfolio_value / portfolio_value_7_days_ago) - 1) * 100
                    # Add the return to the results
                    results_text += f"**7 day Return:** {return_7_days:,.2f}% (${(portfolio_value - portfolio_value_7_days_ago):,.2f} change)\n"

                    ## If we are up more than pct_up_threshold over the past 7 days, send a message to Discord
                    PERCENT_UP_THRESHOLD = 3
                    if return_7_days > PERCENT_UP_THRESHOLD:
                        # Create a message to send to Discord
                        message = f"""
                                🚀 {self.name} is up {return_7_days:,.2f}% in 7 days.
                                """

                        # Remove any leading whitespace
                        # Remove the extra spaces at the beginning of each line
                        message = "\n".join(line.lstrip() for line in message.split("\n"))

                        # Send the message to Discord
                        self.send_discord_message(message, silent=False)

            # Add results for the past 30 days
            # Get the datetime 30 days ago
            datetime_30_days_ago = now - pd.Timedelta(days=30)
            # First check if we have stats that are at least 30 days old
            if stats_df["datetime"].min() < datetime_30_days_ago:
                # Get the df for the past 30 days
                stats_past_30_days = stats_df.loc[stats_df["datetime"] >= datetime_30_days_ago]
                # Check if there are any stats for the past 30 days
                if stats_past_30_days.shape[0] > 0:
                    # Get the portfolio value 30 days ago
                    portfolio_value_30_days_ago = stats_past_30_days.iloc[0]["portfolio_value"]
                    # Calculate the return over the past 30 days
                    return_30_days = ((portfolio_value / portfolio_value_30_days_ago) - 1) * 100
                    # Add the return to the results
                    results_text += f"**30 day Return:** {return_30_days:,.2f}% (${(portfolio_value - portfolio_value_30_days_ago):,.2f} change)\n"

            # Get inception date
            inception_date = stats_df["datetime"].min()

            # Inception date text
            inception_date_text = f"{inception_date.strftime('%b %d, %Y')}"

            # Add results since inception
            # Get the portfolio value at inception
            portfolio_value_inception = stats_df.iloc[0]["portfolio_value"]
            # Calculate the return since inception
            return_since_inception = ((portfolio_value / portfolio_value_inception) - 1) * 100
            # Add the return to the results
            results_text += f"**Since Inception ({inception_date_text}):** {return_since_inception:,.2f}% (${(portfolio_value - portfolio_value_inception):,.2f} change)\n"

            return results_text, stats_df

        else:
            return "Not enough data to calculate returns", stats_df

    def should_send_account_summary_to_discord(self):
        # Check if account_history_db_connection_str has been set, if not, return False
        if not hasattr(self, "account_history_db_connection_str") or self.account_history_db_connection_str is None:
            return False

        # Check if it has been at least 24 hours since the last account summary
        if (
            self.last_account_summary_dt is None
            or (datetime.datetime.now() - self.last_account_summary_dt).total_seconds()
            > 60 * 60 * 24  # 60 seconds * 60 minutes * 24 hours
        ):
            # Set the last account summary datetime to now
            self.last_account_summary_dt = datetime.datetime.now()

            # Sleep for 5 seconds to make sure all the orders go through first
            time.sleep(5)

            # Return True because we should send the account summary to Discord
            return True

        # Return False because we should not send the account summary to Discord
        return False
