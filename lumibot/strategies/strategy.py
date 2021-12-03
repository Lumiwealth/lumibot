import datetime
import logging

import pandas as pd

from lumibot.entities import Asset, Order

from ._strategy import _Strategy


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
    def minutes_before_opening(self):
        """Get or set the number of minutes that the strategy will start executing before the market opens. The lifecycle method before_market_opens is executed minutes_before_opening minutes before the market opens. By default equals to 60 minutes.

        Parameters
        ----------
        minutes_before_opening : int
            Number of minutes before the market opens.

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

        Sleep time is the time the program will pause between executions of on_trading_iteration and trace_stats. This is used to control the speed of the program.

        By default equals 1 minute. You can set the sleep time as an integer which will be interpreted as minutes. eg: sleeptime = 50 would be 50 minutes. Conversely, you can enter the time as a string with the duration numbers first, followed by the time units: ‘M’ for minutes, ‘S’ for seconds eg: ‘300S’ is 300 seconds, ‘10M’ is 10 minutes. Only “S” and “M” are allowed.

        Parameters
        ----------
        sleeptime : int or str
            Sleep time in minutes or a string with the duration numbers first, followed by the time units: ‘M’ for minutes, ‘S’ for seconds.

        Returns
        -------
        sleeptime : int
            Sleep time in minutes or a string with the duration numbers first, followed by the time units: ‘M’ for minutes, ‘S’ for seconds.

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
        """
        return self._sleeptime

    @sleeptime.setter
    def sleeptime(self, value):
        self._sleeptime = value

    @property
    def parameters(self):
        """Returns the parameters of the strategy. This is a dictionary with the strategy parameters.

        Returns
        -------
        dict:
            The parameters of the strategy.

        Example
        -------
        >>> self.log_message(f'Strategy parameters: {self.parameters}')
        """
        return self._parameters

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
    def portfolio_value(self):
        """Returns the current portfolio value (cash + positions value).

        Returns
        -------
        portfolio_value : float
            The current portfolio value. Includes the actual values of shares held by the current strategy plus the total unspent money.

        Example
        -------
        >>> # Get the current portfolio value
        >>> self.log_message(self.portfolio_value)

        """
        return self._portfolio_value

    @property
    def unspent_money(self):
        """Returns the current unspent money. This is the money that is not used for positions or
        orders (in other words, the money that is available to buy new assets, or cash).

        This property is updated whenever a transaction was filled by the broker or when dividends
        are paid.

        Returns
        -------
        unspent_money : float
            The current unspent money.

        Example
        -------
        >>> # Get the current unspent money
        >>> self.log_message(self.unspent_money)
        """
        return self._unspent_money

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
        return self._risk_free_rate

    # =======Helper methods=======================

    def log_message(self, message):
        """Logs an info message prefixed with the strategy name.

        Uses python logging to log the message at the `info` level.
        Logging goes to the logging file, not the console.

        Parameters
        ----------
        message : str
            String message for logging.

        Returns
        -------
        message : str
            Strategy name plus the original message.

        Example
        --------
        >>> self.log_message('Sending a buy order')
        """
        message = "Strategy %s: %s" % (self.name, message)
        logging.info(message)

        return message

    # ======Order methods shortcuts===============

    def create_order(
        self,
        asset,
        quantity,
        side,
        limit_price=None,
        stop_price=None,
        time_in_force="day",
        take_profit_price=None,
        stop_loss_price=None,
        stop_loss_limit_price=None,
        trail_price=None,
        trail_percent=None,
        position_filled=False,
        exchange="SMART",
    ):
        """Creates a new order for this specific strategy. Once created, an order must still be submitted.

        Parameters
        ----------
        asset : str or Asset
            The asset that will be traded. If this is just a stock, then
            `str` is sufficient. However, all assets other than stocks
            must use `Asset`.
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
        time_in_force : str
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

        Returns
        -------
        Order
            Order object ready to be submitted for trading.

        Example
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
        >>>                position_filled=True,
        >>>            )
        """
        asset = self._set_asset_mapping(asset)
        order = Order(
            self.name,
            asset,
            quantity,
            side,
            limit_price=limit_price,
            stop_price=stop_price,
            time_in_force=time_in_force,
            take_profit_price=take_profit_price,
            stop_loss_price=stop_loss_price,
            stop_loss_limit_price=stop_loss_limit_price,
            trail_price=trail_price,
            trail_percent=trail_percent,
            exchange=exchange,
            sec_type=asset.asset_type,
            position_filled=position_filled,
        )
        return order

    # =======Broker methods shortcuts============

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
        return self.broker.sleep(sleeptime)

    def set_market(self, market):
        """Set the market for trading hours.

        Setting the market will determine the trading hours for live
        trading and for Yahoo backtesting. Not applicable to Pandas
        backtesting.
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
        >>> # Set the market to NYSE
        >>> self.set_market('NYSE')

        >>> # Set the market to NASDAQ
        >>> self.set_market('NASDAQ')

        >>> # Set the market to 24/7
        >>> self.set_market('24/7')

        >>> # Set the market to 24/5
        >>> self.set_market('24/5')

        >>> # Set the market to us_futures
        >>> self.set_market('us_futures')

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
            raise ValueError(
                f"Valid market entries are: {markets}. You entered {market}. Please adjust."
            )

        if self.broker.SOURCE == "InteractiveBrokers":
            self.broker.market = market
        else:
            raise ValueError(
                f"Please only adjust market calendars when using a broker that supports assets other than stocks"
            )

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
        if timedelta is None:
            timedelta = self.minutes_before_opening
        return self.broker._await_market_to_open(timedelta)

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
        if timedelta is None:
            timedelta = self.minutes_before_closing
        return self.broker._await_market_to_close(timedelta)

    def get_tracked_position(self, asset):
        """Get a tracked position given an asset for the current
        strategy.

        Seeks out and returns the position object for the given asset
        in the current strategy.

        Parameters
        ----------
        asset : Asset
            Asset object who's traded positions is sought.

        Returns
        -------
        Position or None
            A position object for the assset if there is a tracked
            position or returns None to indicate no tracked position.

        Example
        -------
        >>> # Get the position for the TLT asset
        >>> position = self.get_tracked_position("TLT")
        >>> # Show the quantity of the TLT position
        >>> self.log_message(position.quantity)
        """
        asset = self._set_asset_mapping(asset)
        return self.broker.get_tracked_position(self.name, asset)

    def get_tracked_positions(self):
        """Get all tracked positions for the current strategy

        Seeks out and returns the all tracked positions in the current
        strategy.

        Returns
        -------
        list
            A list of Position objects for the strategy if there are tracked
            positions or returns and empty list to indicate no tracked
            position.

        Example
        -------
        >>> # Get all tracked positions
        >>> positions = self.get_tracked_positions()
        >>> for position in positions:
        >>>     # Show the quantity of each position
        >>>     self.log_message(position.quantity)
        """

        return self.broker.get_tracked_positions(self.name)

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

        asset = self._set_asset_mapping(asset)
        return self.broker.get_contract_details(asset)

    def get_tracked_order(self, identifier):
        """Get a tracked order given an identifier. Check the details of the order including status, etc.

        Returns
        -------
        Order or None
            An order objects for the identifier

        Example
        -------
        >>> # Get the order object for the order id
        >>> order = self.get_tracked_order(order_id)
        """
        order = self.broker.get_tracked_order(identifier)
        if order.strategy == self.name:
            return order
        return None

    def get_tracked_orders(self):
        """Get all the current open orders.

        Returns
        -------
        list of Order objects
            Order objects for the strategy if there are tracked

        Example
        -------
        >>> # Get all tracked orders
        >>> orders = self.get_tracked_orders()
        >>> for order in orders:
        >>>     # Show the status of each order
        >>>     self.log_message(order.status)
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
        int
            The potential total for the asset.

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
        asset = self._set_asset_mapping(asset)
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
        >>> asset = Asset(
        >>>    "ES",
        >>>    asset_type="future",
        >>>    expiration_date="2020-01-01",
        >>>    multiplier=100)
        >>> order = self.create_order(asset, 100, "buy")
        >>> self.submit_order(order)

        >>> # For selling a future
        >>> asset = Asset(
        >>>    "ES",
        >>>    asset_type="future",
        >>>    expiration_date="2020-01-01"
        >>>    multiplier=100)
        >>> order = self.create_order(asset, 100, "sell")
        >>> self.submit_order(order)

        >>> # For buying an option
        >>> asset = Asset(
        >>>    "SPY",
        >>>    asset_type="option",
        >>>    expiration_date="2020-01-01",
        >>>    strike_price=100.00,
        >>>    right="call",
        >>>    multiplier=100)
        >>> order = self.create_order(asset, 10, "buy")
        >>> self.submit_order(order)
        """
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
        self.broker.sell_all(
            self.name,
            cancel_open_orders=cancel_open_orders,
        )

    def get_last_price(self, asset):
        """Takes an asset asset and returns the last known price

        Makes an active call to the market to retrieve the last price.
        In backtesting will provide the close of the last complete bar.

        Parameters
        ----------
        asset : Asset object
            Asset object for which the last closed price will be
            retrieved.

        Returns
        -------
        Float
            Last closed price.

        Example
        -------
        >>> # Will return the last price for the asset
        >>> asset = "SPY"
        >>> last_price = self.get_last_price(asset)
        >>> self.log_message(f"Last price for {asset} is {last_price}")
        """
        asset = self._set_asset_mapping(asset)
        return self.broker.get_last_price(asset)

    def get_tick(self, asset):
        """Takes an asset asset and returns the last known price"""
        asset = self._set_asset_mapping(asset)
        return self.broker._get_tick(asset)

    def get_last_prices(self, assets):
        """Takes a list of assets and returns the last known prices

        Makes an active call to the market to retrieve the last price. In backtesting will provide the close of the last complete bar.

        Parameters
        ----------
        assets : list of Asset objects
            List of Asset objects for which the last closed price will be retrieved.

        Returns
        -------
        list of floats
            Last known closing prices.

        Example
        -------
        >>> # Will return the last price for the assets
        >>> assets = ["SPY", "TLT"]
        >>> last_prices = self.get_last_prices(assets)
        """
        symbol_asset = isinstance(assets[0], str)
        if symbol_asset:
            assets = [self._set_asset_mapping(asset) for asset in assets]

        asset_prices = self.broker.get_last_prices(assets)

        if symbol_asset:
            return {a.symbol: p for a, p in asset_prices.items()}
        else:
            return asset_prices

    # =======Broker methods shortcuts============
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
        dictionary of dictionaries for each exchange. Each exchange
        dictionary has:

            - `Underlying conId` (int)
            - `TradingClass` (str) eg: `FB`
            - `Multiplier` (str) eg: `100`
            - `Expirations` (set of str) eg: {`20230616`, ...}
            - `Strikes` (set of floats)

        Example
        -------
        >>> # Will return the option chains for SPY
        >>> asset = "SPY"
        >>> chains = self.get_chains(asset)
        """
        asset = self._set_asset_mapping(asset)
        return self.broker.get_chains(asset)

    def get_chain(self, chains, exchange="SMART"):
        """Returns option chain for a particular exchange.

        Takes in a full set of chains for all the exchanges and returns
        on chain for a given exchange. The the full chains are returned
        from `get_chains` method.

        Parameters
        ----------
        chains : dictionary of dictionaries
            The chains dictionary created by `get_chains` method.

        exchange : str optional
            The exchange such as `SMART`, `CBOE`. Default is `SMART`

        Returns
        -------
        dictionary
            A dictionary of option chain information for one stock and
            for one exchange. It will contain:

                - `Underlying conId` (int)
                - `TradingClass` (str) eg: `FB`
                - `Multiplier` (str) eg: `100`
                - `Expirations` (set of str) eg: {`20230616`, ...}
                - `Strikes` (set of floats)

        Example
        -------
        >>> # Will return the option chains for SPY
        >>> asset = "SPY"
        >>> chain = self.get_chain(asset)
        """
        return self.broker.get_chain(chains, exchange=exchange)

    def get_expiration(self, chains, exchange="SMART"):
        """Returns expiration dates for an option chain for a particular
        exchange.

        Using the `chains` dictionary obtained from `get_chains` finds
        all of the expiry dates for the option chains on a given
        exchange. The return list is sorted.

        Parameters
        ---------
        chains : dictionary of dictionaries
            The chains dictionary created by `get_chains` method.

        exchange : str optional
            The exchange such as `SMART`, `CBOE`. Default is `SMART`.

        Returns
        -------
        list of datetime.dates
            Sorted list of dates in the form of `20221013`.

        Example
        -------
        >>> # Will return the expiry dates for SPY
        >>> asset = "SPY"
        >>> expiry_dates = self.get_expiration(asset)
        """
        return self.broker.get_expiration(chains, exchange=exchange)

    def get_multiplier(self, chains, exchange="SMART"):
        """Returns option chain for a particular exchange.

        Using the `chains` dictionary obtained from `get_chains` finds
        all of the multiplier for the option chains on a given
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

    def get_strikes(self, asset):
        """Returns a list of strikes for a give underlying asset.

        Using the `chains` dictionary obtained from `get_chains` finds
        all of the multiplier for the option chains on a given
        exchange.

        Parameters
        ----------
        asset : Asset object
            Asset object as normally used for an option but without
            the strike information.

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

        asset = self._set_asset_mapping(asset)

        if self.data_source.SOURCE == "PANDAS":
            return self.broker.get_strikes(asset)

        contract_details = self._get_contract_details(asset)
        if not contract_details:
            return None

        return sorted(list(set(cd.contract.strike for cd in contract_details)))

    def get_greeks(
        self,
        asset,
        implied_volatility=False,
        delta=False,
        option_price=False,
        pv_dividend=False,
        gamma=False,
        vega=False,
        theta=False,
        underlying_price=False,
    ):
        """Returns the greeks for the option asset at the current
        bar.

        Will return all the greeks available unless any of the
        individual greeks are selected, then will only return those
        greeks.

        Parameters
        ----------
        asset : Asset
            Option asset only for with greeks are desired.
        **kwargs
        implied_volatility : boolean
            True to get the implied volatility. (default: True)
        delta : boolean
            True to get the option delta value. (default: True)
        option_price : boolean
            True to get the option price. (default: True)
        pv_dividend : boolean
            True to get the present value of dividends expected on the
            option's  underlying. (default: True)
        gamma : boolean
            True to get the option gamma value. (default: True)
        vega : boolean
            True to get the option vega value. (default: True)
        theta : boolean
            True to get the option theta value. (default: True)
        underlying_price : boolean
            True to get the price of the underlying. (default: True)

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
        >>> asset = "SPY"
        >>> greeks = self.get_greeks(asset)
        >>> implied_volatility = greeks["implied_volatility"]
        >>> delta = greeks["delta"]
        >>> gamma = greeks["gamma"]
        >>> vega = greeks["vega"]
        >>> theta = greeks["theta"]
        """
        if asset.asset_type != "option":
            self.log_message(
                f"The greeks method was called using an asset other "
                f"than an option. Unable to retrieve greeks for non-"
                f"option assest."
            )
            return None

        return self.broker._get_greeks(
            asset,
            implied_volatility=implied_volatility,
            delta=delta,
            option_price=option_price,
            pv_dividend=pv_dividend,
            gamma=gamma,
            vega=vega,
            theta=theta,
            underlying_price=underlying_price,
        )

    # =======Data source methods=================

    @property
    def timezone(self):
        """Returns the timezone of the data source. By default America/New_York.

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
        return self.data_source.DEFAULT_TIMEZONE

    @property
    def pytz(self):
        """Returns the pytz object of the data source. By default America/New_York.

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
        return self.data_source.DEFAULT_PYTZ

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
        return self.data_source.get_datetime()

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
        return self.data_source.get_timestamp()

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
        return self.data_source.get_round_minute(timeshift=timeshift)

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
        return self.data_source.get_last_minute()

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
        return self.data_source.get_round_day(timeshift=timeshift)

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
        return self.data_source.get_last_day()

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
        return self.data_source.get_datetime_range(
            length, timestep=timestep, timeshift=timeshift
        )

    def localize_datetime(self, dt):
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
        return self.data_source.localize_datetime(dt)

    def to_default_timezone(self, dt):
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
        return self.data_source.to_default_timezone(dt)

    def load_pandas(self, asset, df):
        asset = self._set_asset_mapping(asset)
        self.data_source.load_pandas(asset, df)

    def create_asset(
        self,
        symbol,
        asset_type="stock",
        expiration=None,
        strike="",
        right=None,
        multiplier=1,
        currency="USD",
    ):
        """Creates an asset object. This is used to create an asset object.

        Parameters
        ----------
        symbol : str
            The symbol of the asset.

        asset_type : str
            The type of the asset. Can be either "stock", "option", or "future".

        expiration : datetime.datetime
            The expiration date of the asset (optional, only required for options and futures).

        strike : str
            The strike price of the asset (optional, only required for options).

        right : str
            The right of the option (optional, only required for options).

        multiplier : int
            The multiplier of the asset (optional, only required for options and futures).

        currency : str
            The currency of the asset.

        Returns
        -------
        Asset
            The asset object.

        Example
        -------
        >>> # Will create a stock object
        >>> asset = self.create_asset("AAPL", asset_type="stock")

        >>> # Will create an option object
        >>> asset = self.create_asset("AAPL", asset_type="option", expiration=datetime.datetime(2020, 1, 1), strike=100, right="call")

        >>> # Will create a future object
        >>> asset = self.create_asset("AAPL", asset_type="future", multiplier=100)

        >>> # Will create a stock object with a different currency
        >>> asset = self.create_asset("AAPL", asset_type="stock", currency="EUR")
        """
        # If backtesting,  return existing asset if in store.
        if self.broker.IS_BACKTESTING_BROKER:
            # Check for existing asset.
            for asset in self.broker._data_source._data_store:
                is_symbol = asset.symbol == symbol
                is_asset_type = asset.asset_type == asset_type
                is_expiration = asset.expiration == expiration
                if asset.strike != "" and strike != "":
                    is_strike = float(asset.strike) == float(strike)
                else:
                    is_strike = asset.strike == strike
                is_right = asset.right == right
                is_multiplier = asset.multiplier == multiplier
                is_currency = asset.currency == currency

                if asset_type == "stock" and (
                    is_symbol and is_asset_type and is_currency and is_multiplier
                ):
                    return asset
                elif asset_type == "future" and (
                    is_symbol
                    and is_asset_type
                    and is_expiration
                    and is_currency
                    and is_multiplier
                ):
                    return asset
                elif asset_type == "option" and (
                    is_symbol
                    and is_asset_type
                    and is_expiration
                    and is_right
                    and is_strike
                    and is_currency
                    and is_multiplier
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
            currency=currency,
        )

    def get_symbol_bars(
        self,
        asset,
        length,
        timestep="",
        timeshift=None,
    ):
        """Get historical pricing data for a given symbol or asset.

        Return data bars for a given symbol or asset.  Any number of bars can
        be return limited by the data available. This is set with `length` in
        number of bars. Bars may be returned as daily or by minute. And the
        starting point can be shifted backwards by time or bars.

        Parameters
        ----------
        asset : str or Asset
            The symbol string representation (e.g AAPL, GOOG, ...) or asset
            object.
        length : int
            The number of rows (number of timesteps)
        timestep : str
            Either ```"minute""``` for minutes data or ```"day"```
            for days data default value depends on the data_source (minute
            for alpaca, day for yahoo, ...)
        timeshift : timedelta
            ```None``` by default. If specified indicates the time shift from
            the present. If  backtesting in Pandas, use integer representing
            number of bars.

        Returns
        -------
        Bars

        Example
        -------
        Extract 2 rows of SPY data with one day timestep between each row
        with the latest data being 24h ago (timedelta(days=1)) (in a backtest)

        >>> # Get the data for SPY for the last 2 days
        >>> bars =  self.get_symbol_bars("SPY", 2, "day")
        >>> # To get the DataFrame of SPY data
        >>> bars.df

        >>> # Get the data for AAPL for the last 30 minutes
        >>> bars =  self.get_symbol_bars("AAPL", 30, "minute")
        >>> # To get the DataFrame of AAPL data
        >>> bars.df
        """

        asset = self._set_asset_mapping(asset)
        if not timestep:
            timestep = self.data_source.MIN_TIMESTEP
        return self.data_source.get_symbol_bars(
            asset, length, timestep=timestep, timeshift=timeshift
        )

    def get_bars(
        self,
        assets,
        length,
        timestep="minute",
        timeshift=None,
        chunk_size=100,
        max_workers=200,
    ):
        """Get historical pricing data for the list of assets.

        Return data bars for a list of symbols or assets.  Return a dictionary
        of bars for a given list of symbols. Works the same as get_symbol_bars
        but take as first parameter a list of symbols. Any number of bars can
        be return limited by the data available. This is set with `length` in
        number of bars. Bars may be returned as daily or by minute. And the
        starting point can be shifted backwards by time or bars.

        Parameters
        ----------
        assets : list(str/asset)
            The symbol string representation (e.g AAPL, GOOG, ...) or asset
            objects.
        length : int
            The number of rows (number of timesteps)
        timestep : str
            Either ```"minute""``` for minutes data or ```"day"```
            for days data default value depends on the data_source (minute
            for alpaca, day for yahoo, ...)
        timeshift : timedelta
            ```None``` by default. If specified indicates the time shift from
            the present. If  backtesting in Pandas, use integer representing
            number of bars.

        Returns
        -------
        dictionary : Asset : bars
            Return a dictionary bars for a given list of symbols. Works the
            same as get_symbol_bars take as first parameter a list of symbols.

        Example
        -------

        >>> # Get the data for SPY and TLT for the last 2 days
        >>> bars =  self.get_bars(["SPY", "TLT"], 2, "day")
        >>> for asset in bars:
        >>>     self.log_message(asset.df)

        >>> # Get the data for AAPL and GOOG for the last 30 minutes
        >>> bars =  self.get_bars(["AAPL", "GOOG"], 30, "minute")
        >>> for asset in bars:
        >>>     self.log_message(asset.df)
        """
        assets = [self._set_asset_mapping(asset) for asset in assets]

        return self.data_source.get_bars(
            assets,
            length,
            timestep=timestep,
            timeshift=timeshift,
            chunk_size=chunk_size,
            max_workers=max_workers,
        )

    def start_realtime_bars(self, asset, keep_bars=30):
        """Starts a real time stream of tickers for Interactive Broker
        only.

        This allows for real time data to stream to the strategy. Bars
        are fixed at every fix seconds.  The will arrive in the strategy
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
        asset : Asset object
            Asset object that has streaming data to cancel.

        Returns
        -------
        None
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
        """
        asset = self._set_asset_mapping(asset)
        return self.data_source.get_yesterday_dividend(asset)

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
        """
        assets = [self._set_asset_mapping(asset) for asset in assets]
        return self.data_source.get_yesterday_dividends(assets)

    # =======Lifecycle methods====================

    def initialize(self):
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
        """
        pass

    def before_market_opens(self):
        """Use this lifecycle method to execude code
        self.minutes_before_opening minutes before opening.

        Parameters
        ----------
        None

        Returns
        -------
        None

        Example
        -------
        >>> # Get the data for SPY and TLT for the last 2 days
        >>> def before_market_opens(self):
        >>>     bars =  self.get_bars(["SPY", "TLT"], 2, "day")
        >>>     for asset in bars:
        >>>         self.log_message(asset.df)
        """
        pass

    def before_starting_trading(self):
        """Lifecycle method executed after the market opens
        and before entering the trading loop. Use this method
        for daily resetting variables

        Parameters
        ----------
        None

        Returns
        -------
        None

        Example
        -------
        >>> # Get pricing data for the last day
        >>> def before_starting_trading(self):
        >>>     self.get_bars("SPY", 1, "day")
        """
        pass

    def on_trading_iteration(self):
        """Use this lifecycle method for your main trading loop. This method is called every self.sleeptime minutes (or seconds if self.sleeptime is "30S")

        Parameters
        ----------
        None

        Returns
        -------
        None

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
        >>>         "unspent_money": self.unspent_money,
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
        >>>     self.log_message(f"The amount of cash we have is {self.unspent_money})
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

    # ======Events methods========================

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

        """
        pass
