import datetime
import logging
import pandas as pd
from lumibot.entities import Asset, Order

from ._strategy import _Strategy


class Strategy(_Strategy):
    @property
    def name(self):
        return self._name

    @property
    def initial_budget(self):
        return self._initial_budget

    @property
    def minutes_before_opening(self):
        return self._minutes_before_opening

    @minutes_before_opening.setter
    def minutes_before_opening(self, value):
        self._minutes_before_opening = value

    @property
    def minutes_before_closing(self):
        return self._minutes_before_closing

    @minutes_before_closing.setter
    def minutes_before_closing(self, value):
        self._minutes_before_closing = value

    @property
    def sleeptime(self):
        return self._sleeptime

    @sleeptime.setter
    def sleeptime(self, value):
        self._sleeptime = value

    @property
    def parameters(self):
        return self._parameters

    @property
    def is_backtesting(self):
        return self._is_backtesting

    @property
    def portfolio_value(self):
        return self._portfolio_value

    @property
    def unspent_money(self):
        return self._unspent_money

    @property
    def first_iteration(self):
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

        Parameters
        ----------
        message : str
            String message for logging.

        Returns
        -------
        message : str
            Strategy name plus the original message.
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
        """Creates a new order.

        Create an order object attached to this strategy (Check the
        Entities, order section)

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
            expiration=asset.expiration,
            strike=asset.strike,
            right=asset.right,
            multiplier=asset.multiplier,
            position_filled=position_filled,
        )
        return order

    # =======Broker methods shortcuts============

    def sleep(self, sleeptime):
        """Sleeping for sleeptime seconds

        Use to pause the execution of the program.

        Parameters
        ----------
        sleeptime : float
            Time in seconds the program will be paused.

        Returns
        -------
        None
        """
        return self.broker.sleep(sleeptime)

    def set_market(self, market):
        """Set the market for trading hours.
        `NASDAQ` is default.
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
        """
        if timedelta is None:
            timedelta = self.minutes_before_opening
        return self.broker._await_market_to_open(timedelta)

    def await_market_to_close(self, timedelta=None):
        """Sleep until market closes"""
        if timedelta is None:
            timedelta = self.minutes_before_closing
        return self.broker._await_market_to_close(timedelta)

    def get_tracked_position(self, asset):
        """get a tracked position given
        an asset for the current strategy"""
        asset = self._set_asset_mapping(asset)
        return self.broker.get_tracked_position(self.name, asset)

    def get_tracked_positions(self):
        """get all tracked positions for the current strategy"""
        return self.broker.get_tracked_positions(self.name)

    @property
    def positions(self):
        return self.get_tracked_positions()

    def get_contract_details(self, asset):
        # Used for Interactive Brokers. Convert an asset into a IB Contract.
        asset = self._set_asset_mapping(asset)
        return self.broker.get_contract_details(asset)

    def get_tracked_order(self, identifier):
        """get a tracked order given an identifier.
        Check that the order belongs to current strategy"""
        order = self.broker.get_tracked_order(identifier)
        if order.strategy == self.name:
            return order
        return None

    def get_tracked_orders(self):
        """get all tracked orders for a given strategy"""
        return self.broker.get_tracked_orders(self.name)

    def get_tracked_assets(self):
        """Get the list of assets for positions
        and open orders for the current strategy"""
        return self.broker.get_tracked_assets(self.name)

    def get_asset_potential_total(self, asset):
        """given current strategy and a asset, check the ongoing
        position and the tracked order and returns the total
        number of shares provided all orders went through"""
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
        order object
            Processed order object.
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
        """
        return self.broker.submit_orders(orders)

    def wait_for_order_registration(self, order):
        """Wait for the order to be registered by the broker"""
        return self.broker.wait_for_order_registration(order)

    def wait_for_order_execution(self, order):
        """Wait for the order to execute/be canceled"""
        return self.broker.wait_for_order_execution(order)

    def wait_for_orders_registration(self, orders):
        """Wait for the orders to be registered by the broker"""
        return self.broker.wait_for_orders_registration(orders)

    def wait_for_orders_execution(self, orders):
        """Wait for the orders to execute/be canceled"""
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
        """
        asset = self._set_asset_mapping(asset)
        return self.broker.get_last_price(asset)

    def get_tick(self, asset):
        """Takes an asset asset and returns the last known price"""
        asset = self._set_asset_mapping(asset)
        return self.broker.get_tick(asset)

    def get_last_prices(self, assets):
        """Takes a list of assets and returns the last known prices"""
        symbol_asset = isinstance(assets[0], str)
        if symbol_asset:
            assets = [self._set_asset_mapping(asset) for asset in assets]

        asset_prices = self.broker.get_last_prices(assets)

        if symbol_asset:
            return {a.symbol: p for a, p in asset_prices.items()}
        else:
            return asset_prices

    def is_tradable(self, asset, dt, length=1, timestep="minute", timeshift=0):
        """DEPRICATED

        This will not be implemented as it does not have a basis in real time
        trading.

        Determine if the current asset is tradable at the current bar
        in backtesting primarily used with Pandas module.

        Some assets datas will start and end at different times, for
        example options and futures contracts. When backtesting, this
        method will determine if a given asset will have data for the
        current bar given the length, timestep and timeshift required.

        Parameters
        ----------
        asset : Asset object
            The Asset to be checked if data is available for backtesting
            at the current bar.
        dt : datetime.datetime
            Datetime of the bar to check, usually current datetime.
        length : int optional
            Number of bars to check for data. (default is 1)
        timestep : str optional
            Is the timestep `minute` or `day`. (default is `minute`)
        timeshift : int optional
            The number of bars back from `dt` is the last bar.
            (default is 0)

        Returns
        -------
        boolean
            True if is tradable. False or None if not.
        """
        return self.broker._data_source.is_tradable(
            asset, dt, length=length, timestep=timestep, timeshift=timeshift
        )

    def get_tradable_assets(self, dt, length=1, timestep="minute", timeshift=0):
        """DEPRICATED

        This will not be implemented as it does not have a basis in real time
        trading.

        Get the list of all tradable assets within the current broker
        from the market

        Some assets datas will start and end at different times, for
        example options and futures contracts. When backtesting, this
        method will provide a list of assets for the current bar given
        the length, timestep and timeshift required.

        Parameters
        ---------
        asset : Asset object
            The Asset to be checked if data is available for backtesting
            at the current bar.
        dt : datetime.datetime
            Datetime of the bar to check, usually current datetime.
        length : int optional
            Number of bars to check for data. (default is 1)
        timestep : str optional
            Is the timestep `minute` or `day`. (default is `minute`)
        timeshift : int optional
            The number of bars back from `dt` is the last bar.
            (default is 0)

        Returns
        -------
        list of Asset objects
            A list of all the Assets that meet the given criteria.
            Will return an empty list if no assets are available.
        """

        return self.broker._data_source.get_tradable_assets(
            dt, length=length, timestep=timestep, timeshift=timeshift
        )

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
        list of str
            Sorted list of dates in the form of `20221013`.
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

            Example:
            asset = self.create_asset(
                "FB",
                asset_type="option",
                expiration=self.options_expiry_to_datetime_date("20210924"),
                right="CALL",
                multiplier=100,
            )

            `expiration` can also be expressed as
            `datetime.datetime.date()`

        Returns
        -------
        list of floats
            Sorted list of strikes as floats.
        """

        asset = self._set_asset_mapping(asset)

        if self.data_source.SOURCE == "PANDAS":
            return self.broker.get_strikes(asset)

        contract_details = self.get_contract_details(asset)
        if not contract_details:
            return None

        return sorted(list(set(cd.contract.strike for cd in contract_details)))

    # =======Data source methods=================

    @property
    def timezone(self):
        return self.data_source.DEFAULT_TIMEZONE

    @property
    def pytz(self):
        return self.data_source.DEFAULT_PYTZ

    def get_datetime(self):
        return self.data_source.get_datetime()

    def get_timestamp(self):
        return self.data_source.get_timestamp()

    def get_round_minute(self, timeshift=0):
        return self.data_source.get_round_minute(timeshift=timeshift)

    def get_last_minute(self):
        return self.data_source.get_last_minute()

    def get_round_day(self, timeshift=0):
        return self.data_source.get_round_day(timeshift=timeshift)

    def get_last_day(self):
        return self.data_source.get_last_day()

    def get_datetime_range(self, length, timestep="minute", timeshift=None):
        return self.data_source.get_datetime_range(
            length, timestep=timestep, timeshift=timeshift
        )

    def localize_datetime(self, dt):
        return self.data_source.localize_datetime(dt)

    def to_default_timezone(self, dt):
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
        """Create an asset object."""
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
        """Get bars for a given symbol or asset.

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

        Example:
        -------
        Extract 2 rows of SPY data with one day timestep between each row
        with the latest data being 24h ago (timedelta(days=1)) (in a backtest)
        bars =  self.get_symbol_bars("SPY", 2, "day", timedelta(days=1))

                                     open    high     low   close    volume  dividend  \
        Date
        2019-12-24 00:00:00-05:00  321.47  321.52  320.90  321.23  20270000       0.0
        2019-12-26 00:00:00-05:00  321.65  322.95  321.64  322.94  30911200       0.0

                                   stock_splits  price_change  dividend_yield  return
        Date
        2019-12-24 00:00:00-05:00             0          0.00             0.0    0.00
        2019-12-26 00:00:00-05:00             0          0.01             0.0    0.01
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
        """Get bars for the list of assets

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
        asset = self._set_asset_mapping(asset)
        return self.data_source.get_yesterday_dividend(asset)

    def get_yesterday_dividends(self, assets):
        assets = [self._set_asset_mapping(asset) for asset in assets]
        return self.data_source.get_yesterday_dividends(assets)

    # =======Lifecycle methods====================

    def initialize(self):
        """Use this lifecycle method to initialize parameters"""
        pass

    def before_market_opens(self):
        """Use this lifecycle method to execude code
        self.minutes_before_opening minutes before opening.
        Example: self.sell_all()"""
        pass

    def before_starting_trading(self):
        """Lifecycle method executed after the market opens
        and before entering the trading loop. Use this method
        for daily resetting variables"""
        pass

    def on_trading_iteration(self):
        """Use this lifecycle method for trading.
        Will be executed indefinetly until there
        will be only self.minutes_before_closing
        minutes before market closes"""
        pass

    def trace_stats(self, context, snapshot_before):
        """Lifecycle method that will be executed after
        on_trading_iteration. context is a dictionary containing
        on_trading_iteration locals() in last call. Use this
        method to dump stats"""
        return {}

    def before_market_closes(self):
        """Use this lifecycle method to execude code
        self.minutes_before_closing minutes before closing.
        Example: self.sell_all()"""
        pass

    def after_market_closes(self):
        """Use this lifecycle method to execute code
        after market closes. Exampling: dumping stats/reports"""
        pass

    def on_strategy_end(self):
        """Use this lifecycle method to execute code
        when strategy reached its end. Used to dump
        statistics when backtesting finishes"""
        pass

    # ======Events methods========================

    def on_bot_crash(self, error):
        """Use this lifecycle event to execute code
        when an exception is raised and the bot crashes"""
        self.on_abrupt_closing()

    def on_abrupt_closing(self):
        """Use this lifecycle event to execute code
        when the main trader was shut down (Keybord Interuption, ...)
        Example: self.sell_all()"""
        pass

    def on_new_order(self, order):
        """Use this lifecycle event to execute code
        when a new order is being processed by the broker"""
        pass

    def on_canceled_order(self, order):
        """Use this lifecycle event to execute code
        when an order has been canceled by the broker"""
        pass

    def on_partially_filled_order(self, position, order, price, quantity, multiplier):
        """Use this lifecycle event to execute code
        when an order has been partially filled by the broker"""
        pass

    def on_filled_order(self, position, order, price, quantity, multiplier):
        """Use this lifecycle event to execute code
        when an order has been filled by the broker"""
        pass
