import logging

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
            sec_type=asset.asset_types,
            expiration=asset.expiration,
            strike=asset.strike,
            right=asset.right,
            multiplier=asset.multiplier,
            position_filled=position_filled,
        )
        return order

    # =======Broker methods shortcuts============

    def sleep(self, sleeptime):
        """Sleeping for sleeptime seconds"""
        return self.broker.sleep(sleeptime)

    def await_market_to_open(self, timedelta=None):
        """Executes infinite loop until market opens"""
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
        """Submit an order for an asset"""
        return self.broker.submit_order(order)

    def submit_orders(self, orders):
        """submit orders"""
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
        """Cancel an order"""
        return self.broker.cancel_order(order)

    def cancel_orders(self, orders):
        """cancel orders"""
        return self.broker.cancel_orders(orders)

    def cancel_open_orders(self):
        """cancel all the strategy open orders"""
        return self.broker.cancel_open_orders(self.name)

    def sell_all(self, cancel_open_orders=True):
        """sell all strategy positions"""
        self.broker.sell_all(
            self.name, cancel_open_orders=cancel_open_orders,
        )

    def get_last_price(self, asset):
        """Takes an asset asset and returns the last known price"""
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

    def get_tradable_assets(self, easy_to_borrow=None, filter_func=None):
        """Get the list of all tradable assets
        within the current broker from the market"""
        return self.broker.get_tradable_assets(
            easy_to_borrow=easy_to_borrow, filter_func=filter_func
        )

    # =======Broker methods shortcuts============
    def option_params(self, asset, exchange="", underlyingConId=""):
        """Returns option chain data, list of strikes and list of expiry dates."""
        asset = self._set_asset_mapping(asset)
        return self.broker.option_params(
            asset=asset, exchange=exchange, underlyingConId=underlyingConId
        )

    def get_chains(self, asset):
        """Returns option chain."""
        asset = self._set_asset_mapping(asset)
        return self.broker.get_chains(asset)

    def get_chain(self, chains, exchange="SMART"):
        """Returns option chain for a particular exchange."""
        return self.broker.get_chain(chains, exchange=exchange)

    def get_expiration(self, chains, exchange="SMART"):
        """Returns option chain for a particular exchange."""
        return self.broker.get_expiration(chains, exchange=exchange)

    def get_multiplier(self, chains, exchange="SMART"):
        """Returns option chain for a particular exchange."""
        return self.broker.get_multiplier(chains, exchange=exchange)

    def get_strikes(self, asset):
        """Returns a list of strikes for a give underlying asset."""
        asset = self._set_asset_mapping(asset)
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

    def create_asset(
        self,
        symbol,
        asset_type=None,
        name="",
        expiration="",
        strike="",
        right="",
        multiplier=100,
    ):
        """Create an asset object."""
        return Asset(
            symbol,
            asset_type=asset_type,
            name=name,
            expiration=expiration,
            strike=strike,
            right=right,
            multiplier=multiplier,
        )

    def get_symbol_bars(
        self,
        asset,
        length,
        timestep="",
        timeshift=None,
    ):
        """Get bars for a given asset"""
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
        timestep="",
        timeshift=None,
        chunk_size=100,
        max_workers=200,
    ):
        """Get bars for the list of assets"""
        assets = [self._set_asset_mapping(asset) for asset in assets]
        if not timestep:
            timestep = self.data_source.MIN_TIMESTEP
        return self.data_source.get_bars(
            assets,
            length,
            timestep=timestep,
            timeshift=timeshift,
            chunk_size=chunk_size,
            max_workers=max_workers,
        )

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
