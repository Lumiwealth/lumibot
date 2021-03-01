import logging

from lumibot.entities import Order

from ._strategy import _Strategy


class Strategy(_Strategy):
    @property
    def name(self):
        return self._name

    @property
    def initial_budget(self):
        return self._initial_budget

    @property
    def minutes_before_closing(self):
        return self._minutes_before_closing

    @property
    def sleeptime(self):
        return self._sleeptime

    @sleeptime.setter
    def sleeptime(self, value):
        self._sleeptime = value

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
        symbol,
        quantity,
        side,
        limit_price=None,
        stop_price=None,
        time_in_force="day",
    ):
        order = Order(
            self.name,
            symbol,
            quantity,
            side,
            limit_price=limit_price,
            stop_price=stop_price,
            time_in_force=time_in_force,
        )
        return order

    # =======Broker methods shortcuts============

    def await_market_to_open(self):
        """Executes infinite loop until market opens"""
        self.broker.await_market_to_open()

    def await_market_to_close(self):
        """Sleep until market closes"""
        self.broker.await_market_to_close()

    def get_tracked_position(self, symbol):
        """get a tracked position given
        a symbol for the current strategy"""
        return self.broker.get_tracked_position(self.name, symbol)

    def get_tracked_positions(self):
        """get all tracked positions for the current strategy"""
        return self.broker.get_tracked_positions(self.name)

    @property
    def positions(self):
        return self.get_tracked_positions()

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
        """Get the list of symbols for positions
        and open orders for the current strategy"""
        return self.broker.get_tracked_assets(self.name)

    def get_asset_potential_total(self, symbol):
        """given current strategy and a symbol, check the ongoing
        position and the tracked order and returns the total
        number of shares provided all orders went through"""
        return self.broker.get_asset_potential_total(self.name, symbol)

    def submit_order(self, order):
        """Submit an order for an asset"""
        self.broker.submit_order(order)

    def submit_orders(self, orders):
        """submit orders"""
        self.broker.submit_orders(orders)

    def cancel_order(self, order):
        """Cancel an order"""
        self.broker.cancel_order(order)

    def cancel_orders(self, orders):
        """cancel orders"""
        self.broker.cancel_orders(orders)

    def cancel_open_orders(self):
        """cancel all the strategy open orders"""
        self.broker.cancel_open_orders(self.name)

    def sell_all(self, cancel_open_orders=True):
        """sell all strategy positions"""
        self.broker.sell_all(self.name, cancel_open_orders=cancel_open_orders)

    def get_last_price(self, symbol):
        """Takes an asset symbol and returns the last known price"""
        return self.broker.get_last_price(symbol)

    def get_last_prices(self, symbols):
        """Takes a list of symbols and returns the last known prices"""
        return self.broker.get_last_prices(symbols)

    def get_tradable_assets(self, easy_to_borrow=None, filter_func=None):
        """Get the list of all tradable assets
        within the current broker from the market"""
        return self.broker.get_tradable_assets(
            easy_to_borrow=easy_to_borrow, filter_func=filter_func
        )

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

    def get_symbol_bars(
        self,
        symbol,
        length,
        timestep="",
        timeshift=None,
    ):
        """Get bars for a given symbol"""
        if not timestep:
            timestep = self.data_source.MIN_TIMESTEP
        return self.data_source.get_symbol_bars(
            symbol, length, timestep=timestep, timeshift=timeshift
        )

    def get_bars(
        self,
        symbols,
        length,
        timestep="",
        timeshift=None,
        chunk_size=100,
        max_workers=200,
    ):
        """Get bars for the list of symbols"""
        if not timestep:
            timestep = self.data_source.MIN_TIMESTEP
        return self.data_source.get_bars(
            symbols,
            length,
            timestep=timestep,
            timeshift=timeshift,
            chunk_size=chunk_size,
            max_workers=max_workers,
        )

    def get_yesterday_dividend(self, symbol):
        return self.data_source.get_yesterday_dividend(symbol)

    def get_yesterday_dividends(self, symbols):
        return self.data_source.get_yesterday_dividends(symbols)

    # =======Lifecycle methods====================

    def initialize(self):
        """Use this lifecycle method to initialize parameters"""
        pass

    def before_market_opens(self):
        """Lifecycle method executed before market opens
        Example: self.cancel_open_orders()"""
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

    def on_partially_filled_order(self, order, price, quantity):
        """Use this lifecycle event to execute code
        when an order has been partially filled by the broker"""
        pass

    def on_filled_order(self, position, order, price, quantity):
        """Use this lifecycle event to execute code
        when an order has been filled by the broker"""
        pass
