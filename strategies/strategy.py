import logging
import time
import traceback
from copy import deepcopy
from datetime import datetime

from backtesting import BacktestingBroker
from entities import Order
from tools import snatch_method_locals
from traders import Trader


class Strategy:
    def __init__(
        self, budget, broker, data_source=None, minutes_before_closing=5, sleeptime=1
    ):
        # Setting the strategy name and the budget allocated
        self._name = self.__class__.__name__
        self.unspent_money = budget

        # Setting the broker object
        self.broker = broker
        self._is_backtesting = self.broker.IS_BACKTESTING_BROKER
        broker._add_subscriber(self)

        # Initializing the context variables
        # containing on_trading_iteration local variables
        self._trading_context = None

        # Setting how many minutes before market closes
        # The bot should stop
        self.minutes_before_closing = minutes_before_closing

        # Timesleep after each on_trading_iteration execution
        # unity is minutes
        self.sleeptime = sleeptime

        # Setting the data provider
        if self._is_backtesting:
            self.data_source = self.broker._data_source
        elif data_source is None:
            self.data_source = self.broker
        else:
            self.data_source = data_source

        # Ready to close
        self._ready_to_close = False

    def __getattribute__(self, name):
        attr = object.__getattribute__(self, name)
        if name == "on_trading_iteration":
            decorator = snatch_method_locals("_trading_context")
            return decorator(attr)

        return attr

    @staticmethod
    def _copy_instance_dict(instance_dict):
        result = {}
        ignored_fields = ["broker", "data_source"]
        for key in instance_dict:
            if key[0] != "_" and key not in ignored_fields:
                try:
                    result[key] = deepcopy(instance_dict[key])
                except:
                    logging.warning(
                        "Cannot perform deepcopy on %r" % instance_dict[key]
                    )

        return result

    def _safe_sleep_(self, sleeptime):
        """internal function for sleeping"""
        if not self._is_backtesting:
            time.sleep(sleeptime)

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
            self._name,
            symbol,
            quantity,
            side,
            limit_price=limit_price,
            stop_price=stop_price,
            time_in_force=time_in_force,
        )
        return order

    # =======Broker methods shortcuts============

    def get_timestamp(self):
        """return current timestamp"""
        return self.broker.get_timestamp()

    def get_datetime(self):
        """return current datetime"""
        return self.broker.get_datetime()

    def await_market_to_open(self):
        """Executes infinite loop until market opens"""
        self.broker.await_market_to_open()

    def await_market_to_close(self):
        """Sleep until market closes"""
        self.broker.await_market_to_close()

    def get_tracked_position(self, symbol):
        """get a tracked position given
        a symbol for the current strategy"""
        return self.broker.get_tracked_position(self._name, symbol)

    def get_tracked_positions(self):
        """get all tracked positions for the current strategy"""
        return self.broker.get_tracked_positions(self._name)

    def get_tracked_order(self, identifier):
        """get a tracked order given an identifier.
        Check that the order belongs to current strategy"""
        order = self.broker.get_tracked_order(identifier)
        if order.strategy == self._name:
            return order
        return None

    def get_tracked_orders(self):
        """get all tracked orders for a given strategy"""
        return self.broker.get_tracked_orders(self._name)

    def get_tracked_assets(self):
        """Get the list of symbols for positions
        and open orders for the current strategy"""
        return self.broker.get_tracked_assets(self._name)

    def get_asset_potential_total(self, symbol):
        """given current strategy and a symbol, check the ongoing
        position and the tracked order and returns the total
        number of shares provided all orders went through"""
        return self.broker.get_asset_potential_total(self._name, symbol)

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
        self.broker.cancel_open_orders(self._name)

    def sell_all(self, cancel_open_orders=True):
        """sell all strategy positions"""
        self.broker.sell_all(self._name, cancel_open_orders=cancel_open_orders)

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

    def get_symbol_bars(self, symbol, length, time_unit, time_delta=None):
        """Get bars for a given symbol"""
        return self.data_source.get_symbol_bars(
            symbol, length, time_unit, time_delta=time_delta
        )

    def get_bars(
        self,
        symbols,
        length,
        time_unit,
        time_delta=None,
        chunk_size=100,
        max_workers=200,
    ):
        """Get bars for the list of symbols"""
        return self.data_source.get_bars(
            symbols,
            length,
            time_unit,
            time_delta=time_delta,
            chunk_size=chunk_size,
            max_workers=max_workers,
        )

    # =======Helper methods=======================

    def format_log_message(self, message):
        message = "Strategy %s: %s" % (self._name, message)
        return message

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
        pass

    def before_market_closes(self):
        """Use this lifecycle method to execude code
        self.minutes_before_closing minutes before closing.
        Example: self.sell_all()"""
        pass

    def after_market_closes(self):
        """Use this lifecycle method to execute code
        after market closes. Exampling: dumping stats/reports"""
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

    def on_partially_filled_order(self, order):
        """Use this lifecycle event to execute code
        when an order has been partially filled by the broker"""
        pass

    def on_filled_order(self, position, order):
        """Use this lifecycle event to execute code
        when an order has been filled by the broker"""
        pass

    # ======Execution methods ====================

    def get_ready_to_close(self):
        return self._ready_to_close

    def set_ready_to_close(self, value=True):
        self._ready_to_close = value

    def _run_trading_session(self):
        if not self.broker.is_market_open():
            logging.info(
                self.format_log_message(
                    "Executing the before_market_opens lifecycle method"
                )
            )
            self.before_market_opens()

        self.broker.await_market_to_open()
        logging.info(
            self.format_log_message(
                "Executing the before_starting_trading lifecycle method"
            )
        )
        self.before_starting_trading()

        time_to_close = self.broker.get_time_to_close()
        while time_to_close > self.minutes_before_closing * 60:
            logging.info(
                self.format_log_message(
                    "Executing the on_trading_iteration lifecycle method"
                )
            )

            # Executing the on_trading_iteration lifecycle method
            # and tracking stats
            snapshot_before = self._copy_instance_dict(self.__dict__)
            self.on_trading_iteration()
            self.trace_stats(self._trading_context, snapshot_before)
            self._trading_context = None

            time_to_close = self.broker.get_time_to_close()
            sleeptime = time_to_close - 15 * 60
            sleeptime = max(min(sleeptime, 60 * self.sleeptime), 0)
            if sleeptime:
                logging.info(
                    self.format_log_message("Sleeping for %d seconds" % sleeptime)
                )
                self._safe_sleep_(sleeptime)

        if self.broker.is_market_open():
            logging.info(
                self.format_log_message(
                    "Executing the before_market_closes lifecycle method"
                )
            )
            self.before_market_closes()

        self.broker.await_market_to_close()
        logging.info(
            self.format_log_message(
                "Executing the after_market_closes lifecycle method"
            )
        )
        self.after_market_closes()

    def run(self):
        """The main execution point.
        Execute the lifecycle methods"""
        logging.info(
            self.format_log_message("Executing the initialize lifecycle method")
        )
        self.initialize()
        while self.broker.should_continue():
            try:
                self._run_trading_session()
            except Exception as e:
                logging.error(e)
                logging.error(traceback.format_exc())
                self.on_bot_crash(e)
                break

    @classmethod
    def backtest(
        cls,
        datasource_class,
        budget,
        backtesting_start,
        backtesting_end,
        logfile="logs/test.log",
    ):
        trader = Trader(logfile=logfile)
        data_source = datasource_class(backtesting_start, backtesting_end)
        backtesting_broker = BacktestingBroker(data_source)
        strategy = cls(budget=budget, broker=backtesting_broker)
        trader.add_strategy(strategy)
        trader.run_all()
