import logging
import time
import traceback
from functools import wraps
from queue import Empty, Queue
from threading import Event, Lock, Thread

from lumibot.tools import append_locals, staticdecorator


class StrategyExecutor(Thread):
    # Trading events flags
    NEW_ORDER = "new"
    CANCELED_ORDER = "canceled"
    FILLED_ORDER = "fill"
    PARTIALLY_FILLED_ORDER = "partial_fill"

    def __init__(self, strategy):
        super(StrategyExecutor, self).__init__()
        self.daemon = True
        self.stop_event = Event()
        self.lock = Lock()
        self.queue = Queue()

        self.strategy = strategy
        self._strategy_context = None
        self.broker = self.strategy.broker
        self.minutes_before_closing = self.strategy.minutes_before_closing
        self.result = {}

        # Overloading the default time.sleep method
        # In case a user is using it for backtesting
        time.sleep = self.safe_sleep

    @property
    def name(self):
        return self.strategy._name

    @property
    def should_continue(self):
        return not self.stop_event.isSet()

    def safe_sleep(self, sleeptime):
        """internal function for sleeping"""
        if not self.broker.IS_BACKTESTING_BROKER:
            start = time.time()
            end = start + sleeptime

            while self.should_continue:
                # Setting timeout to 1s to allow listening
                # to key interrupts
                timeout = min(end - time.time(), 1)
                if timeout <= 0:
                    break
                try:
                    event, payload = self.queue.get(timeout=timeout)
                    self.process_event(event, payload)
                except Empty:
                    pass
        else:
            self.process_queue()
            self.broker._update_datetime(sleeptime)

    def add_event(self, event_name, payload):
        self.queue.put((event_name, payload))

    def process_event(self, event, payload):
        if event == self.NEW_ORDER:
            self._on_new_order(**payload)
        elif event == self.CANCELED_ORDER:
            self._on_canceled_order(**payload)
        elif event == self.FILLED_ORDER:
            order = payload["order"]
            price = payload["price"]
            quantity = payload["quantity"]
            self.strategy._update_unspent_money(order.side, quantity, price)
            self._on_filled_order(**payload)
        elif event == self.PARTIALLY_FILLED_ORDER:
            order = payload["order"]
            price = payload["price"]
            quantity = payload["quantity"]
            self.strategy._update_unspent_money(order.side, quantity, price)
            self._on_partially_filled_order(**payload)

    def process_queue(self):
        while not self.queue.empty():
            event, payload = self.queue.get()
            self.process_event(event, payload)

    def stop(self):
        self.stop_event.set()
        self._on_abrupt_closing(KeyboardInterrupt())

    def join(self, timeout=None):
        super(StrategyExecutor, self).join(timeout)

    # =======Decorators===========================

    def _before_lifecycle_method(self):
        self.process_queue()

    def _after_lifecycle_method(self):
        self.process_queue()

    @staticdecorator
    @staticmethod
    def lifecycle_method(func_input):
        @wraps(func_input)
        def func_output(self, *args, **kwargs):
            if self.should_continue:
                self._before_lifecycle_method()
                result = func_input(self, *args, **kwargs)
                self._after_lifecycle_method()
                return result

        return func_output

    @staticdecorator
    @staticmethod
    def event_method(func_input):
        @wraps(func_input)
        def func_output(self, *args, **kwargs):
            if self.should_continue:
                result = func_input(self, *args, **kwargs)
                return result

        return func_output

    @staticdecorator
    @staticmethod
    def trace_stats(func_input):
        @wraps(func_input)
        def func_output(self, *args, **kwargs):
            self.strategy._update_portfolio_value()
            snapshot_before = self.strategy._copy_dict()
            result = func_input(self, *args, **kwargs)
            self._trace_stats(self._strategy_context, snapshot_before)
            return result

        return func_output

    def _trace_stats(self, context, snapshot_before):
        result = self.strategy.trace_stats(context, snapshot_before)
        result["datetime"] = self.strategy.get_datetime()
        result["portfolio_value"] = self.strategy.portfolio_value
        result["unspent_money"] = self.strategy.unspent_money
        self.strategy._append_row(result)
        return result

    # =======Lifecycle methods====================

    @lifecycle_method
    def _initialize(self):
        self.strategy.log_message("Executing the initialize lifecycle method")
        self.strategy.initialize()

    @lifecycle_method
    def _before_market_opens(self):
        self.strategy.log_message("Executing the before_market_opens lifecycle method")
        self.strategy.before_market_opens()

    @lifecycle_method
    def _before_starting_trading(self):
        self.strategy.log_message(
            "Executing the before_starting_trading lifecycle method"
        )
        self.strategy.before_starting_trading()

    @lifecycle_method
    @trace_stats
    def _on_trading_iteration(self):
        self._strategy_context = None
        self.strategy.log_message("Executing the on_trading_iteration lifecycle method")
        on_trading_iteration = append_locals(self.strategy.on_trading_iteration)
        on_trading_iteration()
        self._strategy_context = on_trading_iteration.locals
        self.process_queue()

    @lifecycle_method
    def _before_market_closes(self):
        self.strategy.log_message("Executing the before_market_closes lifecycle method")
        self.strategy.before_market_closes()

    @lifecycle_method
    def _after_market_closes(self):
        self.strategy.log_message("Executing the after_market_closes lifecycle method")
        self.strategy.after_market_closes()

    @lifecycle_method
    def _on_strategy_end(self):
        self.strategy.log_message("Executing the on_strategy_end lifecycle method")
        self.strategy.on_strategy_end()
        self.strategy._dump_stats()

    # ======Events methods========================

    @event_method
    def _on_bot_crash(self, error):
        """Use this lifecycle event to execute code
        when an exception is raised and the bot crashes"""
        self.strategy.log_message("Executing the on_bot_crash event method")
        self.strategy.on_bot_crash(error)
        self.strategy._dump_stats()

    def _on_abrupt_closing(self, error):
        """Use this lifecycle event to execute code
        when the main trader was shut down (Keybord Interuption, ...)
        Example: self.sell_all()"""
        self.strategy.log_message("Executing the on_abrupt_closing event method")
        self.strategy.on_abrupt_closing()
        self.strategy._dump_stats()

    @event_method
    def _on_new_order(self, order):
        self.strategy.log_message("Executing the on_new_order event method")
        self.strategy.on_new_order(order)

    @event_method
    def _on_canceled_order(self, order):
        self.strategy.log_message("Executing the on_canceled_order event method")
        self.strategy.on_canceled_order(order)

    @event_method
    def _on_partially_filled_order(self, order, price, quantity):
        self.strategy.log_message(
            "Executing the on_partially_filled_order event method"
        )
        self.strategy.on_partially_filled_order(order, price, quantity)

    @event_method
    def _on_filled_order(self, position, order, price, quantity):
        self.strategy.log_message("Executing the on_filled_order event method")
        self.strategy.on_filled_order(position, order, price, quantity)

    # ======Execution methods ====================

    def _run_trading_session(self):
        if not self.broker.is_market_open():
            self._before_market_opens()

        self.broker.await_market_to_open()
        self.strategy._update_unspent_money_with_dividends()
        self._before_starting_trading()

        time_to_close = self.broker.get_time_to_close()
        while time_to_close > self.minutes_before_closing * 60:
            self._on_trading_iteration()
            time_to_close = self.broker.get_time_to_close()
            sleeptime = time_to_close - self.minutes_before_closing * 60
            sleeptime = max(min(sleeptime, 60 * self.strategy.sleeptime), 0)
            if not self.should_continue or sleeptime == 0:
                break
            else:
                self.strategy.log_message("Sleeping for %d seconds" % sleeptime)
                self.safe_sleep(sleeptime)

        if self.broker.is_market_open():
            self._before_market_closes()

        self.broker.await_market_to_close()
        self._after_market_closes()

    def run(self):
        self._initialize()
        while self.broker.should_continue() and self.should_continue:
            try:
                self._run_trading_session()
            except Exception as e:
                logging.error(e)
                logging.error(traceback.format_exc())
                self._on_bot_crash(e)
                self.result = self.strategy._analysis
                return False

        try:
            self._on_strategy_end()
        except Exception as e:
            logging.error(e)
            logging.error(traceback.format_exc())
            self._on_bot_crash(e)
            self.result = self.strategy._analysis
            return False

        self.result = self.strategy._analysis
        return True
