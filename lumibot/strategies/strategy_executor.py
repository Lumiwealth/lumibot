import logging
import time
import traceback
from datetime import datetime
from functools import wraps
from queue import Empty, Queue
from threading import Event, Lock, Thread

from lumibot.tools import append_locals, lumibot_time, staticdecorator


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
        self.result = {}

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

    @staticdecorator
    @staticmethod
    def sync_broker(func_input):
        @wraps(func_input)
        def func_output(self, *args, **kwargs):
            # Only audit the broker position during live trading.
            if self.broker.IS_BACKTESTING_BROKER:
                return func_input(self, *args, **kwargs)

            # Ensure that the orders are submitted to the broker before auditing.
            orders_queue_len = 1
            while orders_queue_len > 0:
                orders_queue_len = len(self.broker._orders_queue.queue)

            # Traps all new trade/order notifications to list broker._held_trades
            # Trapped at the broker._process_trade_event method
            self.broker._hold_trade_events = True

            # Get the snapshot.
            # If the _held_trades list is not empty, process these and then snapshot again
            # ensuring that the lumibot broker and the real broker should match.
            held_trades_len = 1
            cash_broker_max_retries = 3
            cash_broker_retries = 0
            while held_trades_len > 0:
                # Snapshot for the broker and lumibot:
                cash_broker = self.broker._get_balances_at_broker()
                if (
                    cash_broker is None
                    and cash_broker_retries < cash_broker_max_retries
                ):
                    logging.info("Unable to get cash from broker, trying again.")
                    cash_broker_retries += 1
                    continue
                elif (
                    cash_broker is None
                    and cash_broker_retries >= cash_broker_max_retries
                ):
                    logging.info(
                        f"Unable to get the cash balance after {cash_broker_max_retries} "
                        f"tries, setting cash to zero."
                    )
                    cash_broker = 0
                else:
                    cash_broker = cash_broker[0]

                positions_broker = self.broker._pull_positions(self.name)
                orders_broker = self.broker._pull_open_orders(self.name)

                held_trades_len = len(self.broker._held_trades)
                if held_trades_len > 0:
                    self.broker._hold_trade_events = False
                    self.broker.process_held_trades()
                    self.broker._hold_trade_events = True

            self.strategy._set_cash_position(cash_broker)

            # POSITIONS
            # Update Lumibot positions to match broker positions.
            # Any new trade notifications will not affect the sync as they
            # are being held pending the completion of the sync.
            if len(positions_broker) > 0:
                for position in positions_broker:
                    # Check against existing position.
                    position_lumi = [
                        pos_lumi
                        for pos_lumi in self.broker._filled_positions.get_list()
                        if pos_lumi.asset == position.asset
                    ]
                    position_lumi = position_lumi[0] if len(position_lumi) > 0 else None

                    if position_lumi:
                        # Compare to existing lumi position.
                        if position_lumi.quantity != position.quantity:
                            position_lumi.quantity = position.quantity
                    else:
                        # Add to positions in lumibot, position does not exist
                        # in lumibot.
                        if position.quantity != 0:
                            self.broker._filled_positions.append(position)
            else:
                # There are no positions at the broker, remove any positions
                # in lumibot.
                self.broker._filled_positions.remove_all()

            # Now iterate through lumibot positions.
            # Remove lumibot position if not at the broker.
            if len(positions_broker) < len(self.broker._filled_positions.get_list()):
                for position in self.broker._filled_positions.get_list():
                    if position not in positions_broker:
                        self.broker._filled_positions.remove(position)

            # ORDERS
            if len(orders_broker) > 0:
                orders_lumi = self.broker._tracked_orders

                # Check orders at the broker against those in lumibot.
                for order in orders_broker:
                    if self.strategy._first_iteration:
                        self.broker._process_new_order(order)
                    else:
                        # Check against existing orders.
                        order_lumi = [
                            ord_lumi
                            for ord_lumi in orders_lumi
                            if ord_lumi.identifier == order.identifier
                        ]
                        order_lumi = order_lumi[0] if len(order_lumi) > 0 else None

                        if order_lumi:
                            # Compare the orders.
                            if order_lumi.quantity != order.quantity:
                                order_lumi.quantity = order.quantity
                            order_attrs = [
                                # "position_filled",
                                # "status",
                                "limit_price"
                            ]
                            for order_attr in order_attrs:
                                olumi = getattr(order_lumi, order_attr)
                                obroker = getattr(order, order_attr)
                                if olumi != obroker:
                                    setattr(order_lumi, order_attr, obroker)
                                    logging.warning(
                                        f"We would adjust {order_lumi}, {order_attr}, to be {obroker} her."
                                    )
                        else:
                            # Add to order in lumibot.
                            self.broker._process_new_order(order)

                for order_lumi in orders_lumi:
                    # Remove lumibot orders if not in broker.
                    if order_lumi.identifier not in [
                        order.identifier for order in orders_broker
                    ]:
                        self.broker._process_trade_event(order_lumi, "canceled")

            self.broker._hold_trade_events = False
            self.broker.process_held_trades()
            return func_input(self, *args, **kwargs)

        return func_output

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
            multiplier = payload["multiplier"]

            if order.asset.asset_type != "crypto":
                self.strategy._update_cash(order.side, quantity, price, multiplier)
            self._on_filled_order(**payload)
        elif event == self.PARTIALLY_FILLED_ORDER:
            order = payload["order"]
            price = payload["price"]
            quantity = payload["quantity"]
            multiplier = payload["multiplier"]

            if order.asset.asset_type != "crypto":
                self.strategy._update_cash(order.side, quantity, price, multiplier)
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
        if context is None:
            logging.warning(
                "on_trading_iteration context is not available. "
                "The context is generally unavailable whe debugging "
                "with IDEs like pycharm etc..."
            )
            result = {}
        else:
            result = self.strategy.trace_stats(context, snapshot_before)

        result["datetime"] = self.strategy.get_datetime()
        result["portfolio_value"] = self.strategy.portfolio_value
        result["cash"] = self.strategy.cash
        self.strategy._append_row(result)
        return result

    # =======Lifecycle methods====================

    @lifecycle_method
    def _initialize(self):
        self.strategy.log_message("Executing the initialize lifecycle method")
        self.strategy.initialize(**self.strategy.parameters)

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
    @sync_broker
    def _on_trading_iteration(self):
        self._strategy_context = None
        self.strategy.log_message("Executing the on_trading_iteration lifecycle method")
        on_trading_iteration = append_locals(self.strategy.on_trading_iteration)
        on_trading_iteration()
        self.strategy._first_iteration = False
        self._strategy_context = on_trading_iteration.locals
        self.strategy._last_on_trading_iteration_datetime = datetime.now()
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
    def _on_partially_filled_order(self, position, order, price, quantity, multiplier):
        self.strategy.log_message(
            "Executing the on_partially_filled_order event method"
        )
        self.strategy.on_partially_filled_order(
            position, order, price, quantity, multiplier
        )

    @event_method
    def _on_filled_order(self, position, order, price, quantity, multiplier):
        self.strategy.log_message("Executing the on_filled_order event method")
        self.strategy.on_filled_order(position, order, price, quantity, multiplier)

        # Let our listener know that an order has been filled (set in the callback)
        if hasattr(self.strategy, "_filled_order_callback") and callable(
            self.strategy._filled_order_callback
        ):
            self.strategy._filled_order_callback(
                self, position, order, price, quantity, multiplier
            )

    # ======Execution methods ====================
    def _run_trading_session(self):
        """This is really intraday trading method. Timeframes of less than a day, seconds,
        minutes, hours.
        """
        has_data_source = hasattr(self.broker, "_data_source")
        is_247 = hasattr(self.broker, "market") and self.broker.market == "24/7"
        # Process pandas daily and get out.
        if (
            has_data_source
            and self.broker._data_source.SOURCE == "PANDAS"
            and self.broker._data_source._timestep == "day"
        ):
            if self.broker._data_source._iter_count is None:
                # Get the first date from _date_index equal or greater than
                # backtest start date.
                dates = self.broker._data_source._date_index
                self.broker._data_source._iter_count = dates.get_loc(
                    dates[dates > self.broker.datetime][0]
                )
            else:
                self.broker._data_source._iter_count += 1

            datetime = self.broker._data_source._date_index[
                self.broker._data_source._iter_count
            ]

            self.broker._update_datetime(datetime)
            # Is this update money dividends in the right place? Maybe after orders. or both
            if self.broker.IS_BACKTESTING_BROKER:
                self.broker.process_pending_orders(strategy=self.strategy.name)
            self.strategy._update_cash_with_dividends()
            self._on_trading_iteration()
            return

        if not is_247 and (
            not has_data_source
            or (has_data_source and self.broker._data_source.SOURCE != "PANDAS")
        ):
            self.strategy.await_market_to_open()  # set new time and bar length. Check if hit bar max
            # or date max.
            if not self.broker.is_market_open():
                self._before_market_opens()
            self.strategy._update_cash_with_dividends()

        if not is_247:
            self.strategy.await_market_to_open(timedelta=0)
            self._before_starting_trading()

        if not is_247:
            time_to_close = self.broker.get_time_to_close()

        #####
        # The main loop for backtesting if strategy is 24 hours
        ####
        while is_247 or (time_to_close > self.strategy.minutes_before_closing * 60):
            # Stop after we pass the backtesting end date
            if (
                self.broker.IS_BACKTESTING_BROKER
                and self.broker.datetime.date()
                > self.broker._data_source.datetime_end.date()
            ):
                break

            if self.broker.IS_BACKTESTING_BROKER:
                self.broker.process_pending_orders(strategy=self.strategy.name)
            self._on_trading_iteration()

            # Set the sleeptime to close.
            if is_247:
                sleeptime = float("inf")
            else:
                time_to_close = self.broker.get_time_to_close()
                sleeptime = time_to_close - self.strategy.minutes_before_closing * 60

            sleeptime_err_msg = (
                f"You can set the sleep time as an integer which will be interpreted as "
                f"minutes. eg: sleeptime = 50 would be 50 minutes. Conversely, you can enter "
                f"the time as a string with the duration numbers first, followed by the time "
                f"units: 'M' for minutes, 'S' for seconds eg: '300S' is 300 seconds."
            )
            if isinstance(self.strategy.sleeptime, int):
                units = "M"
                time = self.strategy.sleeptime
            elif isinstance(self.strategy.sleeptime, str):
                units = self.strategy.sleeptime[-1:]
                time = int(self.strategy.sleeptime[:-1])
            else:
                raise ValueError(sleeptime_err_msg)

            if units not in "SMHD":
                raise ValueError(sleeptime_err_msg)

            if units == "S":
                strategy_sleeptime = time
            elif units == "M":
                strategy_sleeptime = 60 * time
            elif units == "H":
                strategy_sleeptime = 60 * 60 * time
            elif units == "D":
                strategy_sleeptime = 60 * 60 * 24 * time
            else:
                strategy_sleeptime = time

            sleeptime = max(min(sleeptime, strategy_sleeptime), 0)
            if not self.should_continue or sleeptime == 0:
                break
            else:
                self.strategy.log_message("Sleeping for %d seconds" % sleeptime)
                self.safe_sleep(sleeptime)

        self.strategy.await_market_to_close()
        if self.broker.is_market_open():
            self._before_market_closes()  # perhaps the user could set the time of day based on
            # their data that the market closes?

        self.strategy.await_market_to_close(timedelta=0)
        self._after_market_closes()

    def run(self):
        # Overloading the broker sleep method
        self.broker.sleep = self.safe_sleep

        self._initialize()

        #####
        # The main loop for running any strategy
        ####
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
