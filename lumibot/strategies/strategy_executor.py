import inspect
import logging
import time
import traceback
from datetime import datetime, timedelta
from functools import wraps
from queue import Empty, Queue
from threading import Event, Lock, Thread

from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from lumibot.tools import append_locals, get_trading_days, staticdecorator
from termcolor import colored


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

        # Create a dictionary of job stores. A job store is where the scheduler persists its jobs. In this case,
        # we create an in-memory job store for "default" and "On_Trading_Iteration" which is the job store we will
        # use to store jobs for the main on_trading_iteration method.
        job_stores = {"default": MemoryJobStore(),
                      "On_Trading_Iteration": MemoryJobStore()}

        # Instantiate a BackgroundScheduler with the job stores we just defined. This scheduler will be used to store
        # the jobs that we create later and execute them at the correct time.
        self.scheduler = BackgroundScheduler(jobstores=job_stores)

        # Initialize a target count and a current count for cron jobs to 0.
        # These are used to determine when to execute the on_trading_iteration method.
        self.cron_count_target = 0
        self.cron_count = 0

        # Create an Event object for the check queue stop event.
        self.check_queue_stop_event = Event()

    @property
    def name(self):
        return self.strategy._name

    @property
    def should_continue(self):
        return not self.stop_event.is_set()

    def check_queue(self):
        # Define a function that checks the queue and processes the queue. This is run continuously in a separate
        # thread in live.
        while not self.check_queue_stop_event.is_set():
            try:
                self.process_queue()
            except Empty:
                pass
            time.sleep(1)

    def safe_sleep(self, sleeptime):
        # This method should only be run in back testing. If it's running during live, something has gone wrong.

        if self.strategy.is_backtesting:
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
                cash_broker = self.broker._get_balances_at_broker(
                    self.strategy.quote_asset
                )
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

                if cash_broker is not None:
                    self.strategy._set_cash_position(cash_broker)

                positions_broker = self.broker._pull_positions(self.strategy)
                orders_broker = self.broker._pull_open_orders(self.name, self.strategy)

                held_trades_len = len(self.broker._held_trades)
                if held_trades_len > 0:
                    self.broker._hold_trade_events = False
                    self.broker.process_held_trades()
                    self.broker._hold_trade_events = True

            # POSITIONS
            # Update Lumibot positions to match broker positions.
            # Any new trade notifications will not affect the sync as they
            # are being held pending the completion of the sync.
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

            # Now iterate through lumibot positions.
            # Remove lumibot position if not at the broker.
            for position in self.broker._filled_positions.get_list():
                found = False
                for position_broker in positions_broker:
                    if position_broker.asset == position.asset:
                        found = True
                        break
                if not found and position.asset != self.strategy.quote_asset:
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
                                        f"We are adjusting the {order_attr} of the order {order_lumi}, from {olumi} "
                                        f"to be {obroker} because what we have in memory does not match the broker."
                                    )
                        else:
                            # Add to order in lumibot.
                            self.broker._process_new_order(order)

                for order_lumi in orders_lumi:
                    # Remove lumibot orders if not in broker.
                    if order_lumi.identifier not in [
                        order.identifier for order in orders_broker
                    ]:
                        logging.info(f"Cannot find order {order_lumi} (id={order_lumi.identifier}) in broker, "
                                     f"canceling.")
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

        # Do this for backwards compatibility.
        initialize_argspecs = inspect.getfullargspec(self.strategy.initialize)
        args = initialize_argspecs.args
        safe_params_to_pass = {}
        for arg in args:
            if arg in self.strategy.parameters and arg != "self":
                safe_params_to_pass[arg] = self.strategy.parameters[arg]
        self.strategy.initialize(**safe_params_to_pass)

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
        # If we are running live, we need to check if it's time to execute the trading iteration.
        if not self.strategy.is_backtesting:
            # Increase the cron count by 1.
            self.cron_count += 1

            # If the cron count is equal to the cron count target, reset the cron count to 0 and continue (execute
            # the on_trading_iteration method).
            if self.cron_count >= self.cron_count_target:
                self.cron_count = 0
            else:
                # If the cron count is not equal to the cron count target, return and do not execute the
                # on_trading_iteration method.
                return

        now = datetime.now()

        # Check if we are in market hours.
        if not self.broker.is_market_open():
            self.strategy.log_message(
                "The market is not currently open, skipping this trading iteration", color="blue")
            return

        start_time = now.strftime("%Y-%m-%d %H:%M:%S")

        self._strategy_context = None
        self.strategy.log_message(f"Executing the on_trading_iteration lifecycle method at {start_time}", color="blue")
        on_trading_iteration = append_locals(self.strategy.on_trading_iteration)

        # Time-consuming
        on_trading_iteration()

        self.strategy._first_iteration = False
        self._strategy_context = on_trading_iteration.locals
        self.strategy._last_on_trading_iteration_datetime = datetime.now()
        self.process_queue()

        end_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        next_run_time = self.get_next_ap_scheduler_run_time()
        if next_run_time is not None:
            # Format the date to be used in the log message.
            dt_str = next_run_time.strftime("%Y-%m-%d %H:%M:%S")
            self.strategy.log_message(
                f"Trading iteration ended at {end_time}, next run time scheduled at {dt_str}", color="blue")

        else:
            self.strategy.log_message(f"Trading iteration ended at {end_time}", color="blue")

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
        if self.broker.IS_BACKTESTING_BROKER:
            self.strategy._dump_stats()

    def _on_abrupt_closing(self, error):
        """Use this lifecycle event to execute code
        when the main trader was shut down (Keyboard Interuption, ...)
        Example: self.sell_all()"""
        self.strategy.log_message("Executing the on_abrupt_closing event method")
        self.strategy.on_abrupt_closing()
        self.strategy._dump_stats()

    @event_method
    def _on_new_order(self, order):
        self.strategy.on_new_order(order)

    @event_method
    def _on_canceled_order(self, order):
        self.strategy.on_canceled_order(order)

    @event_method
    def _on_partially_filled_order(self, position, order, price, quantity, multiplier):
        self.strategy.on_partially_filled_order(
            position, order, price, quantity, multiplier
        )

    @event_method
    def _on_filled_order(self, position, order, price, quantity, multiplier):
        self.strategy.on_filled_order(position, order, price, quantity, multiplier)

        # Let our listener know that an order has been filled (set in the callback)
        if hasattr(self.strategy, "_filled_order_callback") and callable(
                self.strategy._filled_order_callback
        ):
            self.strategy._filled_order_callback(
                self, position, order, price, quantity, multiplier
            )

    # This method calculates the trigger for the strategy based on the 'sleeptime' attribute of the strategy.
    def calculate_strategy_trigger(self, force_start_immediately=True):
        """Calculate the trigger for the strategy based on the 'sleeptime' attribute of the strategy.

        Parameters
        ----------

        force_start_immediately : bool, optional
            When sleeptime is in days (eg. self.sleeptime = "1D") Whether to start the strategy immediately or wait until the market opens. The default is True.
        """

        # Define a standard error message about acceptable formats for 'sleeptime'.
        sleeptime_err_msg = (
            f"You can set the sleep time as an integer which will be interpreted as "
            f"minutes. eg: sleeptime = 50 would be 50 minutes. Conversely, you can enter "
            f"the time as a string with the duration numbers first, followed by the time "
            f"units: 'M' for minutes, 'S' for seconds eg: '300S' is 300 seconds."
        )
        # Check the type of 'sleeptime'. If it's an integer, it is interpreted as minutes.
        # If it's a string, the last character is taken as the unit of time, and the rest is converted to an integer.
        if isinstance(self.strategy.sleeptime, int):
            units = "M"
            time_raw = self.strategy.sleeptime
        elif isinstance(self.strategy.sleeptime, str):
            units = self.strategy.sleeptime[-1:]
            time_raw = int(self.strategy.sleeptime[:-1])
        else:
            raise ValueError(sleeptime_err_msg)  # If it's neither, raise an error with the defined message.

        # Check if the units are valid (S for seconds, M for minutes, H for hours, D for days).
        if units not in "SMHDsmhd":
            raise ValueError(sleeptime_err_msg)

        # Assign the raw time to the target count for cron jobs so that later we can compare the current count to the
        # target count.
        self.cron_count_target = time_raw

        # Create a dictionary to define the cron trigger based on the units of time.
        kwargs = {}
        if units in "Ss":
            kwargs['second'] = "*"
        elif units in "Mm":
            kwargs['minute'] = "*"
        elif units in "Hh":
            kwargs['hour'] = "*"

            # Start immediately (at the closest minute) if force_start_immediately is True
            if force_start_immediately:
                # Get the current time in local timezone
                local_time = datetime.now().astimezone()

                # Add one minute to the local_time
                local_time = local_time + timedelta(minutes=1)

                # Get the minute
                minute = local_time.minute

                # Minute with 0 in front if less than 10
                kwargs['minute'] = f"0{minute}" if minute < 10 else str(minute)

        elif units in "Dd":
            kwargs['day'] = "*"

            # Start immediately (at the closest minute) if force_start_immediately is True
            if force_start_immediately:
                # Get the current time in local timezone
                local_time = datetime.now().astimezone()

                # Add one minute to the local_time
                local_time = local_time + timedelta(minutes=1)

                # Get the hour
                hour = local_time.hour

                # Get the minute
                minute = local_time.minute

                # Hour with 0 in front if less than 10
                kwargs['hour'] = f"0{hour}" if hour < 10 else str(hour)
                # Minute with 0 in front if less than 10
                kwargs['minute'] = f"0{minute}" if minute < 10 else str(minute)

            # Start at the market open time
            else:
                # Get the market hours for the strategy
                open_time_this_day = self.broker.utc_to_local(
                    self.broker.market_hours(close=False, next=False)
                )

                # Get the hour
                hour = open_time_this_day.hour

                # Get the minute
                minute = open_time_this_day.minute

                # Hour with 0 in front if less than 10
                kwargs['hour'] = f"0{hour}" if hour < 10 else str(hour)
                # Minute with 0 in front if less than 10
                kwargs['minute'] = f"0{minute}" if minute < 10 else str(minute)

                logging.warning(
                    f"The strategy will run at {kwargs['hour']}:{kwargs['minute']} every day. If instead you want to start right now and run every {time_raw} days then set force_start_immediately=True in the strategy's initialization.")

        # Return a CronTrigger object with the calculated settings.
        return CronTrigger(**kwargs)

    # TODO: speed up this function, it's a major bottleneck for backtesting
    def _strategy_sleep(self):
        """ Sleep for the strategy's sleep time """

        is_247 = hasattr(self.broker, "market") and self.broker.market == "24/7"

        # Set the sleeptime to close.
        if is_247:
            time_to_before_closing = float("inf")
        else:
            # TODO: next line speed implication: v high (2233 microseconds) get_time_to_close()
            time_to_close = self.broker.get_time_to_close()

            time_to_before_closing = (
                time_to_close - self.strategy.minutes_before_closing * 60
            )

        sleeptime_err_msg = (
            f"You can set the sleep time as an integer which will be interpreted as "
            f"minutes. eg: sleeptime = 50 would be 50 minutes. Conversely, you can enter "
            f"the time as a string with the duration numbers first, followed by the time "
            f"units: 'M' for minutes, 'S' for seconds eg: '300S' is 300 seconds."
        )
        if isinstance(self.strategy.sleeptime, int):
            units = "M"
            time_raw = self.strategy.sleeptime
        elif isinstance(self.strategy.sleeptime, str):
            units = self.strategy.sleeptime[-1:]
            time_raw = int(self.strategy.sleeptime[:-1])
        else:
            raise ValueError(sleeptime_err_msg)

        if units not in "SMHDsmhd":
            raise ValueError(sleeptime_err_msg)

        if units == "S" or units == "s":
            strategy_sleeptime = time_raw
        elif units == "M" or units == "m":
            strategy_sleeptime = 60 * time_raw
        elif units == "H" or units == "h":
            strategy_sleeptime = 60 * 60 * time_raw
        elif units == "D" or units == "d":
            strategy_sleeptime = 60 * 60 * 24 * time_raw
        else:
            strategy_sleeptime = time_raw

        if (
                not self.should_continue
                or strategy_sleeptime == 0
                or time_to_before_closing <= 0
        ):
            return False
        else:
            self.strategy.log_message(
                colored(f"Sleeping for {strategy_sleeptime} seconds", color="blue")
            )
            # TODO: next line speed implication: medium (371 microseconds)
            self.safe_sleep(strategy_sleeptime)

        return True

    # ======Execution methods ====================
    def _run_trading_session(self):
        """This is really intraday trading method. Timeframes of less than a day, seconds,
        minutes, hours.
        """

        has_data_source = hasattr(self.broker, "_data_source")
        is_247 = hasattr(self.broker, "market") and self.broker.market == "24/7"

        # Set the time_to_close variable to infinity if the market is 24/7.
        if is_247:
            time_to_close = float("inf")

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

            dt = self.broker._data_source._date_index[
                self.broker._data_source._iter_count
            ]

            self.broker._update_datetime(dt)

            self.strategy._update_cash_with_dividends()

            self._on_trading_iteration()

            if self.broker.IS_BACKTESTING_BROKER:
                self.broker.process_pending_orders(strategy=self.strategy)
            return

        if not is_247:
            # Set date to the start date, but account for minutes_before_opening
            self.strategy.await_market_to_open()  # set new time and bar length. Check if hit bar max or date max.

            if not has_data_source or (
                    has_data_source and self.broker._data_source.SOURCE != "PANDAS"
            ):
                self.strategy._update_cash_with_dividends()

            if not self.broker.is_market_open():
                self._before_market_opens()

            # Now go to the actual open without considering minutes_before_opening
            self.strategy.await_market_to_open(timedelta=0)
            self._before_starting_trading()

            time_to_close = self.broker.get_time_to_close()

        if not self.strategy.is_backtesting:
            # Start APScheduler for the trading session.
            if not self.scheduler.running:
                self.scheduler.start()

                # Choose the cron trigger for the strategy based on the desired sleep time.
                chosen_trigger = self.calculate_strategy_trigger(
                    force_start_immediately=self.strategy.force_start_immediately)

                # Add the on_trading_iteration method to the scheduler with the chosen trigger.
                self.scheduler.add_job(self._on_trading_iteration, chosen_trigger, id="OTIM",
                                       name="On Trading Iteration Main Thread", jobstore="On_Trading_Iteration")

            # Get the time to close.
            time_to_close = self.broker.get_time_to_close()

            # Check if it's time to stop the strategy based on the time to close and the strategy's minutes before
            # closing.
            should_we_stop = (time_to_close <= self.strategy.minutes_before_closing * 60)

            # Start the check_queue thread which will run continuously in the background, checking if any items have
            # been added to the queue and executing them.
            check_queue_thread = Thread(target=self.check_queue)
            check_queue_thread.start()

            next_run_time = self.get_next_ap_scheduler_run_time()
            if next_run_time is not None:
                # Format the date to be used in the log message.
                dt_str = next_run_time.strftime("%Y-%m-%d %H:%M:%S")
                self.strategy.log_message(f"Strategy will start running at: {dt_str}", color="blue")

            # Loop until the strategy should stop.
            while True:
                # Get the current jobs from the scheduler.
                jobs = self.scheduler.get_jobs()

                # Check if the broker should continue.
                broker_continue = self.broker.should_continue()
                # Check if the strategy should continue.
                should_continue = self.should_continue
                # Check if the strategy is 24/7 or if it's time to stop.
                is_247_or_should_we_stop = not is_247 or not should_we_stop

                if not jobs:
                    print("Breaking loop because no jobs.")
                    break
                if not broker_continue:
                    print("Breaking loop because broker should not continue.")
                    break
                if not should_continue:
                    print("Breaking loop because should not continue.")
                    break
                if not is_247_or_should_we_stop:
                    print("Breaking loop because it's 24/7 and time to stop.")
                    break

                time.sleep(1)  # Sleep to save CPU

        #####
        # The main loop for backtesting if strategy is 24 hours
        ####
        # TODO: speed up this loop for backtesting (it's a major bottleneck)

        if self.strategy.is_backtesting:

            while is_247 or (time_to_close > self.strategy.minutes_before_closing * 60):
                # Stop after we pass the backtesting end date
                if (
                        self.broker.IS_BACKTESTING_BROKER
                        and self.broker.datetime > self.broker._data_source.datetime_end
                ):
                    break

                # TODO: next line speed implication: v high (7563 microseconds) _on_trading_iteration()
                self._on_trading_iteration()

                if self.broker.IS_BACKTESTING_BROKER:
                    self.broker.process_pending_orders(strategy=self.strategy)

                # Sleep until the next trading iteration
                # TODO: next line speed implication: high (2625 microseconds) _strategy_sleep()

                if not self._strategy_sleep():
                    break

        self.strategy.await_market_to_close()
        if self.broker.is_market_open():
            self._before_market_closes()  # perhaps the user could set the time of day based on their data that the market closes?

        self.strategy.await_market_to_close(timedelta=0)
        self._after_market_closes()

    def get_next_ap_scheduler_run_time(self):
        # Check if scheduler object exists.
        if self.scheduler is None or not isinstance(self.scheduler, BackgroundScheduler):
            return None

        # Get the current jobs from the scheduler.
        jobs = self.scheduler.get_jobs()

        if not jobs or len(jobs) == 0:
            return None

        # Log the next run time of the on_trading_iteration method.
        next_run_time = jobs[0].next_run_time

        return next_run_time

    def run(self):
        # Overloading the broker sleep method
        self.broker.sleep = self.safe_sleep

        self._initialize()

        # Get the trading days based on the market that the strategy is trading on
        market = self.broker.market
        self.broker._trading_days = get_trading_days(market)

        #####
        # The main loop for running any strategy
        ####
        while self.broker.should_continue() and self.should_continue:
            try:
                self._run_trading_session()
            except Exception as e:
                # The bot crashed so log the error, call the on_bot_crash method, and continue
                logging.error(e)
                logging.error(traceback.format_exc())
                try:
                    self._on_bot_crash(e)
                except Exception as e1:
                    logging.error(e1)
                    logging.error(traceback.format_exc())

                # In BackTesting, we want to stop the bot if it crashes so there isn't an infinite loop
                if self.strategy.is_backtesting:
                    raise RuntimeError("Exception encountered, stopping BackTest.") from e

                # Only stop the strategy if it's time, otherwise keep running the bot
                if not self._strategy_sleep():
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
