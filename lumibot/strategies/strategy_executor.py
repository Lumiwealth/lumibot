import inspect
import time
import traceback
from datetime import datetime, timedelta
from functools import wraps
from queue import Empty, Queue
from threading import Event, Lock, Thread

from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from termcolor import colored

from lumibot.entities import Asset, Order
from lumibot.tools import append_locals, get_trading_days, staticdecorator


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
        job_stores = {"default": MemoryJobStore(), "On_Trading_Iteration": MemoryJobStore()}

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
            time.sleep(0.5)

    def safe_sleep(self, sleeptime):
        # This method should only be run in back testing. If it's running during live, something has gone wrong.

        if self.strategy.is_backtesting:
            self.process_queue()
            self.broker._update_datetime(
                sleeptime, cash=self.strategy.cash, portfolio_value=self.strategy.portfolio_value
            )

    def sync_broker(self):
        # Only audit the broker position during live trading.
        if self.broker.IS_BACKTESTING_BROKER:
            return

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
        orders_broker = []
        positions_broker = []
        while held_trades_len > 0:
            # Snapshot for the broker and lumibot:
            cash_broker = self.broker._get_balances_at_broker(self.strategy.quote_asset)
            if cash_broker is None and cash_broker_retries < cash_broker_max_retries:
                self.strategy.logger.info("Unable to get cash from broker, trying again.")
                cash_broker_retries += 1
                continue
            elif cash_broker is None and cash_broker_retries >= cash_broker_max_retries:
                self.strategy.logger.info(
                    f"Unable to get the cash balance after {cash_broker_max_retries} "
                    f"tries, setting cash to zero."
                )
                cash_broker = 0
            else:
                cash_broker = cash_broker[0]

            if cash_broker is not None:
                self.strategy._set_cash_position(cash_broker)

            held_trades_len = len(self.broker._held_trades)
            if held_trades_len > 0:
                self.broker._hold_trade_events = False
                self.broker.process_held_trades()
                self.broker._hold_trade_events = True

        # POSITIONS
        # Update Lumibot positions to match broker positions.
        # Any new trade notifications will not affect the sync as they
        # are being held pending the completion of the sync.
        self.broker.sync_positions(self.strategy)

        # ORDERS
        orders_broker = self.broker._pull_all_orders(self.name, self.strategy)
        if len(orders_broker) > 0:
            orders_lumi = self.broker.get_all_orders()

            # Check orders at the broker against those in lumibot.
            for order in orders_broker:
                # Check against existing orders.
                order_lumi = [ord_lumi for ord_lumi in orders_lumi if ord_lumi.identifier == order.identifier]
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
                            self.strategy.logger.warning(
                                f"We are adjusting the {order_attr} of the order {order_lumi}, from {olumi} "
                                f"to be {obroker} because what we have in memory does not match the broker."
                            )
                else:
                    # If it is the brokers first iteration then fully process the order because it is likely
                    # that the order was filled/canceled/etc before the strategy started.
                    if self.broker._first_iteration:
                        if order.status == Order.OrderStatus.FILLED:
                            self.broker._process_new_order(order)
                            self.broker._process_filled_order(order, order.avg_fill_price, order.quantity)
                        elif order.status == Order.OrderStatus.CANCELED:
                            self.broker._process_new_order(order)
                            self.broker._process_canceled_order(order)
                        elif order.status == Order.OrderStatus.PARTIALLY_FILLED:
                            self.broker._process_new_order(order)
                            self.broker._process_partially_filled_order(order, order.avg_fill_price, order.quantity)
                        elif order.status == Order.OrderStatus.NEW:
                            self.broker._process_new_order(order)
                    else:
                        # Add to order in lumibot.
                        self.broker._process_new_order(order)

            for order_lumi in orders_lumi:
                # Remove lumibot orders if not in broker.
                if order_lumi.identifier not in [order.identifier for order in orders_broker]:
                    # Filled or canceled orders can be dropped by the broker as they no longer have any effect.
                    # However, active orders should not be dropped as they are still in effect and if they can't
                    # be found in the broker, they should be canceled because something went wrong.
                    if order_lumi.is_active():
                        self.strategy.logger.info(
                            f"Cannot find order {order_lumi} (id={order_lumi.identifier}) in broker "
                            f"(bkr cnt={len(orders_broker)}), canceling."
                        )
                        self.broker._process_trade_event(order_lumi, "canceled")

        self.broker._hold_trade_events = False
        self.broker.process_held_trades()

    def add_event(self, event_name, payload):
        self.queue.put((event_name, payload))

    def process_event(self, event, payload):
        # If it's the first iteration, we don't want to process any events.
        # This is because in this case we are most likely processing events that occurred before the strategy started.
        if self.strategy._first_iteration or self.broker._first_iteration:
            return

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
        self.strategy.log_message("Executing the before_starting_trading lifecycle method")
        self.strategy.before_starting_trading()

    @lifecycle_method
    @trace_stats
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

        sleep_units = self.strategy.sleeptime[-1].lower()
        start_dt = datetime.now()
        self.sync_broker()

        # Check if we are in market hours.
        if not self.broker.is_market_open():
            self.strategy.log_message("The market is not currently open, skipping this trading iteration", color="blue")
            return

        # Send the account summary to Discord
        self.strategy.send_account_summary_to_discord()

        self._strategy_context = None
        start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
        self.strategy.log_message(f"Executing the on_trading_iteration lifecycle method at {start_str}", color="blue")
        on_trading_iteration = append_locals(self.strategy.on_trading_iteration)

        # Time-consuming
        try:
            on_trading_iteration()

            self.strategy._first_iteration = False
            self.broker._first_iteration = False
            self._strategy_context = on_trading_iteration.locals
            self.strategy._last_on_trading_iteration_datetime = datetime.now()
            self.process_queue()

            end_dt = datetime.now()
            end_str = end_dt.strftime("%Y-%m-%d %H:%M:%S")
            runtime = (end_dt - start_dt).total_seconds()

            # Update cron count to account for how long this iteration took to complete so that the next iteration will
            # occur at the correct time.
            self.cron_count = self._seconds_to_sleeptime_count(int(runtime), sleep_units)
            next_run_time = self.get_next_ap_scheduler_run_time()
            if next_run_time is not None:
                # Format the date to be used in the log message.
                dt_str = next_run_time.strftime("%Y-%m-%d %H:%M:%S")
                self.strategy.log_message(
                    f"Trading iteration ended at {end_str}, next check in time is {dt_str}. Took {runtime:.2f}s", color="blue"
                )

            else:
                self.strategy.log_message(f"Trading iteration ended at {end_str}", color="blue")
        except Exception as e:
            # Log the error
            self.strategy.log_message(
                f"An error occurred during the on_trading_iteration lifecycle method: {e}", color="red"
            )

            # Log the traceback
            self.strategy.log_message(traceback.format_exc(), color="red")

            self._on_bot_crash(e)

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
        self.strategy.on_partially_filled_order(position, order, price, quantity, multiplier)

    @event_method
    def _on_filled_order(self, position, order, price, quantity, multiplier):
        self.strategy.on_filled_order(position, order, price, quantity, multiplier)

        # Get the portfolio value
        portfolio_value = self.strategy.get_portfolio_value()

        # Calculate the value of the position
        order_value = price * float(quantity)

        # If option, multiply % of portfolio by 100
        if order.asset.asset_type == Asset.AssetType.OPTION:
            order_value = order_value * 100

        # Calculate the percent of the portfolio that this position represents
        percent_of_portfolio = order_value / portfolio_value

        # Capitalize the side
        side = order.side.capitalize()

        # Check if we are buying or selling
        if side == "Buy":
            emoji = "🟢📈 "
        else:
            emoji = "🔴📉 "

        # Create a message to send to Discord
        message = f"""
                {emoji} {side} {quantity:,.2f} {position.asset} @ ${price:,.2f} ({percent_of_portfolio:,.0%} of the account)
                Trade Total = ${(price * float(quantity)):,.2f}
                Account Value = ${portfolio_value:,.0f}
                """

        # Send the message to Discord
        self.strategy.send_discord_message(message, silent=False)

        # Let our listener know that an order has been filled (set in the callback)
        if hasattr(self.strategy, "_filled_order_callback") and callable(self.strategy._filled_order_callback):
            self.strategy._filled_order_callback(self, position, order, price, quantity, multiplier)

    @staticmethod
    def _sleeptime_to_seconds(sleeptime):
        """Convert the sleeptime to seconds"""
        val_err_msg = ("You can set the sleep time as an integer which will be interpreted as minutes. "
                       "eg: sleeptime = 50 would be 50 minutes. Conversely, you can enter the time as a string "
                       "with the duration numbers first, followed by the time units: 'M' for minutes, 'S' for seconds "
                       "eg: '300S' is 300 seconds.")

        if isinstance(sleeptime, int):
            return sleeptime * 60
        elif isinstance(sleeptime, str):
            unit = sleeptime[-1]
            time_raw = int(sleeptime[:-1])
            if unit.lower() == "s":
                return time_raw
            elif unit.lower() == "m":
                return time_raw * 60
            elif unit.lower() == "h":
                return time_raw * 60 * 60
            elif unit.lower() == "d":
                return time_raw * 60 * 60 * 24
            else:
                raise ValueError(val_err_msg)
        else:
            raise ValueError(val_err_msg)

    @staticmethod
    def _seconds_to_sleeptime_count(secounds, unit="s"):
        """
        Convert seconds to the sleeptime count
        Parameters
        ----------
        secounds : int
            The number of seconds
        unit : str
            The unit of time to convert to (M, S, H, D)

        Returns
        -------
        int
            The number of units of time that the seconds represent
        """
        if unit.lower() == "s":
            return secounds
        elif unit.lower() == "m":
            return secounds // 60
        elif unit.lower() == "h":
            return secounds // (60 * 60)
        elif unit.lower() == "d":
            return secounds / (60 * 60 * 24)
        else:
            raise ValueError("The unit must be 'S', 'M', 'H', or 'D'")

    # This method calculates the trigger for the strategy based on the 'sleeptime' attribute of the strategy.
    def calculate_strategy_trigger(self, force_start_immediately=False):
        """Calculate the trigger for the strategy based on the 'sleeptime' attribute of the strategy.

        Parameters
        ----------

        force_start_immediately : bool, optional
            When sleeptime is in days (eg. self.sleeptime = "1D") Whether to start the strategy immediately or wait
            until the market opens. The default is True.
        """

        # Define a standard error message about acceptable formats for 'sleeptime'.
        sleeptime_err_msg = (
            "You can set the sleep time as an integer which will be interpreted as "
            "minutes. eg: sleeptime = 50 would be 50 minutes. Conversely, you can enter "
            "the time as a string with the duration numbers first, followed by the time "
            "units: 'M' for minutes, 'S' for seconds eg: '300S' is 300 seconds."
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
            kwargs["second"] = "*"
        elif units in "Mm":
            kwargs["minute"] = "*"
        elif units in "Hh":
            kwargs["hour"] = "*"

            # Start immediately (at the closest minute) if force_start_immediately is True
            if force_start_immediately:
                # Get the current time in local timezone
                local_time = datetime.now().astimezone()

                # Add one minute to the local_time
                local_time = local_time + timedelta(minutes=1)

                # Get the minute
                minute = local_time.minute

                # Minute with 0 in front if less than 10
                kwargs["minute"] = f"0{minute}" if minute < 10 else str(minute)

        elif units in "Dd":
            kwargs["day"] = "*"

            # Start immediately (at the closest minute) if force_start_immediately is True
            # or if the market is currently open
            if force_start_immediately or self.broker.is_market_open():
                # Get the current time in local timezone
                local_time = datetime.now().astimezone()

                # Add one minute to the local_time
                local_time = local_time + timedelta(minutes=1)

                # Get the hour
                hour = local_time.hour

                # Get the minute
                minute = local_time.minute

                # Hour with 0 in front if less than 10
                kwargs["hour"] = f"0{hour}" if hour < 10 else str(hour)
                # Minute with 0 in front if less than 10
                kwargs["minute"] = f"0{minute}" if minute < 10 else str(minute)

            # Start at the market open time
            else:
                # Get the market hours for the strategy
                open_time_this_day = self.broker.utc_to_local(self.broker.market_hours(close=False, next=False))

                # Get the hour
                hour = open_time_this_day.hour

                # Get the minute
                minute = open_time_this_day.minute

                # Add 5 seconds to make sure we don't start trading before the market opens
                second = open_time_this_day.second + 5

                # Hour with 0 in front if less than 10
                kwargs["hour"] = f"0{hour}" if hour < 10 else str(hour)
                # Minute with 0 in front if less than 10
                kwargs["minute"] = f"0{minute}" if minute < 10 else str(minute)
                # Second with 0 in front if less than 10
                kwargs["second"] = f"0{second}" if second < 10 else str(second)

                self.strategy.logger.warning(
                    f"The strategy will run at {kwargs['hour']}:{kwargs['minute']}:{kwargs['second']} every day. "
                    f"If instead you want to start right now and run every {time_raw} days then set "
                    f"force_start_immediately=True in the strategy's initialization."
                )

        # Return a CronTrigger object with the calculated settings.
        return CronTrigger(**kwargs)

    # TODO: speed up this function, it's a major bottleneck for backtesting
    def _strategy_sleep(self):
        """Sleep for the strategy's sleep time"""

        is_247 = hasattr(self.broker, "market") and self.broker.market == "24/7"

        # Set the sleeptime to close.
        if is_247:
            time_to_before_closing = float("inf")
        else:
            # TODO: next line speed implication: v high (2233 microseconds) get_time_to_close()
            result = self.broker.get_time_to_close()

            if result is None:
                time_to_close = 0
            else:
                time_to_close = result

            time_to_before_closing = time_to_close - self.strategy.minutes_before_closing * 60

        sleeptime_err_msg = (
            "You can set the sleep time as an integer which will be interpreted as "
            "minutes. eg: sleeptime = 50 would be 50 minutes. Conversely, you can enter "
            "the time as a string with the duration numbers first, followed by the time "
            "units: 'M' for minutes, 'S' for seconds eg: '300S' is 300 seconds."
        )
        if isinstance(self.strategy.sleeptime, int):
            units = "M"
        elif isinstance(self.strategy.sleeptime, str):
            units = self.strategy.sleeptime[-1:]
        else:
            raise ValueError(sleeptime_err_msg)

        if units not in "SMHDsmhd":
            raise ValueError(sleeptime_err_msg)

        strategy_sleeptime = self._sleeptime_to_seconds(self.strategy.sleeptime)

        if not self.should_continue or strategy_sleeptime == 0 or time_to_before_closing <= 0:
            return False
        else:
            self.strategy.log_message(colored(f"Sleeping for {strategy_sleeptime} seconds", color="blue"))

            # Run process orders at the market close time first (if not 24/7)
            if not is_247:
                # Get the time to close.
                time_to_close = self.broker.get_time_to_close()

                # If strategy sleep time is greater than the time to close, process expired option contracts.
                if strategy_sleeptime > time_to_close:
                    # Sleep until the market closes.
                    self.safe_sleep(time_to_close)

                    # Remove the time to close from the strategy sleep time.
                    strategy_sleeptime -= time_to_close

                    # Check if the broker has a function to process expired option contracts.
                    if hasattr(self.broker, "process_expired_option_contracts"):
                        # Process expired option contracts.
                        self.broker.process_expired_option_contracts(self.strategy)

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
            and self.broker.data_source.SOURCE == "PANDAS"
            and self.broker.data_source._timestep == "day"
        ):
            if self.broker.data_source._iter_count is None:
                # Get the first date from _date_index equal or greater than
                # backtest start date.
                dates = self.broker.data_source._date_index
                self.broker.data_source._iter_count = dates.get_loc(dates[dates > self.broker.datetime][0])
            else:
                self.broker.data_source._iter_count += 1

            dt = self.broker.data_source._date_index[self.broker.data_source._iter_count]

            self.broker._update_datetime(dt, cash=self.strategy.cash, portfolio_value=self.strategy.portfolio_value)

            self.strategy._update_cash_with_dividends()

            self._on_trading_iteration()

            if self.broker.IS_BACKTESTING_BROKER:
                self.broker.process_pending_orders(strategy=self.strategy)
            return

        if not is_247:
            # Set date to the start date, but account for minutes_before_opening
            self.strategy.await_market_to_open()  # set new time and bar length. Check if hit bar max or date max.

            if not has_data_source or (has_data_source and self.broker.data_source.SOURCE != "PANDAS"):
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
                    force_start_immediately=self.strategy.force_start_immediately
                )

                # Add the on_trading_iteration method to the scheduler with the chosen trigger.
                self.scheduler.add_job(
                    self._on_trading_iteration,
                    chosen_trigger,
                    id="OTIM",
                    name="On Trading Iteration Main Thread",
                    jobstore="On_Trading_Iteration",
                )

                # Set the cron count to the cron count target so that the on_trading_iteration method will be executed
                # the first time the scheduler runs.
                self.cron_count = self.cron_count_target

            # Get the time to close.
            time_to_close = self.broker.get_time_to_close()

            if time_to_close is None:
                should_we_stop = False
            else:
                # Check if it's time to stop the strategy based on the time to close and the strategy's minutes before
                # closing.
                should_we_stop = time_to_close <= self.strategy.minutes_before_closing * 60

            # Start the check_queue thread which will run continuously in the background, checking if any items have
            # been added to the queue and executing them.
            check_queue_thread = Thread(target=self.check_queue)
            check_queue_thread.start()

            next_run_time = self.get_next_ap_scheduler_run_time()
            if next_run_time is not None:
                # Format the date to be used in the log message.
                dt_str = next_run_time.strftime("%Y-%m-%d %H:%M:%S")
                self.strategy.log_message(f"Strategy will check in again at: {dt_str}", color="blue")

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
            while is_247 or (time_to_close is not None and (time_to_close > self.strategy.minutes_before_closing * 60)):
                # Stop after we pass the backtesting end date
                if self.broker.IS_BACKTESTING_BROKER and self.broker.datetime > self.broker.data_source.datetime_end:
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

        # Set the strategy name at the broker
        self.broker.set_strategy_name(self.strategy._name)

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
                self.strategy.logger.error(e)
                self.strategy.logger.error(traceback.format_exc())
                try:
                    self._on_bot_crash(e)
                except Exception as e1:
                    self.strategy.logger.error(e1)
                    self.strategy.logger.error(traceback.format_exc())

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
            self.strategy.logger.error(e)
            self.strategy.logger.error(traceback.format_exc())
            self._on_bot_crash(e)
            self.result = self.strategy._analysis
            return False

        self.result = self.strategy._analysis
        return True
