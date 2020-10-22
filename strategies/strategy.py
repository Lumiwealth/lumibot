import logging
import time
import traceback


class Strategy:
    def __init__(
        self, budget, broker, pricing_data=None, minutes_before_closing=5, sleeptime=1
    ):
        # Setting the strategy name and the budget allocated
        self.name = self.__class__.__name__
        self.budget = budget

        # Setting the API object
        self.broker = broker

        # Setting how many minutes before market closes
        # The bot should stop
        self.minutes_before_closing = minutes_before_closing

        # Timesleep after each on_trading_iteration execution
        # unity is minutes
        self.sleeptime = sleeptime

        # Setting the data provider
        if pricing_data is None:
            self.pricing_data = self.broker
        else:
            self.pricing_data = pricing_data

        # Ready to close
        self._ready_to_close = False

    def get_ready_to_close(self):
        return self._ready_to_close

    def set_ready_to_close(self, value=True):
        self._ready_to_close = value

    # =======Helper methods=======================
    def format_log_message(self, message):
        message = "Strategy %s: %s" % (self.name, message)
        return message

    # =======Lifecycle methods====================

    def initialize(self):
        """Use this lifecycle method to initialize parameters"""
        pass

    def before_market_opens(self):
        """Lifecycle method executed before market opens
        Example: self.broker.cancel_open_orders()"""
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

    def before_market_closes(self):
        """Use this lifecycle method to execude code
        self.minutes_before_closing minutes before closing.
        Example: self.broker.sell_all()"""
        pass

    def after_market_closes(self):
        """Use this lifecycle method to execute code
        after market closes. Exampling: dumping stats/reports"""
        pass

    def on_bot_crash(self, error):
        """Use this lifecycle method to execute code
        when an exception is raised and the bot crashes"""
        pass

    def on_abrupt_closing(self):
        """Use this lifecycle method to execute code
        when the main trader was shut down (Keybord Interuption, ...)
        Example: self.broker.sell_all()"""
        pass

    def run_trading_session(self):
        if not self.broker.is_market_open():
            logging.info(
                self.format_log_message(
                    "Executing the before_market_opens lifecycle method"
                )
            )
            self.before_market_opens()

        self.broker.await_market_to_open()
        self.before_starting_trading()

        time_to_close = self.broker.get_time_to_close()
        while time_to_close > self.minutes_before_closing * 60:
            logging.info(
                self.format_log_message(
                    "Executing the on_trading_iteration lifecycle method"
                )
            )
            self.on_trading_iteration()
            time_to_close = self.broker.get_time_to_close()
            sleeptime = time_to_close - 15 * 60
            sleeptime = max(min(sleeptime, 60 * self.sleeptime), 0)
            if sleeptime:
                logging.info(
                    self.format_log_message("Sleeping for %d seconds" % sleeptime)
                )
                time.sleep(sleeptime)

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
        while True:
            try:
                self.run_trading_session()
            except Exception as e:
                logging.error(e)
                logging.error(traceback.format_exc())
                self.on_bot_crash(e)
                break
