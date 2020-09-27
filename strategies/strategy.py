import time, logging

class Strategy:
    def __init__(self, budget, broker, minutes_before_closing=15, sleeptime=1):
        #Setting the strategy name and the budget allocated
        self.name = self.__class__.__name__
        self.budget = budget

        #Setting the API object
        self.api = broker

        #Setting how many minutes before market closes
        #The bot should stop
        self.minutes_before_closing = minutes_before_closing

        #Timesleep after each on_market_open execution
        self.sleeptime = sleeptime

    #=======Helper methods=======================
    def format_log_message(self, message):
        message = "Strategy %s: %s" % (self.name, message)
        return message

    #=======Lifecycle methods====================

    def initialize(self):
        """Use this lifecycle method to initialize parameters"""
        pass

    def before_market_opens(self):
        """Lifecycle method executed before market opens
        Example: self.api.cancel_open_orders()"""
        pass

    def on_market_open(self):
        """Use this lifecycle method for trading.
        Will be executed indefinetly until there
        will be only self.minutes_before_closing
        minutes before market closes"""
        pass

    def before_market_closes(self):
        """Use this lifecycle method to execude code
        self.minutes_before_closing minutes before closing.
        Example: self.api.sell_all()"""
        pass

    def after_market_closes(self):
        """Use this lifecycle method to execute code
        after market closes. Exampling: dumping stats/reports"""
        pass

    def on_bot_crash(self, error):
        """Use this lifecycle method to execute code
        when an exception is raised and the bot crashes"""
        pass

    def run_trading_session(self):
        if not self.api.is_market_open():
            logging.info(self.format_log_message(
                "Executing the before_market_opens lifecycle method"
            ))
            self.before_market_opens()

        self.api.await_market_to_open()
        time_to_close = self.get_time_to_close()
        while time_to_close > self.minutes_before_closing * 60:
            logging.info(self.format_log_message(
                "Executing the on_market_open lifecycle method"
            ))
            self.on_market_open()
            time_to_close = self.get_time_to_close()
            sleeptime = time_to_close - 15 * 60
            sleeptime = max(min(sleeptime, 60 * self.sleeptime), 0)
            logging.info(self.format_log_message(
                "Sleeping for %d seconds" % sleeptime
            ))
            time.sleep(sleeptime)

        if self.api.is_market_open():
            logging.info(self.format_log_message(
                "Executing the before_market_closes lifecycle method"
            ))
            self.before_market_closes()

        self.await_market_to_close()
        logging.info(self.format_log_message(
            "Executing the after_market_closes lifecycle method"
        ))
        self.after_market_closes()

    def run(self):
        """The main execution point.
        Execute the lifecycle methods"""
        logging.info(self.format_log_message(
            "Executing the initialize lifecycle method"
        ))
        self.initialize()
        while True:
            try:
                self.run_trading_session()
            except Exception as e:
                logging.error(e)
                self.on_bot_crash(e)
                break
