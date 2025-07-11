import datetime as dt
import pytz

from lumibot.strategies.strategy import Strategy
from lumibot.credentials import ALPACA_TEST_CONFIG
from lumibot.brokers import Alpaca

"""
Strategy Description

A simple paper trading strategy that demonstrates how to use the register_cron_callback
method to schedule a function to be executed at a specific time. This strategy registers
a toy function to be called 5 minutes before midnight every night.
"""


class ScheduledFunctionStrategy(Strategy):
    """
    A strategy that demonstrates how to schedule a function to be executed at a specific time.
    """

    # =====Overloading lifecycle methods=============

    def initialize(self):
        """
        Called when the strategy is started. Sets up the strategy parameters and
        registers the midnight_update function to be called 5 minutes before midnight every night.
        """
        # Set the sleep time to one day (the strategy will run once per day)
        self.sleeptime = "1D"
        
        # Register the midnight_update function to be called 5 minutes before midnight every night
        # Cron format: minute hour day_of_month month day_of_week
        # 55 23 * * * = 11:55 PM every day
        self.register_cron_callback("55 23 * * *", self.midnight_update)
        
        self.log_message("Strategy initialized. Midnight update scheduled for 11:55 PM every night.")

    def on_trading_iteration(self):
        """
        Called on each trading iteration. Simply logs the current datetime.
        """
        # Get the current datetime and log it
        current_dt = self.get_datetime()
        self.log_message(f"Trading iteration at: {current_dt}")

    def midnight_update(self):
        """
        A toy function that will be called 5 minutes before midnight every night.
        In a real strategy, this could perform end-of-day analysis, rebalancing, etc.
        """
        current_dt = self.get_datetime()
        self.log_message(f"Midnight update triggered at: {current_dt}", color="green")
        self.log_message("Performing end-of-day tasks...", color="green")
        
        # Simulate some work
        self.log_message("1. Analyzing daily performance", color="green")
        self.log_message("2. Preparing for next trading day", color="green")
        self.log_message("3. Sending daily report", color="green")
        
        self.log_message("Midnight update completed!", color="green")


if __name__ == "__main__":

    # Verify we have the necessary configuration
    if not ALPACA_TEST_CONFIG:
        print("This strategy requires an ALPACA_TEST_CONFIG config file to be set.")
        exit()

    if not ALPACA_TEST_CONFIG['PAPER']:
        print(
            "This is an example, you should use paper keys."
        )
        exit()

    broker = Alpaca(ALPACA_TEST_CONFIG)
    strategy = ScheduledFunctionStrategy(broker=broker)
    strategy.run_live()