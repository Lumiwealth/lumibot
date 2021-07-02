import logging
import random

from lumibot.strategies.strategy import Strategy

"""
Strategy Description

Buys and sells 10 of self.buy_symbol every day (not meant to make money, just an example).
For example, Day 1 it will buy 10 shares, Day 2 it will sell all of them, Day 3 it will 
buy 10 shares again, etc.
"""


class Simple(Strategy):
    # =====Overloading lifecycle methods=============

    def initialize(self):
        # Set the initial variables or constants

        # Built in Variables
        self.sleeptime = 1

        # Our Own Variables
        self.counter = 0
        self.buy_symbol = "AGG"

    def on_trading_iteration(self):
        # What to do each iteration
        current_value = self.get_last_price(self.buy_symbol)
        logging.info(
            f"Counter is at {self.counter}, program thinks it is {self.get_datetime()}"
        )
        logging.info(f"The value of {self.buy_symbol} is {current_value}")

        all_positions = self.get_tracked_positions()
        if len(all_positions) > 0:
            for position in all_positions:
                logging.info(
                    f"We own {position.quantity} of {position.symbol}, about to sell"
                )
                selling_order = position.get_selling_order()
                self.submit_order(selling_order)
        else:
            logging.info(f"We have no open positions")

        # We can also do this to sell all our positions:
        # self.sell_all()

        if self.counter % 2 == 0:
            purchase_order = self.create_order(self.buy_symbol, 10, "buy")
            self.submit_order(purchase_order)

        self.counter = self.counter + 1

        # Wait until the end of the day
        self.await_market_to_close()

    def on_abrupt_closing(self):
        self.sell_all()

    def trace_stats(self, context, snapshot_before):
        random_number = random.randint(0, 100)
        row = {"my_custom_stat": random_number, "counter": self.counter}

        return row
