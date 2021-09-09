import logging
import random

from lumibot.strategies.strategy import Strategy

"""
Strategy Description

Simply buys one asset and holds onto it.
"""


class BuyAndHold(Strategy):
    # =====Overloading lifecycle methods=============

    def initialize(self, buy_symbol="SPY"):
        # Set the initial variables or constants

        # Built in Variables
        self.sleeptime = 0

        # Our Own Variables
        self.counter = 0
        self.buy_symbol = buy_symbol

    def on_trading_iteration(self):
        """Buys the self.buy_symbol once, then never again"""
        # What to do each iteration
        current_value = self.get_last_price(self.buy_symbol)
        logging.info(f"The value of {self.buy_symbol} is {current_value}")

        all_positions = self.get_tracked_positions()
        if len(all_positions) == 0:
            quantity = self.portfolio_value // current_value
            purchase_order = self.create_order(self.buy_symbol, quantity, "buy")
            self.submit_order(purchase_order)

        # Wait until the end of the day
        self.await_market_to_close()

    def on_abrupt_closing(self):
        self.sell_all()

    def trace_stats(self, context, snapshot_before):
        row = {"current_value": context["current_value"]}

        return row
