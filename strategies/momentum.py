import logging

from .strategy import Strategy

class Momentum(Strategy):
    # =====Overloading lifecycle methods=============

    def initialize(self):
        # canceling open orders
        self.api.cancel_open_orders()

        # setting sleeptime af each iteration to 5 minutes
        self.sleeptime = 5

        # setting risk management variables
        self.capital_per_asset = self.budget

    def on_market_open(self):
        ongoing_assets = self.api.get_ongoing_assets()
        if len(ongoing_assets) < self.max_positions:
            self.buy_winning_stocks(self.increase_target, self.stop_loss_target, self.limit_increase_target)
        else:
            logging.info("Max positions %d reached" % self.max_positions)
