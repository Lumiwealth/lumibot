import logging
from datetime import timedelta

from .strategy import Strategy


class Momentum(Strategy):
    # =====Overloading lifecycle methods=============

    def initialize(self):
        # setting the waiting period (in days) and the counter
        self.period = 1
        self.counter = 0

        # there is only one trading operation per day
        # no need to sleep betwwen iterations
        self.sleeptime = 0

        # set the symbols variable and initialize the asset_symbol variable
        self.symbols = ["SPY", "VEU", "AGG"]
        self.asset = ""
        self.quantity = 0

    def on_trading_iteration(self):
        if self.counter == self.period or self.counter == 0:
            self.counter = 0
            best_asset = self.get_best_asset()
            if best_asset != self.asset:
                if self.asset:
                    logging.info("Swapping %s for %s." % (self.asset, best_asset))
                    order = self.create_order(self.asset, self.quantity, "sell")
                    self.submit_order(order)

                self.asset = best_asset
                best_asset_price = self.get_last_price(best_asset)
                self.quantity = self.budget // best_asset_price
                order = self.create_order(self.asset, self.quantity, "buy")
                self.submit_order(order)
            else:
                logging.info("Keeping %d shares of %s" % (self.quantity, self.asset))

        logging.info("Sleeping till the market closes.")
        self.counter += 1
        self.await_market_to_close()

    def on_abrupt_closing(self):
        # sell all positions
        self.sell_all()

    # =============Helper methods====================

    def get_best_asset(self):
        momentums = []
        for symbol in self.symbols:
            bars_set = self.get_symbol_bars(symbol, self.period + 1, timedelta(days=1))
            symbol_momentum = bars_set.get_momentum()
            logging.info(
                "%s has a return value of %.2f%% over the last %d day(s)."
                % (symbol, 100 * symbol_momentum, self.period)
            )
            momentums.append({"symbol": symbol, "return": symbol_momentum})

        momentums.sort(key=lambda x: x.get("return"))
        best_asset = momentums[-1].get("symbol")
        logging.info("%s best symbol." % best_asset)
        return best_asset
