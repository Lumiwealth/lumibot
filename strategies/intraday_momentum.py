import logging
from datetime import timedelta

from data_sources import AlpacaData

from .strategy import Strategy


class IntradayMomentum(Strategy):
    # =====Overloading lifecycle methods=============

    def initialize(self):
        # setting the momentum period (in minutes) and the counter
        self.momentum_length = 2
        self.counter = 0

        # there is only one trading operation per day
        # no need to sleep betwwen iterations
        self.sleeptime = 1
        # set the symbols variable and initialize the asset_symbol variable
        self.symbols = ["SPY", "GLD", "TLT", "MSFT", "TSLA"]
        self.asset = ""
        self.quantity = 0

    def on_trading_iteration(self):
        best_asset = self.get_best_asset()
        if best_asset != self.asset:
            if self.asset:
                logging.info("Swapping %s for %s." % (self.asset, best_asset))
                order = self.create_order(self.asset, self.quantity, "sell")
                self.submit_order(order)

            self.asset = best_asset
            best_asset_price = self.get_last_price(best_asset)
            self.quantity = self.unspent_money // best_asset_price
            order = self.create_order(self.asset, self.quantity, "buy")
            self.submit_order(order)
        else:
            logging.info("Keeping %d shares of %s" % (self.quantity, self.asset))

        self.counter += 1

    def before_market_closes(self):
        self.sell_all()

    def on_abrupt_closing(self):
        self.sell_all()

    # =============Helper methods====================

    def get_best_asset(self):
        momentums = []
        for symbol in self.symbols:
            bars_set = self.get_symbol_bars(
                symbol, self.momentum_length + 1, timedelta(minutes=1)
            )
            symbol_momentum = bars_set.get_momentum()
            logging.info(
                "%s has a return value of %.2f%% over the last %d minutes(s)."
                % (symbol, 100 * symbol_momentum, self.momentum_length)
            )
            momentums.append({"symbol": symbol, "return": symbol_momentum})

        momentums.sort(key=lambda x: x.get("return"))
        best_asset = momentums[-1].get("symbol")
        logging.info("%s best symbol." % best_asset)
        return best_asset
