import logging
from datetime import timedelta

from lumibot.data_sources import AlpacaData
from lumibot.strategies.strategy import Strategy


class IntradayMomentum(Strategy):
    # =====Overloading lifecycle methods=============

    def initialize(self):
        # setting the momentum period (in minutes) and the sleeptime
        self.momentum_length = 2
        self.sleeptime = 1

        # set the symbols variable and initialize the asset_symbol variable
        self.symbols = ["SPY", "GLD", "TLT", "MSFT", "TSLA"]
        self.asset = ""
        self.quantity = 0

    def on_trading_iteration(self):
        momentums = self.get_assets_momentums()
        momentums.sort(key=lambda x: x.get("return"))
        best_asset = momentums[-1].get("symbol")
        logging.info("%s best symbol." % best_asset)

        if best_asset != self.asset:
            if self.asset:
                logging.info("Swapping %s for %s." % (self.asset, best_asset))
                order = self.create_order(self.asset, self.quantity, "sell")
                self.submit_order(order)

            self.asset = best_asset
            best_asset_price = [
                m["price"] for m in momentums if m["symbol"] == best_asset
            ][0]
            self.quantity = self.portfolio_value // best_asset_price
            order = self.create_order(self.asset, self.quantity, "buy")
            self.submit_order(order)
        else:
            logging.info("Keeping %d shares of %s" % (self.quantity, self.asset))

    def before_market_closes(self):
        self.sell_all()

    def on_abrupt_closing(self):
        self.sell_all()

    # =============Helper methods====================

    def get_assets_momentums(self):
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
            momentums.append(
                {
                    "symbol": symbol,
                    "price": bars_set.get_last_price(),
                    "return": symbol_momentum,
                }
            )
        return momentums
