import logging

from .strategy import Strategy
from data_sources import Alpaca

class Demo(Strategy):
    # =====Overloading lifecycle methods=============

    def initialize(self):
        # canceling open orders
        self.api.cancel_open_orders()

        #setting the waiting period (in minutes) and the counter
        self.period = 2
        self.counter = 0

        #there is only one trading operation per day
        #no need to sleep betwwen iterations
        self.sleeptime = 1.1
        # set the symbols variable and initialize the asset_symbol variable
        self.symbols = ['SPY', 'GLD', 'TLT', 'MSFT', 'TSLA']
        self.asset = ''
        self.quantity = 0

    def on_trading_iteration(self):
        best_asset = self.get_best_asset()
        if best_asset != self.asset:
            if self.asset:
                logging.info("Swapping %s for %s." % (self.asset, best_asset))
                self.api.submit_order(self.asset, self.quantity, 'sell')

            self.asset = best_asset
            best_asset_price = self.api.get_last_price(best_asset)
            self.quantity = self.budget // best_asset_price
            self.api.submit_order(self.asset, self.quantity, 'buy')
        else:
            logging.info("Keeping %d shares of %s" % (self.quantity, self.asset))

        self.counter += 1

    # =============Helper methods====================

    def get_best_asset(self):
        momentums = []
        for symbol in self.symbols:
            df = Alpaca.get_intraday_returns_for_asset(self.api, symbol, self.period)
            symbol_return = df['momentum'][-1]
            logging.info(
                "%s has a return value of %.2f%% over the last %d minutes(s)." %
                (symbol, 100*symbol_return, self.period)
            )
            momentums.append({
                'symbol': symbol,
                'return': symbol_return
            })

        momentums.sort(key=lambda x:x.get('return'))
        best_asset = momentums[-1].get('symbol')
        logging.info("%s best symbol." % best_asset)
        return best_asset
