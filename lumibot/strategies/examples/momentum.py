import logging
from datetime import timedelta

from lumibot.strategies.strategy import Strategy


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

        logging.info("Sleeping till the market closes.")
        self.counter += 1
        self.await_market_to_close()

    def on_abrupt_closing(self):
        # sell all positions
        self.sell_all()

    def trace_stats(self, context, snapshot_before):
        old_best_asset = None
        old_asset_quantity = None
        old_unspent_money = None
        if snapshot_before:
            old_best_asset = snapshot_before.get("asset")
            old_asset_quantity = snapshot_before.get("quantity")
            old_unspent_money = snapshot_before.get("unspent_money")

        momentums = context.get("momentums")
        VEU_price = None
        VEU_momentum = None
        SPY_price = None
        SPY_momentum = None
        AGG_price = None
        AGG_momentum = None
        if momentums:
            for momentum in momentums:
                symbol = momentum.get("symbol")
                if symbol == "VEU":
                    VEU_price = momentum.get("price")
                    VEU_momentum = momentum.get("return")
                elif symbol == "SPY":
                    SPY_price = momentum.get("price")
                    SPY_momentum = momentum.get("return")
                elif symbol == "AGG":
                    AGG_price = momentum.get("price")
                    AGG_momentum = momentum.get("return")

        new_best_asset = self.asset
        new_asset_quantity = self.quantity

        row = {
            "old_best_asset": old_best_asset,
            "old_asset_quantity": old_asset_quantity,
            "old_unspent_money": old_unspent_money,
            "VEU_price": VEU_price,
            "VEU_momentum": VEU_momentum,
            "SPY_price": SPY_price,
            "SPY_momentum": SPY_momentum,
            "AGG_price": AGG_price,
            "AGG_momentum": AGG_momentum,
            "new_best_asset": new_best_asset,
            "new_asset_quantity": new_asset_quantity,
        }

        return row

    # =============Helper methods====================

    def get_assets_momentums(self):
        momentums = []
        for symbol in self.symbols:
            bars_set = self.get_symbol_bars(symbol, self.period + 1, timedelta(days=1))
            symbol_momentum = bars_set.get_momentum()
            logging.info(
                "%s has a return value of %.2f%% over the last %d day(s)."
                % (symbol, 100 * symbol_momentum, self.period)
            )
            momentums.append(
                {
                    "symbol": symbol,
                    "price": bars_set.get_last_price(),
                    "return": symbol_momentum,
                }
            )
        return momentums
