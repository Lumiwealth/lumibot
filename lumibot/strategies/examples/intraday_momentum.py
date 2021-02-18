import logging

from lumibot.strategies.strategy import Strategy

"""
Strategy Description

Buys the best performing asset from self.symbols over self.momentum_length number of minutes.
For example, if TSLA increased 0.03% in the past two minutes, but SPY, GLD, TLT and MSFT only 
increased 0.01% in the past two minutes, then we will buy TSLA.
"""


class IntradayMomentum(Strategy):
    # =====Overloading lifecycle methods=============

    def initialize(self):
        # Setting the momentum period (in minutes)
        self.momentum_length = 2

        # Set how often (in minutes) we should be running on_trading_iteration
        self.sleeptime = 1

        # Set the symbols that we want to be monitoring
        self.symbols = ["SPY", "GLD", "TLT", "MSFT", "TSLA"]

        # Initialize our variables
        self.asset = ""
        self.quantity = 0

    def on_trading_iteration(self):
        # Get the momentums of all the assets we are tracking
        momentums = self.get_assets_momentums()

        # Get the asset with the highest return in our momentum_length
        # (aka the highest momentum)
        momentums.sort(key=lambda x: x.get("return"))
        best_asset_data = momentums[-1]
        best_asset = best_asset_data.get("symbol")
        best_asset_return = best_asset_data.get("return")

        # Get the data for the currently held asset
        if self.asset:
            current_asset_data = [
                m for m in momentums if m["symbol"] == self.asset
            ][0]
            current_asset_return = current_asset_data["return"]

            # If the returns are equals, keep the current asset
            if current_asset_return >= best_asset_return:
                best_asset = self.asset
                best_asset_data = current_asset_data

        logging.info("%s best symbol." % best_asset)

        # If the asset with the highest momentum has changed, buy the new asset
        if best_asset != self.asset:
            # Sell the current asset that we own
            if self.asset:
                logging.info("Swapping %s for %s." % (self.asset, best_asset))
                order = self.create_order(self.asset, self.quantity, "sell")
                self.submit_order(order)

            # Calculate the quantity and send the buy order for the new asset
            self.asset = best_asset
            best_asset_price = best_asset_data["price"]
            self.quantity = self.portfolio_value // best_asset_price
            order = self.create_order(self.asset, self.quantity, "buy")
            self.submit_order(order)
        else:
            logging.info("Keeping %d shares of %s" % (self.quantity, self.asset))

    def before_market_closes(self):
        # Make sure that we sell everything before the market closes
        self.sell_all()
        self.quantity = 0
        self.asset = ""

    def on_abrupt_closing(self):
        self.sell_all()
        self.quantity = 0
        self.asset = ""

    def trace_stats(self, context, snapshot_before):
        """
        Add additional stats to the CSV logfile
        """
        # Get the values of all our variables from the last iteration
        row = {
            "old_best_asset": snapshot_before.get("asset"),
            "old_asset_quantity": snapshot_before.get("quantity"),
            "old_unspent_money": snapshot_before.get("unspent_money"),
            "old_portfolio_value": snapshot_before.get("portfolio_value"),
            "new_best_asset": self.asset,
            "new_asset_quantity": self.quantity,
        }

        # Get the momentums of all the assets from the context of on_trading_iteration
        # (notice that on_trading_iteration has a variable called momentums, this is what
        # we are reading here)
        momentums = context.get("momentums")
        for item in momentums:
            symbol = item.get("symbol")
            for key in item:
                if key != "symbol":
                    row[f"{symbol}_{key}"] = item[key]

        # Add all of our values to the row in the CSV file. These automatically get
        # added to portfolio_value, unspent_money and return
        return row

    # =============Helper methods====================

    def get_assets_momentums(self):
        """
        Gets the momentums (the percentage return) for all the assets we are tracking,
        over the time period set in self.momentum_length
        """
        momentums = []
        for symbol in self.symbols:
            # Get the return for symbol over self.momentum_length minutes
            bars_set = self.get_symbol_bars(symbol, self.momentum_length + 1)
            start_date = self.get_round_minute(timeshift=self.momentum_length + 1)
            symbol_momentum = bars_set.get_momentum(start=start_date)
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
