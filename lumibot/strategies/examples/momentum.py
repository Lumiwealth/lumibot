import logging

from lumibot.strategies.strategy import Strategy
from lumibot.entities import Asset

"""
Strategy Description

Buys the best performing asset from self.symbols over self.period number of days.
For example, if SPY increased 2% yesterday, but VEU and AGG only increased 1% yesterday,
then we will buy SPY.
"""


class Momentum(Strategy):
    # =====Overloading lifecycle methods=============

    def initialize(self, symbols=None):
        # Setting the waiting period (in days)
        self.period = 1

        # The counter for the number of days we have been holding the current asset
        self.counter = 0

        # There is only one trading operation per day
        # No need to sleep between iterations
        self.sleeptime = 0

        # Set the symbols that we will be monitoring for momentum
        if symbols:
            self.symbols = symbols
        else:
            self.symbols = ["SPY", "VEU", "AGG"]

        # Create asset objects from symbols
        self.assets = list()
        for symbol in self.symbols:
            self.assets.append(Asset(symbol=symbol))

        # The asset that we want to buy/currently own, and the quantity
        self.curr_asset = ""
        self.curr_quantity = 0

    def on_trading_iteration(self):
        # When the counter reaches the desired holding period,
        # re-evaluate which asset we should be holding
        if self.counter == self.period or self.counter == 0:
            self.counter = 0
            momentums = self.get_assets_momentums()

            # Get the asset with the highest return in our period
            # (aka the highest momentum)
            momentums.sort(key=lambda x: x.get("return"))
            best_asset_data = momentums[-1]
            best_asset = best_asset_data["asset"]
            best_asset_return = best_asset_data["return"]

            # Get the data for the currently held asset
            if self.curr_asset:
                current_asset_data = [
                    m for m in momentums if m["asset"] == self.curr_asset
                ][0]
                current_asset_return = current_asset_data["return"]

                # If the returns are equals, keep the current asset
                if current_asset_return >= best_asset_return:
                    best_asset = self.curr_asset
                    best_asset_data = current_asset_data

            logging.info("%s best symbol." % best_asset.symbol)

            # If the asset with the highest momentum has changed, buy the new asset
            if best_asset != self.curr_asset:
                # Sell the current asset that we own
                if self.curr_asset:
                    logging.info(
                        "Swapping %s for %s."
                        % (self.curr_asset.symbol, best_asset.symbol)
                    )
                    order = self.create_order(
                        self.curr_asset, self.curr_quantity, "sell"
                    )
                    self.submit_order(order)

                # Calculate the quantity and send the buy order for the new asset
                self.curr_asset = best_asset
                best_asset_price = best_asset_data["price"]
                self.curr_quantity = self.portfolio_value // best_asset_price
                order = self.create_order(self.curr_asset, self.curr_quantity, "buy")
                self.submit_order(order)
            else:
                logging.info(
                    "Keeping %d shares of %s" % (self.curr_quantity, self.curr_asset)
                )

        self.counter += 1

        # Stop for the day, since we are looking at daily momentums
        self.await_market_to_close()

    def on_abrupt_closing(self):
        # Sell all positions
        self.sell_all()

    def trace_stats(self, context, snapshot_before):
        """
        Add additional stats to the CSV logfile
        """
        # Get the values of all our variables from the last iteration
        row = {
            "old_best_asset": snapshot_before.get("asset"),
            "old_asset_quantity": snapshot_before.get("quantity"),
            "old_unspent_money": snapshot_before.get("unspent_money"),
            "new_best_asset": self.curr_asset,
            "new_asset_quantity": self.curr_quantity,
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
        over the time period set in self.period
        """

        momentums = []
        for asset in self.assets:
            # Get the return for asset over self.period days
            bars_set = self.get_asset_bars(asset, self.period + 2, timestep="day")
            start_date = self.get_round_day(timeshift=self.period + 1)
            asset_momentum = bars_set.get_momentum(start=start_date)
            logging.info(
                "%s has a return value of %.2f%% over the last %d day(s)."
                % (asset.symbol, 100 * asset_momentum, self.period)
            )

            momentums.append(
                {
                    "asset": asset,
                    "price": bars_set.get_last_price(),
                    "return": asset_momentum,
                }
            )
        return momentums
