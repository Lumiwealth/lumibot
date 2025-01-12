from datetime import datetime

from lumibot.strategies.strategy import Strategy

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
        self.period = 2

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

        # The asset that we want to buy/currently own, and the quantity
        self.asset = ""
        self.quantity = 0

    def on_trading_iteration(self):
        # When the counter reaches the desired holding period,
        # re-evaluate which asset we should be holding
        momentums = []
        if self.counter == self.period or self.counter == 0:
            self.counter = 0
            momentums = self.get_assets_momentums()

            # Get the asset with the highest return in our period
            # (aka the highest momentum)
            momentums.sort(key=lambda x: x.get("return"))
            best_asset_data = momentums[-1]
            best_asset = best_asset_data["symbol"]
            best_asset_return = best_asset_data["return"]

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

            self.log_message("%s best symbol." % best_asset)

            # If the asset with the highest momentum has changed, buy the new asset
            if best_asset != self.asset:
                # Sell the current asset that we own
                if self.asset:
                    self.log_message("Swapping %s for %s." % (self.asset, best_asset))
                    order = self.create_order(self.asset, self.quantity, "sell")
                    self.submit_order(order)

                # Calculate the quantity and send the buy order for the new asset
                self.asset = best_asset
                best_asset_price = best_asset_data["price"]
                self.quantity = int(self.portfolio_value // best_asset_price)
                order = self.create_order(self.asset, self.quantity, "buy")
                self.submit_order(order)
            else:
                self.log_message("Keeping %d shares of %s" % (self.quantity, self.asset))

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
            "old_cash": snapshot_before.get("cash"),
            "new_best_asset": self.asset,
            "new_asset_quantity": self.quantity,
        }

        # Get the momentums of all the assets from the context of on_trading_iteration
        # (notice that on_trading_iteration has a variable called momentums, this is what
        # we are reading here)
        momentums = context.get("momentums")
        if len(momentums) != 0:
            for item in momentums:
                symbol = item.get("symbol")
                for key in item:
                    if key != "symbol":
                        row[f"{symbol}_{key}"] = item[key]

        # Add all of our values to the row in the CSV file. These automatically get
        # added to portfolio_value, cash and return
        return row

    # =============Helper methods====================

    def get_assets_momentums(self):
        """
        Gets the momentums (the percentage return) for all the assets we are tracking,
        over the time period set in self.period
        """
        momentums = []
        data = self.get_bars(self.symbols, self.period + 2, timestep="day")
        for asset, bars_set in data.items():
            # Get the return for symbol over self.period days
            symbol = asset.symbol
            symbol_momentum = bars_set.get_momentum(num_periods=self.period)
            self.log_message(
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


if __name__ == "__main__":
    is_live = False

    if is_live:
        from lumibot.credentials import ALPACA_CONFIG
        from lumibot.brokers import Alpaca

        broker = Alpaca(ALPACA_CONFIG)

        strategy = Momentum(broker=broker)
        strategy.run_live()

    else:
        from lumibot.backtesting import YahooDataBacktesting

        # Backtest this strategy
        backtesting_start = datetime(2023, 1, 1)
        backtesting_end = datetime(2023, 8, 1)

        results = Momentum.backtest(
            YahooDataBacktesting,
            backtesting_start,
            backtesting_end,
            benchmark_asset="SPY",
        )
