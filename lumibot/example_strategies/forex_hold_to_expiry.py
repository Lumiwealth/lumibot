from datetime import datetime

from lumibot.entities import Asset
from lumibot.strategies.strategy import Strategy

"""
Strategy Description

An example strategy for buying a future and holding it to expiry.
"""


class FuturesHoldToExpiry(Strategy):
    parameters = {
        "buy_symbol": "GBP",
    }

    # =====Overloading lifecycle methods=============

    def initialize(self):
        # Set the initial variables or constants

        # Built in Variables
        self.sleeptime = "1D"

    def on_trading_iteration(self):
        """Buys the self.buy_symbol once, then never again"""

        buy_symbol = self.parameters["buy_symbol"]

        # What to do each iteration
        #underlying_price = self.get_last_price(underlying_asset)
        #self.log_message(f"The value of {buy_symbol} is {underlying_price}")

        if self.first_iteration:
            # Calculate the strike price (round to nearest 1)

            # Create futures asset
            asset = Asset(
                symbol=buy_symbol,
                asset_type="forex",
            )

            # Create order
            order = self.create_order(
                asset,
                10,
                "buy_to_open",
            )
            
            # Submit order
            self.submit_order(order)

            # Log a message
            self.log_message(f"Bought {order.quantity} of {asset}")


if __name__ == "__main__":
    is_live = True

    if is_live:
        strategy = FuturesHoldToExpiry()
        strategy.run_live()

    else:
        from lumibot.backtesting import PolygonDataBacktesting

        # Backtest this strategy
        backtesting_start = datetime(2023, 10, 19)
        backtesting_end = datetime(2023, 10, 24)

        results = FuturesHoldToExpiry.backtest(
            PolygonDataBacktesting,
            backtesting_start,
            backtesting_end,
            benchmark_asset="SPY",
            polygon_api_key="YOUR_POLYGON_API_KEY_HERE",  # Add your polygon API key here
        )
