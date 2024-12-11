from datetime import datetime

from lumibot.entities import Asset
from lumibot.strategies.strategy import Strategy
from lumibot.credentials import IS_BACKTESTING

"""
Strategy Description

An example strategy for buying a future and holding it to expiry.
"""


class FuturesHoldToExpiry(Strategy):
    parameters = {
        "buy_symbol": "ES",
        "expiry": datetime(2025, 3, 21),
    }

    # =====Overloading lifecycle methods=============

    def initialize(self):
        # Set the initial variables or constants

        # Built in Variables
        self.sleeptime = "1D"
        self.set_market("us_futures")

    def on_trading_iteration(self):
        """Buys the self.buy_symbol once, then never again"""

        buy_symbol = self.parameters["buy_symbol"]
        expiry = self.parameters["expiry"]

        underlying_asset = Asset(
            symbol=buy_symbol,
            asset_type="index"
        )

        # What to do each iteration
        #underlying_price = self.get_last_price(underlying_asset)
        #self.log_message(f"The value of {buy_symbol} is {underlying_price}")

        if self.first_iteration:
            # Calculate the strike price (round to nearest 1)

            # Create futures asset
            asset = Asset(
                symbol=buy_symbol,
                asset_type="future",
                expiration=expiry,
                multiplier=50
            )

            # Create order
            order = self.create_order(
                asset,
                1,
                "buy_to_open",
            )
            
            # Submit order
            self.submit_order(order)

            # Log a message
            self.log_message(f"Bought {order.quantity} of {asset}")


if __name__ == "__main__":
    if IS_BACKTESTING:
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

    else:
        strategy = FuturesHoldToExpiry()
        strategy.run_live()
