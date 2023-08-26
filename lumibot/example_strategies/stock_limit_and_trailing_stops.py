from datetime import datetime

from lumibot.strategies.strategy import Strategy

"""
Strategy Description

An example of how to use limit orders and trailing stops to buy a stock and then sell it when it drops by a certain
percentage. This is a very simple strategy that is meant to demonstrate how to use limit orders and trailing stops.
"""


class LimitAndTrailingStop(Strategy):
    parameters = {
        "buy_symbol": "SPY",
        "limit_price": "410",
        "trail_percent": "0.02",
    }

    # =====Overloading lifecycle methods=============

    def initialize(self):
        # Set the initial variables or constants

        # Built in Variables
        self.sleeptime = "1D"

        # Our Own Variables
        self.counter = 0

    def on_trading_iteration(self):
        """Buys the self.buy_symbol once, then never again"""

        buy_symbol = self.parameters["buy_symbol"]
        limit_price = self.parameters["limit_price"]
        trail_percent = self.parameters["trail_percent"]

        # What to do each iteration
        current_value = self.get_last_price(buy_symbol)
        self.log_message(f"The value of {buy_symbol} is {current_value}")

        all_positions = self.get_positions()
        if len(all_positions) <= 1:  # Because we always have a cash position (USD)
            # Calculate how many shares we can buy with our portfolio value (the total value of all our positions)
            quantity = int(self.portfolio_value // current_value)

            # Create the limit order
            purchase_order = self.create_order(buy_symbol, quantity, "buy", limit_price=limit_price)
            self.submit_order(purchase_order)

            # Place the trailing stop
            trailing_stop_order = self.create_order(buy_symbol, quantity, "sell", trail_percent=trail_percent)
            self.submit_order(trailing_stop_order)


if __name__ == "__main__":
    is_live = False

    if is_live:
        from credentials import ALPACA_CONFIG

        from lumibot.brokers import Alpaca
        from lumibot.traders import Trader

        trader = Trader()

        broker = Alpaca(ALPACA_CONFIG)

        strategy = LimitAndTrailingStop(broker=broker)

        trader.add_strategy(strategy)
        strategy_executors = trader.run_all()

    else:
        from lumibot.backtesting import YahooDataBacktesting

        # Backtest this strategy
        backtesting_start = datetime(2023, 3, 3)
        backtesting_end = datetime(2023, 3, 10)

        results = LimitAndTrailingStop.backtest(
            YahooDataBacktesting,
            backtesting_start,
            backtesting_end,
            benchmark_asset="SPY",
        )
