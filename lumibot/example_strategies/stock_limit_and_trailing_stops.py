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
        "limit_buy_price": 403,
        "limit_sell_price": 407,
        "trail_percent": 0.02,
        "trail_price": 7,
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
        limit_buy_price = self.parameters["limit_buy_price"]
        limit_sell_price = self.parameters["limit_sell_price"]
        trail_percent = self.parameters["trail_percent"]
        trail_price = self.parameters["trail_price"]

        # What to do each iteration
        current_value = self.get_last_price(buy_symbol)
        self.log_message(f"The value of {buy_symbol} is {current_value}")

        if self.first_iteration:
            # Create the limit buy order
            purchase_order = self.create_order(buy_symbol, 100, "buy", limit_price=limit_buy_price)
            self.submit_order(purchase_order)

            # Create the limit sell order
            sell_order = self.create_order(buy_symbol, 100, "sell", limit_price=limit_sell_price)
            self.submit_order(sell_order)

            # Place the trailing percent stop
            trailing_pct_stop_order = self.create_order(buy_symbol, 100, "sell", trail_percent=trail_percent)
            self.submit_order(trailing_pct_stop_order)

            # Place the trailing price stop
            trailing_price_stop_order = self.create_order(buy_symbol, 50, "sell", trail_price=trail_price)
            self.submit_order(trailing_price_stop_order)


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
