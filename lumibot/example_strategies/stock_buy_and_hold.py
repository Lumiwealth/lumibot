from datetime import datetime

from lumibot.strategies.strategy import Strategy

"""
Strategy Description

Simply buys one asset and holds onto it.
"""


class BuyAndHold(Strategy):
    parameters = {
        "buy_symbol": "SPY",
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

        # What to do each iteration
        current_value = self.get_last_price(buy_symbol)
        self.log_message(f"The value of {buy_symbol} is {current_value}")

        all_positions = self.get_positions()
        if len(all_positions) <= 1:  # Because we always have a cash position (USD)
            quantity = int(self.portfolio_value // current_value)
            purchase_order = self.create_order(buy_symbol, quantity, "buy")
            self.submit_order(purchase_order)


if __name__ == "__main__":
    is_live = False

    if is_live:
        from credentials import ALPACA_CONFIG

        from lumibot.brokers import Alpaca
        from lumibot.traders import Trader

        trader = Trader()

        broker = Alpaca(ALPACA_CONFIG)

        strategy = BuyAndHold(broker=broker)

        trader.add_strategy(strategy)
        strategy_executors = trader.run_all()

    else:
        from lumibot.backtesting import YahooDataBacktesting

        # Backtest this strategy
        backtesting_start = datetime(2023, 1, 1)
        backtesting_end = datetime(2023, 6, 1)

        results = BuyAndHold.backtest(
            YahooDataBacktesting,
            backtesting_start,
            backtesting_end,
            benchmark_asset="SPY",
        )
