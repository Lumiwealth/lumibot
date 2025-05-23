from datetime import datetime

from lumibot.entities import Order
from lumibot.strategies.strategy import Strategy

"""
Strategy Description

An example strategy for how to use bracket orders.
"""


class StockBracket(Strategy):
    parameters = {
        "buy_symbol": "SPY",
        "take_profit_price": 405,
        "stop_loss_price": 395,
        "quantity": 10,
    }

    # =====Overloading lifecycle methods=============

    def initialize(self):
        # Set the initial variables or constants

        # Built in Variables
        self.sleeptime = "1D"

        # Our Own Variables
        self.counter = 0
        self.submitted_bracket_order = None  # Useful for updating/canceling orders

    def on_trading_iteration(self):
        """Buys the self.buy_symbol once, then never again"""

        buy_symbol = self.parameters["buy_symbol"]
        take_profit_price = self.parameters["take_profit_price"]
        stop_loss_price = self.parameters["stop_loss_price"]
        quantity = self.parameters["quantity"]

        # What to do each iteration
        current_value = self.get_last_price(buy_symbol)
        self.log_message(f"The value of {buy_symbol} is {current_value}")

        if self.first_iteration:
            # Bracket order
            order = self.create_order(
                buy_symbol,
                quantity,
                Order.OrderSide.BUY,
                secondary_limit_price=take_profit_price,
                secondary_stop_price=stop_loss_price,
                order_class=Order.OrderClass.BRACKET,
            )
            self.submitted_bracket_order = self.submit_order(order)


if __name__ == "__main__":
    is_live = False

    if is_live:
        from credentials import ALPACA_CONFIG

        from lumibot.brokers import Alpaca

        broker = Alpaca(ALPACA_CONFIG)

        strategy = StockBracket(broker=broker)
        strategy.run_live()

    else:
        from lumibot.backtesting import YahooDataBacktesting

        # Backtest this strategy
        backtesting_start = datetime(2023, 3, 3)
        backtesting_end = datetime(2023, 3, 10)

        results = StockBracket.backtest(
            YahooDataBacktesting,
            backtesting_start,
            backtesting_end,
            benchmark_asset="SPY",
        )
