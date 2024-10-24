from datetime import datetime

from lumibot.strategies.strategy import Strategy
from lumibot.traders.trader import Trader

"""
This example shows how to derive from Trader and use that instance when backtesting. This toy example simply
sets debug to True by default.
"""


class DebugLogTrader(Trader):
    """I'm just a trader instance with debug turned on by default"""

    def __init__(self, logfile="", backtest=False, debug=True, strategies=None, quiet_logs=False):
        super().__init__(logfile=logfile, backtest=backtest, debug=debug, strategies=strategies, quiet_logs=quiet_logs)


class BuyAndHold(Strategy):
    parameters = {
        "buy_symbol": "QQQ",
    }

    # =====Overloading lifecycle methods=============

    def initialize(self):
        # Set the sleep time to one day (the strategy will run once per day)
        self.sleeptime = "1D"

    def on_trading_iteration(self):
        """Buys the self.buy_symbol once, then never again"""

        # Get the current datetime and log it
        dt = self.get_datetime() # We use this function so that we get the time in teh backtesting environment
        self.log_message(f"Current datetime: {dt}")

        # Get the symbol to buy from the parameters
        buy_symbol = self.parameters["buy_symbol"]

        # What to do each iteration

        # Get the current value of the symbol and log it
        current_value = self.get_last_price(buy_symbol)
        self.log_message(f"The value of {buy_symbol} is {current_value}")

        # Add a line to the indicator chart
        self.add_line(f"{buy_symbol} Value", current_value)

        # Get all the positions that we have
        all_positions = self.get_positions()

        # If we don't own anything (other than USD), buy the asset
        if len(all_positions) <= 1:  # Because we always have a cash position (USD)

            # Calculate the quantity to buy
            quantity = int(self.portfolio_value // current_value)

            # Create the order and submit it
            purchase_order = self.create_order(buy_symbol, quantity, "buy")
            self.submit_order(purchase_order)


if __name__ == "__main__":

    from lumibot.backtesting import YahooDataBacktesting

    # Backtest this strategy
    backtesting_start = datetime(2023, 1, 1)
    backtesting_end = datetime(2024, 9, 1)

    results = BuyAndHold.backtest(
        YahooDataBacktesting,
        backtesting_start,
        backtesting_end,
        benchmark_asset="SPY",
        trader_class=DebugLogTrader,
        show_plot = False,
        show_tearsheet = False,
        save_tearsheet = False,
        save_logfile = False,
        show_indicators = False,
        show_progress_bar = False,
        quiet_logs = False,
    )

    # Print the results
    print(results)

