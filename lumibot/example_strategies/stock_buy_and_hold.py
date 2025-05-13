import datetime as dt
import pytz

from lumibot.strategies.strategy import Strategy
from lumibot.credentials import ALPACA_TEST_CONFIG

"""
Strategy Description

Simply buys one asset and holds onto it.
"""


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
        if len(all_positions) == 0:

            # Calculate the quantity to buy
            quantity = int(self.portfolio_value // current_value)

            # Create the order and submit it
            purchase_order = self.create_order(buy_symbol, quantity, "buy")
            self.submit_order(purchase_order)


if __name__ == "__main__":
    IS_BACKTESTING = True

    if IS_BACKTESTING:
        from lumibot.backtesting import AlpacaBacktesting

        if not IS_BACKTESTING:
            print("This strategy is not meant to be run live. Please set IS_BACKTESTING to True.")
            exit()

        if not ALPACA_TEST_CONFIG:
            print("This strategy requires an ALPACA_TEST_CONFIG config file to be set.")
            exit()

        if not ALPACA_TEST_CONFIG['PAPER']:
            print(
                "Even though this is a backtest, and only uses the alpaca keys for the data source"
                "you should use paper keys."
            )
            exit()

        tzinfo = pytz.timezone('America/New_York')
        backtesting_start = tzinfo.localize(dt.datetime(2023, 1, 1))
        backtesting_end = tzinfo.localize(dt.datetime(2024, 9, 1))
        timestep = 'day'
        auto_adjust = True
        warm_up_trading_days = 0
        refresh_cache = False

        results, strategy = BuyAndHold.run_backtest(
            datasource_class=AlpacaBacktesting,
            backtesting_start=backtesting_start,
            backtesting_end=backtesting_end,
            minutes_before_closing=0,
            benchmark_asset='SPY',
            analyze_backtest=True,
            parameters={
                "buy_symbol": "SPY",
            },
            show_progress_bar=True,

            # AlpacaBacktesting kwargs
            timestep=timestep,
            market='NYSE',
            config=ALPACA_TEST_CONFIG,
            refresh_cache=refresh_cache,
            warm_up_trading_days=warm_up_trading_days,
            auto_adjust=auto_adjust,
        )

        # Print the results
        print(results)
    else:
        ALPACA_CONFIG = {
            "API_KEY": "YOUR_API_KEY",
            "API_SECRET": "YOUR_API_SECRET",
            "PAPER": True,
        }

        from lumibot.brokers import Alpaca

        broker = Alpaca(ALPACA_CONFIG)

        strategy = BuyAndHold(broker=broker)
        strategy.run_live()
