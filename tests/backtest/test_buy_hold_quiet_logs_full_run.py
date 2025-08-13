#!/usr/bin/env python3
"""
Test version of stock_buy_and_hold.py specifically for testing quiet logs functionality
"""

# Test version - imports local lumibot for development

import datetime as dt
from dotenv import load_dotenv
from pathlib import Path
import pytz

from lumibot.backtesting import BacktestingBroker
from lumibot.traders import Trader
from lumibot.strategies.strategy import Strategy
from lumibot.credentials import ALPACA_TEST_CONFIG


class BuyAndHoldQuietLogsTest(Strategy):
    parameters = {
        "buy_symbol": "SPY",
    }

    def initialize(self):
        # Set the sleep time to one day (the strategy will run once per day)
        self.sleeptime = "10M"

    def after_market_closes(self):
        self.log_message("Custom After Market Closes method called")

    def on_trading_iteration(self):
        """Buys the self.buy_symbol once, then never again"""

        # Get the current datetime and log it
        dt = self.get_datetime()
        self.log_message(f"Current datetime: {dt}")

        # Get the symbol to buy from the parameters
        buy_symbol = self.parameters["buy_symbol"]

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
            quantity = int(self.get_portfolio_value() // current_value)

            # Create the order and submit it
            purchase_order = self.create_order(buy_symbol, quantity, "buy")
            self.submit_order(purchase_order)


if __name__ == "__main__":
    import os

    # Set environment variable for quiet logs testing
    os.environ["BACKTESTING_QUIET_LOGS"] = "true"

    from lumibot.backtesting import AlpacaBacktesting
    from lumibot.backtesting import PolygonDataBacktesting

    if not ALPACA_TEST_CONFIG:
        print("This strategy requires an ALPACA_TEST_CONFIG config file to be set.")
        exit()

    if not ALPACA_TEST_CONFIG['PAPER']:
        print(
            "Even though this is a backtest, and only uses the alpaca keys for the data source"
            "you should use paper keys."
        )
        exit()

    secrets_path = Path(__file__).parent.parent.parent / '.secrets'
    if secrets_path.exists():
        for secret_file in list(secrets_path.glob('*.env')) + list(secrets_path.glob('*.env.example')):
            load_dotenv(secret_file)

    tzinfo = pytz.timezone('America/New_York')
    backtesting_start = tzinfo.localize(dt.datetime(2025, 1, 6))
    backtesting_end = tzinfo.localize(dt.datetime(2025, 1, 8))  # Short test period
    timestep = 'day'
    auto_adjust = True
    warm_up_trading_days = 0
    refresh_cache = False

    # Respect BACKTESTING_QUIET_LOGS environment variable for progress bar
    quiet_logs_enabled = os.environ.get("BACKTESTING_QUIET_LOGS", "").lower() == "true"
    show_progress_bar = not quiet_logs_enabled

    print(f"quiet_logs_enabled: {quiet_logs_enabled}")
    print(f"show_progress_bar: {show_progress_bar}")

    # Execute Backtest | Polygon.io API Connection
    POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY")
    data_source = PolygonDataBacktesting(
        datetime_start=backtesting_start,
        datetime_end=backtesting_end,
        api_key=POLYGON_API_KEY,
    )
    broker = BacktestingBroker(data_source=data_source)
    strategy = BuyAndHoldQuietLogsTest(
        broker=broker,
        sleeptime="5M",
        budget=35000,  # Set a budget for the backtest
        parameters={
            "buy_symbol": "SPY",
        },
        save_logfile=True,
    )
    trader = Trader(logfile="./testing_quiet.log", backtest=True, quiet_logs=False)
    trader.add_strategy(strategy)
    results = trader.run_all(show_plot=False, show_tearsheet=False, save_tearsheet=False)

    # results, strategy = BuyAndHoldQuietLogsTest.run_backtest(
    #     datasource_class=AlpacaBacktesting,
    #     backtesting_start=backtesting_start,
    #     backtesting_end=backtesting_end,
    #     minutes_before_closing=0,
    #     benchmark_asset='SPY',
    #     analyze_backtest=True,
    #     parameters={
    #         "buy_symbol": "SPY",
    #     },
    #     show_progress_bar=show_progress_bar,
    #     show_tearsheet=False,
    #     show_plot=False,
    #     show_indicators=False,
    #
    #     # AlpacaBacktesting kwargs
    #     timestep=timestep,
    #     market='NYSE',
    #     config=ALPACA_TEST_CONFIG,
    #     refresh_cache=refresh_cache,
    #     warm_up_trading_days=warm_up_trading_days,
    #     auto_adjust=auto_adjust,
    # )

    print("Backtest completed successfully!")
    print(f"Results: {results}")