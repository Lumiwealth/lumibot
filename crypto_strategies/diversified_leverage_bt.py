import datetime
from decimal import Decimal
import logging
import pandas as pd
from pathlib import Path
import random
import time

from lumibot.backtesting import PandasDataBacktesting
from lumibot.entities import Asset, Data
from lumibot.strategies.strategy import Strategy


"""
Strategy Description

This strategy will buy a few cryptocurrencies that have 2x or 3x returns (have leverage), 
but will 
also diversify and rebalance the portfolio often.
"""


class DiversifiedLeverage(Strategy):
    # =====Overloading lifecycle methods=============

    def initialize(self):

        self.set_parameter_defaults(
            {
                "portfolio": [
                    {"symbol": "BTC", "weight": 0.35},
                    {"symbol": "ETH", "weight": 0.25},
                    {"symbol": "BNB", "weight": 0.20},
                    {"symbol": "NEO", "weight": 0.10},
                    {"symbol": "LTC", "weight": 0.10},
                ],
                "rebalance_period": 8,
            }
        )

        # Setting the waiting period (in days) and the counter
        self.rebalance_period = self.parameters["rebalance_period"]

        # There is only one trading operation per day
        # no need to sleep between iterations
        self.sleeptime = 30

        # Initializing the portfolio variable with the assets and proportions we want to own
        self.initialized = False

        self.minutes_before_closing = 1

        # Setting the waiting period (in minutes) and the counter
        self.counter = 0


    def on_trading_iteration(self):
        # If the target number of minutes (period) has passed, rebalance the portfolio
        if self.counter == self.rebalance_period or self.counter == 0:
            self.counter = 0
            self.rebalance_portfolio()
            self.log_message(
                f"Next portfolio rebalancing will be in {self.rebalance_period} minute(s)"
            )

        self.counter += 1

    def trace_stats(self, context, snapshot_before):
        # Add the price, quantity and weight of each asset for the time period (row)
        row = {}
        for item in self.parameters["portfolio"]:
            # Symbol is a dictionary with price, quantity and weight of the asset
            symbol = item.get("symbol")
            for key in item:
                if key != "symbol":
                    row[f"{symbol}_{key}"] = item[key]

        return row

    # =============Helper methods====================

    def on_parameters_updated(self, parameters):
        """
        This method is called when the parameters are updated.
        """
        logging.info(f"Parameters updated, {parameters}")
        self.rebalance_portfolio()

    def rebalance_portfolio(self):
        """Rebalance the portfolio and create orders"""

        # Wait until we have the value of the portfolio first before reunning this function
        if self.portfolio_value is None:
            return

        orders = []
        for asset in self.parameters["portfolio"]:
            # Get all of our variables from portfolio
            symbol = asset.get("symbol")
            weight = asset.get("weight")
            last_price = self.get_last_price(symbol)

            # Get how many shares we already own
            # (including orders that haven't been executed yet)
            quantity = Decimal(str(self.get_asset_potential_total(symbol)))
            if quantity:
                logging.info(
                    "Asset %s shares value: %.2f$. %.2f$ per %d shares."
                    % (symbol, float(quantity) * last_price, last_price, quantity)
                )

            # Calculate how many shares we need to buy or sell
            shares_value = self.portfolio_value * weight
            new_quantity = Decimal(str(shares_value / last_price))
            quantity_difference = new_quantity - quantity
            self.log_message(
                "Weighted %s shares value with %.2f%% weight: %.2f$. %.2f$ per %d shares."
                % (symbol, weight * 100, shares_value, last_price, new_quantity)
            )

            # If quantity is positive then buy, if it's negative then sell
            side = ""
            if quantity_difference > 0:
                side = "buy"
            elif quantity_difference < 0:
                side = "sell"

            # Execute the order if necessary
            if side:
                order = self.create_order(symbol, abs(quantity_difference), side)
                orders.append(order)
                asset["quantity"] = new_quantity

        self.submit_orders(orders)

    def trace_stats(self, context, snapshot_before):
        random_number = random.randint(0, 100)
        row = {"my_custom_stat": random_number, "counter": self.counter}

        return row


if __name__ == "__main__":
    # Choose your budget and log file locations
    budget = 1000000
    logfile = "logs/test.log"
    backtesting_start = datetime.datetime(2021, 3, 1)  # 2018
    backtesting_end = datetime.datetime(2021, 5, 1)
    benchmark_asset = "BTC-USD"

    strategy_name = "Diversified Leverage"
    strategy_class = DiversifiedLeverage

    backtesting_datasource = PandasDataBacktesting

    # Development: Minute Data

    symbols = ["BTC", "ETH", "BNB", "NEO", "LTC"]

    pandas_data = dict()
    for symbol in symbols:
        asset = Asset(symbol=symbol, asset_type="stock")

        data_dir = Path("/media/runout/run-out-ssd/data/crypto_data")
        filename = f"{symbol}-USDT.parquet"
        filepath = str(data_dir / filename)
        df = pd.read_parquet(filepath)
        df.index.name = "datetime"
        df.index = df.index.tz_localize("UTC")
        df = df.sort_index()
        df = df[["open", "high", "low", "close", "volume"]]
        df = df.dropna()

        pandas_data[asset] = Data(
            asset,
            df,
            date_start=backtesting_start,
            date_end=backtesting_end,
            timestep="minute",
        )

    stats_file = f"logs/strategy_{strategy_class.__name__}_{int(time.time())}.csv"

    ####
    # Run the strategy
    ####
    strategy_class.backtest(
        backtesting_datasource,
        backtesting_start,
        backtesting_end,
        pandas_data=pandas_data,
        stats_file=stats_file,
        budget=budget,
        name="Diversified Leverage",
        benchmark_asset=benchmark_asset,
    )
