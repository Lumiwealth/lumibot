import datetime
from decimal import Decimal
import logging
import numpy as np
import pandas as pd
import pandas_ta as ta
import random

from lumibot.strategies.strategy import Strategy
from lumibot.brokers.ccxt import Ccxt
from credentials import CcxtConfig
from lumibot.traders import Trader


"""
Strategy Description

The “golden cross/death cross” crypto trading strategy is a method that 
uses two moving averages (MAs) – a chart indicator line that shows the 
mean average price of an asset over a defined period of time. For this 
strategy, you are looking for crossovers between the 50 MA (an average 
of the previous 50 days) and 200 MA (an average of the previous 200 
days) over long chart time frames such as the daily and weekly charts. 
Because it deals with observing price activity over wide time periods, 
this is another long-term trading strategy that works best over 
18 months and onward.
"""


class MovingAverage(Strategy):
    # =====Overloading lifecycle methods=============

    def initialize(self):

        self.set_parameter_defaults(
            {
                "base": self.create_asset(symbol="BTC", asset_type="crypto"),
                "quote": self.create_asset(symbol="USD", asset_type="crypto"),
                "fast": 12,
                "slow": 26,
                "target_units": Decimal("1.0"),
            }
        )

        # There is only one trading operation per day
        # no need to sleep between iterations
        self.sleeptime = "10S"

        # Initializing the portfolio variable with the assets and proportions we want to own
        self.initialized = False

        self.set_market("24/7")

        # Due to the sandbox having a trillion dollars in bitcoin, we
        # will track our own units.
        self.traded = False

        # Get the initial number of units of BTC.
        position = self.get_position(self.parameters["base"])
        self.starting_units = position.quantity

    def on_trading_iteration(self):
        # Determine if MACD is a buy or sell.
        bars = self.get_symbol_bars(
            (self.parameters["base"], self.parameters["quote"]),
            self.parameters["slow"] + 100,
            timestep="day",
        )
        df_res = bars.df.dropna()
        df = df_res.copy().dropna()
        df.ta.macd(
            fast=self.parameters["fast"],
            slow=self.parameters["slow"],
            append=True,
            col_names=("MACD", "MACD_H", "MACD_S"),
        )
        df = df.dropna().copy()
        df["hpos"] = np.where(df["MACD_S"] > 0, 1, 0)
        df["macd_change"] = df["hpos"] - df["hpos"].shift()
        # If `1` then buy, if `-1` then sell, if 0, then nothing.
        signal = int(df["macd_change"][-1])

        if not self.traded and signal == 1:
            side = "buy"
        elif self.traded and signal == -1:
            side = "sell"
        else:
            side = None

        if side:
            order = self.create_order(
                asset=self.parameters["base"],
                quantity=self.parameters["target_units"],
                side=side,
                quote=self.parameters["quote"],
            )

            self.submit_order(order)
            self.traded = True if side == "buy" else False

    # =============Helper methods====================

    def on_parameters_updated(self, parameters):
        """
        This method is called when the parameters are updated.
        """
        logging.info(f"Parameters updated, {parameters}")
        # do something

    def trace_stats(self, context, snapshot_before):
        row = {
            "MACD_S": f"{context['df']['MACD_S'][-1]:7.4f}",
            "macd_change": f"{bool(context['df']['macd_change'][-1])}",
            "traded": context["self"].traded,
        }
        return row


if __name__ == "__main__":

    # Initialize all our classes
    trader = Trader()

    # Select `coinbasepro` or `coinbasepro_sandbox`
    # exchange_id = "coinbasepro_sandbox"
    exchange_id = "coinbasepro_bitcoin"
    broker = Ccxt(CcxtConfig.EXCHANGE_KEYS[exchange_id])

    strategy = MovingAverage(broker)

    trader.add_strategy(strategy)
    trader.run_all()

    # Choose your budget and log file locations
