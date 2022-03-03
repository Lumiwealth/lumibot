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
from ta import momentum


from lumibot.entities import Asset, Data
from lumibot.backtesting import PandasDataBacktesting, YahooDataBacktesting
from lumibot.brokers import Alpaca
from lumibot.strategies.strategy import Strategy
from lumibot.traders import Trader


class Momentum(Strategy):
    def initialize(self, crypto="BTC-USD", length=34):
        if self.is_backtesting:
            self.set_parameter_defaults(
                {
                    "crypto": self.create_asset(symbol=crypto, asset_type="stock"),
                    "target_units": Decimal("1.0"),
                }
            )
        else:
            self.set_parameter_defaults(
                {
                    "base": self.create_asset(
                        symbol=crypto.split("-")[0], asset_type="crypto"
                    ),
                    "quote": self.create_asset(
                        symbol=crypto.split("-")[1], asset_type="crypto"
                    ),
                    "target_units": Decimal("1.0"),
                }
            )
            self.parameters["crypto"] = (
                self.parameters["base"],
                self.parameters["quote"],
            )

        self.parameters["length"] = length

        self.sleeptime = 0
        self.set_market("NASDAQ")
        self.momentum_is_positive = None

    def on_trading_iteration(self):
        bars = self.get_symbol_bars(
            self.parameters["crypto"], length=self.parameters["length"], timestep="day"
        )
        bars = bars.df
        highs = bars[["high"]].squeeze()
        lows = bars[["low"]].squeeze()

        mom = momentum.awesome_oscillator(
            highs,
            lows,
            window1=self.parameters["length"] // 10,
            window2=self.parameters["length"],
        )[-1]

        if self.momentum_is_positive == None:
            if mom > 0:
                self.momentum_is_positive = True
            else:
                self.momentum_is_positive = False

        if (
            self.momentum_is_positive == False
            and mom > 20
            and len(self.get_positions()) == 0
        ):
            order = self.create_order(
                self.parameters["crypto"], self.parameters["target_units"], "buy"
            )
            self.submit_order(order)
            self.momentum_is_positive = True
            self.max_momentum = mom

        if self.momentum_is_positive == True and mom < 10:
            self.momentum_is_positive = False
            self.max_momentum = None
            if len(self.get_positions()) > 0:
                self.sell_all()


def run(live=False, length=34):
    strategy = Momentum

    if not live:
        # Backtest this strategy
        # backtesting_start = datetime.datetime(2016, 1, 1)
        backtesting_start = datetime.datetime(2020, 1, 1)
        backtesting_end = datetime.datetime(2022, 1, 31)
        strategy.backtest(
            YahooDataBacktesting,
            backtesting_start,
            backtesting_end,
            budget=50000,
            crypto="BTC-USD",
            length=length,
            name=f"Momentum {length}",
        )
    else:
        trader = Trader()
        exchange_id = "coinbasepro_bitcoin"
        broker = Ccxt(CcxtConfig.EXCHANGE_KEYS[exchange_id])

        strategy = Momentum(broker, crypto="BTC-USD", length=length)

        trader.add_strategy(strategy)
        trader.run_all()


if __name__ == "__main__":
    lengths = [30, 40,]  # 50, 60, 70, 80]
    for length in lengths:
        run(live=False, length=length)
