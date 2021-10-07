import logging
from datetime import datetime
from time import time

from credentials import InteractiveBrokersConfig
from lumibot.backtesting import YahooDataBacktesting
from lumibot.brokers import InteractiveBrokers
from lumibot.strategies.examples import (
    BuyAndHold,
    DebtTrading,
    Diversification,
    DiversifiedLeverage,
    FastTrading,
    IntradayMomentum,
    Momentum,
    Simple,
    Strangle,

)
from lumibot.tools import indicators
from lumibot.traders import Trader

# Choose your budget and log file locations
budget = 50000
logfile = "logs/test.log"
backtesting_start = datetime(2012, 1, 1)
backtesting_end = datetime(2021, 1, 1)
benchmark_asset = "SPY"

# Initialize all our classes
trader = Trader(logfile=logfile)
broker = InteractiveBrokers(InteractiveBrokersConfig)

####
# Select our strategy
####

strategy_name = "Simple"
strategy = Simple(name=strategy_name, budget=budget, broker=broker)


####
# Run the strategy
####

trader.add_strategy(strategy)
trader.run_all()
