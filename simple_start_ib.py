import logging
from datetime import time
from datetime import datetime

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
from dev_work_ib_real_time_bars import Dev
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
kwargs = dict(
    symbol="SPY",
    time_start=time(9, 30),
    time_end=time(15, 45),
    iteration_time="5S",
    bar_size=5,
    keep_bars=15,
    period=6,
    pfast=3,
    pslow=3,
    vwap_on=False,
    min_days_expiry=2,
    max_days_expiry=44,
    spread=2,
)
strategy_name = "Dev"
strategy = Dev(name=strategy_name, budget=budget, broker=broker, **kwargs)


####
# Run the strategy
####

trader.add_strategy(strategy)
trader.run_all()
