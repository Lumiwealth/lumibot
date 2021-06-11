import logging
from datetime import datetime
from time import time

from credentials import AlpacaConfig
from lumibot.backtesting import YahooDataBacktesting
from lumibot.brokers import Alpaca
from lumibot.strategies.examples import (
    BuyAndHold,
    DebtTrading,
    Diversification,
    DiversifiedLeverage,
    FastTrading,
    IntradayMomentum,
    Momentum,
    Simple,
)
from lumibot.tools import indicators
from lumibot.traders import Trader

# Choose your budget and log file locations
budget = 50000  # - 12.80
logfile = "logs/test.log"
backtesting_start = datetime(2010, 3, 1)
backtesting_end = datetime(2021, 1, 1)
benchmark_asset = "SPY"

# Initialize all our classes
trader = Trader(logfile=logfile)
broker = Alpaca(AlpacaConfig)

####
# Select our strategy
####

strategy_name = "DiversifiedLeverage"
strategy = DiversifiedLeverage(name=strategy_name, budget=budget, broker=broker)

####
# Backtest
####

stats_file = f"logs/strategy_{strategy_name}_{int(time())}.csv"
strategy.backtest(
    strategy_name,
    budget,
    YahooDataBacktesting,
    backtesting_start,
    backtesting_end,
    stats_file=stats_file,
    config=None,
)

logging.info(f"*** Benchmark Performance for {benchmark_asset} ***")
indicators.calculate_returns(benchmark_asset, backtesting_start, backtesting_end)

####
# Run the strategy
####

trader.add_strategy(strategy)
trader.run_all()
