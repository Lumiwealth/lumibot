#####
# Used to load local lumibot folder into a venv
import os
import sys

myPath = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, myPath + "/../")
#####

from datetime import datetime

import pandas as pd

from credentials import AlpacaConfig
from lumibot.backtesting import YahooDataBacktesting
from lumibot.brokers import Alpaca
from lumibot.strategies.examples import Momentum

budget = 50000
logfile = "logs/test.log"
broker = Alpaca(AlpacaConfig)

backtesting_start = datetime(2020, 1, 1)
backtesting_end = datetime(2020, 2, 1)

strategy_name = "Momentum"
strategy = Momentum(name=strategy_name, budget=budget, broker=broker)

datestring = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
stats_file = f"logs/{strategy_name}_{datestring}.csv"
plot_file = f"logs/{strategy_name}_{datestring}.jpg"
result = strategy.backtest(
    strategy_name,
    budget,
    YahooDataBacktesting,
    backtesting_start,
    backtesting_end,
    stats_file=stats_file,
    plot_file=plot_file,
    config=None,
)


def test_result():
    assert result == 5
