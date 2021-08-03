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
from lumibot.backtesting import PandasDataBacktesting
from lumibot.brokers import Alpaca
from lumibot.strategies.examples import Momentum

budget = 50000
logfile = "logs/test.log"
broker = Alpaca(AlpacaConfig)

backtesting_start = datetime(2020, 1, 7)
backtesting_end = datetime(2020, 1, 9)

asset = "SPY"
df = pd.read_csv("tests/data/spy_test.csv")
df = df.set_index("time")
df.index = pd.to_datetime(df.index)
my_data = dict()
my_data[asset] = df

strategy_name = "Momentum"
strategy = Momentum(name=strategy_name, budget=budget, broker=broker)

datestring = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
stats_file = f"logs/{strategy_name}_{datestring}.csv"
plot_file = f"logs/{strategy_name}_{datestring}.jpg"
# result = strategy.backtest(
#     strategy_name,
#     budget,
#     PandasDataBacktesting,
#     backtesting_start,
#     backtesting_end,
#     stats_file=stats_file,
#     plot_file=plot_file,
#     config=None,
#     pandas_data=my_data,
# )


def test_result():
    # Not currently working!
    assert 1 == 5
