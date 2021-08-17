import logging
from datetime import datetime
from time import perf_counter, time

import pandas as pd

from credentials import AlpacaConfig
from lumibot.backtesting import YahooDataBacktesting, PandasDataBacktesting
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
    Strangle,
)
from dev_work import Dev
from lumibot.tools import indicators
from lumibot.traders import Trader

from lumibot.entities import Asset, Data

# Choose your budget and log file locations
budget = 40000
logfile = "logs/test.log"
backtesting_start = datetime(2019, 1, 15)
backtesting_end = datetime(2019, 12, 25)
data_start = datetime(2019, 1, 1)
benchmark_asset = "SPY"

# Initialize all our classes
trader = Trader(logfile=logfile)
# broker = Alpaca(AlpacaConfig)

####
# Select our strategy
####

strategy_name = "Dev"
strategy_class = Dev

# backtesting_datasource = YahooDataBacktesting
backtesting_datasource = PandasDataBacktesting

# Development: Minute Data
# asset = "SPY"
# asset = Asset(symbol="SPY")
# df = pd.read_csv("data/dev_min_2019.csv", parse_dates=True)
# df = df.set_index('date')
# df["SMA15"] = TA.SMA(df, 15)
# df["SMA100"] = TA.SMA(df, 100)
# pandas_data = dict()
# pandas_data[asset] = Data(strategy_name, asset, df)

# Diversification: Multi Daily data.
tickers = ["SPY"]  # , "TLT", "IEF", "GLD", "DJP",]
pandas_data = dict()
for ticker in tickers:
    df = pd.read_csv("data/dev_min_2019.csv", parse_dates=True, index_col=0)
    # df = df.loc["2019-01-01": "2019-01-31", :]
    pandas_data[Asset(symbol=ticker)] = Data(
        strategy_name,
        Asset(symbol="SPY"),
        # df,
        pd.read_csv(f"data/" f"{ticker}.csv", parse_dates=True, index_col=0),
    )

stats_file = f"logs/strategy_{strategy_class.__name__}_{int(time())}.csv"

####
# Run the strategy
####
tic = perf_counter()
strategy_class.backtest(
    strategy_name,
    budget,
    backtesting_datasource,
    backtesting_start,
    backtesting_end,
    pandas_data=pandas_data,
    stats_file=stats_file,
    # config=config,
    # **kwargs,
)
toc = perf_counter()
print("Elapsed time:", toc - tic)

logging.info(f"*** Benchmark Performance for {benchmark_asset} ***")
indicators.calculate_returns(benchmark_asset, backtesting_start, backtesting_end)
