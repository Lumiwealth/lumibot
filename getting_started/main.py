import argparse
import logging
from datetime import datetime
import pandas as pd
from time import perf_counter, time

from credentials import AlpacaConfig
from lumibot.backtesting import YahooDataBacktesting
from lumibot.brokers import Alpaca
from lumibot.data_sources import AlpacaData
from lumibot.entities import Asset
from lumibot.strategies.examples import (
    BuyAndHold,
    DebtTrading,
    Diversification,
    FastTrading,
    IntradayMomentum,
    Momentum,
    Simple,
)
from lumibot.tools import indicators, perf_counters
from lumibot.traders import Trader

# Global parameters
debug = True

backtesting_start = datetime(2019, 1, 4)
backtesting_end = datetime(2019, 4, 30)


logfile = "logs/test.log"

# Trading objects
alpaca_broker = Alpaca(AlpacaConfig)
alpaca_data_source = AlpacaData(AlpacaConfig)
trader = Trader(logfile=logfile, debug=debug)


# This file is currenlty supporting Pandas loading of day data: Stage 1
# development. Load pandas dataframes into dictionaries with `Asset` as key.
# Columns must be ['datetime', 'open', 'high', 'low', 'close', 'volume']
# Use "backtesting_datasource": PandasDataBacktesting,
# Make sure your start and end dates are inside the range of your data.

# Diversification: Multi Daily data.
# CSV Dates are start 2019-01-02 to end 2019-12-31
tickers = ["SPY", "TLT", "IEF", "GLD", "DJP",]
day_data = dict()
for ticker in tickers:
    day_data[Asset(symbol=ticker)] = pd.read_csv(f"data/{ticker}.csv")

# Strategies mapping
mapping = {
    "momentum": {
        "class": Momentum,
        "backtesting_datasource": YahooDataBacktesting,
        "kwargs": {"symbols": tickers},  # use yahoo-> {"symbols": ["SPY", "VEU", "AGG"]},
        "config": None,
        "pandas_data": day_data,
    },
    "diversification": {
        "class": Diversification,
        "backtesting_datasource": YahooDataBacktesting,
        "kwargs": {},
        "config": None,
        "pandas_data": day_data,
    },
    "debt_trading": {
        "class": DebtTrading,
        "backtesting_datasource": YahooDataBacktesting,
        "kwargs": {},
        "config": None,
        "pandas_data": day_data,
    },
    "intraday_momentum": {
        "class": IntradayMomentum,
        "backtesting_datasource": None,
        "kwargs": {},
        "config": None,
    },
    "fast_trading": {
        "class": FastTrading,
        "backtesting_datasource": None,
        "kwargs": {},
        "backtesting_cache": False,
        "config": None,
    },
    "buy_and_hold": {
        "class": BuyAndHold,
        "backtesting_datasource": YahooDataBacktesting,
        "kwargs": {},
        "backtesting_cache": False,
        "config": None,
        "pandas_data": day_data,
    },
    "simple": {
        "class": Simple,
        "backtesting_datasource": YahooDataBacktesting,
        "kwargs": {},
        "backtesting_cache": False,
        "config": None,
    },
}

if __name__ == "__main__":
    # Set the benchmark asset for backtesting to be "SPY" by default
    benchmark_asset = "SPY"

    parser = argparse.ArgumentParser(
        f"\n\
        Running AlgoTrader\n\
        Usage: ‘python main.py [strategies]’\n\
        Where strategies can be any of diversification, momentum, intraday_momentum, simple\n\
        Example: ‘python main.py momentum’ "
    )
    parser.add_argument("strategies", nargs="+", help="list of strategies")
    parser.add_argument(
        "-l",
        "--live-trading",
        default=False,
        action="store_true",
        help="enable live trading",
    )

    args = parser.parse_args()

    strategies = args.strategies
    live_trading = args.live_trading

    for strategy_name in strategies:
        strategy_params = mapping.get(strategy_name)
        if strategy_params is None:
            raise ValueError(f"Strategy {strategy_name} does not exist")

        strategy_class = strategy_params["class"]
        backtesting_datasource = strategy_params["backtesting_datasource"]
        pandas_data = strategy_params['pandas_data'] if 'pandas_data' in strategy_params else None
        kwargs = strategy_params["kwargs"]
        config = strategy_params["config"]

        stats_file = f"logs/strategy_{strategy_class.__name__}_{int(time())}.csv"
        if live_trading:
            strategy = strategy_class(
                broker=alpaca_broker,
                stats_file=stats_file,
                **kwargs,
            )
            trader.add_strategy(strategy)
        else:
            if backtesting_datasource is None:
                raise ValueError(
                    f"Backtesting is not supported for strategy {strategy_name}"
                )

            tic = perf_counter()
            strategy_class.backtest(
                backtesting_datasource,
                backtesting_start,
                backtesting_end,
                pandas_data=pandas_data,
                stats_file=stats_file,
                config=config,
                logfile=logfile,
                **kwargs,
            )
            toc = perf_counter()
            print("Elapsed time:", toc - tic)

            logging.info(f"*** Benchmark Performance for {benchmark_asset} ***")
            indicators.calculate_returns(
                benchmark_asset, backtesting_start, backtesting_end
            )

    if live_trading:
        trader.run_all()

    for counter, values in perf_counters.counters.items():
        print("Count %s spent %fs" % (counter, values[0]))

    logging.info("The end")
