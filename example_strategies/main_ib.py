import argparse
import logging
from datetime import datetime
from time import perf_counter, time

from credentials import InteractiveBrokersConfig
from lumibot.backtesting import YahooDataBacktesting
from lumibot.brokers import InteractiveBrokers
from lumibot.data_sources import InteractiveBrokersData
from lumibot.strategies.examples import (
    BuyAndHold,
    DebtTrading,
    Diversification,
    IntradayMomentum,
    Momentum,
    Simple,
    Strangle,
    FastTrading,
)
from lumibot.tools import indicators, perf_counters
from lumibot.traders import Trader

# Global parameters
debug = False
backtesting_start = datetime(2010, 1, 1)
backtesting_end = datetime(2020, 12, 31)
logfile = "logs/test.log"

# Trading objects
interactive_brokers = InteractiveBrokers(InteractiveBrokersConfig)
interactive_brokers_data_source = InteractiveBrokersData(InteractiveBrokersConfig)
trader = Trader(logfile=logfile, debug=debug)

# Strategies mapping
mapping = {
    "buy_and_hold": {
        "class": BuyAndHold,
        "backtesting_datasource": YahooDataBacktesting,
        "kwargs": {},
        "backtesting_cache": False,
        "config": None,
    },
    "debt_trading": {
        "class": DebtTrading,
        "backtesting_datasource": YahooDataBacktesting,
        "kwargs": {},
        "config": None,
    },
    "diversification": {
        "class": Diversification,
        "backtesting_datasource": YahooDataBacktesting,
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
    "intraday_momentum": {
        "class": IntradayMomentum,
        "backtesting_datasource": None,
        "kwargs": {},
        "config": None,
    },
    "momentum": {
        "class": Momentum,
        "backtesting_datasource": YahooDataBacktesting,
        "kwargs": {"symbols": ["SPY", "VEU", "AGG"]},
        "config": None,
    },
    "simple": {
        "class": Simple,
        "backtesting_datasource": YahooDataBacktesting,
        "kwargs": {},
        "config": None,
    },
    "strangle": {
        "class": Strangle,
        "backtesting_datasource": None,
        "kwargs": {},
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
        Where strategies can be any of diversification, momentum, intraday_momentum, "
        f"simple, strangle\n\
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
        kwargs = strategy_params["kwargs"]
        config = strategy_params["config"]

        stats_file = f"logs/strategy_{strategy_class.__name__}_{int(time())}.csv"
        if live_trading:
            strategy = strategy_class(
                broker=interactive_brokers,
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
                stats_file=stats_file,
                config=config,
                logfile=logfile,
                **kwargs,
            )
            toc = perf_counter()
            print("Elpased time:", toc - tic)

            logging.info(f"*** Benchmark Performance for {benchmark_asset} ***")
            indicators.calculate_returns(
                benchmark_asset, backtesting_start, backtesting_end
            )

    if live_trading:
        trader.run_all()

    for counter, values in perf_counters.counters.items():
        print("Count %s spent %fs" % (counter, values[0]))

    logging.info("The end")
