import argparse
import logging
import sys
from datetime import datetime
from time import time

from credentials import AlpacaConfig
from lumibot.backtesting import YahooDataBacktesting, AlpacaDataBacktesting
from lumibot.brokers import Alpaca
from lumibot.data_sources import AlpacaData
from lumibot.strategies.examples import (
    Diversification,
    IntradayMomentum,
    Momentum,
    Screener,
)
from lumibot.tools import indicators
from lumibot.traders import Trader

# Global parameters
debug = False
budget = 40000
backtesting_start = datetime(2018, 1, 1)
backtesting_end = datetime(2018, 5, 1)  # datetime.now()
logfile = "logs/test.log"

# Trading objects
alpaca_broker = Alpaca(AlpacaConfig)
alpaca_data_source = AlpacaData(AlpacaConfig)
trader = Trader(logfile=logfile, debug=debug)

# Strategies mapping
mapping = {
    "momentum": {
        "class": Momentum,
        "backtesting_datasource": YahooDataBacktesting,
        "auth": None,
    },
    "diversification": {
        "class": Diversification,
        "backtesting_datasource": YahooDataBacktesting,
        "auth": None,
    },
    "intraday_momentum": {
        "class": IntradayMomentum,
        "backtesting_datasource": AlpacaDataBacktesting,
        "auth": AlpacaConfig,
    },
    "screener": {"class": Screener, "backtesting_datasource": None, "auth": None},
}

if __name__ == "__main__":
    # Set the benchmark asset for backtesting to be "SPY" by default
    benchmark_asset = "SPY"

    parser = argparse.ArgumentParser("Running AlgoTrader")
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
        auth = strategy_params["auth"]

        stat_file = f"logs/strategy_{strategy_class.__name__}_{int(time())}.csv"
        if live_trading:
            strategy = strategy_class(
                budget=budget, broker=alpaca_broker, stat_file=stat_file
            )
            trader.add_strategy(strategy)
        else:
            if backtesting_datasource is None:
                raise ValueError(
                    f"Backtesting is not supported for strategy {strategy_name}"
                )

            strategy_class.backtest(
                backtesting_datasource,
                budget,
                backtesting_start,
                backtesting_end,
                stat_file=stat_file,
                auth=auth,
            )

            logging.info(f"*** Benchmark Performance for {benchmark_asset} ***")
            indicators.calculate_returns(
                benchmark_asset, backtesting_start, backtesting_end
            )

    if live_trading:
        trader.run_all()

    logging.info("The end")
