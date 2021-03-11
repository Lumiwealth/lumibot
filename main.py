import argparse
import logging
from datetime import datetime
from time import time

from credentials import AlpacaConfig
from lumibot.backtesting import YahooDataBacktesting
from lumibot.brokers import Alpaca
from lumibot.data_sources import AlpacaData
from lumibot.strategies.examples import Diversification, IntradayMomentum, Momentum
from lumibot.tools import indicators
from lumibot.traders import Trader
from lumibot.trading_builtins import set_redis_db

# Global parameters
debug = False
budget = 40000
backtesting_start = datetime(2020, 1, 1)
backtesting_end = datetime(2020, 12, 31)
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
        "backtesting_cache": False,
        "config": None,
    },
    "diversification": {
        "class": Diversification,
        "backtesting_datasource": YahooDataBacktesting,
        "backtesting_cache": False,
        "config": None,
    },
    "intraday_momentum": {
        "class": IntradayMomentum,
        "backtesting_datasource": None,
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
        backtesting_cache = strategy_params["backtesting_cache"]
        config = strategy_params["config"]

        stats_file = f"logs/strategy_{strategy_class.__name__}_{int(time())}.csv"
        if live_trading:
            strategy = strategy_class(
                strategy_name,
                budget=budget,
                broker=alpaca_broker,
                stats_file=stats_file,
            )
            trader.add_strategy(strategy)
        else:
            if backtesting_datasource is None:
                raise ValueError(
                    f"Backtesting is not supported for strategy {strategy_name}"
                )

            if backtesting_cache:
                set_redis_db()

            strategy_class.backtest(
                strategy_name,
                budget,
                backtesting_datasource,
                backtesting_start,
                backtesting_end,
                stats_file=stats_file,
                config=config,
            )

            logging.info(f"*** Benchmark Performance for {benchmark_asset} ***")
            indicators.calculate_returns(
                benchmark_asset, backtesting_start, backtesting_end
            )

    if live_trading:
        trader.run_all()

    logging.info("The end")
