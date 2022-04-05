import argparse
import logging
import datetime
import pandas as pd
from time import perf_counter, time

from lumibot.backtesting import YahooDataBacktesting, PandasDataBacktesting
from lumibot.entities import Asset, Data
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
backtesting_start = datetime.datetime(2019, 2, 28)
backtesting_end = datetime.datetime(2019, 12, 1)
logfile = "logs/test.log"

# Trading objects
trader = Trader(logfile=logfile, debug=debug)


trading_hours_start = datetime.time(9, 30)
trading_hours_end = datetime.time(15, 30)
pandas_data = dict()
tickers = ["SPY", "DJP", "TLT", "GLD", "IEF"]
for ticker in tickers:
    asset = Asset(
        symbol=ticker,
        asset_type="stock",
    )
    df = pd.read_csv(
        f"data/{ticker}.csv",
        parse_dates=True,
        index_col=0,
        header=0,
        usecols=[0, 1, 2, 3, 4, 6],
        names=["Date", "Open", "High", "Low", "Close", "Volume"],
    )
    df = df.rename(
        columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume",
        }
    )
    df = df[["open", "high", "low", "close", "volume"]]
    df.index.name = "datetime"

    data = Data(
        asset,
        df,
        timestep="day",
    )
    pandas_data[asset] = data

# Strategies mapping
mapping = {
    "momentum_pandas": {
        "class": Momentum,
        "backtesting_datasource": PandasDataBacktesting,
        "kwargs": {"symbols": list(pandas_data)},
        "config": None,
        "pandas_data": pandas_data,
    },
    "diversification": {
        "class": Diversification,
        "backtesting_datasource": PandasDataBacktesting,
        "kwargs": {},
        "config": None,
        "pandas_data": pandas_data,
    },
    "debt_trading": {
        "class": DebtTrading,
        "backtesting_datasource": PandasDataBacktesting,
        "kwargs": {},
        "config": None,
        "pandas_data": pandas_data,
    },
    "buy_and_hold": {
        "class": BuyAndHold,
        "backtesting_datasource": PandasDataBacktesting,
        "kwargs": {},
        "backtesting_cache": False,
        "config": None,
        "pandas_data": pandas_data,
    },
    "simple": {
        "class": Simple,
        "backtesting_datasource": PandasDataBacktesting,
        "kwargs": {},
        "backtesting_cache": False,
        "config": None,
        "pandas_data": pandas_data,
    },
}

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        f"\n\
        Running AlgoTrader\n\
        Usage: ‘python main.py [strategies]’\n\
        Where strategies can be any of diversification, momentum, intraday_momentum, simple\n\
        Example: ‘python main.py momentum’ "
    )
    parser.add_argument("strategies", nargs="+", help="list of strategies")

    args = parser.parse_args()

    strategies = args.strategies

    for strategy_name in strategies:
        strategy_params = mapping.get(strategy_name)
        if strategy_params is None:
            raise ValueError(f"Strategy {strategy_name} does not exist")

        strategy_class = strategy_params["class"]
        backtesting_datasource = strategy_params["backtesting_datasource"]
        pandas_data = (
            strategy_params["pandas_data"] if "pandas_data" in strategy_params else None
        )
        kwargs = strategy_params["kwargs"]
        config = strategy_params["config"]

        stats_file = f"logs/strategy_{strategy_class.__name__}_{int(time())}.csv"

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
            show_plot=True,
            **kwargs,
        )
        toc = perf_counter()
        print("Elapsed time:", toc - tic)

        # logging.info(f"*** Benchmark Performance for {benchmark_asset} ***")
        # indicators.calculate_returns(
        #     benchmark_asset, backtesting_start, backtesting_end
        # )

    for counter, values in perf_counters.counters.items():
        print("Count %s spent %fs" % (counter, values[0]))

    logging.info("The end")
