import datetime
import multiprocessing
import os
import shutil
from pathlib import Path
from time import sleep, time

import pandas as pd
import pytest

from lumibot.backtesting import PandasDataBacktesting
from lumibot.entities import Asset, Data
from lumibot.example_strategies.stock_momentum import Momentum
from lumibot.traders import Trader

# Skip all the tests in this file
pytest.skip("all tests still WIP", allow_module_level=True)

os.makedirs("./logs", exist_ok=True)

# Global parameters
debug = True
backtesting_start = datetime.datetime(2019, 3, 1)
backtesting_end = datetime.datetime(2019, 11, 1)

logfile = "logs/test.log"
trader = Trader(logfile=logfile, debug=debug)


trading_hours_start = datetime.time(9, 30)
trading_hours_end = datetime.time(15, 30)
pandas_data = dict()
tickers = ["SPY", "DJP", "TLT", "GLD", "IEF"]
data_dir = Path(__file__).parent.parent / "data"
for ticker in tickers:
    csv_path = data_dir / f"{ticker}.csv"
    asset = Asset(
        symbol=ticker,
        asset_type="stock",
    )
    df = pd.read_csv(
        csv_path,
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
        date_start=datetime.datetime(2019, 1, 6),
        date_end=datetime.datetime(2019, 12, 15),
        timestep="day",
    )
    pandas_data[asset] = data

# Strategies mapping
mapping = {
    "momentum": {
        "class": Momentum,
        "backtesting_datasource": PandasDataBacktesting,
        "kwargs": {"symbols": list(pandas_data)},
        "config": None,
        "pandas_data": pandas_data,
        "logfile": "logs/log_momentum.txt",
    },
    # "diversification": {
    #     "class": Diversification,
    #     "backtesting_datasource": PandasDataBacktesting,
    #     "kwargs": {},
    #     "config": None,
    #     "pandas_data": pandas_data,
    #     "logfile": "logs/log_diversification.txt",
    # },
    # "debt_trading": {
    #     "class": DebtTrading,
    #     "backtesting_datasource": PandasDataBacktesting,
    #     "kwargs": {},
    #     "config": None,
    #     "pandas_data": pandas_data,
    #     "logfile": "logs/log_debt.txt",
    # },
    # "buy_and_hold": {
    #     "class": BuyAndHold,
    #     "backtesting_datasource": PandasDataBacktesting,
    #     "kwargs": {},
    #     "backtesting_cache": False,
    #     "config": None,
    #     "pandas_data": pandas_data,
    #     "logfile": "logs/log_buyhold.txt",
    # },
    # "simple": {
    #     "class": Simple,
    #     "backtesting_datasource": PandasDataBacktesting,
    #     "kwargs": {},
    #     "backtesting_cache": False,
    #     "config": None,
    #     "pandas_data": pandas_data,
    #     "logfile": "logs/log_simple.txt",
    # },
}


def run_test(strategy_name):
    strategy_params = mapping.get(strategy_name)
    logfile = strategy_params["logfile"]
    trader = Trader(logfile=strategy_params["logfile"], debug=debug)
    strategy_class = strategy_params["class"]
    backtesting_datasource = strategy_params["backtesting_datasource"]
    pandas_data = (
        strategy_params["pandas_data"] if "pandas_data" in strategy_params else None
    )
    kwargs = strategy_params["kwargs"]
    config = strategy_params["config"]

    if backtesting_datasource is None:
        raise ValueError(f"Backtesting is not supported for strategy {strategy_name}")

    # Replace the strategy name now that it's known.
    for data in pandas_data.values():
        data.strategy = strategy_name
    stats_file = f"logs/strategy_{strategy_class.__name__}_{int(time())}.csv"
    result = strategy_class.backtest(
        backtesting_datasource,
        backtesting_start,
        backtesting_end,
        pandas_data=pandas_data,
        stats_file=stats_file,
        config=config,
        logfile=logfile,
        risk_free_rate=0,
        show_plot=False,
        save_tearsheet=False,
        show_tearsheet=False,
        name=strategy_name,
        budget=40000,
        **kwargs,
    )
    result['strategy'] = strategy_name
    return result


def test_integration():
    # delete_logs_directory()
    agg_results = dict()

    strategies = list(mapping.keys())

    pool = multiprocessing.Pool(processes=multiprocessing.cpu_count() - 2)
    for result in pool.imap_unordered(run_test, strategies):
        agg_results[result['strategy']] = result

    pool.close()

    expected_result = {
        "buy_and_hold": {
            "cagr": 0.14760275904326248,
            "volatility": 0.13352288364371973,
            "sharpe": 1.1054491560945578,
            "romad": 2.2315656039426397,
            "total_return": 0.09509502182006835,
        },
        "simple": {
            "cagr": 0.005829326013952141,
            "volatility": 0.006478334119743725,
            "sharpe": 0.8998186734744613,
            "romad": 1.6573403606987755,
            "total_return": 0.003842514038087108,
        },
        "debt_trading": {
            "cagr": 0.20554760657213134,
            "volatility": 0.07017662276132623,
            "sharpe": 2.929003968617979,
            "romad": 8.00686191195888,
            "total_return": 0.13127250881195174,
        },
        "momentum": {
            "cagr": 0.48736434348385216,
            "volatility": 0.11638046363125136,
            "sharpe": 4.187681748957919,
            "romad": 14.647567392415182,
            "total_return": 0.29946557283401365,
        },
        "diversification": {
            "cagr": 0.19794477299026614,
            "volatility": 0.0570697876160907,
            "sharpe": 3.4684687162644363,
            "romad": 6.443675455209815,
            "total_return": 0.1265600008964547,
        },
    }
    for strategy, results in agg_results.items():
        assert round(results["cagr"], 5) == round(expected_result[strategy]["cagr"], 5)
        assert round(results["romad"], 5) == round(
            expected_result[strategy]["romad"], 5
        )
        assert round(results["sharpe"], 5) == round(
            expected_result[strategy]["sharpe"], 5
        )
        assert round(results["total_return"], 5) == round(
            expected_result[strategy]["total_return"], 5
        )
        assert round(results["volatility"], 5) == round(
            expected_result[strategy]["volatility"], 5
        )


@pytest.fixture(scope="session", autouse=True)
def cleanup(request):
    """Cleanup a testing directory once we are finished."""

    def remove_test_dir():
        if os.path.exists("logs"):
            try:
                shutil.rmtree("logs")
            except PermissionError:
                # Unit test race condition where files were still marked as "in use" as this ran
                sleep(1)
                shutil.rmtree("logs")

    request.addfinalizer(remove_test_dir)


if __name__ == "__main__":
    test_integration()
