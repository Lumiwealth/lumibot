from datetime import datetime

from lumibot.components.configs_helper import ConfigsHelper
from lumibot.credentials import IS_BACKTESTING
from lumibot.example_strategies.drift_rebalancer import DriftRebalancer
from lumibot.backtesting import YahooDataBacktesting

"""
Strategy Description

This is an implementation of a classic 60% stocks, 40% bonds portfolio, that demonstration the DriftRebalancer strategy. 
It rebalances a portfolio of assets to a target weight every time the asset drifts
by a certain threshold. The strategy will sell the assets that has drifted the most and buy the
assets that has drifted the least to bring the portfolio back to the target weights.
"""


if __name__ == "__main__":

    configs_helper = ConfigsHelper(configs_folder="example_strategies")
    parameters = configs_helper.load_config("classic_60_40_config")

    if not IS_BACKTESTING:
        print("This strategy is not meant to be run live. Please set IS_BACKTESTING to True.")
        exit()

    backtesting_start = datetime(2023, 1, 2)
    backtesting_end = datetime(2025, 1, 1)

    results = DriftRebalancer.backtest(
        YahooDataBacktesting,
        backtesting_start,
        backtesting_end,
        benchmark_asset="SPY",
        parameters=parameters,
        show_plot=True,
        show_tearsheet=False,
        save_tearsheet=False,
        show_indicators=False,
        save_logfile=True,
        show_progress_bar=True
    )

    print(results)
