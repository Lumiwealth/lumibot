from datetime import datetime

from lumibot.components.configs_helper import ConfigsHelper
from lumibot.credentials import IS_BACKTESTING, ALPACA_TEST_CONFIG
from lumibot.example_strategies.drift_rebalancer import DriftRebalancer
from lumibot.backtesting import AlpacaBacktesting
from lumibot.tools.pandas import print_full_pandas_dataframes

print_full_pandas_dataframes()

"""
Strategy Description

This is an implementation of a classic 60% stocks, 40% bonds portfolio. 
It demonstration the DriftRebalancer strategy and AlpacaBacktesting. 
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

    results, strategy = DriftRebalancer.run_backtest(
        datasource_class=AlpacaBacktesting,
        backtesting_start= datetime(2023, 1, 2),
        backtesting_end=datetime(2025, 1, 1),
        benchmark_asset=None,
        parameters=parameters,
        show_plot=False,
        show_tearsheet=False,
        save_tearsheet=False,
        show_indicators=False,
        save_logfile=False,
        show_progress_bar=True,

        # AlpacaBacktesting kwargs
        tickers=['SPY', 'TLT'],
        timestep='day',
        config=ALPACA_TEST_CONFIG,
    )

    print(results)
    trades_df = strategy.broker._trade_event_log_df  # noqa
    filled_orders = trades_df[(trades_df["status"] == "fill")]
    print(
        f"\nfilled_orders:\n",
        f"{filled_orders[['time', 'symbol', 'type', 'side', 'status', 'price', 'filled_quantity', 'trade_cost']]}"
    )
