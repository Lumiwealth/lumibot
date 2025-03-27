from datetime import datetime
import pytz

from lumibot.components.configs_helper import ConfigsHelper
from lumibot.credentials import IS_BACKTESTING, ALPACA_TEST_CONFIG
from lumibot.example_strategies.drift_rebalancer import DriftRebalancer
from lumibot.backtesting import AlpacaBacktesting
from lumibot.tools.pandas import print_full_pandas_dataframes
from lumibot.traders.debug_log_trader import DebugLogTrader
from lumibot.entities import Asset

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

    if not ALPACA_TEST_CONFIG:
        print("This strategy requires an ALPACA_TEST_CONFIG config file to be set.")
        exit()

    if not ALPACA_TEST_CONFIG['PAPER']:
        print(
            "Even though this is a backtest, and only uses the alpaca keys for the data source"
            "you should use paper keys."
        )
        exit()

    tzinfo = pytz.timezone('America/New_York')
    backtesting_start = tzinfo.localize(datetime(2022, 1, 1))
    backtesting_end = tzinfo.localize(datetime(2025, 1, 1))
    timestep = 'day'
    auto_adjust = True
    warm_up_trading_days = 0
    refresh_cache = False

    results, strategy = DriftRebalancer.run_backtest(
        name="classic_60_40",
        datasource_class=AlpacaBacktesting,
        backtesting_start=backtesting_start,
        backtesting_end=backtesting_end,
        minutes_before_closing=0,
        benchmark_asset=Asset("SPY"),
        analyze_backtest=True,
        parameters=parameters,

        # For seeing logs (if using DebugLogTrader, set show_progress_bar to false)
        trader_class=DebugLogTrader,
        # show_progress_bar=True,

        # AlpacaBacktesting kwargs
        timestep=timestep,
        market=parameters['market'],
        config=ALPACA_TEST_CONFIG,
        refresh_cache=refresh_cache,
        warm_up_trading_days=warm_up_trading_days,
        auto_adjust=auto_adjust,
    )

    print(results)
    trades_df = strategy.broker._trade_event_log_df  # noqa
    filled_orders = trades_df[(trades_df["status"] == "fill")]
    print(
        f"\nfilled_orders:\n",
        f"{filled_orders[['time', 'symbol', 'type', 'side', 'status', 'price', 'filled_quantity', 'trade_cost']]}"
    )
