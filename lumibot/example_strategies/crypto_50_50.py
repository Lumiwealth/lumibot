from datetime import datetime, time
from zoneinfo import ZoneInfo

from lumibot.components.configs_helper import ConfigsHelper
from lumibot.credentials import IS_BACKTESTING
from lumibot.example_strategies.drift_rebalancer import DriftRebalancer
from lumibot.backtesting import AlpacaBacktesting
from lumibot.credentials import ALPACA_TEST_CONFIG
from lumibot.tools.pandas import print_full_pandas_dataframes

print_full_pandas_dataframes()

"""
Strategy Description

This example demonstrates using crypto with the DriftRebalancer and AlpacaBacktesting. 
It rebalances a portfolio of assets to a target weight every time the asset drifts
by a certain threshold. The strategy will sell the assets that has drifted the most and buy the
assets that has drifted the least to bring the portfolio back to the target weights.
"""


if __name__ == "__main__":

    configs_helper = ConfigsHelper(configs_folder="example_strategies")
    parameters = configs_helper.load_config("crypto_50_50_config")

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

    strategy: DriftRebalancer
    results, strategy = DriftRebalancer.run_backtest(
        datasource_class=AlpacaBacktesting,
        backtesting_start=datetime(2025, 1, 1),
        backtesting_end=datetime(2025, 2, 1),
        parameters=parameters,
        show_plot=False,
        show_tearsheet=False,
        save_tearsheet=False,
        show_indicators=False,
        save_logfile=False,
        show_progress_bar=True,

        # AlpacaBacktesting kwargs
        tickers = ["BTC/USD", "ETH/USD"],  # must be the Alpaca tickers for the Assets in the config file
        timestep='hour',
        config=ALPACA_TEST_CONFIG,

        # Crypto trades 24 hours a day
        market='24/7',
        trading_hours_start=time(0, 0),
        trading_hours_end=time(23, 59),

        # Alpaca crypto daily bars are natively indexed at midnight central time
        tzinfo=ZoneInfo("America/Chicago")
    )

    print(results)
    trades_df = strategy.broker._trade_event_log_df # noqa
    filled_orders = trades_df[(trades_df["status"] == "fill")]
    print(
        f"\nfilled_orders:\n",
        f"{filled_orders[['time','symbol','type','side','status', 'price', 'filled_quantity', 'trade_cost']]}"
    )
