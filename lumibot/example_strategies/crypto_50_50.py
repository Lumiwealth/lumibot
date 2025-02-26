from datetime import datetime

from lumibot.components.configs_helper import ConfigsHelper
from lumibot.credentials import IS_BACKTESTING
from lumibot.example_strategies.drift_rebalancer import DriftRebalancer
from lumibot.backtesting import AlpacaBacktesting, PandasDataBacktesting
from lumibot.credentials import ALPACA_CONFIG

"""
Strategy Description

This strategy demonstration shows the DriftRebalancer hourly data using AlpacaBacktesting. 
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

    if not ALPACA_CONFIG:
        print("This strategy requires an ALPACA_CONFIG config file to be set.")
        exit()

    if not ALPACA_CONFIG['PAPER']:
        print(
            "Even though this is a backtest, and only uses the alpaca keys for the data source"
            "you should use paper keys."
        )
        exit()

    backtesting_start = datetime(2025, 1, 1)
    backtesting_end = datetime(2025, 2, 1)
    tickers = ["BTC/USD", "ETH/USD"]  # must be the Alpaca tickers for the Assets in the config file
    timestep = 'hour'

    data_source = AlpacaBacktesting(
        tickers=tickers,
        start_date=backtesting_start.date().isoformat(),
        end_date=backtesting_end.date().isoformat(),
        timestep=timestep,
        config=ALPACA_CONFIG,
    )

    strategy: DriftRebalancer
    results, strategy = DriftRebalancer.run_backtest(
        datasource_class=PandasDataBacktesting,
        pandas_data=data_source.pandas_data,
        backtesting_start=backtesting_start,
        backtesting_end=backtesting_end,
        parameters=parameters,
        show_plot=False,
        show_tearsheet=False,
        save_tearsheet=False,
        show_indicators=False,
        save_logfile=False,
        show_progress_bar=True,
        include_cash_positions=True,
    )

    trades_df = strategy.broker._trade_event_log_df # noqa
    filled_orders = trades_df[(trades_df["status"] == "fill")]
    print(
        f"\nfilled_orders:\n"
        f"{filled_orders[['time','symbol','type','side','status', 'price', 'filled_quantity', 'trade_cost']]}"
    )
