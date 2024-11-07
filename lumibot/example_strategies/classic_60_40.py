from datetime import datetime

from lumibot.credentials import IS_BACKTESTING
from lumibot.example_strategies.drift_rebalancer import DriftRebalancer

"""
Strategy Description

This strategy demonstrates the DriftRebalancer by rebalancing to a classic 60% stocks, 40% bonds portfolio.
It rebalances a portfolio of assets to a target weight every time the asset drifts
by a certain threshold. The strategy will sell the assets that has drifted the most and buy the
assets that has drifted the least to bring the portfolio back to the target weights.
"""


if __name__ == "__main__":

    if not IS_BACKTESTING:
        print("This strategy is not meant to be run live. Please set IS_BACKTESTING to True.")
        exit()
    else:

        parameters = {
            "market": "NYSE",
            "sleeptime": "1D",
            "drift_threshold": "0.05",
            "acceptable_slippage": "0.005",  # 50 BPS
            "fill_sleeptime": 15,
            "target_weights": {
                "SPY": "0.60",
                "TLT": "0.40"
            },
            "shorting": False
        }

        from lumibot.backtesting import YahooDataBacktesting

        backtesting_start = datetime(2023, 1, 2)
        backtesting_end = datetime(2024, 10, 31)

        results = DriftRebalancer.backtest(
            YahooDataBacktesting,
            backtesting_start,
            backtesting_end,
            benchmark_asset="SPY",
            parameters=parameters,
            show_plot=False,
            show_tearsheet=False,
            save_tearsheet=False,
            show_indicators=False,
            save_logfile=False,
            # show_progress_bar=False,
            # quiet_logs=False
        )

        print(results)
