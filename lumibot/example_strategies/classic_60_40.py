from datetime import datetime

from lumibot.strategies.drift_rebalancer import DriftRebalancer

"""
Strategy Description

This strategy rebalances a portfolio of assets to a target weight every time the asset drifts
by a certain threshold. The strategy will sell the assets that has drifted the most and buy the
assets that has drifted the least to bring the portfolio back to the target weights.
"""


if __name__ == "__main__":
    is_live = False

    parameters = {
        "market": "NYSE",
        "sleeptime": "1D",
        "absolute_drift_threshold": "0.15",
        "acceptable_slippage": "0.0005",
        "fill_sleeptime": 15,
        "target_weights": {
            "SPY": "0.60",
            "TLT": "0.40"
        }
    }

    if is_live:
        from credentials import ALPACA_CONFIG
        from lumibot.brokers import Alpaca
        from lumibot.traders import Trader

        trader = Trader()
        broker = Alpaca(ALPACA_CONFIG)
        strategy = DriftRebalancer(broker=broker, parameters=parameters)
        trader.add_strategy(strategy)
        strategy_executors = trader.run_all()

    else:
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
