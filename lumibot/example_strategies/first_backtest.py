"""Offline installation check using synthetic prices and the real backtest engine.

Run with: python -m lumibot.example_strategies.first_backtest
No credentials, network data, model calls or browser opening are required.
The results demonstrate order mechanics, not an investment opportunity.
"""
from datetime import datetime
import os

import pandas as pd

from lumibot.backtesting import PandasDataBacktesting
from lumibot.entities import Asset, Data
from lumibot.strategies import Strategy


class FirstBacktest(Strategy):
    def initialize(self):
        self.sleeptime = "1M"

    def on_trading_iteration(self):
        if self.first_iteration:
            self.submit_order(self.create_order("DEMO", 1, "buy"))


def run_example():
    override = os.environ.get("BACKTESTING_DATA_SOURCE", "").strip().lower()
    if override not in {"", "none"}:
        raise ValueError("For this offline check, set BACKTESTING_DATA_SOURCE=none before running.")
    asset = Asset("DEMO", Asset.AssetType.STOCK)
    prices = [100.0, 101.0, 102.0, 103.0, 104.0, 105.0]
    frame = pd.DataFrame(
        {"open": prices, "high": prices, "low": prices, "close": prices, "volume": 1000},
        index=pd.date_range("2025-01-06 09:30", periods=6, freq="min", tz="America/New_York"),
    )
    return FirstBacktest.run_backtest(
        PandasDataBacktesting,
        datetime(2025, 1, 6, 9, 30), datetime(2025, 1, 6, 9, 35),
        pandas_data={asset: Data(asset, frame, timestep="minute")},
        budget=10_000, benchmark_asset=None, analyze_backtest=False,
        show_plot=False, show_tearsheet=False, save_tearsheet=False,
        show_indicators=False, save_logfile=False, show_progress_bar=False,
    )


if __name__ == "__main__":
    _, strategy = run_example()
    print("Synthetic installation check complete; not historical performance.")
    print(f"Ending portfolio value: {strategy.portfolio_value:.2f}")
    print(strategy.broker._trade_event_log_df.to_string(index=False))
