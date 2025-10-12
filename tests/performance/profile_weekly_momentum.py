"""Profile Weekly Momentum Options strategy under pandas vs Polars ThetaData backtesting."""

from __future__ import annotations

import argparse
import time
from datetime import datetime
from pathlib import Path

import yappi

from lumibot.entities import Asset, TradingFee
from lumibot.backtesting import ThetaDataBacktesting, ThetaDataBacktestingPandas

from tests.performance.strategies.weekly_momentum_options import WeeklyMomentumOptionsStrategy

OUTPUT_DIR = Path("tests/performance/logs")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

BACKTEST_START = datetime(2025, 3, 1)
BACKTEST_END = datetime(2025, 9, 30)
STRATEGY_PARAMS = {}


def run_profile(mode: str) -> float:
    datasource = ThetaDataBacktesting if mode == "polars" else ThetaDataBacktestingPandas
    label = f"weekly_momentum_{mode}"
    profile_path = OUTPUT_DIR / f"{label}.prof"

    trading_fee = TradingFee(percent_fee=0.001)

    yappi.clear_stats()
    yappi.set_clock_type("wall")
    yappi.start()
    start = time.time()

    WeeklyMomentumOptionsStrategy.backtest(
        datasource,
        backtesting_start=BACKTEST_START,
        backtesting_end=BACKTEST_END,
        budget=100000,
        benchmark_asset=Asset("SPY", Asset.AssetType.STOCK),
        quote_asset=Asset("USD", Asset.AssetType.FOREX),
        buy_trading_fees=[trading_fee],
        sell_trading_fees=[trading_fee],
        parameters=STRATEGY_PARAMS,
        show_plot=False,
        show_tearsheet=False,
        save_tearsheet=False,
        show_indicators=False,
        quiet_logs=True,
        show_progress_bar=False,
        save_stats_file=False,
        save_logfile=False,
    )

    elapsed = time.time() - start
    yappi.stop()

    yappi.get_func_stats().save(str(profile_path), type="pstat")
    return elapsed


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["pandas", "polars", "both"], default="both")
    args = parser.parse_args()

    modes = [args.mode] if args.mode != "both" else ["pandas", "polars"]
    for m in modes:
        elapsed = run_profile(m)
        print(f"mode={m} elapsed={elapsed:.2f}s")


if __name__ == "__main__":
    main()
