"""Utility to run a narrow Weekly Momentum backtest for broker fill diagnostics."""

from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta

os.environ.setdefault("QUIET_LOGS", "false")
os.environ.setdefault("BACKTESTING_QUIET_LOGS", "false")
os.environ.setdefault("IS_BACKTESTING", "true")

from lumibot.entities import Asset, TradingFee
from lumibot.backtesting import ThetaDataBacktesting, ThetaDataBacktestingPandas
from lumibot.tools import thetadata_helper

from tests.performance.strategies.weekly_momentum_options import WeeklyMomentumOptionsStrategy


def run_range(mode: str, start: datetime, end: datetime) -> None:
    datasource = ThetaDataBacktesting if mode == "polars" else ThetaDataBacktestingPandas

    trading_fee = TradingFee(percent_fee=0.001)

    thetadata_helper.reset_connection_diagnostics()
    WeeklyMomentumOptionsStrategy.backtest(
        datasource,
        backtesting_start=start,
        backtesting_end=end,
        budget=100000,
        benchmark_asset=Asset("SPY", Asset.AssetType.STOCK),
        quote_asset=Asset("USD", Asset.AssetType.FOREX),
        buy_trading_fees=[trading_fee],
        sell_trading_fees=[trading_fee],
        parameters={},
        show_plot=False,
        show_tearsheet=False,
        save_tearsheet=False,
        show_indicators=False,
        quiet_logs=False,
        show_progress_bar=False,
        save_stats_file=False,
        save_logfile=False,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a short Weekly Momentum span for fill debugging.")
    parser.add_argument("mode", choices=["pandas", "polars"], help="Backtesting engine to use.")
    parser.add_argument(
        "--start",
        default="2025-03-10",
        help="Inclusive start date (YYYY-MM-DD).",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of calendar days to simulate (default: 7).",
    )
    args = parser.parse_args()

    start_dt = datetime.fromisoformat(args.start)
    end_dt = start_dt + timedelta(days=args.days)

    print(
        f"\n{'=' * 60}\nRunning {args.mode} from {start_dt.date()} to {end_dt.date()}\n{'=' * 60}\n"
    )
    run_range(args.mode, start_dt, end_dt)


if __name__ == "__main__":
    main()
