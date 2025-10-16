"""Profile Weekly Momentum Options strategy under pandas vs Polars ThetaData backtesting."""

from __future__ import annotations

################################################################################
# Must Be Imported First If Run Locally
if True:
    import os
    import sys
    from pathlib import Path

    # Add the lumibot root directory to sys.path
    # This finds the lumibot directory by going up from this file's location
    current_file = Path(__file__).resolve()
    lumibot_root = current_file.parent.parent.parent
    if str(lumibot_root) not in sys.path:
        sys.path.insert(0, str(lumibot_root))
################################################################################

import argparse
import time
from datetime import datetime
from pathlib import Path

import yappi

# Force verbose logging for broker fill debugging
os.environ['BACKTESTING_QUIET_LOGS'] = 'false'

from lumibot.entities import Asset, TradingFee
from lumibot.backtesting import ThetaDataBacktesting, ThetaDataBacktestingPandas
from lumibot.tools import thetadata_helper

from tests.performance.strategies.weekly_momentum_options import WeeklyMomentumOptionsStrategy

OUTPUT_DIR = Path("tests/performance/logs")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Full 6-month backtest to see real bottlenecks
BACKTEST_START = datetime(2024, 7, 1)
BACKTEST_END = datetime(2024, 12, 31)  # 6 months for proper profiling
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

    thetadata_helper.reset_connection_diagnostics()
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
        quiet_logs=False,  # Leave logs enabled; profiling must include log overhead
        show_progress_bar=False,
        save_stats_file=True,
        save_logfile=True,
    )

    elapsed = time.time() - start
    yappi.stop()

    yappi.get_func_stats().save(str(profile_path), type="pstat")
    diagnostics = thetadata_helper.CONNECTION_DIAGNOSTICS.copy()
    print(
        f"[theta diagnostics] mode={mode} "
        f"network_requests={diagnostics['network_requests']} "
        f"check_connection_calls={diagnostics['check_connection_calls']} "
        f"start_terminal_calls={diagnostics['start_terminal_calls']}"
    )
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
