"""Profile DataBento pandas vs Polars backtesting performance."""

from __future__ import annotations

import argparse
import os
import shutil
import time
from datetime import datetime
from pathlib import Path

import yappi
import pytz

# Force verbose logging
os.environ['BACKTESTING_QUIET_LOGS'] = 'false'

from lumibot.backtesting import DataBentoDataBacktestingPandas, DataBentoDataBacktestingPolars
from lumibot.entities import Asset
from lumibot.credentials import DATABENTO_CONFIG
from lumibot.tools import databento_helper

OUTPUT_DIR = Path("tests/performance/logs")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

DATABENTO_API_KEY = DATABENTO_CONFIG.get("API_KEY")

# Use the same test parameters as the parity test
BACKTEST_START = pytz.timezone("America/New_York").localize(datetime(2025, 9, 15, 0, 0))
BACKTEST_END = pytz.timezone("America/New_York").localize(datetime(2025, 9, 29, 23, 59))


def run_profile(mode: str, clear_cache: bool = False) -> float:
    """Profile a single mode (pandas or polars)"""
    datasource_cls = DataBentoDataBacktestingPolars if mode == "polars" else DataBentoDataBacktestingPandas
    label = f"databento_parity_{mode}"
    profile_path = OUTPUT_DIR / f"{label}.prof"

    # Optionally clear cache (use False for warm cache profiling)
    if clear_cache:
        cache_path = Path(databento_helper.LUMIBOT_DATABENTO_CACHE_FOLDER)
        if cache_path.exists():
            print(f"Clearing cache: {cache_path}")
            shutil.rmtree(cache_path)
            cache_path.mkdir(parents=True, exist_ok=True)

    yappi.clear_stats()
    yappi.set_clock_type("wall")  # Wall clock for real-world timing
    yappi.start()
    start = time.time()

    # Create FRESH asset for this run (avoid state pollution)
    asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)

    # Create data source and fetch data
    ds = datasource_cls(
        datetime_start=BACKTEST_START,
        datetime_end=BACKTEST_END,
        api_key=DATABENTO_API_KEY,
        show_progress_bar=False,
    )

    # Fetch historical prices (simulates backtest load)
    bars = ds.get_historical_prices(asset, 500, timestep="minute")

    # Simulate multiple get_last_price calls (common in backtesting)
    tz = pytz.timezone("America/New_York")
    test_datetimes = [
        tz.localize(datetime(2025, 9, 15, 0, 0)),
        tz.localize(datetime(2025, 9, 15, 3, 40)),
        tz.localize(datetime(2025, 9, 15, 4, 0)),
        tz.localize(datetime(2025, 9, 15, 7, 35)),
        tz.localize(datetime(2025, 9, 15, 11, 5)),
        tz.localize(datetime(2025, 9, 15, 14, 5)),
    ]

    for dt in test_datetimes:
        ds._datetime = dt
        price = ds.get_last_price(asset)

    elapsed = time.time() - start
    yappi.stop()

    # Save profile
    yappi.get_func_stats().save(str(profile_path), type="pstat")

    # Print results
    print(f"\n{'='*60}")
    print(f"MODE: {mode.upper()}")
    print(f"{'='*60}")
    print(f"Elapsed time: {elapsed:.2f}s")
    print(f"Bars fetched: {len(bars.df) if bars else 0}")
    print(f"Profile saved: {profile_path}")
    print(f"{'='*60}\n")

    return elapsed


def main() -> None:
    parser = argparse.ArgumentParser(description="Profile DataBento pandas vs polars backends")
    parser.add_argument("--mode", choices=["pandas", "polars", "both"], default="both")
    parser.add_argument("--cache", choices=["cold", "warm"], default="warm",
                        help="cold=clear cache each run, warm=use cached data (default: warm)")
    args = parser.parse_args()

    if not DATABENTO_API_KEY or DATABENTO_API_KEY == '<your key here>':
        print("ERROR: DATABENTO_API_KEY not configured")
        return

    clear_cache = (args.cache == "cold")

    # If warm cache, do a warm-up run first to populate cache
    if not clear_cache:
        print("Warming up cache with initial fetch...")
        from lumibot.backtesting import DataBentoDataBacktestingPandas
        asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)
        ds = DataBentoDataBacktestingPandas(
            datetime_start=BACKTEST_START,
            datetime_end=BACKTEST_END,
            api_key=DATABENTO_API_KEY,
            show_progress_bar=False,
        )
        ds.get_historical_prices(asset, 500, timestep="minute")
        print("Cache warmed up.\n")

    modes = [args.mode] if args.mode != "both" else ["pandas", "polars"]
    results = {}

    for m in modes:
        elapsed = run_profile(m, clear_cache=clear_cache)
        results[m] = elapsed

    # Print comparison
    if len(results) > 1:
        print(f"\n{'='*60}")
        print("COMPARISON")
        print(f"{'='*60}")
        pandas_time = results.get("pandas", 0)
        polars_time = results.get("polars", 0)
        if pandas_time > 0 and polars_time > 0:
            speedup = pandas_time / polars_time
            print(f"Pandas: {pandas_time:.2f}s")
            print(f"Polars: {polars_time:.2f}s")
            print(f"Speedup: {speedup:.2f}x")
        print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
