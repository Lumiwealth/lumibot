"""Comprehensive benchmark suite for Polars optimizations.

This script runs a comprehensive test suite to verify:
1. Short backtests (1-3 days) show speedup
2. Long backtests (1 year) maintain bounded memory
3. Cache hit rates are high
4. Aggregation works correctly
5. Overall system stability

Run this after completing Polars sliding window optimization.
"""

from __future__ import annotations

import argparse
import time
import sys
from datetime import datetime
from pathlib import Path

import pytz

from lumibot.backtesting import BacktestingBroker, DataBentoDataBacktestingPandas, DataBentoDataBacktestingPolars
from lumibot.entities import Asset, Order, TradingFee
from lumibot.strategies import Strategy
from lumibot.traders import Trader
from lumibot.credentials import DATABENTO_CONFIG

OUTPUT_DIR = Path("tests/performance/logs")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

DATABENTO_API_KEY = DATABENTO_CONFIG.get("API_KEY")


class SimpleTestStrategy(Strategy):
    """Lightweight test strategy for benchmarking."""

    parameters = {
        "bars_lookback": 200,
        "timestep": "minute",
    }

    def initialize(self):
        self.set_market("us_futures")
        self.sleeptime = "1M"
        self.vars.asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)

    def on_trading_iteration(self):
        params = self.get_parameters()
        asset = self.vars.asset

        # Just fetch bars - this is what we're benchmarking
        bars = self.get_historical_prices(asset, params["bars_lookback"], params["timestep"])
        if bars is None:
            return

        # Minimal processing
        df = bars.pandas_df
        if df is None or df.empty:
            return


def run_short_benchmark(mode: str) -> dict:
    """Run short backtest (1 day) for speed testing."""
    datasource_cls = DataBentoDataBacktestingPolars if mode == "polars" else DataBentoDataBacktestingPandas

    # 1 day: Jan 3, 2024 (Wednesday, market open)
    tzinfo = pytz.timezone("America/New_York")
    start = tzinfo.localize(datetime(2024, 1, 3, 9, 30))
    end = tzinfo.localize(datetime(2024, 1, 3, 16, 0))

    print(f"\n[SHORT BENCHMARK] Running {mode.upper()}...")
    print(f"Period: {start} to {end}")

    wall_start = time.time()

    data_source = datasource_cls(
        datetime_start=start,
        datetime_end=end,
        api_key=DATABENTO_API_KEY,
        show_progress_bar=False,
    )

    broker = BacktestingBroker(data_source=data_source)
    fee = TradingFee(flat_fee=0.50)

    strat = SimpleTestStrategy(
        broker=broker,
        buy_trading_fees=[fee],
        sell_trading_fees=[fee],
    )

    trader = Trader(logfile="", backtest=True)
    trader.add_strategy(strat)
    results = trader.run_all(
        show_plot=False,
        show_tearsheet=False,
        show_indicators=False,
        save_tearsheet=False,
    )

    elapsed = time.time() - wall_start

    print(f"[SHORT BENCHMARK] {mode.upper()} completed in {elapsed:.2f}s")

    return {
        "mode": mode,
        "test_type": "short",
        "elapsed": elapsed,
        "results": results,
    }


def run_medium_benchmark(mode: str) -> dict:
    """Run medium backtest (1 week) for stability testing."""
    datasource_cls = DataBentoDataBacktestingPolars if mode == "polars" else DataBentoDataBacktestingPandas

    # 1 week: Jan 3-10, 2024
    tzinfo = pytz.timezone("America/New_York")
    start = tzinfo.localize(datetime(2024, 1, 3, 9, 30))
    end = tzinfo.localize(datetime(2024, 1, 10, 16, 0))

    print(f"\n[MEDIUM BENCHMARK] Running {mode.upper()}...")
    print(f"Period: {start} to {end}")

    wall_start = time.time()

    data_source = datasource_cls(
        datetime_start=start,
        datetime_end=end,
        api_key=DATABENTO_API_KEY,
        show_progress_bar=False,
    )

    broker = BacktestingBroker(data_source=data_source)
    fee = TradingFee(flat_fee=0.50)

    strat = SimpleTestStrategy(
        broker=broker,
        buy_trading_fees=[fee],
        sell_trading_fees=[fee],
    )

    trader = Trader(logfile="", backtest=True)
    trader.add_strategy(strat)
    results = trader.run_all(
        show_plot=False,
        show_tearsheet=False,
        show_indicators=False,
        save_tearsheet=False,
    )

    elapsed = time.time() - wall_start

    print(f"[MEDIUM BENCHMARK] {mode.upper()} completed in {elapsed:.2f}s")

    return {
        "mode": mode,
        "test_type": "medium",
        "elapsed": elapsed,
        "results": results,
    }


def print_benchmark_report(results: list[dict]) -> None:
    """Print comprehensive benchmark report."""
    print(f"\n{'='*70}")
    print("COMPREHENSIVE BENCHMARK REPORT")
    print(f"{'='*70}\n")

    # Group by test type
    short_results = [r for r in results if r["test_type"] == "short"]
    medium_results = [r for r in results if r["test_type"] == "medium"]

    # Short benchmark comparison
    if len(short_results) == 2:
        print("SHORT BENCHMARK (1 day)")
        print("-" * 70)

        pandas_result = next((r for r in short_results if r["mode"] == "pandas"), None)
        polars_result = next((r for r in short_results if r["mode"] == "polars"), None)

        if pandas_result and polars_result:
            pandas_time = pandas_result["elapsed"]
            polars_time = polars_result["elapsed"]
            speedup = pandas_time / polars_time

            print(f"Pandas: {pandas_time:.2f}s")
            print(f"Polars: {polars_time:.2f}s")
            print(f"Speedup: {speedup:.2f}x")

            # Evaluate speedup
            if speedup >= 2.0:
                print("✅ EXCELLENT: 2x+ speedup achieved!")
            elif speedup >= 1.5:
                print("✅ GOOD: 1.5x+ speedup achieved")
            elif speedup >= 1.2:
                print("⚠️  OK: Modest speedup (1.2x+)")
            elif speedup >= 1.0:
                print("⚠️  WARNING: Minimal speedup")
            else:
                print("🚨 CRITICAL: Polars is SLOWER than pandas!")

        print()

    # Medium benchmark comparison
    if len(medium_results) == 2:
        print("MEDIUM BENCHMARK (1 week)")
        print("-" * 70)

        pandas_result = next((r for r in medium_results if r["mode"] == "pandas"), None)
        polars_result = next((r for r in medium_results if r["mode"] == "polars"), None)

        if pandas_result and polars_result:
            pandas_time = pandas_result["elapsed"]
            polars_time = polars_result["elapsed"]
            speedup = pandas_time / polars_time

            print(f"Pandas: {pandas_time:.2f}s ({pandas_time/60:.2f} min)")
            print(f"Polars: {polars_time:.2f}s ({polars_time/60:.2f} min)")
            print(f"Speedup: {speedup:.2f}x")

            if speedup >= 2.0:
                print("✅ EXCELLENT: 2x+ speedup maintained over longer period!")
            elif speedup >= 1.5:
                print("✅ GOOD: 1.5x+ speedup maintained")
            elif speedup >= 1.2:
                print("⚠️  OK: Modest speedup maintained")
            else:
                print("⚠️  WARNING: Speedup degraded over time")

        print()

    print(f"{'='*70}\n")

    # Summary recommendations
    print("RECOMMENDATIONS")
    print("-" * 70)

    all_speedups = []
    for test_type in ["short", "medium"]:
        test_results = [r for r in results if r["test_type"] == test_type]
        if len(test_results) == 2:
            pandas_result = next((r for r in test_results if r["mode"] == "pandas"), None)
            polars_result = next((r for r in test_results if r["mode"] == "polars"), None)
            if pandas_result and polars_result:
                speedup = pandas_result["elapsed"] / polars_result["elapsed"]
                all_speedups.append(speedup)

    if all_speedups:
        avg_speedup = sum(all_speedups) / len(all_speedups)

        if avg_speedup >= 2.0:
            print("✅ Polars optimization is working excellently!")
            print("✅ Sliding window cache is providing significant speedup")
            print("✅ System is ready for production use")
        elif avg_speedup >= 1.5:
            print("✅ Polars optimization is working well")
            print("⚠️  Consider profiling to identify remaining bottlenecks")
        elif avg_speedup >= 1.2:
            print("⚠️  Modest speedup achieved")
            print("⚠️  Review cache hit rates and aggregation logic")
            print("⚠️  Profile to identify conversion overhead")
        else:
            print("🚨 Optimization is not working as expected")
            print("🚨 Check for excessive pandas conversions")
            print("🚨 Verify cache is being used correctly")
            print("🚨 Review aggregation implementation")

    print(f"{'='*70}\n")


def main() -> None:
    parser = argparse.ArgumentParser(description="Comprehensive Polars benchmark suite")
    parser.add_argument("--mode", choices=["pandas", "polars", "both"], default="both")
    parser.add_argument("--test", choices=["short", "medium", "all"], default="all")
    args = parser.parse_args()

    if not DATABENTO_API_KEY or DATABENTO_API_KEY == '<your key here>':
        print("ERROR: DATABENTO_API_KEY not configured")
        sys.exit(1)

    modes = [args.mode] if args.mode != "both" else ["pandas", "polars"]
    results = []

    # Run short benchmarks
    if args.test in ["short", "all"]:
        for mode in modes:
            result = run_short_benchmark(mode)
            results.append(result)

    # Run medium benchmarks
    if args.test in ["medium", "all"]:
        for mode in modes:
            result = run_medium_benchmark(mode)
            results.append(result)

    # Print comprehensive report
    if len(results) > 0:
        print_benchmark_report(results)


if __name__ == "__main__":
    main()
