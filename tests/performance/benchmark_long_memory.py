"""Long backtest memory and stability benchmark (1 year).

This script tests:
1. Memory usage stays bounded during long backtests
2. Cache hit rates are high (should avoid re-aggregation)
3. Performance is stable over extended periods
4. No memory leaks or runaway growth
"""

from __future__ import annotations

import argparse
import time
import sys
import psutil
import gc
from datetime import datetime
from pathlib import Path

import pytz
import pandas as pd

from lumibot.backtesting import BacktestingBroker, DataBentoDataBacktestingPandas, DataBentoDataBacktestingPolars
from lumibot.entities import Asset, Order, TradingFee
from lumibot.strategies import Strategy
from lumibot.traders import Trader
from lumibot.credentials import DATABENTO_CONFIG

OUTPUT_DIR = Path("tests/performance/logs")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

DATABENTO_API_KEY = DATABENTO_CONFIG.get("API_KEY")


class SimpleMomentumStrategy(Strategy):
    """Simple momentum strategy for long-duration testing."""

    parameters = {
        "sma_fast": 20,
        "sma_slow": 50,
        "bars_lookback": 100,
        "timestep": "minute",
    }

    def initialize(self):
        self.set_market("us_futures")
        self.sleeptime = "1M"
        self.vars.asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)

    def on_trading_iteration(self):
        params = self.get_parameters()
        asset = self.vars.asset

        # Get historical bars
        bars = self.get_historical_prices(asset, params["bars_lookback"], params["timestep"])
        if bars is None or bars.df is None:
            return

        df = bars.pandas_df
        if df is None or df.empty:
            return

        # Compute simple moving averages
        df["sma_fast"] = df["close"].rolling(window=params["sma_fast"]).mean()
        df["sma_slow"] = df["close"].rolling(window=params["sma_slow"]).mean()

        if pd.isna(df["sma_fast"].iloc[-1]) or pd.isna(df["sma_slow"].iloc[-1]):
            return

        # Simple crossover strategy
        fast_above_slow = df["sma_fast"].iloc[-1] > df["sma_slow"].iloc[-1]
        pos = self.get_position(asset)
        has_pos = pos is not None and abs(pos.quantity) > 0

        if fast_above_slow and not has_pos:
            # Go long
            order = self.create_order(asset, 1, Order.OrderSide.BUY)
            self.submit_order(order)
        elif not fast_above_slow and has_pos:
            # Close position
            self.sell_all()


class MemoryTracker:
    """Track memory usage throughout backtest."""

    def __init__(self):
        self.process = psutil.Process()
        self.samples = []
        self.start_time = time.time()

    def sample(self, label=""):
        """Record current memory usage."""
        mem_info = self.process.memory_info()
        rss_mb = mem_info.rss / 1024 / 1024  # Convert to MB
        elapsed = time.time() - self.start_time

        self.samples.append({
            "time": elapsed,
            "rss_mb": rss_mb,
            "label": label
        })

        return rss_mb

    def report(self):
        """Generate memory usage report."""
        if not self.samples:
            return

        print(f"\n{'='*60}")
        print("MEMORY USAGE REPORT")
        print(f"{'='*60}")

        # Statistics
        rss_values = [s["rss_mb"] for s in self.samples]
        min_rss = min(rss_values)
        max_rss = max(rss_values)
        avg_rss = sum(rss_values) / len(rss_values)

        print(f"Samples: {len(self.samples)}")
        print(f"Min RSS: {min_rss:.2f} MB")
        print(f"Max RSS: {max_rss:.2f} MB")
        print(f"Avg RSS: {avg_rss:.2f} MB")
        print(f"Growth: {max_rss - min_rss:.2f} MB")

        # Check for concerning growth
        if len(rss_values) >= 2:
            start_rss = rss_values[0]
            end_rss = rss_values[-1]
            growth_pct = ((end_rss - start_rss) / start_rss) * 100
            print(f"Growth %: {growth_pct:.2f}%")

            if growth_pct > 50:
                print("⚠️  WARNING: Memory grew by more than 50%")
            elif growth_pct > 100:
                print("🚨 CRITICAL: Memory grew by more than 100% - possible leak!")
            else:
                print("✅ Memory growth is acceptable")

        print(f"{'='*60}\n")

        # Save detailed report
        report_path = OUTPUT_DIR / "memory_usage_long_backtest.csv"
        df = pd.DataFrame(self.samples)
        df.to_csv(report_path, index=False)
        print(f"Detailed memory log saved to: {report_path}")


def run_long_backtest(mode: str, duration_days: int = 365) -> dict:
    """Run long backtest with memory tracking."""
    datasource_cls = DataBentoDataBacktestingPolars if mode == "polars" else DataBentoDataBacktestingPandas
    label = f"long_backtest_{mode}_{duration_days}d"

    # 1 year backtest: Jan 1, 2024 - Dec 31, 2024
    tzinfo = pytz.timezone("America/New_York")
    start = tzinfo.localize(datetime(2024, 1, 1, 9, 30))

    # Calculate end date based on duration
    from datetime import timedelta
    end = start + timedelta(days=duration_days)

    print(f"\n{'='*60}")
    print(f"Starting LONG {mode.upper()} backtest...")
    print(f"Period: {start} to {end}")
    print(f"Duration: {duration_days} days")
    print(f"{'='*60}")

    # Initialize memory tracker
    tracker = MemoryTracker()
    tracker.sample("start")

    # Force garbage collection before starting
    gc.collect()

    wall_start = time.time()

    # Run backtest
    data_source = datasource_cls(
        datetime_start=start,
        datetime_end=end,
        api_key=DATABENTO_API_KEY,
        show_progress_bar=True,  # Show progress for long backtest
    )

    tracker.sample("after_datasource_init")

    broker = BacktestingBroker(data_source=data_source)
    fee = TradingFee(flat_fee=0.50)

    strat = SimpleMomentumStrategy(
        broker=broker,
        buy_trading_fees=[fee],
        sell_trading_fees=[fee],
    )

    tracker.sample("before_backtest_start")

    trader = Trader(logfile="", backtest=True)
    trader.add_strategy(strat)

    # Sample memory periodically during backtest
    # We'll do this by monkey-patching the strategy's on_trading_iteration
    original_on_trading_iteration = strat.on_trading_iteration
    iteration_count = [0]  # Use list to allow modification in closure

    def tracked_on_trading_iteration():
        iteration_count[0] += 1
        # Sample memory every 1000 iterations
        if iteration_count[0] % 1000 == 0:
            tracker.sample(f"iteration_{iteration_count[0]}")
        return original_on_trading_iteration()

    strat.on_trading_iteration = tracked_on_trading_iteration

    results = trader.run_all(
        show_plot=False,
        show_tearsheet=False,
        show_indicators=False,
        save_tearsheet=False,
    )

    elapsed = time.time() - wall_start
    tracker.sample("after_backtest_complete")

    # Print results
    print(f"\n{'='*60}")
    print(f"MODE: {mode.upper()}")
    print(f"{'='*60}")
    print(f"Elapsed time: {elapsed:.2f}s ({elapsed/60:.2f} minutes)")
    print(f"Total iterations: {iteration_count[0]}")

    if iteration_count[0] > 0:
        time_per_iteration = elapsed / iteration_count[0]
        print(f"Time per iteration: {time_per_iteration*1000:.2f}ms")

    # Report memory usage
    tracker.report()

    # Check for cache statistics if available
    if hasattr(data_source, '_aggregated_cache'):
        cache_size = len(data_source._aggregated_cache)
        print(f"\nAggregated cache size: {cache_size}")

    if hasattr(data_source, '_data_store'):
        data_store_size = len(data_source._data_store)
        print(f"Data store size: {data_store_size}")

    print(f"{'='*60}\n")

    return {
        "mode": mode,
        "elapsed": elapsed,
        "iterations": iteration_count[0],
        "memory_samples": tracker.samples,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Long backtest memory benchmark")
    parser.add_argument("--mode", choices=["pandas", "polars", "both"], default="polars")
    parser.add_argument("--days", type=int, default=365, help="Backtest duration in days")
    args = parser.parse_args()

    if not DATABENTO_API_KEY or DATABENTO_API_KEY == '<your key here>':
        print("ERROR: DATABENTO_API_KEY not configured")
        sys.exit(1)

    modes = [args.mode] if args.mode != "both" else ["pandas", "polars"]
    results = []

    for m in modes:
        result = run_long_backtest(m, duration_days=args.days)
        results.append(result)

    # Print comparison
    if len(results) > 1:
        print(f"\n{'='*60}")
        print("COMPARISON")
        print(f"{'='*60}")

        pandas_result = next((r for r in results if r["mode"] == "pandas"), None)
        polars_result = next((r for r in results if r["mode"] == "polars"), None)

        if pandas_result and polars_result:
            pandas_time = pandas_result["elapsed"]
            polars_time = polars_result["elapsed"]
            speedup = pandas_time / polars_time

            print(f"Pandas: {pandas_time:.2f}s ({pandas_time/60:.2f} minutes)")
            print(f"Polars: {polars_time:.2f}s ({polars_time/60:.2f} minutes)")
            print(f"Speedup: {speedup:.2f}x")

            # Memory comparison
            pandas_mem = pandas_result["memory_samples"][-1]["rss_mb"]
            polars_mem = polars_result["memory_samples"][-1]["rss_mb"]
            print(f"\nFinal memory:")
            print(f"Pandas: {pandas_mem:.2f} MB")
            print(f"Polars: {polars_mem:.2f} MB")
            print(f"Memory ratio: {pandas_mem/polars_mem:.2f}x")

        print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
