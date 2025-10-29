"""
Performance profiling comparison between ThetaData and Polygon.

This script uses YAPPI (thread-safe profiler) to identify bottlenecks in both data sources.

Usage:
    python profile_thetadata_vs_polygon.py

Requirements:
    pip install yappi snakeviz

To visualize results:
    snakeviz thetadata_nocache.prof
    snakeviz thetadata_cached.prof
    snakeviz polygon_nocache.prof
    snakeviz polygon_cached.prof
"""

import datetime
import os
import shutil
from pathlib import Path
import yappi
from dotenv import load_dotenv
from lumibot.strategies import Strategy
from lumibot.backtesting import ThetaDataBacktesting, PolygonDataBacktesting
from lumibot.entities import Asset

# Load environment variables from .env file
load_dotenv()


class SimpleBacktestStrategy(Strategy):
    """Simple buy-and-hold strategy for profiling"""

    parameters = {
        "symbol": "AMZN",
        "quantity": 10
    }

    def initialize(self):
        self.sleeptime = "1D"

    def on_trading_iteration(self):
        if self.first_iteration:
            asset = Asset(self.parameters["symbol"])
            order = self.create_order(asset, self.parameters["quantity"], "buy")
            self.submit_order(order)


def get_cache_dir():
    """Get the lumibot cache directory"""
    cache_dir = Path.home() / ".lumibot"
    return cache_dir


def clear_cache():
    """Clear all cached data"""
    cache_dir = get_cache_dir()
    if cache_dir.exists():
        print(f"Clearing cache at {cache_dir}")
        shutil.rmtree(cache_dir)
        print("Cache cleared")
    else:
        print("No cache to clear")


def profile_backtest(data_source_class, name, profile_file, clear_cache_first=True):
    """
    Profile a backtest run.

    Args:
        data_source_class: ThetaDataBacktesting or PolygonDataBacktesting
        name: Name for logging
        profile_file: Output file for profiling results
        clear_cache_first: Whether to clear cache before running
    """
    if clear_cache_first:
        clear_cache()

    print(f"\n{'='*80}")
    print(f"PROFILING: {name}")
    print(f"Cache: {'CLEARED' if clear_cache_first else 'WARMED'}")
    print(f"{'='*80}\n")

    # Configure data source
    start = datetime.datetime(2024, 8, 1)
    end = datetime.datetime(2024, 8, 2)

    # Get credentials
    thetadata_username = os.environ.get("THETADATA_USERNAME")
    thetadata_password = os.environ.get("THETADATA_PASSWORD")
    polygon_api_key = os.environ.get("POLYGON_API_KEY")

    # Start profiling
    yappi.clear_stats()
    yappi.set_clock_type("wall")  # Use wall clock time
    yappi.start()

    # Run backtest
    start_time = datetime.datetime.now()

    try:
        results, strategy = SimpleBacktestStrategy.run_backtest(
            data_source_class,
            start,
            end,
            show_plot=False,
            show_tearsheet=False,
            save_tearsheet=False,
            parameters={"symbol": "AMZN", "quantity": 10},
            thetadata_username=thetadata_username,
            thetadata_password=thetadata_password,
            polygon_api_key=polygon_api_key,
        )

        end_time = datetime.datetime.now()
        elapsed = (end_time - start_time).total_seconds()

        print(f"✓ Backtest completed in {elapsed:.2f} seconds")
        print(f"  Orders: {len(strategy.orders)}")
        print(f"  Final portfolio value: ${strategy.get_portfolio_value():,.2f}")

    except Exception as e:
        print(f"✗ Backtest failed: {e}")
        raise

    finally:
        # Stop profiling
        yappi.stop()

        # Save profiling results
        func_stats = yappi.get_func_stats()

        # Save to pstat format for snakeviz
        func_stats.save(profile_file, type="pstat")
        print(f"  Profile saved to: {profile_file}")

        # Print top 30 time-consuming functions
        print(f"\nTop 30 time-consuming functions:")
        print("="*120)
        func_stats.sort("totaltime", "desc")
        # Print first 30 functions
        for i, stat in enumerate(func_stats[:30]):
            if i == 0:
                print(f"{'Function':<60} {'Calls':<10} {'TotTime':<12} {'PerCall':<12}")
                print("-"*120)
            print(f"{stat.name:<60} {stat.ncall:<10} {stat.ttot:<12.6f} {stat.tavg:<12.6f}")

        return elapsed


def main():
    """Run profiling comparison"""

    # Check if credentials are available
    thetadata_username = os.environ.get("THETADATA_USERNAME")
    thetadata_password = os.environ.get("THETADATA_PASSWORD")
    polygon_api_key = os.environ.get("POLYGON_API_KEY")

    if not thetadata_username or not thetadata_password:
        print("ERROR: ThetaData credentials not found")
        print("Set THETADATA_USERNAME and THETADATA_PASSWORD environment variables")
        return

    if not polygon_api_key:
        print("ERROR: Polygon API key not found")
        print("Set POLYGON_API_KEY environment variable")
        return

    print("\n" + "="*80)
    print("PERFORMANCE PROFILING: ThetaData vs Polygon")
    print("="*80)
    print(f"Date range: 2024-08-01 to 2024-08-02 (1 trading day)")
    print(f"Strategy: Buy & hold 10 shares of AMZN")
    print("="*80)

    results = {}

    # 1. ThetaData with cache cleared
    results["thetadata_nocache"] = profile_backtest(
        ThetaDataBacktesting,
        "ThetaData (NO CACHE)",
        "thetadata_nocache.prof",
        clear_cache_first=True
    )

    # 2. ThetaData with cache warmed
    results["thetadata_cached"] = profile_backtest(
        ThetaDataBacktesting,
        "ThetaData (CACHED)",
        "thetadata_cached.prof",
        clear_cache_first=False
    )

    # 3. Polygon with cache cleared
    results["polygon_nocache"] = profile_backtest(
        PolygonDataBacktesting,
        "Polygon (NO CACHE)",
        "polygon_nocache.prof",
        clear_cache_first=True
    )

    # 4. Polygon with cache warmed
    results["polygon_cached"] = profile_backtest(
        PolygonDataBacktesting,
        "Polygon (CACHED)",
        "polygon_cached.prof",
        clear_cache_first=False
    )

    # Summary
    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"{'Test':<30} {'Time (s)':<15} {'Speedup vs ThetaData'}")
    print("-"*80)

    baseline = results["thetadata_nocache"]
    for key, elapsed in results.items():
        speedup = baseline / elapsed if elapsed > 0 else 0
        speedup_str = f"{speedup:.1f}x" if speedup != 1.0 else "-"
        print(f"{key:<30} {elapsed:>10.2f}      {speedup_str:>10}")

    print("\n" + "="*80)
    print("ANALYSIS")
    print("="*80)

    theta_cache_benefit = results["thetadata_nocache"] / results["thetadata_cached"] if results["thetadata_cached"] > 0 else 0
    polygon_cache_benefit = results["polygon_nocache"] / results["polygon_cached"] if results["polygon_cached"] > 0 else 0

    print(f"ThetaData cache benefit: {theta_cache_benefit:.1f}x faster with cache")
    print(f"Polygon cache benefit: {polygon_cache_benefit:.1f}x faster with cache")

    # Compare cached performance (most relevant for production)
    if results["thetadata_cached"] > results["polygon_cached"]:
        slowdown = results["thetadata_cached"] / results["polygon_cached"]
        print(f"\n⚠️  ThetaData (cached) is {slowdown:.1f}x SLOWER than Polygon (cached)")
    else:
        speedup = results["polygon_cached"] / results["thetadata_cached"]
        print(f"\n✓ ThetaData (cached) is {speedup:.1f}x FASTER than Polygon (cached)")

    print("\n" + "="*80)
    print("PROFILING FILES GENERATED")
    print("="*80)
    print("To visualize bottlenecks, run:")
    print("  snakeviz thetadata_nocache.prof")
    print("  snakeviz thetadata_cached.prof")
    print("  snakeviz polygon_nocache.prof")
    print("  snakeviz polygon_cached.prof")
    print("="*80 + "\n")


if __name__ == "__main__":
    main()
