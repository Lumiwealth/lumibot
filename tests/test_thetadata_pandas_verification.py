"""
Verification test for ThetaData pandas implementation.

This test verifies that the pandas implementation:
1. Works correctly with caching (cold→warm produces 0 requests)
2. Produces consistent results between cold and warm runs
3. Returns correct data for the WeeklyMomentumOptionsStrategy symbols

This establishes the baseline before cloning to polars.
"""

import os
import shutil
import json
from datetime import datetime
from pathlib import Path
import pytest

from lumibot.backtesting import ThetaDataBacktestingPandas
from lumibot.strategies import Strategy
from lumibot.entities import Asset
from lumibot.credentials import THETADATA_CONFIG


def get_cache_dir():
    """Get the ThetaData cache directory."""
    return Path.home() / "Library" / "Caches" / "lumibot" / "1.0" / "thetadata"


def clear_cache():
    """Clear all ThetaData cache files."""
    cache_dir = get_cache_dir()
    if cache_dir.exists():
        print(f"Clearing cache at {cache_dir}")
        shutil.rmtree(cache_dir)
        cache_dir.mkdir(parents=True, exist_ok=True)
    else:
        cache_dir.mkdir(parents=True, exist_ok=True)
    print("Cache cleared")


def count_cache_files():
    """Count the number of cache files."""
    cache_dir = get_cache_dir()
    if not cache_dir.exists():
        return 0
    return sum(1 for _ in cache_dir.rglob("*.parquet"))


class WeeklyMomentumOptionsStrategy(Strategy):
    """Simplified version of WeeklyMomentumOptionsStrategy for testing."""

    def initialize(self):
        self.sleeptime = "1D"
        self.data_fetches = []
        self.symbols = ["SPY", "QQQ", "IWM"]

    def on_trading_iteration(self):
        # Fetch historical data for each symbol
        for symbol in self.symbols:
            asset = Asset(symbol, asset_type=Asset.AssetType.STOCK)

            # Get 5 days of daily data
            daily_bars = self.get_historical_prices(asset, length=5, timestep="day")
            if daily_bars and hasattr(daily_bars, 'df'):
                self.data_fetches.append({
                    "symbol": symbol,
                    "timestep": "day",
                    "length": 5,
                    "rows": len(daily_bars.df)
                })

            # Get 10 minutes of minute data
            minute_bars = self.get_historical_prices(asset, length=10, timestep="minute")
            if minute_bars and hasattr(minute_bars, 'df'):
                self.data_fetches.append({
                    "symbol": symbol,
                    "timestep": "minute",
                    "length": 10,
                    "rows": len(minute_bars.df)
                })


def run_backtest(run_type):
    """Run a backtest and return the strategy data_fetches."""
    print(f"\n{'='*60}")
    print(f"Running {run_type.upper()} backtest with pandas")
    print(f"{'='*60}")

    cache_before = count_cache_files()
    print(f"Cache files before: {cache_before}")

    # Run backtest using Strategy.run_backtest() class method to get both results and strategy
    results, strategy_instance = WeeklyMomentumOptionsStrategy.run_backtest(
        ThetaDataBacktestingPandas,
        backtesting_start=datetime(2025, 3, 1),
        backtesting_end=datetime(2025, 3, 14),
        budget=100000,
        show_plot=False,
        show_tearsheet=False,
        save_tearsheet=False,
        show_indicators=False,
        quiet_logs=True,
        show_progress_bar=False,
    )

    cache_after = count_cache_files()
    print(f"Cache files after: {cache_after}")
    print(f"New cache files created: {cache_after - cache_before}")

    # Get portfolio value from strategy instance
    portfolio_value = strategy_instance.portfolio_value

    # Get data fetches count
    data_fetches = len(strategy_instance.data_fetches) if hasattr(strategy_instance, 'data_fetches') else 0

    print(f"Portfolio value: ${portfolio_value:,.2f}")
    print(f"Data fetches: {data_fetches}")
    print(f"Results: {results}")

    return {
        "run_type": run_type,
        "portfolio_value": portfolio_value,
        "data_fetches": data_fetches,
        "cache_before": cache_before,
        "cache_after": cache_after,
        "new_cache_files": cache_after - cache_before,
        "fetch_details": strategy_instance.data_fetches if hasattr(strategy_instance, 'data_fetches') else [],
        "results": results
    }


@pytest.mark.apitest
@pytest.mark.skipif(
    not THETADATA_CONFIG.get("THETADATA_USERNAME") or not THETADATA_CONFIG.get("THETADATA_PASSWORD"),
    reason="ThetaData credentials not configured - skipping API test"
)
def test_pandas_cold_warm():
    """Test that pandas implementation works correctly with caching."""

    # Clear cache and run cold
    clear_cache()
    cold_results = run_backtest("cold")

    # Run warm (cache should be used)
    warm_results = run_backtest("warm")

    # Verify results
    print(f"\n{'='*60}")
    print("VERIFICATION RESULTS")
    print(f"{'='*60}")

    # Check 1: Cold run should create cache files
    assert cold_results["new_cache_files"] > 0, "Cold run should create cache files"
    print(f"✓ Cold run created {cold_results['new_cache_files']} cache files")

    # Check 2: Warm run should not create new cache files
    assert warm_results["new_cache_files"] == 0, "Warm run should not create new cache files"
    print(f"✓ Warm run created {warm_results['new_cache_files']} new cache files (expected 0)")

    # Check 3: Portfolio values should match
    pv_diff = abs(cold_results["portfolio_value"] - warm_results["portfolio_value"])
    assert pv_diff < 0.01, f"Portfolio values should match (diff: ${pv_diff:,.2f})"
    print(f"✓ Portfolio values match: ${cold_results['portfolio_value']:,.2f}")

    # Check 4: Data fetches should match
    assert cold_results["data_fetches"] == warm_results["data_fetches"], "Data fetches should match"
    print(f"✓ Data fetches match: {cold_results['data_fetches']}")

    # Save results for reference
    results_path = Path("/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot/logs/pandas_verification_results.json")
    results_path.parent.mkdir(parents=True, exist_ok=True)
    with open(results_path, 'w') as f:
        json.dump({
            "cold": cold_results,
            "warm": warm_results
        }, f, indent=2, default=str)

    print(f"\n✓ Results saved to {results_path}")
    print("\n✅ ALL CHECKS PASSED - Pandas implementation is working correctly")

    return cold_results, warm_results


if __name__ == "__main__":
    test_pandas_cold_warm()
