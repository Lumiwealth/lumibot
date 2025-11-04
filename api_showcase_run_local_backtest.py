#!/usr/bin/env python3
"""
BotSpot API Client - Run Saved Strategy Locally

Demonstrates:
- Loading a saved strategy file
- Running a local backtest
- Validating execution

Prerequisites:
- Run api_showcase_save_and_run.py first to save a strategy
- Install Lumibot: pip install lumibot

SECURITY WARNING:
This script dynamically loads and executes Python code from local files.
Only run strategy files from trusted sources that you have created or reviewed.
Never load strategy files from untrusted or external sources without thorough review.

Usage:
    python api_showcase_run_local_backtest.py [strategy_filename]
"""

import importlib.util
import sys
from datetime import datetime, timedelta
from pathlib import Path


def load_strategy_from_file(filepath: str):
    """
    Dynamically load a strategy class from a Python file.

    SECURITY: This function executes arbitrary Python code. Only use with
    trusted files from known sources.

    Args:
        filepath: Path to the strategy Python file

    Returns:
        The Strategy class from the file

    Raises:
        ValueError: If filepath is outside the expected strategies directory
        ImportError: If module cannot be loaded
    """
    # Validate filepath is within strategies directory (security check)
    filepath_obj = Path(filepath).resolve()
    expected_dir = Path("strategies").resolve()

    # Ensure the file is within the strategies directory
    try:
        filepath_obj.relative_to(expected_dir)
    except ValueError as e:
        raise ValueError(
            f"Security: Strategy file must be within strategies/ directory.\n"
            f"Attempted path: {filepath_obj}\n"
            f"Expected parent: {expected_dir}"
        ) from e

    # Check for symlink attacks
    if filepath_obj.is_symlink():
        raise ValueError(
            f"Security: Strategy file cannot be a symlink.\n"
            f"File: {filepath_obj}\n"
            f"Symlinks could point to malicious code outside the strategies directory."
        )

    # Warn user about code execution
    print(f"  ⚠️  Loading and executing code from: {filepath_obj.name}")
    print("     (Only run files you trust)")

    # Load module from file
    spec = importlib.util.spec_from_file_location("strategy_module", str(filepath_obj))
    if spec is None or spec.loader is None:
        raise ImportError(f"Could not load strategy from {filepath_obj}")

    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    # Find Strategy class
    # Note: Excludes base "Strategy" class from Lumibot imports, looking for user-defined subclass
    strategy_class = None
    for name in dir(module):
        obj = getattr(module, name)
        if isinstance(obj, type) and name != "Strategy" and hasattr(obj, "on_trading_iteration"):
            strategy_class = obj
            break

    if strategy_class is None:
        raise ValueError(f"No Strategy class found in {filepath}")

    return strategy_class


def run_backtest(strategy_file: Path):
    """
    Run a backtest for the saved strategy.

    SECURITY NOTE: This function will execute Python code from the strategy file.
    Only run files you have created or thoroughly reviewed.

    Args:
        strategy_file: Path to strategy Python file
    """
    print("\n" + "=" * 70)
    print("  🚀 Running Local Backtest")
    print("=" * 70)

    print("\n" + "⚠️  SECURITY WARNING" + " " * 46 + "⚠️")
    print("  This script will execute Python code from a local file.")
    print("  Only proceed if you trust the source of this strategy file.")
    print("=" * 70)

    print(f"\n📂 Strategy File: {strategy_file}")

    # Check if file exists
    if not strategy_file.exists():
        print(f"  ❌ File not found: {strategy_file}")
        print("\n  💡 First run: python api_showcase_save_and_run.py")
        return

    # Load the strategy class
    print("\n" + "-" * 70)
    print("  📥 Loading strategy class...")
    print("-" * 70)

    try:
        StrategyClass = load_strategy_from_file(str(strategy_file))
        print(f"\n  ✅ Loaded: {StrategyClass.__name__}")
    except Exception as e:
        print(f"  ❌ Failed to load strategy: {e}")
        return

    # Run backtest
    print("\n" + "-" * 70)
    print("  🧪 Running backtest...")
    print("-" * 70)

    try:
        # Import Lumibot components
        from lumibot.backtesting import YahooDataBacktesting

        # Set backtest dates (last 3 months for faster testing)
        backtesting_end = datetime.now()
        backtesting_start = backtesting_end - timedelta(days=90)

        print(f"\n  📅 Date Range: {backtesting_start.date()} → {backtesting_end.date()}")
        print("  💰 Initial Budget: $10,000")
        print("\n  ⏳ Running backtest (this may take a minute)...\n")

        # Run the backtest
        result = StrategyClass.backtest(
            YahooDataBacktesting,
            backtesting_start,
            backtesting_end,
            budget=10000,
        )

        print("\n" + "-" * 70)
        print("  ✅ Backtest Complete!")
        print("-" * 70)

        # Display results
        if hasattr(result, "get_portfolio_value"):
            final_value = result.get_portfolio_value()
            print(f"\n  💵 Final Portfolio Value: ${final_value:,.2f}")
            print(f"  📈 Return: ${final_value - 10000:,.2f} ({((final_value / 10000 - 1) * 100):.2f}%)")

        print("\n" + "=" * 70)
        print("  ✅ Local Execution Validated!")
        print("=" * 70)

        print("\n  📝 The strategy executed successfully!")
        print("  💡 You can now:")
        print(f"     - Modify the code: {strategy_file}")
        print("     - Run longer backtests")
        print("     - Deploy to paper trading")
        print()

    except ImportError as e:
        print(f"\n  ❌ Missing dependencies: {e}")
        print("\n  💡 Install Lumibot:")
        print("     pip install lumibot")
    except Exception as e:
        print(f"\n  ❌ Backtest failed: {e}")
        print("\n  💡 Check the strategy code for issues:")
        print(f"     {strategy_file}")


def main():
    """Main entry point."""

    # Get strategy filename from args or use default
    if len(sys.argv) > 1:
        # Use specified strategy
        strategy_name = sys.argv[1]
        if not strategy_name.endswith(".py"):
            strategy_name += ".py"
        strategy_file = Path("strategies") / strategy_name
    else:
        # Find first .py file in strategies/ directory
        strategies_dir = Path("strategies")
        if not strategies_dir.exists():
            print("  ❌ No strategies/ directory found")
            print("  💡 First run: python api_showcase_save_and_run.py")
            return

        strategy_files = list(strategies_dir.glob("*.py"))
        if not strategy_files:
            print("  ❌ No strategy files found in strategies/")
            print("  💡 First run: python api_showcase_save_and_run.py")
            return

        strategy_file = strategy_files[0]
        print(f"  ℹ️  Using first available strategy: {strategy_file.name}")

    # Run the backtest
    run_backtest(strategy_file)


if __name__ == "__main__":
    main()
