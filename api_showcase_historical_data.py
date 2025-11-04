#!/usr/bin/env python3
"""
BotSpot API Client - Historical Data Showcase

Demonstrates listing and retrieving historical data:
- List all strategies
- Get specific strategy details
- View backtest history for a strategy

Usage:
    python api_showcase_historical_data.py
"""

from botspot_api_class import BotSpot


def main():
    """List strategies and view backtest history."""

    print("\n" + "=" * 70)
    print("  📚 BotSpot Historical Data Viewer")
    print("=" * 70)

    with BotSpot() as client:
        # Step 1: List all strategies
        print("\n📋 Listing your strategies...")
        ai_strategies = client.strategies.list()

        if not ai_strategies:
            print("  ❌ No strategies found. Generate one first with api_showcase_generate.py")
            return

        print(f"  ✅ Found {len(ai_strategies)} strategy(s):\n")

        for i, ai_strategy in enumerate(ai_strategies, 1):
            strategy = ai_strategy["strategy"]
            strategy_name = strategy["name"]
            revision_count = ai_strategy["revisionCount"]
            created_at = ai_strategy["createdAt"][:10]  # YYYY-MM-DD

            print(f"  {i}. {strategy_name}")
            print(f"     Revisions: {revision_count}")
            print(f"     Created: {created_at}\n")

        # Step 2: Get details for first strategy
        print("-" * 70)
        print("  📄 Fetching details for first strategy...")
        print("-" * 70)

        ai_strategy = ai_strategies[0]
        ai_strategy_id = ai_strategy["id"]
        strategy_id = ai_strategy["strategy"]["id"]
        strategy_name = ai_strategy["strategy"]["name"]

        versions_data = client.strategies.get_versions(ai_strategy_id)
        versions = versions_data["versions"]
        latest_version = versions[0]

        print(f"\n  🎯 Strategy: {strategy_name}")
        print(f"  🔢 Total Versions: {len(versions)}")
        print(f"  📝 Latest Version: {latest_version['version']}")
        print(f"  💻 Code Length: {len(latest_version['code_out']):,} characters")

        # Step 3: Get backtest history
        print("\n" + "-" * 70)
        print("  📊 Fetching backtest history...")
        print("-" * 70)

        stats = client.backtests.get_stats(strategy_id)
        backtests = stats["backtests"]

        print(f"\n  📈 Total Backtests: {len(backtests)}")

        if len(backtests) > 0:
            print(f"\n  Recent backtests for '{strategy_name}':")
            for backtest in backtests[:5]:  # Show first 5
                backtest_id = backtest.get("id", "N/A")[:8]  # First 8 chars
                print(f"    • {backtest_id}...")
        else:
            print(f"\n  No backtests found for '{strategy_name}'")
            print("  💡 Run a backtest with: python api_showcase_backtests.py")

        print("\n" + "=" * 70)
        print("  ✅ Historical Data Retrieval Complete!")
        print("=" * 70)
        print()


if __name__ == "__main__":
    main()
