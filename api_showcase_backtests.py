#!/usr/bin/env python3
"""
BotSpot API Client - Backtest Showcase

Demonstrates backtest submission and status polling:
- Submit backtest for AI-generated strategy
- Poll for progress (backtests take 10-30+ minutes)
- Display real-time status updates

Usage:
    python api_showcase_backtests.py
"""

from botspot_api_class import BotSpot


def main():
    """Submit and monitor a backtest."""

    print("\n" + "=" * 70)
    print("  📊 BotSpot Backtest Runner")
    print("=" * 70)

    with BotSpot() as client:
        # Step 1: Get a strategy to backtest
        print("\n📋 Fetching your strategies...")
        ai_strategies = client.strategies.list()

        if not ai_strategies:
            print("  ❌ No strategies found. Generate one first with api_showcase_generate.py")
            return

        # Use first strategy
        ai_strategy = ai_strategies[0]
        ai_strategy_id = ai_strategy["id"]
        strategy_name = ai_strategy["strategy"]["name"]

        print(f"  ✅ Found {len(ai_strategies)} strategy(s)")
        print(f"  🎯 Using: {strategy_name}")

        # Step 2: Get strategy code and version
        print("\n" + "-" * 70)
        print("  📥 Fetching strategy code...")
        print("-" * 70)

        versions_data = client.strategies.get_versions(ai_strategy_id)
        latest_version = versions_data["versions"][0]
        code = latest_version["code_out"]
        revision_id = str(latest_version["version"])

        print(f"\n  📄 Code Length: {len(code):,} characters")
        print(f"  🔢 Version: {revision_id}")

        # Step 3: Submit backtest
        print("\n" + "-" * 70)
        print("  🚀 Submitting Backtest...")
        print("-" * 70)

        print("\n  📅 Date Range: Nov 1, 2024 → Nov 30, 2024 (1 month)")
        print("  📊 Data Provider: Theta Data")

        result = client.backtests.run(
            bot_id=ai_strategy_id,
            code=code,
            start_date="2024-11-01T00:00:00.000Z",
            end_date="2024-11-30T00:00:00.000Z",
            revision_id=revision_id,
            data_provider="theta_data",
        )

        backtest_id = result["backtestId"]
        print("\n  ✅ Backtest Submitted!")
        print(f"  🆔 Backtest ID: {backtest_id}")
        print(f"  📩 Message: {result.get('message', 'N/A')}")

        # Step 4: Poll for progress
        print("\n" + "-" * 70)
        print("  ⏳ Monitoring Progress...")
        print("-" * 70)
        print("\n  ⚠️  Backtests typically take 10-30+ minutes to complete")
        print("  💡 You can stop this script and check status later\n")

        # Poll a few times to show progress
        import time

        max_polls = 5  # Only poll a few times for demo purposes
        poll_count = 0

        while poll_count < max_polls:
            status = client.backtests.get_status(backtest_id)

            running = status.get("running", False)
            stage = status.get("stage", "unknown")
            elapsed_ms = status.get("elapsed_ms", 0)
            elapsed_sec = elapsed_ms / 1000
            description = status.get("status_description", "")

            print(f"  📊 Poll #{poll_count + 1}:")
            print(f"     Running: {running}")
            print(f"     Stage: {stage}")
            print(f"     Elapsed: {elapsed_sec:.1f}s")
            print(f"     Description: {description}")

            if not running:
                print("\n  🎉 Backtest completed!")
                break

            poll_count += 1

            if poll_count < max_polls:
                print("     Waiting 5 seconds before next poll...\n")
                time.sleep(5)

        if poll_count >= max_polls and status.get("running"):
            print(f"\n  ⏸️  Still running after {max_polls} polls")
            print("     To continue monitoring, use:")
            print(f"     client.backtests.wait_for_completion('{backtest_id}')")
            print("\n     Or check status later:")
            print(f"     client.backtests.get_status('{backtest_id}')")

        print("\n" + "=" * 70)
        print("  ✅ Backtest Monitoring Complete!")
        print("=" * 70)

        print("\n  💡 Tips:")
        print("     - Use wait_for_completion() to block until done")
        print("     - Use get_status() to check progress anytime")
        print("     - Use get_results() to retrieve metrics once complete")
        print("     - Backtests typically take 10-30+ minutes")
        print()


if __name__ == "__main__":
    main()
