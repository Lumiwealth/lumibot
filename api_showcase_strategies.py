#!/usr/bin/env python3
"""
BotSpot API Client - Strategies Showcase

Demonstrates AI strategy management capabilities:
- List all AI-generated strategies
- Get strategy versions and revisions
- Generate Mermaid diagrams from code

Usage:
    python api_showcase_strategies.py
"""

from botspot_api_class import BotSpot


def main():
    """Showcase strategy management features."""

    with BotSpot() as client:
        print("\n" + "=" * 70)
        print("  📊 BotSpot AI Strategies - Management Demo")
        print("=" * 70)

        # Get usage limits
        print("\n" + "-" * 70)
        print("  💡 Checking Prompt Usage Limits...")
        print("-" * 70)
        try:
            limits = client.strategies.get_usage_limits()
            print(f"  📈 Usage Data: {limits}")
        except Exception as e:
            print(f"  ⚠️  Could not fetch usage limits: {e}")

        # List all strategies
        print("\n" + "-" * 70)
        print("  📋 Listing All AI Strategies...")
        print("-" * 70)

        ai_strategies = client.strategies.list()

        if not ai_strategies:
            print("  ℹ️  No strategies found. Create one at https://botspot.trade")
            return

        print(f"  ✓ Found {len(ai_strategies)} strategy/strategies\n")

        for idx, ai_strategy in enumerate(ai_strategies, 1):
            strategy = ai_strategy["strategy"]
            revision_count = ai_strategy.get("revisionCount", 1)

            print(f"  {idx}. {strategy['name']}")
            print(f"     🔑 Strategy ID: {strategy['id']}")
            print(f"     🆔 AI Strategy ID: {ai_strategy['id']}")
            print(f"     📝 Revisions: {revision_count}")
            print(f"     📅 Created: {strategy['createdAt']}")
            print()

        # Get versions for first strategy
        if ai_strategies:
            print("-" * 70)
            print("  🔍 Fetching Versions for First Strategy...")
            print("-" * 70)

            first_ai_strategy = ai_strategies[0]
            ai_strategy_id = first_ai_strategy["id"]
            strategy_name = first_ai_strategy["strategy"]["name"]

            try:
                versions_data = client.strategies.get_versions(ai_strategy_id)
                versions = versions_data.get("versions", [])

                print(f"  ✓ Strategy: {strategy_name}")
                print(f"  ✓ Found {len(versions)} version(s)\n")

                for version in versions:
                    version_num = version.get("version", "?")
                    code_length = len(version.get("code_out", ""))
                    comments = version.get("comments", "")[:100]

                    print(f"  Version {version_num}:")
                    print(f"    📄 Code Length: {code_length} characters")
                    print(f"    💬 Description: {comments}{'...' if len(comments) == 100 else ''}")
                    print()

            except Exception as e:
                print(f"  ⚠️  Error fetching versions: {e}")

        print("=" * 70)
        print("  ✅ Showcase Complete!")
        print("=" * 70 + "\n")


if __name__ == "__main__":
    main()
