#!/usr/bin/env python3
"""
BotSpot API Client - View Strategy Results Showcase

Demonstrates viewing complete strategy results including:
- Generated Python code
- Mermaid flowchart diagrams
- Strategy metadata
- Version history

Usage:
    python api_showcase_strategy_results.py
"""

from botspot_api_class import BotSpot


def main():
    """View complete strategy results (code, diagram, metadata)."""

    print("\n" + "=" * 70)
    print("  📊 BotSpot Strategy Results Viewer")
    print("=" * 70)

    with BotSpot() as client:
        # Step 1: List available strategies
        print("\n📋 Fetching your strategies...")
        ai_strategies = client.strategies.list()

        if not ai_strategies:
            print("  ❌ No strategies found. Generate one first with api_showcase_generate.py")
            return

        # Use first strategy as example
        ai_strategy = ai_strategies[0]
        ai_strategy_id = ai_strategy["id"]
        strategy_name = ai_strategy["strategy"]["name"]

        print(f"  ✅ Found {len(ai_strategies)} strategy(s)")
        print(f"  🎯 Viewing: {strategy_name}")

        # Step 2: Get complete strategy data (code, diagram, metadata)
        print("\n" + "-" * 70)
        print("  📥 Fetching complete strategy data...")
        print("-" * 70)

        data = client.strategies.get_versions(ai_strategy_id)

        # Display metadata
        strategy = data["strategy"]
        print(f"\n  📛 Strategy Name: {strategy['name']}")
        print(f"  🏷️  Strategy Type: {strategy['strategyType']}")
        print(f"  🔒 Visibility: {'Public' if strategy['isPublic'] else 'Private'}")
        print(f"  📅 Created: {strategy['createdAt'][:10]}")

        # Display versions
        versions = data["versions"]
        print(f"\n  📚 Versions: {len(versions)}")

        # Display latest version details
        latest = versions[0]
        print(f"\n  🔢 Latest Version: {latest['version']}")

        # Display code stats
        code = latest["code_out"]
        print("\n  💻 Generated Code:")
        print(f"     - Length: {len(code):,} characters")
        print(f"     - Lines: {code.count(chr(10)) + 1}")

        # Extract class name from code
        if "class " in code:
            try:
                class_line = [line for line in code.split("\n") if line.strip().startswith("class ")][0]
                class_name = class_line.split("class ")[1].split("(")[0].strip()
                print(f"     - Class: {class_name}")
            except (IndexError, AttributeError):
                pass

        # Display code preview (first 10 lines)
        print("\n  📄 Code Preview (first 10 lines):")
        print("  " + "-" * 66)
        code_lines = code.split("\n")[:10]
        for line in code_lines:
            print(f"  {line}")
        print("  " + "-" * 66)

        # Display diagram status
        print("\n  🎨 Mermaid Diagram: ", end="")
        if latest.get("mermaidDiagram"):
            diagram = latest["mermaidDiagram"]
            print(f"Available ({len(diagram)} chars)")
            # Count nodes in diagram (rough estimate)
            node_count = diagram.count("-->") + diagram.count("---")
            print(f"     - Flow connections: ~{node_count}")
            # Show first line of diagram
            first_line = diagram.split("\n")[0]
            print(f"     - Starts with: {first_line}")
        else:
            print("Not available")

        # Display description/comments
        print("\n  📝 AI Description: ", end="")
        if latest.get("comments"):
            comments = latest["comments"]
            print(f"Available ({len(comments)} chars)")
            # Show first 200 chars
            preview = comments[:200] + "..." if len(comments) > 200 else comments
            print(f"     {preview}")
        else:
            print("Not available")

        # Display backtest metrics if available
        print("\n  📊 Backtest Metrics: ", end="")
        if latest.get("backtestMetrics"):
            print("Available")
            metrics = latest["backtestMetrics"]
            print(f"     Metrics: {list(metrics.keys())[:5]}")
        else:
            print("Not run yet")

        print("\n" + "=" * 70)
        print("  ✅ Strategy Results Retrieved Successfully!")
        print("=" * 70)

        print("\n  💡 Tips:")
        print("     - Use data['versions'][0]['code_out'] to access the full code")
        print("     - Use data['versions'][0]['mermaidDiagram'] for the flowchart")
        print("     - Modify strategy name via PUT /strategies/{id} endpoint")
        print("     - Run backtests via api_showcase_backtest.py (coming soon)")
        print()


if __name__ == "__main__":
    main()
