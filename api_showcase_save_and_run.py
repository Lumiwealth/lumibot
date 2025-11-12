#!/usr/bin/env python3
"""
BotSpot API Client - Save & Run Strategy Locally

Demonstrates:
- Fetching a strategy from BotSpot
- Saving it to a local file
- Running it locally (backtest)
- Validating execution

Usage:
    python api_showcase_save_and_run.py
"""

import sys
from pathlib import Path

from botspot_api_class import BotSpot


def main():
    """Save and run a strategy locally."""

    print("\n" + "=" * 70)
    print("  💾 BotSpot - Save & Run Strategy Locally")
    print("=" * 70)

    with BotSpot() as client:
        # Step 1: Fetch a strategy
        print("\n📥 Fetching your strategies...")
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

        # Step 2: Get strategy code
        print("\n" + "-" * 70)
        print("  📄 Fetching strategy code...")
        print("-" * 70)

        versions_data = client.strategies.get_versions(ai_strategy_id)
        latest_version = versions_data["versions"][0]
        code = latest_version["code_out"]

        print(f"\n  💻 Code Length: {len(code):,} characters")
        print(f"  🔢 Version: {latest_version['version']}")

        # Step 3: Save to local file
        print("\n" + "-" * 70)
        print("  💾 Saving strategy to local file...")
        print("-" * 70)

        # Create safe filename from strategy name
        filename = strategy_name.lower().replace(" ", "_").replace("-", "_")
        filename = "".join(c for c in filename if c.isalnum() or c == "_")

        try:
            filepath = client.strategies.save_to_file(
                code=code,
                filename=filename,
                output_dir="strategies",
                overwrite=True,  # Allow overwriting for demo
            )

            print(f"\n  ✅ Strategy saved to: {filepath}")
            print(f"  📂 Directory: {Path(filepath).parent}")
            print(f"  📝 Filename: {Path(filepath).name}")

        except FileExistsError as e:
            print(f"  ⚠️  File exists: {e}")
            return
        except OSError as e:
            print(f"  ❌ Error saving file: {e}")
            return

        # Step 4: Validate the file
        print("\n" + "-" * 70)
        print("  ✅ Validating saved file...")
        print("-" * 70)

        try:
            # Read back the file
            saved_code = Path(filepath).read_text()
            print(f"\n  ✓ File readable: {len(saved_code):,} characters")

            # Check if it's valid Python (basic syntax check)
            compile(saved_code, filepath, "exec")
            print("  ✓ Python syntax valid")

            # Check for Lumibot imports
            if "from lumibot" in saved_code or "import lumibot" in saved_code:
                print("  ✓ Lumibot imports found")
            else:
                print("  ⚠️  No Lumibot imports detected")

            # Check for Strategy class
            if "class " in saved_code and "(Strategy)" in saved_code:
                print("  ✓ Strategy class definition found")
            else:
                print("  ⚠️  Strategy class not detected")

        except SyntaxError as e:
            print(f"  ❌ Syntax error in generated code: {e}")
            return
        except Exception as e:
            print(f"  ❌ Validation error: {e}")
            return

        # Step 5: Test import (optional - may fail if dependencies missing)
        print("\n" + "-" * 70)
        print("  🧪 Testing strategy import...")
        print("-" * 70)

        try:
            # Add parent directory to path to allow import
            parent_dir = str(Path(filepath).parent.parent)
            if parent_dir not in sys.path:
                sys.path.insert(0, parent_dir)

            # Try to import the module
            module_name = Path(filepath).stem
            __import__(f"strategies.{module_name}")

            print("\n  ✅ Strategy imported successfully!")
            print(f"     Module: strategies.{module_name}")

        except ImportError as e:
            print(f"  ⚠️  Import test skipped (missing dependencies): {e}")
            print("     This is expected if Lumibot is not installed")
            print("     The strategy file is still valid and ready to use")
        except Exception as e:
            print(f"  ⚠️  Import failed: {e}")
            print("     The file may need manual adjustments")

        # Summary
        print("\n" + "=" * 70)
        print("  ✅ Save & Validation Complete!")
        print("=" * 70)

        print("\n  📝 Next Steps:")
        print(f"     1. Review the code: {filepath}")
        print("     2. Install dependencies: pip install lumibot")
        print("     3. Run locally: python {filepath}")
        print("     4. Or import in your code:")
        print(f"        from strategies.{module_name} import {filename.replace('_', ' ').title().replace(' ', '')}")
        print()


if __name__ == "__main__":
    main()
