#!/usr/bin/env python3
"""
Quick test to verify profiler can import strategy
"""

import sys
import os
from pathlib import Path

# Add examples to path (same way profiler does it)
sys.path.insert(0, str(Path(__file__).parent / "examples"))

print("Testing strategy import...")
print(f"Python path includes: {sys.path[0]}")

try:
    from gc_futures_optimized import GCFuturesOptimized
    print(f"✅ Success! Imported {GCFuturesOptimized.__name__}")
    print(f"   Class: {GCFuturesOptimized}")
except ImportError as e:
    print(f"❌ Import failed: {e}")
    sys.exit(1)

print("\nTesting DataBento API key...")
api_key = os.getenv("DATABENTO_API_KEY")
if api_key:
    print(f"✅ API key found (length: {len(api_key)})")
else:
    print("❌ DATABENTO_API_KEY not set")
    sys.exit(1)

print("\nTesting BacktestProfiler...")
try:
    from profiler.runner import BacktestProfiler
    profiler = BacktestProfiler()
    print("✅ BacktestProfiler initialized")

    # Test config loading
    run = profiler.config.get_run_by_id("phase0-cold-2024-09")
    if run:
        print(f"✅ Found run: {run['id']}")
        print(f"   Status: {run['status']}")
    else:
        print("❌ Could not find phase0-cold-2024-09")

except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print("\n" + "="*60)
print("ALL TESTS PASSED ✅")
print("="*60)
print("\nProfiler should work. Try running:")
print("  python -m profiler.runner --phase 0")
