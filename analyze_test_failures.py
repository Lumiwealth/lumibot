"""
Quick script to analyze test failures and create a comprehensive catalog.
"""

# Catalog of Failed Tests
catalog = {
    "TestCacheWarmth": {
        "stock-minute": "FAILED - warm run data mismatch",
        "stock-5minute": "FAILED - warm run data mismatch",
        "stock-15minute": "FAILED - warm run data mismatch",
        "stock-hour": "FAILED - warm run data mismatch",
        "option-hour": "FAILED - warm run data mismatch",
    },
    "TestPandasPolarsParity": {
        "stock-minute": "FAILED - pandas vs polars mismatch",
        "stock-5minute": "FAILED - pandas vs polars mismatch",
        "stock-15minute": "FAILED - pandas vs polars mismatch",
        "stock-hour": "FAILED - pandas vs polars mismatch",
        "stock-day": "FAILED - pandas vs polars mismatch",
        "option-minute": "FAILED - pandas vs polars mismatch",
        "option-5minute": "FAILED - pandas vs polars mismatch",
        "option-15minute": "FAILED - pandas vs polars mismatch",
        "option-hour": "FAILED - pandas vs polars mismatch",
        "option-day": "FAILED - pandas vs polars mismatch",
        "index-minute": "FAILED - pandas vs polars mismatch",
        "index-5minute": "FAILED - pandas vs polars mismatch",
        "index-15minute": "FAILED - pandas vs polars mismatch",
        "index-hour": "FAILED - pandas vs polars mismatch",
        "index-day": "FAILED - pandas vs polars mismatch",
    }
}

print("=" * 80)
print("TEST FAILURE CATALOG")
print("=" * 80)
print()
print(f"Total Failed Tests: 20")
print()
print("TestCacheWarmth Failures (5):")
for test, status in catalog["TestCacheWarmth"].items():
    print(f"  - {test}: {status}")
print()
print("TestPandasPolarsParity Failures (15):")
for test, status in catalog["TestPandasPolarsParity"].items():
    print(f"  - {test}: {status}")
print()
print("=" * 80)
print("NEXT STEPS:")
print("=" * 80)
print("1. Add diff/comparison code to test to see exact differences")
print("2. Map DEBUG logs back to each failure")
print("3. Identify patterns")
print("4. Fix one pattern at a time")
print("=" * 80)
