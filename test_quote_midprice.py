#!/usr/bin/env python3
"""Test that Quote class properly calculates mid_price"""

import sys
sys.path.insert(0, "/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot")

from lumibot.entities import Asset, Quote

# Test 1: Quote with bid and ask should auto-calculate mid_price
asset = Asset("TEST", asset_type="stock")
quote1 = Quote(asset=asset, bid=100.0, ask=101.0)
print(f"Test 1 - Bid: {quote1.bid}, Ask: {quote1.ask}, Mid: {quote1.mid_price}")
assert quote1.mid_price == 100.5, "Mid price should be 100.5"
print("✓ Test 1 passed: mid_price auto-calculated from bid/ask")

# Test 2: Quote with explicit mid_price should use that
quote2 = Quote(asset=asset, bid=100.0, ask=101.0, mid_price=100.45)
print(f"Test 2 - Bid: {quote2.bid}, Ask: {quote2.ask}, Mid: {quote2.mid_price}")
assert quote2.mid_price == 100.45, "Should use explicit mid_price"
print("✓ Test 2 passed: explicit mid_price preserved")

# Test 3: Quote with no bid/ask should fall back to price
quote3 = Quote(asset=asset, price=99.5)
print(f"Test 3 - Price: {quote3.price}, Mid: {quote3.mid_price}")
assert quote3.mid_price == 99.5, "Should fall back to price"
print("✓ Test 3 passed: falls back to price when no bid/ask")

# Test 4: Quote with None bid/ask should also fall back
quote4 = Quote(asset=asset, bid=None, ask=None, price=98.0)
print(f"Test 4 - Bid: {quote4.bid}, Ask: {quote4.ask}, Price: {quote4.price}, Mid: {quote4.mid_price}")
assert quote4.mid_price == 98.0, "Should fall back to price when bid/ask are None"
print("✓ Test 4 passed: handles None bid/ask correctly")

print("\n✅ All Quote mid_price tests passed!")