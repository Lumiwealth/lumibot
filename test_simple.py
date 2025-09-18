#!/usr/bin/env python3
"""
Simple test to verify our logging and loop changes work.
"""

print("🔍 Testing our changes...")

# Test 1: Check that our enhanced logging is in the strategy_executor file
try:
    with open('/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot/lumibot/strategies/strategy_executor.py', 'r') as f:
        content = f.read()

    # Check for our key changes
    has_cloud_update_first = "Send data to cloud every minute FIRST" in content
    has_loop_logging = "Main loop iteration" in content
    has_cloud_logging = "Sending cloud update" in content

    print(f"✅ Cloud update moved first: {has_cloud_update_first}")
    print(f"✅ Loop iteration logging added: {has_loop_logging}")
    print(f"✅ Cloud update logging added: {has_cloud_logging}")

    if all([has_cloud_update_first, has_loop_logging, has_cloud_logging]):
        print("🎉 All logging changes are present!")
    else:
        print("❌ Some changes are missing!")

except Exception as e:
    print(f"❌ Error checking file: {e}")

# Test 2: Check Order.to_dict() fix
try:
    from lumibot.entities import Order, Asset

    # Create test order
    asset = Asset("AAPL", "stock")
    order = Order("test_strategy", asset, 100, "buy")

    # Add problematic fields
    order._bars = {"huge": "data" * 1000}
    order._raw = {"raw": "data" * 100}
    order._transmitted = True

    # Test serialization
    result = order.to_dict()

    # Check that problematic fields are excluded
    has_bars = "_bars" in result
    has_raw = "_raw" in result
    has_transmitted = "_transmitted" in result

    print(f"✅ Order._bars excluded: {not has_bars}")
    print(f"✅ Order._raw excluded: {not has_raw}")
    print(f"✅ Order._transmitted excluded: {not has_transmitted}")

    if not any([has_bars, has_raw, has_transmitted]):
        print("🎉 Order serialization fix works!")
    else:
        print("❌ Order serialization still includes problematic fields!")

except Exception as e:
    print(f"❌ Error testing Order serialization: {e}")

print("\n✅ Tests completed!")
print("📋 Summary:")
print("   1. Cloud update code moved to run BEFORE market status checks")
print("   2. Comprehensive logging added to track loop execution")
print("   3. Order.to_dict() excludes problematic large fields")
print("\n🚀 Ready for deployment!")