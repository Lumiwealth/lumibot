#!/usr/bin/env python3
"""
Test script to verify strike calculation logic in OptionsHelper
"""

import sys
sys.path.insert(0, '.')

from lumibot.entities import Asset
from lumibot.components.options_helper import OptionsHelper
from datetime import date, timedelta

class MockStrategy:
    """Mock strategy for testing"""
    
    def __init__(self):
        self.log_messages = []
    
    def log_message(self, message, color=None):
        self.log_messages.append(f"[{color}] {message}")
        print(f"[{color}] {message}")
    
    def get_last_price(self, asset):
        # Mock prices for testing
        if asset.asset_type == "option":
            return 5.0  # Mock option price
        return 200.0  # Mock underlying price for LULU
    
    def get_greeks(self, option, underlying_price=None):
        # Mock greeks calculation
        # For testing, return deltas that make sense for the strikes
        strike = option.strike
        if option.right.lower() == "put":
            # For puts, delta should be negative and closer to 0 as strike gets lower
            if strike < underlying_price:
                delta = -0.1  # Out of the money put
            else:
                delta = -0.5  # At/in the money put
        else:  # call
            # For calls, delta should be positive and closer to 1 as strike gets lower
            if strike < underlying_price:
                delta = 0.8  # In the money call
            else:
                delta = 0.2  # Out of the money call
        
        return {"delta": delta}

def test_strike_calculation():
    """Test the strike calculation for LULU-like scenario"""
    
    # Create mock strategy
    strategy = MockStrategy()
    
    # Create options helper
    options_helper = OptionsHelper(strategy)
    
    # Create LULU-like asset
    underlying_asset = Asset("LULU", asset_type="stock")
    
    # Test parameters
    underlying_price = 200.0  # LULU trading around $200
    target_delta = -0.3  # Looking for put with -0.3 delta
    expiry = date.today() + timedelta(days=30)  # 30 days out
    right = "put"
    
    print(f"\nTesting strike calculation for:")
    print(f"  Underlying: {underlying_asset.symbol}")
    print(f"  Underlying price: ${underlying_price}")
    print(f"  Target delta: {target_delta}")
    print(f"  Expiry: {expiry}")
    print(f"  Option type: {right}")
    
    # Call the function
    result_strike = options_helper.find_strike_for_delta(
        underlying_asset=underlying_asset,
        underlying_price=underlying_price,
        target_delta=target_delta,
        expiry=expiry,
        right=right
    )
    
    print(f"\nResult strike: {result_strike}")
    
    # Check if result makes sense
    if result_strike is not None:
        if result_strike < 50:
            print(f"❌ ERROR: Strike {result_strike} is unreasonably low for a ${underlying_price} stock!")
        elif result_strike > 160 and result_strike < 240:
            print(f"✅ SUCCESS: Strike {result_strike} is reasonable for a ${underlying_price} stock")
        else:
            print(f"⚠️  WARNING: Strike {result_strike} might be unusual for a ${underlying_price} stock")
    else:
        print("❌ ERROR: No strike found!")
    
    print(f"\nLogs from the calculation:")
    for log in strategy.log_messages:
        print(f"  {log}")

if __name__ == "__main__":
    test_strike_calculation()
