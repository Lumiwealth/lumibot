#!/usr/bin/env python3
"""
Test the options helper strike calculation with proper logging
"""

import sys
import os
sys.path.append('/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot')

import pytest
from unittest.mock import Mock, MagicMock
from datetime import date, timedelta

# Test if we can import the options helper
try:
    from lumibot.components.options_helper import OptionsHelper
    from lumibot.entities import Asset
    print("✅ Successfully imported OptionsHelper")
except ImportError as e:
    print(f"❌ Failed to import: {e}")
    exit(1)

def test_strike_calculation_logging():
    """Test that the enhanced logging works correctly"""
    
    # Create a mock strategy
    mock_strategy = Mock()
    mock_strategy.log_message = Mock()
    
    # Mock get_last_price to return valid prices
    mock_strategy.get_last_price = Mock(return_value=5.0)
    
    # Mock get_greeks to return reasonable deltas
    def mock_get_greeks(option, underlying_price=None):
        strike = option.strike
        if option.right.lower() == "put":
            # Lower strikes have lower (less negative) deltas for puts
            if strike < underlying_price:
                return {"delta": -0.2}  # OTM put
            else:
                return {"delta": -0.6}  # ITM put
        else:
            # Lower strikes have higher deltas for calls
            if strike < underlying_price:
                return {"delta": 0.7}  # ITM call
            else:
                return {"delta": 0.3}  # OTM call
    
    mock_strategy.get_greeks = Mock(side_effect=mock_get_greeks)
    
    # Create the options helper
    options_helper = OptionsHelper(mock_strategy)
    
    # Create test parameters
    underlying_asset = Asset("TEST", asset_type="stock")
    underlying_price = 200.0
    target_delta = -0.3
    expiry = date.today() + timedelta(days=30)
    right = "put"
    
    print(f"\n🧪 Testing strike calculation with enhanced logging...")
    print(f"   Underlying: {underlying_asset.symbol} @ ${underlying_price}")
    print(f"   Target delta: {target_delta}")
    print(f"   Option type: {right}")
    
    # Call the function
    result = options_helper.find_strike_for_delta(
        underlying_asset=underlying_asset,
        underlying_price=underlying_price,
        target_delta=target_delta,
        expiry=expiry,
        right=right
    )
    
    print(f"\n📊 Result: Strike = {result}")
    
    # Check that logging was called
    assert mock_strategy.log_message.called, "log_message should have been called"
    
    # Print all the log messages
    print(f"\n📝 Log messages generated:")
    for call in mock_strategy.log_message.call_args_list:
        args, kwargs = call
        color = kwargs.get('color', 'none')
        print(f"   [{color}] {args[0]}")
    
    # Verify the result makes sense
    if result is not None:
        if 150 <= result <= 250:
            print(f"✅ Strike {result} is reasonable for ${underlying_price} stock")
        else:
            print(f"⚠️  Strike {result} seems unusual for ${underlying_price} stock")
            
        # Check for our warning message
        warning_found = any("WARNING" in str(call[0][0]) for call in mock_strategy.log_message.call_args_list)
        if result < 50 and underlying_price > 100:
            assert warning_found, "Should have warned about unreasonably low strike"
            print(f"✅ Warning correctly triggered for low strike")
    else:
        print(f"❌ No strike found")

if __name__ == "__main__":
    test_strike_calculation_logging()
    print(f"\n🎉 Test completed successfully!")
