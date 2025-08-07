#!/usr/bin/env python3
"""
Unit test for options_helper strike calculation enhancements.
Run this test to ensure the logging and validation improvements work correctly.
"""

import unittest
from unittest.mock import Mock, MagicMock
from datetime import date, timedelta
import sys
import os

# Add the lumibot path
sys.path.insert(0, '/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot')

from lumibot.components.options_helper import OptionsHelper
from lumibot.entities import Asset

class TestOptionsHelperEnhancements(unittest.TestCase):
    
    def setUp(self):
        """Set up test fixtures"""
        self.mock_strategy = Mock()
        self.mock_strategy.log_message = Mock()
        self.mock_strategy.get_last_price = Mock(return_value=5.0)
        
        # Mock get_greeks with realistic delta values
        def mock_get_greeks(option, underlying_price=None):
            strike = option.strike
            if option.right.lower() == "put":
                if strike < underlying_price * 0.95:  # OTM put
                    return {"delta": -0.15}
                elif strike < underlying_price * 1.05:  # Near ATM put
                    return {"delta": -0.45}
                else:  # ITM put
                    return {"delta": -0.75}
            else:  # call
                if strike > underlying_price * 1.05:  # OTM call
                    return {"delta": 0.25}
                elif strike > underlying_price * 0.95:  # Near ATM call
                    return {"delta": 0.55}
                else:  # ITM call
                    return {"delta": 0.85}
        
        self.mock_strategy.get_greeks = Mock(side_effect=mock_get_greeks)
        self.options_helper = OptionsHelper(self.mock_strategy)
    
    def test_normal_strike_calculation(self):
        """Test normal strike calculation for a typical stock"""
        underlying_asset = Asset("TEST", asset_type="stock")
        underlying_price = 200.0
        target_delta = -0.3
        expiry = date.today() + timedelta(days=30)
        right = "put"
        
        result = self.options_helper.find_strike_for_delta(
            underlying_asset=underlying_asset,
            underlying_price=underlying_price,
            target_delta=target_delta,
            expiry=expiry,
            right=right
        )
        
        # Should find a reasonable strike
        self.assertIsNotNone(result)
        self.assertGreater(result, 150)  # Should be reasonable for $200 stock
        self.assertLess(result, 250)
        
        # Should have logged the search parameters
        log_calls = [str(call[0][0]) for call in self.mock_strategy.log_message.call_args_list]
        self.assertTrue(any("STRIKE SEARCH" in msg for msg in log_calls))
        self.assertTrue(any("underlying_price=$200" in msg for msg in log_calls))
    
    def test_invalid_underlying_price(self):
        """Test handling of invalid underlying price"""
        underlying_asset = Asset("TEST", asset_type="stock")
        target_delta = -0.3
        expiry = date.today() + timedelta(days=30)
        right = "put"
        
        # Test with negative price
        result = self.options_helper.find_strike_for_delta(
            underlying_asset=underlying_asset,
            underlying_price=-10.0,  # Invalid
            target_delta=target_delta,
            expiry=expiry,
            right=right
        )
        
        self.assertIsNone(result)
        
        # Should have logged an error
        log_calls = [str(call[0][0]) for call in self.mock_strategy.log_message.call_args_list]
        self.assertTrue(any("ERROR: Invalid underlying price" in msg for msg in log_calls))
    
    def test_invalid_delta(self):
        """Test handling of invalid delta values"""
        underlying_asset = Asset("TEST", asset_type="stock")
        underlying_price = 200.0
        expiry = date.today() + timedelta(days=30)
        right = "put"
        
        # Test with delta > 1
        result = self.options_helper.find_strike_for_delta(
            underlying_asset=underlying_asset,
            underlying_price=underlying_price,
            target_delta=1.5,  # Invalid
            expiry=expiry,
            right=right
        )
        
        self.assertIsNone(result)
        
        # Should have logged an error
        log_calls = [str(call[0][0]) for call in self.mock_strategy.log_message.call_args_list]
        self.assertTrue(any("ERROR: Invalid target delta" in msg for msg in log_calls))
    
    def test_warning_for_unrealistic_strike(self):
        """Test that warnings are generated for unrealistic strikes"""
        # Mock a scenario where we get an unrealistically low strike
        def mock_get_greeks_low_strike(option, underlying_price=None):
            # Always return a delta that would make very low strikes look good
            return {"delta": -0.3}
        
        self.mock_strategy.get_greeks = Mock(side_effect=mock_get_greeks_low_strike)
        
        underlying_asset = Asset("TEST", asset_type="stock")
        underlying_price = 200.0  # High stock price
        target_delta = -0.3
        expiry = date.today() + timedelta(days=30)
        right = "put"
        
        # This should find a low strike due to our mocked greeks
        result = self.options_helper.find_strike_for_delta(
            underlying_asset=underlying_asset,
            underlying_price=underlying_price,
            target_delta=target_delta,
            expiry=expiry,
            right=right
        )
        
        # Should have found something (mocked to return low strike)
        self.assertIsNotNone(result)
        
        # Should have warned about the unrealistic strike
        log_calls = [str(call[0][0]) for call in self.mock_strategy.log_message.call_args_list]
        if result and result < 10:  # If we got an unrealistically low strike
            self.assertTrue(any("WARNING" in msg and "too low" in msg for msg in log_calls))
    
    def test_enhanced_logging_format(self):
        """Test that the enhanced logging includes emoji and detailed information"""
        underlying_asset = Asset("LULU", asset_type="stock")
        underlying_price = 200.0
        target_delta = -0.3
        expiry = date.today() + timedelta(days=30)
        right = "put"
        
        result = self.options_helper.find_strike_for_delta(
            underlying_asset=underlying_asset,
            underlying_price=underlying_price,
            target_delta=target_delta,
            expiry=expiry,
            right=right
        )
        
        # Check for enhanced logging format
        log_calls = [str(call[0][0]) for call in self.mock_strategy.log_message.call_args_list]
        
        # Should have emoji in logs
        self.assertTrue(any("🎯" in msg for msg in log_calls))  # Target emoji
        self.assertTrue(any("🔍" in msg for msg in log_calls))  # Search emoji
        
        # Should show the search range
        self.assertTrue(any("Search range: strikes" in msg for msg in log_calls))
        
        # Should show individual strike attempts
        self.assertTrue(any("Trying strike" in msg for msg in log_calls))

if __name__ == "__main__":
    print("🧪 Running enhanced options helper tests...")
    unittest.main(verbosity=2)
