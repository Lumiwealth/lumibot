#!/usr/bin/env python3
"""Tests covering OptionsHelper behaviours and chain normalization."""

import unittest
from unittest.mock import Mock, MagicMock
from datetime import date, timedelta, datetime
import sys
import os

# Add the lumibot path
sys.path.insert(0, '/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot')

from lumibot.components.options_helper import OptionsHelper
from lumibot.entities import Asset
from lumibot.entities.chains import normalize_option_chains
from lumibot.brokers.broker import Broker


class _StubDataSource:
    def __init__(self, payload):
        self.payload = payload

    def get_chains(self, asset):
        return self.payload


class _StubBroker(Broker):
    IS_BACKTESTING_BROKER = True

    def __init__(self, data_source):
        super().__init__(data_source=data_source)

    def _get_stream_object(self):
        return None

    def _register_stream_events(self):
        return None

    def _run_stream(self):
        return None

    def cancel_order(self, order):
        return None

    def _modify_order(self, order, limit_price=None, stop_price=None):
        return None

    def _submit_order(self, order):
        return None

    def _get_balances_at_broker(self, quote_asset, strategy):
        return 0, 0, 0

    def get_historical_account_value(self):
        return {}

    def _pull_positions(self, strategy):
        return []

    def _pull_position(self, strategy, asset):
        return None

    def _parse_broker_order(self, response, strategy_name, strategy_object=None):
        return None

    def _pull_broker_order(self, identifier):
        return None

    def _pull_broker_all_orders(self):
        return []

class TestOptionsHelper(unittest.TestCase):
    
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

    def test_missing_chains_returns_none(self):
        """Ensure missing option-chain structures do not crash and return None."""
        target_dt = datetime(2024, 1, 2)

        result = self.options_helper.get_expiration_on_or_after_date(target_dt, {}, "call")

        self.assertIsNone(result)
        log_calls = [str(call[0][0]) for call in self.mock_strategy.log_message.call_args_list]
        self.assertTrue(any("option chains" in msg.lower() for msg in log_calls))

    def test_normalize_option_chains_adds_missing_keys(self):
        normalized_empty = normalize_option_chains({})
        self.assertIn("Chains", normalized_empty)
        self.assertEqual(normalized_empty["Chains"]["CALL"], {})
        self.assertFalse(normalized_empty)

        normalized_partial = normalize_option_chains({"Chains": {"CALL": {"2024-01-02": [100.0, 101.0]}}})
        self.assertIn("PUT", normalized_partial["Chains"])
        self.assertTrue(normalized_partial)

    def test_broker_get_chains_handles_missing_payload(self):
        asset = Asset("TEST", asset_type=Asset.AssetType.STOCK)

        broker_empty = _StubBroker(data_source=_StubDataSource({}))
        chains_empty = broker_empty.get_chains(asset)
        self.assertFalse(chains_empty)
        self.assertIn("CALL", chains_empty["Chains"])
        self.assertIn("PUT", chains_empty["Chains"])

        payload_partial = {"Chains": {"CALL": {"2024-01-02": [100.0]}}}
        broker_partial = _StubBroker(data_source=_StubDataSource(payload_partial))
        chains_partial = broker_partial.get_chains(asset)
        self.assertTrue(chains_partial)
        self.assertEqual(chains_partial["Chains"]["CALL"]["2024-01-02"], [100.0])
        self.assertIn("PUT", chains_partial["Chains"])

if __name__ == "__main__":
    print("🧪 Running enhanced options helper tests...")
    unittest.main(verbosity=2)
