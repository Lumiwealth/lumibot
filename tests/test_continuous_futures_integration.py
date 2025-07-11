"""
Integration tests for continuous futures resolution across different components.

This test file verifies that the centralized continuous futures logic in the 
Asset class is properly used by all components (DataBento helper, ProjectX data source, etc.)
and that there's no duplicate or inconsistent logic.
"""

import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime, date

from lumibot.entities import Asset
from lumibot.tools.databento_helper import _format_futures_symbol_for_databento


class TestContinuousFuturesIntegration(unittest.TestCase):
    """Integration tests for continuous futures resolution across the system."""

    def setUp(self):
        """Set up test fixtures."""
        self.test_date = datetime(2025, 7, 15)  # July 15, 2025
        
    @patch('datetime.datetime')
    def test_asset_class_consistency(self, mock_datetime):
        """Test that Asset class methods provide consistent results."""
        mock_datetime.now.return_value = self.test_date
        
        # Test multiple symbols
        test_symbols = ["ES", "MES", "NQ", "MNQ", "RTY", "CL", "GC"]
        
        for symbol in test_symbols:
            with self.subTest(symbol=symbol):
                asset = Asset(symbol, asset_type=Asset.AssetType.CONT_FUTURE)
                
                # Get primary contract and potential contracts
                primary_contract = asset.resolve_continuous_futures_contract()
                potential_contracts = asset.get_potential_futures_contracts()
                
                # Primary contract should be in the potential contracts list
                self.assertIn(primary_contract, potential_contracts)
                
                # Primary contract should be the first in the list (highest priority)
                self.assertEqual(potential_contracts[0], primary_contract)
                
                # All contracts should start with the base symbol
                for contract in potential_contracts:
                    self.assertTrue(contract.startswith(symbol))

    @patch('datetime.datetime')
    def test_databento_integration_consistency(self, mock_datetime):
        """Test that DataBento helper uses Asset class methods consistently."""
        mock_datetime.now.return_value = self.test_date
        
        # Test continuous futures
        continuous_asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)
        
        # Get result from DataBento helper
        databento_result = _format_futures_symbol_for_databento(continuous_asset)
        
        # Get result from Asset class directly
        asset_result = continuous_asset.resolve_continuous_futures_contract()
        
        # DataBento helper should use Asset class method and then apply DataBento formatting
        # Asset returns MESU25, DataBento formats it to MESU5 (removes one digit from year)
        self.assertEqual(asset_result, "MESU25")  # Asset class returns full format
        self.assertEqual(databento_result, "MESU5")  # DataBento applies its formatting
        
        # Test with specific futures (should not use continuous resolution)
        specific_asset = Asset("MES", asset_type=Asset.AssetType.FUTURE, 
                             expiration=date(2025, 9, 19))
        specific_result = _format_futures_symbol_for_databento(specific_asset)
        self.assertEqual(specific_result, "MESU25")  # September 2025

    def test_error_handling_consistency(self):
        """Test that error handling is consistent across components."""
        # Test with non-continuous futures asset
        stock_asset = Asset("AAPL", asset_type=Asset.AssetType.STOCK)
        
        # Asset class should raise ValueError
        with self.assertRaises(ValueError) as context:
            stock_asset.resolve_continuous_futures_contract()
        self.assertIn("can only be called on CONT_FUTURE assets", str(context.exception))
        
        with self.assertRaises(ValueError) as context:
            stock_asset.get_potential_futures_contracts()
        self.assertIn("can only be called on CONT_FUTURE assets", str(context.exception))

    @patch('datetime.datetime')
    def test_multi_component_consistency(self, mock_datetime):
        """Test that multiple components produce consistent results."""
        mock_datetime.now.return_value = self.test_date
        
        # Create the same continuous futures asset
        symbol = "ES"
        asset = Asset(symbol, asset_type=Asset.AssetType.CONT_FUTURE)
        
        # Get results from different sources
        asset_primary = asset.resolve_continuous_futures_contract()
        asset_potential = asset.get_potential_futures_contracts()
        databento_result = _format_futures_symbol_for_databento(asset)
        
        # Asset class should return full format
        self.assertEqual(asset_primary, "ESU25")
        self.assertIn("ESU25", asset_potential)
        
        # DataBento helper should apply its specific formatting
        self.assertEqual(databento_result, "ESU5")  # DataBento format

    @patch('datetime.datetime')
    def test_contract_format_standardization(self, mock_datetime):
        """Test that all components follow the same contract format standards."""
        mock_datetime.now.return_value = self.test_date
        
        test_cases = [
            ("ES", r"^ES[A-Z]\d{2}$", r"^ES[A-Z]\d$"),      # Asset format vs DataBento format
            ("MES", r"^MES[A-Z]\d{2}$", r"^MES[A-Z]\d$"),   # Asset format vs DataBento format  
            ("NQ", r"^NQ[A-Z]\d{2}$", r"^NQ[A-Z]\d$"),      # Asset format vs DataBento format
        ]
        
        for symbol, asset_pattern, databento_pattern in test_cases:
            with self.subTest(symbol=symbol):
                asset = Asset(symbol, asset_type=Asset.AssetType.CONT_FUTURE)
                
                # Test Asset class returns full format
                asset_result = asset.resolve_continuous_futures_contract()
                self.assertRegex(asset_result, asset_pattern)
                
                # Test DataBento integration returns DataBento format
                databento_result = _format_futures_symbol_for_databento(asset)
                self.assertRegex(databento_result, databento_pattern)

    def test_no_duplicate_logic_verification(self):
        """Verify that continuous futures logic is centralized in Asset class."""
        # This test ensures that there's no hardcoded continuous futures logic
        # outside of the Asset class by checking that all integrations delegate
        # to Asset class methods.
        
        # Create continuous futures asset
        asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)
        
        # Test that all public continuous futures functionality goes through Asset class
        self.assertTrue(hasattr(asset, 'resolve_continuous_futures_contract'))
        self.assertTrue(hasattr(asset, 'get_potential_futures_contracts'))
        self.assertTrue(callable(asset.resolve_continuous_futures_contract))
        self.assertTrue(callable(asset.get_potential_futures_contracts))
        
        # Test that DataBento helper doesn't have its own continuous futures logic
        # by verifying it uses Asset class methods
        with patch.object(Asset, 'resolve_continuous_futures_contract', 
                         return_value='MOCKED_CONTRACT') as mock_method:
            
            result = _format_futures_symbol_for_databento(asset)
            
            # Should use the centralized method
            mock_method.assert_called_once()
            # DataBento helper should apply its own formatting to the centralized result
            # 'MOCKED_CONTRACT' gets formatted to DataBento's {ROOT}{MONTH}{YEAR} format
            # MES + K (from 'MOCKED_CONTRACT'[6]) + T (from 'MOCKED_CONTRACT'[-1]) = 'MESKT'
            self.assertEqual(result, 'MESKT')

    @patch('datetime.datetime')
    def test_quarterly_preference_consistency(self, mock_datetime):
        """Test that quarterly contract preference is consistent across components."""
        # Test different months throughout the year
        quarterly_test_cases = [
            (datetime(2025, 1, 15), 'H', 'H'),   # Jan -> Mar
            (datetime(2025, 4, 15), 'M', 'M'),   # Apr -> Jun
            (datetime(2025, 7, 15), 'U', 'U'),   # Jul -> Sep
            (datetime(2025, 10, 15), 'Z', 'Z'),  # Oct -> Dec
        ]
        
        for test_date, expected_asset_month, expected_databento_month in quarterly_test_cases:
            with self.subTest(date=test_date):
                mock_datetime.now.return_value = test_date
                
                asset = Asset("ES", asset_type=Asset.AssetType.CONT_FUTURE)
                
                # Test Asset class
                asset_result = asset.resolve_continuous_futures_contract()
                asset_month_code = asset_result[-3]  # Third from end
                
                # Test DataBento integration
                databento_result = _format_futures_symbol_for_databento(asset)
                databento_month_code = databento_result[-2]  # Second from end (DataBento format)
                
                # Both should produce the same month code
                self.assertEqual(asset_month_code, expected_asset_month)
                self.assertEqual(databento_month_code, expected_databento_month)

    def test_centralization_verification(self):
        """Verify that continuous futures logic is centralized in Asset class."""
        # This test ensures that there's no hardcoded continuous futures logic
        # outside of the Asset class by checking that all integrations delegate
        # to Asset class methods.
        
        # Create continuous futures asset
        asset = Asset("ES", asset_type=Asset.AssetType.CONT_FUTURE)
        
        # Test that all public continuous futures functionality goes through Asset class
        self.assertTrue(hasattr(asset, 'resolve_continuous_futures_contract'))
        self.assertTrue(hasattr(asset, 'get_potential_futures_contracts'))
        self.assertTrue(callable(asset.resolve_continuous_futures_contract))
        self.assertTrue(callable(asset.get_potential_futures_contracts))
        
        # Test that DataBento helper doesn't have its own continuous futures logic
        # by verifying it uses Asset class methods
        with patch.object(Asset, 'resolve_continuous_futures_contract', 
                         return_value='CENTRALIZED_RESULT') as mock_method:
            
            result = _format_futures_symbol_for_databento(asset)
            
            # Should use the centralized method
            mock_method.assert_called_once()
            # DataBento helper should apply its own formatting to the centralized result
            # 'CENTRALIZED_RESULT' gets formatted to DataBento's {ROOT}{MONTH}{YEAR} format
            # ES + N (from 'CENTRALIZED_RESULT'[2]) + T (from 'CENTRALIZED_RESULT'[-1]) = 'ESNT'
            self.assertEqual(result, 'ESNT')


if __name__ == '__main__':
    unittest.main(verbosity=2)
