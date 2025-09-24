"""
Unit tests for continuous futures resolution in DataBento helper.
"""

import unittest
from datetime import datetime, date

from lumibot.tools.databento_helper import (
    _format_futures_symbol_for_databento,
)
from lumibot.entities import Asset


class TestContinuousFuturesResolution(unittest.TestCase):
    """Test continuous futures resolution for DataBento integration."""

    def setUp(self):
        """Set up test fixtures."""
        self.test_date = datetime(2025, 7, 2)  # July 2, 2025
        
    def test_generate_current_futures_contract_july(self):
        """Test contract generation for July 2025."""
        asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)
        contract = asset.resolve_continuous_futures_contract(reference_date=self.test_date)
        self.assertEqual(contract, 'MESU25')

        asset_es = Asset("ES", asset_type=Asset.AssetType.CONT_FUTURE)
        contract = asset_es.resolve_continuous_futures_contract(reference_date=self.test_date)
        self.assertEqual(contract, 'ESU25')

    def test_generate_potential_contracts_july(self):
        """Test potential contracts generation for July 2025."""
        contracts = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE).get_potential_futures_contracts(
            reference_date=self.test_date
        )

        self.assertIn('MESU25', contracts)
        self.assertIn('MESZ25', contracts)
        self.assertIn('MESH26', contracts)
        self.assertLess(contracts.index('MESU25'), contracts.index('MESZ25'))
    
    def test_format_continuous_futures_symbol(self):
        """Test formatting of continuous futures symbols."""
        asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)
        resolved = _format_futures_symbol_for_databento(asset, self.test_date)
        self.assertEqual(resolved, 'MESU5')
    
    def test_format_specific_futures_symbol(self):
        """Test formatting of specific futures contracts."""
        # Create specific futures asset with expiration
        expiration_date = date(2025, 12, 19)  # December 2025
        asset = Asset("MES", asset_type=Asset.AssetType.FUTURE, expiration=expiration_date)
        
        # Should format to MESZ25 (December 2025)
        resolved = _format_futures_symbol_for_databento(asset)
        self.assertEqual(resolved, 'MESZ25')
    
    def test_contract_ordering_priority(self):
        """Test that contracts are ordered by trading priority."""
        contracts = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE).get_potential_futures_contracts(
            reference_date=self.test_date
        )
        
        # First few contracts should be the most liquid quarterly months
        # For July, September should be first (closest quarter)
        self.assertEqual(contracts[0], 'MESU25')  # Sep 2025 standard format
        
        # Should include multiple formats
        formats = [c for c in contracts if 'MESU25' in c or 'MES.U25' in c]
        self.assertGreater(len(formats), 1)  # Should have multiple format variations
    
    def test_month_code_mapping(self):
        """Test that month codes are correctly mapped."""
        test_cases = [
            (datetime(2025, 1, 15), 'H'),
            (datetime(2025, 4, 15), 'M'),
            (datetime(2025, 7, 15), 'U'),
            (datetime(2025, 10, 15), 'Z'),
        ]

        for test_date, expected_month_code in test_cases:
            contract = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE).resolve_continuous_futures_contract(
                reference_date=test_date
            )
            self.assertIn(expected_month_code, contract)

    def test_error_handling_non_continuous_futures(self):
        """Test that proper errors are raised for non-continuous futures assets."""
        # Test with stock
        stock_asset = Asset("AAPL", asset_type=Asset.AssetType.STOCK)
        with self.assertRaises(ValueError) as context:
            stock_asset.resolve_continuous_futures_contract()
        self.assertIn("can only be called on CONT_FUTURE assets", str(context.exception))
        
        # Test with option
        option_asset = Asset("AAPL", asset_type=Asset.AssetType.OPTION)
        with self.assertRaises(ValueError) as context:
            option_asset.resolve_continuous_futures_contract()
        self.assertIn("can only be called on CONT_FUTURE assets", str(context.exception))
        
        # Test get_potential_futures_contracts with non-continuous asset
        with self.assertRaises(ValueError) as context:
            stock_asset.get_potential_futures_contracts()
        self.assertIn("can only be called on CONT_FUTURE assets", str(context.exception))

    def test_year_rollover_edge_cases(self):
        """Test contract generation around year boundaries with expiration-aware logic."""
        asset = Asset("ES", asset_type=Asset.AssetType.CONT_FUTURE)

        contract = asset.resolve_continuous_futures_contract(reference_date=datetime(2025, 12, 31))
        self.assertEqual(contract, 'ESH26')

        contract = asset.resolve_continuous_futures_contract(reference_date=datetime(2026, 1, 1))
        self.assertEqual(contract, 'ESH26')

        contract = asset.resolve_continuous_futures_contract(reference_date=datetime(2025, 12, 14))
        self.assertEqual(contract, 'ESZ25')

    def test_different_symbol_formats(self):
        """Test continuous futures resolution with different symbol formats."""
        test_symbols = ["MES", "ES", "NQ", "RTY", "CL", "GC", "SI"]

        for symbol in test_symbols:
            contract = Asset(symbol, asset_type=Asset.AssetType.CONT_FUTURE).resolve_continuous_futures_contract(
                reference_date=self.test_date
            )
            self.assertTrue(contract.startswith(symbol))
            self.assertTrue(len(contract) >= len(symbol) + 3)

    def test_potential_contracts_uniqueness(self):
        """Test that potential contracts list contains no duplicates."""
        asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)
        contracts = asset.get_potential_futures_contracts(reference_date=self.test_date)
        
        # Check for uniqueness
        self.assertEqual(len(contracts), len(set(contracts)))
        
        # Should contain multiple format variations
        standard_formats = [c for c in contracts if '.' not in c and len(c.split(asset.symbol)[1]) == 3]
        dot_formats = [c for c in contracts if '.' in c]
        full_year_formats = [c for c in contracts if '.' not in c and len(c.split(asset.symbol)[1]) > 3]
        
        self.assertGreater(len(standard_formats), 0)
        self.assertGreater(len(dot_formats), 0) 
        self.assertGreater(len(full_year_formats), 0)

    def test_quarterly_contract_preference(self):
        """Test that quarterly contracts (Mar, Jun, Sep, Dec) are preferred."""
        asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)
        contracts = asset.get_potential_futures_contracts(reference_date=self.test_date)
        
        # Extract month codes from contracts
        quarterly_months = {'H', 'M', 'U', 'Z'}  # Mar, Jun, Sep, Dec
        
        # First several contracts should be quarterly
        first_few_contracts = contracts[:6]  # Check first 6 contracts
        quarterly_count = 0
        
        for contract in first_few_contracts:
            # Extract month code (last 3 characters: month + 2-digit year)
            if len(contract) >= 3:
                month_code = contract[-3]
                if month_code in quarterly_months:
                    quarterly_count += 1
        
        # At least half should be quarterly contracts
        self.assertGreaterEqual(quarterly_count, 3)

    def test_month_progression_logic(self):
        """Test that month selection follows proper logic with expiration-aware rollover."""
        asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)
        
        # Test each month and verify the logic accounts for 3rd Friday expiration
        # Rollover happens on 15th of expiry month to avoid expired contracts
        test_cases = [
            # (month, expected_front_month_quarter_code)
            (1, 'H'),   # Jan -> Mar (H) - before rollover
            (2, 'H'),   # Feb -> Mar (H) - before rollover  
            (3, 'M'),   # Mar 15 -> Jun (M) - after rollover (Mar expires ~21st)
            (4, 'M'),   # Apr -> Jun (M) - before rollover
            (5, 'M'),   # May -> Jun (M) - before rollover
            (6, 'U'),   # Jun 15 -> Sep (U) - after rollover (Jun expires ~20th)
            (7, 'U'),   # Jul -> Sep (U) - before rollover
            (8, 'U'),   # Aug -> Sep (U) - before rollover
            (9, 'Z'),   # Sep 15 -> Dec (Z) - after rollover (Sep expires ~19th)
            (10, 'Z'),  # Oct -> Dec (Z) - before rollover
            (11, 'Z'),  # Nov -> Dec (Z) - before rollover
            (12, 'H'),  # Dec 15 -> Mar next year (H) - after rollover (Dec expires ~19th)
        ]
        
        for month, expected_month_code in test_cases:
            contract = asset.resolve_continuous_futures_contract(
                reference_date=datetime(2025, month, 15)
            )
            actual_month_code = contract[-3]  # Third character from end
            self.assertEqual(actual_month_code, expected_month_code, 
                           f"Month {month} (15th) should resolve to {expected_month_code}, got {actual_month_code}")

    def test_contract_format_validation(self):
        """Test that generated contracts follow proper format conventions."""
        asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)

        contract = asset.resolve_continuous_futures_contract(reference_date=self.test_date)
        self.assertRegex(contract, r'^[A-Z]+[A-Z]\d{2}$')

        contracts = asset.get_potential_futures_contracts(reference_date=self.test_date)
        for contract in contracts:
            is_valid_format = (
                ('.' not in contract and contract[-3:].isalnum()) or
                ('.' in contract and contract.split('.')[1].isalnum()) or
                ('.' not in contract and contract[-4:].isdigit())
            )
            self.assertTrue(is_valid_format, f"Invalid contract format: {contract}")

    def test_integration_with_databento_helper(self):
        """Test integration between Asset class and DataBento helper."""
        continuous_asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)
        resolved_symbol = _format_futures_symbol_for_databento(continuous_asset, self.test_date)
        expected_symbol = continuous_asset.resolve_continuous_futures_contract(reference_date=self.test_date)

        self.assertEqual(expected_symbol, "MESU25")
        self.assertEqual(resolved_symbol, "MESU5")

    def test_quarterly_contract_consistency(self):
        """
        Test that contract resolution follows quarterly pattern with proper expiration handling.
        This test prevents regression of the original MES backtesting issue while ensuring
        we don't use expired contracts in live trading.
        """
        asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)
        
        # Test that contract resolution properly accounts for 3rd Friday expiration
        # Rollover happens on 15th of expiry month to avoid expired contracts
        quarterly_tests = [
            # Q1: Jan-Feb should resolve to March (H), Mar 15+ should roll to June (M)
            (datetime(2024, 1, 15), 'H24'),
            (datetime(2024, 2, 15), 'H24'),
            (datetime(2024, 3, 14), 'H24'),  # Before rollover
            (datetime(2024, 3, 15), 'M24'),  # After rollover (Mar expires ~21st)
            # Q2: Apr-May should resolve to June (M), Jun 15+ should roll to Sep (U)
            (datetime(2024, 4, 15), 'M24'),
            (datetime(2024, 5, 15), 'M24'),
            (datetime(2024, 6, 14), 'M24'),  # Before rollover
            (datetime(2024, 6, 15), 'U24'),  # After rollover (Jun expires ~20th)
            # Q3: Jul-Aug should resolve to September (U), Sep 15+ should roll to Dec (Z)
            (datetime(2024, 7, 15), 'U24'),
            (datetime(2024, 8, 15), 'U24'),
            (datetime(2024, 9, 14), 'U24'),  # Before rollover
            (datetime(2024, 9, 15), 'Z24'),  # After rollover (Sep expires ~19th)
            # Q4: Oct-Nov should resolve to December (Z), Dec 15+ should roll to Mar next year (H)
            (datetime(2024, 10, 15), 'Z24'),
            (datetime(2024, 11, 15), 'Z24'),
            (datetime(2024, 12, 14), 'Z24'),  # Before rollover
            (datetime(2024, 12, 15), 'H25'),  # After rollover (Dec expires ~19th)
        ]
        
        for test_date, expected_suffix in quarterly_tests:
            contract = asset.resolve_continuous_futures_contract(reference_date=test_date)
            expected_contract = f"MES{expected_suffix}"
            self.assertEqual(
                contract,
                expected_contract,
                f"Date {test_date.strftime('%Y-%m-%d')} should resolve to {expected_contract}, got {contract}",
            )

    def test_expiration_edge_cases(self):
        """
        Test contract resolution around actual futures expiration dates.
        This ensures we properly avoid expired contracts.
        """
        asset = Asset("ES", asset_type=Asset.AssetType.CONT_FUTURE)
        
        # Test around March 2025 expiration (3rd Friday is March 21st)
        test_cases = [
            (datetime(2025, 3, 14), 'ESH25'),  # Before rollover - still March
            (datetime(2025, 3, 15), 'ESM25'),  # Rollover day - move to June
            (datetime(2025, 3, 21), 'ESM25'),  # Actual expiry day - already rolled
            (datetime(2025, 3, 22), 'ESM25'),  # After expiry - definitely rolled
            
            # Test around June 2025 expiration (3rd Friday is June 20th) 
            (datetime(2025, 6, 14), 'ESM25'),  # Before rollover - still June
            (datetime(2025, 6, 15), 'ESU25'),  # Rollover day - move to September
            (datetime(2025, 6, 20), 'ESU25'),  # Actual expiry day - already rolled
            
            # Test around December 2025 expiration (3rd Friday is December 19th)
            (datetime(2025, 12, 14), 'ESZ25'),  # Before rollover - still December
            (datetime(2025, 12, 15), 'ESH26'),  # Rollover day - move to March next year
            (datetime(2025, 12, 19), 'ESH26'),  # Actual expiry day - already rolled
        ]
        
        for test_date, expected_contract in test_cases:
            contract = asset.resolve_continuous_futures_contract(reference_date=test_date)
            self.assertEqual(
                contract,
                expected_contract,
                f"Date {test_date.strftime('%Y-%m-%d')} should resolve to {expected_contract}, got {contract}",
            )


if __name__ == '__main__':
    # Run the tests
    unittest.main(verbosity=2)
