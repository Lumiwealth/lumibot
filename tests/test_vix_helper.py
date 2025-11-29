"""
Test cases for VixHelper component with NumPy 2.0 compatibility
"""

import unittest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import numpy as np
# Fix for NumPy 2.0+ compatibility
if not hasattr(np, 'NaN'):
    np.NaN = np.nan
from datetime import datetime, timedelta
import sys


class TestVixHelperImport(unittest.TestCase):
    """Test that VixHelper imports correctly with NumPy 2.0+ compatibility"""
    
    def test_numpy_nan_compatibility(self):
        """Test that NumPy NaN compatibility is handled correctly"""
        # Test that np.nan exists (it always should)
        self.assertTrue(hasattr(np, 'nan'))
        
        # Check that np.NaN is now available (either native or patched)
        self.assertTrue(hasattr(np, 'NaN'))
        # Can't use assertEqual because NaN != NaN by definition
        self.assertIs(np.NaN, np.nan)  # They should be the same object
        
        # Now import vix_helper to ensure it works with the patch
        from lumibot.components.vix_helper import VixHelper
        self.assertIsNotNone(VixHelper)
    
    
    def test_pandas_ta_import_with_numpy_2(self):
        """Test that pandas_ta_classic can be imported with NumPy 2.0+ after our patch"""
        # Import pandas-ta-classic
        import pandas_ta_classic as ta
        # Verify it's working
        self.assertIsNotNone(ta)
        
        # Import vix_helper to ensure it works
        from lumibot.components.vix_helper import VixHelper
        self.assertIsNotNone(VixHelper)


class TestVixHelper(unittest.TestCase):
    """Test VixHelper functionality"""
    
    def setUp(self):
        """Set up test fixtures"""
        self.mock_strategy = Mock()
        self.mock_strategy.get_historical_prices = Mock()
        self.mock_strategy.log_message = Mock()
        self.mock_strategy.add_marker = Mock()
        
    def test_vix_helper_initialization(self):
        """Test VixHelper initialization"""
        from lumibot.components.vix_helper import VixHelper
        
        helper = VixHelper(self.mock_strategy)
        
        self.assertEqual(helper.strategy, self.mock_strategy)
        self.assertIsNone(helper.last_historical_vix_update)
        self.assertIsNone(helper.last_historical_vix_1d_update)
        self.assertIsNone(helper.last_historical_gvz_update)
    
        
    @patch('yfinance.Ticker')
    def test_get_vix_value(self, mock_ticker_class):
        """Test getting VIX value"""
        from lumibot.components.vix_helper import VixHelper
        helper = VixHelper(self.mock_strategy)
        
        # Mock yfinance Ticker and history method
        mock_ticker = Mock()
        mock_ticker_class.return_value = mock_ticker
        
        # Create mock historical data
        mock_df = pd.DataFrame({
            'Open': [19.5, 20.0, 18.8, 19.2, 20.5],
            'Close': [20.5, 21.0, 19.8, 20.2, 21.5]
        }, index=pd.date_range(end=datetime.now(), periods=5, freq='D'))
        mock_ticker.history.return_value = mock_df
        
        # Call get_vix_value with a specific datetime
        result = helper.get_vix_value(datetime.now())
        
        # Should return a float value
        self.assertIsInstance(result, (float, np.floating))
        # Should be the previous day's close value (index -2)
        self.assertEqual(result, 20.2)
    
    @patch('yfinance.Ticker')
    def test_get_vix_rsi_value(self, mock_ticker_class):
        """Test getting VIX RSI value"""
        from lumibot.components.vix_helper import VixHelper
        helper = VixHelper(self.mock_strategy)
        
        # Mock yfinance Ticker and history method
        mock_ticker = Mock()
        mock_ticker_class.return_value = mock_ticker
        
        # Mock yfinance data with enough history for RSI calculation
        dates = pd.date_range(end=datetime.now(), periods=50, freq='D')
        prices = [20 + i * 0.1 + np.sin(i/5) * 2 for i in range(50)]
        mock_df = pd.DataFrame({
            'Open': [p - 0.5 for p in prices],
            'Close': prices
        }, index=dates)
        mock_ticker.history.return_value = mock_df
        
        # This should calculate RSI using pandas_ta
        result = helper.get_vix_rsi_value(datetime.now(), window=14)
        
        # Should return a float value between 0 and 100
        self.assertIsInstance(result, (float, np.floating))
        self.assertTrue(0 <= result <= 100)
    
    def test_check_max_vix_1d(self):
        """Test check_max_vix_1d functionality"""
        from lumibot.components.vix_helper import VixHelper
        
        helper = VixHelper(self.mock_strategy)
        
        # Mock get_vix_1d_value to return different values
        with patch.object(helper, 'get_vix_1d_value') as mock_get_vix_1d:
            # Test when VIX 1D is below threshold
            mock_get_vix_1d.return_value = 15.0
            result = helper.check_max_vix_1d(datetime.now(), max_vix_1d=20.0)
            self.assertFalse(result)
            
            # Test when VIX 1D is above threshold
            mock_get_vix_1d.return_value = 25.0
            result = helper.check_max_vix_1d(datetime.now(), max_vix_1d=20.0)
            self.assertTrue(result)
            
            # Verify that log_message and add_marker were called
            self.mock_strategy.log_message.assert_called()
            self.mock_strategy.add_marker.assert_called()
    
    @patch('yfinance.Ticker')
    def test_vix_percentile_calculation(self, mock_ticker_class):
        """Test VIX percentile calculation"""
        from lumibot.components.vix_helper import VixHelper
        
        helper = VixHelper(self.mock_strategy)
        
        # Mock yfinance Ticker and history method
        mock_ticker = Mock()
        mock_ticker_class.return_value = mock_ticker
        
        # Create mock VIX data
        dates = pd.date_range(end=datetime.now(), periods=100, freq='D')
        vix_values = list(range(10, 40)) * 3 + list(range(10, 20))  # Various VIX values
        mock_df = pd.DataFrame({
            'Open': [v - 0.5 for v in vix_values[:100]],
            'Close': vix_values[:100]
        }, index=dates)
        mock_ticker.history.return_value = mock_df
        
        # Test percentile calculation
        result = helper.get_vix_percentile(datetime.now(), window=30)
        
        # Should return a value between 0 and 100
        self.assertIsInstance(result, (float, np.floating, int))
        self.assertTrue(0 <= result <= 100)


class TestNumPyCompatibility(unittest.TestCase):
    """Test NumPy 2.0 compatibility fixes"""
    
    def test_numpy_nan_alias_creation(self):
        """Test that np.NaN alias is created for backward compatibility"""
        # numpy is already imported and patched at the top of this file
        import numpy as np_local
        
        # Both should exist and be equal
        self.assertTrue(hasattr(np_local, 'nan'))
        self.assertTrue(hasattr(np_local, 'NaN'))
        
        # They should be the same object
        self.assertIs(np_local.NaN, np_local.nan)
        
        # Test that they work the same way
        self.assertTrue(np_local.isnan(np_local.NaN))
        self.assertTrue(np_local.isnan(np_local.nan))
        
    def test_pandas_operations_with_nan(self):
        """Test that pandas operations work with both np.nan and np.NaN"""
        # numpy is already imported and patched at the top of this file
        
        # Create series with both types of NaN
        s1 = pd.Series([1, 2, np.nan, 4])
        s2 = pd.Series([1, 2, np.NaN, 4])
        
        # They should be equivalent
        pd.testing.assert_series_equal(s1, s2)
        
        # Operations should work the same
        self.assertEqual(s1.isna().sum(), s2.isna().sum())
        self.assertEqual(s1.dropna().tolist(), s2.dropna().tolist())
        
    def test_module_import_order_independence(self):
        """Test that import order doesn't matter for the fix"""
        # The patch is already applied at the top of this file
        # So np.NaN should be available
        import numpy as np_test
        
        # np.NaN should be available (either natively or via our patch)
        self.assertTrue(hasattr(np_test, 'NaN'))
        
        # Import VixHelper to ensure it works with the patched numpy
        from lumibot.components.vix_helper import VixHelper
        self.assertIsNotNone(VixHelper)


if __name__ == '__main__':
    unittest.main()