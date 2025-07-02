import unittest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
import pandas as pd

from lumibot.tools import databento_helper
from lumibot.entities import Asset


class TestDataBentoHelper(unittest.TestCase):
    """Test cases for DataBento helper functions"""

    def setUp(self):
        """Set up test fixtures"""
        self.api_key = "test_api_key"
        self.test_asset_future = Asset(
            symbol="ES",
            asset_type="future",
            expiration=datetime(2025, 3, 15).date()
        )
        self.test_asset_stock = Asset(
            symbol="AAPL",
            asset_type="stock"
        )
        self.start_date = datetime(2025, 1, 1)
        self.end_date = datetime(2025, 1, 31)

    def test_format_futures_symbol_for_databento(self):
        """Test futures symbol formatting"""
        # Test continuous contract (no expiration)
        continuous_asset = Asset(symbol="ES", asset_type="future")
        result = databento_helper._format_futures_symbol_for_databento(continuous_asset)
        self.assertEqual(result, "ES")
        
        # Test specific contract with expiration
        result = databento_helper._format_futures_symbol_for_databento(self.test_asset_future)
        self.assertEqual(result, "ES202503")  # Should be YYYYMM format

    def test_determine_databento_dataset(self):
        """Test dataset determination logic"""
        # Test futures asset
        result = databento_helper._determine_databento_dataset(self.test_asset_future)
        self.assertEqual(result, "GLBX.MDP3")
        
        # Test stock asset
        result = databento_helper._determine_databento_dataset(self.test_asset_stock)
        self.assertEqual(result, "XNAS.ITCH")
        
        # Test with specific venue
        result = databento_helper._determine_databento_dataset(self.test_asset_future, venue="ICE")
        self.assertEqual(result, "IFEU.IMPACT")

    def test_determine_databento_schema(self):
        """Test schema mapping"""
        test_cases = [
            ("minute", "ohlcv-1m"),
            ("1m", "ohlcv-1m"),
            ("hour", "ohlcv-1h"),
            ("1h", "ohlcv-1h"),
            ("day", "ohlcv-1d"),
            ("1d", "ohlcv-1d"),
        ]
        
        for timestep, expected in test_cases:
            with self.subTest(timestep=timestep):
                result = databento_helper._determine_databento_schema(timestep)
                self.assertEqual(result, expected)

    def test_normalize_databento_dataframe(self):
        """Test DataFrame normalization"""
        # Create test DataFrame with DataBento-style columns
        test_data = {
            'ts_event': pd.to_datetime(['2025-01-01 09:30:00', '2025-01-01 09:31:00']),
            'open': [100.0, 101.0],
            'high': [102.0, 103.0],
            'low': [99.0, 100.0],
            'close': [101.0, 102.0],
            'volume': [1000, 1100]
        }
        df = pd.DataFrame(test_data)
        
        result = databento_helper._normalize_databento_dataframe(df)
        
        # Check that timestamp column became index
        self.assertIsInstance(result.index, pd.DatetimeIndex)
        
        # Check that required columns exist
        required_cols = ['open', 'high', 'low', 'close', 'volume']
        for col in required_cols:
            self.assertIn(col, result.columns)
        
        # Check data types
        for col in ['open', 'high', 'low', 'close', 'volume']:
            self.assertTrue(pd.api.types.is_numeric_dtype(result[col]))

    @patch('lumibot.tools.databento_helper.DATABENTO_AVAILABLE', True)
    def test_databento_client_initialization(self):
        """Test DataBento client initialization"""        
        client = databento_helper.DataBentoClient(
            api_key=self.api_key,
            timeout=30,
            max_retries=3
        )
        
        self.assertEqual(client.api_key, self.api_key)
        self.assertEqual(client.timeout, 30)
        self.assertEqual(client.max_retries, 3)

    @patch('lumibot.tools.databento_helper.DATABENTO_AVAILABLE', True)
    @patch('lumibot.tools.databento_helper.DataBentoClient')
    def test_get_price_data_from_databento_success(self, mock_client_class):
        """Test successful data retrieval"""
        # Mock client and response
        mock_client_instance = Mock()
        mock_client_class.return_value = mock_client_instance
        
        # Create mock DataFrame response
        mock_df = pd.DataFrame({
            'ts_event': pd.to_datetime(['2025-01-01 09:30:00', '2025-01-01 09:31:00']),
            'open': [100.0, 101.0],
            'high': [102.0, 103.0],
            'low': [99.0, 100.0],
            'close': [101.0, 102.0],
            'volume': [1000, 1100]
        })
        
        mock_client_instance.get_historical_data.return_value = mock_df
        
        # Mock cache functions
        with patch('lumibot.tools.databento_helper._load_cache', return_value=None), \
             patch('lumibot.tools.databento_helper._save_cache') as mock_save:
            
            result = databento_helper.get_price_data_from_databento(
                api_key=self.api_key,
                asset=self.test_asset_future,
                start=self.start_date,
                end=self.end_date,
                timestep="minute"
            )
            
            # Verify result
            self.assertIsNotNone(result)
            self.assertIsInstance(result, pd.DataFrame)
            self.assertEqual(len(result), 2)
            
            # Verify cache was called
            mock_save.assert_called_once()

    @patch('lumibot.tools.databento_helper.DATABENTO_AVAILABLE', False)
    def test_get_price_data_databento_unavailable(self):
        """Test behavior when DataBento package is unavailable"""
        result = databento_helper.get_price_data_from_databento(
            api_key=self.api_key,
            asset=self.test_asset_future,
            start=self.start_date,
            end=self.end_date
        )
        
        self.assertIsNone(result)

    @patch('lumibot.tools.databento_helper.DATABENTO_AVAILABLE', True)
    @patch('lumibot.tools.databento_helper._load_cache')
    def test_cache_loading(self, mock_load_cache):
        """Test that cache is loaded when available"""
        # Mock cached data
        cached_df = pd.DataFrame({
            'open': [100.0, 101.0],
            'high': [102.0, 103.0],
            'low': [99.0, 100.0],
            'close': [101.0, 102.0],
            'volume': [1000, 1100]
        })
        cached_df.index = pd.to_datetime(['2025-01-01 09:30:00', '2025-01-01 09:31:00'])
        
        mock_load_cache.return_value = cached_df
        
        result = databento_helper.get_price_data_from_databento(
            api_key=self.api_key,
            asset=self.test_asset_future,
            start=self.start_date,
            end=self.end_date,
            force_cache_update=False
        )
        
        # Should return cached data
        self.assertIsNotNone(result)
        pd.testing.assert_frame_equal(result, cached_df)

    def test_build_cache_filename(self):
        """Test cache filename generation"""
        filename = databento_helper._build_cache_filename(
            self.test_asset_future,
            self.start_date,
            self.end_date,
            "minute"
        )
        
        expected_name = "ES_20250315_minute_20250101_20250131.feather"
        self.assertEqual(filename.name, expected_name)


if __name__ == '__main__':
    unittest.main()
