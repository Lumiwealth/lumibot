import unittest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta, timezone
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
        # Test continuous futures (CONT_FUTURE) - should resolve to specific contract
        continuous_asset = Asset(symbol="ES", asset_type=Asset.AssetType.CONT_FUTURE)
        
        # Test with specific reference date (January 1, 2025) - should resolve to March contract
        reference_date = datetime(2025, 1, 1)
        result = databento_helper._format_futures_symbol_for_databento(continuous_asset, reference_date)
        self.assertIn("ESH25", result)
        
        # Test MES continuous futures with same reference date
        mes_continuous = Asset(symbol="MES", asset_type=Asset.AssetType.CONT_FUTURE)
        result = databento_helper._format_futures_symbol_for_databento(mes_continuous, reference_date)
        self.assertIn("MESH25", result)
        
        # Test regular future (no expiration) - should return raw symbol
        regular_future = Asset(symbol="ES", asset_type="future")
        result = databento_helper._format_futures_symbol_for_databento(regular_future)
        self.assertEqual(result, "ES")
        
        # Test specific contract with expiration (March 2025 = H25)
        result = databento_helper._format_futures_symbol_for_databento(self.test_asset_future)
        self.assertEqual(result, "ESH25")  # March 2025 = H25
        
        # Test another month (December 2024 = Z24)
        dec_asset = Asset(
            symbol="ES",
            asset_type="future", 
            expiration=datetime(2024, 12, 15).date()
        )
        result = databento_helper._format_futures_symbol_for_databento(dec_asset)
        self.assertEqual(result, "ESZ24")

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

    @patch('lumibot.tools.databento_helper.DATABENTO_AVAILABLE', True)
    @patch('lumibot.tools.databento_helper.DataBentoClient')
    def test_no_retry_logic_for_correct_symbol(self, mock_client_class):
        """Test that the function uses correct symbol/dataset without retry logic"""
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
             patch('lumibot.tools.databento_helper._save_cache'):
            
            # Test with MES continuous futures
            mes_asset = Asset(symbol="MES", asset_type="future")
            result = databento_helper.get_price_data_from_databento(
                api_key=self.api_key,
                asset=mes_asset,
                start=self.start_date,
                end=self.end_date,
                timestep="minute"
            )
            
            # Verify result
            self.assertIsNotNone(result)
            
            # Verify that get_historical_data was called exactly once with correct parameters
            mock_client_instance.get_historical_data.assert_called_once()
            call_args = mock_client_instance.get_historical_data.call_args
            
            # Check that it was called with the correct symbol (MES) and dataset (GLBX.MDP3)
            self.assertEqual(call_args[1]['symbols'], 'MES')
            self.assertEqual(call_args[1]['dataset'], 'GLBX.MDP3')
            self.assertEqual(call_args[1]['schema'], 'ohlcv-1m')

    def test_continuous_futures_integration_edge_cases(self):
        """Test edge cases for continuous futures integration with Asset class"""
        # Test with different symbols
        test_symbols = ["ES", "MES", "NQ", "MNQ", "RTY", "M2K", "CL", "GC"]
        
        for symbol in test_symbols:
            with self.subTest(symbol=symbol):
                continuous_asset = Asset(symbol=symbol, asset_type=Asset.AssetType.CONT_FUTURE)
                result = databento_helper._format_futures_symbol_for_databento(continuous_asset)
                
                # Should start with the symbol
                self.assertTrue(result.startswith(symbol))
                # Should have month code and year
                self.assertGreater(len(result), len(symbol))
                # Should not be the raw symbol (should be resolved)
                self.assertNotEqual(result, symbol)

    def test_futures_month_code_consistency(self):
        """Test that month codes are consistent between Asset class and DataBento helper"""
        # Test specific expiration dates
        test_cases = [
            (datetime(2025, 1, 15).date(), 'F'),  # January = F
            (datetime(2025, 3, 15).date(), 'H'),  # March = H  
            (datetime(2025, 6, 15).date(), 'M'),  # June = M
            (datetime(2025, 9, 15).date(), 'U'),  # September = U
            (datetime(2025, 12, 15).date(), 'Z'), # December = Z
        ]
        
        for expiration_date, expected_month_code in test_cases:
            with self.subTest(expiration=expiration_date):
                asset = Asset(
                    symbol="ES",
                    asset_type="future", 
                    expiration=expiration_date
                )
                result = databento_helper._format_futures_symbol_for_databento(asset)
                # Should be in format ES + month_code + year
                self.assertEqual(result[2], expected_month_code)  # Third character is month code

    @patch('lumibot.tools.databento_helper.DATABENTO_AVAILABLE', True)
    @patch('lumibot.tools.databento_helper.DataBentoClient')
    def test_error_handling_empty_dataframe(self, mock_client_class):
        """Test behavior when DataBento returns empty DataFrame"""
        mock_client_instance = Mock()
        mock_client_class.return_value = mock_client_instance
        
        # Return empty DataFrame
        empty_df = pd.DataFrame()
        mock_client_instance.get_historical_data.return_value = empty_df
        
        with patch('lumibot.tools.databento_helper._load_cache', return_value=None), \
             patch('lumibot.tools.databento_helper._save_cache'):
            
            result = databento_helper.get_price_data_from_databento(
                api_key=self.api_key,
                asset=self.test_asset_stock,
                start=self.start_date,
                end=self.end_date
            )
            
            # Should return None for empty DataFrame as per implementation
            self.assertIsNone(result)

    @patch('lumibot.tools.databento_helper.DATABENTO_AVAILABLE', True)
    @patch('lumibot.tools.databento_helper.DataBentoClient')
    def test_error_handling_api_exception(self, mock_client_class):
        """Test behavior when DataBento API raises an exception"""
        mock_client_instance = Mock()
        mock_client_class.return_value = mock_client_instance
        
        # Simulate API exception
        mock_client_instance.get_historical_data.side_effect = Exception("API Error")
        
        with patch('lumibot.tools.databento_helper._load_cache', return_value=None), \
             patch('lumibot.tools.databento_helper._save_cache'):
            
            result = databento_helper.get_price_data_from_databento(
                api_key=self.api_key,
                asset=self.test_asset_stock,
                start=self.start_date,
                end=self.end_date
            )
            
            # Should return None on exception
            self.assertIsNone(result)

    def test_dataset_selection_comprehensive(self):
        """Test dataset selection for various asset types and venues"""
        test_cases = [
            # (asset_type, venue, expected_dataset)
            ("future", None, "GLBX.MDP3"),
            ("future", "CME", "GLBX.MDP3"), 
            ("future", "ICE", "IFEU.IMPACT"),
            ("future", "EUREX", "GLBX.MDP3"),  # EUREX defaults to GLBX.MDP3
            ("stock", None, "XNAS.ITCH"),
            ("stock", "NASDAQ", "XNAS.ITCH"),
            ("stock", "NYSE", "XNAS.ITCH"),  # NYSE also defaults to XNAS.ITCH
        ]
        
        for asset_type, venue, expected_dataset in test_cases:
            with self.subTest(asset_type=asset_type, venue=venue):
                asset = Asset(symbol="TEST", asset_type=asset_type)
                result = databento_helper._determine_databento_dataset(asset, venue=venue)
                self.assertEqual(result, expected_dataset)

    def test_schema_mapping_comprehensive(self):
        """Test comprehensive schema mapping for all supported timesteps"""
        test_cases = [
            # Standard formats
            ("minute", "ohlcv-1m"),
            ("hour", "ohlcv-1h"), 
            ("day", "ohlcv-1d"),
            # Alternative formats
            ("1m", "ohlcv-1m"),
            ("1h", "ohlcv-1h"),
            ("1d", "ohlcv-1d"),
            ("1minute", "ohlcv-1m"),
            ("1hour", "ohlcv-1h"),
            ("1day", "ohlcv-1d"),
            # Edge cases that default to 1m
            ("m", "ohlcv-1m"),  # Defaults to 1m since not in mapping
            ("h", "ohlcv-1m"),  # Defaults to 1m since not in mapping  
            ("d", "ohlcv-1m"),  # Defaults to 1m since not in mapping
        ]
        
        for timestep, expected_schema in test_cases:
            with self.subTest(timestep=timestep):
                result = databento_helper._determine_databento_schema(timestep)
                self.assertEqual(result, expected_schema)

    def test_cache_filename_edge_cases(self):
        """Test cache filename generation for edge cases"""
        # Test with different asset types
        assets = [
            Asset(symbol="AAPL", asset_type="stock"),
            Asset(symbol="ES", asset_type="future", expiration=datetime(2025, 3, 15).date()),
            Asset(symbol="MES", asset_type=Asset.AssetType.CONT_FUTURE),
        ]
        
        for asset in assets:
            with self.subTest(asset=asset):
                filename = databento_helper._build_cache_filename(
                    asset,
                    self.start_date,
                    self.end_date,
                    "minute"
                )
                
                # Should be a valid filename
                self.assertTrue(filename.name.endswith('.feather'))
                self.assertNotIn('/', filename.name)  # No path separators
                self.assertNotIn('\\', filename.name)  # No path separators
                
                # Should contain asset symbol
                self.assertIn(asset.symbol, filename.name)

    def test_dataframe_normalization_edge_cases(self):
        """Test DataFrame normalization with various edge cases"""
        # Test with missing columns
        incomplete_df = pd.DataFrame({
            'ts_event': pd.to_datetime(['2025-01-01 09:30:00']),
            'open': [100.0],
            'close': [101.0],
            # Missing high, low, volume
        })
        
        result = databento_helper._normalize_databento_dataframe(incomplete_df)
        self.assertIsNotNone(result)
        
        # Test with extra columns
        extra_df = pd.DataFrame({
            'ts_event': pd.to_datetime(['2025-01-01 09:30:00']),
            'open': [100.0],
            'high': [102.0],
            'low': [99.0],
            'close': [101.0],
            'volume': [1000],
            'extra_column': ['extra_value'],  # Extra column
        })
        
        result = databento_helper._normalize_databento_dataframe(extra_df)
        self.assertIsNotNone(result)
        # Extra columns should be preserved
        self.assertIn('extra_column', result.columns)

    def test_continuous_futures_resolution(self):
        """Test that DataBento helper properly resolves continuous futures"""
        # Test that continuous futures use internal resolution logic
        continuous_asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)
        
        result = databento_helper._format_futures_symbol_for_databento(continuous_asset)
        
        # Should return a resolved contract with month code and year
        self.assertIn("MES", result)
        self.assertTrue(len(result) > 3)  # Should have month code and year
        
        # Test that specific futures don't use continuous resolution
        specific_asset = Asset("MES", asset_type="future", expiration=datetime(2025, 9, 15).date())
        
        with patch.object(Asset, 'resolve_continuous_futures_contract') as mock_resolve:
            result = databento_helper._format_futures_symbol_for_databento(specific_asset)
            
            # Should NOT have called the continuous futures method
            mock_resolve.assert_not_called()
            self.assertEqual(result, 'MESU25')  # Direct formatting

    @patch('lumibot.tools.databento_helper.Historical')
    def test_get_last_price_from_databento_success(self, mock_historical):
        """Test successful last price retrieval"""
        # Setup mock data
        mock_df = pd.DataFrame({
            'open': [5000.0, 5010.0],
            'high': [5020.0, 5030.0],
            'low': [4990.0, 5000.0],
            'close': [5010.0, 5025.0],
            'volume': [1000, 1200]
        })
        
        # Mock the DataBento client and its methods
        mock_client = MagicMock()
        mock_range_result = MagicMock()
        mock_range_result.end = pd.Timestamp('2025-07-01 00:00:00+00:00')
        mock_client.metadata.get_dataset_range.return_value = mock_range_result
        mock_client.timeseries.get_range.return_value = mock_df
        mock_historical.return_value = mock_client
        
        # Test asset
        asset = Asset("MESU5", asset_type=Asset.AssetType.FUTURE)
        
        # Call function
        result = databento_helper.get_last_price_from_databento(
            api_key="test_key",
            asset=asset
        )
        
        # Verify result (should be last close price)
        self.assertEqual(result, 5025.0)
        
        # Verify client methods were called
        mock_client.metadata.get_dataset_range.assert_called_once()
        mock_client.timeseries.get_range.assert_called_once()

    @patch('lumibot.tools.databento_helper.Historical')
    def test_get_last_price_from_databento_empty_data(self, mock_historical):
        """Test handling of empty data response"""
        # Setup mock to return empty DataFrame
        mock_df = pd.DataFrame()
        
        # Mock the DataBento client and metadata response
        mock_client = MagicMock()
        mock_range_result = MagicMock()
        mock_range_result.end = pd.Timestamp('2025-07-01 00:00:00+00:00')
        mock_client.metadata.get_dataset_range.return_value = mock_range_result
        mock_client.timeseries.get_range.return_value = mock_df
        mock_historical.return_value = mock_client
        
        asset = Asset("MESU5", asset_type=Asset.AssetType.FUTURE)
        
        result = databento_helper.get_last_price_from_databento(
            api_key="test_key",
            asset=asset
        )
        
        # Should return None for no data
        self.assertIsNone(result)

    @patch('lumibot.tools.databento_helper.Historical')
    def test_get_last_price_from_databento_no_data(self, mock_historical):
        """Test handling of None data response"""
        # Setup mock to return None
        
        # Mock the DataBento client and metadata response
        mock_client = MagicMock()
        mock_range_result = MagicMock()
        mock_range_result.end = pd.Timestamp('2025-07-01 00:00:00+00:00')
        mock_client.metadata.get_dataset_range.return_value = mock_range_result
        mock_client.timeseries.get_range.return_value = None
        mock_historical.return_value = mock_client
        
        asset = Asset("MESU5", asset_type=Asset.AssetType.FUTURE)
        
        result = databento_helper.get_last_price_from_databento(
            api_key="test_key",
            asset=asset
        )
        
        # Should return None for no data
        self.assertIsNone(result)

    @patch('lumibot.tools.databento_helper.Historical')
    def test_get_last_price_from_databento_exception(self, mock_historical):
        """Test handling of exception during data retrieval"""
        # Setup mock to raise exception
        mock_client = MagicMock()
        mock_range_result = MagicMock()
        mock_range_result.end = pd.Timestamp('2025-07-01 00:00:00+00:00')
        mock_client.metadata.get_dataset_range.return_value = mock_range_result
        mock_client.timeseries.get_range.side_effect = Exception("Test exception")
        mock_historical.return_value = mock_client
        
        asset = Asset("MESU5", asset_type=Asset.AssetType.FUTURE)
        
        result = databento_helper.get_last_price_from_databento(
            api_key="test_key",
            asset=asset
        )
        
        # Should return None when exception occurs
        self.assertIsNone(result)

    def test_timezone_aware_datetime_usage(self):
        """Test that the last price function uses timezone-aware datetime objects"""
        with patch('lumibot.tools.databento_helper.Historical') as mock_historical:
            mock_df = pd.DataFrame({
                'close': [5025.0]
            })
            
            # Mock the DataBento client and metadata response
            mock_client = MagicMock()
            mock_range_result = MagicMock()
            mock_range_result.end = pd.Timestamp('2025-07-01 00:00:00+00:00')
            mock_client.metadata.get_dataset_range.return_value = mock_range_result
            mock_client.timeseries.get_range.return_value = mock_df
            mock_historical.return_value = mock_client
            
            asset = Asset("MESU5", asset_type=Asset.AssetType.FUTURE)
            
            result = databento_helper.get_last_price_from_databento(
                api_key="test_key",
                asset=asset
            )
            
            # Should successfully return the price
            self.assertEqual(result, 5025.0)
            
            # Verify the function was called with timezone-aware datetimes
            call_args = mock_client.timeseries.get_range.call_args
            start_dt = call_args[1]['start']
            end_dt = call_args[1]['end']
            
            # Both should be timezone-aware (not naive)
            self.assertIsNotNone(start_dt.tzinfo, "start datetime should be timezone-aware")
            self.assertIsNotNone(end_dt.tzinfo, "end datetime should be timezone-aware")
            
            # Should be UTC timezone
            self.assertEqual(start_dt.tzinfo, timezone.utc, "start datetime should use UTC timezone")
            self.assertEqual(end_dt.tzinfo, timezone.utc, "end datetime should use UTC timezone")

    @patch('lumibot.tools.databento_helper.DATABENTO_AVAILABLE', True)
    @patch('lumibot.tools.databento_helper.DataBentoClient')
    def test_get_price_data_continuous_futures_resolution(self, mock_client_class):
        """Test that get_price_data_from_databento resolves continuous futures using internal logic"""
        
        # Mock the DataBento client
        mock_client = MagicMock()
        mock_client_class.return_value = mock_client
        
        # Mock the historical data method to return empty data (simulating no data found)
        mock_client.get_historical_data.return_value = None
        
        # Create continuous futures asset
        es_cont = Asset(symbol="ES", asset_type="cont_future")
        
        # Call the function
        result = databento_helper.get_price_data_from_databento(
            api_key="test_api_key",
            asset=es_cont,
            start=datetime(2025, 1, 1),
            end=datetime(2025, 1, 2),
            timestep="minute"
        )
        
        # Verify that DataBento client get_historical_data was called
        self.assertGreater(mock_client.get_historical_data.call_count, 0)
        
        # Verify the specific call was made with the resolved symbol
        call_args = mock_client.get_historical_data.call_args
        # Should contain the resolved contract (ESH5 for January 2025 using DataBento short year format)
        self.assertIn('ESH5', str(call_args))

    @patch('lumibot.tools.databento_helper.DATABENTO_AVAILABLE', True)
    @patch('lumibot.tools.databento_helper.Historical')
    def test_get_last_price_resolves_once_no_retry(self, mock_historical_class):
        """Test that get_last_price_from_databento resolves continuous futures once, no retry"""
        
        # Mock the DataBento Historical client
        mock_client = MagicMock()
        mock_historical_class.return_value = mock_client
        
        # Mock dataset range response
        mock_range_result = MagicMock()
        mock_range_result.end = pd.Timestamp('2025-07-01 00:00:00+00:00')
        mock_client.metadata.get_dataset_range.return_value = mock_range_result
        
        # Mock the timeseries data response (empty to simulate no data)
        mock_client.timeseries.get_range.return_value = None
        
        # Create continuous futures asset
        es_cont = Asset(symbol="ES", asset_type="cont_future")
        
        # Mock the asset's resolve method to return a specific contract
        with patch.object(es_cont, 'resolve_continuous_futures_contract', return_value='ESH25'):
            
            # Call the function
            result = databento_helper.get_last_price_from_databento(
                api_key="test_api_key",
                asset=es_cont
            )
            
            # Verify that resolve_continuous_futures_contract was called exactly once
            es_cont.resolve_continuous_futures_contract.assert_called_once()
            
            # Verify that DataBento client get_range was called exactly once
            # (no retry logic should mean only one call)
            self.assertEqual(mock_client.timeseries.get_range.call_count, 1)
            
            # Verify the specific call was made with the resolved symbol
            call_args = mock_client.timeseries.get_range.call_args
            self.assertIn('ESH5', call_args[1]['symbols'])  # symbols parameter should contain ESH5

    def test_no_get_potential_futures_contracts_usage(self):
        """Test that DataBento helper doesn't use the old get_potential_futures_contracts method"""
        import inspect
        
        # Get the source code of the databento_helper module
        source = inspect.getsource(databento_helper)
        
        # Verify that the old method is not used
        self.assertNotIn('get_potential_futures_contracts', source, 
                         "DataBento helper should not use get_potential_futures_contracts method")
        
        # Verify that resolve_continuous_futures_contract is used instead
        self.assertIn('resolve_continuous_futures_contract', source,
                      "DataBento helper should use resolve_continuous_futures_contract method")

    def test_market_calendar_spanning_sessions(self):
        """Test market calendar logic for sessions spanning multiple days."""
        from unittest.mock import MagicMock
        from lumibot.brokers.broker import Broker
        
        # Create a mock broker
        broker = MagicMock(spec=Broker)
        broker.market = "CME_Equity"
        
        # Mock the market_hours method to simulate CME futures schedule
        def mock_market_hours(close=True, next=False):
            if next:  # Friday's session
                if close:
                    return datetime(2025, 1, 10, 23, 0, tzinfo=timezone.utc)  # 6pm ET Fri
                else:
                    return datetime(2025, 1, 9, 23, 0, tzinfo=timezone.utc)   # 6pm ET Thu
            else:  # Thursday's session  
                if close:
                    return datetime(2025, 1, 9, 23, 0, tzinfo=timezone.utc)   # 6pm ET Thu
                else:
                    return datetime(2025, 1, 8, 23, 0, tzinfo=timezone.utc)   # 6pm ET Wed
        
        def mock_utc_to_local(utc_time):
            # Convert UTC to ET (UTC-5) and return as naive datetime
            from dateutil import tz
            return utc_time.replace(tzinfo=timezone.utc).astimezone(tz=tz.tzlocal()).replace(tzinfo=None)
        
        broker.market_hours.side_effect = mock_market_hours
        broker.utc_to_local.side_effect = mock_utc_to_local
        
        # Test Thursday 7pm ET (should be open - Friday's session)
        current_time = datetime(2025, 1, 9, 19, 0, 0)  # 7pm ET Thursday
        
        # Simulate the logic from is_market_open
        result_today = False
        result_tomorrow = False
        
        # Check today's session (Thursday)
        try:
            open_time_today = mock_utc_to_local(mock_market_hours(close=False, next=False))
            close_time_today = mock_utc_to_local(mock_market_hours(close=True, next=False))
            
            if (current_time >= open_time_today) and (close_time_today >= current_time):
                result_today = True
        except:
            result_today = False
        
        # Check tomorrow's session (Friday)
        try:
            open_time_tomorrow = mock_utc_to_local(mock_market_hours(close=False, next=True))
            close_time_tomorrow = mock_utc_to_local(mock_market_hours(close=True, next=True))
            
            if (current_time >= open_time_tomorrow) and (close_time_tomorrow >= current_time):
                result_tomorrow = True
        except:
            result_tomorrow = False
        
        is_open = result_today or result_tomorrow
        
        # 7pm Thursday should be market open (Friday's session started at 6pm Thursday)
        self.assertTrue(is_open, "CME futures should be open at 7pm Thursday ET (Friday's session)")
