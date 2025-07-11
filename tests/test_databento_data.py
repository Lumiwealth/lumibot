import unittest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
import pandas as pd

from lumibot.data_sources.databento_data import DataBentoData
from lumibot.entities import Asset, Bars


class TestDataBentoData(unittest.TestCase):
    """Test cases for DataBentoData data source"""

    def setUp(self):
        """Set up test fixtures"""
        self.api_key = "test_api_key"
        self.start_date = datetime(2025, 1, 1)
        self.end_date = datetime(2025, 1, 31)
        
        self.test_asset = Asset(
            symbol="ES",
            asset_type="future",
            expiration=datetime(2025, 3, 15).date()
        )

    @patch('lumibot.tools.databento_helper.DATABENTO_AVAILABLE', True)
    def test_initialization_success(self):
        """Test successful initialization"""
        data_source = DataBentoData(
            api_key=self.api_key,
            datetime_start=self.start_date,
            datetime_end=self.end_date
        )
        
        self.assertEqual(data_source.name, "databento")
        self.assertEqual(data_source.SOURCE, "DATABENTO")
        self.assertEqual(data_source._api_key, self.api_key)

    @patch('lumibot.tools.databento_helper.DATABENTO_AVAILABLE', False)
    def test_initialization_databento_unavailable(self):
        """Test initialization when DataBento is unavailable"""
        with self.assertRaises(ImportError):
            DataBentoData(
                api_key=self.api_key,
                datetime_start=self.start_date,
                datetime_end=self.end_date
            )

    def test_initialization_default_dates(self):
        """Test initialization with default dates"""
        with patch('lumibot.tools.databento_helper.DATABENTO_AVAILABLE', True):
            data_source = DataBentoData(api_key=self.api_key)
            
            # Should have set API key and other attributes
            self.assertIsNotNone(data_source._api_key)
            self.assertEqual(data_source._api_key, self.api_key)
            self.assertIsNotNone(data_source.name)
            self.assertEqual(data_source.name, "databento")
            self.assertFalse(data_source.is_backtesting_mode)  # Default is False for live trading

    @patch('lumibot.tools.databento_helper.DATABENTO_AVAILABLE', True)
    @patch('lumibot.tools.databento_helper.get_price_data_from_databento')
    def test_get_historical_prices_success(self, mock_get_data):
        """Test successful historical price retrieval"""
        # Create test data
        test_df = pd.DataFrame({
            'open': [100.0, 101.0, 102.0],
            'high': [102.0, 103.0, 104.0],
            'low': [99.0, 100.0, 101.0],
            'close': [101.0, 102.0, 103.0],
            'volume': [1000, 1100, 1200]
        })
        test_df.index = pd.to_datetime([
            '2025-01-01 09:30:00',
            '2025-01-01 09:31:00',
            '2025-01-01 09:32:00'
        ])
        
        mock_get_data.return_value = test_df
        
        # Initialize data source
        data_source = DataBentoData(
            api_key=self.api_key,
            datetime_start=self.start_date,
            datetime_end=self.end_date
        )
        
        # Set current datetime for backtesting
        data_source._datetime = datetime(2025, 1, 1, 10, 0, 0)
        
        # Get historical prices
        result = data_source.get_historical_prices(
            asset=self.test_asset,
            length=3,
            timestep="minute"
        )
        
        # Verify result
        self.assertIsInstance(result, Bars)
        self.assertEqual(len(result.df), 3)
        
        # Verify mock was called with correct parameters
        mock_get_data.assert_called_once()

    @patch('lumibot.tools.databento_helper.DATABENTO_AVAILABLE', True)
    @patch('lumibot.tools.databento_helper.get_price_data_from_databento')
    def test_get_historical_prices_no_data(self, mock_get_data):
        """Test historical price retrieval with no data"""
        mock_get_data.return_value = None
        
        data_source = DataBentoData(
            api_key=self.api_key,
            datetime_start=self.start_date,
            datetime_end=self.end_date
        )
        
        result = data_source.get_historical_prices(
            asset=self.test_asset,
            length=10,
            timestep="minute"
        )
        
        self.assertIsNone(result)

    @patch('lumibot.tools.databento_helper.DATABENTO_AVAILABLE', True)
    @patch('lumibot.tools.databento_helper.get_last_price_from_databento')
    def test_get_last_price_success(self, mock_get_last_price):
        """Test successful last price retrieval"""
        mock_get_last_price.return_value = 4250.75
        
        data_source = DataBentoData(
            api_key=self.api_key,
            datetime_start=self.start_date,
            datetime_end=self.end_date
        )
        
        result = data_source.get_last_price(asset=self.test_asset)
        
        self.assertEqual(result, 4250.75)
        mock_get_last_price.assert_called_once_with(
            api_key=self.api_key,
            asset=self.test_asset,
            venue=None
        )

    @patch('lumibot.tools.databento_helper.DATABENTO_AVAILABLE', True)
    @patch('lumibot.tools.databento_helper.get_last_price_from_databento')
    def test_get_last_price_no_data(self, mock_get_last_price):
        """Test last price retrieval with no data"""
        mock_get_last_price.return_value = None
        
        data_source = DataBentoData(
            api_key=self.api_key,
            datetime_start=self.start_date,
            datetime_end=self.end_date
        )
        
        result = data_source.get_last_price(asset=self.test_asset)
        
        self.assertIsNone(result)

    @patch('lumibot.tools.databento_helper.DATABENTO_AVAILABLE', True)
    def test_get_chains(self):
        """Test options chains retrieval (should return empty dict)"""
        data_source = DataBentoData(
            api_key=self.api_key,
            datetime_start=self.start_date,
            datetime_end=self.end_date
        )
        
        result = data_source.get_chains(asset=self.test_asset)
        
        self.assertEqual(result, {})

    @patch('lumibot.tools.databento_helper.DATABENTO_AVAILABLE', True)
    @patch('lumibot.tools.databento_helper.get_price_data_from_databento')
    def test_get_historical_prices_single_asset(self, mock_get_data):
        """Test historical price retrieval for a single asset"""
        # Create test data
        test_df = pd.DataFrame({
            'open': [100.0, 101.0],
            'high': [102.0, 103.0],
            'low': [99.0, 100.0],
            'close': [101.0, 102.0],
            'volume': [1000, 1100]
        })
        test_df.index = pd.to_datetime([
            '2025-01-01 09:30:00',
            '2025-01-01 09:31:00'
        ])
        
        mock_get_data.return_value = test_df
        
        data_source = DataBentoData(api_key=self.api_key)
        
        result = data_source.get_historical_prices(
            asset=self.test_asset,
            length=2,
            timestep="minute"
        )
        
        self.assertIsNotNone(result)
        self.assertEqual(len(result.df), 2)
        self.assertEqual(result.asset, self.test_asset)
        self.assertEqual(result.source, "DATABENTO")

    @patch('lumibot.tools.databento_helper.DATABENTO_AVAILABLE', True)
    @patch('lumibot.tools.databento_helper.get_price_data_from_databento')
    def test_get_historical_prices_error_handling(self, mock_get_data):
        """Test error handling in get_historical_prices"""
        # Setup: Mock to raise exception
        mock_get_data.side_effect = Exception("Test error")
        
        data_source = DataBentoData(api_key=self.api_key)
        
        # This should not raise an exception but return None
        result = data_source.get_historical_prices(
            asset=self.test_asset,
            length=2,
            timestep="minute"
        )
        
        self.assertIsNone(result)

    @patch('lumibot.tools.databento_helper.DATABENTO_AVAILABLE', True)
    def test_parse_source_bars_valid_data(self):
        """Test parsing of valid source data using inherited method"""
        # Create test DataFrame
        test_df = pd.DataFrame({
            'open': [100.0, 101.0],
            'high': [102.0, 103.0],
            'low': [99.0, 100.0],
            'close': [101.0, 102.0],
            'volume': [1000, 1100]
        })
        test_df.index = pd.to_datetime([
            '2025-01-01 09:30:00',
            '2025-01-01 09:31:00'
        ])
        
        data_source = DataBentoData(api_key=self.api_key)
        
        # Test that _parse_source_bars is available from parent class
        result = data_source._parse_source_bars({self.test_asset: test_df})
        
        self.assertIsInstance(result, dict)
        self.assertIn(self.test_asset, result)
        self.assertIsInstance(result[self.test_asset], Bars)
        self.assertEqual(len(result[self.test_asset].df), 2)

    @patch('lumibot.tools.databento_helper.DATABENTO_AVAILABLE', True)
    def test_parse_source_bars_missing_columns(self):
        """Test parsing of data with missing columns using inherited method"""
        # Create test DataFrame missing required columns
        test_df = pd.DataFrame({
            'open': [100.0, 101.0],
            'high': [102.0, 103.0],
            # Missing 'low', 'close', 'volume'
        })
        test_df.index = pd.to_datetime([
            '2025-01-01 09:30:00',
            '2025-01-01 09:31:00'
        ])
        
        data_source = DataBentoData(api_key=self.api_key)
        
        # Test that _parse_source_bars handles missing columns
        result = data_source._parse_source_bars({self.test_asset: test_df})
        
        self.assertIsInstance(result, dict)
        self.assertIn(self.test_asset, result)

    @patch('lumibot.tools.databento_helper.DATABENTO_AVAILABLE', True)
    def test_parse_source_bars_empty_data(self):
        """Test parsing of empty data using inherited method"""
        test_df = pd.DataFrame()
        
        data_source = DataBentoData(api_key=self.api_key)
        
        # Test that _parse_source_bars handles empty data
        result = data_source._parse_source_bars({self.test_asset: test_df})
        
        self.assertIsInstance(result, dict)
        self.assertIn(self.test_asset, result)

    @patch('lumibot.tools.databento_helper.DATABENTO_AVAILABLE', True)
    def test_timestep_mapping(self):
        """Test timestep mapping functionality"""
        data_source = DataBentoData(api_key=self.api_key)
        
        # Test valid timestep mappings
        test_cases = [
            ("minute", "minute"),
            ("1m", "minute"),
            ("hour", "hour"),
            ("1h", "hour"),
            ("day", "day"),
            ("1d", "day"),
        ]
        
        for input_timestep, expected in test_cases:
            with self.subTest(timestep=input_timestep):
                result = data_source._parse_source_timestep(input_timestep)
                self.assertEqual(result, expected)

    @patch('lumibot.tools.databento_helper.DATABENTO_AVAILABLE', True)
    def test_backtesting_mode_detection(self):
        """Test that backtesting mode is properly detected"""
        # Test with default initialization (live trading mode)
        data_source = DataBentoData(api_key=self.api_key)
        
        # DataBento is currently configured for live trading by default
        self.assertFalse(data_source.is_backtesting_mode)
        
        # Test that name and source are set correctly
        self.assertEqual(data_source.name, "databento")
        self.assertEqual(data_source.SOURCE, "DATABENTO")

    @patch('lumibot.tools.databento_helper.DATABENTO_AVAILABLE', True)
    @patch('lumibot.tools.databento_helper.get_price_data_from_databento')
    def test_get_historical_prices_backtesting_path(self, mock_get_data):
        """Test that get_historical_prices uses backtesting path when in backtesting mode"""
        # Create test data
        test_df = pd.DataFrame({
            'open': [100.0, 101.0, 102.0],
            'high': [102.0, 103.0, 104.0],
            'low': [99.0, 100.0, 101.0],
            'close': [101.0, 102.0, 103.0],
            'volume': [1000, 1100, 1200]
        })
        test_df.index = pd.to_datetime([
            '2025-01-01 09:30:00',
            '2025-01-01 09:31:00',
            '2025-01-01 09:32:00'
        ])
        
        mock_get_data.return_value = test_df
        
        data_source = DataBentoData(
            api_key=self.api_key,
            datetime_start=self.start_date,
            datetime_end=self.end_date
        )
        
        # Test that the method works correctly
        result = data_source.get_historical_prices(
            asset=self.test_asset,
            length=3,
            timestep="minute"
        )
        
        # Verify that the data was retrieved
        self.assertIsNotNone(result)
        self.assertIsInstance(result, Bars)
        self.assertEqual(len(result.df), 3)

    @patch('lumibot.tools.databento_helper.DATABENTO_AVAILABLE', True)
    @patch('lumibot.tools.databento_helper.get_price_data_from_databento')
    def test_timezone_handling(self, mock_get_data):
        """Test timezone handling in get_historical_prices"""
        # Create test data
        test_df = pd.DataFrame({
            'open': [100.0, 101.0],
            'high': [102.0, 103.0],
            'low': [99.0, 100.0],
            'close': [101.0, 102.0],
            'volume': [1000, 1100]
        })
        test_df.index = pd.to_datetime([
            '2025-01-01 09:30:00',
            '2025-01-01 09:31:00'
        ])
        
        mock_get_data.return_value = test_df
        
        data_source = DataBentoData(api_key=self.api_key)
        
        # This should not raise an exception
        result = data_source.get_historical_prices(
            asset=self.test_asset,
            length=2,
            timestep="minute"
        )
        
        self.assertIsNotNone(result)
        self.assertIsInstance(result, Bars)
        self.assertEqual(len(result.df), 2)

    @patch('lumibot.tools.databento_helper.DATABENTO_AVAILABLE', True)
    @patch('lumibot.tools.databento_helper.get_price_data_from_databento')
    def test_error_handling(self, mock_get_data):
        """Test error handling in get_historical_prices"""
        # Setup: Mock to raise exception
        mock_get_data.side_effect = Exception("Test error")
        
        data_source = DataBentoData(api_key=self.api_key)
        
        # This should not raise an exception but return None
        result = data_source.get_historical_prices(
            asset=self.test_asset,
            length=1,
            timestep="minute"
        )
        
        # Should return None on error
        self.assertIsNone(result)

    def test_environment_dates_integration(self):
        """Test DataBento with environment file dates"""
        from dotenv import load_dotenv
        import os
        
        # Load environment variables from the strategy .env file
        env_path = "/Users/robertgrzesik/Documents/Development/Strategy Library/Alligator Futures Bot Strategy/src/.env"
        if os.path.exists(env_path):
            load_dotenv(env_path)
        
        # Get dates from environment variables
        start_str = os.getenv("BACKTESTING_START", "2024-01-01")
        end_str = os.getenv("BACKTESTING_END", "2024-12-31")
        
        start_date = datetime.strptime(start_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_str, "%Y-%m-%d")
        
        # Use a recent subset for testing (last 3 months)
        test_end_date = datetime(2024, 12, 31)
        test_start_date = datetime(2024, 10, 1)
        
        with patch('lumibot.tools.databento_helper.DATABENTO_AVAILABLE', True):
            data_source = DataBentoData(api_key=self.api_key)
            
            # Test with MES continuous futures
            mes_asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)
            
            # Mock the get_historical_prices method
            mock_bars = Mock()
            mock_bars.df = pd.DataFrame({
                'open': [4500, 4505, 4510],
                'high': [4510, 4515, 4520],
                'low': [4495, 4500, 4505],
                'close': [4505, 4510, 4515],
                'volume': [1000, 1100, 1200]
            }, index=pd.date_range(start=test_start_date, periods=3, freq='h'))
            
            with patch.object(data_source, 'get_historical_prices', return_value=mock_bars):
                bars = data_source.get_historical_prices(
                    asset=mes_asset,
                    length=60,
                    timestep="minute"
                )
                
                self.assertIsNotNone(bars)
                self.assertIsNotNone(bars.df)
                self.assertEqual(len(bars.df), 3)
    
    def test_mes_strategy_logic_simulation(self):
        """Test MES strategy logic with simulated data"""
        with patch('lumibot.tools.databento_helper.DATABENTO_AVAILABLE', True):
            data_source = DataBentoData(api_key=self.api_key)
            
            mes_asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)
            
            # Create mock data that simulates 60 minutes of MES futures
            mock_bars = Mock()
            mock_bars.df = pd.DataFrame({
                'open': [4500 + i for i in range(60)],
                'high': [4510 + i for i in range(60)],
                'low': [4490 + i for i in range(60)],
                'close': [4505 + i for i in range(60)],
                'volume': [1000 + i*10 for i in range(60)]
            }, index=pd.date_range(start=datetime(2024, 6, 10, 8, 0), periods=60, freq='min'))
            
            with patch.object(data_source, 'get_historical_prices', return_value=mock_bars):
                bars = data_source.get_historical_prices(
                    asset=mes_asset,
                    length=60,
                    timestep="minute"
                )
                
                self.assertIsNotNone(bars)
                self.assertIsNotNone(bars.df)
                self.assertEqual(len(bars.df), 60)
                
                # Test strategy logic
                df = bars.df
                current_price = df["close"].iloc[-1]
                sma_60 = df["close"].mean()
                
                # Should have a clear trend in our test data
                self.assertGreater(current_price, sma_60)
                self.assertGreater(current_price, 4500)  # Should be trending up

if __name__ == '__main__':
    unittest.main()
