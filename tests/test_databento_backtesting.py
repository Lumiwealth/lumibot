import unittest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
import pandas as pd

from lumibot.backtesting.databento_backtesting import DataBentoDataBacktesting
from lumibot.entities import Asset, Data


class TestDataBentoDataBacktesting(unittest.TestCase):
    """Test cases for DataBento backtesting implementation"""

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
        backtester = DataBentoDataBacktesting(
            datetime_start=self.start_date,
            datetime_end=self.end_date,
            api_key=self.api_key
        )
        
        self.assertEqual(backtester._api_key, self.api_key)
        # Note: Parent class converts datetime to timezone-aware, so check date portion
        self.assertEqual(backtester.datetime_start.date(), self.start_date.date())
        # Parent class subtracts 1 minute from datetime_end, so end date may be 1 day earlier
        expected_end_date = (self.end_date - timedelta(minutes=1)).date()
        self.assertEqual(backtester.datetime_end.date(), expected_end_date)

    @patch('lumibot.tools.databento_helper.DATABENTO_AVAILABLE', False)
    def test_initialization_databento_unavailable(self):
        """Test initialization when DataBento is unavailable"""
        with self.assertRaises(ImportError):
            DataBentoDataBacktesting(
                datetime_start=self.start_date,
                datetime_end=self.end_date,
                api_key=self.api_key
            )

    @patch('lumibot.tools.databento_helper.DATABENTO_AVAILABLE', True)
    @patch('lumibot.tools.databento_helper.get_price_data_from_databento')
    def test_update_pandas_data_success(self, mock_get_data):
        """Test successful pandas data update"""
        # Create test DataFrame
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
        
        backtester = DataBentoDataBacktesting(
            datetime_start=self.start_date,
            datetime_end=self.end_date,
            api_key=self.api_key
        )
        
        # Mock the get_start_datetime_and_ts_unit method
        with patch.object(backtester, 'get_start_datetime_and_ts_unit') as mock_get_start:
            mock_get_start.return_value = (self.start_date, "minute")
            
            # Call update method
            backtester._update_pandas_data(
                asset=self.test_asset,
                quote=None,
                length=10,
                timestep="minute"
            )
            
            # Verify data was stored
            search_asset = (self.test_asset, Asset("USD", "forex"))
            self.assertIn(search_asset, backtester.pandas_data)
            
            stored_data = backtester.pandas_data[search_asset]
            self.assertIsInstance(stored_data, Data)
            self.assertEqual(len(stored_data.df), 3)

    @patch('lumibot.tools.databento_helper.DATABENTO_AVAILABLE', True)
    @patch('lumibot.tools.databento_helper.get_price_data_from_databento')
    def test_update_pandas_data_no_data(self, mock_get_data):
        """Test pandas data update with no data returned"""
        mock_get_data.return_value = None
        
        backtester = DataBentoDataBacktesting(
            datetime_start=self.start_date,
            datetime_end=self.end_date,
            api_key=self.api_key
        )
        
        with patch.object(backtester, 'get_start_datetime_and_ts_unit') as mock_get_start:
            mock_get_start.return_value = (self.start_date, "minute")
            
            backtester._update_pandas_data(
                asset=self.test_asset,
                quote=None,
                length=10,
                timestep="minute"
            )
            
            # When DataBento returns None, the implementation fails during Data object creation
            # due to timezone handling, so no data gets stored
            search_asset = (self.test_asset, Asset("USD", "forex"))
            self.assertNotIn(search_asset, backtester.pandas_data)

    @patch('lumibot.tools.databento_helper.DATABENTO_AVAILABLE', True)
    @patch('lumibot.tools.databento_helper.get_price_data_from_databento')
    def test_update_pandas_data_existing_sufficient_data(self, mock_get_data):
        """Test pandas data update when sufficient data already exists"""
        # Create existing data
        existing_df = pd.DataFrame({
            'open': [100.0, 101.0],
            'high': [102.0, 103.0],
            'low': [99.0, 100.0],
            'close': [101.0, 102.0],
            'volume': [1000, 1100]
        })
        existing_df.index = pd.to_datetime([
            '2024-12-01 09:30:00',  # Much earlier date
            '2024-12-01 09:31:00'
        ])
        
        backtester = DataBentoDataBacktesting(
            datetime_start=self.start_date,
            datetime_end=self.end_date,
            api_key=self.api_key
        )
        
        # Pre-populate with existing data
        search_asset = (self.test_asset, Asset("USD", "forex"))
        existing_data = Data(
            self.test_asset,
            df=existing_df,
            timestep="minute",
            quote=Asset("USD", "forex")
        )
        backtester.pandas_data[search_asset] = existing_data
        
        with patch.object(backtester, 'get_start_datetime_and_ts_unit') as mock_get_start:
            mock_get_start.return_value = (self.start_date, "minute")
            
            backtester._update_pandas_data(
                asset=self.test_asset,
                quote=None,
                length=10,
                timestep="minute"
            )
            
            # Should have called DataBento API since existing data is too old
            mock_get_data.assert_called()

    @patch('lumibot.tools.databento_helper.DATABENTO_AVAILABLE', True)
    def test_get_last_price_from_cached_data(self):
        """Test getting last price from cached data"""
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
        
        backtester = DataBentoDataBacktesting(
            datetime_start=self.start_date,
            datetime_end=self.end_date,
            api_key=self.api_key
        )
        
        # Set current datetime
        backtester._datetime = datetime(2025, 1, 1, 9, 32, 0)
        
        # Pre-populate with data
        search_asset = (self.test_asset, Asset("USD", "forex"))
        data = Data(
            self.test_asset,
            df=test_df,
            timestep="minute",
            quote=Asset("USD", "forex")
        )
        backtester.pandas_data[search_asset] = data
        
        result = backtester.get_last_price(asset=self.test_asset)
        
        self.assertEqual(result, 103.0)  # Last close price

    @patch('lumibot.tools.databento_helper.DATABENTO_AVAILABLE', True)
    @patch('lumibot.tools.databento_helper.get_last_price_from_databento')
    def test_get_last_price_no_cached_data(self, mock_get_last_price):
        """Test getting last price when no cached data available"""
        mock_get_last_price.return_value = 4250.75
        
        backtester = DataBentoDataBacktesting(
            datetime_start=self.start_date,
            datetime_end=self.end_date,
            api_key=self.api_key
        )
        
        result = backtester.get_last_price(asset=self.test_asset)
        
        self.assertEqual(result, 4250.75)
        mock_get_last_price.assert_called_once()

    @patch('lumibot.tools.databento_helper.DATABENTO_AVAILABLE', True)
    def test_get_chains(self):
        """Test options chains retrieval (should return empty dict)"""
        backtester = DataBentoDataBacktesting(
            datetime_start=self.start_date,
            datetime_end=self.end_date,
            api_key=self.api_key
        )
        
        result = backtester.get_chains(asset=self.test_asset)
        
        self.assertEqual(result, {})

    @patch('lumibot.tools.databento_helper.DATABENTO_AVAILABLE', True)
    def test_get_bars_dict_success(self):
        """Test getting bars dictionary for multiple assets"""
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
        
        backtester = DataBentoDataBacktesting(
            datetime_start=self.start_date,
            datetime_end=self.end_date,
            api_key=self.api_key
        )
        
        # Set current datetime
        backtester._datetime = datetime(2025, 1, 1, 10, 0, 0)
        
        # Pre-populate with data
        search_asset = (self.test_asset, Asset("USD", "forex"))
        data = Data(
            self.test_asset,
            df=test_df,
            timestep="minute",
            quote=Asset("USD", "forex")
        )
        backtester.pandas_data[search_asset] = data
        
        # Mock _update_pandas_data to prevent API calls
        with patch.object(backtester, '_update_pandas_data'):
            result = backtester._get_bars_dict(
                assets=[self.test_asset],
                length=2,
                timestep="minute"
            )
            
            self.assertIsInstance(result, dict)
            self.assertIn(self.test_asset, result)
            self.assertIsNotNone(result[self.test_asset])
            self.assertEqual(len(result[self.test_asset]), 2)  # Should get last 2 bars

    @patch('lumibot.tools.databento_helper.DATABENTO_AVAILABLE', True)
    def test_get_bars_dict_with_timeshift(self):
        """Test getting bars dictionary with timeshift"""
        # Create test data
        test_df = pd.DataFrame({
            'open': [100.0, 101.0, 102.0, 103.0],
            'high': [102.0, 103.0, 104.0, 105.0],
            'low': [99.0, 100.0, 101.0, 102.0],
            'close': [101.0, 102.0, 103.0, 104.0],
            'volume': [1000, 1100, 1200, 1300]
        })
        test_df.index = pd.to_datetime([
            '2025-01-01 09:30:00',
            '2025-01-01 09:31:00',
            '2025-01-01 09:32:00',
            '2025-01-01 09:33:00'
        ])
        
        backtester = DataBentoDataBacktesting(
            datetime_start=self.start_date,
            datetime_end=self.end_date,
            api_key=self.api_key
        )
        
        # Set current datetime
        backtester._datetime = datetime(2025, 1, 1, 9, 33, 0)
        
        # Pre-populate with data
        search_asset = (self.test_asset, Asset("USD", "forex"))
        data = Data(
            self.test_asset,
            df=test_df,
            timestep="minute",
            quote=Asset("USD", "forex")
        )
        backtester.pandas_data[search_asset] = data
        
        # Mock _update_pandas_data to prevent API calls
        with patch.object(backtester, '_update_pandas_data'):
            # Apply 1-minute timeshift
            result = backtester._get_bars_dict(
                assets=[self.test_asset],
                length=2,
                timestep="minute",
                timeshift=timedelta(minutes=1)
            )
            
            self.assertIsInstance(result, dict)
            self.assertIn(self.test_asset, result)
            
            # With 1-minute timeshift, we should get data up to 09:32:00
            result_df = result[self.test_asset]
            self.assertEqual(len(result_df), 2)
            # Should be the data up to 09:32:00 (103.0 close), not 09:33:00 (104.0 close)
            self.assertEqual(result_df['close'].iloc[-1], 103.0)

    @patch('lumibot.tools.databento_helper.DATABENTO_AVAILABLE', True)
    def test_get_bars_dict_no_data(self):
        """Test getting bars dictionary when no data is available"""
        backtester = DataBentoDataBacktesting(
            datetime_start=self.start_date,
            datetime_end=self.end_date,
            api_key=self.api_key
        )
        
        # Mock _update_pandas_data to do nothing
        with patch.object(backtester, '_update_pandas_data'):
            result = backtester._get_bars_dict(
                assets=[self.test_asset],
                length=2,
                timestep="minute"
            )
            
            self.assertIsInstance(result, dict)
            self.assertIn(self.test_asset, result)
            self.assertIsNone(result[self.test_asset])

    @patch('lumibot.tools.databento_helper.DATABENTO_AVAILABLE', True)
    @patch('lumibot.tools.databento_helper.get_price_data_from_databento')
    def test_prefetch_data_single_asset(self, mock_get_data):
        """Test prefetching data for a single asset"""
        # Mock DataBento data response
        mock_df = pd.DataFrame({
            'open': [100.0, 101.0, 102.0],
            'high': [101.0, 102.0, 103.0],
            'low': [99.0, 100.0, 101.0],
            'close': [100.5, 101.5, 102.5],
            'volume': [1000, 1100, 1200]
        }, index=pd.date_range('2023-01-01', periods=3, freq='1min'))
        
        mock_get_data.return_value = mock_df
        
        # Create DataBento backtesting instance
        data_source = DataBentoDataBacktesting(
            datetime_start=self.start_date,
            datetime_end=self.end_date,
            api_key=self.api_key
        )
        
        # Test prefetch
        test_asset = Asset("ESH23", "future")
        data_source.prefetch_data([test_asset], timestep="minute")
        
        # Verify data was fetched and cached
        search_key = (test_asset, Asset("USD", "forex"))
        self.assertIn(search_key, data_source.pandas_data)
        self.assertIn(search_key, data_source._prefetched_assets)
        
        # Verify get_price_data_from_databento was called
        mock_get_data.assert_called_once()

    @patch('lumibot.tools.databento_helper.DATABENTO_AVAILABLE', True)
    @patch('lumibot.tools.databento_helper.get_price_data_from_databento')
    def test_prefetch_data_multiple_assets(self, mock_get_data):
        """Test prefetching data for multiple assets"""
        # Mock DataBento data response
        mock_df = pd.DataFrame({
            'open': [100.0, 101.0],
            'high': [101.0, 102.0],
            'low': [99.0, 100.0],
            'close': [100.5, 101.5],
            'volume': [1000, 1100]
        }, index=pd.date_range('2023-01-01', periods=2, freq='1min'))
        
        mock_get_data.return_value = mock_df
        
        # Create DataBento backtesting instance
        data_source = DataBentoDataBacktesting(
            datetime_start=self.start_date,
            datetime_end=self.end_date,
            api_key=self.api_key
        )
        
        # Test prefetch multiple assets
        test_assets = [
            Asset("ESH23", "future"),
            Asset("NQH23", "future")
        ]
        data_source.prefetch_data(test_assets, timestep="minute")
        
        # Verify both assets were prefetched
        for asset in test_assets:
            search_key = (asset, Asset("USD", "forex"))
            self.assertIn(search_key, data_source.pandas_data)
            self.assertIn(search_key, data_source._prefetched_assets)
        
        # Verify get_price_data_from_databento was called twice
        self.assertEqual(mock_get_data.call_count, 2)

    @patch('lumibot.tools.databento_helper.DATABENTO_AVAILABLE', True)
    @patch('lumibot.tools.databento_helper.get_price_data_from_databento')
    def test_prefetch_skips_duplicate_requests(self, mock_get_data):
        """Test that prefetch skips assets that were already prefetched"""
        # Mock DataBento data response
        mock_df = pd.DataFrame({
            'open': [100.0],
            'high': [101.0],
            'low': [99.0],
            'close': [100.5],
            'volume': [1000]
        }, index=pd.date_range('2023-01-01', periods=1, freq='1min'))
        
        mock_get_data.return_value = mock_df
        
        # Create DataBento backtesting instance
        data_source = DataBentoDataBacktesting(
            datetime_start=self.start_date,
            datetime_end=self.end_date,
            api_key=self.api_key
        )
        
        # Prefetch same asset twice
        test_asset = Asset("ESH23", "future")
        data_source.prefetch_data([test_asset], timestep="minute")
        data_source.prefetch_data([test_asset], timestep="minute")
        
        # Verify get_price_data_from_databento was called only once
        mock_get_data.assert_called_once()

    @patch('lumibot.tools.databento_helper.DATABENTO_AVAILABLE', True)
    @patch('lumibot.tools.databento_helper.get_price_data_from_databento')
    def test_prefetch_handles_no_data(self, mock_get_data):
        """Test prefetch handling when no data is returned"""
        # Mock DataBento to return None (no data)
        mock_get_data.return_value = None
        
        # Create DataBento backtesting instance
        data_source = DataBentoDataBacktesting(
            datetime_start=self.start_date,
            datetime_end=self.end_date,
            api_key=self.api_key
        )
        
        # Test prefetch
        test_asset = Asset("ESH23", "future")
        data_source.prefetch_data([test_asset], timestep="minute")
        
        # When DataBento returns None, the implementation fails during Data object creation
        # due to timezone handling, so the asset won't be marked as prefetched
        search_key = (test_asset, Asset("USD", "forex"))
        self.assertNotIn(search_key, data_source._prefetched_assets)
        
        # No data should be stored
        self.assertNotIn(search_key, data_source.pandas_data)


if __name__ == '__main__':
    unittest.main()
