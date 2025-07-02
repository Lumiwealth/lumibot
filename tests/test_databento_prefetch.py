"""
Tests for DataBento backtesting prefetch functionality
"""
import pytest
from datetime import datetime, timedelta
from unittest.mock import Mock, patch, MagicMock
import pandas as pd

from lumibot.backtesting.databento_backtesting import DataBentoDataBacktesting
from lumibot.entities import Asset


class TestDataBentoPrefetch:
    """Test prefetch functionality for DataBento backtesting"""
    
    def setup_method(self):
        """Set up test fixtures"""
        self.start_date = datetime(2023, 1, 1)
        self.end_date = datetime(2023, 1, 31)
        self.api_key = "test_api_key"
        
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
        assert search_key in data_source.pandas_data
        assert search_key in data_source._prefetched_assets
        
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
            assert search_key in data_source.pandas_data
            assert search_key in data_source._prefetched_assets
        
        # Verify get_price_data_from_databento was called twice
        assert mock_get_data.call_count == 2
        
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
        data_source.prefetch_data([test_asset], timestep="minute")  # Should be skipped
        
        # Verify get_price_data_from_databento was called only once
        assert mock_get_data.call_count == 1
        
    @patch('lumibot.tools.databento_helper.DATABENTO_AVAILABLE', True)
    def test_initialize_data_for_backtest_with_symbols(self):
        """Test initialize_data_for_backtest with string symbols"""
        # Create DataBento backtesting instance
        data_source = DataBentoDataBacktesting(
            datetime_start=self.start_date,
            datetime_end=self.end_date,
            api_key=self.api_key
        )
        
        # Mock the prefetch_data method
        data_source.prefetch_data = Mock()
        
        # Test with string symbols
        symbols = ["ESH23", "NQH23", "AAPL"]
        data_source.initialize_data_for_backtest(symbols)
        
        # Verify prefetch_data was called with Asset objects
        data_source.prefetch_data.assert_called_once()
        args = data_source.prefetch_data.call_args[0]
        assets = args[0]
        
        # Check that strings were converted to appropriate Asset objects
        assert len(assets) == 3
        assert all(isinstance(asset, Asset) for asset in assets)
        
        # Check asset types were inferred correctly
        es_asset = next(a for a in assets if a.symbol == "ESH23")
        nq_asset = next(a for a in assets if a.symbol == "NQH23")
        aapl_asset = next(a for a in assets if a.symbol == "AAPL")
        
        assert es_asset.asset_type == "future"  # Contains month code
        assert nq_asset.asset_type == "future"  # Contains month code
        assert aapl_asset.asset_type == "stock"  # No month code
        
    @patch('lumibot.tools.databento_helper.DATABENTO_AVAILABLE', True)
    @patch('lumibot.tools.databento_helper.get_price_data_from_databento')
    def test_update_pandas_data_skips_prefetched_assets(self, mock_get_data):
        """Test that _update_pandas_data skips assets that were prefetched"""
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
        
        # Prefetch asset
        test_asset = Asset("ESH23", "future")
        data_source.prefetch_data([test_asset], timestep="minute")
        
        # Reset mock to track new calls
        mock_get_data.reset_mock()
        
        # Call _update_pandas_data - should skip because asset is prefetched
        data_source._update_pandas_data(test_asset, None, 100, "minute")
        
        # Verify no additional API calls were made
        mock_get_data.assert_not_called()


if __name__ == "__main__":
    pytest.main([__file__])
