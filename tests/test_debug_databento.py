"""
Debug test for DataBento empty data handling
"""
import pytest
from datetime import datetime
from unittest.mock import Mock, patch
import pandas as pd

from lumibot.backtesting.databento_backtesting import DataBentoDataBacktesting
from lumibot.entities import Asset


def test_debug_empty_data():
    """Debug test to understand empty data handling"""
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2023, 1, 31)
    api_key = "test_key"
    
    test_asset = Asset("ES", "future", expiration=datetime(2025, 3, 15).date())
    
    with patch('lumibot.tools.databento_helper.DATABENTO_AVAILABLE', True):
        with patch('lumibot.tools.databento_helper.get_price_data_from_databento') as mock_get_data:
            mock_get_data.return_value = None
            
            backtester = DataBentoDataBacktesting(
                datetime_start=start_date,
                datetime_end=end_date,
                api_key=api_key
            )
            
            print(f"Initial pandas_data: {backtester.pandas_data}")
            print(f"Prefetched assets: {backtester._prefetched_assets}")
            
            with patch.object(backtester, 'get_start_datetime_and_ts_unit') as mock_get_start:
                mock_get_start.return_value = (start_date, "minute")
                
                print("Calling _update_pandas_data...")
                backtester._update_pandas_data(
                    asset=test_asset,
                    quote=None,
                    length=10,
                    timestep="minute"
                )
                
                print(f"After update pandas_data: {backtester.pandas_data}")
                print(f"Mock get_data called: {mock_get_data.called}")
                print(f"Mock get_data call_count: {mock_get_data.call_count}")
                
                search_asset = (test_asset, Asset("USD", "forex"))
                print(f"Search asset: {search_asset}")
                print(f"Is search_asset in pandas_data: {search_asset in backtester.pandas_data}")


if __name__ == "__main__":
    test_debug_empty_data()
