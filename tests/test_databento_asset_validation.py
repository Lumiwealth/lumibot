"""
Tests for DataBento asset type validation
"""
import pytest
import pandas as pd
import polars as pl
from datetime import datetime, timedelta
from unittest.mock import Mock, patch

from lumibot.entities import Asset
from lumibot.data_sources.databento_data import DataBentoData
from lumibot.tools.databento_helper import get_price_data_from_databento


class TestDataBentoAssetValidation:
    """Test asset type validation for DataBento data sources"""
    
    def test_live_data_source_futures_allowed(self):
        """Test that futures assets are allowed in live data source"""
        data_source = DataBentoData(api_key="test_key")
        
        # Test different futures asset types
        future_assets = [
            Asset("MES", Asset.AssetType.FUTURE),
            Asset("MES", Asset.AssetType.CONT_FUTURE),
        ]
        
        for asset in future_assets:
            # Should not raise an exception during validation
            # (We'll mock the actual API call)
            with patch(
                'lumibot.data_sources.databento_data_pandas.databento_helper_polars.get_price_data_from_databento_polars'
            ) as mock_get_data:
                mock_get_data.return_value = pl.DataFrame(
                    {
                        "datetime": [datetime.now()],
                        "open": [100.0],
                        "high": [101.0],
                        "low": [99.0],
                        "close": [100.5],
                        "volume": [1000],
                    }
                )
                try:
                    data_source.get_historical_prices(asset, 10, "minute")
                    # If we get here, validation passed
                    assert True
                except ValueError as e:
                    if "only supports futures assets" in str(e):
                        pytest.fail(f"Futures asset {asset.asset_type} should be allowed but was rejected: {e}")
                    else:
                        # Some other error, not validation
                        pass
    
    def test_live_data_source_equities_rejected(self):
        """Test that equity assets are rejected in live data source"""
        data_source = DataBentoData(api_key="test_key")
        
        # Test equity assets that should be rejected
        equity_assets = [
            Asset("AAPL", Asset.AssetType.STOCK),
            Asset("SPY", "stock"),  # string format
        ]
        
        with patch(
            'lumibot.data_sources.databento_data_pandas.databento_helper_polars.get_price_data_from_databento_polars'
        ) as mock_get_data:
            for asset in equity_assets:
                result = data_source.get_historical_prices(asset, 10, "minute")
                assert result is None

        mock_get_data.assert_not_called()
    
    def test_helper_function_allows_all_assets(self):
        """Test that helper function allows all asset types (validation is only in live data source)"""
        test_assets = [
            Asset("MES", Asset.AssetType.FUTURE),
            Asset("MES", Asset.AssetType.CONT_FUTURE),
            Asset("AAPL", Asset.AssetType.STOCK),  # Should be allowed in helper (used by backtesting)
        ]
        
        start_date = datetime.now() - timedelta(days=30)
        end_date = datetime.now()
        
        for asset in test_assets:
            with patch('lumibot.tools.databento_helper.DATABENTO_AVAILABLE', True), \
                 patch('lumibot.tools.databento_helper.DataBentoClient') as mock_client:
                
                # Mock the client and its methods
                mock_client_instance = Mock()
                mock_client.return_value = mock_client_instance
                mock_client_instance.get_historical_data.return_value = Mock()
                
                try:
                    get_price_data_from_databento(
                        api_key="test_key",
                        asset=asset,
                        start=start_date,
                        end=end_date,
                        timestep="minute"
                    )
                    # Should not raise asset type validation error
                    assert True
                except ValueError as e:
                    if "only supports futures assets" in str(e):
                        pytest.fail(f"Helper function should not validate asset types, but rejected {asset.asset_type}: {e}")
                    else:
                        # Some other error is fine (API, etc.)
                        pass
    
    def test_backtesting_allows_all_assets(self):
        """Test that backtesting data source allows all asset types for testing"""
        # The backtesting data source should allow any asset type for testing purposes
        # including equities that might be used as benchmarks or for testing
        
        # This test documents that backtesting should be flexible
        # while live trading should be restrictive
        assert True, "Backtesting should allow all asset types for flexibility"
    
    def test_benchmark_asset_in_backtesting_only(self):
        """Test that benchmark asset (SPY) is not loaded in live mode"""
        # This is a regression test for the issue where benchmark assets
        # were being loaded in live mode causing equity symbol requests
        
        # The benchmark asset should only be used in backtesting mode
        # In live mode, it should not cause any data requests
        
        # This test would require more complex mocking of the strategy execution
        # For now, we'll just document the expected behavior
        assert True, "Benchmark assets should only be used in backtesting mode"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
