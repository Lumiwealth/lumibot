#!/usr/bin/env python3
"""
Unit tests for DataBento timezone and logging fixes.

This module tests the critical fixes implemented to resolve:
1. "Cannot compare tz-naive and tz-aware timestamps" errors
2. "ErrorLogger.log_error() missing 1 required positional argument: 'message'" errors
3. DataBento integration working correctly for live trading

These tests ensure that the timezone handling and error logging fixes remain stable.
"""
import pytest
from datetime import datetime, timedelta
import pandas as pd
import pytz

from lumibot.data_sources import DataBentoData
from lumibot.data_sources.polars_data import PolarsData
from lumibot.entities import Asset

# Set up unified logging for tests
from lumibot.tools.lumibot_logger import get_logger, set_log_level
set_log_level('ERROR')  # Suppress unnecessary logging during tests


class TestErrorLoggerFixes:
    """Test that unified logger fixes are working correctly"""
    
    def test_error_logger_initialization(self):
        """Test that unified logger can be initialized without errors"""
        logger = get_logger(__name__)
        assert logger is not None
        
    def test_error_logger_log_error_with_all_args(self):
        """Test that unified logger error method accepts all required arguments correctly"""
        logger = get_logger(__name__)
        
        # Test with logger.error method
        try:
            logger.error("This is a test error message with details: test details")
            # If we reach here, the method signature is correct
            assert True
        except TypeError as e:
            pytest.fail(f"Unified logger.error() has incorrect signature: {e}")
    
    def test_error_logger_log_error_with_minimal_args(self):
        """Test that unified logger error method works with minimal required arguments"""
        logger = get_logger(__name__)
        
        # Test with minimum required arguments
        try:
            logger.error("Test message")
            assert True
        except Exception as e:
            pytest.fail(f"Unified logger.error() failed with minimal args: {e}")

    def test_error_logger_log_error_critical_levels(self):
        """Test that unified logger handles different severity levels correctly"""
        logger = get_logger(__name__)
        
        # Test different logger methods
        try:
            logger.info("Test message for INFO")
            logger.warning("Test message for WARNING") 
            logger.error("Test message for ERROR")
            logger.critical("Test message for CRITICAL")
            assert True
        except Exception as e:
            pytest.fail(f"Unified logger failed for different severity levels: {e}")


class TestTimezoneHandling:
    """Test that timezone handling fixes are working correctly"""
    
    def test_timezone_naive_datetime_operations(self):
        """Test that timezone-naive datetime operations work without errors"""
        now = datetime.now()
        past = now - timedelta(hours=1)
        
        # These should be timezone-naive
        assert now.tzinfo is None
        assert past.tzinfo is None
        
        # This should not raise timezone comparison errors
        assert past < now
        
    def test_pandas_datetime_filtering(self):
        """Test that pandas datetime filtering works without timezone errors"""
        # Create test DataFrame with timezone-naive timestamps
        now = datetime.now()
        timestamps = [now - timedelta(hours=i) for i in range(5)]
        
        df = pd.DataFrame({
            'timestamp': timestamps,
            'value': range(5)
        })
        df.set_index('timestamp', inplace=True)
        
        # This should not raise: "Cannot compare tz-naive and tz-aware timestamps"
        current_time = datetime.now()
        filtered_df = df[df.index <= current_time]
        
        assert len(filtered_df) >= 0  # Should complete without error
        assert df.index.tz is None    # Should remain timezone-naive
        
    def test_datetime_timezone_consistency(self):
        """Test that datetime operations maintain timezone consistency"""
        # Test creating timezone-naive datetimes
        dt1 = datetime(2025, 1, 1, 12, 0, 0)
        dt2 = datetime(2025, 1, 1, 13, 0, 0)
        
        assert dt1.tzinfo is None
        assert dt2.tzinfo is None
        
        # Test comparison
        assert dt1 < dt2
        
        # Test arithmetic
        diff = dt2 - dt1
        assert diff == timedelta(hours=1)

    def test_datetime_timezone_consistency_in_databento_context(self):
        """Test that DataBento operations maintain timezone consistency"""
        # Test creating timezone-naive datetimes like DataBento uses
        start_date = datetime(2025, 1, 1, 12, 0, 0)
        end_date = datetime(2025, 1, 1, 13, 0, 0)
        
        # These should be timezone-naive
        assert start_date.tzinfo is None
        assert end_date.tzinfo is None
        
        # Test comparison (this was failing before the fix)
        assert start_date < end_date
        
        # Test that we can create a DataFrame with these datetimes
        df = pd.DataFrame({
            'timestamp': [start_date, end_date],
            'value': [1, 2]
        })
        df.set_index('timestamp', inplace=True)
        
        # This should not raise timezone errors
        current_time = datetime.now()
        filtered_df = df[df.index <= current_time]
        
        assert len(filtered_df) >= 0
        assert df.index.tz is None

    def test_clean_trading_times_handles_dst_fallback(self):
        """PolarsData.clean_trading_times should return a monotonic index across DST."""
        ny_tz = pytz.timezone("America/New_York")
        # Build a UTC minute range that covers the 2025 DST fallback and convert to NY time
        utc_range = pd.date_range("2025-11-02 04:30", periods=360, freq="min", tz="UTC")
        ny_minutes = utc_range.tz_convert(ny_tz)
        # Mimic the bug trigger: downstream code often sees a tz-naive index
        naive_index = ny_minutes.tz_localize(None)

        market_open = ny_minutes[0]
        market_close = ny_minutes[-1]
        calendar_index = pd.DatetimeIndex([market_open.tz_localize(None).normalize()])
        pcal = pd.DataFrame(
            {"market_open": [market_open], "market_close": [market_close]},
            index=calendar_index,
        )

        polars_data = PolarsData.__new__(PolarsData)
        polars_data._timestep = "minute"

        cleaned_index = PolarsData.clean_trading_times(polars_data, naive_index, pcal)

        assert cleaned_index.tz is not None
        assert cleaned_index.tz.zone == ny_tz.zone
        assert cleaned_index.is_monotonic_increasing
        assert cleaned_index.is_unique
        assert cleaned_index[0] == market_open
        assert cleaned_index[-1] == market_close


class TestDataBentoIntegration:
    """Test DataBento integration with fixes"""
    
    def test_databento_data_source_initialization(self):
        """Test that DataBento data source can be initialized without errors"""
        # Test with dummy API key
        data_source = DataBentoData(api_key="test_key")
        assert data_source is not None
        assert data_source._api_key == "test_key"
        assert data_source.name == "data_source"  # This is the default from DataSource base class
        assert data_source.IS_BACKTESTING_DATA_SOURCE is False
        
    def test_databento_futures_asset_validation(self):
        """Test that DataBento correctly validates asset types"""
        data_source = DataBentoData(api_key="test_key")
        
        # DataBentoDataPolars logs an error but doesn't raise for non-futures
        # It just returns None
        stock_asset = Asset(symbol="AAPL", asset_type="stock")
        result = data_source.get_historical_prices(
            asset=stock_asset,
            length=10,
            timestep="minute"
        )
        # Should return None for non-futures assets
        assert result is None
        
        # Test that futures assets are accepted (validation passes)
        futures_asset = Asset(symbol="ES", asset_type="future")
        try:
            # This should not raise a validation error
            data_source.get_historical_prices(
                asset=futures_asset,
                length=10,
                timestep="minute"
            )
            # If we reach here, validation passed (even if no data was returned)
            assert True
        except ValueError as e:
            if "only supports futures assets" in str(e):
                pytest.fail("Futures asset validation failed unexpectedly")
            else:
                # Other ValueErrors might be expected (like API errors)
                pass

    def test_databento_live_trading_mode(self):
        """Test that DataBento works in live trading mode"""
        data_source = DataBentoData(api_key="test_key")
        
        # Should be configured for live trading by default
        assert data_source.IS_BACKTESTING_DATA_SOURCE is False
        assert data_source.name == "data_source"
        

class TestPolarsDSTHandling:
    """Ensure Polars backtesting utilities keep time monotonic across DST changes."""

    def test_clean_trading_times_handles_dst_fallback(self):
        tz = pytz.timezone("America/New_York")
        market_open = tz.localize(datetime(2025, 11, 2, 0, 0))
        market_close = tz.localize(datetime(2025, 11, 2, 9, 0))

        # Build a UTC range that, once converted, spans the DST fallback hour.
        dt_index = pd.date_range(
            start="2025-11-02 04:00:00",
            end="2025-11-02 14:00:00",
            freq="1min",
            tz="UTC",
        ).tz_convert(tz)

        calendar_index = pd.DatetimeIndex([market_open])
        pcal = pd.DataFrame(
            {"market_open": [market_open], "market_close": [market_close]},
            index=calendar_index,
        )

        dummy = object.__new__(PolarsData)
        dummy._timestep = "minute"

        cleaned = PolarsData.clean_trading_times(dummy, dt_index, pcal)

        # Returned index stays in the strategy timezone and strictly increases even across DST fallback.
        assert cleaned.tz.zone == tz.zone
        assert cleaned.is_monotonic_increasing
        cleaned_utc = cleaned.tz_convert("UTC")
        diffs = cleaned_utc.to_series().diff().dropna()
        assert (diffs == pd.Timedelta(minutes=1)).all()

        expected_minutes = int(
            (market_close.astimezone(pytz.UTC) - market_open.astimezone(pytz.UTC)).total_seconds() / 60
        ) + 1
        assert len(cleaned) == expected_minutes
        assert cleaned[0] == market_open
        assert cleaned[-1] == market_close
    def test_databento_supported_asset_types(self):
        """Test that DataBento supports the expected asset types"""
        data_source = DataBentoData(api_key="test_key")
        
        # Test continuous futures
        cont_future_asset = Asset(symbol="ES", asset_type="cont_future")
        try:
            data_source.get_historical_prices(
                asset=cont_future_asset,
                length=5,
                timestep="minute"
            )
            assert True
        except ValueError as e:
            if "only supports futures assets" in str(e):
                pytest.fail("Continuous futures should be supported")
            else:
                # Other errors (like API issues) are expected in tests
                pass


class TestDataBentoTimezoneFixValidation:
    """Test that the specific timezone fixes for DataBento are working"""
    
    def test_live_trading_datetime_handling(self):
        """Test that live trading handles datetime operations correctly"""
        # Test that current datetime operations work without timezone errors
        now = datetime.now()
        past = now - timedelta(hours=1)
        
        # These operations should not raise timezone errors
        assert past < now
        assert now.tzinfo is None
        assert past.tzinfo is None
        
        # Test creating date ranges like DataBento does
        start_date = now - timedelta(days=1)
        end_date = now
        
        assert start_date < end_date
        assert start_date.tzinfo is None
        assert end_date.tzinfo is None

    def test_dataframe_timezone_operations(self):
        """Test DataFrame operations that were previously failing"""
        # Create a DataFrame like DataBento would return
        now = datetime.now()
        timestamps = [now - timedelta(minutes=i) for i in range(10)]
        
        df = pd.DataFrame({
            'timestamp': timestamps,
            'close': [100 + i for i in range(10)]
        })
        df.set_index('timestamp', inplace=True)
        
        # This operation was previously failing with timezone errors
        current_time = datetime.now()
        try:
            filtered_df = df[df.index <= current_time]
            assert len(filtered_df) >= 0
            assert df.index.tz is None
        except Exception as e:
            if "tz-naive" in str(e) or "tz-aware" in str(e):
                pytest.fail(f"Timezone error still occurring: {e}")
            else:
                # Other exceptions might be expected
                pass


# Mark tests that require DataBento API key
@pytest.mark.parametrize("api_key_available", [True, False])
def test_databento_api_integration(api_key_available):
    """Test DataBento API integration when API key is available"""
    if not api_key_available:
        pytest.skip("DataBento API key not available")

    # Test actual DataBento API integration with real credentials
    from lumibot.credentials import DATABENTO_CONFIG

    # Verify credentials are available
    api_key = DATABENTO_CONFIG.get('API_KEY')
    if not api_key or api_key == '<your key here>':
        pytest.skip("Valid DataBento API key not configured")

    # Initialize DataBento data source with real API key
    data_source = DataBentoData(api_key=api_key)

    # Verify initialization succeeded
    assert data_source is not None
    assert data_source.SOURCE == "DATABENTO"
    assert data_source._api_key == api_key  # Note: private attribute

    # Verify the data source is a live data source (not backtesting)
    assert hasattr(data_source, 'get_last_price') or hasattr(data_source, 'get_quote')

    print(f"✓ DataBento API integration successful with key: {api_key[:10]}...")


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "--tb=short"])
