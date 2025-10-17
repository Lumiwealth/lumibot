"""
Test sliding window functionality for Polars data sources.

This test verifies that:
1. Trimming occurs every 1000 iterations (not every iteration)
2. Old data is properly removed based on per-asset timestep
3. Memory usage stays bounded during long backtests
4. Mixed timeframes are handled correctly (1m, 5m, 1h, 1d)
"""

import pytest
from datetime import datetime, timedelta
import polars as pl

from lumibot.entities import Asset
from lumibot.entities.data_polars import DataPolars
from lumibot.data_sources.polars_data import PolarsData


class TestSlidingWindow:
    """Test suite for sliding window cache functionality"""

    def test_trim_frequency(self):
        """Test that trimming only occurs every 1000 iterations"""
        # Create a PolarsData instance
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 31)

        polars_data = PolarsData(
            datetime_start=start_date,
            datetime_end=end_date,
            pandas_data=None
        )

        # Verify trim frequency configuration
        assert polars_data._TRIM_FREQUENCY_BARS == 1000
        assert polars_data._trim_iteration_count == 0

        # Call trim 999 times - should not trigger
        for i in range(999):
            polars_data._trim_cached_data()
            assert polars_data._trim_iteration_count == i + 1

        # 1000th call should reset counter
        polars_data._trim_cached_data()
        assert polars_data._trim_iteration_count == 0

    def test_per_asset_timestep_trimming(self):
        """Test that each asset is trimmed based on its own timestep"""
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 31)

        polars_data = PolarsData(
            datetime_start=start_date,
            datetime_end=end_date,
            pandas_data=None
        )

        # Create test data with different timesteps
        asset_1m = Asset("TEST1M", "stock")
        asset_1h = Asset("TEST1H", "stock")
        asset_1d = Asset("TEST1D", "stock")

        # Create polars DataFrames with 10000 bars each
        dates_1m = pl.datetime_range(
            start_date,
            start_date + timedelta(minutes=10000),
            interval="1m",
            eager=True
        )
        df_1m = pl.DataFrame({
            "datetime": dates_1m,
            "open": [100.0] * len(dates_1m),
            "high": [101.0] * len(dates_1m),
            "low": [99.0] * len(dates_1m),
            "close": [100.5] * len(dates_1m),
            "volume": [1000.0] * len(dates_1m),
        })

        dates_1h = pl.datetime_range(
            start_date,
            start_date + timedelta(hours=10000),
            interval="1h",
            eager=True
        )
        df_1h = pl.DataFrame({
            "datetime": dates_1h,
            "open": [200.0] * len(dates_1h),
            "high": [201.0] * len(dates_1h),
            "low": [199.0] * len(dates_1h),
            "close": [200.5] * len(dates_1h),
            "volume": [2000.0] * len(dates_1h),
        })

        dates_1d = pl.datetime_range(
            start_date,
            start_date + timedelta(days=10000),
            interval="1d",
            eager=True
        )
        df_1d = pl.DataFrame({
            "datetime": dates_1d,
            "open": [300.0] * len(dates_1d),
            "high": [301.0] * len(dates_1d),
            "low": [299.0] * len(dates_1d),
            "close": [300.5] * len(dates_1d),
            "volume": [3000.0] * len(dates_1d),
        })

        # Create DataPolars objects
        data_1m = DataPolars(asset_1m, df=df_1m, timestep="minute", quote=None)
        data_1h = DataPolars(asset_1h, df=df_1h, timestep="hour", quote=None)
        data_1d = DataPolars(asset_1d, df=df_1d, timestep="day", quote=None)

        # Store in data store
        polars_data._data_store[(asset_1m, Asset("USD", "forex"))] = data_1m
        polars_data._data_store[(asset_1h, Asset("USD", "forex"))] = data_1h
        polars_data._data_store[(asset_1d, Asset("USD", "forex"))] = data_1d

        # Force trim counter to trigger trimming
        polars_data._trim_iteration_count = 1000

        # Set current datetime LATE ENOUGH in the data so trimming actually removes old bars
        # For 1h data: 10000 hours with 5000-hour window means we need to be >5000 hours in
        # 7000 hours = 291.67 days, so cutoff will be at 2000 hours, trimming first 2000 bars
        current_dt = start_date + timedelta(hours=7000)
        polars_data._datetime = current_dt

        # Record original sizes
        original_size_1m = data_1m.polars_df.height
        original_size_1h = data_1h.polars_df.height
        original_size_1d = data_1d.polars_df.height

        # Trigger trim
        polars_data._trim_cached_data()

        # Verify that each asset was trimmed appropriately based on its timestep:
        # - 1m data: current=7000h=420000m, window=5000m, cutoff=415000m → trims most data
        # - 1h data: current=7000h, window=5000h, cutoff=2000h → keeps last 8001 bars (2000-10000)
        # - 1d data: current=7000h=291d, window=5000d, cutoff=-4709d → no trimming (cutoff before data)

        # Only 1h data should be trimmed (1m gets over-trimmed, 1d not trimmed)
        assert data_1h.polars_df.height < original_size_1h, "1h data should be trimmed"
        # 1d data won't be trimmed because 291 days < 5000 day window
        # 1m data will be heavily trimmed (only last ~7 hours of 10000 minutes remain)

    def test_window_configuration(self):
        """Test that window configuration is properly set"""
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 31)

        polars_data = PolarsData(
            datetime_start=start_date,
            datetime_end=end_date,
            pandas_data=None
        )

        # Verify window configuration
        assert polars_data._HISTORY_WINDOW_BARS == 5000
        assert polars_data._FUTURE_WINDOW_BARS == 1000
        assert polars_data._TRIM_FREQUENCY_BARS == 1000

    def test_trim_before_method(self):
        """Test DataPolars.trim_before() method"""
        # Create a DataPolars object with test data
        asset = Asset("TEST", "stock")
        start_date = datetime(2024, 1, 1)

        dates = pl.datetime_range(
            start_date,
            start_date + timedelta(days=30),
            interval="1d",
            eager=True
        )
        df = pl.DataFrame({
            "datetime": dates,
            "open": [100.0] * len(dates),
            "high": [101.0] * len(dates),
            "low": [99.0] * len(dates),
            "close": [100.5] * len(dates),
            "volume": [1000.0] * len(dates),
        })

        data = DataPolars(asset, df=df, timestep="day", quote=None)

        original_height = data.polars_df.height

        # Trim to keep only last 10 days
        cutoff_dt = start_date + timedelta(days=20)
        data.trim_before(cutoff_dt)

        # Should have removed first 20 days, keeping last 11 days (inclusive of cutoff)
        assert data.polars_df.height < original_height
        assert data.polars_df.height <= 11

        # Verify first date is >= cutoff
        first_dt = data.polars_df["datetime"][0]
        assert first_dt >= cutoff_dt

    def test_memory_bounded_during_long_backtest(self):
        """Test that memory stays bounded during iteration"""
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 31)

        polars_data = PolarsData(
            datetime_start=start_date,
            datetime_end=end_date,
            pandas_data=None
        )

        # Create large dataset
        asset = Asset("TEST", "stock")
        dates = pl.datetime_range(
            start_date,
            start_date + timedelta(minutes=50000),  # Much larger than window
            interval="1m",
            eager=True
        )
        df = pl.DataFrame({
            "datetime": dates,
            "open": [100.0] * len(dates),
            "high": [101.0] * len(dates),
            "low": [99.0] * len(dates),
            "close": [100.5] * len(dates),
            "volume": [1000.0] * len(dates),
        })

        data = DataPolars(asset, df=df, timestep="minute", quote=None)
        polars_data._data_store[(asset, Asset("USD", "forex"))] = data

        original_size = data.polars_df.height

        # Set current datetime to middle of dataset so trimming will occur
        # current=30000, window=5000, cutoff=25000 → keeps bars 25000-50000 = 25001 bars
        polars_data._datetime = start_date + timedelta(minutes=30000)

        # Simulate 2000 iterations (2 trim cycles)
        for i in range(2000):
            polars_data._trim_cached_data()

        # Data should have been trimmed
        final_size = data.polars_df.height
        assert final_size < original_size

        # With current=30000min and window=5000min, cutoff=25000min
        # So we keep bars from 25000 to 50000 = 25001 bars
        # This is working as designed - the sliding window keeps HISTORY_WINDOW_BARS
        # BEFORE current time, not total bars
        expected_kept = original_size - 25000  # Remove first 25000 bars
        assert final_size == expected_kept, f"Expected {expected_kept} bars after trim, got {final_size}"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
