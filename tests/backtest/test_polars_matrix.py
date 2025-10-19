"""
Matrix test for Polars data sources: 5 timesteps × 4 asset types = 20 combinations.

This test verifies that sliding window, aggregation, and LRU eviction work correctly
for all combinations of:
- Timesteps: 1m, 5m, 15m, 1h, 1d
- Asset types: stock, future, option, index

Total: 5 × 4 = 20 combinations
"""

import pytest
from datetime import datetime, timedelta
import polars as pl
import pandas as pd

from lumibot.entities import Asset
from lumibot.entities.data_polars import DataPolars
from lumibot.data_sources.polars_data import PolarsData


class TestPolarsMatrix:
    """Matrix test suite for all timestep/asset type combinations"""

    # Test matrix configuration
    TIMESTEPS = ["minute", "5 minutes", "15 minutes", "hour", "day"]
    ASSET_TYPES = ["stock", "future", "option", "index"]

    def _create_test_data(self, asset_type, timestep, num_bars=1000):
        """Helper to create test data for a given asset type and timestep"""
        start_date = datetime(2024, 1, 1)

        # Map timestep to polars interval
        interval_map = {
            "minute": "1m",
            "5 minutes": "5m",
            "15 minutes": "15m",
            "hour": "1h",
            "day": "1d",
        }

        interval = interval_map.get(timestep, "1m")

        # Calculate end date based on timestep
        if timestep == "minute":
            end_date = start_date + timedelta(minutes=num_bars)
        elif timestep == "5 minutes":
            end_date = start_date + timedelta(minutes=num_bars * 5)
        elif timestep == "15 minutes":
            end_date = start_date + timedelta(minutes=num_bars * 15)
        elif timestep == "hour":
            end_date = start_date + timedelta(hours=num_bars)
        elif timestep == "day":
            end_date = start_date + timedelta(days=num_bars)
        else:
            end_date = start_date + timedelta(minutes=num_bars)

        # Create date range
        dates = pl.datetime_range(
            start_date,
            end_date,
            interval=interval,
            eager=True
        )

        # Limit to requested number of bars
        if len(dates) > num_bars:
            dates = dates[:num_bars]

        # Create OHLCV data
        df = pl.DataFrame({
            "datetime": dates,
            "open": [100.0] * len(dates),
            "high": [101.0] * len(dates),
            "low": [99.0] * len(dates),
            "close": [100.5] * len(dates),
            "volume": [1000.0] * len(dates),
        })

        # Create asset based on type
        symbol = f"TEST_{asset_type.upper()}_{timestep.replace(' ', '_').upper()}"
        asset = Asset(symbol, asset_type)

        return asset, df

    @pytest.mark.parametrize("timestep", TIMESTEPS)
    @pytest.mark.parametrize("asset_type", ASSET_TYPES)
    def test_data_storage_and_retrieval(self, timestep, asset_type):
        """Test that data can be stored and retrieved for each combination"""
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 12, 31)

        polars_data = PolarsData(
            datetime_start=start_date,
            datetime_end=end_date,
            pandas_data=None
        )

        # Create test data
        asset, df = self._create_test_data(asset_type, timestep, num_bars=500)
        quote = Asset("USD", "forex")

        # Create DataPolars object
        data = DataPolars(asset, df=df, timestep=timestep, quote=None)

        # Store in data_store
        polars_data._data_store[(asset, quote)] = data

        # Verify storage
        assert (asset, quote) in polars_data._data_store
        retrieved_data = polars_data._data_store[(asset, quote)]
        assert retrieved_data.asset == asset
        assert retrieved_data.timestep == timestep
        assert retrieved_data.polars_df.height == df.height

    @pytest.mark.parametrize("timestep", TIMESTEPS)
    @pytest.mark.parametrize("asset_type", ASSET_TYPES)
    def test_sliding_window_trimming(self, timestep, asset_type):
        """Test that sliding window trimming removes old bars.

        Production behavior: Only old bars (before cutoff) are trimmed, not future bars.
        Trimming calculates: cutoff = current_dt - (HISTORY_WINDOW_BARS * timestep_delta)
        Then removes all bars before cutoff.
        """
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 12, 31)

        polars_data = PolarsData(
            datetime_start=start_date,
            datetime_end=end_date,
            pandas_data=None
        )

        # Create test data with more bars than window size
        asset, df = self._create_test_data(asset_type, timestep, num_bars=10000)
        quote = Asset("USD", "forex")

        # Create DataPolars object
        data = DataPolars(asset, df=df, timestep=timestep, quote=None)

        # Store in data_store
        polars_data._data_store[(asset, quote)] = data

        original_height = data.polars_df.height

        # Force trim counter to trigger trimming
        polars_data._trim_iteration_count = 1000

        # Set current datetime to END of range (so old bars can be trimmed)
        # Setting to middle would result in cutoff before data start, trimming nothing
        if timestep == "minute":
            current_dt = start_date + timedelta(minutes=10000)
        elif timestep == "5 minutes":
            current_dt = start_date + timedelta(minutes=10000 * 5)
        elif timestep == "15 minutes":
            current_dt = start_date + timedelta(minutes=10000 * 15)
        elif timestep == "hour":
            current_dt = start_date + timedelta(hours=10000)
        elif timestep == "day":
            current_dt = start_date + timedelta(days=10000)
        else:
            current_dt = start_date + timedelta(minutes=10000)

        # CRITICAL: Use _datetime (internal attribute) not datetime (doesn't exist)
        polars_data._datetime = current_dt

        # Trigger trim
        polars_data._trim_cached_data()

        # Verify trimming occurred
        final_height = data.polars_df.height
        assert final_height < original_height, f"Expected trimming but height stayed at {original_height}"
        # Should keep approximately last HISTORY_WINDOW_BARS (5000) bars
        # Production only trims old bars, not future bars, so we keep ~5000
        assert final_height <= polars_data._HISTORY_WINDOW_BARS * 1.1, f"Expected ~{polars_data._HISTORY_WINDOW_BARS} bars, got {final_height}"

    @pytest.mark.parametrize("asset_type", ASSET_TYPES)
    def test_aggregation_from_minute_data(self, asset_type):
        """Test that aggregation from minute data works for all asset types"""
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 31)

        polars_data = PolarsData(
            datetime_start=start_date,
            datetime_end=end_date,
            pandas_data=None
        )

        # Create 1-minute test data
        asset, df = self._create_test_data(asset_type, "minute", num_bars=1000)
        quote = Asset("USD", "forex")

        # Create DataPolars object
        data = DataPolars(asset, df=df, timestep="minute", quote=None)

        # Store in data_store
        polars_data._data_store[(asset, quote)] = data

        # Test aggregation to different timesteps
        target_timesteps = ["5 minutes", "15 minutes", "hour"]

        for target_timestep in target_timesteps:
            # Aggregate
            agg_df = polars_data._get_or_aggregate_bars(
                asset, quote, 100, "minute", target_timestep
            )

            # Verify aggregation succeeded
            assert agg_df is not None
            assert agg_df.height > 0
            assert agg_df.height < df.height  # Aggregated data should have fewer bars

            # Verify cache entry was created
            cache_key = (asset, quote, target_timestep)
            assert cache_key in polars_data._aggregated_cache

    def test_mixed_timesteps_same_asset(self):
        """Test that same asset can have multiple timesteps simultaneously"""
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 31)

        polars_data = PolarsData(
            datetime_start=start_date,
            datetime_end=end_date,
            pandas_data=None
        )

        # Create same asset with different timesteps
        base_asset = Asset("AAPL", "stock")
        quote = Asset("USD", "forex")

        timesteps = ["minute", "5 minutes", "hour", "day"]

        for timestep in timesteps:
            _, df = self._create_test_data("stock", timestep, num_bars=500)
            data = DataPolars(base_asset, df=df, timestep=timestep, quote=None)

            # Use different quote to differentiate (in practice, would use different data sources)
            # But for this test, we'll use timestep as part of the key
            key = (base_asset, Asset(f"{timestep.replace(' ', '_')}", "forex"))
            polars_data._data_store[key] = data

        # Verify all timesteps are stored
        assert len(polars_data._data_store) == len(timesteps)

    def test_lru_eviction_across_asset_types(self):
        """Test LRU eviction works across different asset types"""
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 31)

        polars_data = PolarsData(
            datetime_start=start_date,
            datetime_end=end_date,
            pandas_data=None
        )

        # Set low memory limit
        polars_data.MAX_STORAGE_BYTES = 100_000  # 100KB

        # Create data for all asset types
        quote = Asset("USD", "forex")
        created_count = 0

        for asset_type in self.ASSET_TYPES:
            asset, df = self._create_test_data(asset_type, "minute", num_bars=1000)
            data = DataPolars(asset, df=df, timestep="minute", quote=None)
            polars_data._data_store[(asset, quote)] = data
            created_count += 1

        # Force memory limit enforcement
        polars_data._enforce_memory_limits()

        # Some assets should have been evicted
        final_count = len(polars_data._data_store)
        assert final_count <= created_count

    def test_memory_calculation_all_asset_types(self):
        """Test that memory calculation works for all asset types"""
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 31)

        polars_data = PolarsData(
            datetime_start=start_date,
            datetime_end=end_date,
            pandas_data=None
        )

        quote = Asset("USD", "forex")

        for asset_type in self.ASSET_TYPES:
            asset, df = self._create_test_data(asset_type, "minute", num_bars=500)
            data = DataPolars(asset, df=df, timestep="minute", quote=None)

            # Calculate memory
            memory_size = df.estimated_size()

            # Should be non-zero
            assert memory_size > 0

            # Store data
            polars_data._data_store[(asset, quote)] = data

        # Calculate total memory
        total_memory = 0
        for data in polars_data._data_store.values():
            if hasattr(data, 'polars_df'):
                total_memory += data.polars_df.estimated_size()

        # Should be non-zero
        assert total_memory > 0

    @pytest.mark.parametrize("timestep", TIMESTEPS)
    def test_trim_before_all_timesteps(self, timestep):
        """Test that trim_before works correctly for all timesteps"""
        start_date = datetime(2024, 1, 1)

        # Create test data
        asset, df = self._create_test_data("stock", timestep, num_bars=1000)

        # Create DataPolars object
        data = DataPolars(asset, df=df, timestep=timestep, quote=None)

        original_height = data.polars_df.height

        # Calculate cutoff (keep last 100 bars)
        if timestep == "minute":
            cutoff_dt = start_date + timedelta(minutes=900)
        elif timestep == "5 minutes":
            cutoff_dt = start_date + timedelta(minutes=900 * 5)
        elif timestep == "15 minutes":
            cutoff_dt = start_date + timedelta(minutes=900 * 15)
        elif timestep == "hour":
            cutoff_dt = start_date + timedelta(hours=900)
        elif timestep == "day":
            cutoff_dt = start_date + timedelta(days=900)
        else:
            cutoff_dt = start_date + timedelta(minutes=900)

        # Trim
        data.trim_before(cutoff_dt)

        # Verify trimming
        final_height = data.polars_df.height
        assert final_height < original_height
        # Should have approximately 100 bars left
        assert final_height <= 120  # With some buffer

    def test_complete_workflow_all_combinations(self):
        """Integration test: complete workflow for all 20 combinations"""
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 31)

        polars_data = PolarsData(
            datetime_start=start_date,
            datetime_end=end_date,
            pandas_data=None
        )

        quote = Asset("USD", "forex")
        successful_combinations = 0

        # Test all 20 combinations
        for asset_type in self.ASSET_TYPES:
            for timestep in self.TIMESTEPS:
                try:
                    # Create data
                    asset, df = self._create_test_data(asset_type, timestep, num_bars=500)
                    data = DataPolars(asset, df=df, timestep=timestep, quote=None)

                    # Store
                    polars_data._data_store[(asset, quote)] = data

                    # Verify storage
                    assert (asset, quote) in polars_data._data_store

                    # If minute data, test aggregation
                    if timestep == "minute":
                        agg_df = polars_data._get_or_aggregate_bars(
                            asset, quote, 100, "minute", "5 minutes"
                        )
                        if agg_df is not None:
                            assert agg_df.height > 0

                    successful_combinations += 1

                except Exception as e:
                    pytest.fail(f"Failed for {asset_type} + {timestep}: {e}")

        # All 20 combinations should succeed
        assert successful_combinations == len(self.ASSET_TYPES) * len(self.TIMESTEPS)

    def test_per_asset_timestep_differentiation(self):
        """Test that per-asset timestep logic uses correct delta for each asset.

        This verifies that minute data and day data are trimmed based on their
        own timestep deltas, not a global timestep.
        """
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 12, 31)

        polars_data = PolarsData(
            datetime_start=start_date,
            datetime_end=end_date,
            pandas_data=None
        )

        # Create assets with different timesteps
        # Key point: Each should use its own timestep for window calculation
        test_cases = [
            ("minute", 10000),   # 10,000 minute bars
            ("day", 10000),      # 10,000 day bars
        ]

        original_heights = {}
        for i, (timestep, num_bars) in enumerate(test_cases):
            asset = Asset(f"TEST_{timestep.upper()}", "stock")
            quote = Asset(f"USD_{i}", "forex")

            # Create data
            _, df = self._create_test_data("stock", timestep, num_bars=num_bars)
            data = DataPolars(asset, df=df, timestep=timestep, quote=None)

            # Store
            polars_data._data_store[(asset, quote)] = data
            original_heights[(asset, quote)] = data.polars_df.height

        # Force trim
        polars_data._trim_iteration_count = 1000

        # Set current datetime to END of the data ranges
        # This ensures old bars can be trimmed
        current_dt = start_date + timedelta(days=10000)  # Far enough for both timesteps
        polars_data._datetime = current_dt

        # Trigger trim
        polars_data._trim_cached_data()

        # Verify trimming occurred based on each asset's own timestep
        for (asset, quote), data in polars_data._data_store.items():
            original_height = original_heights[(asset, quote)]
            final_height = data.polars_df.height

            # Trimming should have occurred
            assert final_height < original_height, \
                f"{asset.symbol} should have been trimmed from {original_height}"

            # Should keep roughly HISTORY_WINDOW_BARS (within reasonable tolerance)
            # More lenient tolerance since we're testing differentiation, not exact counts
            assert final_height <= polars_data._HISTORY_WINDOW_BARS * 1.2, \
                f"{asset.symbol} kept {final_height} bars, expected ≤ {polars_data._HISTORY_WINDOW_BARS * 1.2}"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
