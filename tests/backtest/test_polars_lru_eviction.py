"""
Test LRU eviction functionality for Polars data sources.

This test verifies that:
1. Memory limits are enforced via LRU eviction
2. Two-tier eviction works (aggregated cache first, then data store)
3. LRU order is maintained (oldest unused items evicted first)
4. Memory calculation is accurate
5. Multiple symbols are handled correctly under memory pressure
"""

import pytest
from datetime import datetime, timedelta
import polars as pl

from lumibot.entities import Asset
from lumibot.entities.data_polars import DataPolars
from lumibot.data_sources.polars_data import PolarsData


class TestLRUEviction:
    """Test suite for LRU eviction functionality"""

    def test_memory_limit_configuration(self):
        """Test that memory limit is configured correctly"""
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 31)

        polars_data = PolarsData(
            datetime_start=start_date,
            datetime_end=end_date,
            pandas_data=None
        )

        # Default should be 1GB
        assert polars_data.MAX_STORAGE_BYTES == 1_000_000_000

    def test_eviction_from_aggregated_cache_first(self):
        """Test that eviction happens from aggregated cache first.

        This test verifies the two-tier eviction priority:
        1. Aggregated cache is evicted first (less critical)
        2. Data store is evicted only if aggregated cache eviction isn't enough
        """
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 31)

        polars_data = PolarsData(
            datetime_start=start_date,
            datetime_end=end_date,
            pandas_data=None
        )

        # Set memory limit high enough that evicting aggregated cache is sufficient
        # Each asset: ~48KB data + ~10KB aggregated = ~58KB total
        # 5 assets = ~290KB total
        # Set limit to 250KB so aggregated cache eviction (50KB) is enough
        polars_data.MAX_STORAGE_BYTES = 250_000  # 250KB

        # Create 1-minute test data for 5 assets
        assets = [Asset(f"TEST{i}", "stock") for i in range(5)]
        quote = Asset("USD", "forex")

        for asset in assets:
            dates = pl.datetime_range(
                start_date,
                start_date + timedelta(minutes=1000),
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
            polars_data._data_store[(asset, quote)] = data

        # Aggregate all 5 assets to create aggregated cache entries
        for asset in assets:
            polars_data._get_or_aggregate_bars(asset, quote, 100, "minute", "5 minutes")

        assert len(polars_data._aggregated_cache) == 5
        original_data_store_size = len(polars_data._data_store)

        # CRITICAL: Set _trim_iteration_count = 0 to actually trigger enforcement
        # (Production code only enforces when _trim_iteration_count == 0)
        polars_data._trim_iteration_count = 0

        # Force memory limit enforcement
        polars_data._enforce_memory_limits()

        # Aggregated cache should be partially/fully evicted
        # Data store should still have all 5 assets (evicting agg cache was enough)
        assert len(polars_data._aggregated_cache) < 5, "Aggregated cache should have been evicted"
        assert len(polars_data._data_store) == original_data_store_size, \
            f"Data store should be untouched (expected {original_data_store_size}, got {len(polars_data._data_store)})"

    def test_eviction_from_data_store_when_aggregated_empty(self):
        """Test that eviction happens from data_store when aggregated cache is empty"""
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 31)

        polars_data = PolarsData(
            datetime_start=start_date,
            datetime_end=end_date,
            pandas_data=None
        )

        # Set a very low memory limit
        polars_data.MAX_STORAGE_BYTES = 50_000  # 50KB

        # Create 1-minute test data for 10 assets (no aggregated cache)
        assets = [Asset(f"TEST{i}", "stock") for i in range(10)]
        quote = Asset("USD", "forex")

        for asset in assets:
            dates = pl.datetime_range(
                start_date,
                start_date + timedelta(minutes=1000),
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
            polars_data._data_store[(asset, quote)] = data

        # No aggregated cache entries
        assert len(polars_data._aggregated_cache) == 0

        # Set _trim_iteration_count = 0 to trigger enforcement
        polars_data._trim_iteration_count = 0

        # Force memory limit enforcement
        polars_data._enforce_memory_limits()

        # Data store should have been evicted
        assert len(polars_data._data_store) < 10

    def test_lru_order_maintained(self):
        """Test that LRU order is maintained - oldest unused items evicted first"""
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 31)

        polars_data = PolarsData(
            datetime_start=start_date,
            datetime_end=end_date,
            pandas_data=None
        )

        # Create 5 assets
        assets = [Asset(f"TEST{i}", "stock") for i in range(5)]
        quote = Asset("USD", "forex")

        for asset in assets:
            dates = pl.datetime_range(
                start_date,
                start_date + timedelta(minutes=1000),
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
            polars_data._data_store[(asset, quote)] = data

        # Aggregate all 5 to create cache entries in order
        for asset in assets:
            polars_data._get_or_aggregate_bars(asset, quote, 100, "minute", "5 minutes")

        # Access TEST0, TEST1, TEST2 again (should move to end)
        polars_data._get_or_aggregate_bars(assets[0], quote, 100, "minute", "5 minutes")
        polars_data._get_or_aggregate_bars(assets[1], quote, 100, "minute", "5 minutes")
        polars_data._get_or_aggregate_bars(assets[2], quote, 100, "minute", "5 minutes")

        # Order should now be: TEST3, TEST4, TEST0, TEST1, TEST2 (least to most recent)
        keys = list(polars_data._aggregated_cache.keys())
        assert keys[0][0] == assets[3]  # TEST3 is oldest
        assert keys[1][0] == assets[4]  # TEST4 is second oldest
        assert keys[-3][0] == assets[0]  # TEST0 is third newest
        assert keys[-2][0] == assets[1]  # TEST1 is second newest
        assert keys[-1][0] == assets[2]  # TEST2 is newest

    def test_memory_calculation_accuracy(self):
        """Test that memory calculation is accurate using polars estimated_size()"""
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 31)

        polars_data = PolarsData(
            datetime_start=start_date,
            datetime_end=end_date,
            pandas_data=None
        )

        # Create test data
        asset = Asset("TEST", "stock")
        quote = Asset("USD", "forex")
        dates = pl.datetime_range(
            start_date,
            start_date + timedelta(minutes=1000),
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
        polars_data._data_store[(asset, quote)] = data

        # Calculate memory manually
        expected_size = df.estimated_size()

        # Should be non-zero
        assert expected_size > 0

        # Create aggregated cache entry
        polars_data._get_or_aggregate_bars(asset, quote, 100, "minute", "5 minutes")

        # Calculate total memory
        total_memory = 0
        for data in polars_data._data_store.values():
            if hasattr(data, 'polars_df'):
                total_memory += data.polars_df.estimated_size()

        for agg_df in polars_data._aggregated_cache.values():
            if agg_df is not None:
                total_memory += agg_df.estimated_size()

        # Should be larger than original df
        assert total_memory > expected_size

    def test_two_tier_eviction(self):
        """Test that two-tier eviction works: aggregated first, then data store"""
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 31)

        polars_data = PolarsData(
            datetime_start=start_date,
            datetime_end=end_date,
            pandas_data=None
        )

        # Set memory limit to force eviction
        polars_data.MAX_STORAGE_BYTES = 80_000  # 80KB

        # Create 5 assets
        assets = [Asset(f"TEST{i}", "stock") for i in range(5)]
        quote = Asset("USD", "forex")

        for asset in assets:
            dates = pl.datetime_range(
                start_date,
                start_date + timedelta(minutes=1000),
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
            polars_data._data_store[(asset, quote)] = data

        # Create aggregated cache entries for all 5
        for asset in assets:
            polars_data._get_or_aggregate_bars(asset, quote, 100, "minute", "5 minutes")

        initial_data_store_size = len(polars_data._data_store)
        initial_agg_cache_size = len(polars_data._aggregated_cache)

        assert initial_data_store_size == 5
        assert initial_agg_cache_size == 5

        # Set _trim_iteration_count = 0 to trigger enforcement
        polars_data._trim_iteration_count = 0

        # Force eviction
        polars_data._enforce_memory_limits()

        # Aggregated cache should be evicted first
        after_eviction_agg_size = len(polars_data._aggregated_cache)
        after_eviction_data_size = len(polars_data._data_store)

        # Either aggregated cache was reduced, or if that wasn't enough, data store was reduced
        assert after_eviction_agg_size < initial_agg_cache_size or \
               after_eviction_data_size < initial_data_store_size

    def test_multiple_symbols_under_pressure(self):
        """Test handling of multiple symbols under memory pressure"""
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 31)

        polars_data = PolarsData(
            datetime_start=start_date,
            datetime_end=end_date,
            pandas_data=None
        )

        # Very low memory limit
        polars_data.MAX_STORAGE_BYTES = 30_000  # 30KB

        # Create 20 assets (more than can fit in memory)
        assets = [Asset(f"TEST{i}", "stock") for i in range(20)]
        quote = Asset("USD", "forex")

        for asset in assets:
            dates = pl.datetime_range(
                start_date,
                start_date + timedelta(minutes=500),  # Smaller dataset
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
            polars_data._data_store[(asset, quote)] = data

            # Set _trim_iteration_count = 0 to trigger enforcement
            polars_data._trim_iteration_count = 0

            # Enforce limits after each addition
            polars_data._enforce_memory_limits()

        # Should have evicted some items
        final_size = len(polars_data._data_store)
        assert final_size < 20
        assert final_size > 0  # Should keep at least some data

    def test_no_eviction_under_limit(self):
        """Test that no eviction happens when under memory limit"""
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 31)

        polars_data = PolarsData(
            datetime_start=start_date,
            datetime_end=end_date,
            pandas_data=None
        )

        # Very high memory limit (default 1GB should be fine)
        # Create just 2 small assets
        assets = [Asset(f"TEST{i}", "stock") for i in range(2)]
        quote = Asset("USD", "forex")

        for asset in assets:
            dates = pl.datetime_range(
                start_date,
                start_date + timedelta(minutes=100),
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
            polars_data._data_store[(asset, quote)] = data

        # Aggregate both
        for asset in assets:
            polars_data._get_or_aggregate_bars(asset, quote, 50, "minute", "5 minutes")

        # Record sizes
        data_store_size = len(polars_data._data_store)
        agg_cache_size = len(polars_data._aggregated_cache)

        # Set _trim_iteration_count = 0 to trigger enforcement
        polars_data._trim_iteration_count = 0

        # Force enforcement
        polars_data._enforce_memory_limits()

        # Nothing should be evicted
        assert len(polars_data._data_store) == data_store_size
        assert len(polars_data._aggregated_cache) == agg_cache_size

    def test_eviction_updates_lru_order(self):
        """Test that eviction correctly updates LRU order"""
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 31)

        polars_data = PolarsData(
            datetime_start=start_date,
            datetime_end=end_date,
            pandas_data=None
        )

        # Set memory limit
        polars_data.MAX_STORAGE_BYTES = 60_000  # 60KB

        # Create 5 assets
        assets = [Asset(f"TEST{i}", "stock") for i in range(5)]
        quote = Asset("USD", "forex")

        for asset in assets:
            dates = pl.datetime_range(
                start_date,
                start_date + timedelta(minutes=1000),
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
            polars_data._data_store[(asset, quote)] = data

        # Aggregate all 5 in order: 0, 1, 2, 3, 4
        for asset in assets:
            polars_data._get_or_aggregate_bars(asset, quote, 100, "minute", "5 minutes")

        # Access 4 again (should move to end)
        polars_data._get_or_aggregate_bars(assets[4], quote, 100, "minute", "5 minutes")

        # Set _trim_iteration_count = 0 to trigger enforcement
        polars_data._trim_iteration_count = 0

        # Force eviction
        polars_data._enforce_memory_limits()

        # After eviction, if TEST4 is still in cache, it should be at the end
        if (assets[4], quote, "5 minutes") in polars_data._aggregated_cache:
            keys = list(polars_data._aggregated_cache.keys())
            # TEST4 should be last (most recent)
            assert keys[-1][0] == assets[4]


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-s"])
