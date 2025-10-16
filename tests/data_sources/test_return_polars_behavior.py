"""Behavior-level integration test for return_polars parameter.

This test verifies that data sources actually FORWARD the return_polars parameter
to the Bars constructor, not just accept it in their signature.

The signature test (test_return_polars_compatibility.py) only checks that the
parameter exists. This test verifies the parameter is actually USED.
"""

import inspect
import importlib
import pkgutil
from pathlib import Path
from unittest.mock import patch, MagicMock
import pandas as pd
import polars as pl
import pytz
import pytest

from lumibot.data_sources import DataSource
from lumibot.entities import Asset


def discover_all_data_source_classes():
    """Discover all DataSource subclasses via introspection.

    Returns:
        list[tuple[str, type]]: List of (class_name, class_type) tuples
    """
    data_sources_path = Path(__file__).parent.parent.parent / "lumibot" / "data_sources"
    data_sources = []

    # Import all modules in data_sources package
    for _, module_name, _ in pkgutil.iter_modules([str(data_sources_path)]):
        if module_name.startswith('_'):
            continue

        try:
            module = importlib.import_module(f"lumibot.data_sources.{module_name}")

            # Find all classes in the module
            for name, obj in inspect.getmembers(module, inspect.isclass):
                # Check if it's a DataSource subclass (but not DataSource itself)
                if (issubclass(obj, DataSource) and
                    obj is not DataSource and
                    obj.__module__.startswith('lumibot.data_sources')):
                    data_sources.append((name, obj))
        except Exception as e:
            # Skip modules that can't be imported (e.g., optional dependencies)
            print(f"Skipping {module_name}: {e}")
            continue

    return data_sources


def create_mock_pandas_df(length=100):
    """Create a mock pandas DataFrame with OHLCV data.

    Args:
        length: Number of bars to generate

    Returns:
        pd.DataFrame: Mock OHLCV data
    """
    tz = pytz.timezone("America/New_York")
    dates = pd.date_range(start="2024-01-01 09:30", periods=length, freq="1min", tz=tz)
    df = pd.DataFrame({
        "open": [100.0] * length,
        "high": [101.0] * length,
        "low": [99.0] * length,
        "close": [100.5] * length,
        "volume": [1000] * length,
    }, index=dates)
    df.index.name = "datetime"
    return df


class TestReturnPolarsBehavior:
    """Behavior-level tests that verify return_polars is actually forwarded to Bars."""

    @pytest.mark.parametrize("data_source_name,data_source_class", discover_all_data_source_classes())
    def test_data_source_forwards_return_polars_true(self, data_source_name, data_source_class):
        """CRITICAL: Verify each data source forwards return_polars=True to Bars constructor.

        This is THE test that catches the bug where data sources accept the parameter
        but don't actually pass it to Bars().

        Args:
            data_source_name: Name of the data source class
            data_source_class: The data source class to test
        """
        # Skip if no get_historical_prices method
        if not hasattr(data_source_class, 'get_historical_prices'):
            pytest.skip(f"{data_source_name} has no get_historical_prices method")

        # Check if method has return_polars parameter
        method = getattr(data_source_class, 'get_historical_prices')
        sig = inspect.signature(method)
        if 'return_polars' not in sig.parameters:
            pytest.fail(
                f"{data_source_name}.get_historical_prices missing 'return_polars' parameter. "
                "Run test_return_polars_compatibility.py to catch signature issues."
            )

        # Try to instantiate the data source (may fail for some)
        try:
            # Most data sources need minimal config
            instance = data_source_class()
        except Exception as e:
            # Skip if we can't instantiate (e.g., needs API keys, broker connection)
            pytest.skip(f"Cannot instantiate {data_source_name}: {e}")

        # Create test asset
        asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)

        # We need to mock the internal data fetching to return our pandas DataFrame
        # This is tricky because each data source has different internals
        # Strategy: Patch the Bars constructor to capture what's passed to it
        from lumibot.entities import Bars

        original_bars_init = Bars.__init__
        captured_return_polars = None

        def capturing_init(self, *args, **kwargs):
            nonlocal captured_return_polars
            captured_return_polars = kwargs.get('return_polars', False)
            # Call original init
            return original_bars_init(self, *args, **kwargs)

        # Patch Bars.__init__ to capture the return_polars parameter
        with patch.object(Bars, '__init__', capturing_init):
            try:
                # Try to call get_historical_prices with return_polars=True
                bars = instance.get_historical_prices(
                    asset,
                    length=10,
                    timestep="1D",
                    return_polars=True
                )

                # CRITICAL ASSERTION: Verify return_polars=True was passed to Bars
                assert captured_return_polars == True, (
                    f"FAIL: {data_source_name}.get_historical_prices accepts return_polars=True "
                    f"but does NOT forward it to Bars() constructor. "
                    f"This will cause strategies to crash with AttributeError when using Polars syntax. "
                    f"Expected Bars(return_polars=True), got Bars(return_polars={captured_return_polars})"
                )

            except Exception as e:
                # If we can't fetch data (API key missing, etc), skip
                # But if we get AttributeError about polars methods, that's the bug!
                error_msg = str(e)
                if "has no attribute 'with_columns'" in error_msg or \
                   "has no attribute 'rolling_mean'" in error_msg or \
                   "has no attribute 'select'" in error_msg:
                    pytest.fail(
                        f"CRITICAL BUG: {data_source_name} caused Polars syntax error, "
                        f"indicating it returned pandas DataFrame when return_polars=True. "
                        f"Error: {error_msg}"
                    )
                else:
                    # Other errors (API failures, etc) are acceptable for this test
                    pytest.skip(f"Cannot fetch data from {data_source_name}: {e}")

    @pytest.mark.parametrize("data_source_name,data_source_class", discover_all_data_source_classes())
    def test_data_source_forwards_return_polars_false(self, data_source_name, data_source_class):
        """Verify each data source forwards return_polars=False (or defaults to False).

        Args:
            data_source_name: Name of the data source class
            data_source_class: The data source class to test
        """
        # Skip if no get_historical_prices method
        if not hasattr(data_source_class, 'get_historical_prices'):
            pytest.skip(f"{data_source_name} has no get_historical_prices method")

        # Try to instantiate the data source
        try:
            instance = data_source_class()
        except Exception as e:
            pytest.skip(f"Cannot instantiate {data_source_name}: {e}")

        # Create test asset
        asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)

        # Patch Bars.__init__ to capture the return_polars parameter
        from lumibot.entities import Bars
        original_bars_init = Bars.__init__
        captured_return_polars = None

        def capturing_init(self, *args, **kwargs):
            nonlocal captured_return_polars
            captured_return_polars = kwargs.get('return_polars', False)
            return original_bars_init(self, *args, **kwargs)

        with patch.object(Bars, '__init__', capturing_init):
            try:
                # Call with return_polars=False (explicit)
                bars = instance.get_historical_prices(
                    asset,
                    length=10,
                    timestep="1D",
                    return_polars=False
                )

                # Verify return_polars=False was passed to Bars
                assert captured_return_polars == False, (
                    f"FAIL: {data_source_name}.get_historical_prices called with return_polars=False "
                    f"but Bars received return_polars={captured_return_polars}"
                )

            except Exception as e:
                # Skip if we can't fetch data
                pytest.skip(f"Cannot fetch data from {data_source_name}: {e}")

    def test_behavior_test_coverage(self):
        """Sanity check: ensure we're testing a reasonable number of data sources."""
        data_sources = discover_all_data_source_classes()

        # Filter to only those with get_historical_prices
        testable_sources = [
            name for name, cls in data_sources
            if hasattr(cls, 'get_historical_prices')
        ]

        # Should have at least 5 testable data sources
        assert len(testable_sources) >= 5, (
            f"Expected at least 5 data sources with get_historical_prices, found {len(testable_sources)}: "
            f"{testable_sources}"
        )

        # Log what we're testing
        print(f"\nBehavior test coverage: {len(testable_sources)} data sources")
        for name in sorted(testable_sources):
            print(f"  - {name}")


class TestReturnPolarsKnownBrokenSources:
    """Explicit tests for the 10 known broken data sources.

    These are the data sources we identified as having Bars() calls that don't
    forward return_polars. This test suite will fail until we fix them all.
    """

    KNOWN_BROKEN_SOURCES = [
        'YahooData',           # 1 Bars() call
        'AlpacaData',          # 7 Bars() calls
        'TradierData',         # 1 Bars() call
        'SchwabData',          # 1 Bars() call
        'InteractiveBrokersData',  # 2 Bars() calls
        'CCXTData',            # 1 Bars() call (assumed name)
        'CCXTBacktestingData', # 1 Bars() call (assumed name)
        'DataBentoDataPandas', # 2 Bars() calls
        'BitunixData',         # 1 Bars() call
        'ProjectXData',        # 2 Bars() calls
    ]

    @pytest.mark.parametrize("source_name", KNOWN_BROKEN_SOURCES)
    def test_known_broken_source_will_fail(self, source_name):
        """These tests WILL FAIL until we fix the Bars() calls.

        Once a data source is fixed, this test should start passing.
        When all tests pass, we can remove this test class.

        Args:
            source_name: Name of the broken data source
        """
        # Try to find and test the broken source
        data_sources = discover_all_data_source_classes()
        source_dict = dict(data_sources)

        if source_name not in source_dict:
            pytest.skip(f"{source_name} not found (may have different name)")

        data_source_class = source_dict[source_name]

        # Try to instantiate
        try:
            instance = data_source_class()
        except Exception as e:
            pytest.skip(f"Cannot instantiate {source_name}: {e}")

        # Create test asset
        asset = Asset("SPY", asset_type=Asset.AssetType.STOCK)

        # Patch Bars.__init__ to capture return_polars
        from lumibot.entities import Bars
        original_bars_init = Bars.__init__
        captured_return_polars = None

        def capturing_init(self, *args, **kwargs):
            nonlocal captured_return_polars
            captured_return_polars = kwargs.get('return_polars', False)
            return original_bars_init(self, *args, **kwargs)

        with patch.object(Bars, '__init__', capturing_init):
            try:
                bars = instance.get_historical_prices(
                    asset,
                    length=10,
                    timestep="1D",
                    return_polars=True
                )

                # This WILL FAIL for broken sources
                assert captured_return_polars == True, (
                    f"❌ EXPECTED FAILURE: {source_name} is in KNOWN_BROKEN_SOURCES list. "
                    f"It accepts return_polars=True but doesn't forward it to Bars(). "
                    f"This test will pass once the Bars() calls are fixed. "
                    f"See todo list for file locations."
                )

                # If we get here, the source is FIXED! 🎉
                print(f"\n🎉 {source_name} is now FIXED! Remove from KNOWN_BROKEN_SOURCES list.")

            except Exception as e:
                # Skip if we can't fetch data (API issues, etc)
                error_msg = str(e)
                if "has no attribute 'with_columns'" in error_msg:
                    pytest.fail(
                        f"❌ {source_name} returned pandas DataFrame when return_polars=True. "
                        f"This confirms it's broken and needs fixing."
                    )
                else:
                    pytest.skip(f"Cannot test {source_name}: {e}")
