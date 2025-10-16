"""Auto-discovery integration test for return_polars parameter compatibility.

This test ensures ALL data sources have the return_polars parameter in their
get_historical_prices method signature, preventing regression when new data
sources are added.
"""

import inspect
import importlib
import pkgutil
from pathlib import Path

import pytest

from lumibot.data_sources import DataSource


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


class TestReturnPolarsCompatibility:
    """Test suite to ensure all data sources support return_polars parameter."""

    def test_all_data_sources_have_return_polars_parameter(self):
        """Verify every DataSource subclass has return_polars in get_historical_prices."""
        data_sources = discover_all_data_source_classes()

        # Should have discovered multiple data sources
        assert len(data_sources) > 5, f"Expected to find multiple data sources, found {len(data_sources)}"

        missing_param = []

        for class_name, class_type in data_sources:
            # Check if class has get_historical_prices method
            if not hasattr(class_type, 'get_historical_prices'):
                missing_param.append(f"{class_name}: missing get_historical_prices method")
                continue

            # Get the method signature
            method = getattr(class_type, 'get_historical_prices')
            sig = inspect.signature(method)

            # Check if return_polars parameter exists
            if 'return_polars' not in sig.parameters:
                missing_param.append(
                    f"{class_name}.get_historical_prices: missing 'return_polars' parameter"
                )

        # Assert no data sources are missing the parameter
        if missing_param:
            error_msg = (
                "The following data sources are missing 'return_polars' parameter:\n" +
                "\n".join(f"  - {item}" for item in missing_param) +
                "\n\nAll data sources must accept 'return_polars: bool = False' to ensure "
                "universal compatibility with polars optimization."
            )
            pytest.fail(error_msg)

    def test_return_polars_parameter_has_correct_default(self):
        """Verify return_polars defaults to False for backward compatibility."""
        data_sources = discover_all_data_source_classes()

        incorrect_defaults = []

        for class_name, class_type in data_sources:
            if not hasattr(class_type, 'get_historical_prices'):
                continue

            method = getattr(class_type, 'get_historical_prices')
            sig = inspect.signature(method)

            if 'return_polars' in sig.parameters:
                param = sig.parameters['return_polars']

                # Check default value is False
                if param.default != False and param.default != inspect.Parameter.empty:
                    incorrect_defaults.append(
                        f"{class_name}.get_historical_prices: "
                        f"return_polars default is {param.default}, expected False"
                    )

        # Assert all defaults are False
        if incorrect_defaults:
            error_msg = (
                "The following data sources have incorrect default for 'return_polars':\n" +
                "\n".join(f"  - {item}" for item in incorrect_defaults) +
                "\n\nFor backward compatibility, return_polars must default to False."
            )
            pytest.fail(error_msg)

    def test_data_source_count_sanity_check(self):
        """Sanity check: ensure we discovered a reasonable number of data sources."""
        data_sources = discover_all_data_source_classes()
        discovered_names = [name for name, _ in data_sources]

        # Expected data sources based on what we know exists
        expected_sources = [
            'YahooData',
            'AlpacaData',
            'TradierData',
            'SchwabData',
            'InteractiveBrokersData',
            'DataBentoDataBacktestingPolars',
            'DataBentoDataPandas',
        ]

        # Check we found at least some of the expected sources
        found_expected = [name for name in expected_sources if name in discovered_names]

        assert len(found_expected) >= 3, (
            f"Expected to find at least 3 known data sources, but only found {len(found_expected)}: {found_expected}. "
            f"All discovered: {discovered_names}"
        )


class TestReturnPolarsIntegration:
    """Integration tests with stub data source to verify end-to-end behavior."""

    def test_stub_data_source_accepts_return_polars_true(self):
        """Test that a pandas-only stub source accepts return_polars=True without error."""
        import pandas as pd
        import pytz
        from lumibot.entities import Asset, Bars

        class StubPandasDataSource(DataSource):
            """Minimal stub that returns pandas DataFrames."""
            SOURCE = "STUB_PANDAS"

            def get_last_price(self, asset, quote=None, exchange=None):
                """Stub implementation."""
                return 100.0

            def get_chains(self, asset, quote=None):
                """Stub implementation."""
                return {}

            def get_historical_prices(
                self, asset, length, timestep="", timeshift=None, quote=None,
                exchange=None, include_after_hours=True, return_polars: bool = False
            ):
                # Create simple pandas DataFrame
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

                # Return Bars with return_polars flag (Bars handles conversion)
                return Bars(df, source=self.SOURCE, asset=asset, return_polars=return_polars)

        # Test calling with return_polars=True doesn't raise TypeError
        stub = StubPandasDataSource()
        asset = Asset("SPY")

        # Should not raise TypeError for unexpected keyword
        bars = stub.get_historical_prices(asset, 10, timestep="minute", return_polars=True)

        assert bars is not None
        # Bars should auto-convert to polars when return_polars=True
        import polars as pl
        assert isinstance(bars.df, pl.DataFrame), \
            "Stub with return_polars=True should produce polars DataFrame via Bars conversion"

    def test_stub_data_source_accepts_return_polars_false(self):
        """Test that stub source with return_polars=False returns pandas (default behavior)."""
        import pandas as pd
        import pytz
        from lumibot.entities import Asset, Bars

        class StubPandasDataSource(DataSource):
            """Minimal stub that returns pandas DataFrames."""
            SOURCE = "STUB_PANDAS"

            def get_last_price(self, asset, quote=None, exchange=None):
                """Stub implementation."""
                return 100.0

            def get_chains(self, asset, quote=None):
                """Stub implementation."""
                return {}

            def get_historical_prices(
                self, asset, length, timestep="", timeshift=None, quote=None,
                exchange=None, include_after_hours=True, return_polars: bool = False
            ):
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

                return Bars(df, source=self.SOURCE, asset=asset, return_polars=return_polars)

        stub = StubPandasDataSource()
        asset = Asset("SPY")

        # Default behavior (return_polars=False)
        bars = stub.get_historical_prices(asset, 10, timestep="minute", return_polars=False)

        assert bars is not None
        import pandas as pd
        assert isinstance(bars.df, pd.DataFrame), \
            "Stub with return_polars=False should return pandas DataFrame (backward compat)"
