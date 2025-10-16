"""Test automatic pandas↔polars conversion in Bars class.

This test suite verifies that Bars automatically converts between pandas and polars
DataFrames based on the return_polars flag, ensuring universal compatibility regardless
of the data source implementation.
"""
import pytest
import pandas as pd
import polars as pl
from datetime import datetime
import pytz

from lumibot.entities import Bars, Asset


class TestBarsPolarsConversion:
    """Lean test suite for Bars automatic conversion logic."""

    @pytest.fixture
    def test_pandas_df(self):
        """Create test pandas DataFrame with timezone-aware index."""
        tz = pytz.timezone("America/New_York")
        dates = pd.date_range(start="2024-01-01 09:30", periods=10, freq="1min", tz=tz)
        df = pd.DataFrame({
            "open": [100.0 + i * 0.1 for i in range(10)],
            "high": [101.0 + i * 0.1 for i in range(10)],
            "low": [99.0 + i * 0.1 for i in range(10)],
            "close": [100.5 + i * 0.1 for i in range(10)],
            "volume": [1000 + i * 100 for i in range(10)],
        }, index=dates)
        df.index.name = "datetime"
        return df

    @pytest.fixture
    def test_polars_df(self):
        """Create test polars DataFrame with timezone-aware datetime column."""
        tz = pytz.timezone("America/New_York")
        dates = pd.date_range(start="2024-01-01 09:30", periods=10, freq="1min", tz=tz)

        # Create polars DataFrame with proper datetime type
        df = pl.DataFrame({
            "datetime": dates,  # Pass pandas DatetimeIndex directly
            "open": [100.0 + i * 0.1 for i in range(10)],
            "high": [101.0 + i * 0.1 for i in range(10)],
            "low": [99.0 + i * 0.1 for i in range(10)],
            "close": [100.5 + i * 0.1 for i in range(10)],
            "volume": [1000 + i * 100 for i in range(10)],
        })

        return df

    # Test Case 1: Pandas input, return_polars=False (default backward compat)
    def test_pandas_input_return_polars_false(self, test_pandas_df):
        """Pandas DataFrame + return_polars=False → pandas Bars.df (backward compat)"""
        bars = Bars(test_pandas_df, source="test", asset=Asset("SPY"), return_polars=False)

        assert isinstance(bars.df, pd.DataFrame), "Expected pandas DataFrame"
        assert bars._return_polars is False
        assert len(bars.df) == 10

    # Test Case 2: Pandas input, return_polars=True (AUTO-CONVERTS!)
    def test_pandas_input_return_polars_true_auto_converts(self, test_pandas_df):
        """Pandas DataFrame + return_polars=True → auto-converts to polars (KEY TEST)"""
        bars = Bars(test_pandas_df, source="test", asset=Asset("SPY"), return_polars=True)

        # CRITICAL: Must return polars DataFrame (auto-converted from pandas)
        assert isinstance(bars.df, pl.DataFrame), \
            "FAILED: Pandas input with return_polars=True should auto-convert to polars"
        assert bars._return_polars is True
        assert bars.df.height == 10

    # Test Case 3: Polars input, return_polars=True (native polars, no conversion)
    def test_polars_input_return_polars_true(self, test_polars_df):
        """Polars DataFrame + return_polars=True → native polars (no conversion)"""
        bars = Bars(test_polars_df, source="test", asset=Asset("SPY"), return_polars=True)

        assert isinstance(bars.df, pl.DataFrame), "Expected polars DataFrame"
        assert bars._return_polars is True
        assert bars.df.height == 10

    # Test Case 4: Polars input, return_polars=False (converts to pandas)
    def test_polars_input_return_polars_false(self, test_polars_df):
        """Polars DataFrame + return_polars=False → converts to pandas"""
        bars = Bars(test_polars_df, source="test", asset=Asset("SPY"), return_polars=False)

        assert isinstance(bars.df, pd.DataFrame), "Expected pandas DataFrame"
        assert bars._return_polars is False
        assert len(bars.df) == 10

    # Test Case 5: Timezone preservation during conversion
    def test_timezone_preserved_pandas_to_polars(self, test_pandas_df):
        """Timezone is preserved during pandas→polars conversion"""
        bars = Bars(test_pandas_df, source="test", asset=Asset("SPY"), return_polars=True)

        polars_df = bars.df
        assert isinstance(polars_df, pl.DataFrame)

        # Check timezone preserved
        datetime_dtype = polars_df["datetime"].dtype
        assert datetime_dtype.time_zone == "America/New_York", \
            f"Timezone not preserved, got: {datetime_dtype.time_zone}"

    # Test Case 6: Data integrity after conversion
    def test_data_integrity_after_conversion(self, test_pandas_df):
        """Data values match after pandas→polars conversion"""
        original_close_first = test_pandas_df["close"].iloc[0]
        original_close_last = test_pandas_df["close"].iloc[-1]

        bars = Bars(test_pandas_df, source="test", asset=Asset("SPY"), return_polars=True)

        polars_df = bars.df
        converted_close_first = polars_df["close"][0]
        converted_close_last = polars_df["close"][-1]

        assert abs(original_close_first - converted_close_first) < 0.001, \
            f"First close mismatch: {original_close_first} != {converted_close_first}"
        assert abs(original_close_last - converted_close_last) < 0.001, \
            f"Last close mismatch: {original_close_last} != {converted_close_last}"

    # Test Case 7: Cache behavior (no duplicate conversion)
    def test_cache_behavior_no_duplicate_conversion(self, test_pandas_df):
        """Conversion happens once, subsequent .df accesses use cache (object identity check)"""
        bars = Bars(test_pandas_df, source="test", asset=Asset("SPY"), return_polars=True)

        # First access triggers conversion
        df1 = bars.df
        # Second access should use cache (same object)
        df2 = bars.df

        # Should be the exact same object (not reconverted)
        assert df1 is df2, "Cache not working: df accessed twice created different objects"

    # Test Case 8: pandas_df property still works
    def test_pandas_df_property_with_polars_storage(self, test_pandas_df):
        """bars.pandas_df property works even when polars is stored internally"""
        bars = Bars(test_pandas_df, source="test", asset=Asset("SPY"), return_polars=True)

        # .df should be polars
        assert isinstance(bars.df, pl.DataFrame)

        # .pandas_df should still work (convert back on demand)
        assert isinstance(bars.pandas_df, pd.DataFrame)
        assert len(bars.pandas_df) == 10

    # Test Case 9: polars_df property works with pandas storage
    def test_polars_df_property_with_pandas_storage(self, test_pandas_df):
        """bars.polars_df property works even when pandas is stored internally"""
        bars = Bars(test_pandas_df, source="test", asset=Asset("SPY"), return_polars=False)

        # .df should be pandas
        assert isinstance(bars.df, pd.DataFrame)

        # .polars_df should still work (convert on demand)
        assert isinstance(bars.polars_df, pl.DataFrame)
        assert bars.polars_df.height == 10

    # Test Case 10: Default behavior (no return_polars specified)
    def test_default_behavior_is_pandas(self, test_pandas_df):
        """Default behavior (no return_polars flag) returns pandas for backward compat"""
        # Don't specify return_polars (defaults to False)
        bars = Bars(test_pandas_df, source="test", asset=Asset("SPY"))

        # Should default to pandas
        assert isinstance(bars.df, pd.DataFrame), \
            "Default behavior should be pandas for backward compatibility"


class TestStubDataSourceIntegration:
    """Integration test with stub data source to verify end-to-end behavior."""

    class StubPandasDataSource:
        """Stub data source that only returns pandas (like Yahoo, etc.)"""

        def get_historical_prices(self, asset, length, timestep, return_polars=False):
            """Simulate a pandas-only data source"""
            # Create pandas DataFrame
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

            # Return Bars with the flag
            return Bars(df, source="stub", asset=asset, return_polars=return_polars)

    def test_pandas_source_with_return_polars_auto_converts(self):
        """Pandas-only source + return_polars=True → auto-converts to polars"""
        stub = self.StubPandasDataSource()

        # Request polars from a pandas-only source
        bars = stub.get_historical_prices(
            Asset("SPY"),
            10,
            "minute",
            return_polars=True  # Request polars even though source is pandas-only
        )

        # Should auto-convert to polars
        assert bars is not None
        assert isinstance(bars.df, pl.DataFrame), \
            "CRITICAL: Pandas-only source with return_polars=True should auto-convert"
        assert bars.df.height == 10

    def test_pandas_source_without_return_polars(self):
        """Pandas-only source without return_polars → returns pandas (default)"""
        stub = self.StubPandasDataSource()

        # Don't request polars
        bars = stub.get_historical_prices(Asset("SPY"), 10, "minute", return_polars=False)

        # Should stay pandas
        assert bars is not None
        assert isinstance(bars.df, pd.DataFrame)
        assert len(bars.df) == 10
