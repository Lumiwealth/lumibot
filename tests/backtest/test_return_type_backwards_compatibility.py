"""
Test that data sources always return pandas DataFrames by default.

CRITICAL: This test ensures backwards compatibility with ALL existing strategies.
Thousands of strategies expect pandas DataFrames from get_historical_prices().
Only when return_polars=True is explicitly set should polars DataFrames be returned.
"""

import pytest
import pandas as pd
import polars as pl
from datetime import datetime
import pytz

from lumibot.backtesting.databento_backtesting_pandas import DataBentoDataBacktestingPandas
from lumibot.backtesting.databento_backtesting_polars import DataBentoDataBacktestingPolars
from lumibot.entities import Asset
from lumibot.credentials import DATABENTO_CONFIG

DATABENTO_API_KEY = DATABENTO_CONFIG.get("API_KEY")


@pytest.mark.apitest
@pytest.mark.skipif(
    not DATABENTO_API_KEY or DATABENTO_API_KEY == '<your key here>',
    reason="This test requires a Databento API key",
)
def test_pandas_data_source_returns_pandas_by_default():
    """
    Test that DataBento pandas data source returns pandas DataFrames by default.

    This is the expected behavior for backwards compatibility.
    """
    tz = pytz.timezone("America/New_York")
    start = tz.localize(datetime(2025, 9, 15, 0, 0))
    end = tz.localize(datetime(2025, 9, 16, 23, 59))
    asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)

    ds = DataBentoDataBacktestingPandas(
        datetime_start=start,
        datetime_end=end,
        api_key=DATABENTO_API_KEY,
        show_progress_bar=False,
    )

    # Call WITHOUT return_polars parameter (default behavior)
    bars = ds.get_historical_prices(asset, 10, timestep="minute")

    # CRITICAL ASSERTION: Must return pandas DataFrame by default
    assert bars is not None, "get_historical_prices returned None"
    assert hasattr(bars, 'df'), "Bars object missing .df property"

    df = bars.df
    assert isinstance(df, pd.DataFrame), (
        f"BACKWARDS COMPATIBILITY BROKEN! "
        f"get_historical_prices() returned {type(df)} instead of pandas DataFrame. "
        f"This will break ALL existing strategies!"
    )
    assert not isinstance(df, pl.DataFrame), (
        "get_historical_prices() returned polars DataFrame by default! "
        "This breaks backwards compatibility with existing strategies."
    )


@pytest.mark.apitest
@pytest.mark.skipif(
    not DATABENTO_API_KEY or DATABENTO_API_KEY == '<your key here>',
    reason="This test requires a Databento API key",
)
def test_polars_data_source_returns_pandas_by_default():
    """
    Test that DataBento POLARS data source STILL returns pandas DataFrames by default.

    CRITICAL: Even though the underlying implementation uses polars for performance,
    it MUST return pandas DataFrames by default for backwards compatibility!
    """
    tz = pytz.timezone("America/New_York")
    start = tz.localize(datetime(2025, 9, 15, 0, 0))
    end = tz.localize(datetime(2025, 9, 16, 23, 59))
    asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)

    ds = DataBentoDataBacktestingPolars(
        datetime_start=start,
        datetime_end=end,
        api_key=DATABENTO_API_KEY,
        show_progress_bar=False,
    )

    # Call WITHOUT return_polars parameter (default behavior)
    bars = ds.get_historical_prices(asset, 10, timestep="minute")

    # CRITICAL ASSERTION: Must return pandas DataFrame by default
    assert bars is not None, "get_historical_prices returned None"
    assert hasattr(bars, 'df'), "Bars object missing .df property"

    df = bars.df
    assert isinstance(df, pd.DataFrame), (
        f"BACKWARDS COMPATIBILITY BROKEN! "
        f"DataBentoDataBacktestingPolars.get_historical_prices() returned {type(df)} "
        f"instead of pandas DataFrame. This will break ALL existing strategies! "
        f"Even though the implementation uses polars internally for performance, "
        f"it MUST return pandas DataFrames by default."
    )
    assert not isinstance(df, pl.DataFrame), (
        "get_historical_prices() returned polars DataFrame by default! "
        "This breaks backwards compatibility with existing strategies."
    )


@pytest.mark.apitest
@pytest.mark.skipif(
    not DATABENTO_API_KEY or DATABENTO_API_KEY == '<your key here>',
    reason="This test requires a Databento API key",
)
def test_polars_data_source_returns_polars_when_requested():
    """
    Test that DataBento polars data source returns polars DataFrames when explicitly requested.

    This is the opt-in behavior for performance optimization.
    """
    tz = pytz.timezone("America/New_York")
    start = tz.localize(datetime(2025, 9, 15, 0, 0))
    end = tz.localize(datetime(2025, 9, 16, 23, 59))
    asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)

    ds = DataBentoDataBacktestingPolars(
        datetime_start=start,
        datetime_end=end,
        api_key=DATABENTO_API_KEY,
        show_progress_bar=False,
    )

    # Call WITH return_polars=True (explicit opt-in for performance)
    bars = ds.get_historical_prices(asset, 10, timestep="minute", return_polars=True)

    assert bars is not None, "get_historical_prices returned None"
    assert hasattr(bars, 'df'), "Bars object missing .df property"

    df = bars.df
    assert isinstance(df, pl.DataFrame), (
        f"Expected polars DataFrame when return_polars=True, got {type(df)}"
    )


@pytest.mark.apitest
@pytest.mark.skipif(
    not DATABENTO_API_KEY or DATABENTO_API_KEY == '<your key here>',
    reason="This test requires a Databento API key",
)
def test_explicit_return_polars_false():
    """
    Test that return_polars=False explicitly returns pandas DataFrames.

    This tests the explicit opt-out of polars performance optimization.
    """
    tz = pytz.timezone("America/New_York")
    start = tz.localize(datetime(2025, 9, 15, 0, 0))
    end = tz.localize(datetime(2025, 9, 16, 23, 59))
    asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)

    ds = DataBentoDataBacktestingPolars(
        datetime_start=start,
        datetime_end=end,
        api_key=DATABENTO_API_KEY,
        show_progress_bar=False,
    )

    # Call WITH return_polars=False (explicit pandas request)
    bars = ds.get_historical_prices(asset, 10, timestep="minute", return_polars=False)

    assert bars is not None, "get_historical_prices returned None"
    assert hasattr(bars, 'df'), "Bars object missing .df property"

    df = bars.df
    assert isinstance(df, pd.DataFrame), (
        f"Expected pandas DataFrame when return_polars=False, got {type(df)}"
    )
    assert not isinstance(df, pl.DataFrame), (
        "return_polars=False returned polars DataFrame!"
    )


def test_backwards_compatibility_documentation():
    """
    This is a documentation test that fails if the backwards compatibility contract is not met.

    CONTRACT:
    - get_historical_prices() MUST return pandas DataFrames by default
    - return_polars parameter MUST default to False
    - Only when return_polars=True is EXPLICITLY set should polars DataFrames be returned

    REASON:
    - Thousands of existing strategies depend on pandas DataFrame API
    - Strategies use pandas-specific operations: .rolling(), .shift(), .iloc[], etc.
    - Breaking this contract would break ALL existing strategies

    PERFORMANCE:
    - Polars is used internally for performance (filtering, storage)
    - Conversion to pandas happens once when strategy accesses data
    - This still provides 1.73x speedup while maintaining compatibility
    - Future: strategies can opt-in to polars with return_polars=True for additional speedup
    """
    from lumibot.data_sources.polars_data import PolarsData
    import inspect

    # Get the signature of get_historical_prices
    sig = inspect.signature(PolarsData.get_historical_prices)
    return_polars_param = sig.parameters.get('return_polars')

    assert return_polars_param is not None, (
        "get_historical_prices() missing return_polars parameter! "
        "This parameter is required for backwards compatibility."
    )

    assert return_polars_param.default is False, (
        f"CRITICAL BACKWARDS COMPATIBILITY BUG! "
        f"return_polars parameter defaults to {return_polars_param.default} "
        f"but MUST default to False to maintain backwards compatibility with existing strategies. "
        f"Only when return_polars=True is EXPLICITLY set should polars DataFrames be returned."
    )


@pytest.mark.apitest
@pytest.mark.skipif(
    not DATABENTO_API_KEY or DATABENTO_API_KEY == '<your key here>',
    reason="This test requires a Databento API key",
)
def test_timezone_correctness_pandas_return():
    """
    Test that pandas return from polars data source has correct timezone.

    CRITICAL: Data from DataBento is in UTC. When returning pandas DataFrames
    (return_polars=False), the timezone must be correctly converted from UTC
    to America/New_York, not just localized.

    For example:
    - UTC: 2025-09-12 18:39:00 UTC
    - Should become: 2025-09-12 14:39:00-04:00 (EDT, which is UTC-4)
    - NOT: 2025-09-12 18:39:00-04:00 (incorrect, 4 hours off)
    """
    tz = pytz.timezone("America/New_York")
    start = tz.localize(datetime(2025, 9, 15, 0, 0))
    end = tz.localize(datetime(2025, 9, 16, 23, 59))
    asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)

    # Create both pandas and polars data sources
    pandas_ds = DataBentoDataBacktestingPandas(
        datetime_start=start,
        datetime_end=end,
        api_key=DATABENTO_API_KEY,
        show_progress_bar=False,
    )
    polars_ds = DataBentoDataBacktestingPolars(
        datetime_start=start,
        datetime_end=end,
        api_key=DATABENTO_API_KEY,
        show_progress_bar=False,
    )

    # Get data with return_polars=False (default)
    pandas_bars = pandas_ds.get_historical_prices(asset, 10, timestep="minute")
    polars_bars = polars_ds.get_historical_prices(asset, 10, timestep="minute")

    assert pandas_bars is not None and polars_bars is not None

    # Get DataFrames
    pandas_df = pandas_bars.df
    polars_df = polars_bars.df  # This triggers polars→pandas conversion

    # Both should be pandas DataFrames
    assert isinstance(pandas_df, pd.DataFrame)
    assert isinstance(polars_df, pd.DataFrame)

    # CRITICAL: Check that timezones match
    # Both should have America/New_York timezone
    assert pandas_df.index.tz is not None, "Pandas DataFrame index missing timezone!"
    assert polars_df.index.tz is not None, "Polars DataFrame index missing timezone after conversion!"

    assert str(pandas_df.index.tz) == "America/New_York", \
        f"Pandas DataFrame has wrong timezone: {pandas_df.index.tz}"
    assert str(polars_df.index.tz) == "America/New_York", \
        f"Polars DataFrame has wrong timezone: {polars_df.index.tz}"

    # CRITICAL: Check that timestamps match exactly
    # If timezone conversion is wrong, times will be off by 4 hours
    assert len(pandas_df) == len(polars_df), \
        f"Row count mismatch: pandas={len(pandas_df)}, polars={len(polars_df)}"

    # Compare first and last timestamps
    pandas_first = pandas_df.index[0]
    polars_first = polars_df.index[0]
    pandas_last = pandas_df.index[-1]
    polars_last = polars_df.index[-1]

    assert pandas_first == polars_first, (
        f"TIMEZONE BUG! First timestamp mismatch:\n"
        f"  Pandas: {pandas_first}\n"
        f"  Polars: {polars_first}\n"
        f"  Difference: {(polars_first - pandas_first).total_seconds() / 3600} hours"
    )
    assert pandas_last == polars_last, (
        f"TIMEZONE BUG! Last timestamp mismatch:\n"
        f"  Pandas: {pandas_last}\n"
        f"  Polars: {polars_last}\n"
        f"  Difference: {(polars_last - pandas_last).total_seconds() / 3600} hours"
    )


@pytest.mark.apitest
@pytest.mark.skipif(
    not DATABENTO_API_KEY or DATABENTO_API_KEY == '<your key here>',
    reason="This test requires a Databento API key",
)
def test_timezone_correctness_polars_return():
    """
    Test that polars DataFrames (return_polars=True) have correct timezone-aware datetime column.

    When return_polars=True, the polars DataFrame should maintain timezone information
    in its datetime column. This ensures that when strategies or tests convert back to
    pandas, the timezone is preserved.
    """
    tz = pytz.timezone("America/New_York")
    start = tz.localize(datetime(2025, 9, 15, 0, 0))
    end = tz.localize(datetime(2025, 9, 16, 23, 59))
    asset = Asset("MES", asset_type=Asset.AssetType.CONT_FUTURE)

    polars_ds = DataBentoDataBacktestingPolars(
        datetime_start=start,
        datetime_end=end,
        api_key=DATABENTO_API_KEY,
        show_progress_bar=False,
    )

    # Get data with return_polars=True (explicit opt-in)
    polars_bars = polars_ds.get_historical_prices(asset, 10, timestep="minute", return_polars=True)

    assert polars_bars is not None
    assert hasattr(polars_bars, 'df')

    # Access the underlying polars DataFrame
    polars_df = polars_bars.df  # This should be a polars DataFrame

    assert isinstance(polars_df, pl.DataFrame), \
        f"Expected polars DataFrame with return_polars=True, got {type(polars_df)}"

    # CRITICAL: Check that datetime column has timezone
    assert "datetime" in polars_df.columns, "Polars DataFrame missing 'datetime' column"

    datetime_dtype = polars_df["datetime"].dtype
    assert hasattr(datetime_dtype, 'time_zone'), \
        f"Polars datetime column missing time_zone attribute: {datetime_dtype}"

    polars_tz = datetime_dtype.time_zone
    assert polars_tz is not None, \
        "TIMEZONE BUG! Polars DataFrame datetime column is timezone-naive"

    # Polars DataFrames should have America/New_York timezone (same as pandas)
    # Data from DataBento arrives in UTC but is converted to America/New_York by Bars class
    assert polars_tz == "America/New_York", \
        f"Expected polars DataFrame to have America/New_York timezone, got: {polars_tz}"
