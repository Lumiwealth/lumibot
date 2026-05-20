"""Regression coverage for Data, DataPolars, Bars, and provider-boundary Polars behavior."""

from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from unittest.mock import PropertyMock, patch

import pandas as pd
import polars as pl
import pytest

from lumibot.data_sources.polars_data import PolarsData
from lumibot.entities import Asset, Bars, Data, DataPolars


def _create_mock_ohlc_data(start: datetime, periods: int = 300) -> pd.DataFrame:
    """Create mock OHLC data for testing.

    Args:
        start: Starting datetime (must be timezone-aware)
        periods: Number of minute bars to generate

    Returns:
        DataFrame with OHLC data indexed by timestamp
    """
    index = pd.date_range(start=start, periods=periods, freq="1min", tz=timezone.utc)
    data = {
        "open": [200 + i * 0.1 for i in range(periods)],
        "high": [201 + i * 0.1 for i in range(periods)],
        "low": [199 + i * 0.1 for i in range(periods)],
        "close": [200.5 + i * 0.1 for i in range(periods)],
        "volume": [10000 + i * 100 for i in range(periods)],
    }
    return pd.DataFrame(data, index=index)


def _create_data_pair(start: datetime, periods: int = 300, df_mutator=None):
    mock_df = _create_mock_ohlc_data(start, periods=periods)
    if df_mutator is not None:
        df_mutator(mock_df)

    asset = Asset("HIMS", asset_type=Asset.AssetType.STOCK)
    data_pandas = Data(
        asset=asset,
        df=mock_df.copy(),
        timestep="minute",
        quote=asset,
    )

    mock_df_reset = mock_df.reset_index()
    mock_df_reset.columns = ["datetime", *mock_df.columns]
    mock_polars = pl.from_pandas(mock_df_reset)
    data_polars = DataPolars(
        asset=asset,
        df=mock_polars,
        timestep="minute",
        quote=asset,
    )
    return data_pandas, data_polars


def _assert_frame_values_equal(left: pd.DataFrame, right: pd.DataFrame):
    pd.testing.assert_frame_equal(
        left,
        right,
        check_dtype=False,
        check_freq=False,
    )


class ProviderBoundaryPolarsData(PolarsData):
    """Small provider-like source used to test PolarsData integration boundaries."""

    SOURCE = "GENERIC_PROVIDER"

    def __init__(self, response, *args, **kwargs):
        super().__init__(*args, pandas_data=[], **kwargs)
        self.response = response
        self.calls = []

    def _pull_source_symbol_bars(
        self,
        asset,
        length,
        timestep="",
        timeshift=0,
        quote=None,
        exchange=None,
        include_after_hours=True,
        return_polars=False,
    ):
        self.calls.append(
            {
                "asset": asset,
                "length": length,
                "timestep": timestep,
                "quote": quote,
                "return_polars": return_polars,
            }
        )
        return self.response


class LegacyGetBarsData:
    timestep = "minute"

    def __init__(self, response, failure_message=None):
        self.response = response
        self.failure_message = failure_message

    def get_bars(self, *args, **kwargs):
        if "return_polars" in kwargs:
            raise TypeError(self.failure_message or "got an unexpected keyword argument 'return_polars'")
        return self.response


def test_data_polars_row_count_parity():
    """
    Test that Data and DataPolars return the same number of rows for identical requests.

    This reproduces the bug where:
    - Data.get_bars(length=2, timeshift=-2) returns 2 rows
    - DataPolars.get_bars(length=2, timeshift=-2) returns 234 rows
    """
    # Create mock data starting at market open
    start = datetime(2024, 7, 18, 9, 30, tzinfo=timezone.utc)
    mock_df = _create_mock_ohlc_data(start, periods=300)

    # Create asset
    asset = Asset("HIMS", asset_type=Asset.AssetType.STOCK)

    # Create Data instance (pandas mode)
    data_pandas = Data(
        asset=asset,
        df=mock_df.copy(),
        timestep="minute",
        quote=asset,
    )

    # Create DataPolars instance (polars mode)
    # Convert to polars format with datetime column
    mock_df_reset = mock_df.reset_index()
    mock_df_reset.columns = ["datetime", "open", "high", "low", "close", "volume"]
    mock_polars = pl.from_pandas(mock_df_reset)

    data_polars = DataPolars(
        asset=asset,
        df=mock_polars,
        timestep="minute",
        quote=asset,
    )

    # Test at a specific datetime (10:00 AM = 30 minutes after market open)
    test_dt = datetime(2024, 7, 18, 10, 0, tzinfo=timezone.utc)

    # Request 2 bars with timeshift=-2
    # This should return bars at 09:58 and 09:59
    # get_bars() returns DataFrames directly
    df_pandas = data_pandas.get_bars(
        dt=test_dt,
        length=2,
        timestep="minute",
        timeshift=-2
    )

    df_polars = data_polars.get_bars(
        dt=test_dt,
        length=2,
        timestep="minute",
        timeshift=-2
    )

    # CRITICAL ASSERTIONS
    assert len(df_pandas) == 2, f"Pandas should return 2 rows, got {len(df_pandas)}"
    assert len(df_polars) == 2, f"Polars should return 2 rows, got {len(df_polars)}"
    assert len(df_pandas) == len(df_polars), (
        f"Row count mismatch! Pandas returned {len(df_pandas)} rows, "
        f"Polars returned {len(df_polars)} rows"
    )


def test_data_polars_timeshift_timedelta():
    """
    Test timeshift parameter handling when passed as timedelta.

    Tests the conversion of timedelta(minutes=-2) to integer offset.
    """
    start = datetime(2024, 7, 18, 9, 30, tzinfo=timezone.utc)
    mock_df = _create_mock_ohlc_data(start, periods=300)

    asset = Asset("HIMS", asset_type=Asset.AssetType.STOCK)

    # Create Data instance
    data_pandas = Data(
        asset=asset,
        df=mock_df.copy(),
        timestep="minute",
        quote=asset,
    )

    # Create DataPolars instance
    mock_df_reset = mock_df.reset_index()
    mock_df_reset.columns = ["datetime", "open", "high", "low", "close", "volume"]
    mock_polars = pl.from_pandas(mock_df_reset)

    data_polars = DataPolars(
        asset=asset,
        df=mock_polars,
        timestep="minute",
        quote=asset,
    )

    test_dt = datetime(2024, 7, 18, 10, 0, tzinfo=timezone.utc)

    # Test with timedelta parameter (this is what the backtest engine uses)
    timeshift_td = timedelta(minutes=-2)

    # get_bars() returns DataFrames directly
    df_pandas = data_pandas.get_bars(
        dt=test_dt,
        length=2,
        timestep="minute",
        timeshift=timeshift_td
    )

    df_polars = data_polars.get_bars(
        dt=test_dt,
        length=2,
        timestep="minute",
        timeshift=timeshift_td
    )

    assert len(df_pandas) == 2, "Pandas should return 2 rows with timedelta timeshift"
    assert len(df_polars) == 2, "Polars should return 2 rows with timedelta timeshift"
    assert len(df_pandas) == len(df_polars), "Row count mismatch with timedelta timeshift"


@pytest.mark.parametrize(
    "requested_timestep,length,current_dt",
    [
        ("5minute", 4, datetime(2024, 7, 18, 12, 15, tzinfo=timezone.utc)),
        ("hour", 3, datetime(2024, 7, 18, 14, 30, tzinfo=timezone.utc)),
        ("day", 1, datetime(2024, 7, 19, 12, 30, tzinfo=timezone.utc)),
    ],
)
def test_data_polars_resample_value_parity(requested_timestep, length, current_dt):
    data_pandas, data_polars = _create_data_pair(
        datetime(2024, 7, 18, 0, 0, tzinfo=timezone.utc),
        periods=3_000,
    )

    df_pandas = data_pandas.get_bars(
        dt=current_dt,
        length=length,
        timestep=requested_timestep,
        timeshift=0,
    )
    df_polars = data_polars.get_bars(
        dt=current_dt,
        length=length,
        timestep=requested_timestep,
        timeshift=0,
    )

    _assert_frame_values_equal(df_polars, df_pandas)


@pytest.mark.parametrize("requested_timestep", ["15minute", "hour"])
def test_data_polars_between_dates_resample_value_parity(requested_timestep):
    start = datetime(2024, 7, 18, 0, 0, tzinfo=timezone.utc)
    data_pandas, data_polars = _create_data_pair(start, periods=1_000)

    start_date = start + timedelta(hours=2)
    end_date = start + timedelta(hours=9, minutes=30)

    df_pandas = data_pandas.get_bars_between_dates(
        timestep=requested_timestep,
        start_date=start_date,
        end_date=end_date,
    )
    df_polars = data_polars.get_bars_between_dates(
        timestep=requested_timestep,
        start_date=start_date,
        end_date=end_date,
    )

    _assert_frame_values_equal(df_polars, df_pandas)


def test_data_polars_resample_ignores_extra_nan_columns_like_data():
    def mutate(df):
        df["bid"] = None
        df["volume"] = None

    data_pandas, data_polars = _create_data_pair(
        datetime(2024, 7, 18, 9, 30, tzinfo=timezone.utc),
        periods=300,
        df_mutator=mutate,
    )
    test_dt = datetime(2024, 7, 18, 10, 0, tzinfo=timezone.utc)

    df_pandas = data_pandas.get_bars(
        dt=test_dt,
        length=2,
        timestep="5minute",
        timeshift=0,
    )
    df_polars = data_polars.get_bars(
        dt=test_dt,
        length=2,
        timestep="5minute",
        timeshift=0,
    )

    _assert_frame_values_equal(df_polars, df_pandas)
    assert "bid" not in df_polars.columns
    assert df_polars["volume"].isna().sum() == 0
    assert all(float(v) == 0.0 for v in df_polars["volume"])


def test_provider_return_polars_keeps_internal_response_polars():
    start = datetime(2024, 7, 18, 0, 0, tzinfo=timezone.utc)
    data_pandas, data_polars = _create_data_pair(start, periods=1_000)
    asset = data_polars.asset
    quote = data_polars.quote
    current_dt = start + timedelta(hours=10)

    polars_source = PolarsData(
        datetime_start=start,
        datetime_end=start + timedelta(days=1),
        pandas_data=[data_polars],
    )
    polars_source._datetime = current_dt

    with patch.object(Bars, "pandas_df", new_callable=PropertyMock) as mock_pandas_df:
        mock_pandas_df.side_effect = AssertionError("return_polars=True should not force pandas conversion")
        bars = polars_source.get_historical_prices(
            asset,
            length=4,
            timestep="5minute",
            quote=quote,
            return_polars=True,
        )

    assert bars is not None
    assert isinstance(bars.df, pl.DataFrame)
    assert bars.polars_df.height == 4
    mock_pandas_df.assert_not_called()

    pandas_bars = polars_source.get_historical_prices(
        asset,
        length=4,
        timestep="5minute",
        quote=quote,
        return_polars=False,
    )
    _assert_frame_values_equal(bars.pandas_df, pandas_bars.pandas_df)


def test_return_polars_fallback_only_swallows_unsupported_keyword_typeerror():
    asset = Asset("LEGACY", asset_type=Asset.AssetType.STOCK)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    source = PolarsData(
        datetime_start=datetime(2024, 7, 18, tzinfo=timezone.utc),
        datetime_end=datetime(2024, 7, 19, tzinfo=timezone.utc),
        pandas_data=[],
    )
    response = _create_mock_ohlc_data(datetime(2024, 7, 18, tzinfo=timezone.utc), periods=1)
    source._data_store = OrderedDict({(asset, quote, "minute"): LegacyGetBarsData(response)})

    bars = source.get_historical_prices(asset, length=1, timestep="minute", quote=quote, return_polars=True)
    assert bars is not None
    assert len(bars.df) == 1

    source._data_store = OrderedDict({
        (asset, quote, "minute"): LegacyGetBarsData(response, failure_message="internal type bug")
    })
    with pytest.raises(TypeError, match="internal type bug"):
        source.get_historical_prices(asset, length=1, timestep="minute", quote=quote, return_polars=True)


@pytest.mark.parametrize("provider_frame_type", ["polars", "pandas"])
def test_provider_boundary_threads_return_polars_and_preserves_quote_pair(provider_frame_type):
    start = datetime(2024, 7, 18, 9, 30, tzinfo=timezone.utc)
    pandas_frame = _create_mock_ohlc_data(start, periods=5)
    polars_frame = pl.from_pandas(
        pandas_frame.reset_index().rename(columns={"index": "datetime"})
    )
    provider_response = polars_frame if provider_frame_type == "polars" else pandas_frame
    base = Asset("ETH", asset_type=Asset.AssetType.CRYPTO)
    quote = Asset("USD", asset_type=Asset.AssetType.FOREX)
    source = ProviderBoundaryPolarsData(
        provider_response,
        datetime_start=start,
        datetime_end=start + timedelta(hours=1),
    )

    bars = source.get_historical_prices(
        (base, quote),
        length=3,
        timestep="minute",
        return_polars=True,
    )

    assert source.calls[-1]["return_polars"] is True
    assert source.calls[-1]["asset"] == (base, quote)
    assert bars.asset == base
    assert bars.quote == quote
    assert bars.source == "GENERIC_PROVIDER"
    assert isinstance(bars.df, pl.DataFrame)

    pandas_bars = source.get_historical_prices(
        (base, quote),
        length=3,
        timestep="minute",
        return_polars=False,
    )

    assert source.calls[-1]["return_polars"] is False
    assert isinstance(pandas_bars.df, pd.DataFrame)
    assert pandas_bars.asset == base
    assert pandas_bars.quote == quote


def test_data_get_bars_fast_path_does_not_drop_on_nan_extra_columns():
    """
    Ensure the Data.get_bars() fast-path matches legacy resample semantics.

    Historically, get_bars() resampled/aggregated OHLCV and therefore ignored unrelated columns
    (e.g. bid/ask) when dropping NaNs. The fast-path must not drop rows just because an extra
    column is NaN.
    """
    start = datetime(2024, 7, 18, 9, 30, tzinfo=timezone.utc)
    mock_df = _create_mock_ohlc_data(start, periods=300)

    # Add an "extra" column that is entirely NaN. The output should still include bars.
    mock_df["bid"] = None

    # Make volume NaN everywhere. The legacy resample path turns NaN volumes into 0 via `sum`,
    # so the fast-path must do the same.
    mock_df["volume"] = None

    asset = Asset("HIMS", asset_type=Asset.AssetType.STOCK)
    data_pandas = Data(
        asset=asset,
        df=mock_df.copy(),
        timestep="minute",
        quote=asset,
    )

    test_dt = datetime(2024, 7, 18, 10, 0, tzinfo=timezone.utc)
    df_bars = data_pandas.get_bars(
        dt=test_dt,
        length=2,
        timestep="minute",
        timeshift=-2,
    )

    assert len(df_bars) == 2
    assert "bid" not in df_bars.columns, "Extra columns should not be returned by get_bars()"
    assert df_bars["volume"].isna().sum() == 0, "NaN volume should be normalized to 0 (resample parity)"
    assert all(float(v) == 0.0 for v in df_bars["volume"]), "Expected filled volume to be 0.0 for NaN inputs"


if __name__ == "__main__":
    # Run tests with verbose output
    pytest.main([__file__, "-v", "-s"])
