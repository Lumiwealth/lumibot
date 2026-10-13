"""Corporate-action alignment must preserve recorded events, not copy prior events."""

import numpy as np
import pandas as pd
import polars as pl
import pytest

from lumibot.data_sources import PandasData, PolarsData
from lumibot.entities import Asset, Bars, Data, DataPolars

BACKENDS = (Data, DataPolars)


def _frame(index, dividends=None, splits=None):
    """Build a constant-price fixture so action returns have an independent oracle."""
    frame = pd.DataFrame(
        {"open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0, "volume": 1.0},
        index=index,
    )
    if dividends is not None:
        frame["dividend"] = dividends
    if splits is not None:
        frame["stock_splits"] = splits
    return frame


def _data(backend, frame, timestep="day", symbol="SPY"):
    """Use real constructors and preserve a separate input for fixture assertions."""
    value = pl.from_pandas(frame.reset_index()) if backend is DataPolars else frame.copy()
    return backend(Asset(symbol), value, timestep=timestep)


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("timestep,frequency", [("day", "D"), ("hour", "h"), ("minute", "min")])
@pytest.mark.parametrize("timezone", ["America/New_York", "UTC"])
def test_sparse_actions_keep_exact_timestamps_and_datalines(backend, timestep, frequency, timezone):
    """Inserted rows and original nulls are neutral; real and reverse splits survive."""
    index = pd.date_range("2026-10-12 09:30", periods=5, freq=frequency, tz=timezone, name="datetime")
    frame = _frame(index[[0, 2, 4]], [1.0, np.nan, 0.5], [2.0, np.nan, 0.5])
    data = _data(backend, frame, timestep)
    original = data.df
    original_values = original.copy(deep=True)

    data.repair_times_and_fill(index)

    expected_dividends = [1.0, 0.0, 0.0, 0.0, 0.5]
    expected_splits = [2.0, 0.0, 0.0, 0.0, 0.5]
    pd.testing.assert_index_equal(data.df.index, index)
    assert data.df["dividend"].tolist() == expected_dividends
    assert data.df["stock_splits"].tolist() == expected_splits
    assert data.df.index[data.df["dividend"].ne(0)].tolist() == [index[0], index[4]]
    assert data.df["dividend"].sum() == 1.5
    np.testing.assert_array_equal(data.datalines["dividend"].dataline, expected_dividends)
    np.testing.assert_array_equal(data.datalines["stock_splits"].dataline, expected_splits)
    pd.testing.assert_frame_equal(original, original_values)

    # Keep the established mark filling. This change is not a replacement price policy.
    for column, value in [("open", 100.0), ("high", 101.0), ("low", 99.0), ("close", 100.0)]:
        assert data.df[column].tolist() == [value] * 5

    # Constant prices remove the separate return-denominator question: only the two
    # recorded dividends contribute yield, whether Data precomputes it or Bars does.
    bars = Bars(data.df.copy(), source="test", asset=data.asset)
    np.testing.assert_allclose(bars.df["dividend_yield"], [0.01, 0.0, 0.0, 0.0, 0.005])
    np.testing.assert_allclose(bars.df["return"], [np.nan, 0.0, 0.0, 0.0, 0.005], equal_nan=True)


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("columns", [(), ("dividend",), ("stock_splits",)])
def test_optional_action_columns_stay_optional_and_neutral(backend, columns):
    """Absent columns stay absent, and null-only actions cannot become events."""
    index = pd.date_range("2026-10-12", periods=3, tz="America/New_York", name="datetime")
    frame = _frame(index[[0, 2]])
    for column in columns:
        frame[column] = [np.nan, 0.0]
    data = _data(backend, frame)

    data.repair_times_and_fill(index)

    for column in ("dividend", "stock_splits"):
        if column in columns:
            assert data.df[column].tolist() == [0.0, 0.0, 0.0]
        else:
            assert column not in data.df
            assert column not in data.datalines


@pytest.mark.parametrize("backend", BACKENDS)
@pytest.mark.parametrize("column", ["dividend", "stock_splits"])
def test_integer_event_alignment_preserves_exact_values(backend, column):
    """Neutral inserted rows must not promote and round an original integer event."""
    index = pd.date_range("2026-10-12", periods=3, tz="America/New_York", name="datetime")
    frame = _frame(index[[0, 2]])
    event = 2**53 + 1  # The nearest integer a float64 reindex would silently round.
    frame[column] = pd.Series([event, 0], index=frame.index, dtype="int64")
    data = _data(backend, frame)

    data.repair_times_and_fill(index)

    expected = pd.Series([event, 0, 0], index=index, name=column, dtype="int64")
    pd.testing.assert_series_equal(data.df[column], expected)
    assert data.datalines[column].dataline.dtype == np.dtype("int64")


@pytest.mark.parametrize("backend", BACKENDS)
def test_complete_action_history_preserves_values(backend):
    """An identity split ratio and two adjacent real events must not be erased."""
    index = pd.date_range("2026-10-12", periods=3, tz="America/New_York", name="datetime")
    frame = _frame(index, [1.0, 0.5, 0.0], [1.0, 2.0, 0.0])
    data = _data(backend, frame)

    data.repair_times_and_fill(index)

    pd.testing.assert_frame_equal(data.df[frame.columns], frame)


@pytest.mark.parametrize("backend", BACKENDS)
def test_duplicate_timestamps_keep_first_recorded_action(backend):
    """Match the existing first-row duplicate policy before exact event alignment."""
    index = pd.date_range("2026-10-12", periods=3, tz="America/New_York", name="datetime")
    data = _data(backend, _frame(index[[0, 0, 2]], [1.0, 99.0, 0.0], [2.0, 99.0, 0.0]))

    data.repair_times_and_fill(index)

    assert data.df["dividend"].tolist() == [1.0, 0.0, 0.0]
    assert data.df["stock_splits"].tolist() == [2.0, 0.0, 0.0]


@pytest.mark.parametrize("backend", BACKENDS)
def test_off_grid_marks_do_not_reschedule_actions(backend):
    """A prior close can supply a mark without inventing an event at market open."""
    source_index = pd.DatetimeIndex(["2026-10-12 16:00", "2026-10-14 16:00"], tz="America/New_York", name="datetime")
    target_index = pd.DatetimeIndex(["2026-10-13 09:30", "2026-10-14 09:30"], tz="America/New_York", name="datetime")
    data = _data(backend, _frame(source_index, [1.0, 0.0], [2.0, 0.0]))

    data.repair_times_and_fill(target_index)

    assert data.df["close"].tolist() == [100.0, 100.0]
    assert data.df["dividend"].tolist() == [0.0, 0.0]
    assert data.df["stock_splits"].tolist() == [0.0, 0.0]


@pytest.mark.parametrize("backend", BACKENDS)
def test_repeated_alignment_cannot_multiply_events(backend):
    """A second alignment reads the repaired cache and must keep the same event dates."""
    index = pd.date_range("2026-10-12", periods=3, tz="America/New_York", name="datetime")
    data = _data(backend, _frame(index[[0, 2]], [1.0, 0.0], [2.0, 0.0]))
    data.repair_times_and_fill(index)
    first = data.df.copy(deep=True)

    data.repair_times_and_fill(index)

    pd.testing.assert_frame_equal(data.df, first)
    assert data.df["dividend"].tolist() == [1.0, 0.0, 0.0]
    assert data.df["stock_splits"].tolist() == [2.0, 0.0, 0.0]


@pytest.mark.parametrize("backend,source_class", [(Data, PandasData), (DataPolars, PolarsData)])
def test_public_daily_action_reads_do_not_repeat_another_assets_event(backend, source_class):
    """Loading another asset's date must not fabricate a second cash/split date."""
    index = pd.date_range("2026-10-12", periods=4, tz="America/New_York", name="datetime")
    action_data = _data(backend, _frame(index[[0, 2, 3]], [1.0, 0.0, 0.0], [2.0, 0.0, 0.0]))
    companion = _data(backend, _frame(index), symbol="OTHER")
    source = source_class(datetime_start=index[0], datetime_end=index[-1], pandas_data=[action_data, companion])

    source.load_data()

    # The real local calendar/load path constructs a union containing the missing day.
    # Historical bars expose dividends in both backends. PolarsData's aggregate
    # dividend reader is currently a stub, so exercise that reader only for PandasData.
    # Split readers consume the full repaired daily frame in both backends.
    for timestamp, dividend, split in zip(index[:3], [1.0, 0.0, 0.0], [2.0, 0.0, 0.0]):
        source._datetime = timestamp.to_pydatetime()
        if source_class is PandasData:
            assert source.get_yesterday_dividends([action_data.asset])[action_data.asset] == dividend
        assert source.get_yesterday_stock_splits([action_data.asset])[action_data.asset] == split

        # Polars daily history excludes the current row. The trailing observation
        # lets each of the original three dates be read as a completed bar.
        if source_class is PolarsData:
            source._datetime = (timestamp + pd.Timedelta(days=1)).to_pydatetime()
        bars = source.get_historical_prices(action_data.asset, 1, timestep="day")
        assert bars.df.index[-1].date() == timestamp.date()
        assert bars.df["dividend"].iloc[-1] == dividend
