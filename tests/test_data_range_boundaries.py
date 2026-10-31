"""Date-range requests are half-open even when the endpoint is not a bar."""
import pandas as pd
import pytest

from lumibot.entities import Asset, Data


@pytest.mark.parametrize("start,end,expected", [
    ("2025-01-06", "2025-01-09", [10., 11., 12.]),
    ("2025-01-06T12:00", "2025-01-09", [11., 12.]),
    ("2025-01-06", "2025-01-08", [10., 11.]),
    ("2025-01-09", "2025-01-10", []),
])
def test_daily_range_keeps_last_completed_bar_without_including_outside_window(start, end, expected):
    index = pd.date_range("2025-01-06", periods=3, tz="America/New_York")
    frame = pd.DataFrame({"open": [10., 11., 12.], "high": [10., 11., 12.],
        "low": [10., 11., 12.], "close": [10., 11., 12.], "volume": [100.] * 3}, index=index)
    data = Data(Asset("ALPHA"), frame, timestep="day")
    begin, finish = (pd.Timestamp(v).tz_localize(index.tz) for v in (start, end))
    result = data.get_bars_between_dates(timestep="day", start_date=begin, end_date=finish)
    assert ([] if result is None else result.close.tolist()) == expected
    legacy = data._get_bars_between_dates_dict(start_date=begin, end_date=finish, timestep="day")
    assert list(legacy["close"]) == expected


@pytest.mark.parametrize("constructor_window", [False, True])
@pytest.mark.parametrize("source_timezone", ["UTC", "America/Los_Angeles", "Asia/Tokyo"])
@pytest.mark.parametrize("bound_timezone", ["America/New_York", "America/Los_Angeles", "Asia/Tokyo"])
def test_midnight_daily_labels_keep_requested_calendar_sessions(
    constructor_window, source_timezone, bound_timezone,
):
    index = pd.date_range("2025-01-05", periods=5, tz=source_timezone)
    prices = [9., 10., 11., 12., 13.]
    frame = pd.DataFrame({"open": prices, "high": prices,
        "low": prices, "close": prices, "volume": [100.] * 5}, index=index)
    begin = pd.Timestamp("2025-01-06", tz=bound_timezone)
    end = pd.Timestamp("2025-01-09", tz=bound_timezone)
    window = {"date_start": begin.to_pydatetime(),
              "date_end": (end - pd.Timedelta(days=1)).to_pydatetime()} if constructor_window else {}
    data = Data(Asset("ALPHA"), frame, timestep="day", **window)
    result = data.get_bars_between_dates(timestep="day", start_date=begin, end_date=end)
    assert result.close.tolist() == [10., 11., 12.]
    legacy = data._get_bars_between_dates_dict(start_date=begin, end_date=end, timestep="day")
    assert list(legacy["close"]) == [10., 11., 12.]


def test_non_midnight_daily_instants_and_minute_bounds_remain_timestamp_based():
    for timestep, index, begin, end, expected in [
        ("day", pd.date_range("2025-01-06T21:00", periods=3, tz="UTC"),
         "2025-01-06T12:00", "2025-01-07T12:00", [10.]),
        ("minute", pd.date_range("2025-01-06T14:30", periods=3, freq="min", tz="UTC"),
         "2025-01-06T09:30:30", "2025-01-06T09:33", [11., 12.]),
    ]:
        frame = pd.DataFrame({"open": [10., 11., 12.], "high": [10., 11., 12.],
            "low": [10., 11., 12.], "close": [10., 11., 12.], "volume": [100.] * 3}, index=index)
        data = Data(Asset("ALPHA"), frame, timestep=timestep)
        begin, end = (pd.Timestamp(v, tz="America/New_York") for v in (begin, end))
        result = data.get_bars_between_dates(timestep=timestep, start_date=begin, end_date=end)
        assert result.close.tolist() == expected


def test_daily_object_date_index_keeps_compatible_input_support():
    from datetime import date
    index = pd.Index([date(2025, 1, 6), date(2025, 1, 7)], dtype=object)
    frame = pd.DataFrame({"open": [10., 11.], "high": [10., 11.],
        "low": [10., 11.], "close": [10., 11.], "volume": [100., 100.]}, index=index)
    data = Data(Asset("ALPHA"), frame, timestep="day")
    result = data.get_bars_between_dates(timestep="day",
        start_date=pd.Timestamp("2025-01-06", tz="America/New_York"),
        end_date=pd.Timestamp("2025-01-08", tz="America/New_York"))
    assert result.close.tolist() == [10., 11.]
