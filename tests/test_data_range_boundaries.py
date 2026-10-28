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
