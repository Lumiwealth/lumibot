import datetime

import pandas as pd
import pytz

from lumibot.tools import thetadata_helper


def test_align_day_index_to_market_close_utc_sets_ny_close_time():
    df = pd.DataFrame(
        {"close": [1.0, 2.0]},
        index=pd.DatetimeIndex(
            [
                pd.Timestamp("2025-10-01", tz="UTC"),
                pd.Timestamp("2025-12-01", tz="UTC"),
            ],
            name="datetime",
        ),
    )

    aligned = thetadata_helper._align_day_index_to_market_close_utc(df)
    ny = pytz.timezone("America/New_York")
    idx_ny = aligned.index.tz_convert(ny)

    assert idx_ny[0].date() == datetime.date(2025, 10, 1)
    assert idx_ny[0].hour == 16 and idx_ny[0].minute == 0
    assert idx_ny[1].date() == datetime.date(2025, 12, 1)
    assert idx_ny[1].hour == 16 and idx_ny[1].minute == 0

    aligned_again = thetadata_helper._align_day_index_to_market_close_utc(aligned)
    pd.testing.assert_index_equal(aligned.index, aligned_again.index)


def test_append_missing_markers_uses_market_close_time():
    missing = [datetime.date(2025, 10, 1)]
    df = thetadata_helper.append_missing_markers(None, missing)

    assert df is not None
    assert len(df) == 1

    ny = pytz.timezone("America/New_York")
    ts_ny = df.index[0].tz_convert(ny)
    assert ts_ny.date() == datetime.date(2025, 10, 1)
    assert ts_ny.hour == 16 and ts_ny.minute == 0
