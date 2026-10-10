"""Session alignment remains independent of price correction.

Shape-only split-spike correction was removed: a real large price move is not
proof of a bad quote. Public-reader regressions live in test_ibkr_equity_action_integrity.
"""
from __future__ import annotations

import pandas as pd

from lumibot.tools.ibkr_helper import _align_stock_index_daily_to_session_close


def test_align_stock_index_daily_to_session_close_from_early_morning_timestamps():
    idx = pd.to_datetime(
        ["2024-01-02 04:00:00-05:00", "2024-01-03 04:00:00-05:00", "2024-01-04 04:00:00-05:00"]
    )
    frame = pd.DataFrame(
        {
            "open": [10.0, 11.0, 12.0],
            "high": [10.2, 11.2, 12.2],
            "low": [9.8, 10.8, 11.8],
            "close": [10.1, 11.1, 12.1],
        },
        index=idx,
    )

    aligned = _align_stock_index_daily_to_session_close(frame)
    aligned_idx = pd.DatetimeIndex(aligned.index).tz_convert("America/New_York")

    assert list(aligned["close"]) == [10.1, 11.1, 12.1]
    assert all(ts.hour == 16 and ts.minute == 0 for ts in aligned_idx)


def test_align_stock_index_daily_to_session_close_is_idempotent():
    idx = pd.to_datetime(
        ["2024-01-02 16:00:00-05:00", "2024-01-03 16:00:00-05:00", "2024-01-04 16:00:00-05:00"]
    )
    frame = pd.DataFrame(
        {
            "open": [20.0, 21.0, 22.0],
            "high": [20.2, 21.2, 22.2],
            "low": [19.8, 20.8, 21.8],
            "close": [20.1, 21.1, 22.1],
        },
        index=idx,
    )

    aligned = _align_stock_index_daily_to_session_close(frame)
    aligned_idx = pd.DatetimeIndex(aligned.index).tz_convert("America/New_York")
    frame_idx = pd.DatetimeIndex(frame.index).tz_convert("America/New_York")
    assert aligned_idx.equals(frame_idx)
    assert list(aligned["close"]) == [20.1, 21.1, 22.1]
