from __future__ import annotations

import pandas as pd

from lumibot.tools.ibkr_helper import (
    _align_stock_index_daily_to_session_close,
    _repair_isolated_split_spikes_daily,
)


def _build_frame(closes):
    idx = pd.date_range(start="2020-01-01", periods=len(closes), freq="D", tz="America/New_York")
    close = pd.Series(closes, index=idx, dtype="float64")
    return pd.DataFrame(
        {
            "open": close * 1.00,
            "high": close * 1.01,
            "low": close * 0.99,
            "close": close,
        },
        index=idx,
    )


def test_repair_isolated_upward_split_spike_day():
    frame = _build_frame([10.0, 11.0, 22.0, 11.2, 11.4])
    repaired = _repair_isolated_split_spikes_daily(frame)
    assert abs(float(repaired["close"].iloc[2]) - 11.0) < 1e-9
    assert abs(float(repaired["open"].iloc[2]) - 11.0) < 1e-9


def test_repair_isolated_upward_three_to_one_spike_day():
    frame = _build_frame([10.0, 10.2, 30.6, 10.3, 10.4])
    repaired = _repair_isolated_split_spikes_daily(frame)
    assert abs(float(repaired["close"].iloc[2]) - 10.2) < 1e-9


def test_repair_isolated_downward_split_spike_day():
    frame = _build_frame([10.0, 10.5, 5.2, 10.6, 10.8])
    repaired = _repair_isolated_split_spikes_daily(frame)
    assert abs(float(repaired["close"].iloc[2]) - 10.4) < 1e-9


def test_does_not_modify_persistent_regime_shift():
    frame = _build_frame([10.0, 11.0, 22.0, 21.5, 21.8])
    repaired = _repair_isolated_split_spikes_daily(frame)
    assert abs(float(repaired["close"].iloc[2]) - 22.0) < 1e-9


def test_repair_terminal_split_spike_without_next_day_reversion():
    # Final row is a 2x split-like spike; rolling windows won't include a future reversion row.
    frame = _build_frame([10.0, 10.1, 10.2, 10.3, 20.6])
    repaired = _repair_isolated_split_spikes_daily(frame)
    assert abs(float(repaired["close"].iloc[-1]) - 10.3) < 1e-9


def test_align_stock_index_daily_to_session_close_from_early_morning_timestamps():
    idx = pd.to_datetime(["2024-01-02 04:00:00-05:00", "2024-01-03 04:00:00-05:00", "2024-01-04 04:00:00-05:00"])
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
    idx = pd.to_datetime(["2024-01-02 16:00:00-05:00", "2024-01-03 16:00:00-05:00", "2024-01-04 16:00:00-05:00"])
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
