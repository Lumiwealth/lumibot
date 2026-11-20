"""The OHLC presence check gives the same answer with its one-row fast path.

Why: price reads ask "does this frame have OHLC?" on every bar. The check converted
whole columns each time, O(rows) per read (21 profiled seconds on a 16-year daily run).
"""

import numpy as np
import pandas as pd
import pytest

from lumibot.backtesting.thetadata_backtesting_pandas import ThetaDataBacktestingPandas


def _full_scan(frame):
    if frame is None or frame.empty:
        return False
    required = {"open", "high", "low", "close"}
    if not required.issubset(set(frame.columns)):
        return False
    return any(pd.to_numeric(frame[c], errors="coerce").notna().any() for c in required)


def _frames():
    idx = pd.date_range("2024-01-01", periods=5, freq="D")
    full = pd.DataFrame({c: np.arange(5.0) for c in ("open", "high", "low", "close")}, index=idx)
    nulls = full * np.nan
    last_null = full.copy()
    last_null.iloc[-1] = np.nan
    only_first = nulls.copy()
    only_first.iloc[0, 0] = 1.0
    text = full.astype(object)
    text.iloc[-1] = "n/a"
    missing_col = full.drop(columns=["low"])
    return [full, nulls, last_null, only_first, text, missing_col, full.iloc[0:0], None]


@pytest.mark.parametrize("position", range(8))
def test_fast_path_matches_full_scan(position):
    frame = _frames()[position]
    checker = ThetaDataBacktestingPandas._frame_has_ohlc_columns
    assert checker(object(), frame) is _full_scan(frame)
