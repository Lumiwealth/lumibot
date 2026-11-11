"""Market input evidence reads plain numeric columns directly, with identical records.

Every get_historical_prices call records the returned rows. Building the rows with
``frame[columns].itertuples()`` made a pandas Series per column per call and was about
two thirds of a 20-bar stock history request. Plain numpy columns are now read as Python
lists, which yields the same Python values in the same order; anything else (nullable or
extension dtypes, duplicate column names) keeps the itertuples path.
"""

import numpy as np
import pandas as pd
import pytest

from lumibot.backtesting import market_input_evidence as mie
from lumibot.backtesting.market_input_evidence import MarketInputEvidence
from lumibot.entities import Asset

WHEN = pd.Timestamp("2026-10-19 10:00", tz="America/New_York")


def _frames():
    idx = pd.date_range("2026-10-19 09:30", periods=6, freq="min", tz="America/New_York")
    base = pd.DataFrame({"open": [1.0, 2.5, -0.0, np.nan, 3.25, 1e300], "high": [2.0] * 6,
                         "low": [0.5] * 6, "close": [1.5, 2.0, 0.0, 4.0, np.inf, 2.0],
                         "volume": [10, 0, 5, 7, 8, 9], "label": list("abcdef")}, index=idx)
    yield "numeric with int volume", base
    yield "float volume", base.assign(volume=base["volume"].astype(float))
    yield "object column", base.assign(vwap=["1", 2, None, 3.5, "x", 4])
    yield "nullable int", base.assign(volume=pd.array([1, None, 3, 4, 5, 6], dtype="Int64"))
    yield "bool split", base.assign(split=[True, False, True, False, True, False])
    yield "duplicate columns", pd.concat([base[["open"]], base[["open", "close"]]], axis=1)
    yield "empty", base.iloc[0:0]
    yield "float32", base.assign(open=base["open"].astype("float32"))


def _records(recorder):
    return list(recorder.rows.values()), list(recorder.observations)


def _capture(frames, *, reference):
    recorder = MarketInputEvidence()
    for k, (_, frame) in enumerate(frames):
        if reference:
            original = mie._frame_rows
            mie._frame_rows = lambda f, cols: f[cols].itertuples(name=None)
            try:
                recorder.record(asset=Asset("SPY"), timestep="minute", when=WHEN + pd.Timedelta(minutes=k),
                                frame=frame, purpose="history")
            finally:
                mie._frame_rows = original
        else:
            recorder.record(asset=Asset("SPY"), timestep="minute", when=WHEN + pd.Timedelta(minutes=k),
                            frame=frame, purpose="history")
    out = _records(recorder)
    recorder.close()
    return out, recorder.incomplete


@pytest.mark.parametrize("name,frame", list(_frames()))
def test_each_frame_kind_records_exactly_as_itertuples(name, frame):
    assert _capture([(name, frame)], reference=False) == _capture([(name, frame)], reference=True)


def test_overlapping_windows_record_exactly_as_itertuples():
    frames = list(_frames())[:2]
    windows = [(n, f.iloc[i:i + 3]) for n, f in frames for i in range(4)]
    assert _capture(windows, reference=False) == _capture(windows, reference=True)


def test_plain_numeric_frames_skip_itertuples(monkeypatch):
    calls = []
    original = pd.DataFrame.itertuples
    monkeypatch.setattr(pd.DataFrame, "itertuples", lambda self, *a, **k: calls.append(1) or original(self, *a, **k))
    name, frame = next(_frames())
    recorder = MarketInputEvidence()
    recorder.record(asset=Asset("SPY"), timestep="minute", when=WHEN, frame=frame, purpose="history")
    recorder.close()
    assert calls == []
