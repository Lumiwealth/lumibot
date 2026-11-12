"""Growing history frames are recorded in time proportional to the new rows.

``self.indicators`` hands the recorder the whole visible history on every call, and that
history grows by one bar per minute. Re-serializing every row in Python on every call made
a one-minute RSI cost 356 to 501 ms per call over a month (98% of simulation time). When a
frame starts with exactly the rows the recorder saw last time, only the appended rows are
serialized; the stored rows and observations stay byte-identical to a full re-read.
"""

import numpy as np
import pandas as pd

from lumibot.backtesting import market_input_evidence as mie
from lumibot.backtesting.market_input_evidence import MarketInputEvidence
from lumibot.entities import Asset

START = pd.Timestamp("2026-08-03 04:00", tz="America/New_York")


def _history(rows, seed=7):
    rng = np.random.default_rng(seed)
    close = 100 + np.cumsum(rng.normal(0, 0.1, rows))
    close[5] = -0.0
    close[9] = np.nan
    idx = pd.date_range(START, periods=rows, freq="min")
    return pd.DataFrame({"open": close, "high": close + 0.25, "low": close - 0.25, "close": close,
                         "volume": rng.integers(0, 1000, rows)}, index=idx)


def _capture(calls, *, incremental):
    recorder = MarketInputEvidence()
    recorder.incremental = incremental
    for k, (asset, frame, purpose) in enumerate(calls):
        recorder.record(asset=asset, timestep="minute", when=START + pd.Timedelta(minutes=k),
                        frame=frame, purpose=purpose)
    out = list(recorder.rows.values()), list(recorder.observations), set(recorder.incomplete)
    recorder.close()
    return out


def _counting(monkeypatch):
    seen = []
    original = mie._frame_rows

    def counted(frame, columns):
        for row in original(frame, columns):
            seen.append(1)
            yield row

    monkeypatch.setattr(mie, "_frame_rows", counted)
    return seen


def test_growing_history_serializes_only_appended_rows(monkeypatch):
    seen = _counting(monkeypatch)
    full = _history(2_100)
    recorder = MarketInputEvidence()
    for step in range(100):
        frame = full.iloc[: 2_000 + step]
        for _repeat in range(2):  # a second call inside the same bar adds nothing
            recorder.record(asset=Asset("SPY"), timestep="minute", when=frame.index[-1],
                            frame=frame, purpose="indicator")
    recorder.close()
    assert len(seen) <= 2_000 + 100, f"{len(seen)} rows serialized for 2,100 distinct rows"


def test_sliding_history_serializes_only_new_rows(monkeypatch):
    seen = _counting(monkeypatch)
    full = _history(2_100)
    recorder = MarketInputEvidence()
    for step in range(100):
        frame = full.iloc[step: 2_000 + step]  # a fixed-length lookback that moves one bar
        recorder.record(asset=Asset("SPY"), timestep="minute", when=frame.index[-1], frame=frame, purpose="indicator")
        recorder.record(asset=Asset("SPY"), timestep="minute", when=frame.index[-1],
                        frame=frame.iloc[-30:], purpose="history")  # a short read on the same stream
    recorder.close()
    assert len(seen) <= 2_000 + 100 + 30, f"{len(seen)} rows serialized for 2,100 distinct rows"


def test_incremental_capture_is_identical_to_full_reads():
    full = _history(400)
    spy, qqq = Asset("SPY"), Asset("QQQ")
    corrected = full.iloc[:330].copy()
    corrected.iloc[100, corrected.columns.get_loc("close")] = 123.0
    calls = []
    for step in range(300, 320):
        calls.append((spy, full.iloc[:step], "indicator"))
        calls.append((spy, full.iloc[step - 30:step], "history"))  # sliding window, same stream
        calls.append((qqq, full.iloc[:step], "indicator"))
        calls.append((spy, full.iloc[:step], "indicator"))  # same bar again
    calls.append((spy, corrected, "indicator"))  # an older row changed
    calls.append((spy, full.iloc[:330], "indicator"))
    calls.append((spy, full.iloc[:310], "indicator"))  # shorter than last time
    calls.append((spy, full.iloc[:331].astype({"volume": float}), "indicator"))  # dtype changed
    calls.append((spy, full.iloc[:331].tz_convert("UTC"), "indicator"))  # timezone changed
    calls.append((spy, full.iloc[:332], "indicator"))
    for step in range(340, 360):
        calls.append((spy, full.iloc[step - 300:step], "indicator"))  # moving lookback
        calls.append((spy, full.iloc[step - 10:step], "history"))
    calls.append((spy, full.iloc[50:60], "history"))  # entirely inside earlier rows
    assert _capture(calls, incremental=True) == _capture(calls, incremental=False)


def test_incremental_state_does_not_trust_mutated_caller_frames():
    frame = _history(50)
    calls = [(Asset("SPY"), frame.iloc[:40], "indicator")]
    first = _capture(calls, incremental=True)
    recorder = MarketInputEvidence()
    shared = frame.copy()
    recorder.record(asset=Asset("SPY"), timestep="minute", when=START, frame=shared.iloc[:40], purpose="indicator")
    shared.iloc[3, shared.columns.get_loc("close")] = 999.0  # caller edits its frame in place
    recorder.record(asset=Asset("SPY"), timestep="minute", when=START, frame=shared.iloc[:41], purpose="indicator")
    reference = MarketInputEvidence()
    reference.incremental = False
    reference.record(asset=Asset("SPY"), timestep="minute", when=START, frame=frame.iloc[:40], purpose="indicator")
    reference.record(asset=Asset("SPY"), timestep="minute", when=START, frame=shared.iloc[:41], purpose="indicator")
    assert list(recorder.observations) == list(reference.observations)
    assert list(recorder.rows.values()) == list(reference.rows.values())
    assert first[1][0] == list(recorder.observations)[0]
    recorder.close()
    reference.close()


def test_incremental_state_memory_is_bounded():
    recorder = MarketInputEvidence()
    for k in range(200):
        frame = _history(30, seed=k)
        recorder.record(asset=Asset(f"S{k}"), timestep="minute", when=START, frame=frame, purpose="indicator")
    assert len(recorder._prefix_states) <= recorder.max_prefix_states
    assert all(len(entries) <= recorder.max_prefix_states_per_stream for entries in recorder._prefix_states.values())
    rows = sum(state.rows for entries in recorder._prefix_states.values() for state in entries)
    assert rows <= recorder.max_prefix_rows
    recorder.close()
