"""Recording consumed history is cheap when nothing can be wrong, and records the same evidence.

Why: every indicator call in a backtest records the history it consumed
(``record_required_history``). On a three-month MNQ 1-minute run that was 12,902 calls and
about 36 of 379 profiled seconds, nearly all of it copying and filtering the frame
(``missing`` rows dropped, rows without prices dropped, first and last dates read) only to
learn that the decision had all the bars it asked for and no known hole.

Invariant under test: when the series has no incomplete health record and the frame holds
at least the requested priced rows, nothing is recorded except "this series has data", the
same as before, and no frame is copied. Every other case (too few rows, a known missing
session, an unresolved contract, missing rows, NaN prices) keeps the full path and records
the same evidence as before. Counts, never seconds.
"""

import copy
import random

import numpy as np
import pandas as pd
import pytest

import lumibot.tools.ibkr_history_health as health
from lumibot.entities import Asset


@pytest.fixture(autouse=True)
def _clean_health():
    health.reset_ibkr_history_health_for_testing()
    yield
    health.reset_ibkr_history_health_for_testing()


def _frame(rows, *, nan_rows=(), missing_rows=None, start="2026-09-01 09:30", freq="1min"):
    index = pd.date_range(start, periods=rows, freq=freq, tz="America/New_York")
    close = np.linspace(100.0, 101.0, rows)
    frame = pd.DataFrame({"open": close, "high": close + 0.5, "low": close - 0.5, "close": close,
                          "volume": 10.0}, index=index)
    for row in nan_rows:
        frame.iloc[row, frame.columns.get_loc("close")] = np.nan
    if missing_rows is not None:
        frame["missing"] = [i in missing_rows for i in range(rows)]
    return frame


def _state():
    return copy.deepcopy((health._HEALTH_BY_SERIES, health._REQUIRED_FAILURES, health._HISTORY_STARTS,
                          health._SERIES_WITH_DATA))


def _known_hole(symbol="MNQ", timestep="minute", day="2026-09-01"):
    health._HEALTH_BY_SERIES[f"{symbol}-{timestep}"] = {
        "symbol": symbol, "asset_type": "future", "timestep": timestep, "outcome": "incomplete",
        "missing_sessions": [day], "reason": "missing_sessions",
        "requested_start": "2026-08-01T00:00:00+00:00", "requested_end": "2026-10-01T00:00:00+00:00",
    }


def _record(frame, requested, timestep="minute", requirement="history"):
    health.record_required_history(
        asset=Asset("MNQ", asset_type=Asset.AssetType.FUTURE), timestep=timestep, requested_bars=requested,
        frame=frame, when=pd.Timestamp("2026-09-01 12:00", tz="America/New_York"), requirement=requirement)


def test_enough_bars_and_no_known_hole_records_only_that_the_series_has_data(monkeypatch):
    copies = {"n": 0}
    original = pd.DataFrame.dropna

    def counting(self, *args, **kwargs):
        copies["n"] += 1
        return original(self, *args, **kwargs)

    monkeypatch.setattr(pd.DataFrame, "dropna", counting)
    for requirement in ("history", "valuation", "availability"):
        _record(_frame(50), 50, requirement=requirement)
    assert copies["n"] == 0
    assert health._SERIES_WITH_DATA == {("MNQ", "future")}
    assert not health._REQUIRED_FAILURES and not health._HISTORY_STARTS


@pytest.mark.parametrize("seed", range(40))
def test_every_case_records_the_same_evidence_as_the_full_path(seed, monkeypatch):
    rng = random.Random(seed)
    rows = rng.choice([0, 1, 5, 30, 60])
    requested = rng.choice([1, 5, 30, 60, 90])
    frame = _frame(rows, nan_rows=rng.sample(range(rows), k=min(rows, rng.choice([0, 0, 1, 3]))),
                   missing_rows=set(rng.sample(range(rows), k=min(rows, rng.choice([0, 2])))) if rng.random() < 0.4 else None)
    timestep = rng.choice(["minute", "day"])
    requirement = rng.choice(["history", "valuation", "availability"])
    hole = rng.random() < 0.5

    def run(fast):
        health.reset_ibkr_history_health_for_testing()
        if hole:
            _known_hole(timestep=timestep)
        monkeypatch.setattr(health, "_RECORD_FAST_PATH", fast)
        _record(frame.copy(), requested, timestep=timestep, requirement=requirement)
        return _state()

    assert run(True) == run(False)
