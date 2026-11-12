"""Count guards for one ``self.indicators`` call in a backtest.

A one-minute strategy calling a built-in or custom indicator spent 356 to 543 ms per call,
almost all of it re-recording the whole visible history as backtest input evidence. These
guards pin the work per call: one recompute per new bar, none for a repeat inside the same
bar, and evidence serialization proportional to the new rows rather than the whole history.
"""
import types

import numpy as np
import pandas as pd
import pytest

from lumibot.backtesting import market_input_evidence as mie
from lumibot.entities import Asset
from lumibot.indicators import Indicators

ROWS, STEP, CALLS = 3_000, 20, 60


def _setup():
    rows = ROWS + STEP * CALLS
    rng = np.random.default_rng(11)
    close = 400 + np.cumsum(rng.normal(0, 0.05, rows))
    idx = pd.date_range("2026-07-01 04:00", periods=rows, freq="min", tz="America/New_York")
    full = pd.DataFrame({"open": close, "high": close + 0.1, "low": close - 0.1, "close": close,
                         "volume": rng.integers(100, 10_000, rows)}, index=idx)
    asset = Asset("SPY")
    data = types.SimpleNamespace(df=full.iloc[:ROWS])
    source = types.SimpleNamespace(_data_store={(asset, None): data})
    clock = {"now": full.index[ROWS - 1]}
    strategy = types.SimpleNamespace(broker=types.SimpleNamespace(data_source=source), is_backtesting=True,
                                     get_datetime=lambda: clock["now"], quote_asset=None)
    return full, asset, data, source, clock, Indicators(strategy)


def _wilder(df, length=14):
    delta = df["close"].diff()
    up = delta.clip(lower=0).ewm(alpha=1 / length, adjust=False).mean()
    down = (-delta.clip(upper=0)).ewm(alpha=1 / length, adjust=False).mean()
    return 100 - 100 / (1 + up / down)


@pytest.mark.parametrize("kind", ["builtin", "custom"])
def test_indicator_call_work_is_bounded_by_new_bars(monkeypatch, kind):
    full, asset, data, source, clock, indicators = _setup()
    computes, serialized = [], []
    compute = indicators._compute
    monkeypatch.setattr(indicators, "_compute", lambda *a, **k: computes.append(1) or compute(*a, **k))
    rows = mie._frame_rows

    def counted(frame, columns):
        for row in rows(frame, columns):
            serialized.append(1)
            yield row

    monkeypatch.setattr(mie, "_frame_rows", counted)

    def call():
        if kind == "custom":
            return indicators.custom("wilder", _wilder, asset, timestep="minute", length=14)
        return indicators.rsi(asset, length=14, timestep="minute")

    for step in range(CALLS):
        end = ROWS + STEP * step
        data.df = full.iloc[:end]
        clock["now"] = full.index[end - 1]
        first = call()
        before = len(computes)
        assert call() == first
        assert len(computes) == before, "a repeat call inside the same bar must not recompute"
    assert len(computes) == CALLS, "exactly one recompute per new bar"
    distinct = ROWS + STEP * (CALLS - 1)
    assert len(serialized) <= distinct, f"{len(serialized)} rows serialized for {distinct} distinct rows"
    source._market_input_evidence.close()
