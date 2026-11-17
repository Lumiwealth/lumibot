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


# ---------------------------------------------------------------------------------------
# One calculation per stored series for prefix-stable built-in indicators (backtests).
#
# WHY (2026-11-17): a three month futures backtest calling ``self.indicators.supertrend``
# once per 5 minute bar spent 85% of its simulation recalculating the indicator over the
# whole history seen so far, about 18,900 times over up to 28,000 bars. For an indicator
# whose row i depends only on rows 0..i, the row read from one calculation over the stored
# series is the same number, so it is calculated once and read at the current bar.
# ---------------------------------------------------------------------------------------

WHOLE_SERIES_CASES = [
    ("supertrend", {"length": 10, "multiplier": 3.0}), ("supertrend", {}), ("ema", {"length": 21}),
    ("rsi", {"length": 14}), ("atr", {"length": 14}), ("macd", {}), ("macd", {"fast": 8, "slow": 21, "signal": 5}),
    ("adx", {"length": 14}), ("bbands", {"length": 20, "std": 2}), ("obv", {}), ("cci", {"length": 20}),
    ("willr", {}), ("roc", {"length": 10}), ("mom", {}), ("wma", {"length": 30}),
    ("donchian", {}), ("psar", {}), ("hma", {"length": 55}), ("dema", {}), ("tema", {}), ("mfi", {}),
    ("rma", {"length": 14}), ("aroon", {}), ("cmf", {}), ("kama", {}), ("vwma", {}), ("stdev", {"length": 30}),
    ("zscore", {"length": 30}),
]


def _intraday_series(rows=2_400, seed=5):
    from lumibot.entities import Data

    rng = np.random.default_rng(seed)
    close = 20_000 + np.cumsum(rng.normal(0, 8, rows))
    idx = pd.date_range("2026-08-03 18:00", periods=rows, freq="5min", tz="America/New_York")
    frame = pd.DataFrame({"open": close + rng.normal(0, 4, rows), "high": close + rng.uniform(0.5, 12, rows),
                          "low": close - rng.uniform(0.5, 12, rows), "close": close,
                          "volume": rng.integers(1, 5_000, rows).astype(float)}, index=idx)
    asset = Asset("SYN", asset_type="crypto")
    return asset, frame, Data(asset, frame, timestep="minute", quote=Asset("USD", asset_type="forex"))


def _backtest_indicators(asset, data, clock):
    source = types.SimpleNamespace(_data_store={(asset, "minute"): data})
    strategy = types.SimpleNamespace(broker=types.SimpleNamespace(data_source=source), is_backtesting=True,
                                     get_datetime=lambda: clock["now"], quote_asset=None)
    return Indicators(strategy), source


def _plain(value):
    if value is None:
        return None
    if hasattr(value, "as_dict"):
        return {k: (None if isinstance(v, float) and np.isnan(v) else v) for k, v in value.as_dict().items()}
    return None if isinstance(value, float) and np.isnan(value) else value


@pytest.mark.parametrize("name,kwargs", WHOLE_SERIES_CASES)
def test_prefix_stable_builtin_is_calculated_once_and_matches_the_per_bar_path(monkeypatch, name, kwargs):
    asset, frame, data = _intraday_series()
    clock = {"now": frame.index[0]}
    fast, fast_source = _backtest_indicators(asset, data, clock)
    slow, slow_source = _backtest_indicators(asset, data, clock)
    # The reference recalculates over the bars seen so far on every call, as before.
    monkeypatch.setattr(slow, "_WHOLE_SERIES_BUILTINS", frozenset(), raising=False)
    computes = []
    compute = fast._compute
    monkeypatch.setattr(fast, "_compute", lambda *a, **k: computes.append(1) or compute(*a, **k))

    # Warm-up bars, then every 7th bar, plus times inside a forming bar.
    steps = list(range(0, 60)) + list(range(60, len(frame), 7)) + [len(frame) - 1]
    late_calls, before_late = 0, None
    for row in steps:
        if row >= 800 and before_late is None:
            before_late = len(computes)  # the few early bars are calculated bar by bar (warm-up answers)
        for inside in (pd.Timedelta(0), pd.Timedelta(minutes=2)):
            clock["now"] = frame.index[row] + inside
            got = getattr(fast, name)(asset, timestep="minute", **kwargs)
            want = getattr(slow, name)(asset, timestep="minute", **kwargs)
            assert _plain(got) == _plain(want), (name, row, inside)
            late_calls += row >= 800
    assert late_calls > 400
    assert len(computes) - before_late == 0, f"{len(computes) - before_late} calculations for {late_calls} calls"
    for source in (fast_source, slow_source):
        source._market_input_evidence.close()


def test_whole_series_value_never_depends_on_later_bars():
    asset, frame, data = _intraday_series()
    changed = frame.copy()
    cut = 1_500
    changed.iloc[cut + 1:, :4] *= 3.0  # everything after the current bar is different
    from lumibot.entities import Data

    other = Data(asset, changed, timestep="minute", quote=Asset("USD", asset_type="forex"))
    clock = {"now": frame.index[cut]}
    first, first_source = _backtest_indicators(asset, data, clock)
    second, second_source = _backtest_indicators(asset, other, clock)
    for row in (cut - 40, cut - 1, cut):
        clock["now"] = frame.index[row]
        for name, kwargs in (("supertrend", {"length": 10, "multiplier": 3.0}), ("ema", {"length": 21}), ("rsi", {})):
            assert _plain(getattr(first, name)(asset, timestep="minute", **kwargs)) == _plain(
                getattr(second, name)(asset, timestep="minute", **kwargs))
    for source in (first_source, second_source):
        source._market_input_evidence.close()


def test_whole_series_is_recalculated_when_the_stored_series_changes(monkeypatch):
    from lumibot.entities import Data

    asset, frame, _ = _intraday_series()
    data = Data(asset, frame.iloc[:1_200], timestep="minute", quote=Asset("USD", asset_type="forex"))
    clock = {"now": frame.index[900]}
    fast, source = _backtest_indicators(asset, data, clock)
    slow, slow_source = _backtest_indicators(asset, data, clock)
    monkeypatch.setattr(slow, "_WHOLE_SERIES_BUILTINS", frozenset(), raising=False)
    assert _plain(fast.supertrend(asset, timestep="minute", length=10)) == _plain(
        slow.supertrend(asset, timestep="minute", length=10))
    # The backtest loads a longer series for the same asset.
    longer = Data(asset, frame, timestep="minute", quote=Asset("USD", asset_type="forex"))
    source._data_store[(asset, "minute")] = longer
    slow_source._data_store[(asset, "minute")] = longer
    for row in (1_100, 1_900, 2_399):
        clock["now"] = frame.index[row]
        assert _plain(fast.supertrend(asset, timestep="minute", length=10)) == _plain(
            slow.supertrend(asset, timestep="minute", length=10))
    for each in (source, slow_source):
        each._market_input_evidence.close()


WINDOW_CASES = [
    ("supertrend", {"length": 10, "multiplier": 3.0}), ("supertrend", {}), ("ema", {"length": 21}),
    ("rsi", {"length": 14}), ("atr", {"length": 14}), ("macd", {}), ("adx", {"length": 14}), ("rma", {"length": 14}),
    ("dema", {}), ("tema", {}), ("willr", {}), ("roc", {"length": 10}), ("mom", {}), ("wma", {"length": 30}),
    ("hma", {"length": 55}), ("donchian", {}), ("aroon", {}), ("psar", {}), ("kama", {}),
]


def _windowed_indicators(asset, data, clock):
    """A source that hands indicators the most recent N bars (not the stored series from its start)."""
    source = types.SimpleNamespace(_data_store={(asset, "other spelling"): data})

    def history(asset, length, timestep="", **_):
        return types.SimpleNamespace(df=data.get_bars(clock["now"], length=length, timestep="minute"))

    strategy = types.SimpleNamespace(broker=types.SimpleNamespace(data_source=source), is_backtesting=True,
                                     get_datetime=lambda: clock["now"], quote_asset=None,
                                     get_historical_prices=history)
    return Indicators(strategy), source


@pytest.mark.parametrize("seed", [5, 23])
@pytest.mark.parametrize("name,kwargs", WINDOW_CASES)
def test_window_stable_builtin_matches_the_per_bar_path_on_a_moving_window(monkeypatch, name, kwargs, seed):
    asset, frame, data = _intraday_series(rows=13_000, seed=seed)
    clock = {"now": frame.index[0]}
    fast, fast_source = _windowed_indicators(asset, data, clock)
    slow, slow_source = _windowed_indicators(asset, data, clock)
    monkeypatch.setattr(slow, "_WHOLE_SERIES_BUILTINS", frozenset(), raising=False)
    computes = []
    compute = fast._compute
    monkeypatch.setattr(fast, "_compute", lambda *a, **k: computes.append(1) or compute(*a, **k))
    # Bars 0..~10,000 are a growing input from the first stored bar; later ones a moving 10,000 bar window.
    for row in (700, 4_000, 9_990, 10_050, 10_051, 10_777, 11_500, 12_113, 12_998, 12_999):
        clock["now"] = frame.index[row] + pd.Timedelta(minutes=5)
        got = getattr(fast, name)(asset, timestep="5 minutes", **kwargs)
        want = getattr(slow, name)(asset, timestep="5 minutes", **kwargs)
        assert _plain(got) == _plain(want), (name, row)
        assert _plain(got) is not None
    assert len(computes) == 1, f"{len(computes)} calculations for one stored series"
    for source in (fast_source, slow_source):
        source._market_input_evidence.close()


def test_running_totals_keep_the_per_bar_path_on_a_moving_window(monkeypatch):
    """obv on the latest 10,000 bars is a different number from obv since the first stored bar."""
    asset, frame, data = _intraday_series(rows=11_000)
    clock = {"now": frame.index[10_800]}
    fast, fast_source = _windowed_indicators(asset, data, clock)
    slow, slow_source = _windowed_indicators(asset, data, clock)
    monkeypatch.setattr(slow, "_WHOLE_SERIES_BUILTINS", frozenset(), raising=False)
    for name in ("obv", "bbands", "cci"):
        assert _plain(getattr(fast, name)(asset, timestep="5 minutes")) == _plain(
            getattr(slow, name)(asset, timestep="5 minutes"))
    for source in (fast_source, slow_source):
        source._market_input_evidence.close()
