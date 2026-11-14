"""Optional whole-history indicator protocol for backtests.

A prepared indicator object (what an IndicatorExecutor returns) may expose
``compute_history(bars_df) -> {output_id: sequence with one value per row}``.
In a backtest, ``self.indicators.get`` then calls it once per stream with the
full stored bar series and reads the value at the current bar; a data change
triggers one recompute. Live strategies keep feeding bars one at a time.
Objects without the method behave exactly as before.
"""

from __future__ import annotations

import pandas as pd
import pytest

from lumibot.indicators import IndicatorExecutor, use_indicator_executor
from lumibot.indicators.base import iter_bars
from lumibot.indicators.indicators import Indicators


def _bars(n=80):
    index = pd.date_range("2024-01-01", periods=n, freq="1D", tz="UTC")
    close = [100 + i + (i % 7) * 1.5 - (i % 3) for i in range(n)]
    return pd.DataFrame(
        {"open": close, "high": [c + 2 for c in close], "low": [c - 2 for c in close], "close": close, "volume": 1.0},
        index=index,
    )


class _WholeHistory:
    """Wraps a real indicator; computes whole history by replaying its own update.

    Any executor (another process, a vector engine) may implement the method
    however it likes; the contract is one value per row, causal.
    """

    def __init__(self, indicator, calls):
        self._indicator = indicator
        self._calls = calls
        self.key, self.version, self.inputs, self.outputs = (
            indicator.key, indicator.version, indicator.inputs, indicator.outputs)

    def update(self, bar, closed=True):
        return self._indicator.update(bar, closed=closed)

    def reset(self, inputs=None):
        return self._indicator.reset(inputs)

    def describe(self, inputs=None):
        return self._indicator.describe(inputs)

    def compute_history(self, bars_df):
        self._calls.append(len(bars_df))
        fresh = type(self._indicator)(**self._indicator.inputs)
        rows = [fresh.update(bar, closed=True) for bar in iter_bars(bars_df)]
        return {spec.id: [row[spec.id] for row in rows] for spec in fresh.outputs}


class _HistoryExecutor(IndicatorExecutor):
    name = "whole_history_test"

    def __init__(self):
        self.calls = []

    def prepare(self, indicator):
        return _WholeHistory(indicator, self.calls)


class _Strategy:
    def __init__(self, df, *, backtesting=True):
        self.is_backtesting = backtesting
        self.now = df.index[0]

    def get_datetime(self):
        return self.now


def _accessor(holder, strategy):
    accessor = Indicators(strategy)
    accessor._full_history = lambda asset, timestep, quote=None, exchange=None: holder["df"]
    accessor._record_consumed_input = lambda *a, **k: None
    return accessor


def _walk(accessor, strategy, df, key, **inputs):
    values = []
    for stamp in df.index:
        strategy.now = stamp
        values.append(accessor.get(key, "X", **inputs))
    return values


@pytest.mark.parametrize("key,inputs", [("rsi", {"length": 14}), ("bbands", {"length": 20}), ("sma", {"length": 5})])
def test_whole_history_values_match_the_per_bar_path(key, inputs):
    df = _bars()
    holder = {"df": df}

    per_bar_strategy = _Strategy(df)
    per_bar = _walk(_accessor(holder, per_bar_strategy), per_bar_strategy, df, key, **inputs)

    executor = _HistoryExecutor()
    strategy = _Strategy(df)
    with use_indicator_executor(executor):
        whole = _walk(_accessor(holder, strategy), strategy, df, key, **inputs)

    assert whole == per_bar
    assert executor.calls == [len(df)]  # exactly one whole-history call for this stream


def test_data_change_triggers_one_recompute():
    df = _bars()
    holder = {"df": df.iloc[:60]}
    executor = _HistoryExecutor()
    strategy = _Strategy(df)
    with use_indicator_executor(executor):
        accessor = _accessor(holder, strategy)
        for stamp in df.index[:50]:
            strategy.now = stamp
            accessor.get("sma", "X", length=5)
        holder["df"] = df  # the data source extended the stored series
        for stamp in df.index[50:]:
            strategy.now = stamp
            last = accessor.get("sma", "X", length=5)

    assert executor.calls == [60, 80]
    assert last["sma"] == pytest.approx(df["close"].iloc[-5:].mean())


def test_live_trading_keeps_feeding_bars():
    df = _bars(30)
    holder = {"df": df}
    executor = _HistoryExecutor()
    strategy = _Strategy(df, backtesting=False)
    with use_indicator_executor(executor):
        values = _walk(_accessor(holder, strategy), strategy, df, "sma", length=5)

    assert executor.calls == []
    assert values[-1]["sma"] == pytest.approx(df["close"].iloc[-5:].mean())


def test_objects_without_the_method_are_unchanged():
    df = _bars(30)
    holder = {"df": df}

    class _Plain(IndicatorExecutor):
        def prepare(self, indicator):
            return indicator

    strategy = _Strategy(df)
    with use_indicator_executor(_Plain()):
        values = _walk(_accessor(holder, strategy), strategy, df, "sma", length=5)
    assert values[3]["sma"] is None
    assert values[-1]["sma"] == pytest.approx(df["close"].iloc[-5:].mean())


def test_wrong_length_history_is_rejected():
    df = _bars(20)
    holder = {"df": df}

    class _Short(_HistoryExecutor):
        def prepare(self, indicator):
            handle = super().prepare(indicator)
            handle.compute_history = lambda bars_df: {"sma": [1.0]}
            return handle

    strategy = _Strategy(df)
    with use_indicator_executor(_Short()):
        with pytest.raises(ValueError, match="one value per row"):
            _accessor(holder, strategy).get("sma", "X", length=5)
