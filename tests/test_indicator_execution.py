"""The indicator execution hook: how indicators run.

Default is in-process (the indicator object itself). A host may register an
executor that runs indicator code somewhere else, for example another
process. These tests use a small process-backed executor written only for the
test to prove the hook works end to end.
"""

from __future__ import annotations

import multiprocessing as mp

import pandas as pd
import pytest

from lumibot.indicators import (
    Indicator,
    IndicatorExecutor,
    IndicatorRegistry,
    IndicatorRunner,
    InProcessExecutor,
    InputSpec,
    OutputSpec,
    get_indicator_executor,
    set_indicator_executor,
    use_indicator_executor,
)
from lumibot.indicators.indicator_registry import registry as shared_registry


def _bars(n=60):
    index = pd.date_range("2024-01-01", periods=n, freq="1D", tz="UTC")
    close = [100 + i + (i % 5) for i in range(n)]
    return pd.DataFrame(
        {"open": close, "high": [c + 1 for c in close], "low": [c - 1 for c in close], "close": close, "volume": 1.0},
        index=index,
    )


class _FakeDataSource:
    def __init__(self, df):
        self.df = df

    def get_historical_prices(self, asset, length, timestep="day", quote=None, **_):
        return self.df.iloc[-length:]


# ---- a test-only executor that runs each indicator in a child process -------


def _child(conn, key, version, inputs):
    from lumibot.indicators.indicator_registry import registry

    indicator = registry.create(key, version, **inputs)
    while True:
        message = conn.recv()
        if message is None:
            break
        method, args, kwargs = message
        try:
            conn.send(("ok", getattr(indicator, method)(*args, **kwargs)))
        except Exception as exc:  # noqa: BLE001 - sent back to the caller
            conn.send(("error", repr(exc)))


class _ProcessHandle:
    def __init__(self, indicator: Indicator):
        self.key, self.version, self.outputs = indicator.key, indicator.version, indicator.outputs
        self.inputs = dict(indicator.inputs)
        self._describe = indicator.describe
        ctx = mp.get_context("spawn")
        self._conn, child = ctx.Pipe()
        self.process = ctx.Process(target=_child, args=(child, self.key, self.version, self.inputs), daemon=True)
        self.process.start()

    def _call(self, method, *args, **kwargs):
        self._conn.send((method, args, kwargs))
        status, value = self._conn.recv()
        if status != "ok":
            raise RuntimeError(value)
        return value

    def update(self, bar, closed=True):
        return self._call("update", bar, closed=closed)

    def reset(self, inputs=None):
        return self._call("reset", inputs)

    def snapshot(self):
        return self._call("snapshot")

    def describe(self, inputs=None):
        return self._describe(inputs)

    def close(self):
        self._conn.send(None)
        self.process.join(5)


class _ProcessExecutor(IndicatorExecutor):
    name = "test-process"

    def __init__(self):
        self.handles = []

    def prepare(self, indicator):
        handle = _ProcessHandle(indicator)
        self.handles.append(handle)
        return handle

    def close(self):
        for handle in self.handles:
            handle.close()


class _RecordingExecutor(IndicatorExecutor):
    name = "recording"

    def __init__(self):
        self.prepared = []

    def prepare(self, indicator):
        self.prepared.append(indicator)
        return indicator


# ---- tests ------------------------------------------------------------------


def test_default_executor_is_in_process_and_returns_the_same_object():
    executor = get_indicator_executor()
    assert isinstance(executor, InProcessExecutor)
    indicator = shared_registry.create("sma", length=3)
    assert executor.prepare(indicator) is indicator


def test_set_and_restore_executor():
    recording = _RecordingExecutor()
    previous = set_indicator_executor(recording)
    try:
        assert get_indicator_executor() is recording
    finally:
        set_indicator_executor(previous)
    assert isinstance(get_indicator_executor(), InProcessExecutor)
    set_indicator_executor(None)  # None restores the default
    assert isinstance(get_indicator_executor(), InProcessExecutor)


def test_set_executor_rejects_objects_without_prepare():
    with pytest.raises(TypeError):
        set_indicator_executor(object())


def test_context_manager_restores_on_error():
    recording = _RecordingExecutor()
    with pytest.raises(RuntimeError):
        with use_indicator_executor(recording):
            assert get_indicator_executor() is recording
            raise RuntimeError("boom")
    assert isinstance(get_indicator_executor(), InProcessExecutor)


def test_runner_routes_every_indicator_through_the_executor():
    recording = _RecordingExecutor()
    runner = IndicatorRunner(_FakeDataSource(_bars()), "X", timestep="day", executor=recording)
    runner.add("sma", length=5)
    runner.add("rsi", length=7)
    assert [i.key for i in recording.prepared] == ["sma", "rsi"]


def test_runner_uses_the_registered_executor_by_default():
    recording = _RecordingExecutor()
    with use_indicator_executor(recording):
        runner = IndicatorRunner(_FakeDataSource(_bars()), "X", timestep="day")
        runner.add("ema", length=4)
    assert [i.key for i in recording.prepared] == ["ema"]


def test_strategy_accessor_routes_through_the_executor():
    from lumibot.indicators.indicators import Indicators

    df = _bars()

    class _Strategy:
        is_backtesting = True

        def get_datetime(self):
            return df.index[-1]

    accessor = Indicators(_Strategy())
    accessor._full_history = lambda asset, timestep, quote=None, exchange=None: df
    accessor._record_consumed_input = lambda *a, **k: None
    recording = _RecordingExecutor()
    with use_indicator_executor(recording):
        value = accessor.get("sma", "X", length=3)
    assert value is not None and value["sma"] == pytest.approx(df["close"].iloc[-3:].mean())
    assert [i.key for i in recording.prepared] == ["sma"]


def test_out_of_process_executor_gives_identical_numbers():
    df = _bars(80)
    in_process = IndicatorRunner(_FakeDataSource(df), "X", timestep="day")
    process_executor = _ProcessExecutor()
    out_of_process = IndicatorRunner(_FakeDataSource(df), "X", timestep="day", executor=process_executor)
    try:
        for runner in (in_process, out_of_process):
            runner.add("rsi", length=14, instance_id="rsi")
            runner.add("bbands", length=20, instance_id="bb")
        a = in_process.load_history(60)
        b = out_of_process.load_history(60)
        assert a["indicators"]["rsi"]["outputs"] == b["indicators"]["rsi"]["outputs"]
        assert a["indicators"]["bb"]["outputs"] == b["indicators"]["bb"]["outputs"]
        bar = {"t": df.index[-1] + pd.Timedelta(days=1), "o": 150, "h": 152, "l": 149, "c": 151, "v": 1}
        assert in_process.on_bar(bar, closed=False) == out_of_process.on_bar(bar, closed=False)
        assert in_process.on_bar(bar, closed=True) == out_of_process.on_bar(bar, closed=True)
        assert all(h.process.pid != mp.current_process().pid for h in process_executor.handles)
    finally:
        process_executor.close()


def test_custom_registry_indicator_runs_through_executor():
    private = IndicatorRegistry()

    @private.register
    class Range(Indicator):
        key = "range_test"
        inputs = [InputSpec("scale", "float", 1.0)]
        outputs = [OutputSpec("range", "Range")]

        def step(self, state, bar):
            return {"range": (bar.high - bar.low) * self.inputs["scale"]}

    recording = _RecordingExecutor()
    runner = IndicatorRunner(_FakeDataSource(_bars()), "X", timestep="day", registry=private, executor=recording)
    runner.add("range_test", scale=2.0)
    snapshot = runner.load_history(10)
    (only,) = snapshot["indicators"].values()
    assert only["outputs"]["range"] == [4.0] * 10
    assert recording.prepared[0].key == "range_test"
