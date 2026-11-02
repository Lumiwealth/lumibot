"""Indicator v2: all-or-nothing bar commits and scheduled inference.

Two guarantees that matter once indicator code can fail or do expensive work
(for example a call to an AI model):

* A closed bar commits all of its work or none of it. An exception part way
  through ``step`` (or through one of several indicators in a runner) leaves
  the committed state exactly as it was, so retrying the same bar gives the
  same answer as a clean run.
* ``infer`` (the hook for expensive or side-effecting work) runs only on live
  closed bars, on its own schedule. History replay, ``compute``, catch-up over
  older bars and forming-bar previews never call it, because those calls could
  not be undone and would repeat on every preview or replay.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from lumibot.indicators import Indicator, IndicatorRunner, InputSpec, OutputSpec


def bars(closes, *, minutes=1, start=None):
    start = start or datetime(2026, 1, 5, 14, 30, tzinfo=timezone.utc)
    return [
        {"t": start + timedelta(minutes=i * minutes), "o": c, "h": c + 1, "l": c - 1, "c": c, "v": 10.0}
        for i, c in enumerate(closes)
    ]


class _Sum(Indicator):
    """Running sum of closes; mutates state before it can fail."""

    key = "test_atomic_sum"
    outputs = [OutputSpec("sum", "Sum", "line", "#2962FF")]
    fail_on: set = set()

    def init_state(self):
        return {"sum": 0.0, "count": 0}

    def step(self, state, bar):
        state["sum"] += bar.close
        state["count"] += 1
        if bar.close in self.fail_on:
            self.fail_on.discard(bar.close)
            raise RuntimeError("failed part way through the bar")
        return {"sum": state["sum"]}


class _Signal(Indicator):
    """Counts how often ``infer`` runs and stores its answer in state."""

    key = "test_inference_signal"
    inputs = [InputSpec("every", "int", 1, min=1, label="Every")]
    outputs = [
        OutputSpec("close", "Close", "line", "#2962FF"),
        OutputSpec("signal", "Signal", "line", "#ef4444"),
    ]

    def __init__(self, **inputs):
        self.calls = []
        self.fail_next = False
        super().__init__(**inputs)
        self.infer_every_bars = self.inputs["every"]

    def init_state(self):
        return {"signal": None}

    def step(self, state, bar):
        return {"close": bar.close, "signal": state["signal"]}

    def infer(self, state, bar):
        self.calls.append(bar.close)
        if self.fail_next:
            self.fail_next = False
            raise RuntimeError("model call failed")
        state["signal"] = bar.close * 10
        return {"signal": state["signal"]}


# --------------------------------------------------------------------------
# All-or-nothing commits
# --------------------------------------------------------------------------


def test_failed_closed_bar_leaves_committed_state_untouched_and_retry_matches_clean_run():
    data = bars([1, 2, 3, 4])
    clean = _Sum().feed(data)["sum"]

    flaky = _Sum()
    flaky.fail_on = {3}
    flaky.update(data[0], closed=True)
    flaky.update(data[1], closed=True)
    with pytest.raises(RuntimeError):
        flaky.update(data[2], closed=True)
    assert flaky.bars_seen == 2
    assert flaky.snapshot()["state"] == {"sum": 3.0, "count": 2}
    # Retrying the same bar works and is not double counted.
    assert flaky.update(data[2], closed=True) == {"sum": clean[2]}
    assert flaky.update(data[3], closed=True) == {"sum": clean[3]}


def test_runner_commits_a_closed_bar_for_every_indicator_or_none():
    history = bars([1, 2, 3])
    live = bars([1, 2, 3, 4, 5])[3:]
    source = _History(history)
    runner = IndicatorRunner(source, "BTC", timestep="minute")
    healthy = runner.add(_Sum())
    flaky_instance = _Sum()
    flaky = runner.add(flaky_instance)
    runner.load_history(3)

    flaky_instance.fail_on = {4}
    with pytest.raises(RuntimeError):
        runner.on_bar(live[0], closed=True)
    # Nothing moved: no new bar, no new output on either indicator.
    assert len(runner.snapshot()["bars"]) == 3
    assert len(runner.outputs(healthy)["sum"]) == 3
    assert len(runner.outputs(flaky)["sum"]) == 3

    # The retry succeeds, and the healthy indicator is not applied twice.
    result = runner.on_bar(live[0], closed=True)
    assert result[healthy] == {"sum": 10.0}
    assert result[flaky] == {"sum": 10.0}
    assert runner.on_bar(live[1], closed=True)[healthy] == {"sum": 15.0}
    # The runner keeps the last 3 bars (the history length).
    assert runner.outputs(healthy)["sum"] == runner.outputs(flaky)["sum"] == [6.0, 10.0, 15.0]


# --------------------------------------------------------------------------
# Scheduled inference
# --------------------------------------------------------------------------


def test_history_compute_and_forming_previews_never_call_infer():
    data = bars([1, 2, 3, 4, 5])
    signal = _Signal()
    signal.feed(data[:4])
    assert signal.compute(data)["signal"] == [None] * 5
    for close in (50, 60, 70):
        signal.update({**data[4], "c": close}, closed=False)
    assert signal.calls == []
    # The first live closed bar infers once.
    assert signal.update(data[4], closed=True) == {"close": 5.0, "signal": 50.0}
    assert signal.calls == [5]


def test_infer_runs_on_its_own_bar_schedule():
    data = bars(range(1, 11))
    signal = _Signal(every=3)
    for bar in data:
        signal.update(bar, closed=True)
    # Every third live closed bar, starting with the first.
    assert signal.calls == [1, 4, 7, 10]


def test_infer_respects_a_minimum_bar_time_gap():
    data = bars(range(1, 8))  # one-minute bars
    signal = _Signal()
    signal.infer_min_seconds = 180
    for bar in data:
        signal.update(bar, closed=True)
    assert signal.calls == [1, 4, 7]


def test_failed_infer_commits_nothing_and_the_retry_calls_it_once_more():
    data = bars([1, 2])
    signal = _Signal()
    signal.update(data[0], closed=True)
    signal.fail_next = True
    with pytest.raises(RuntimeError):
        signal.update(data[1], closed=True)
    assert signal.bars_seen == 1
    assert signal.update(data[1], closed=True) == {"close": 2.0, "signal": 20.0}
    assert signal.calls == [1, 2, 2]


def test_snapshot_keeps_the_inference_schedule():
    data = bars(range(1, 6))
    first = _Signal(every=3)
    for bar in data[:2]:
        first.update(bar, closed=True)
    second = _Signal(every=3)
    second.restore(first.snapshot())
    for bar in data[2:]:
        second.update(bar, closed=True)
    # Bar 1 inferred before the hand-off; the schedule continues at bar 4.
    assert first.calls == [1]
    assert second.calls == [4]


def test_runner_history_and_late_add_never_infer_but_live_bars_do():
    data = bars(range(1, 9))
    runner = IndicatorRunner(_History(data[:6]), "BTC", timestep="minute")
    early = _Signal()
    runner.add(early)
    runner.load_history(6)
    late = _Signal()
    runner.add(late)
    runner.on_bar({**data[6], "c": 99}, closed=False)
    assert early.calls == late.calls == []
    runner.on_bar(data[6], closed=True)
    assert early.calls == late.calls == [7]


def test_strategy_indicators_catch_up_infers_only_on_the_newest_bar():
    from lumibot.indicators import Indicators, registry

    reg_key = _Signal.key
    registry.register(_Signal)
    try:
        dates = pd.date_range("2024-01-02", periods=40, freq="B", tz="UTC")
        df = pd.DataFrame(
            {"open": 1.0, "high": 2.0, "low": 0.5, "close": [float(i) for i in range(40)], "volume": 1.0},
            index=dates,
        )
        strat = _strategy_stub(df, dates[30])
        ind = Indicators(strat)
        assert ind.get(reg_key, strat.asset) == {"close": 30.0, "signal": 300.0}
        stream = next(iter(ind._streams.values()))["indicator"]
        assert stream.calls == [30.0]
        strat.set_now(dates[31])
        ind.get(reg_key, strat.asset)
        assert stream.calls == [30.0, 31.0]
    finally:
        registry.unregister(reg_key)


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------


class _History:
    def __init__(self, data):
        self.data = data

    def get_historical_prices(self, asset, length, timestep="", quote=None, **kwargs):
        rows = self.data[-length:]
        frame = pd.DataFrame(
            {
                "open": [b["o"] for b in rows],
                "high": [b["h"] for b in rows],
                "low": [b["l"] for b in rows],
                "close": [b["c"] for b in rows],
                "volume": [b["v"] for b in rows],
            },
            index=pd.DatetimeIndex([b["t"] for b in rows]),
        )
        return type("Bars", (), {"df": frame})()


def _strategy_stub(df, now):
    import tests.test_indicators_v2 as v2

    return v2._strategy_stub(df, now)
