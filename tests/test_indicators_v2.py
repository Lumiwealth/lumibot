"""Indicator v2: one-bar updates, rollback, snapshots, registry and runner.

Expected values come from two independent places, never from the module under
test:

* hand-typed numbers on tiny series (worked out in the comments), and
* plain-loop reference implementations written from the published formulas
  (TradingView Pine v6 semantics: ``ema``/``rma`` seeded with the SMA of the
  first ``length`` values, ``rsi``/``atr`` smoothed with ``rma``, population
  standard deviation for Bollinger Bands, ``vwap`` reset at each UTC day).
"""

from __future__ import annotations

import math
import pickle
import types
from datetime import datetime, timedelta, timezone
from fractions import Fraction
from typing import Any

import numpy as np
import pandas as pd
import pytest

from lumibot.indicators import (
    Indicator,
    IndicatorRegistry,
    IndicatorRunner,
    InputSpec,
    InvalidIndicatorInputError,
    OutputSpec,
    UnknownIndicatorError,
    UnknownIndicatorVersionError,
    registry,
)

TOL = 1e-9
BUILTIN_KEYS = {"sma", "ema", "rsi", "macd", "bbands", "atr", "stoch", "vwap"}


# --------------------------------------------------------------------------
# Fixture data
# --------------------------------------------------------------------------


def make_bars(count: int = 80) -> list[dict[str, Any]]:
    start = datetime(2026, 10, 1, 0, 0, tzinfo=timezone.utc)
    bars: list[dict[str, Any]] = []
    prev_close = 100.0
    for i in range(count):
        close = 100.0 + 10.0 * math.sin(i / 5.0) + 0.3 * i + (1.7 if i % 7 == 0 else 0.0)
        open_ = prev_close
        high = max(open_, close) + 1.0 + (i % 3) * 0.2
        low = min(open_, close) - 1.0 - (i % 4) * 0.1
        volume = 1000.0 + float((i * 37) % 500)
        bars.append(
            {"t": start + timedelta(hours=i), "o": open_, "h": high, "l": low, "c": close, "v": volume}
        )
        prev_close = close
    return bars


BARS = make_bars()


def cols(bars):
    return (
        [b["o"] for b in bars],
        [b["h"] for b in bars],
        [b["l"] for b in bars],
        [b["c"] for b in bars],
        [b["v"] for b in bars],
    )


def tiny_bars(closes, *, spread=1.0, volume=10.0, start=None):
    start = start or datetime(2026, 1, 5, 14, 30, tzinfo=timezone.utc)
    return [
        {"t": start + timedelta(minutes=i), "o": c, "h": c + spread, "l": c - spread, "c": c, "v": volume}
        for i, c in enumerate(closes)
    ]


# --------------------------------------------------------------------------
# Independent reference implementations (plain loops)
# --------------------------------------------------------------------------

Num = float | None


def ref_sma(xs: list[Num], n: int) -> list[Num]:
    out: list[Num] = []
    seen: list[float] = []
    for x in xs:
        if x is None:
            out.append(None)
            continue
        seen.append(x)
        out.append(None if len(seen) < n else sum(seen[-n:]) / n)
    return out


def _ref_seeded(xs: list[Num], n: int, step) -> list[Num]:
    out: list[Num] = []
    seen: list[float] = []
    prev = None
    for x in xs:
        if x is None:
            out.append(None)
            continue
        if prev is None:
            seen.append(x)
            if len(seen) < n:
                out.append(None)
                continue
            prev = sum(seen[-n:]) / n
        else:
            prev = step(prev, x)
        out.append(prev)
    return out


def ref_ema(xs: list[Num], n: int) -> list[Num]:
    a = 2.0 / (n + 1)
    return _ref_seeded(xs, n, lambda p, x: p + a * (x - p))


def ref_rma(xs: list[Num], n: int) -> list[Num]:
    return _ref_seeded(xs, n, lambda p, x: (p * (n - 1) + x) / n)


def sub(a: list[Num], b: list[Num]) -> list[Num]:
    return [None if x is None or y is None else x - y for x, y in zip(a, b)]


def assert_series(actual, expected, tol=TOL):
    assert len(actual) == len(expected)
    for i, (got, want) in enumerate(zip(actual, expected)):
        if want is None:
            assert got is None, f"bar {i}: expected None, got {got}"
        else:
            assert got is not None, f"bar {i}: expected {want}, got None"
            assert abs(got - want) <= tol * max(1.0, abs(want)), f"bar {i}: {got} != {want}"


def compute(key, bars=None, **inputs):
    return registry.create(key, **inputs).compute(BARS if bars is None else bars)


# --------------------------------------------------------------------------
# Hand-computed values on tiny series
# --------------------------------------------------------------------------


def test_sma_and_ema_hand_values():
    bars = tiny_bars([1, 2, 3, 4, 5, 6])
    assert compute("sma", bars, length=3)["sma"] == pytest.approx([None, None, 2.0, 3.0, 4.0, 5.0])
    # EMA(3): seeded with SMA 2.0 on bar 2, alpha .5 -> 3, 4, 5
    assert compute("ema", bars, length=3)["ema"] == pytest.approx([None, None, 2.0, 3.0, 4.0, 5.0])
    # seed (2+4+6)/3 = 4; 4+.5*(2-4) = 3; 3+.5*(10-3) = 6.5
    assert compute("ema", tiny_bars([2, 4, 6, 2, 10]), length=3)["ema"] == pytest.approx(
        [None, None, 4.0, 3.0, 6.5]
    )


def test_rsi_hand_values_and_saturation():
    # changes +1 +1 -1 +2, length 2
    out = compute("rsi", tiny_bars([10, 11, 12, 11, 13]), length=2)["rsi"]
    # bar 2: up seed (1+1)/2 = 1, down 0 -> saturated 100
    # bar 3: up (1+0)/2 = .5, down (0+1)/2 = .5 -> 50
    # bar 4: up (.5+2)/2 = 1.25, down (.5+0)/2 = .25 -> 100 - 100/6
    assert out[:2] == [None, None]
    assert out[2] == 100.0
    assert out[3] == pytest.approx(50.0)
    assert out[4] == pytest.approx(100 - 100 / 6)
    # straight down: up is 0 -> 0
    assert compute("rsi", tiny_bars([5, 4, 3]), length=2)["rsi"][2] == 0.0


def test_macd_hand_values():
    out = compute("macd", tiny_bars([1, 2, 3, 4, 6, 9]), fast_length=2, slow_length=3, signal_length=2)
    f = Fraction
    # ema2 (alpha 2/3): b1 3/2, b2 5/2, b3 7/2, b4 31/6, b5 139/18
    # ema3 (alpha 1/2): b2 2, b3 3, b4 9/2, b5 27/4
    macd = [None, None, f(1, 2), f(1, 2), f(2, 3), f(35, 36)]
    # signal ema2 over macd, seeded on b3 with (1/2+1/2)/2
    signal = [None, None, None, f(1, 2), f(11, 18), f(23, 27)]
    assert out["macd"] == pytest.approx([None if v is None else float(v) for v in macd])
    assert out["signal"] == pytest.approx([None if v is None else float(v) for v in signal])
    assert out["histogram"] == pytest.approx([None, None, None, 0.0, float(f(2, 3) - f(11, 18)), float(f(35, 36) - f(23, 27))])


def test_bbands_hand_values_population_stdev():
    out = compute("bbands", tiny_bars([1, 3, 5]), length=2, mult=1.0)
    # bar 1: basis 2, population stdev 1 -> 3 / 1 ; bar 2: basis 4, stdev 1 -> 5 / 3
    assert out["basis"] == pytest.approx([None, 2.0, 4.0])
    assert out["upper"] == pytest.approx([None, 3.0, 5.0])
    assert out["lower"] == pytest.approx([None, 1.0, 3.0])


def test_atr_hand_values_first_true_range_is_high_minus_low():
    bars = [
        {"t": datetime(2026, 1, 1, tzinfo=timezone.utc), "o": 10, "h": 11, "l": 9, "c": 10, "v": 1},
        {"t": datetime(2026, 1, 2, tzinfo=timezone.utc), "o": 10, "h": 12, "l": 10, "c": 11, "v": 1},
        {"t": datetime(2026, 1, 3, tzinfo=timezone.utc), "o": 11, "h": 13, "l": 10, "c": 12, "v": 1},
    ]
    # tr: 2 (h-l), max(2, 2, 0) = 2, max(3, 2, 1) = 3 ; rma(2): seed 2, then (2+3)/2
    assert compute("atr", bars, length=2)["atr"] == pytest.approx([None, 2.0, 2.5])


def test_stoch_hand_values_not_clamped():
    bars = tiny_bars([10, 12, 11])  # high = c+1, low = c-1
    out = compute("stoch", bars, period_k=2, smooth_k=1, period_d=2)
    # bar 1: hh 13, ll 9 -> 75 ; bar 2: hh 13, ll 10 -> 100/3
    assert out["k"] == pytest.approx([None, 75.0, 100 / 3])
    assert out["d"] == pytest.approx([None, None, (75 + 100 / 3) / 2])
    # Close outside the window range is not clamped to 0..100
    spike = [
        {"t": datetime(2026, 1, 1, tzinfo=timezone.utc), "o": 1, "h": 2, "l": 1, "c": 1.5, "v": 1},
        {"t": datetime(2026, 1, 2, tzinfo=timezone.utc), "o": 1, "h": 2, "l": 1, "c": 3.0, "v": 1},
    ]
    assert compute("stoch", spike, period_k=2, smooth_k=1, period_d=1)["k"][1] == pytest.approx(200.0)


def test_vwap_hand_values_reset_each_utc_day():
    day1 = datetime(2026, 3, 2, 22, 0, tzinfo=timezone.utc)
    bars = [
        {"t": day1, "o": 10, "h": 10, "l": 10, "c": 10, "v": 1},
        {"t": day1 + timedelta(hours=1), "o": 20, "h": 20, "l": 20, "c": 20, "v": 3},
        {"t": day1 + timedelta(hours=2), "o": 30, "h": 30, "l": 30, "c": 30, "v": 2},  # next UTC day
    ]
    assert compute("vwap", bars)["vwap"] == pytest.approx([10.0, 17.5, 30.0])


def test_vwap_requires_bar_time():
    with pytest.raises(ValueError, match="time"):
        registry.create("vwap").update({"o": 1, "h": 1, "l": 1, "c": 1, "v": 1}, closed=True)


# --------------------------------------------------------------------------
# Each built-in against loop references on the fixture
# --------------------------------------------------------------------------


def test_sma_ema_reference_with_source():
    o, h, l, c, _ = cols(BARS)
    assert_series(compute("sma", length=10)["sma"], ref_sma(list(c), 10))
    hl2 = [(a + b) / 2 for a, b in zip(h, l)]
    assert_series(compute("ema", length=9, source="hl2")["ema"], ref_ema(hl2, 9))


def test_rsi_reference():
    *_, c, _ = cols(BARS)
    up = [None] + [max(c[i] - c[i - 1], 0.0) for i in range(1, len(c))]
    dn = [None] + [max(c[i - 1] - c[i], 0.0) for i in range(1, len(c))]
    expected = []
    for u, d in zip(ref_rma(up, 14), ref_rma(dn, 14)):
        if u is None or d is None:
            expected.append(None)
        elif d <= 1e-10:
            expected.append(100.0)
        elif u <= 1e-10:
            expected.append(0.0)
        else:
            expected.append(100 - 100 / (1 + u / d))
    out = compute("rsi")["rsi"]
    assert_series(out, expected)
    assert out[13] is None and out[14] is not None


def test_macd_reference():
    *_, c, _ = cols(BARS)
    out = compute("macd")
    macd = sub(ref_ema(list(c), 12), ref_ema(list(c), 26))
    signal = ref_ema(macd, 9)
    assert_series(out["macd"], macd)
    assert_series(out["signal"], signal)
    assert_series(out["histogram"], sub(macd, signal))
    assert out["macd"][24] is None and out["macd"][25] is not None
    assert out["signal"][32] is None and out["signal"][33] is not None


def test_bbands_reference():
    *_, c, _ = cols(BARS)
    out = compute("bbands", length=20, mult=2.5)
    basis = ref_sma(list(c), 20)
    upper, lower = [], []
    for i, mid in enumerate(basis):
        if mid is None:
            upper.append(None)
            lower.append(None)
            continue
        window = c[i - 19 : i + 1]
        sd = math.sqrt(sum((x - mid) ** 2 for x in window) / 20)
        upper.append(mid + 2.5 * sd)
        lower.append(mid - 2.5 * sd)
    assert_series(out["basis"], basis)
    assert_series(out["upper"], upper, tol=1e-8)
    assert_series(out["lower"], lower, tol=1e-8)


def test_atr_reference():
    _, h, l, c, _ = cols(BARS)
    tr = [h[0] - l[0]] + [
        max(h[i] - l[i], abs(h[i] - c[i - 1]), abs(l[i] - c[i - 1])) for i in range(1, len(c))
    ]
    out = compute("atr", length=14)["atr"]
    assert_series(out, ref_rma(tr, 14))
    assert out[12] is None and out[13] is not None


def test_stoch_reference():
    _, h, l, c, _ = cols(BARS)
    raw = [None] * 13 + [
        100 * (c[i] - min(l[i - 13 : i + 1])) / (max(h[i - 13 : i + 1]) - min(l[i - 13 : i + 1]))
        for i in range(13, len(c))
    ]
    k = ref_sma(raw, 3)
    out = compute("stoch", period_k=14, smooth_k=3, period_d=3)
    assert_series(out["k"], k)
    assert_series(out["d"], ref_sma(k, 3))


def test_vwap_reference_resets_each_utc_day():
    _, h, l, c, v = cols(BARS)
    expected, day, pv, vol = [], None, 0.0, 0.0
    for i, bar in enumerate(BARS):
        if bar["t"].date() != day:
            day, pv, vol = bar["t"].date(), 0.0, 0.0
        pv += (h[i] + l[i] + c[i]) / 3 * v[i]
        vol += v[i]
        expected.append(pv / vol)
    out = compute("vwap")["vwap"]
    assert_series(out, expected)
    assert out[24] == pytest.approx((h[24] + l[24] + c[24]) / 3)


# --------------------------------------------------------------------------
# Streaming contract: parity, replay, rollback, snapshot
# --------------------------------------------------------------------------

DEFAULT_CASES = [
    ("sma", {}),
    ("ema", {}),
    ("rsi", {}),
    ("macd", {}),
    ("bbands", {}),
    ("atr", {}),
    ("stoch", {}),
    ("vwap", {}),
]


def test_default_cases_cover_every_builtin():
    assert {k for k, _ in DEFAULT_CASES} == BUILTIN_KEYS
    assert {d["key"] for d in registry.list()} >= BUILTIN_KEYS


def _loop(ind, bars):
    out = {o.id: [] for o in ind.outputs}
    for bar in bars:
        values = ind.update(bar, closed=True)
        assert set(values) == set(out)
        for k, v in values.items():
            out[k].append(v)
    return out


@pytest.mark.parametrize("key,inputs", DEFAULT_CASES)
def test_compute_equals_update_loop(key, inputs):
    bars = make_bars(150)
    assert registry.create(key, **inputs).compute(bars) == _loop(registry.create(key, **inputs), bars)


@pytest.mark.parametrize("key,inputs", DEFAULT_CASES)
@pytest.mark.parametrize("k", [1, 17, 40, 63, 149])
def test_replay_prefix_equals_full_run_no_lookahead(key, inputs, k):
    bars = make_bars(150)
    full = registry.create(key, **inputs).compute(bars)
    prefix = registry.create(key, **inputs).compute(bars[:k])
    for output_id, values in full.items():
        assert prefix[output_id] == values[:k], (key, output_id)


def _perturb(bar, j):
    shift = (j % 5 - 2) * 0.7
    c = bar["c"] + shift
    return {**bar, "h": max(bar["h"], c) + 0.1 * j, "l": min(bar["l"], c) - 0.05 * j, "c": c, "v": bar["v"] + j}


@pytest.mark.parametrize("key,inputs", DEFAULT_CASES)
def test_forming_bar_updates_roll_back(key, inputs):
    bars = make_bars(120)
    streamed = registry.create(key, **inputs)
    closed_only = registry.create(key, **inputs)
    for bar in bars:
        for j in range(7):
            streamed.update(_perturb(bar, j), closed=False)
        # A forming update with the final values shows exactly what closing it will commit.
        forming = streamed.update(bar, closed=False)
        committed = streamed.update(bar, closed=True)
        assert committed == closed_only.update(bar, closed=True)
        assert forming == committed
    assert streamed.snapshot() == closed_only.snapshot()


@pytest.mark.parametrize("key,inputs", DEFAULT_CASES)
def test_snapshot_restore_round_trip(key, inputs):
    bars = make_bars(120)
    full = registry.create(key, **inputs).compute(bars)
    first = registry.create(key, **inputs)
    head = _loop(first, bars[:57])
    state = pickle.loads(pickle.dumps(first.snapshot()))
    second = registry.create(key, **inputs)
    second.restore(state)
    tail = _loop(second, bars[57:])
    for output_id, values in full.items():
        assert head[output_id] + tail[output_id] == values
    # Restoring must not alias the original state.
    first.update(bars[57], closed=True)
    assert second.snapshot() != first.snapshot()


def test_restore_rejects_other_indicator_or_version():
    snap = registry.create("sma").snapshot()
    with pytest.raises(ValueError):
        registry.create("ema").restore(snap)
    with pytest.raises(ValueError):
        registry.create("sma", length=5).restore(snap)


def test_closed_bars_must_move_forward_in_time():
    ind = registry.create("sma", length=2)
    bars = tiny_bars([1, 2, 3])
    ind.update(bars[1], closed=True)
    with pytest.raises(ValueError, match="time"):
        ind.update(bars[0], closed=True)
    with pytest.raises(ValueError, match="time"):
        ind.update(bars[1], closed=False)


def test_bar_shapes_dataframe_and_named_fields():
    df = pd.DataFrame(
        {"open": [1.0, 2.0, 3.0], "high": [1.0, 2.0, 3.0], "low": [1.0, 2.0, 3.0], "close": [1.0, 2.0, 3.0], "volume": [1, 1, 1]},
        index=pd.date_range("2026-01-01", periods=3, freq="D", tz="UTC"),
    )
    assert registry.create("sma", length=2).compute(df)["sma"] == pytest.approx([None, 1.5, 2.5])
    named = {"time": "2026-01-01T00:00:00Z", "open": 1, "high": 2, "low": 0.5, "close": 1.5, "volume": 3}
    assert registry.create("sma", length=1).update(named, closed=True) == {"sma": 1.5}
    with pytest.raises(ValueError, match="finite"):
        registry.create("sma").update({**named, "close": float("nan")}, closed=True)


def test_reset_clears_state_and_applies_new_inputs():
    ind = registry.create("sma", length=2)
    for bar in tiny_bars([1, 2, 3]):
        ind.update(bar, closed=True)
    ind.reset({"length": 3})
    assert ind.inputs["length"] == 3
    assert [ind.update(b, closed=True)["sma"] for b in tiny_bars([4, 5, 6])] == pytest.approx([None, None, 5.0])


# --------------------------------------------------------------------------
# Inputs, describe() and registry
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "inputs",
    [{"lenght": 5}, {"length": 0}, {"length": 2.5}, {"length": True}, {"length": "5"}, {"source": "median"}],
)
def test_invalid_inputs_raise(inputs):
    with pytest.raises(InvalidIndicatorInputError):
        registry.create("sma", **inputs)


def test_float_input_accepts_int_and_enforces_range():
    assert registry.create("bbands", mult=3).inputs["mult"] == 3.0
    with pytest.raises(InvalidIndicatorInputError):
        registry.create("bbands", mult=-1)


def test_describe_schema_for_every_builtin():
    allowed_types = {"int", "float", "bool", "color", "source", "string", "enum"}
    allowed_kinds = {"line", "area", "histogram", "scatter", "markers"}
    described = {d["key"]: d for d in registry.list()}
    for key in BUILTIN_KEYS:
        d = described[key]
        assert isinstance(d["version"], int) and d["version"] >= 1
        assert d["title"] and d["short_title"]
        assert isinstance(d["overlay"], bool)
        assert d["outputs"], key
        for spec in d["inputs"]:
            assert spec["type"] in allowed_types
            assert {"name", "type", "default", "min", "max", "step", "options", "label"} <= spec.keys()
        for out in d["outputs"]:
            assert {"id", "label", "kind", "color"} <= out.keys()
            assert out["kind"] in allowed_kinds
            assert out["color"].startswith("#")
        for lv in d["levels"]:
            assert {"value", "label", "style"} <= lv.keys()
    assert described["sma"]["overlay"] is True and described["rsi"]["overlay"] is False
    src = next(s for s in described["sma"]["inputs"] if s["name"] == "source")
    assert src["type"] == "source" and "ohlc4" in src["options"]
    # describe is JSON-safe
    import json

    json.dumps(registry.list())


class _Momentum(Indicator):
    key = "test_momentum"
    version = 1
    title = "Momentum"
    short_title = "MOM"
    overlay = False
    inputs = [InputSpec("length", "int", 3, min=1, label="Length")]
    outputs = [OutputSpec("mom", "Momentum", "line", "#2962FF")]

    def init_state(self):
        return {"closes": []}

    def step(self, state, bar):
        state["closes"].append(bar.close)
        n = self.inputs["length"]
        if len(state["closes"]) <= n:
            return {"mom": None}
        del state["closes"][:-(n + 1)]
        return {"mom": state["closes"][-1] - state["closes"][0]}


class _MomentumV2(_Momentum):
    version = 2


def test_registry_get_list_version_pinning_and_errors():
    reg = IndicatorRegistry()
    reg.register(_Momentum)
    reg.register(_MomentumV2)
    reg.register(_Momentum)  # idempotent
    assert reg.get("test_momentum") is _MomentumV2
    assert reg.get("test_momentum", 1) is _Momentum
    assert [d["version"] for d in reg.list()] == [2]
    assert sorted(d["version"] for d in reg.list(all_versions=True)) == [1, 2]
    with pytest.raises(UnknownIndicatorError):
        reg.get("nope")
    with pytest.raises(UnknownIndicatorVersionError):
        reg.get("test_momentum", 7)

    class _Clash(_Momentum):
        pass

    with pytest.raises(ValueError, match="immutable"):
        reg.register(_Clash)
    with pytest.raises(TypeError):
        reg.register(object)


def test_global_registry_builtins_and_custom_registration():
    assert registry.get("rsi").version == 1
    assert registry.get("rsi", 1) is registry.get("rsi")
    with pytest.raises(UnknownIndicatorError):
        registry.get("not_a_thing")
    with pytest.raises(UnknownIndicatorVersionError):
        registry.get("rsi", 99)
    registry.register(_Momentum)
    try:
        assert registry.create("test_momentum", length=2).compute(tiny_bars([1, 2, 4, 7]))["mom"] == [None, None, 3, 5]
    finally:
        registry.unregister("test_momentum")
    with pytest.raises(UnknownIndicatorError):
        registry.get("test_momentum")


def test_custom_indicator_gets_rollback_for_free():
    ind = _Momentum(length=1)
    bars = tiny_bars([1, 2, 4])
    ind.update(bars[0], closed=True)
    for c in (50, 60, 70):
        assert ind.update({**bars[1], "c": c}, closed=False) == {"mom": c - 1}
    assert ind.update(bars[1], closed=True) == {"mom": 1}
    assert ind.update(bars[2], closed=True) == {"mom": 2}


# --------------------------------------------------------------------------
# IndicatorRunner
# --------------------------------------------------------------------------


class _FakeBars:
    def __init__(self, df):
        self.df = df


def _bars_df(bars):
    return pd.DataFrame(
        {
            "open": [b["o"] for b in bars],
            "high": [b["h"] for b in bars],
            "low": [b["l"] for b in bars],
            "close": [b["c"] for b in bars],
            "volume": [b["v"] for b in bars],
        },
        index=pd.DatetimeIndex([b["t"] for b in bars]),
    )


class _FakeDataSource:
    """Stand-in for an external market data source (only the public getter)."""

    def __init__(self, bars):
        self.bars = bars
        self.calls = []

    def get_historical_prices(self, asset, length, timestep="", quote=None, **kwargs):
        self.calls.append({"asset": asset, "length": length, "timestep": timestep, "quote": quote})
        return _FakeBars(_bars_df(self.bars[-length:]))


def test_runner_history_then_live_updates():
    history, live = BARS[:60], BARS[60:]
    source = _FakeDataSource(history)
    runner = IndicatorRunner(source, "BTC", quote="USD", timestep="minute")
    rsi_id = runner.add("rsi", length=14)
    macd_id = runner.add("macd", instance_id="m1")
    assert macd_id == "m1"
    snap = runner.load_history(60)
    assert source.calls == [{"asset": "BTC", "length": 60, "timestep": "minute", "quote": "USD"}]
    assert len(snap["bars"]) == 60
    assert snap["indicators"][rsi_id]["key"] == "rsi" and snap["indicators"][rsi_id]["version"] == 1
    assert snap["indicators"][rsi_id]["outputs"] == compute("rsi", history, length=14)
    full_rsi = compute("rsi", BARS, length=14)["rsi"]
    full_macd = compute("macd", BARS)
    for i, bar in enumerate(live):
        forming = runner.on_bar(_perturb(bar, 3), closed=False)
        assert set(forming) == {rsi_id, macd_id}
        closed = runner.on_bar(bar, closed=True)
        assert closed[rsi_id]["rsi"] == full_rsi[60 + i]
        assert closed[macd_id]["signal"] == full_macd["signal"][60 + i]


def test_runner_add_and_remove_after_history_replays_kept_bars():
    source = _FakeDataSource(BARS)
    runner = IndicatorRunner(source, "SPY", timestep="day")
    runner.load_history(80)
    late = runner.add("bbands", length=10)
    assert runner.outputs(late) == compute("bbands", BARS, length=10)
    runner.on_bar(make_bars(81)[80], closed=True)
    # The runner keeps the last 80 bars, so the oldest value rolls off.
    assert runner.outputs(late)["basis"] == compute("bbands", make_bars(81), length=10)["basis"][1:]
    runner.remove(late)
    with pytest.raises(KeyError):
        runner.outputs(late)


def test_runner_with_real_pandas_backtesting_data_source():
    from lumibot.backtesting import PandasDataBacktesting
    from lumibot.entities import Asset, Data

    index = pd.date_range("2025-01-02", periods=60, freq="B", tz="America/New_York")
    closes = [100 + 5 * math.sin(i / 4) + 0.2 * i for i in range(60)]
    df = pd.DataFrame(
        {"open": closes, "high": [c + 1 for c in closes], "low": [c - 1 for c in closes], "close": closes, "volume": [1000] * 60},
        index=index,
    )
    asset = Asset("TESTSTOCK", asset_type=Asset.AssetType.STOCK)
    source = PandasDataBacktesting(
        datetime_start=datetime(2025, 1, 2),
        datetime_end=datetime(2025, 4, 1),
        pandas_data={asset: Data(asset, df, timestep="day")},
    )
    source._update_datetime(index[40].to_pydatetime())
    runner = IndicatorRunner(source, asset, timestep="day")
    sma_id = runner.add("sma", length=5)
    snap = runner.load_history(20)
    got_closes = [b["c"] for b in snap["bars"]]
    assert 0 < len(got_closes) <= 20
    # Only bars at or before the data source's clock enter the history.
    assert all(pd.Timestamp(b["t"]) <= index[40] for b in snap["bars"])
    expected = ref_sma(got_closes, 5)
    assert_series(snap["indicators"][sma_id]["outputs"]["sma"], expected)


# --------------------------------------------------------------------------
# Inside a strategy: self.indicators.get
# --------------------------------------------------------------------------


def _strategy_stub(df, now):
    class _Data:
        def __init__(self, frame):
            self.df = frame
            self.timestep = "day"

    class _Asset:
        symbol = "TESTSTOCK"
        asset_type = "stock"

    asset = _Asset()
    source = types.SimpleNamespace(_data_store={(asset, None): _Data(df)})
    state = {"now": now}
    strat = types.SimpleNamespace(broker=types.SimpleNamespace(data_source=source), asset=asset)
    strat.get_datetime = lambda: state["now"]
    strat.set_now = lambda t: state.__setitem__("now", t)
    return strat


def test_indicators_get_matches_compute_on_observed_prefix_and_handles_rewind():
    from lumibot.indicators import Indicators

    dates = pd.date_range("2024-01-02", periods=120, freq="B", tz="UTC")
    rng = np.random.default_rng(7)
    prices = 100 + np.cumsum(rng.normal(0, 1, size=len(dates)))
    df = pd.DataFrame(
        {"open": prices, "high": prices + 0.5, "low": prices - 0.5, "close": prices, "volume": 1000},
        index=dates,
    )
    strat = _strategy_stub(df, dates[30])
    ind = Indicators(strat)
    full = registry.create("rsi", length=7).compute(df)
    for i in list(range(30, 120, 7)) + [119, 50, 51]:  # forward, then rewind
        strat.set_now(dates[i])
        got = ind.get("rsi", strat.asset, length=7)
        assert got == {"rsi": full["rsi"][i]}
    # Version pinning and unknown keys surface clearly.
    assert ind.get("rsi", strat.asset, version=1, length=7) == {"rsi": full["rsi"][51]}
    with pytest.raises(UnknownIndicatorError):
        ind.get("not_a_thing", strat.asset)
    # Existing passthrough is untouched.
    assert ind.sma(strat.asset, length=5) == pytest.approx(df["close"].iloc[47:52].mean())


def test_indicators_get_recomputes_when_last_observed_bar_is_corrected():
    from lumibot.indicators import Indicators

    dates = pd.date_range("2024-01-02", periods=10, freq="B", tz="UTC")
    df = pd.DataFrame({"open": 1.0, "high": 1.0, "low": 1.0, "close": np.arange(1.0, 11.0), "volume": 1}, index=dates)
    strat = _strategy_stub(df, dates[4])
    ind = Indicators(strat)
    assert ind.get("sma", strat.asset, length=2) == {"sma": 4.5}
    df.loc[dates[4], "close"] = 9.0
    assert ind.get("sma", strat.asset, length=2) == {"sma": 6.5}


@pytest.mark.usefixtures("disable_datasource_override")
def test_strategy_backtest_indicators_get_and_existing_passthrough(tmp_path, monkeypatch):
    from lumibot.backtesting import PandasDataBacktesting
    from lumibot.entities import Asset, Data
    from lumibot.strategies import Strategy

    monkeypatch.setenv("LUMIBOT_CACHE_FOLDER", str(tmp_path / "cache"))
    index = pd.date_range("2024-12-02", periods=40, freq="B", tz="America/New_York")
    closes = [50 + 3 * math.sin(i / 3) + 0.1 * i for i in range(40)]
    frame = pd.DataFrame(
        {"open": closes, "high": [c + 0.5 for c in closes], "low": [c - 0.5 for c in closes], "close": closes, "volume": [1000] * 40},
        index=index,
    )
    asset = Asset("TESTSTOCK", asset_type=Asset.AssetType.STOCK)

    class _IndicatorStrategy(Strategy):
        def initialize(self):
            self.sleeptime = "1D"
            self.vars.rows = []

        def on_trading_iteration(self):
            v2 = self.indicators.get("sma", asset, length=3)
            legacy = self.indicators.sma(asset, length=3)
            self.vars.rows.append((self.get_datetime(), v2, legacy))

    _, strategy = _IndicatorStrategy.run_backtest(
        datasource_class=PandasDataBacktesting,
        backtesting_start=datetime(2025, 1, 6),
        backtesting_end=datetime(2025, 1, 24),
        pandas_data={asset: Data(asset, frame, timestep="day")},
        benchmark_asset=None,
        analyze_backtest=False,
        show_plot=False,
        save_tearsheet=False,
        show_tearsheet=False,
        show_indicators=False,
        save_logfile=False,
        show_progress_bar=False,
        quiet_logs=True,
    )
    rows = strategy.vars.rows
    assert len(rows) >= 5
    for _, v2, legacy in rows:
        assert v2 is not None and v2["sma"] == pytest.approx(float(legacy))


def test_forming_bar_copy_handles_state_that_cannot_be_pickled():
    """Forming bars copy state with pickle for speed; unpicklable state still works."""
    from lumibot.indicators import Indicator, OutputSpec

    class LambdaState(Indicator):
        key = "lambda_state_test"
        outputs = [OutputSpec("x", "X")]

        def init_state(self):
            return {"fn": lambda v: v * 2, "count": 0}

        def step(self, state, bar):
            state["count"] += 1
            return {"x": state["fn"](bar.close) + state["count"]}

    ind = LambdaState()
    t0 = pd.Timestamp("2024-01-01", tz="UTC")
    assert ind.update({"t": t0, "o": 1, "h": 1, "l": 1, "c": 1}, closed=True)["x"] == 3
    forming = {"t": t0 + pd.Timedelta(days=1), "o": 2, "h": 2, "l": 2, "c": 2}
    assert ind.update(forming, closed=False)["x"] == 6
    assert ind.update(forming, closed=False)["x"] == 6  # state did not advance
    assert ind.update(forming, closed=True)["x"] == 6
