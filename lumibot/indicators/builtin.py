"""Built-in Indicator v2 implementations (TradingView Pine v6 semantics).

Every built-in keeps only rolling state, so ``update`` is O(1) per bar
(amortized). None of them recomputes or rescans history.
"""

from __future__ import annotations

from lumibot.indicators.base import Indicator, InputSpec, Level, OutputSpec
from lumibot.indicators.primitives import (
    Previous,
    RollingMax,
    RollingMin,
    RollingSMA,
    RollingStdev,
    SeededEMA,
    SeededRMA,
    safe_div,
)


def _length(name="length", default=14, label="Length"):
    return InputSpec(name, "int", default, min=1, max=5000, step=1, label=label)


def _source(default="close"):
    return InputSpec("source", "source", default, label="Source")


class SMAIndicator(Indicator):
    key = "sma"
    version = 1
    title = "Simple Moving Average"
    short_title = "SMA"
    overlay = True
    inputs = [_length(default=20), _source()]
    outputs = [OutputSpec("sma", "SMA", "line", "#2962FF")]

    def init_state(self):
        return RollingSMA(self.inputs["length"])

    def step(self, state, bar):
        return {"sma": state.update(bar.source(self.inputs["source"]))}


class EMAIndicator(Indicator):
    key = "ema"
    version = 1
    title = "Exponential Moving Average"
    short_title = "EMA"
    overlay = True
    inputs = [_length(default=20), _source()]
    outputs = [OutputSpec("ema", "EMA", "line", "#FF6D00")]

    def init_state(self):
        return SeededEMA(self.inputs["length"])

    def step(self, state, bar):
        return {"ema": state.update(bar.source(self.inputs["source"]))}


def _rsi_value(up, down):
    """Pine ``ta.rsi``: 100 when there are no losses, 0 when there are no gains."""
    if up is None or down is None:
        return None
    if down <= 1e-10:
        return 100.0
    if up <= 1e-10:
        return 0.0
    return 100.0 - 100.0 / (1.0 + up / down)


class RSIIndicator(Indicator):
    key = "rsi"
    version = 1
    title = "Relative Strength Index"
    short_title = "RSI"
    overlay = False
    inputs = [_length(default=14), _source()]
    outputs = [OutputSpec("rsi", "RSI", "line", "#7E57C2")]
    levels = [Level(70, "Overbought"), Level(50, "Middle", "dotted"), Level(30, "Oversold")]

    def init_state(self):
        n = self.inputs["length"]
        return {"prev": Previous(), "up": SeededRMA(n), "down": SeededRMA(n)}

    def step(self, state, bar):
        x = bar.source(self.inputs["source"])
        prev = state["prev"].update(x)
        if prev is None:
            return {"rsi": None}
        change = x - prev
        up = state["up"].update(max(change, 0.0))
        down = state["down"].update(max(-change, 0.0))
        return {"rsi": _rsi_value(up, down)}


class MACDIndicator(Indicator):
    key = "macd"
    version = 1
    title = "Moving Average Convergence Divergence"
    short_title = "MACD"
    overlay = False
    inputs = [
        _length("fast_length", 12, "Fast length"),
        _length("slow_length", 26, "Slow length"),
        _length("signal_length", 9, "Signal length"),
        _source(),
    ]
    outputs = [
        OutputSpec("macd", "MACD", "line", "#2962FF"),
        OutputSpec("signal", "Signal", "line", "#FF6D00"),
        OutputSpec("histogram", "Histogram", "histogram", "#26A69A"),
    ]
    levels = [Level(0, "Zero", "dotted")]

    def init_state(self):
        return {
            "fast": SeededEMA(self.inputs["fast_length"]),
            "slow": SeededEMA(self.inputs["slow_length"]),
            "signal": SeededEMA(self.inputs["signal_length"]),
        }

    def step(self, state, bar):
        x = bar.source(self.inputs["source"])
        fast, slow = state["fast"].update(x), state["slow"].update(x)
        macd = None if fast is None or slow is None else fast - slow
        signal = state["signal"].update(macd)
        histogram = None if macd is None or signal is None else macd - signal
        return {"macd": macd, "signal": signal, "histogram": histogram}


class BollingerBandsIndicator(Indicator):
    key = "bbands"
    version = 1
    title = "Bollinger Bands"
    short_title = "BB"
    overlay = True
    inputs = [
        _length(default=20),
        InputSpec("mult", "float", 2.0, min=0.001, max=50, step=0.1, label="StdDev multiplier"),
        _source(),
    ]
    outputs = [
        OutputSpec("basis", "Basis", "line", "#FF6D00"),
        OutputSpec("upper", "Upper", "line", "#2962FF"),
        OutputSpec("lower", "Lower", "line", "#2962FF"),
    ]

    def init_state(self):
        n = self.inputs["length"]
        return {"sma": RollingSMA(n), "stdev": RollingStdev(n)}

    def step(self, state, bar):
        x = bar.source(self.inputs["source"])
        basis, sd = state["sma"].update(x), state["stdev"].update(x)
        if basis is None or sd is None:
            return {"basis": basis, "upper": None, "lower": None}
        dev = self.inputs["mult"] * sd
        return {"basis": basis, "upper": basis + dev, "lower": basis - dev}


class ATRIndicator(Indicator):
    key = "atr"
    version = 1
    title = "Average True Range"
    short_title = "ATR"
    overlay = False
    inputs = [_length(default=14)]
    outputs = [OutputSpec("atr", "ATR", "line", "#B71C1C")]

    def init_state(self):
        return {"prev_close": Previous(), "rma": SeededRMA(self.inputs["length"])}

    def step(self, state, bar):
        prev = state["prev_close"].update(bar.close)
        # Pine ta.tr(true): the first bar has no previous close and uses high - low.
        if prev is None:
            tr = bar.high - bar.low
        else:
            tr = max(bar.high - bar.low, abs(bar.high - prev), abs(bar.low - prev))
        return {"atr": state["rma"].update(tr)}


class StochasticIndicator(Indicator):
    key = "stoch"
    version = 1
    title = "Stochastic"
    short_title = "Stoch"
    overlay = False
    inputs = [
        _length("period_k", 14, "%K length"),
        _length("smooth_k", 1, "%K smoothing"),
        _length("period_d", 3, "%D smoothing"),
    ]
    outputs = [
        OutputSpec("k", "%K", "line", "#2962FF"),
        OutputSpec("d", "%D", "line", "#FF6D00"),
    ]
    levels = [Level(80, "Upper"), Level(50, "Middle", "dotted"), Level(20, "Lower")]

    def init_state(self):
        n = self.inputs["period_k"]
        return {
            "hh": RollingMax(n),
            "ll": RollingMin(n),
            "k": RollingSMA(self.inputs["smooth_k"]),
            "d": RollingSMA(self.inputs["period_d"]),
        }

    def step(self, state, bar):
        hh, ll = state["hh"].update(bar.high), state["ll"].update(bar.low)
        # Pine ta.stoch is not clamped to 0..100; a flat range is na.
        raw = None if hh is None or ll is None else safe_div(100.0 * (bar.close - ll), hh - ll)
        k = state["k"].update(raw)
        return {"k": k, "d": state["d"].update(k)}


class VWAPIndicator(Indicator):
    key = "vwap"
    version = 1
    title = "Volume Weighted Average Price"
    short_title = "VWAP"
    overlay = True
    inputs = [_source("hlc3")]
    outputs = [OutputSpec("vwap", "VWAP", "line", "#2962FF")]

    def init_state(self):
        return {"day": None, "pv": 0.0, "volume": 0.0}

    def step(self, state, bar):
        if bar.time is None:
            raise ValueError("vwap needs a bar time to know when each UTC day starts")
        day = bar.time.date()
        if day != state["day"]:
            state.update(day=day, pv=0.0, volume=0.0)
        if bar.volume is not None:
            state["pv"] += bar.source(self.inputs["source"]) * bar.volume
            state["volume"] += bar.volume
        return {"vwap": safe_div(state["pv"], state["volume"])}


class WaveTrendIndicator(Indicator):
    """WaveTrend oscillator (the widely published public formula).

    ``ap`` = source (default ohlc4), ``esa = ema(ap, n1)``,
    ``d = ema(|ap - esa|, n1)``, ``ci = (ap - esa) / (0.015 * d)``,
    ``wave1 = ema(ci, n2)``, ``wave2 = sma(wave1, signal_length)``,
    ``diff = wave1 - wave2``. ``cross`` marks bars where wave1 crosses wave2
    (value = wave2 there) and ``cross_signal`` is +1 for an upward cross,
    -1 for a downward cross.
    """

    key = "wavetrend"
    version = 1
    title = "WaveTrend Oscillator"
    short_title = "WT"
    overlay = False
    inputs = [
        _length("channel_length", 10, "Channel length"),
        _length("average_length", 21, "Average length"),
        _length("signal_length", 3, "Signal length"),
        _source("ohlc4"),
        InputSpec("ob1", "float", 60.0, min=-200, max=200, step=1, label="Overbought 1"),
        InputSpec("ob2", "float", 53.0, min=-200, max=200, step=1, label="Overbought 2"),
        InputSpec("os1", "float", -60.0, min=-200, max=200, step=1, label="Oversold 1"),
        InputSpec("os2", "float", -53.0, min=-200, max=200, step=1, label="Oversold 2"),
    ]
    outputs = [
        OutputSpec("wave1", "Wave 1", "area", "#26A69A"),
        OutputSpec("wave2", "Wave 2", "area", "#2962FF"),
        OutputSpec("diff", "Wave 1 - Wave 2", "histogram", "#FFB300"),
        OutputSpec("cross", "Cross", "markers", "#FFFFFF"),
        OutputSpec("cross_signal", "Cross direction (+1 up, -1 down)", "markers", "#FFFFFF"),
    ]

    @classmethod
    def levels_for(cls, inputs):
        return [
            Level(0, "Zero", "dotted"),
            Level(inputs["ob1"], "OB1"),
            Level(inputs["ob2"], "OB2", "dotted"),
            Level(inputs["os1"], "OS1"),
            Level(inputs["os2"], "OS2", "dotted"),
            Level(75, "+75", "dotted"),
            Level(-75, "-75", "dotted"),
            Level(100, "100", "dotted"),
        ]

    def init_state(self):
        n1 = self.inputs["channel_length"]
        return {
            "esa": SeededEMA(n1),
            "d": SeededEMA(n1),
            "wave1": SeededEMA(self.inputs["average_length"]),
            "wave2": RollingSMA(self.inputs["signal_length"]),
            "prev": None,  # (wave1, wave2) of the previous bar
        }

    def step(self, state, bar):
        ap = bar.source(self.inputs["source"])
        esa = state["esa"].update(ap)
        d = state["d"].update(None if esa is None else abs(ap - esa))
        ci = None if esa is None or d is None else safe_div(ap - esa, 0.015 * d)
        wt1 = state["wave1"].update(ci)
        wt2 = state["wave2"].update(wt1)
        diff = None if wt1 is None or wt2 is None else wt1 - wt2
        cross = signal = None
        prev = state["prev"]
        if prev is not None and None not in prev and wt1 is not None and wt2 is not None:
            pa, pb = prev
            if wt1 > wt2 and pa <= pb:
                cross, signal = wt2, 1.0
            elif wt1 < wt2 and pa >= pb:
                cross, signal = wt2, -1.0
        state["prev"] = (wt1, wt2)
        return {"wave1": wt1, "wave2": wt2, "diff": diff, "cross": cross, "cross_signal": signal}


BUILTIN_INDICATORS = (
    SMAIndicator,
    EMAIndicator,
    RSIIndicator,
    MACDIndicator,
    BollingerBandsIndicator,
    ATRIndicator,
    StochasticIndicator,
    VWAPIndicator,
    WaveTrendIndicator,
)

__all__ = [cls.__name__ for cls in BUILTIN_INDICATORS] + ["BUILTIN_INDICATORS"]
