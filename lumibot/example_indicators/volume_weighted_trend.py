"""Volume-weighted trend: how far the volume-weighted average sits above or
below the plain average, in percent.

Positive means recent high-volume bars traded higher than the average bar
(buyers in control); negative means the opposite. Bars without volume are
skipped, never counted as zero.
"""

from collections import deque

from lumibot.indicators import Indicator, InputSpec, Level, OutputSpec, register


@register
class VolumeWeightedTrend(Indicator):
    key = "vw_trend"
    version = 1
    title = "Volume-Weighted Trend"
    short_title = "VWT"
    overlay = False

    inputs = [InputSpec("length", "int", 20, min=2, label="Length")]
    outputs = [OutputSpec("trend", "Trend %", "histogram", "#0EA5E9")]
    levels = [Level(0, "Zero", "solid")]

    def init_state(self):
        return {"window": deque(maxlen=self.inputs["length"])}

    def step(self, state, bar):
        if bar.volume is None:
            return {"trend": None}
        window = state["window"]
        window.append((bar.close, bar.volume))
        if len(window) < window.maxlen:
            return {"trend": None}
        volume = sum(v for _, v in window)
        sma = sum(c for c, _ in window) / len(window)
        if volume <= 0 or sma == 0:
            return {"trend": None}
        vwma = sum(c * v for c, v in window) / volume
        return {"trend": 100.0 * (vwma - sma) / sma}
