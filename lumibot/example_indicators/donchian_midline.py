"""Donchian channel midline: the middle of the highest high and lowest low.

Draws on the price chart (``overlay = True``).
"""

from collections import deque

from lumibot.indicators import Indicator, InputSpec, OutputSpec, register


@register
class DonchianMidline(Indicator):
    key = "donchian_mid"
    version = 1
    title = "Donchian Channel Midline"
    short_title = "DCM"
    overlay = True  # same pane as the price

    inputs = [InputSpec("length", "int", 20, min=1, label="Length")]
    outputs = [
        OutputSpec("upper", "Upper", "line", "#22C55E"),
        OutputSpec("mid", "Midline", "line", "#F59E0B"),
        OutputSpec("lower", "Lower", "line", "#EF4444"),
    ]

    def init_state(self):
        n = self.inputs["length"]
        return {"highs": deque(maxlen=n), "lows": deque(maxlen=n)}

    def step(self, state, bar):
        state["highs"].append(bar.high)
        state["lows"].append(bar.low)
        if len(state["highs"]) < self.inputs["length"]:
            return {"upper": None, "mid": None, "lower": None}
        upper, lower = max(state["highs"]), min(state["lows"])
        return {"upper": upper, "mid": (upper + lower) / 2, "lower": lower}
