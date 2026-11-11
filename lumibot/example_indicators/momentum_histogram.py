"""Momentum histogram with four colors and a state dot on the zero line.

Shows per-bar colors: a histogram whose color on each bar comes from a palette,
picked by a helper output that is never drawn (``display="none"``), and a row
of cross marks on the zero line colored by a second helper.

Momentum here is the close minus its simple moving average. Color index:
0 rising above zero, 1 falling above zero, 2 falling below zero, 3 rising
below zero. Draws in its own pane (``overlay = False``).
"""

from collections import deque

from lumibot.indicators import Indicator, InputSpec, Level, OutputSpec, register

RISING_UP, FALLING_UP, FALLING_DOWN, RISING_DOWN = 0, 1, 2, 3


@register
class MomentumHistogram(Indicator):
    key = "momentum_histogram"
    version = 1
    title = "Momentum Histogram"
    short_title = "MOMH"
    overlay = False

    inputs = [
        InputSpec("length", "int", 20, min=2, label="Length"),
        InputSpec("calm", "float", 0.5, min=0, step=0.1, label="Calm range (%)"),
    ]
    outputs = [
        OutputSpec(
            "momentum",
            "Momentum",
            "histogram",
            "#26A69A",
            line_width=4,
            palette=("#26A69A", "#B2DFDB", "#FF5252", "#FFCDD2"),
            color_by="momentum_color",
        ),
        OutputSpec("momentum_color", "Momentum color", "line", display="none"),
        OutputSpec(
            "state",
            "Calm",
            "cross",
            "#2962FF",
            line_width=2,
            palette=("#2962FF", "#787B86"),
            color_by="state_color",
            display="pane",
        ),
        OutputSpec("state_color", "Calm color", "line", display="none"),
    ]
    levels = [Level(0, "Zero", "dotted", color="#787B8680")]

    def init_state(self):
        return {"closes": deque(maxlen=self.inputs["length"]), "previous": None}

    def step(self, state, bar):
        closes = state["closes"]
        closes.append(bar.close)
        if len(closes) < closes.maxlen:
            return {}
        average = sum(closes) / len(closes)
        momentum = bar.close - average
        previous = state["previous"]
        state["previous"] = momentum
        rising = previous is None or momentum >= previous
        if momentum >= 0:
            color = RISING_UP if rising else FALLING_UP
        else:
            color = RISING_DOWN if rising else FALLING_DOWN
        calm = abs(momentum) / average * 100 <= self.inputs["calm"]
        return {
            "momentum": momentum,
            "momentum_color": color,
            "state": 0.0,
            "state_color": 1 if calm else 0,
        }
