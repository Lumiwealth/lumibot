"""Oscillator zones: an RSI drawn with a gradient, a shaded band and zone backgrounds.

Shows:

* a line whose color blends from red (0) to green (100) by its own value
  (``gradient``), at width 2;
* a ``fill`` between two levels (the 30 to 70 band), and levels with their
  own colors and line styles;
* ``background`` zones when the value is overbought or oversold;
* ``track_price``: a dotted line across the pane at the newest value;
* ``precision``: one decimal in the status line.
"""

from lumibot.indicators import Indicator, InputSpec, Level, OutputSpec, register


@register
class OscillatorZones(Indicator):
    key = "oscillator_zones"
    version = 1
    title = "Oscillator Zones"
    short_title = "OSCZ"
    overlay = False

    inputs = [
        InputSpec("length", "int", 14, min=2, label="Length"),
        InputSpec("upper", "float", 70, min=50, max=100, label="Upper"),
        InputSpec("lower", "float", 30, min=0, max=50, label="Lower"),
    ]
    outputs = [
        OutputSpec(
            "rsi",
            "RSI",
            "line",
            "#7E57C2",
            line_width=2,
            palette=("#FF5252", "#7E57C2", "#00E676"),
            color_by="rsi_shade",
            gradient=(0, 100),
            precision=1,
            track_price=True,
        ),
        OutputSpec("rsi_shade", "RSI shade", "line", display="none"),
        OutputSpec("band", "Band", "fill", "#7E57C21A", between=(70.0, 30.0)),
        OutputSpec("zone", "Zone", "background", palette=("#FF525226", "#00E67626"), display="pane"),
    ]

    @classmethod
    def levels_for(cls, inputs):
        return [
            Level(inputs["upper"], "Upper", "dashed", color="#787B86"),
            Level(50, "Middle", "dotted", color="#787B8680"),
            Level(inputs["lower"], "Lower", "dashed", color="#787B86"),
        ]

    def init_state(self):
        return {"prev": None, "gain": None, "loss": None, "count": 0}

    def step(self, state, bar):
        prev = state["prev"]
        state["prev"] = bar.close
        if prev is None:
            return {}
        n = self.inputs["length"]
        change = bar.close - prev
        gain, loss = max(change, 0.0), max(-change, 0.0)
        state["count"] += 1
        if state["gain"] is None:
            state["gain"], state["loss"] = gain, loss
        else:
            state["gain"] += (gain - state["gain"]) / n
            state["loss"] += (loss - state["loss"]) / n
        if state["count"] < n:
            return {}
        avg_gain, avg_loss = state["gain"], state["loss"]
        rsi = 100.0 if avg_loss == 0 else 100 - 100 / (1 + avg_gain / avg_loss)
        zone = 0 if rsi >= self.inputs["upper"] else 1 if rsi <= self.inputs["lower"] else None
        return {"rsi": rsi, "rsi_shade": rsi, "zone": zone}
