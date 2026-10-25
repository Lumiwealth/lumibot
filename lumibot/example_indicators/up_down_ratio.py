"""Up/down ratio: an RSI-style oscillator from 0 to 100.

It is the share of recent price movement that went up:
``100 * gains / (gains + losses)`` over the last ``length`` bars, using plain
sums (Cutler's variant of RSI). It draws in its own pane with guide lines at
30 and 70.
"""

from collections import deque

from lumibot.indicators import Indicator, InputSpec, Level, OutputSpec, register


@register
class UpDownRatio(Indicator):
    key = "up_down_ratio"
    version = 1
    title = "Up/Down Ratio"
    short_title = "UDR"
    overlay = False  # its own pane under the price chart

    inputs = [
        InputSpec("length", "int", 14, min=1, label="Length"),
        InputSpec("source", "source", "close", label="Source"),
    ]
    outputs = [OutputSpec("ratio", "Ratio", "line", "#7C3AED")]
    levels = [Level(70, "High"), Level(30, "Low")]

    def init_state(self):
        return {"prev": None, "moves": deque(maxlen=self.inputs["length"]), "gains": 0.0, "losses": 0.0}

    def step(self, state, bar):
        price = bar.source(self.inputs["source"])
        prev, state["prev"] = state["prev"], price
        if prev is None:
            return {"ratio": None}
        moves = state["moves"]
        if len(moves) == moves.maxlen:  # drop the oldest move from the running sums
            old = moves[0]
            state["gains"] -= max(old, 0.0)
            state["losses"] -= max(-old, 0.0)
        change = price - prev
        moves.append(change)
        state["gains"] += max(change, 0.0)
        state["losses"] += max(-change, 0.0)
        if len(moves) < moves.maxlen:
            return {"ratio": None}
        total = state["gains"] + state["losses"]
        return {"ratio": 50.0 if total <= 0 else 100.0 * state["gains"] / total}
