"""Smoothed candles: Heikin-Ashi candles drawn in their own pane.

Shows the ``candle`` kind: four helper outputs (open, high, low, close, each
``display="none"``) and one ``candle`` output that draws them, colored up or
down by a palette. Also shows ``pane="own"``: the candles get a pane of their
own even though the indicator overlays the price (``overlay = True``), while
a step line of the smoothed close stays on the price chart.
"""

from lumibot.indicators import Indicator, OutputSpec, register


@register
class SmoothedCandles(Indicator):
    key = "smoothed_candles"
    version = 1
    title = "Smoothed Candles"
    short_title = "SMC"
    overlay = True

    inputs = []
    outputs = [
        OutputSpec("ha_open", "Open", "line", display="none"),
        OutputSpec("ha_high", "High", "line", display="none"),
        OutputSpec("ha_low", "Low", "line", display="none"),
        OutputSpec("ha_close", "Close", "line", display="none"),
        OutputSpec("ha_direction", "Direction", "line", display="none"),
        OutputSpec(
            "candles",
            "Smoothed candles",
            "candle",
            "#26A69A",
            candle=("ha_open", "ha_high", "ha_low", "ha_close"),
            palette=("#26A69A", "#EF5350"),
            color_by="ha_direction",
            pane="own",
        ),
        OutputSpec("trail", "Smoothed close", "step", "#FFB74D", line_style="dashed"),
    ]

    def init_state(self):
        return {"open": None, "close": None}

    def step(self, state, bar):
        close = bar.ohlc4
        if state["open"] is None:
            open_ = (bar.open + bar.close) / 2
        else:
            open_ = (state["open"] + state["close"]) / 2
        state["open"], state["close"] = open_, close
        return {
            "ha_open": open_,
            "ha_high": max(bar.high, open_, close),
            "ha_low": min(bar.low, open_, close),
            "ha_close": close,
            "ha_direction": 0 if close >= open_ else 1,
            "trail": close,
        }
