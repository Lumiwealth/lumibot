"""Trailing band: a stop line that flips sides with the trend.

Shows line breaks, fills, markers and candle colors on the price chart:

* two lines (``up_band`` below price in an up trend, ``down_band`` above price
  in a down trend). Each is ``None`` while the other side is active, so each
  line breaks instead of joining across the flip.
* a ``fill`` between the price middle and each band, half transparent
  (``#RRGGBB80``).
* ``shape`` markers with text where the trend flips.
* ``bar_color``: candles take the trend's color.

The band is the bar's middle price minus (or plus) a multiple of the average
true range, and it only moves in the trend's direction.
"""

from lumibot.indicators import Indicator, InputSpec, OutputSpec, register

UP_COLOR = "#00C853"
DOWN_COLOR = "#FF1744"


@register
class TrailingBand(Indicator):
    key = "trailing_band"
    version = 1
    title = "Trailing Band"
    short_title = "TBAND"
    overlay = True

    inputs = [
        InputSpec("length", "int", 10, min=1, label="ATR length"),
        InputSpec("mult", "float", 3.0, min=0.1, step=0.1, label="Multiplier"),
    ]
    outputs = [
        OutputSpec("up_band", "Up band", "line", UP_COLOR, line_width=2),
        OutputSpec("down_band", "Down band", "line", DOWN_COLOR, line_width=2),
        OutputSpec("middle", "Middle", "line", "#787B86", display="none"),
        OutputSpec("up_fill", "Up fill", "fill", UP_COLOR + "20", between=("middle", "up_band")),
        OutputSpec("down_fill", "Down fill", "fill", DOWN_COLOR + "20", between=("middle", "down_band")),
        OutputSpec("buy", "Buy", "shape", UP_COLOR, shape="label_up", location="below_bar", text="Buy",
                   text_color="#FFFFFF", size="small", display="pane"),
        OutputSpec("sell", "Sell", "shape", DOWN_COLOR, shape="label_down", location="above_bar", text="Sell",
                   text_color="#FFFFFF", size="small", display="pane"),
        OutputSpec("trend_color", "Candle color", "bar_color", palette=(UP_COLOR, DOWN_COLOR), display="pane"),
    ]

    def init_state(self):
        return {"atr": None, "prev_close": None, "up": None, "down": None, "trend": 1, "count": 0}

    def step(self, state, bar):
        length, mult = self.inputs["length"], self.inputs["mult"]
        prev_close = state["prev_close"]
        true_range = bar.high - bar.low
        if prev_close is not None:
            true_range = max(true_range, abs(bar.high - prev_close), abs(bar.low - prev_close))
        atr = state["atr"]
        atr = true_range if atr is None else atr + (true_range - atr) / length  # Wilder's average
        state["atr"] = atr
        state["count"] += 1
        middle = bar.hl2
        up, down = middle - mult * atr, middle + mult * atr
        if state["up"] is not None and prev_close is not None and prev_close > state["up"]:
            up = max(up, state["up"])
        if state["down"] is not None and prev_close is not None and prev_close < state["down"]:
            down = min(down, state["down"])
        trend = state["trend"]
        flipped = False
        if trend == -1 and state["down"] is not None and bar.close > state["down"]:
            trend, flipped = 1, True
        elif trend == 1 and state["up"] is not None and bar.close < state["up"]:
            trend, flipped = -1, True
        state.update(up=up, down=down, trend=trend, prev_close=bar.close)
        if state["count"] < length:
            return {}
        return {
            "up_band": up if trend == 1 else None,
            "down_band": down if trend == -1 else None,
            "middle": middle,
            "buy": 1 if flipped and trend == 1 else None,
            "sell": 1 if flipped and trend == -1 else None,
            "trend_color": 0 if trend == 1 else 1,
        }
