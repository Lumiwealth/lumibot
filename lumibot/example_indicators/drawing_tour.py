"""Drawing tour: one indicator that uses every drawing object, as a working reference.

It watches swing highs and lows (the highest high or lowest low of the last
``length`` bars) and marks them with the same tools TradingView has in its
drawing toolbar: trend lines, rays, extended and horizontal lines, a vertical
line, a crossline, a trend angle, an info line, a channel shaded with a line
fill, a range box, a zigzag polyline, labels in several styles (text, callout, note, pin,
flag, price label, price note, signpost) and a small table.

Every one of these is a plain ``self.draw`` call, so the same code works in a
backtest, a live chart and on `BotSpot <https://botspot.trade>`_ Spot Charts.
It is a teaching example, not trading advice.
"""

from collections import deque

from lumibot.indicators import Indicator, InputSpec, OutputSpec, register


@register
class DrawingTour(Indicator):
    key = "drawing_tour"
    version = 1
    title = "Drawing Tour"
    short_title = "Draw"
    overlay = True

    inputs = [InputSpec("length", "int", 10, min=2, max=200, label="Swing length")]
    outputs = [OutputSpec("close", "Close", "line", "#787B86", display="none")]

    def init_state(self):
        n = self.inputs["length"]
        return {"bars": deque(maxlen=n), "swings": [], "zigzag": None, "table": None, "count": 0}

    def step(self, state, bar):
        draw = self.draw
        window = state["bars"]
        window.append(bar)
        if len(window) < window.maxlen:
            return {"close": bar.close}
        if state["table"] is None:
            state["table"] = draw.table("top_right", columns=2, rows=2, bg_color="#131722CC")
            draw.cell(state["table"], 0, 0, "Swings", text_color="#D1D4DC")
        high = max(window, key=lambda b: b.high)
        low = min(window, key=lambda b: b.low)
        swing = None
        if high is bar:
            swing = ("high", bar.time, bar.high)
        elif low is bar:
            swing = ("low", bar.time, bar.low)
        if swing is not None and (not state["swings"] or state["swings"][-1][0] != swing[0]):
            state["swings"].append(swing)
            state["count"] += 1
            self._mark(state, swing)
            draw.cell(state["table"], 1, 0, f"{state['count']:,}", text_color="#D1D4DC")
            draw.cell(state["table"], 0, 1, "Last", text_color="#D1D4DC")
            draw.cell(state["table"], 1, 1, f"{swing[2]:,.2f}", text_color="#22AB94" if swing[0] == "high" else "#F23645")
        return {"close": bar.close}

    def _mark(self, state, swing):
        draw = self.draw
        kind, time, price = swing
        swings = state["swings"]
        color = "#22AB94" if kind == "high" else "#F23645"
        if kind == "high":
            draw.callout(time, price, "High", color=color)
        else:
            draw.label(time, price, "Low", style="label_up", color=color)
        draw.pin(time, price, f"{kind} at {price:,.2f}", color=color)
        points = [(t, p) for _, t, p in swings[-20:]]
        if len(points) >= 2:
            if state["zigzag"] is None:
                state["zigzag"] = draw.polyline(points, color="#F59E0B", width=2)
            else:
                draw.update(state["zigzag"], points=points)
        if len(swings) < 4:
            return
        (_, t1, p1), (_, t2, p2), (_, t3, p3), (_, t4, p4) = swings[-4:]
        # Every fourth swing shows the rest of the toolbox once, so the chart stays readable.
        if state["count"] % 4:
            return
        up = draw.line(t1, p1, t3, p3, color="#2962FF")
        down = draw.line(t2, p2, t4, p4, color="#2962FF")
        draw.linefill(up, down, color="#2962FF1A")
        draw.box(t3, max(p3, p4), t4, min(p3, p4), border_color="#787B86", bg_color="#787B861A", text="Range",
                 text_color="#787B86")
        draw.ray(t3, p3, t4, p4, color="#9C27B0", style="dashed")
        draw.extended_line(t1, p1, t2, p2, color="#787B8680", style="dotted")
        draw.horizontal_ray(time, price, color=color, style="dashed")
        draw.trend_angle(t3, p3, t4, p4, color="#00BCD4")
        draw.info_line(t2, p2, t4, p4, color="#FF9800")
        draw.price_note(t3, p3, t4)
        draw.flag_mark(t4, p4, color=color)
        draw.text(t1, p1, "start", color="#787B86")
        draw.note(t2, p2, "Swing", tooltip="A swing is the highest high or lowest low of the window.")
        draw.price_label(t4, p4, color=color)
        if state["count"] % 8 == 0:
            draw.vertical_line(time, color="#787B86", style="dotted")
            draw.crossline(t3, p3, color="#787B8680")
            draw.signpost(t4, p4, "Turn")
            draw.horizontal_line(p4, t4, color="#787B8640")
