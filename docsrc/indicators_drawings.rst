:orphan:

Draw Boxes, Lines, Labels and Tables from an Indicator
======================================================

.. meta::
   :description: Let a LumiBot indicator draw boxes, lines, text labels and corner tables at bar times and prices, like Pine Script's box.new, line.new, label.new and table.new. Capped at 500 per kind.

Outputs on :doc:`indicators_styles` are numbers, one per bar. Some things an
indicator draws are not per-bar numbers: a box over a price zone that began
twenty bars ago, a line between two swing points, a text label on one bar, a
small table of values. These are *drawing objects*. They are the same things
Pine Script makes with ``box.new``, ``line.new``, ``label.new`` and
``table.new``, so a Pine indicator that uses them can be rebuilt in LumiBot.
You can also paste a Pine indicator into `BotSpot <https://botspot.trade>`_
and its builder converts it for you.

Inside ``step``, reach them through ``self.draw``::

    from lumibot.indicators import Indicator, OutputSpec

    class SwingZones(Indicator):
        key = "swing_zones"
        title = "Swing zones"
        overlay = True
        outputs = [OutputSpec("close", "Close", "line")]

        def init_state(self):
            return {"high": None, "box": None}

        def step(self, state, bar):
            if state["high"] is None or bar.high > state["high"]:
                state["high"] = bar.high
                state["box"] = self.draw.box(
                    bar.time, bar.high, bar.time, bar.low,
                    bg_color="#26A69A33", border_color="#26A69A", text="High",
                )
                self.draw.label(bar.time, bar.high, "H", style="label_down", color="#EF5350")
            elif state["box"] is not None:
                self.draw.update(state["box"], x2=bar.time)   # grow the box to this bar
            return {"close": bar.close}

Times are bar times (``bar.time``), prices are numbers, colors are
``#RRGGBB``, ``#RRGGBBAA`` (the last two digits are transparency) or the theme
colors ``chart.fg`` and ``chart.bg``.

What you can draw
-----------------

.. list-table::
   :header-rows: 1

   * - Call
     - Pine equivalent
     - Main options
   * - ``self.draw.box(x1, y1, x2, y2, ...)``
     - ``box.new``
     - ``bg_color``, ``border_color``, ``border_width``, ``border_style``, ``text``, ``extend``
   * - ``self.draw.line(x1, y1, x2, y2, ...)``
     - ``line.new``
     - ``color``, ``width``, ``style`` (solid, dashed, dotted), ``arrow``, ``extend``
   * - ``self.draw.label(x, y, text, ...)``
     - ``label.new``
     - ``style`` (every Pine label style, see below), ``color``, ``text_color``, ``size``, ``text_align``, ``tooltip`` (hover text)
   * - ``self.draw.polyline(points, ...)``
     - ``polyline.new``
     - ``points`` (2 to 500 ``(time, price)`` pairs), ``curved``, ``closed``, ``color``, ``width``, ``style``, ``fill_color``
   * - ``self.draw.linefill(line1, line2, color)``
     - ``linefill.new``
     - shades between two lines from ``self.draw.line``; removed with either line
   * - ``self.draw.table(position, columns, rows, ...)`` then ``self.draw.cell(table, column, row, text, ...)``
     - ``table.new``, ``table.cell``
     - ``position`` (nine screen positions), ``bg_color``, ``frame_color``, per-cell ``text_color`` and ``bg_color``

Label styles: ``label_up``, ``label_down``, ``label_left``, ``label_right``,
``label_center``, ``label_upper_left``, ``label_upper_right``,
``label_lower_left``, ``label_lower_right`` (a bubble pointing that way),
``none`` (text alone), ``text_outline`` (text with an outline in ``color``)
and the marks ``circle``, ``square``, ``diamond``, ``triangle_up``,
``triangle_down``, ``arrow_up``, ``arrow_down``, ``cross``, ``xcross`` and
``flag`` (a small mark at the point, with the text beside it).

TradingView drawing tools
-------------------------

The tools in TradingView's drawing toolbar are lines and labels with set
options. These shortcuts build them, so an indicator can draw what a person
would draw by hand. Each returns the id (or ids) it made, which work with
``update`` and ``delete``.

.. list-table::
   :header-rows: 1

   * - TradingView tool
     - Call
     - What it makes
   * - Trendline
     - ``self.draw.line(x1, y1, x2, y2)``
     - a line between two points
   * - Ray
     - ``self.draw.ray(x1, y1, x2, y2)``
     - from the first point through the second to the right edge
   * - Extended line
     - ``self.draw.extended_line(x1, y1, x2, y2)``
     - through both points to both edges
   * - Info line
     - ``self.draw.info_line(x1, y1, x2, y2)``
     - a line plus its price change and percent at the end
   * - Trend angle
     - ``self.draw.trend_angle(x1, y1, x2, y2)``
     - a line plus its angle at the start (a 1% rise reads 45 degrees on every screen)
   * - Horizontal line
     - ``self.draw.horizontal_line(price, x)``
     - a level across the whole chart
   * - Horizontal ray
     - ``self.draw.horizontal_ray(x, price)``
     - a level from bar x to the right edge
   * - Vertical line
     - ``self.draw.vertical_line(x)``
     - top to bottom at bar x
   * - Crossline
     - ``self.draw.crossline(x, price)``
     - a vertical and a horizontal line crossing at the point
   * - Text
     - ``self.draw.text(x, y, text)``
     - text with no bubble
   * - Callout
     - ``self.draw.callout(x, y, text)``
     - a bubble pointing at the point
   * - Note
     - ``self.draw.note(x, y, text, tooltip=...)``
     - a text box, with longer text on hover
   * - Pin
     - ``self.draw.pin(x, y, tooltip)``
     - a small dot that shows its text on hover
   * - Flag mark
     - ``self.draw.flag_mark(x, y)``
     - a flag at the point
   * - Price label
     - ``self.draw.price_label(x, price)``
     - a tag showing the price
   * - Price note
     - ``self.draw.price_note(x1, price, x2)``
     - a short level with the price written at its end
   * - Signpost
     - ``self.draw.signpost(x, y, text)``
     - a dotted post at bar x with a sign
   * - Table
     - ``self.draw.table(...) and self.draw.cell(...)``
     - a grid of text pinned to the chart

``lumibot/example_indicators/drawing_tour.py`` uses every one of these on
swing highs and lows. On `BotSpot <https://botspot.trade>`_ Spot Charts they
look the way TradingView draws them.

Change or remove an object later, even from a later bar::

    self.draw.update(drawing_id, x2=bar.time, text="Broken")   # Pine's set_* functions
    self.draw.delete(drawing_id)                                # Pine's delete

Updating or deleting an object that is already gone does nothing, like Pine.

Limits
------

- Each kind (box, line, label, line fill) keeps at most **500** objects, the
  same ceiling as TradingView. Adding one more drops the oldest of that kind.
  Polylines keep at most 100, each with up to 500 points. Tables are limited
  to nine, one per screen position.
- Text is plain text of at most 80 characters.
- A forming bar can create, change or delete objects while it builds. Only the
  closed bar keeps them, like indicator state.
- ``indicator.drawings()`` returns the current set as plain dictionaries, for
  tests and for chart servers.

A ``fill`` output may also shade against the price itself: put a price name
(``close``, ``ohlc4`` and the other sources) as one side of ``between``.

Next: :doc:`indicators_styles` for per-bar lines, shapes and fills, and
:doc:`indicators_tradingview_numbers` for matching TradingView values.
