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
     - ``style`` (``label_up``, ``label_down``, ``label_left``, ``label_right``, ``label_center``, ``none``), ``color``, ``text_color``, ``size``
   * - ``self.draw.table(position, columns, rows, ...)`` then ``self.draw.cell(table, column, row, text, ...)``
     - ``table.new``, ``table.cell``
     - ``position`` (nine screen positions), ``bg_color``, ``frame_color``, per-cell ``text_color`` and ``bg_color``

Change or remove an object later, even from a later bar::

    self.draw.update(drawing_id, x2=bar.time, text="Broken")   # Pine's set_* functions
    self.draw.delete(drawing_id)                                # Pine's delete

Updating or deleting an object that is already gone does nothing, like Pine.

Limits
------

- Each kind (box, line, label) keeps at most **500** objects, the same ceiling
  as TradingView. Adding one more drops the oldest of that kind. Tables are
  limited to nine, one per screen position.
- Text is plain text of at most 80 characters.
- A forming bar can create, change or delete objects while it builds. Only the
  closed bar keeps them, like indicator state.
- ``indicator.drawings()`` returns the current set as plain dictionaries, for
  tests and for chart servers.

Next: :doc:`indicators_styles` for per-bar lines, shapes and fills, and
:doc:`indicators_tradingview_numbers` for matching TradingView values.
