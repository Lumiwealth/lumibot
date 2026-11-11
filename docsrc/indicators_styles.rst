Indicator outputs and styles
===========================

.. meta::
   :description: How a LumiBot indicator sets its look on a chart: line widths and styles, opacity, colors per bar, fills, background zones, candle colors, markers, panes and levels.

An indicator returns **one number per output on each bar** (or ``None`` for
"nothing here"). Everything about how that number looks on a chart is declared
once, in the indicator's ``outputs`` and ``levels``. A chart reads it from
``describe()``.

This keeps the math simple and the data small: the per-bar values never carry
colors or text, so the same indicator works in a backtest, in a live strategy
and on any chart that understands these options.

.. contents:: On this page
   :local:
   :depth: 1

Quick reference
---------------

.. list-table::
   :header-rows: 1
   :widths: 22 78

   * - Option
     - What it does
   * - ``kind``
     - How the values draw: ``line``, ``step``, ``area``, ``histogram``,
       ``columns``, ``circles``, ``cross``, ``scatter``, ``markers``,
       ``shape``, ``fill``, ``background``, ``bar_color``, ``candle``.
   * - ``color``
     - ``#RRGGBB``, ``#RRGGBBAA`` (opacity in the last two digits) or a theme
       color, ``chart.fg`` or ``chart.bg``.
   * - ``line_width``
     - Pixels, above 0 and at most 10. For ``circles``, ``cross`` and
       ``scatter`` it is the mark size. Default 1.
   * - ``line_style``
     - ``solid`` (default), ``dashed`` or ``dotted``.
   * - ``palette``, ``color_by``, ``gradient``
     - A color per bar, picked by another output's value.
   * - ``offset``
     - Shift the drawing left (negative) or right (positive) by whole bars.
   * - ``display``
     - ``all`` (default), ``pane``, ``status_line``, ``data_window``, ``none``.
   * - ``precision``
     - Decimals shown for the value, 0 to 10.
   * - ``pane``
     - ``price`` or ``own``: overrides the indicator's ``overlay`` for this
       output.
   * - ``track_price``
     - A dotted line across the pane at the newest value.
   * - ``shape``, ``location``, ``size``, ``text``, ``text_color``
     - Markers for ``shape`` outputs.
   * - ``between``
     - The two outputs or levels a ``fill`` shades between.
   * - ``candle``
     - The open, high, low and close outputs a ``candle`` output draws.

Every option is checked when the class is defined (for a single output) or
when the indicator is created (for references between outputs). A typo raises
``ValueError`` that names the output and the allowed values.

Kinds
-----

``line``
    A line. Where the value is ``None`` the line breaks; it never joins across
    a gap.
``step``
    A line that moves in steps: flat until the next bar, then straight up or
    down.
``area``
    A line with the space under it filled in the line's color. Use a color
    with opacity (``#2962FF33``) for a light fill.
``histogram``
    Bars from zero, thin.
``columns``
    Bars from zero, full bar width.
``circles``, ``cross``
    A dot or a cross on each bar with a value. ``line_width`` sets the size.
``scatter``
    Dots, like ``circles``, kept for older indicators.
``markers``
    Reserved for marker-only outputs some charts read separately; prefer
    ``shape``.
``shape``
    A mark on every bar whose value is set and not 0. See `Markers`_.
``fill``
    Shades the space between two outputs or two levels. See `Fills`_.
``background``
    Colors the bar's whole column of its pane. See `Background zones`_.
``bar_color``
    Recolors the price candles. See `Candle colors`_.
``candle``
    Draws candles from four other outputs. See `Candles of your own`_.

Width and line style
--------------------

::

    outputs = [
        OutputSpec("fast", "Fast", "line", "#2962FF", line_width=2),
        OutputSpec("slow", "Slow", "line", "#FF6D00", line_width=1, line_style="dashed"),
        OutputSpec("signal", "Signal", "step", "#AB47BC", line_style="dotted"),
    ]

Colors and opacity
------------------

A color is ``#RRGGBB`` or ``#RRGGBBAA``. The last pair is opacity in hex:
``FF`` is solid, ``80`` is half, ``1A`` is about a tenth, ``00`` is invisible::

    OutputSpec("cloud", "Cloud", "area", "#26A69A33")     # a light green area

Theme colors
~~~~~~~~~~~~

``chart.fg`` is the chart's text color and ``chart.bg`` its background color.
They change with the chart's light or dark theme, so a line drawn in
``chart.fg`` is always readable::

    OutputSpec("basis", "Basis", "line", "chart.fg")

A person viewing the chart can still choose their own color, width and style
for each output in the chart's settings; their choice wins over the class.

A color per bar
---------------

Give the output a ``palette`` and name another output in ``color_by``. On each
bar, that output's value picks the color:

* without ``gradient``, the value is an index into the palette (0 is the first
  color, 1 the second...). Values are rounded and kept inside the palette.
* with ``gradient=(low, high)``, the value is a position: ``low`` gets the first
  color, ``high`` the last, and values in between are blended across the
  palette.
* ``None`` on a bar uses the output's own ``color``.

Hide the helper output with ``display="none"``. A four-color histogram::

    outputs = [
        OutputSpec("hist", "Histogram", "histogram", line_width=4,
                   palette=("#26A69A", "#B2DFDB", "#FF5252", "#FFCDD2"),
                   color_by="hist_color"),
        OutputSpec("hist_color", "Histogram color", "line", display="none"),
    ]

    def step(self, state, bar):
        ...
        return {"hist": value, "hist_color": 0 if rising_above_zero else 1}

A line colored by its own value::

    outputs = [
        OutputSpec("rsi", "RSI", "line", palette=("#FF5252", "#7E57C2", "#00E676"),
                   color_by="rsi_shade", gradient=(0, 100)),
        OutputSpec("rsi_shade", "RSI shade", "line", display="none"),
    ]
    # step(): return {"rsi": rsi, "rsi_shade": rsi}

Fills
-----

A ``fill`` output shades between two things. Each side is an output id or a
level value. Its color (and ``palette`` / ``color_by``) works like any other
output. ``step`` does not need to return a value for it.

Between two outputs::

    OutputSpec("cloud", "Cloud", "fill", "#2962FF26", between=("upper", "lower"))

Between two levels, like a shaded 30 to 70 band::

    OutputSpec("band", "Band", "fill", "#7E57C21A", between=(70.0, 30.0))

Where either side is ``None`` on a bar, that bar is not shaded.

Background zones
----------------

A ``background`` output colors the whole height of its pane on bars where its
value is set. With a ``palette``, the value picks the color; without one, any
value uses ``color``. An indicator that overlays the price colors the price
chart's background::

    OutputSpec("zone", "Zone", "background", palette=("#FF525226", "#00E67626"), display="pane")

    # step(): return {"zone": 0 if overbought else 1 if oversold else None}

Candle colors
-------------

A ``bar_color`` output recolors the price candles on bars where its value is
set, the same way as a background::

    OutputSpec("trend", "Trend color", "bar_color", palette=("#00C853", "#FF1744"), display="pane")

Markers
-------

A ``shape`` output puts a mark on each bar whose value is set and not 0.

``shape``
    ``circle``, ``square``, ``diamond``, ``cross``, ``xcross``,
    ``triangle_up``, ``triangle_down``, ``arrow_up``, ``arrow_down``,
    ``label_up``, ``label_down`` (a tag pointing at the bar, with the text
    inside it) and ``flag``.
``location``
    ``above_bar``, ``below_bar``, ``absolute`` (at the output's value),
    ``top`` or ``bottom`` of the pane.
``size``
    ``auto``, ``tiny``, ``small``, ``normal``, ``large``, ``huge``.
``text``, ``text_color``
    Up to 40 characters drawn with the mark. One character with a small shape
    works like a character marker::

        OutputSpec("buy", "Buy", "shape", "#00C853", shape="label_up",
                   location="below_bar", text="Buy", text_color="#FFFFFF", size="small")
        OutputSpec("pivot", "Pivot", "shape", "#FFB74D", shape="circle",
                   location="absolute", size="tiny")   # step() returns the pivot price

Candles of your own
-------------------

A ``candle`` output draws candles from four other outputs (usually hidden with
``display="none"``). ``palette`` and ``color_by`` color each candle::

    outputs = [
        OutputSpec("o", "Open", display="none"),
        OutputSpec("h", "High", display="none"),
        OutputSpec("l", "Low", display="none"),
        OutputSpec("c", "Close", display="none"),
        OutputSpec("dir", "Direction", display="none"),
        OutputSpec("candles", "Candles", "candle", candle=("o", "h", "l", "c"),
                   palette=("#26A69A", "#EF5350"), color_by="dir", pane="own"),
    ]

Panes
-----

The indicator's ``overlay`` decides where its outputs draw: ``True`` on the
price chart, ``False`` in a pane of its own under it. One output can go the
other way with ``pane``::

    class Trend(Indicator):
        overlay = False                      # oscillator in its own pane
        outputs = [
            OutputSpec("osc", "Oscillator"),
            OutputSpec("flip", "Flip", "shape", shape="triangle_up",
                       location="below_bar", pane="price"),   # marks on the candles
        ]

What shows where
----------------

``display`` controls where an output appears:

``all``
    Drawn, and its value is shown in the chart's status line.
``pane``
    Drawn, value not shown. Good for fills, zones and markers.
``status_line``
    Value shown, nothing drawn.
``data_window``
    Only in a chart's data window, where it has one.
``none``
    Not shown at all. Use it for helper outputs such as the one ``color_by``
    reads.

``precision`` sets how many decimals the value shows.

Shift and price line
--------------------

``offset`` moves an output by whole bars: ``offset=-5`` draws each value five
bars earlier, ``offset=5`` five bars later. A chart only draws the part that
falls on bars it has.

``track_price=True`` draws a dotted line across the pane at the output's
newest value, so the latest reading is easy to see.

Levels
------

A ``Level`` is a fixed horizontal guide line. ``style`` is ``solid``,
``dashed`` (default) or ``dotted``. ``color`` and ``line_width`` are optional;
without a color the chart uses its own::

    levels = [
        Level(70, "Upper", "dashed", color="#787B86"),
        Level(50, "Middle", "dotted", color="#787B8680"),
        Level(30, "Lower", "dashed", color="#787B86"),
    ]

Shade between two levels with a ``fill`` (see `Fills`_).

Complete examples
-----------------

These ship with LumiBot in ``lumibot/example_indicators/`` and run in the test
suite. Each is small and shows several options together.

``momentum_histogram.py``
    Four-color histogram (``palette`` + ``color_by``), cross marks on the zero
    line colored by a second helper, a dotted zero level.
``trailing_band.py``
    Two lines that break where the trend flips, half-transparent fills between
    the price middle and the band, ``Buy`` / ``Sell`` tag markers, and candle
    colors.
``oscillator_zones.py``
    A line colored by a gradient, a fill between two levels, background zones,
    ``track_price`` and ``precision``.
``smoothed_candles.py``
    Candles of its own in a separate pane (``candle`` + ``pane="own"``) and a
    dashed step line on the price chart.

Load one and look at its schema::

    from lumibot.example_indicators import oscillator_zones
    oscillator_zones.OscillatorZones.describe()

What is not an output
---------------------

Some charting tools also let a script create free-standing objects: lines
between two points, text labels, boxes, tables pinned to a corner of the chart,
or horizontal bars growing from the side of the chart (volume at each price).
These are not one number per bar, so they are not outputs. They are not part
of the indicator contract yet.
