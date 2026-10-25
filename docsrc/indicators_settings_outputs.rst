Indicator settings and outputs
==============================

.. meta::
   :description: Declare the settings (inputs), plotted outputs and guide lines of a LumiBot indicator, and how they are checked and described to charts and AI agents.

Every indicator says up front which **settings** it takes and what it
**draws**. Charts use this to build a settings form and a legend. AI agents
use it to know how to call the indicator.

Settings (inputs)
-----------------

::

    from lumibot.indicators import InputSpec

    inputs = [
        InputSpec("length", "int", 14, min=1, label="Length"),
        InputSpec("mult", "float", 2.0, min=0.1, step=0.1, label="Multiplier"),
        InputSpec("source", "source", "close", label="Source"),
        InputSpec("smooth", "bool", True),
        InputSpec("mode", "enum", "fast", options=("fast", "slow")),
        InputSpec("color", "color", "#2962FF"),
        InputSpec("symbol", "string", "BTC-USDT-SWAP"),
    ]

Types: ``int``, ``float``, ``bool``, ``color``, ``source``, ``string``,
``enum``. A ``source`` setting picks ``close``, ``open``, ``high``, ``low``,
``hl2``, ``hlc3``, ``ohlc4`` or ``hlcc4``; read it with
``bar.source(self.inputs["source"])``.

Settings are checked when the indicator is created. A wrong name, wrong type
or a value outside ``min``/``max`` raises ``InvalidIndicatorInputError`` with
the reason, instead of drawing wrong numbers later. For checks across two
settings, override ``validate_inputs(inputs)``.

Inside the indicator, ``self.inputs`` is a dict of the final values.

Outputs
-------

::

    from lumibot.indicators import OutputSpec

    outputs = [
        OutputSpec("macd", "MACD", "line", "#2962FF"),
        OutputSpec("hist", "Histogram", "histogram", "#26A69A"),
    ]

Kinds: ``line``, ``area``, ``histogram``, ``scatter``, ``markers``. ``step``
returns one value (or ``None``) per output ``id``. ``NaN`` and infinity are
turned into ``None``.

Guide lines (levels)
--------------------

::

    from lumibot.indicators import Level

    levels = [Level(70, "Overbought"), Level(30, "Oversold")]

When the lines depend on settings, override ``levels_for(inputs)``.

Describe
--------

``YourIndicator.describe()`` returns all of the above as JSON-safe data. The
registry returns it for every indicator::

    from lumibot.indicators import registry
    registry.list()        # one describe() per indicator

Example output (trimmed)::

    {"key": "up_down_ratio", "version": 1, "overlay": false,
     "inputs": [{"name": "length", "type": "int", "default": 14, "min": 1, ...}],
     "outputs": [{"id": "ratio", "label": "Ratio", "kind": "line", "color": "#7C3AED"}],
     "levels": [{"value": 70, "label": "High", "style": "dashed"}, ...]}
