Build your first custom indicator
=================================

.. meta::
   :description: Write a custom LumiBot indicator in Python step by step: declare settings and outputs, keep a small state, and update it one bar at a time.

This page builds a **Donchian channel**: the highest high and lowest low of
the last *N* bars, and the line halfway between them.

1. Install LumiBot
------------------

::

    pip install lumibot

2. Write the class
------------------

.. literalinclude:: ../lumibot/example_indicators/donchian_midline.py
   :language: python
   :lines: 6-

What each part does:

``key`` and ``version``
    The name you use to find it, and the version of its math. If you change
    the formula later, raise ``version`` so saved charts and strategies that
    used the old numbers keep them.

``inputs``
    The settings a person can change (here, ``length``).

``outputs``
    What it draws: three lines.

``init_state()``
    The data that changes from bar to bar. Keep **all** of it here.

``step(state, bar)``
    Called once per bar. Update ``state`` and return this bar's values.
    Return ``None`` while there is not enough data yet. Never invent a number.

``@register``
    Adds it to LumiBot's registry so every part of LumiBot can find it by
    ``key``.

3. Run it on bars, then feed it live bars
-----------------------------------------

.. literalinclude:: ../lumibot/example_indicators/quickstart.py
   :language: python

``compute`` returns lists that line up with your bars. The first 19 values
are ``None`` because 20 bars are needed. ``update(bar, closed=False)`` gives a
live value for a forming bar without saving anything; ``closed=True`` saves
it. Run it with ``python -m lumibot.example_indicators.quickstart``.

4. Use it in a strategy
-----------------------

::

    from lumibot.example_indicators import donchian_midline  # registers "donchian_mid"

    def on_trading_iteration(self):
        dcm = self.indicators.get("donchian_mid", self.asset, length=20)
        if dcm and dcm["mid"] is not None and self.get_last_price(self.asset) > dcm["upper"]:
            ...

See :doc:`indicators_backtesting` and :doc:`indicators_live_strategies`.

Three rules that keep your numbers right
----------------------------------------

1. **All changing data goes in** ``state``. LumiBot copies it to try a forming
   bar, then throws the copy away. Data saved on ``self`` would leak live
   ticks into history. (Outside data you download is the exception; see
   :doc:`indicators_outside_data`.)
2. **Never look ahead.** ``step`` only sees the current bar. A value for bar
   *k* may only use bars up to *k*.
3. **Do not recompute history.** Keep running sums or a fixed window. Then
   every new bar costs the same small amount of time.

The full contract is on :doc:`building_indicators`.
