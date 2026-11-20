Supertrend indicator (built in)
===============================

.. meta::
   :description: The built-in LumiBot Supertrend indicator: ATR trend line with Buy and Sell markers where the trend flips, like TradingView. Settings, outputs and a working example.

``supertrend`` is a built-in LumiBot indicator. It follows TradingView's Pine
``ta.supertrend``: an ATR band that sits under price while the trend is up and
above price while the trend is down, and flips sides when price closes through it.

What it draws
-------------

- A green line under price while the trend is up (output ``uptrend``).
- A red line above price while the trend is down (output ``downtrend``).
- A **Buy** label under the bar where the trend turns up (output ``buy``).
- A **Sell** label above the bar where the trend turns down (output ``sell``).

The first bar with a full ATR starts in a down trend, as in Pine, so a short
window can show only the red line until the first flip.

Settings
--------

.. list-table::
   :header-rows: 1

   * - Setting
     - Default
     - What it does
   * - ``length``
     - 10
     - Number of bars in the ATR (Wilder smoothing).
   * - ``factor``
     - 3.0
     - ATR multiplier: how far the line sits from the middle of the bar.
   * - ``signals``
     - ``True``
     - Show the Buy and Sell labels. ``False`` hides them and keeps the lines.

Example
-------

::

    from lumibot.indicators import registry

    st = registry.create("supertrend", length=10, factor=3.0, signals=True)
    series = st.compute(bars)          # bars: list of dicts with t, o, h, l, c, v
    buys = [i for i, v in enumerate(series["buy"]) if v]
    sells = [i for i, v in enumerate(series["sell"]) if v]

``compute`` and the one-bar ``update`` give the same numbers, so the indicator works the
same in a backtest and live. See :doc:`indicators_backtesting` and :doc:`indicators_live_strategies`.

On BotSpot
----------

On `Spot Charts <https://botspot.trade/charts>`_ open **Indicators**, pick **Supertrend**, and
the lines and labels appear on the price chart. You can also ask Spot to add it.
