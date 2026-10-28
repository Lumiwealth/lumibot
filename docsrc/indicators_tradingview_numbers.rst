Matching TradingView's numbers
==============================

.. meta::
   :description: LumiBot's built-in indicators follow TradingView Pine Script v6 formulas (seeding, Wilder smoothing, population standard deviation) so values line up with TradingView charts.

People moving from TradingView check one thing first: do the numbers match?
Libraries often disagree on RSI, ATR and long EMAs, mostly in how the first
values are started (seeded) and how much warm-up history there is. LumiBot's
built-ins follow the published Pine Script v6 formulas:

.. list-table::
   :header-rows: 1

   * - Indicator
     - How LumiBot computes it (Pine v6 rule)
   * - ``ema``
     - Starts with the simple average of the first ``length`` values, then
       ``alpha = 2 / (length + 1)``.
   * - ``rsi``
     - Gains and losses smoothed with Wilder's average (``rma``,
       ``alpha = 1 / length``), seeded with a simple average.
   * - ``atr``
     - True range smoothed with ``rma``.
   * - ``bbands``
     - Population standard deviation (divide by ``length``, not ``length - 1``).
   * - ``stoch``
     - Not clamped to 0..100 (same as Pine).
   * - ``vwap``
     - Restarts at each UTC day.

The test suite checks every built-in against plain-loop reference
implementations written from those formulas, on top of hand-worked values.

Why your numbers may still differ
---------------------------------

- **Warm-up.** EMA and RSI depend on every bar since the start. TradingView
  starts at the first bar it loaded; if you load fewer bars, early values
  differ. Load several times ``length`` bars and compare recent values.
- **Different data.** Exchanges and data vendors disagree slightly on
  candles, especially volume. Compare against the same exchange.
- **Session vs UTC.** LumiBot's ``vwap`` resets at UTC midnight. A stock
  chart that resets at the session open will differ.
- **The live bar.** TradingView redraws the live bar on every tick. Compare
  closed bars.

How to check a value yourself
-----------------------------

1. On TradingView, add the indicator to a chart and note its value on a few
   closed bars (or export the chart data, where your plan allows it).
2. Load the same exchange's candles into a DataFrame (for crypto,
   :doc:`indicators_live_crypto_stream` shows how).
3. Run ``registry.create("rsi", length=14).compute(df)`` and compare the same
   bars.

Porting your own Pine script
----------------------------

Map each Pine piece to Python. All of these live in
``lumibot.indicators.primitives`` and cost the same small time per bar:

.. list-table::
   :header-rows: 1

   * - Pine
     - LumiBot
   * - ``ta.sma``
     - ``RollingSMA``
   * - ``ta.ema``
     - ``SeededEMA``
   * - ``ta.rma``
     - ``SeededRMA``
   * - ``ta.stdev``
     - ``RollingStdev``
   * - ``ta.highest`` / ``ta.lowest``
     - ``RollingMax`` / ``RollingMin``
   * - ``var`` variables
     - keys in ``state``

Only port code you wrote yourself or have the right to use.
