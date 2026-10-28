:seo_title: Custom Trading Indicators in Python: Live Updates, Backtests, Outside Data and ML

Custom indicators in Python
===========================

.. meta::
   :description: LumiBot custom indicators are Python classes that update live bar by bar, run in backtests and live strategies on your broker, and can use outside web data and ML. Free, open source.

**A LumiBot custom indicator is a small Python class that turns price bars,
and any outside data, into numbers you can chart, backtest and trade with.**
The same class updates live one bar at a time, runs inside a backtest, runs
inside a live strategy on your own broker, and can use numpy, pandas, a
trained machine-learning model or any web API. LumiBot is free and open
source (GPL-3.0) and runs on your own computer with ``pip install lumibot``.

This page answers the main questions in plain words. Each section links to a
page with the full detail and tested code.

What a custom indicator can do
------------------------------

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Capability
     - In plain words
   * - Live updates
     - Updates one bar at a time. A still-forming bar gets a value, then that work is thrown away so it never leaks into history. More: :doc:`indicators_live_crypto_stream`.
   * - Backtests
     - The same class runs in a LumiBot backtest and only sees bars that existed at each moment. More: :doc:`indicators_backtesting`.
   * - Live strategies
     - The same class runs in a live or paper strategy on any broker LumiBot supports. More: :doc:`indicators_live_strategies`.
   * - Streaming without a paid feed
     - For crypto, a plain Python script can stream public exchange candles through CCXT with no API key. More: :doc:`indicators_live_crypto_stream`.
   * - Outside data
     - An indicator can call any web API (for example a funding rate), using only data that existed at each bar's time. More: :doc:`indicators_outside_data`.
   * - Machine learning
     - Plain Python, so numpy, scikit-learn or a model file you trained all work. More: :doc:`indicators_examples`.
   * - Settings and outputs
     - Declares settings people can change, the lines it draws, colors and guide lines. More: :doc:`indicators_settings_outputs`.
   * - Chart placement
     - Draws on the price chart or in its own pane. More: :doc:`indicators_chart_placement`.
   * - TradingView numbers
     - Built-in ``sma``, ``ema``, ``rsi``, ``macd``, ``bbands``, ``atr``, ``stoch`` and ``vwap`` follow Pine Script v6 formulas. More: :doc:`indicators_tradingview_numbers`.

How an indicator works in one picture
-------------------------------------

::

    bars (history, then live)          your indicator              you
    -------------------------   --->   update(bar, closed)   --->  values per bar
    open high low close volume          keeps a small state        {"rsi": 61.2}

* ``closed=True`` means the bar is finished. The indicator commits it.
* ``closed=False`` means the bar is still forming (a live tick). The
  indicator gives a value for it but throws its work away afterwards, so the
  forming bar never leaks into history. TradingView calls this "rolling back".

Because each new bar costs one update, a live chart does not recompute all of
history on every tick. To write one, start with
:doc:`indicators_first_custom` (about five minutes).

How LumiBot compares, in one table
----------------------------------

A summary of what an indicator author can do in each tool. The full tables,
plain-words notes and every source are on :doc:`indicators_comparison`
(checked October 25, 2026).

.. list-table::
   :header-rows: 1
   :widths: 22 20 30 28

   * - Product
     - Language
     - Outside web data
     - Same code places live broker orders
   * - **LumiBot**
     - Python
     - Yes, inside the indicator
     - Yes, inside a strategy on your broker
   * - **TradingView**
     - Pine Script
     - No
     - Through alerts and webhooks to other services
   * - **QuantConnect LEAN**
     - Python or C#
     - Yes, in the algorithm (limits apply in their cloud)
     - Yes, LEAN algorithms
   * - **NinjaTrader 8**
     - C# (NinjaScript)
     - Possible with .NET classes, not supported by NinjaTrader
     - Yes, NinjaScript strategies
   * - **MetaTrader 5**
     - MQL5
     - No; web requests are blocked in indicators
     - Yes, Expert Advisors
   * - **LuxAlgo Vela + PineTS**
     - Pine run in TypeScript
     - No; the chart feeds the data
     - No (paper trading)
   * - **OpenMarket (wrun)**
     - TypeScript to WebAssembly
     - Built-in feeds only
     - No

Frequently asked questions
--------------------------

What is a custom indicator in LumiBot?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A Python class that reads price bars one at a time and returns values, such
as an RSI line or a channel. It declares its settings and outputs, so a chart
knows how to draw it, and it works in backtests and live strategies without
changes.

Is LumiBot free?
~~~~~~~~~~~~~~~~

Yes. LumiBot is free and open source under GPL-3.0. Install it with
``pip install lumibot``. Nothing on these pages needs an account or a hosted
service.

Can an indicator use machine learning?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes. It is plain Python, so numpy, scikit-learn, LightGBM or a model file you
trained all work. :doc:`indicators_examples` includes a small numpy
classifier.

Can an indicator use data from outside the chart?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes. It can call any web API. :doc:`indicators_outside_data` shows a funding
rate from a public exchange, done without look-ahead in backtests.

Can I get live indicator values without a paid data service?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For crypto, yes. :doc:`indicators_live_crypto_stream` streams public candles
from an exchange through CCXT with no API key. Stocks need a data source or
broker that provides them.

Will my indicator repaint?
~~~~~~~~~~~~~~~~~~~~~~~~~~

Closed-bar values never change. A forming bar's value can change until the
bar closes, which is why LumiBot labels it ``closed=False`` and never saves
it into history.

Does an indicator make money?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No tool can promise that. Indicators describe the past. Test any idea in a
backtest and on a paper account first.

All indicator pages
-------------------

- :doc:`indicators_first_custom`: write your first indicator.
- :doc:`indicators_settings_outputs`: settings people can change, and what it draws.
- :doc:`indicators_chart_placement`: on the price chart or its own pane.
- :doc:`indicators_backtesting`: use an indicator in a backtest.
- :doc:`indicators_live_strategies`: use it in a live strategy on your broker.
- :doc:`indicators_live_crypto_stream`: live values from an exchange in plain Python.
- :doc:`indicators_outside_data`: call a web API from an indicator.
- :doc:`indicators_tradingview_numbers`: match TradingView's numbers and convert Pine.
- :doc:`indicators_examples`: five complete, tested examples.
- :doc:`indicators_comparison`: full comparison with TradingView, Vela,
  OpenMarket, NinjaTrader, MetaTrader and QuantConnect.
- Reference: :doc:`building_indicators` (the full contract) and
  :doc:`indicators` (the ``self.indicators`` accessor).
