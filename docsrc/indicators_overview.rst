What an indicator is
====================

.. meta::
   :description: LumiBot indicators are small Python classes that turn price bars, and any outside data, into numbers you can chart, backtest and trade with. Open source, free, no hosted service needed.

An **indicator** turns price bars into numbers. A moving average, RSI or a
funding-rate line are all indicators. In LumiBot an indicator is a small
Python class. The same class:

- draws on a chart, on the price or in its own pane,
- runs inside a backtest,
- runs inside a live strategy on your own broker, and
- streams live from a public crypto exchange in a plain Python script.

Because it is plain Python, an indicator can use numpy, pandas, a trained
machine-learning model, or data from any web API. It processes **one bar at a
time**, so a live chart pays for one update per bar instead of recomputing all
of history.

LumiBot is free and open source (GPL-3.0). Nothing on these pages needs an
account or a hosted service.

How it works in one picture
---------------------------

::

    bars (history, then live)          your indicator              you
    -------------------------   --->   update(bar, closed)   --->  values per bar
    open high low close volume          keeps a small state        {"rsi": 61.2}

* ``closed=True`` means the bar is finished. The indicator commits it.
* ``closed=False`` means the bar is still forming (a live tick). The
  indicator gives a value for it but throws its work away afterwards, so the
  forming bar never leaks into history. TradingView calls this "rolling back".

Two kinds of indicators
-----------------------

Built-in
    ``sma``, ``ema``, ``rsi``, ``macd``, ``bbands``, ``atr``, ``stoch`` and
    ``vwap``. Their math matches TradingView's Pine Script v6 (see
    :doc:`indicators_tradingview_numbers`).

Custom
    Your own class. It works everywhere a built-in works. Start with
    :doc:`indicators_first_custom`.

Where to go next
----------------

- :doc:`indicators_first_custom`: write one in five minutes.
- :doc:`indicators_settings_outputs`: settings people can change, and what it draws.
- :doc:`indicators_chart_placement`: on the price chart or its own pane.
- :doc:`indicators_backtesting` and :doc:`indicators_live_strategies`.
- :doc:`indicators_live_crypto_stream`: live values from an exchange in plain Python.
- :doc:`indicators_outside_data`: call a web API from an indicator.
- :doc:`indicators_examples`: five complete, tested examples.
- :doc:`indicators_comparison`: LumiBot next to TradingView, Vela, OpenMarket,
  NinjaTrader, MetaTrader and QuantConnect, plus an FAQ.
- Reference: :doc:`building_indicators` (the full contract) and
  :doc:`indicators` (the ``self.indicators`` accessor).
