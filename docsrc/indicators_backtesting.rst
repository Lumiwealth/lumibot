Using an indicator in a backtest
================================

.. meta::
   :description: Use built-in and custom LumiBot indicators inside a backtest with self.indicators.get, with no look-ahead and one update per new bar.

Inside a strategy, ``self.indicators.get(key, asset, **settings)`` returns the
indicator's values for the current bar as a dict::

    rsi = self.indicators.get("rsi", asset, length=14)          # {"rsi": 48.3}
    dcm = self.indicators.get("donchian_mid", asset, length=20) # {"upper": ..., "mid": ..., "lower": ...}

- Only bars at or before the strategy's current time are used. A backtest
  cannot see the future through an indicator.
- The indicator keeps its state between calls, so each new bar costs one
  update, not a full recompute.
- A value is ``None`` while the indicator is warming up. ``None`` overall
  means there are no bars yet.
- ``timestep="minute"`` (or ``"day"``, the default) picks the bar size.
- ``version=1`` pins the math so a later change cannot move your results.

A complete strategy
-------------------

This strategy uses two custom indicators from :doc:`indicators_examples`.
It is a teaching example, not a recommendation.

.. literalinclude:: ../lumibot/example_indicators/indicator_strategy.py
   :language: python

Run the backtest with free Yahoo data::

    python -m lumibot.example_indicators.indicator_strategy

LumiBot's test suite runs this exact class in a backtest on recorded-style
data and checks every indicator value against a direct calculation.

Tips
----

- Register your indicator before the backtest starts: import its module at
  the top of the strategy file.
- Outside data inside an indicator (an API) must respect the bar's time in a
  backtest. See :doc:`indicators_outside_data`.
- An indicator that calls an AI model may "remember" what happened after the
  bar from its training data. Backtests of those can look better than live
  results. Paper trade them before trusting a backtest.
