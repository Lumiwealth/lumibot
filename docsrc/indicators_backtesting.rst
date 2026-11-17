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

Built-in indicators by name, and backtest speed
-----------------------------------------------

The short form ``self.indicators.<name>(asset, ...)`` runs any pandas-ta-classic
indicator on the bars seen so far::

    trend = self.indicators.supertrend(asset, length=10, multiplier=3.0, timestep="5 minutes")
    fast = self.indicators.ema(asset, length=21, timestep="5 minutes")

Calling it once per bar is fine in a long backtest. For the indicators listed
below LumiBot calculates the indicator once over the stored bar series and reads
the row of the current bar, instead of recalculating the whole history on every
bar. The row of a bar depends only on that bar and earlier ones, so the value is
the same number the bar-by-bar calculation gives and nothing after the current bar
can reach it; the test suite compares both paths bar by bar. A three month
backtest on 5 minute bars that reads Supertrend on every bar went from about four
minutes of simulation to about one.

- Always (the bars seen so far start at the first stored bar): ``supertrend``,
  ``ema``, ``rsi``, ``atr``, ``macd``, ``adx``, ``bbands``, ``obv``, ``cci``,
  ``willr``, ``roc``, ``mom``, ``wma``, ``donchian``, ``psar``, ``hma``, ``dema``,
  ``tema``, ``mfi``, ``rma``, ``aroon``, ``cmf``, ``kama``, ``vwma``, ``stdev``,
  ``zscore``.
- Also when the data source hands over only the most recent 10,000 bars:
  ``supertrend``, ``ema``, ``rsi``, ``atr``, ``macd``, ``adx``, ``rma``, ``dema``,
  ``tema``, ``willr``, ``roc``, ``mom``, ``wma``, ``hma``, ``donchian``,
  ``aroon``, ``psar``, ``kama``. Running totals and rolling sums (``obv``,
  ``bbands``, ``cci`` and others) keep the bar-by-bar calculation there, because
  their last digits depend on where the calculation started.

Limits: every other indicator, custom functions, the first 500 bars, calls with
``offset`` or fill options, and live trading are calculated bar by bar as before.

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
