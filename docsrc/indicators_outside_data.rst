Using outside data in an indicator
==================================

.. meta::
   :description: Call a web API from a LumiBot indicator: a perpetual futures funding rate from a public exchange, with no look-ahead in backtests.

An indicator is plain Python, so it can download data from any web API: a
funding rate, an economic series, news scores, your own database. Pine Script
cannot call outside APIs; LumiBot indicators can.

The example below downloads the funding rate history of a perpetual swap from
OKX's public API (no key needed) and shows, for each bar, the funding rate
that was already settled at that bar's time.

.. literalinclude:: ../lumibot/example_indicators/funding_rate.py
   :language: python

Rules for outside data
----------------------

1. **Only use data that existed at the bar's time.** The example looks up the
   latest funding event at or before ``bar.time``. In a backtest this keeps
   the future out; live, it is simply the latest value.
2. **Cache downloads on the indicator, not in** ``state``. ``state`` is copied
   for every forming bar; a download does not need to be copied. Data you
   *compute from bars* still belongs in ``state``.
3. **Do not download on every bar.** Fetch history once, then refresh on a
   timer (``refresh_seconds`` in the example).
4. **Make the source swappable.** The example's ``loader`` can be replaced by
   another exchange or a recorded file, which is also how its tests run
   without the network.
5. **Bring your own keys** for APIs that need one, through environment
   variables. Never put a key in the indicator's code or settings.
6. **Check the data's terms.** Some providers allow personal use only.

Try it:

.. literalinclude:: ../lumibot/example_indicators/funding_rate_demo.py
   :language: python

LumiBot's tests run this indicator against a response in OKX's recorded shape
(offline) and once by hand against the live public endpoint.
