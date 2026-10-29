Stream an indicator live from a crypto exchange
===============================================

.. meta::
   :description: Stream live indicator values from a public crypto exchange in plain Python with LumiBot and CCXT: native candles first, forming bars that update and lock on close.

You do not need a hosted service or an API key to get live indicator values.
LumiBot reads public candles from an exchange through
`CCXT <https://github.com/ccxt/ccxt>`_ and updates your indicators on every
poll.

Run it::

    python -m lumibot.example_indicators.live_crypto_stream kraken BTC/USD 15m

Output (one line per update; ``forming`` lines change until the candle
closes)::

    2026-10-25 20:15 forming close=67412.10 rsi=55.2 ema=67350.8
    2026-10-25 20:15 closed  close=67420.00 rsi=55.6 ema=67351.6

The code
--------

.. literalinclude:: ../lumibot/example_indicators/live_crypto_stream.py
   :language: python

Native candles first, built candles as a fallback
-------------------------------------------------

``choose_candle_source(timeframe, exchange.timeframes)`` returns:

- the exchange's own timeframe when it has one (``("15m", False)``), or
- ``("1m", True)`` when it does not, and LumiBot builds the candles from 1
  minute candles.

Native candles are the exchange's own numbers, so prefer them.

The candle builder
------------------

:py:class:`~lumibot.tools.candles.CandleBuilder` turns small candles into
bigger ones, one at a time:

- Buckets are cut on **UTC** boundaries, like exchanges do: 15 minute candles
  start at :00, :15, :30 and :45 UTC; daily candles at UTC midnight.
- The **forming** candle updates on every incoming bar, wicks included.
- When the exchange re-sends the current minute, it replaces the earlier copy
  instead of counting it twice.
- The candle **locks** when a bar from the next bucket arrives, when the
  bucket's last bar arrives closed, or when you call ``close_through(now)``
  after the bucket ends (quiet markets).
- Nothing is invented. An empty bucket makes no candle; missing volume stays
  missing.

For a DataFrame of 1 minute bars, ``build_candles(df, "15m")`` does the same
in one call, also on UTC boundaries.

::

    from lumibot.tools.candles import CandleBuilder

    builder = CandleBuilder("15m")                 # from 1 minute bars
    for candle in builder.update(minute_bar):      # minute_bar: dict or CCXT row
        values = rsi.update(candle, closed=candle.closed)

Which exchange?
---------------

Any CCXT exchange with public candles works. Some exchanges refuse
connections from some countries; if one fails, try another (Kraken, Coinbase
and OKX all publish 1 minute and 15 minute candles). LumiBot's tests run this
script against a fake exchange in CCXT's real data shape, and once by hand
against Kraken's public API.

.. currentmodule:: lumibot.tools.candles

.. autoclass:: CandleBuilder
   :members: update, close_through, forming, reset

.. autofunction:: choose_candle_source

.. autofunction:: build_candles
