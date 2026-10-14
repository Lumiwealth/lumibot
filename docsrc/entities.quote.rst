Quote
=====

.. meta::
   :description: Keep option bid, ask, midpoint, last trade and their source times distinct with LumiBot Quote.

.. autoclass:: lumibot.entities.Quote

An option quote keeps the latest observed bid and ask separately from the last
trade. ``last_price`` is the actual last trade when the data source supplies it;
``mid_price`` is the bid/ask midpoint. A midpoint does not establish a last trade.
``trade_time`` dates the last trade; ``quote_time``, ``bid_time`` and ``ask_time``
date the supplied quotes. Missing values and source times remain unavailable.

Alpaca option quotes use snapshots containing the latest quote, trade and
available Greeks. Schwab and Tradier retain their returned last trade and source
times. A closed market does not turn the latest observations into current
executable prices. Data access and available fields depend on the provider.
