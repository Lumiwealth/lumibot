IBKR equity history accuracy
============================

Stock Trades history uses split-adjusted prices and separately accounts for cash dividends.
LumiBot verifies corporate-action evidence and keeps each historical cache tied to its
split basis. A new split requires a new price cache; existing files remain available for
older readers. Large market moves are preserved rather than guessed to be split errors.

A backtest cannot combine prices adjusted to different split dates. Unknown corporate
actions or missing required prices remain visible data failures, while a complete run
with no qualifying trades remains a valid result. Retrying must preserve the original
strategy, dates and provider settings.

The first read after an incompatible cache change downloads fresh history. Repeated
qualified historical reads reuse cached data. Brokerage feeds may differ in price,
trade filtering and settlement; independent comparisons must use matching semantics.

Developer details: `IBKR equity cache accuracy <https://github.com/Lumiwealth/lumibot/blob/dev/docs/IBKR_EQUITY_CACHE_ACCURACY.md>`_.
