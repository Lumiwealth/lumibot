self.get\_quotes
=================================================================

.. autofunction:: lumibot.strategies.strategy.Strategy.get_quotes
   :noindex:

Use the same selected assets as the original strategy. A batch returns a mapping
from each exact ``Asset`` to its ``Quote`` or ``None`` when unavailable::

    quotes = self.get_quotes(selected_assets)
    for asset in selected_assets:
        quote = quotes.get(asset)
        if quote is None:
            continue
        # Apply the existing bid/ask, freshness and trading rules to this quote.

Schwab uses requests of at most 50 exact symbols, including option contracts.
Other providers retain their existing single-quote behavior unless they implement
a native bulk adapter. Option assets honor the configured option data source.
Provider failures raise instead of triggering hidden per-contract retries.
Missing prices remain missing; a last trade is not substituted for bid/ask.
The method does not cache across scans or authorize any order.
