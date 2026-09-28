SEC Form 4 Insider-Filing Agent
===============================

.. meta::
   :description: This example reads public SEC Form 4 filings for a fixed watchlist. It uses no material non-public information.

.. image:: ../docs/assets/ai-agent-workflows/ai-sec-insider-filings.png
   :alt: SEC Form 4 insider-filing AI trading team workflow
   :width: 100%

This example reads public SEC **Form 4** filings for a fixed watchlist. It uses
no material non-public information. A filing becomes visible at its SEC
acceptance time, never on the earlier transaction date.

How it works:

1. ``insider_trade_researcher`` calls ``get_filings(symbol, form='4')`` for each
   watchlist ticker. It opens recent filings with ``get_filing_document``. It
   keeps open-market purchases (code ``P``) and discretionary open-market
   sales (code ``S``). It ignores grants, gifts, option exercises, tax
   withholding, 10b5-1 plan sales, and amendments.
2. ``bull`` and ``bear`` argue for and against the tilts.
3. ``interpreter`` assigns weights across the watchlist.
4. ``trading_risk_manager`` starts from equal weight. It tilts toward names
   with insider buying and trims names with discretionary selling. It keeps
   cash near 0% to 5%. It is the only agent allowed to submit orders.

Point-in-time safety: in a backtest, the SEC tools cap every ``as_of`` at the
backtest clock. An agent cannot see a filing accepted after that moment, even
if it passes a later date.

This example does not ship sample trades. Every trade comes from real SEC
filings the agents read at run time.

Parameters:

- ``watchlist``: the tickers the strategy may hold.
- ``lookback_days``: how many days of filings the researcher reads.

.. literalinclude:: ../lumibot/example_strategies/ai_sec_insider_filings.py
   :language: python
   :linenos:
