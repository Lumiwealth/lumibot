SEC Form 4 Insider-Filing Agent
===============================

.. image:: ../docs/assets/ai-agent-workflows/ai-sec-insider-filings.png
   :alt: SEC Form 4 insider-filing AI trading team workflow
   :width: 100%

This example analyzes public SEC **Form 4** filings. It does not use material
non-public information. Records first become visible at the SEC
``acceptance``/publication time (normalized as ``published_at``), never merely
on their earlier transaction date.

The parser preserves transaction codes, direct versus indirect ownership,
derivative status, amendments, quantities, prices, and computed values. The
default strategy considers only open-market transactions; grants, gifts,
option exercises, derivatives, and amendments must not be treated as ordinary
open-market buying or selling.

``form4_researcher`` builds the evidence packet without trading permission.
``trading_risk_manager`` independently verifies account state and price and is
the only agent allowed to submit an order.

This example does not ship sample trades. Pass official EDGAR filings in
``transactions`` or a JSON file of those filings in ``transactions_path``.
Running the module with neither argument stops instead of inventing trades.
A backtest on this page is real only after those filings and market prices
are supplied.

.. literalinclude:: ../lumibot/example_strategies/ai_sec_insider_filings.py
   :language: python
   :linenos:
