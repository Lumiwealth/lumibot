Congressional Disclosure Agent
===============================

.. image:: ../docs/assets/ai-agent-workflows/ai-congress-disclosures.png
   :alt: Congressional disclosure AI trading team workflow
   :width: 100%

This example turns a public congressional financial disclosure into a
point-in-time research packet, then gives that packet to a dedicated trading
and risk agent. It is a disclosure-following example, not a claim that the
member traded on the publication date or that copying the trade is profitable.

Availability
------------

The source ``TransactionDate`` describes when the reported transaction
occurred. ``ReportDate`` (normalized as ``published_at``) is when the example
first makes the record visible. Federal disclosure rules may permit a report
as late as **45 days** after the transaction, so this is not a low-latency
signal and the example must never backdate availability to ``TransactionDate``.

House and Senate periodic transaction reports are public filings. Official
instructions say an option row should name the underlying security, put or
call, strike, and expiration. Real filings are PDFs, and some rows leave the
contract fields incomplete. A stock example can use the ticker and buy or
sell. An option example must skip any row that lacks the underlying, put or
call, strike, and expiration.

This example does not ship sample trades. Pass parsed official filings in
``disclosures`` or a JSON file of those filings in ``disclosures_path``.
Running the module with neither argument stops instead of inventing a
portfolio. A backtest on this page is real only after those filings and
market prices are supplied. Amounts on the filings are ranges, not exact
share counts.

Architecture
------------

``disclosure_researcher`` cannot trade. ``trading_risk_manager`` is the only
agent with trading tools and caps a new position at the configured percentage.
Already processed disclosure IDs are ignored and old records are rejected by
the configured age limit. Records stay hidden until ``ReportDate``.

.. literalinclude:: ../lumibot/example_strategies/ai_congress_disclosures.py
   :language: python
   :linenos:
