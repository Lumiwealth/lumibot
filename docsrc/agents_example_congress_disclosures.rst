Congressional Disclosure Agent
===============================

.. image:: ../docs/assets/ai-agent-workflows/ai-congress-disclosures.png
   :alt: Congressional disclosure AI trading team workflow
   :width: 100%

This example turns a public congressional financial disclosure into a
point-in-time research packet, then gives that packet to a dedicated trading
and risk agent. It is a disclosure-following example, not a claim that the
member traded on the publication date or that copying the trade is profitable.

Availability and licensing
--------------------------

The source ``TransactionDate`` describes when the reported transaction
occurred. ``ReportDate`` (normalized as ``published_at``) is when the example
first makes the record visible. Federal disclosure rules may permit a report
as late as **45 days** after the transaction, so this is not a low-latency
signal and the example must never backdate availability to ``TransactionDate``.

The repository includes a small frozen fixture only for deterministic testing.
Use an authorized, licensed data source and follow its redistribution terms
before operating or publishing a live/commercial Congress strategy.

Architecture
------------

``disclosure_researcher`` cannot trade. ``trading_risk_manager`` is the only
agent with trading tools and caps a new position at the configured percentage.
Already processed disclosure IDs are ignored and old records are rejected by
the configured age limit.

.. literalinclude:: ../lumibot/example_strategies/ai_congress_disclosures.py
   :language: python
   :linenos:
