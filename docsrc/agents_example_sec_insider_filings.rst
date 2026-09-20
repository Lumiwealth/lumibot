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
the only agent allowed to submit an order. The bundled data is a frozen test
fixture, not a live filing feed or performance claim.

Verified execution
------------------

A fresh September 20, 2026 Gemini run waited until the session after SEC
acceptance, excluded the synthetic grant/award, submitted ``bt_1``, and filled
18 AAPL shares at 275.59 in the backtest. This proves the point-in-time filter,
classification, agent handoff, order tool, and simulated fill path.

`Inspect the execution receipt <https://github.com/Lumiwealth/lumibot/blob/version/4.5.92/docs/research/2026-09-20_AGENT_STRATEGY_EXECUTION_PROOF.md>`_.

.. literalinclude:: ../lumibot/example_strategies/ai_sec_insider_filings.py
   :language: python
   :linenos:
