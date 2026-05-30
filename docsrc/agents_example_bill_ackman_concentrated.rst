Bill Ackman Concentrated Trading Team
=====================================

.. image:: ../docs/assets/ai-trading-team-workflows/bill-ackman-concentrated.png
   :alt: AI trading team workflow for Bill Ackman concentrated style investing
   :width: 100%

This example models a concentrated idea process: quality research, catalyst
analysis, an adversarial bear case, then one final position decision.

Agent flow
----------

* ``quality_researcher`` finds the best high-quality large-cap business.
* ``activist_bull`` argues for catalysts, pricing power, and value creation.
* ``short_seller_bear`` attacks the thesis.
* ``portfolio_manager`` builds one concentrated position with trading permission.

Example code
------------

.. literalinclude:: ../lumibot/example_strategies/ai_trading_team_bill_ackman_concentrated.py
   :language: python
