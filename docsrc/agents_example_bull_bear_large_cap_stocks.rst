Bull/Bear Large-Cap Stocks Trading Team
=======================================

.. image:: ../docs/assets/ai-trading-team-workflows/bull-bear-large-cap-stocks.png
   :alt: AI trading team workflow for bull/bear large-cap stocks
   :width: 100%

This example applies the same bull/bear debate pattern to large-cap stocks. It
keeps the flow intentionally small so the agent behavior is easy to inspect in
backtest traces.

Agent flow
----------

* ``researcher`` ranks the large-cap stock universe.
* ``bull`` argues for the strongest upside case.
* ``bear`` flags the biggest risk.
* ``trader`` sells non-picks and buys the chosen stock with trading permission.

Example code
------------

.. literalinclude:: ../lumibot/example_strategies/ai_trading_team_bull_bear_large_cap_stocks.py
   :language: python
