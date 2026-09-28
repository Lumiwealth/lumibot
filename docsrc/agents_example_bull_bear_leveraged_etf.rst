Bull/Bear Leveraged ETF AI Trading Team
=======================================

.. meta::
   :description: This is a fast, dramatic AI trading team demo. It gives the agents a universe of leveraged long and inverse ETFs, turns a bull and bear debate into account weights.

.. image:: ../docs/assets/ai-trading-team-workflows/bull-bear-leveraged-etf.png
   :alt: AI trading team workflow for bull/bear leveraged ETFs
   :width: 100%

This is a fast, dramatic AI trading team demo. It gives the agents a universe
of leveraged long and inverse ETFs, turns a bull and bear debate into account
weights, and asks a dedicated trading-and-risk agent to rebalance to them. The
purpose is to show the full loop clearly: research, upside case, risk
challenge, risk-controlled execution.

Because the universe includes both bull and bear instruments, the team can
choose risk-on or risk-off exposure. That makes the decision trail easy to
audit: you can inspect why the agents liked a sector, why the bear agent
objected, and why the final trader still bought or sold.

How the team works
------------------

* ``researcher`` ranks the leveraged ETF universe.
* ``bull`` argues for the strongest money-making trade.
* ``bear`` points out the biggest risk.
* ``interpreter`` weighs both cases and returns target weights for the account.
* ``trader`` is the only agent that can place orders. It holds one direction per index (never TQQQ with SQQQ, or UPRO with SPXU), sells what the weights dropped, and never buys more than its cash.
* The universe pairs leveraged funds that move more than the index, such as TQQQ against SQQQ and UPRO against SPXU.

Latest run
----------

A check with ``openai/gpt-6-luna`` on high reasoning, Yahoo daily prices, a
$100,000 simulated account, and a TQQQ, SQQQ, UPRO, and SPXU universe ran from
January 5 to 15, 2026. It bought 841 UPRO on the first session, later split the
book between UPRO and TQQQ, and never held an ETF with its inverse. Cash stayed
positive the whole run; the lowest balance was $189. The account ended at
$101,466, up 1.47%. This is one short simulation, not a forecast.

Run it with a broker
--------------------

The file defaults to broker-connected execution. With Alpaca, it runs in paper
mode unless you set ``ALPACA_IS_PAPER=false``.

.. code-block:: bash

   export OPENAI_API_KEY='your-key-here'
   export ALPACA_API_KEY='your-alpaca-key'
   export ALPACA_API_SECRET='your-alpaca-secret'
   export ALPACA_IS_PAPER=true
   python lumibot/example_strategies/ai_trading_team_bull_bear_leveraged_etf.py

Backtest it
-----------

Use the same strategy class and change ``IS_BACKTESTING = False`` to ``IS_BACKTESTING = True`` in the runner:

.. code-block:: bash

   export OPENAI_API_KEY='your-key-here'
   python lumibot/example_strategies/ai_trading_team_bull_bear_leveraged_etf.py

Example code
------------

.. literalinclude:: ../lumibot/example_strategies/ai_trading_team_bull_bear_leveraged_etf.py
   :language: python
