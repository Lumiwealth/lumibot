Bull/Bear Leveraged ETF AI Trading Team
=======================================

.. image:: ../docs/assets/ai-trading-team-workflows/bull-bear-leveraged-etf.png
   :alt: AI trading team workflow for bull/bear leveraged ETFs
   :width: 100%

This is the fast, dramatic AI trading team demo. It gives the agents a universe
of leveraged long and inverse ETFs, then asks them to rotate aggressively into
one ETF. The purpose is not subtle portfolio construction. The purpose is to
show the full trading-team loop clearly: research, upside case, risk challenge,
and final execution.

Because the universe includes both bull and bear instruments, the team can
choose risk-on or risk-off exposure. That makes the decision trail easy to
audit: you can inspect why the agents liked a sector, why the bear agent
objected, and why the final trader still bought or sold.

How the team works
------------------

* ``researcher`` ranks the leveraged ETF universe.
* ``bull`` argues for the strongest money-making trade.
* ``bear`` points out the biggest risk.
* ``trader`` sells non-picks, buys the chosen ETF, and is the only agent allowed to trade.

Backtest snapshot
-----------------

.. image:: ../docs/assets/ai-trading-team-backtests/bull-bear-leveraged-etf-backtest-top.png
   :alt: Top of the bull bear leveraged ETF AI trading team backtest tear sheet
   :width: 100%

Run it with a broker
--------------------

The file defaults to broker-connected execution. With Alpaca, it runs in paper
mode unless you set ``ALPACA_IS_PAPER=false``.

.. code-block:: bash

   export GEMINI_API_KEY='your-key-here'
   export ALPACA_API_KEY='your-alpaca-key'
   export ALPACA_API_SECRET='your-alpaca-secret'
   export ALPACA_IS_PAPER=true
   python lumibot/example_strategies/ai_trading_team_bull_bear_leveraged_etf.py

Backtest it
-----------

Use the same strategy class and change ``IS_BACKTESTING = False`` to ``IS_BACKTESTING = True`` in the runner:

.. code-block:: bash

   export GEMINI_API_KEY='your-key-here'
   python lumibot/example_strategies/ai_trading_team_bull_bear_leveraged_etf.py

Example code
------------

.. literalinclude:: ../lumibot/example_strategies/ai_trading_team_bull_bear_leveraged_etf.py
   :language: python
