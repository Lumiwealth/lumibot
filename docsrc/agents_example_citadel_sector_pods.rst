Citadel Sector Pods AI Trading Team
===================================

.. meta::
   :description: This strategy is inspired by the pod-style structure associated with Ken Griffin's Citadel and other multi-manager platforms.

.. image:: ../docs/assets/ai-trading-team-workflows/citadel-sector-pods.png
   :alt: AI trading team workflow for Citadel-style sector pods
   :width: 100%

This strategy is inspired by the pod-style structure associated with Ken
Griffin's Citadel and other multi-manager platforms. The idea is simple: do not
ask one generalist to understand every market at once. Give each specialist a
clear lane, let them pitch their strongest idea, then put a risk manager and
portfolio manager above the debate.

In Lumibot, that becomes an AI trading team. Five independent sector-pod
research branches study different parts of the market, their evidence converges
into a risk manager, and only the portfolio manager can build the diversified
allocation and place broker orders. It is a good example when you want to test
whether specialist agents can create better decisions than one broad prompt.

The diagram uses parallel branches to show that all five pods are peer inputs
with no dependency on one another. The current example invokes those branches
sequentially before the fan-in; it does not claim simultaneous model execution.

These are educational examples with no affiliation or endorsement from Citadel
or its personnel, and are not replicas of a proprietary strategy.

How the team works
------------------

* ``technology_pod`` looks at technology and communications exposure.
* ``financials_pod`` looks at financials and rate-sensitive exposure.
* ``healthcare_pod`` looks at healthcare and defensive growth.
* ``energy_pod`` looks at energy and commodity-sensitive exposure.
* ``consumer_pod`` looks at consumer and housing-sensitive exposure.
* ``risk_manager`` challenges crowding, drawdown, macro, and reversal risk.
* ``portfolio_manager`` builds a diversified three-or-more-sector allocation and is the only agent allowed to place broker orders.

Run it with a broker
--------------------

The file defaults to broker-connected execution. With Alpaca, it runs in paper
mode unless you set ``ALPACA_IS_PAPER=false``.

.. code-block:: bash

   export OPENAI_API_KEY='your-key-here'
   export AI_TRADING_TEAM_MODEL='openai/gpt-6-luna'
   export ALPACA_API_KEY='your-alpaca-key'
   export ALPACA_API_SECRET='your-alpaca-secret'
   export ALPACA_IS_PAPER=true
   python lumibot/example_strategies/ai_trading_team_citadel_sector_pods.py

Backtest it
-----------

Use the same strategy class and change ``IS_BACKTESTING = False`` to ``IS_BACKTESTING = True`` in the runner:

.. code-block:: bash

   export OPENAI_API_KEY='your-key-here'
   export AI_TRADING_TEAM_MODEL='openai/gpt-6-luna'
   python lumibot/example_strategies/ai_trading_team_citadel_sector_pods.py

Example code
------------

Regular ETF source:

.. literalinclude:: ../lumibot/example_strategies/ai_trading_team_citadel_sector_pods.py
   :language: python

Leveraged ETF source:

.. literalinclude:: ../lumibot/example_strategies/ai_trading_team_citadel_sector_pods_leveraged.py
   :language: python
