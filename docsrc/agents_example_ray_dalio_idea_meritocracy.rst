Ray Dalio Idea Meritocracy AI Trading Team
==========================================

.. image:: ../docs/assets/ai-trading-team-workflows/ray-dalio-idea-meritocracy.png
   :alt: AI trading team workflow for Ray Dalio idea-meritocracy style macro debate
   :width: 100%

Track or copy it on BotSpot
---------------------------

* `Regular ETFs on BotSpot <https://botspot.trade/marketplace/strategy/b00c5f9c-beea-46fe-bdba-fc65c1315d5f?utm_source=documentation&utm_medium=example&utm_campaign=lumibot_ai_examples&utm_content=ray_regular>`_
* `Leveraged ETFs on BotSpot <https://botspot.trade/marketplace/strategy/362a50a1-d501-4b08-8d42-c7701a363731?utm_source=documentation&utm_medium=example&utm_campaign=lumibot_ai_examples&utm_content=ray_leveraged>`_

These two approved free listings are the source of truth for the code below.
Their published ``main.py`` revisions were copied byte-for-byte into this
repository on September 20, 2026.

This strategy is inspired by Ray Dalio's public writing about idea meritocracy
and thoughtful disagreement. It is not an "All Weather" clone. The important
idea is the operating system: independent thinkers argue from different models
of the world, the disagreement is explicit, and the final decision should be
stronger because weak assumptions were challenged.

In Lumibot, that turns into a macro trading team. Three independent research
branches argue from growth, inflation and rates, and debt/liquidity/currency.
Their evidence converges into a disagreement agent, then a dedicated trader
builds the final diversified basket and places broker orders.

The diagram uses parallel branches to show that the three specialists are peer
inputs with no dependency on one another. The current example invokes those
branches sequentially before the fan-in; it does not claim simultaneous model
execution.

These are educational examples with no affiliation or endorsement from Ray
Dalio or Bridgewater, and are not replicas of their proprietary strategies.

How the team works
------------------

* ``growth_agent`` asks what wins if growth improves.
* ``inflation_agent`` asks what wins or loses if inflation and rates surprise.
* ``debt_liquidity_agent`` argues from debt, liquidity, currency, and policy pressure.
* ``thoughtful_disagreement`` challenges the other agents and names the strongest idea.
* ``trader`` builds the diversified macro ETF basket and is the only agent allowed to place broker orders.

Backtest snapshot
-----------------

.. image:: ../docs/assets/ai-trading-team-backtests/ray-dalio-idea-meritocracy-backtest-top.png
   :alt: Top of the Ray Dalio idea meritocracy AI trading team backtest tear sheet
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
   python lumibot/example_strategies/ai_trading_team_ray_dalio_idea_meritocracy.py

Backtest it
-----------

Use the same strategy class and change ``IS_BACKTESTING = False`` to ``IS_BACKTESTING = True`` in the runner:

.. code-block:: bash

   export GEMINI_API_KEY='your-key-here'
   python lumibot/example_strategies/ai_trading_team_ray_dalio_idea_meritocracy.py

Example code
------------

Regular ETF source:

.. literalinclude:: ../lumibot/example_strategies/ai_trading_team_ray_dalio_idea_meritocracy.py
   :language: python

Leveraged ETF source:

.. literalinclude:: ../lumibot/example_strategies/ai_trading_team_ray_dalio_idea_meritocracy_leveraged.py
   :language: python
