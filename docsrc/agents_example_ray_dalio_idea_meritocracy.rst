Ray Dalio Idea-Meritocracy Trading Team
=======================================

.. image:: ../docs/assets/ai-trading-team-workflows/ray-dalio-idea-meritocracy.png
   :alt: AI trading team workflow for Ray Dalio idea-meritocracy style macro debate
   :width: 100%

This example uses independent macro viewpoints and an explicit disagreement
step. The point is not to imitate a specific fund product; it is to show how
specialists can argue from different macro assumptions before one final trade.

Agent flow
----------

* ``growth_agent`` argues from a growth-surprise view.
* ``inflation_agent`` argues from rates and inflation.
* ``debt_liquidity_agent`` argues from debt, liquidity, currency, and policy pressure.
* ``thoughtful_disagreement`` stress-tests all views.
* ``trader`` chooses one macro ETF idea with trading permission.

Example code
------------

.. literalinclude:: ../lumibot/example_strategies/ai_trading_team_ray_dalio_idea_meritocracy.py
   :language: python
