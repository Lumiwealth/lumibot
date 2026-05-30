AI Trading Team Examples
========================

LumiBot includes several copy-paste AI trading team examples. Each one keeps the
Python simple: create agents in ``initialize()``, pass context through them in
``on_trading_iteration()``, and let only the final portfolio-manager agent trade.

.. toctree::
   :maxdepth: 1

   agents_example_bull_bear_leveraged_etf
   agents_example_bull_bear_large_cap_stocks
   agents_example_ray_dalio_idea_meritocracy
   agents_example_warren_buffett_value
   agents_example_bill_ackman_concentrated
   agents_example_citadel_sector_pods

These examples are inspired by public investing styles and firms. They are not
affiliated with or endorsed by the investors, firms, or companies named.

Examples
--------

``ai_trading_team_bull_bear_leveraged_etf.py``
   Aggressive bull/bear team for leveraged ETF rotation.

``ai_trading_team_bull_bear_large_cap_stocks.py``
   Large-cap stock team with evidence, bull, bear, and portfolio-manager roles.

``ai_trading_team_ray_dalio_idea_meritocracy.py``
   Macro specialists argue growth, inflation, debt, and liquidity, then a
   disagreement agent stress-tests the views before the trader acts.

``ai_trading_team_warren_buffett_value.py``
   Annual-report, valuation, and long-term business-quality team.

``ai_trading_team_bill_ackman_concentrated.py``
   Concentrated high-conviction large-cap team with an activist bull case and
   short-seller bear case.

``ai_trading_team_citadel_sector_pods.py``
   Sector-pod team that compares sector ETFs through cyclical, defensive, and
   risk lenses.

Backward Compatibility
----------------------

Legacy imports still work:

* ``lumibot/example_strategies/ai_trading_team.py``
* ``lumibot/example_strategies/ai_investment_committee.py``

New code should prefer the descriptive filenames above.
