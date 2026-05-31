AI Trading Team Examples
========================

LumiBot includes several copy-paste AI trading team examples. Each one keeps the
Python simple: create agents in ``initialize()``, pass context through them in
``on_trading_iteration()``, and let only the final portfolio-manager or trader
agent submit orders. The same file runs with a broker by default, or backtests
when you set ``IS_BACKTESTING = True`` in the flat runner.

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

``ai_trading_team_citadel_sector_pods.py``
   Inspired by the pod-style structure associated with Ken Griffin's Citadel:
   sector specialists pitch their best ETF ideas, a risk manager challenges the
   setup, and a portfolio manager rotates into the strongest sector.
   `Watch it live on BotSpot <https://botspot.trade/marketplace/strategy/0b4576c7-f78b-4477-ba3a-630758fb0168>`__.

``ai_trading_team_warren_buffett_value.py``
   A value-investing team where one agent reads for business quality and annual
   report evidence, one agent demands valuation discipline, and the portfolio
   manager buys only the best long-term compounder.
   `Watch it live on BotSpot <https://botspot.trade/marketplace/strategy/bdd324e9-8026-4115-b26e-30cccf6e00e8>`__.

``ai_trading_team_ray_dalio_idea_meritocracy.py``
   A Bridgewater-style idea-meritocracy workflow where growth, inflation, and
   liquidity agents argue, a disagreement agent stress-tests the assumptions,
   and the trader chooses one macro ETF.
   `Watch it live on BotSpot <https://botspot.trade/marketplace/strategy/81af73b8-7dec-4941-ba35-d5a06fee6863>`__.

``ai_trading_team_bill_ackman_concentrated.py``
   A concentrated investing workflow where a quality researcher, activist bull,
   and short-seller bear debate whether one high-conviction large-cap position
   deserves capital.
   `Watch it live on BotSpot <https://botspot.trade/marketplace/strategy/d56d5bf1-293b-44d8-a18c-bdda969b82f3>`__.

``ai_trading_team_bull_bear_leveraged_etf.py``
   An aggressive bull/bear demo where agents debate leveraged long and inverse
   ETFs before rotating into one high-conviction ETF.
   `Watch it live on BotSpot <https://botspot.trade/marketplace/strategy/4aa43848-54d6-48bf-b2e4-b266f9fec6ad>`__.

``ai_trading_team_bull_bear_large_cap_stocks.py``
   The same bull/bear debate structure applied to familiar large-cap stocks, so
   the reasoning is easier to inspect before using more volatile instruments.
   `Watch it live on BotSpot <https://botspot.trade/marketplace/strategy/932f3661-c552-4723-b247-869518a5d30f>`__.

``ai_trading_team.py`` remains the shortest alias for the leveraged ETF example.
New code should prefer the descriptive filenames above.
