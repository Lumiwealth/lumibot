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

``ai_trading_team_warren_buffett_value.py``
   A value-investing team where one agent reads for business quality and annual
   report evidence, one agent demands valuation discipline, and the portfolio
   manager buys only the best long-term compounder.

``ai_trading_team_ray_dalio_idea_meritocracy.py``
   A Bridgewater-style idea-meritocracy workflow where growth, inflation, and
   liquidity agents argue, a disagreement agent stress-tests the assumptions,
   and the trader chooses one macro ETF.

``ai_trading_team_bill_ackman_concentrated.py``
   A concentrated investing workflow where a quality researcher, activist bull,
   and short-seller bear debate whether one high-conviction large-cap position
   deserves capital.

``ai_trading_team_bull_bear_leveraged_etf.py``
   An aggressive bull/bear demo where agents debate leveraged long and inverse
   ETFs before rotating into one high-conviction ETF.

``ai_trading_team_bull_bear_large_cap_stocks.py``
   The same bull/bear debate structure applied to familiar large-cap stocks, so
   the reasoning is easier to inspect before using more volatile instruments.

``ai_trading_team.py`` remains the shortest alias for the leveraged ETF example.
New code should prefer the descriptive filenames above.
