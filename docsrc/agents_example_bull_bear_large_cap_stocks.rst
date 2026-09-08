Bull/Bear Large-Cap Stocks AI Trading Team
==========================================

.. image:: ../docs/assets/ai-trading-team-workflows/bull-bear-large-cap-stocks.png
   :alt: AI trading team workflow for bull/bear large-cap stocks
   :width: 100%

This strategy uses the same simple bull/bear pattern as the leveraged ETF demo,
but applies it to familiar large-cap stocks. It is a cleaner starting point for
people who want to understand the AI agent behavior before using more volatile
leveraged instruments.

The researcher picks the strongest stock, the bull agent argues the upside
case, the bear agent forces a risk check, and the trader decides whether to
rotate into the pick. Because the symbols are recognizable, it is easier to
read the trace and decide whether the agents are making sensible arguments.

`View the BotSpot marketplace listing <https://botspot.trade/marketplace/strategy/932f3661-c552-4723-b247-869518a5d30f>`__

How the team works
------------------

* ``researcher`` ranks the large-cap stock universe.
* ``bull`` argues for the strongest upside case.
* ``bear`` flags the biggest risk.
* ``trader`` sells non-picks, buys the chosen stock, and is the only agent allowed to trade.

Backtest snapshot
-----------------

Historical saved report from LumiBot 4.5.42, April 7–May 22, 2026. This is not
the current five-day tutorial run. The annualized figure extrapolates a short
historical window; it is not an observed annual return or a forecast.

.. image:: ../docs/assets/ai-trading-team-backtests/bull-bear-large-cap-stocks-backtest-top.png
   :alt: Top of the bull bear large cap stocks AI trading team backtest tear sheet
   :width: 100%

Start with a historical backtest
--------------------------------

Use Python 3.10 or later and a current LumiBot source checkout. Install it in a
virtual environment and configure your model account:

.. code-block:: bash

   python -m pip install -e .
   export GEMINI_API_KEY="your-gemini-key"
   export AI_TRADING_TEAM_MODEL="gemini-3.5-flash-lite"
   export LUMIBOT_AGENT_MAX_MODEL_CALLS="40"

Save the following complete runner as ``stock_team_backtest.py`` in the checkout:

.. code-block:: python

   from datetime import datetime
   from lumibot.backtesting import YahooDataBacktesting
   from lumibot.example_strategies.ai_trading_team_bull_bear_large_cap_stocks import (
       AITradingTeamBullBearLargeCapStocksStrategy,
   )

   if __name__ == "__main__":
       AITradingTeamBullBearLargeCapStocksStrategy.backtest(
           YahooDataBacktesting,
           datetime(2026, 4, 6),
           datetime(2026, 4, 11),
           budget=100_000,
           benchmark_asset="SPY",
           parameters={"universe": ["AAPL", "MSFT", "NVDA"]},
       )

.. code-block:: bash

   python stock_team_backtest.py

This imports the existing strategy class without invoking its broker runner.
It uses Yahoo daily prices, three stocks, and daily agent decisions over April
6–10, 2026. Broker credentials are not required for this runner. The $100,000
budget is simulated portfolio capital, not a model-spending allowance.

The researcher, bull, bear, and trader each run during a decision cycle, and a
run can include several provider calls. Model usage may incur charges. The
agent-call limit is not a dollar cap; a limit exit is incomplete. The original
trader prompt seeks a concentrated allocation using nearly all available cash.
This is an aggressive educational example even though its instruments are
ordinary stocks.

Inspect the decision summaries and generated backtest artifacts. Reconcile the
selected stock, submitted orders, fills or no-action outcome, and terminal run
status. The historical screenshot above is prior evidence; it is not a new
result for this exact source and window. A new full-window model-backed run is
required before calling the current tutorial runtime-verified. Historical LLM
knowledge can extend beyond the simulated date.

Run the existing broker entry point
-----------------------------------

Only use this path when you intend broker-connected execution. The original
module defaults to it; the separate runner above avoids changing its mode flag.
For Alpaca, explicitly set the account mode and credentials:

.. code-block:: bash

   export ALPACA_API_KEY="your-alpaca-key"
   export ALPACA_API_SECRET="your-alpaca-secret"
   export ALPACA_IS_PAPER=true
   python -m lumibot.example_strategies.ai_trading_team_bull_bear_large_cap_stocks

Example code
------------

.. literalinclude:: ../lumibot/example_strategies/ai_trading_team_bull_bear_large_cap_stocks.py
   :language: python
