Bull/Bear Large-Cap Stocks AI Trading Team
==========================================

.. meta::
   :description: This strategy uses the same simple bull/bear pattern as the leveraged ETF demo, but applies it to familiar large-cap stocks.

.. image:: ../docs/assets/ai-trading-team-workflows/bull-bear-large-cap-stocks.png
   :alt: AI trading team workflow for bull/bear large-cap stocks
   :width: 100%

This strategy uses the same simple bull/bear pattern as the leveraged ETF demo,
but applies it to familiar large-cap stocks. It is a cleaner starting point for
people who want to understand the AI agent behavior before using more volatile
leveraged instruments.

The researcher ranks the stocks, the bull and bear agents argue both sides, the
interpreter turns the debate into account weights, and the trader rebalances to
those weights. Because the symbols are recognizable, it is easier to
read the trace and decide whether the agents are making sensible arguments.

How the team works
------------------

* ``researcher`` ranks the large-cap stock universe.
* ``bull`` argues for the strongest upside case.
* ``bear`` flags the biggest risk.
* ``interpreter`` weighs both cases and returns target weights for the account.
* ``trader`` is the only agent that can place orders. It reads the account once, sells names the weights dropped, leaves holdings within 2 percentage points of target alone, and never buys more than its cash.

Start with a historical backtest
--------------------------------

Use Python 3.10 or later and a current LumiBot source checkout. Install it in a
virtual environment and configure your model account:

.. code-block:: bash

   python -m pip install -e .
   export OPENAI_API_KEY="your-openai-key"
   export AI_EXAMPLE_MODEL="openai/gpt-6-luna"
   export LUMIBOT_AGENT_MAX_MODEL_CALLS="80"

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
           datetime(2026, 1, 5),
           datetime(2026, 1, 16),
           budget=100_000,
           benchmark_asset="SPY",
           parameters={"universe": ["AAPL", "MSFT", "NVDA", "AMZN"]},
       )

.. code-block:: bash

   python stock_team_backtest.py

This imports the existing strategy class without invoking its broker runner.
It uses Yahoo daily prices, four stocks, and daily agent decisions over January
5 to 15, 2026. Broker credentials are not required for this runner. The $100,000
budget is simulated portfolio capital, not a model-spending allowance.

The researcher, bull, bear, interpreter, and trader each run during a decision cycle, and a
run can include several provider calls. Model usage may incur charges. The
agent-call limit is not a dollar cap; a limit exit is incomplete. The trader is
the only agent allowed to execute and owns the final risk check.

Inspect the decision summaries and generated backtest artifacts. Reconcile the
selected stock, submitted orders, fills or no-action outcome, and terminal run
status.

Latest run
----------

The runner above was checked with ``openai/gpt-6-luna`` on high reasoning,
Yahoo daily prices, and a $100,000 simulated account from January 5 to 15,
2026. On January 5 the trader split the account across NVDA, AAPL, AMZN, and
MSFT, spending about $98,700. On later sessions it resized toward each day's
weights and never bought and sold the same stock on the same day. Cash stayed
positive the whole run; the lowest balance was $206.

The account ended at $97,844, down 2.16%, while SPY rose about 1% over the
same window. The largest drawdown was 2.5%. This is one short simulation, not a
forecast, and a fresh model run can choose different weights.

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
