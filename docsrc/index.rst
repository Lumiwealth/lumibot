LumiBot AI Trading
==================

.. meta::
   :description: Build AI-powered trading strategies in Python. Run an AI backtest, inspect its decisions and orders, then connect a supported broker with LumiBot.

.. container:: lumibot-hero-lead

   **Turn trading ideas into working strategies.**

LumiBot is a **free, open-source Python framework** for building trading bots.
Write your own trading rules, let AI agents make decisions, or combine both.
LumiBot runs your strategy against historical data, records its trades and
results, and connects it to supported brokers for paper or live trading.

Why build with LumiBot?
************************

* **Backtest before trading:** test on historical data and inspect charts, trades and results.
* **Same strategy for backtesting and trading:** use the same strategy class for historical tests, paper trading and live trading. Change the data source and broker runner.
* **Multiple asset classes:** trade stocks, options, futures, forex, crypto and prediction markets through :doc:`supported brokers <brokers.all>`.

Three ways to build a strategy
*******************************

* **Python strategies:** write the rules, indicators, position sizing and orders in Python.
* **AI-native strategies:** write prompts for agents that research, reason and place trades inside your strategy.
* **Hybrid strategies:** combine them. For example, Python finds an entry signal, an AI agent reviews it, and Python controls sizing and submits the order.

.. container:: lumibot-start-routes

   :ref:`Python quickstart <first-python-backtest>` · :ref:`AI example <first-ai-strategy>` · :doc:`More strategy examples <examples>`

.. _first-ai-strategy:

Build an AI-native strategy
****************************

**The trading rules go in the prompt.** This example creates one trading agent
in ``initialize()`` and calls it on every ``on_trading_iteration()``. Edit
``system_prompt`` to change its instructions; ``task_prompt`` starts each review.
You can add research agents later and pass their answers to the trading agent.

Install LumiBot with Python 3.10+ and set your model credentials. This example
uses an OpenAI model and Yahoo daily prices; model calls incur charges.

.. code-block:: bash

   pip install lumibot
   export OPENAI_API_KEY="your-openai-api-key"

Save this as ``my_ai_strategy.py``:

.. literalinclude:: _snippets/ai_strategy.py
   :language: python

.. code-block:: bash

   python my_ai_strategy.py

Review the agent's decisions, orders and backtest charts. This is an example
of the API, not a claim that the trading rule is profitable.
:doc:`The AI quickstart <agents_quickstart>` shows a complete researcher-and-trader
team. To run that existing example directly:

.. code-block:: bash

   python -m lumibot.example_strategies.ai_researcher_trader

.. _first-python-backtest:

Build a Python strategy
************************

Here the decisions are Python rules. This complete example buys 10 AAPL shares
on its first iteration and holds them. Save it as ``my_strategy.py``:

.. code-block:: python

   from datetime import datetime
   from lumibot.backtesting import YahooDataBacktesting
   from lumibot.strategies import Strategy

   class MyStrategy(Strategy):
       def on_trading_iteration(self):
           if self.first_iteration:
               order = self.create_order("AAPL", 10, "buy")
               self.submit_order(order)

   MyStrategy.backtest(
       YahooDataBacktesting,
       datetime(2023, 1, 1),
       datetime(2024, 1, 1),
   )

.. code-block:: bash

   python my_strategy.py

See :doc:`getting_started` for setup and :doc:`examples` for indicators,
options and other Python strategies.

Build a hybrid strategy
***********************

Keep the Python rules you need and add an agent for a specific decision.
For example, a Python moving-average signal can trigger a read-only AI risk
review. Python checks the structured answer, sizes the position and submits
the order. Give that reviewer ``allow_trading=False``; your Python code owns
the order submission. See :doc:`agents_flows` for the code and
:doc:`agents_quickstart` for structured answers.

Run the same strategy with a broker
************************************

The strategy class can stay the same. The code that starts it must select a
backtest or a broker run: ``Strategy.backtest(...)`` runs historical data;
``Trader.run_all()`` runs a strategy with a configured broker.
``IS_BACKTESTING=false`` alone cannot change a backtest-only runner.
See :doc:`strategy_run_modes` and the :doc:`broker guides <brokers>` for setup.

Connect your AI assistant
**************************

Use a coding assistant to help write either kind of strategy. This is separate
from the AI agents that run **inside** an AI-native strategy: the assistant
helps you build and test the code; the strategy's agents make its trading decisions.

.. include:: _snippets/botspot_mcp_callout.rst

For local coding help, :doc:`install the free LumiBot trading skills <claude_code_codex_cursor>`.
To give agents inside a running strategy BotSpot's research tools, see
:doc:`LumiBot strategy agents <botspot_mcp>` and ``lumibot connect botspot``.

Explore the framework
*********************

.. container:: lumibot-entry-grid lumibot-start-routes

   .. container:: lumibot-entry-card

      **Strategy examples**

      :doc:`Python strategies <examples>` · :doc:`AI strategies <agents_examples>` · :doc:`Agent teams and hybrids <agents_flows>`

   .. container:: lumibot-entry-card

      **Backtesting and trading**

      :doc:`Data sources <backtesting>` · :doc:`Broker setup <brokers>` · :doc:`Cash accounting <cash_accounting>`

   .. container:: lumibot-entry-card

      **API and comparisons**

      :doc:`Strategy API <strategy_api_overview>` · :doc:`Reusable components <standalone_components>` · :doc:`Compare frameworks <ai_trading_project_comparison>`

.. image:: ../docs/assets/ai-trading/benefit-hero.png
   :alt: LumiBot: Python rules or AI agents, historical backtests and broker connections
   :width: 640px
   :align: center
   :class: lumibot-entry-hero

Want help building your first AI trading bot?
---------------------------------------------

**Join the free challenge with Rob Grzesik, creator of LumiBot.**
Follow the training and learn how to build and backtest a trading strategy.

.. image:: ../docs/assets/ai-trading/rob-challenge-home.png
   :alt: Learn with Rob Grzesik, creator of LumiBot. Join the FREE challenge.
   :width: 640px
   :align: center
   :class: lumibot-learning-image
   :target: https://botspot.trade/challenges?utm_source=documentation&utm_medium=docs&utm_campaign=lumibot_ai_trading&utm_content=home_challenge_image

`Join the free challenge <https://botspot.trade/challenges?utm_source=documentation&utm_medium=docs&utm_campaign=lumibot_ai_trading&utm_content=free_challenge>`_
or explore the `AI Trading Bootcamp <https://botspot.trade/courses/ai-trading-bootcamp?utm_source=documentation&utm_medium=docs&utm_campaign=lumibot_ai_trading&utm_content=bootcamp>`_.

Table of Contents
*****************

.. toctree::
   :maxdepth: 1
   :caption: Start here

   Home <self>
   Build and Deploy with BotSpot <https://botspot.trade/sales?showLogin=1&utm_source=documentation&utm_medium=sidebar&utm_campaign=lumibot&utm_content=sidebar_build_bots&sample=lumibot_deploy_sample>
   BotSpot MCP Integration <botspot_mcp>
   Claude Code, Codex and Cursor <claude_code_codex_cursor>
   Start Here / Python Setup <getting_started>
   The lumibot Command <cli>
   Python Strategy Examples <examples>
   AI Quickstart <agents_quickstart>
   AI Examples <agents_examples>
   Backtest, Paper, or Live <strategy_run_modes>
   For Coding Agents <agent_start_here>

.. toctree::
   :maxdepth: 1
   :caption: AI trading

   AI Agent Guide <agents>
   The Execution Gap <execution_gap>
   Compare AI Frameworks <ai_trading_project_comparison>

.. toctree::
   :maxdepth: 1
   :caption: Build strategies

   Strategy API <strategy_api_overview>
   Imports and Startup <imports_and_startup>
   Reusable Components <standalone_components>
   Lifecycle Methods <lifecycle_methods>
   Strategy Methods <strategy_methods>
   Strategy Properties <strategy_properties>
   Assets and Orders <entities>
   Indicators <indicators>
   SEC Fundamentals <fundamentals>
   Macro Data <macro_data>

.. toctree::
   :maxdepth: 1
   :caption: Backtest and trade

   Backtesting <backtesting>
   Brokers <brokers>
   Cash Accounting <cash_accounting>
   Deployment <deployment>
   Reference <reference>
   Common Mistakes <common_mistakes>
   FAQ <faq>

.. toctree::
   :maxdepth: 1
   :caption: Community and learning

   Free AI Trading Challenge <https://botspot.trade/challenges?utm_source=documentation&utm_medium=sidebar&utm_campaign=lumibot_ai_trading&utm_content=free_challenge>
   Partnerships and Sponsorship <PARTNERSHIPS>
   Migrate from Backtrader <MIGRATING_FROM_BACKTRADER>
   GitHub <https://github.com/Lumiwealth/lumibot>
   Reddit Community <https://www.reddit.com/r/BotSpotTrade/>
   Discord Community <https://discord.gg/4R9j6T3PN8>
   Get Pre-Built Strategies <https://botspot.trade/marketplace?utm_source=documentation&utm_medium=sidebar&utm_campaign=lumibot&utm_content=sidebar_marketplace>

Frequently asked questions
**************************

What is LumiBot?
----------------

LumiBot is a Python framework for building trading strategies. You can write
strategies with your own Python rules, with AI agents, or with both. You can
backtest a strategy on historical data and then connect the same strategy class
to a supported broker for paper or live trading.

Is LumiBot free and open source?
--------------------------------

Yes. LumiBot is free and open source under the GNU General Public License
version 3 (GPL-3.0). The source code is on GitHub at
`github.com/Lumiwealth/lumibot <https://github.com/Lumiwealth/lumibot>`_.

Who created LumiBot?
--------------------

LumiBot was created by Rob Grzesik and is published by Lumiwealth. The project
lives in the ``Lumiwealth/lumibot`` repository on GitHub.

How do I install LumiBot in Python?
-----------------------------------

Run ``pip install lumibot``. To get the newest release later, run
``pip install lumibot --upgrade``. LumiBot needs Python 3.10 or newer.

Do I need AI or an API key to use LumiBot?
------------------------------------------

Python strategies run your rules directly. AI-native and hybrid strategies
that call a model need that provider's authentication. The AI example above
uses ``OPENAI_API_KEY`` and billed model calls. Data-provider credentials
depend on the data you choose. See :doc:`agents_quickstart`.

Do I need a broker account to run a backtest?
---------------------------------------------

No. A backtest uses historical data, not a broker. The quickstart backtest on
this page uses free Yahoo Finance data and no broker account. You only need a
broker account when you want to paper trade or trade live.

Which brokers does LumiBot work with?
-------------------------------------

LumiBot connects to 100+ brokers and crypto exchanges: Alpaca, Interactive
Brokers, Tradier, Schwab, Tradovate, ProjectX, Bitunix, Polymarket, and the
crypto exchanges your installed CCXT version lists. See :doc:`brokers` for
brokers by type, or :doc:`the A to Z list <brokers.all>` for every guide.

Can I use the same code for backtesting and live trading?
---------------------------------------------------------

Yes. The strategy class can stay the same. The code that starts it decides
whether it runs as a backtest or against a broker. Setting
``IS_BACKTESTING=false`` alone does not turn a backtest-only file into paper or
live trading. See :doc:`strategy_run_modes` for how each mode starts.

Where can I get help with LumiBot?
----------------------------------

Start with the :doc:`faq` and :doc:`common_mistakes` pages. You can also ask
questions in the community links in the sidebar, including the LumiBot GitHub
repository, Reddit, and Discord.

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
