Lumibot: Backtesting and Algorithmic Trading Library
====================================================

**An Easy to Use and Powerful Backtesting and Trading Library for Crypto, Stocks, Options, Futures and FOREX**

.. raw:: html
   :file: _html/main.html

AI Trading Agents -- Backtest AI Agents with Real External Tools
*****************************************************************

LumiBot is built for **AI agents that reason, call external tools, and make trading decisions on every bar during a backtest** -- then run the exact same code live. This is real agentic backtesting: the LLM is inside the simulation loop, not bolted onto the side.

- **Backtest AI trading agents** with real external data from 20,000+ MCP servers
- **LLM in the loop on every bar** -- the agent reasons over point-in-time market state, calls tools, and submits orders
- **Replay caching** makes warm backtest reruns deterministic and fast (zero LLM calls on rerun)
- **Any LLM provider per agent** -- use a cheaper model for evidence gathering and a stronger model for debate/trading
- **Built-in SEC fundamentals and filings** -- agents can inspect income statements, balance sheets, cash flow, company facts, and annual reports
- **Built-in FRED macro data** -- agents can inspect rates, inflation, labor, growth, liquidity, credit spreads, and market-risk series
- **Trading permissions** -- research agents can use read-only tools while portfolio-manager agents place orders
- **Same code for backtest and live** -- write once, backtest it, deploy it
- **External MCP servers are just a URL** -- no local scripts, no npm installs

Key AI agent docs:

- :doc:`agents` -- main guide: agentic backtesting framework, MCP trading tools, and competitive positioning
- :doc:`agents_flows` -- design single-agent, multi-agent, debate, committee, and hybrid flows
- :doc:`agents_investment_committee` -- one concrete multi-agent investment committee example
- :doc:`fundamentals` -- SEC fundamentals and filing research tools
- :doc:`macro_data` -- FRED macro data tools and point-in-time behavior
- :doc:`agents_builtin_tools` -- built-in tools, indicators, and trading permissions
- :doc:`agents_quickstart` -- quick start with code examples for AI agent backtesting
- :doc:`agents_canonical_demos` -- three reference demos: news sentiment, macro risk, and M2 liquidity
- :doc:`agents_observability` -- traces, replay cache, warnings, and debugging workflow

Start with :doc:`agents` to learn how LumiBot puts AI agents inside the backtest loop with external MCP tools.

Cash Accounting
***************

Lumibot supports explicit cash accounting for both backtests and live broker
telemetry. Use the strategy cash methods for deposits, withdrawals, direct
cash adjustments, and financing setup, then review the resulting
cash-adjusted returns in the standard backtest artifacts.

- Backtests keep external cashflows out of strategy performance
- Live cloud payloads can include normalized broker ``cash_events``
- Listener storage keeps raw normalized events in a dedicated event table

Start with :doc:`cash_accounting` for the end-to-end guide.

Getting Started
****************

After you have installed Lumibot on your computer, you can create a strategy and backtest it using free data available from Yahoo Finance, or use your own data. Here's how to get started:

Step 1: Install Lumibot
------------------------

.. note::

   **Ensure you have installed the latest version of Lumibot**. Upgrade using the following command:

   .. code-block:: bash

       pip install lumibot --upgrade

Install the package on your computer:

.. code-block:: bash

    pip install lumibot

Step 2: Create a Strategy for Backtesting
------------------------------------------

Here's some code to get you started:

.. code-block:: python

    from datetime import datetime
    from lumibot.backtesting import YahooDataBacktesting
    from lumibot.strategies import Strategy

    # A simple strategy that buys AAPL on the first day and holds it
    class MyStrategy(Strategy):
        def on_trading_iteration(self):
            if self.first_iteration:
                aapl_price = self.get_last_price("AAPL")
                quantity = self.portfolio_value // aapl_price
                order = self.create_order("AAPL", quantity, "buy")
                self.submit_order(order)

    # Pick the dates that you want to start and end your backtest
    backtesting_start = datetime(2020, 11, 1)
    backtesting_end = datetime(2020, 12, 31)

    # Run the backtest
    MyStrategy.backtest(
        YahooDataBacktesting,
        backtesting_start,
        backtesting_end,
    )

Step 3: Take Your Bot Live
---------------------------

Once you have backtested your strategy and found it to be profitable on historical data, you can take your bot live. Notice how the strategy code is exactly the same. Here's an example using Alpaca (you can create a free Paper Trading account here in minutes: `https://alpaca.markets/ <https://alpaca.markets/>`_).

.. code-block:: python

   from lumibot.brokers import Alpaca
   from lumibot.strategies.strategy import Strategy
   from lumibot.traders import Trader

   ALPACA_CONFIG = {
        "API_KEY": "YOUR_ALPACA_API_KEY",
        "API_SECRET": "YOUR_ALPACA_SECRET",
        "PAPER": True  # Set to True for paper trading, False for live trading
    }

   # A simple strategy that buys AAPL on the first day and holds it
   class MyStrategy(Strategy):
      def on_trading_iteration(self):
         if self.first_iteration:
               aapl_price = self.get_last_price("AAPL")
               quantity = self.portfolio_value // aapl_price
               order = self.create_order("AAPL", quantity, "buy")
               self.submit_order(order)

   trader = Trader()
   broker = Alpaca(ALPACA_CONFIG)
   strategy = MyStrategy(broker=broker)

   # Run the strategy live
   trader.add_strategy(strategy)
   trader.run_all()

.. important::

   **Remember to start with a paper trading account** to ensure everything works as expected before moving to live trading.

All Together
************

Here's the complete code:

.. code-block:: python

    from datetime import datetime
    from lumibot.backtesting import YahooDataBacktesting
    from lumibot.brokers import Alpaca
    from lumibot.strategies import Strategy
    from lumibot.traders import Trader

    ALPACA_CONFIG = {
        "API_KEY": "YOUR_ALPACA_API_KEY",
        "API_SECRET": "YOUR_ALPACA_SECRET",
        "PAPER": True
    }

    class MyStrategy(Strategy):
        def on_trading_iteration(self):
            if self.first_iteration:
                aapl_price = self.get_last_price("AAPL")
                quantity = self.portfolio_value // aapl_price
                order = self.create_order("AAPL", quantity, "buy")
                self.submit_order(order)

    trader = Trader()
    broker = Alpaca(ALPACA_CONFIG)
    strategy = MyStrategy(broker=broker)

    # Run the strategy live
    trader.add_strategy(strategy)
    trader.run_all()

Or you can download the file here: `https://github.com/Lumiwealth/lumibot/blob/dev/lumibot/example_strategies/simple_start_single_file.py <https://github.com/Lumiwealth/lumibot/blob/dev/lumibot/example_strategies/simple_start_single_file.py>`_

Additional Resources
********************

If you would like to learn how to modify your strategies, we suggest that you first learn about Lifecycle Methods, then Strategy Methods, and Strategy Properties. You can find the documentation for these in the menu, with the main pages describing what they are, then the sub-pages describing each method and property individually.

We also have some more sample code that you can check out here: `https://github.com/Lumiwealth/lumibot/tree/dev/lumibot/example_strategies <https://github.com/Lumiwealth/lumibot/tree/dev/lumibot/example_strategies>`_

We wish you good luck with your trading strategies. Don't forget us when you're swimming in cash!

Need Extra Help?
****************

.. raw:: html
   :file: _html/course_list.html

.. important::

   **Build Trading Bots with AI**
   
   Want to create trading bots without writing code? Visit `BotSpot <https://botspot.trade/sales?showLogin=1&utm_source=documentation&utm_medium=home&utm_campaign=lumibot&utm_content=need_extra_help&prompt=I%20want%20to%20build%20and%20run%20a%20Lumibot%20trading%20strategy%20on%20BotSpot.%20Please%20help%20me%20set%20up%20a%20backtest%20and%20paper%20or%20live%20deployment.>`_ - our platform for building, testing, and running trading strategies using AI.
   
   - Create strategies using natural language
   - Backtest on historical data
   - Deploy to live markets
   - Join a community of algorithmic traders
   
   **Start on BotSpot:** `https://botspot.trade <https://botspot.trade/sales?showLogin=1&utm_source=documentation&utm_medium=home&utm_campaign=lumibot&utm_content=need_extra_help_start&prompt=I%20want%20to%20build%20and%20run%20a%20Lumibot%20trading%20strategy%20on%20BotSpot.%20Please%20help%20me%20set%20up%20a%20backtest%20and%20paper%20or%20live%20deployment.>`_

Table of Contents
*****************

.. toctree::
   :maxdepth: 2

   Home <self>
   Build Bots with AI <https://botspot.trade/sales?showLogin=1&utm_source=documentation&utm_medium=sidebar&utm_campaign=lumibot&utm_content=sidebar_build_bots&prompt=I%20want%20to%20build%20and%20run%20a%20Lumibot%20trading%20strategy%20on%20BotSpot.%20Please%20help%20me%20set%20up%20a%20backtest%20and%20paper%20or%20live%20deployment.>
   BotSpot MCP Integration <botspot_mcp>
   GitHub <https://github.com/Lumiwealth/lumibot>
   getting_started
   imports_and_startup
   agents
   cash_accounting
   lifecycle_methods
   strategy_methods
   strategy_properties
   entities
   indicators
   fundamentals
   macro_data
   backtesting
   brokers
   reference
   examples
   deployment
   common_mistakes
   faq
   Get Pre-Built Strategies <https://botspot.trade/marketplace?utm_source=documentation&utm_medium=sidebar&utm_campaign=lumibot&utm_content=sidebar_marketplace>

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
