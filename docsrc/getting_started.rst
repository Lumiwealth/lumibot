Getting Started
***************

Lumibot is a Python library that allows you to create trading strategies and backtest them. It also allows you to run your strategies live on a paper trading account. You can also use Lumibot to run your strategies live on a real trading account, but we recommend you start with paper trading first.

Lumibot is designed to be easy to use, but also powerful. It is designed to be used by both beginners and advanced users. It is also designed to be flexible, so you can use it to create any kind of trading strategy you want. It is also designed to be fast, so you can backtest your strategies quickly.

Build AI Trading Agents
=======================

Lumibot now supports **AI trading agents** inside the ``Strategy`` class. If you want an **agentic trading** workflow, you can create an agent in ``initialize()``, run it from ``on_trading_iteration()`` or ``on_filled_order()``, query time-series data with DuckDB, and replay the same agent decisions during backtests.

Read :doc:`agents` for the full guide.

Need Help Building Strategies?
==============================

Our **AI agent** was built specifically for LumiBot and can help you create strategies in minutes—no coding required. Just describe your strategy in plain English, and the AI generates the Python code for you.

At `BotSpot <https://www.botspot.trade/?utm_source=documentation&utm_medium=referral&utm_campaign=lumibot_getting_started>`_, you can:

- **Build strategies with AI** — Describe what you want in plain English, and our AI creates the code
- **Explore our marketplace** — Access 50+ proven trading strategies built by our community
- **Join 2,400+ traders** — Connect with other builders in our Discord community
- **Take our AI Bootcamp** — Live hands-on training to master automated trading

.. important::

   `Claim your free trial <https://www.botspot.trade/?utm_source=documentation&utm_medium=referral&utm_campaign=lumibot_getting_started>`_ while spots last at BotSpot.trade.

Getting Started With Lumibot
============================

Welcome to Lumibot! This guide will help you get started with Lumibot. We hope you enjoy it!

Here are the steps to get started using the Alpaca broker. If you want to use a different broker, you can see the list of supported brokers under the brokers section.

.. note::

   **Advanced Configuration:** For live trading, you can optionally configure separate brokers for trading and data by setting the ``TRADING_BROKER`` and ``DATA_SOURCE`` environment variables. See the :doc:`deployment` section for details.

Step 1: Install the Package
---------------------------

.. note::

   **Before proceeding, ensure you have installed the latest version of Lumibot**. You can do this by running the following command:

.. code-block:: bash

    pip install lumibot --upgrade

Install the package on your computer:

.. code-block:: bash

    pip install lumibot

Step 2: Import the Following Modules
------------------------------------

.. code-block:: python

    # importing the trader class
    from lumibot.traders import Trader
    # importing the alpaca broker class
    from lumibot.brokers import Alpaca

Step 3: Create an Alpaca Paper Trading Account
----------------------------------------------

Create an Alpaca paper trading account: `https://alpaca.markets/ <https://alpaca.markets/>`_ (you can also use other brokers, but Alpaca is easiest to get started with).

.. note::

   **Make sure to use a paper trading account** at first to get comfortable with Lumibot without risking real money.

Step 4: Configure Your API Keys
-------------------------------

Copy your API_KEY and API_SECRET from the Alpaca dashboard and create a Config class like this:

.. code-block:: python

    ALPACA_CONFIG = {
        # Put your own Alpaca key here:
        "API_KEY": "YOUR_ALPACA_API_KEY",
        # Put your own Alpaca secret here:
        "API_SECRET": "YOUR_ALPACA_SECRET",
        # Set this to False to use a live account
        "PAPER": True
    }

Step 5: Create a Strategy Class
-------------------------------

Create a strategy class (See strategy section) e.g. class MyStrategy(Strategy) or import an example from our libraries, like this:

.. code-block:: python

    class MyStrategy(Strategy):
        # Custom parameters
        parameters = {
            "symbol": "SPY",
            "quantity": 1,
            "side": "buy"
        }

        def initialize(self, symbol=""):
            # Will make on_trading_iteration() run every 180 minutes
            self.sleeptime = "180M"

        def on_trading_iteration(self):
            symbol = self.parameters["symbol"]
            quantity = self.parameters["quantity"]
            side = self.parameters["side"]

            order = self.create_order(symbol, quantity, side)
            self.submit_order(order)

Step 6: Instantiate the Trader, Alpaca, and Strategy Classes
------------------------------------------------------------

.. code-block:: python

    trader = Trader()
    broker = Alpaca(ALPACA_CONFIG)
    strategy = MyStrategy(name="My Strategy", budget=10000, broker=broker, symbol="SPY")

Step 7: Backtest the Strategy (Optional)
----------------------------------------

.. note::

   **Backtesting is a crucial step** to understand how your strategy would have performed in the past. It helps in refining and improving your strategy before going live.

.. code-block:: python

    from datetime import datetime
    from lumibot.backtesting import YahooDataBacktesting

    backtesting_start = datetime(2020, 1, 1)
    backtesting_end = datetime(2020, 12, 31)
    strategy.run_backtest(
        YahooDataBacktesting,
        backtesting_start,
        backtesting_end,
        parameters={
            "symbol": "SPY"
        },
    )

Step 8: Run the Strategy
------------------------

.. note::

   **Running a strategy live** carries real financial risks. Start with paper trading to get familiar with the process and ensure your strategy works as expected.

.. code-block:: python

    trader.add_strategy(strategy)
    trader.run_all()

.. important::

   **And that's it!** Now try modifying the strategy to do what you want it to do.

Here it is all together:

.. code-block:: python

    from datetime import datetime
    from lumibot.backtesting import YahooDataBacktesting
    from lumibot.brokers import Alpaca
    from lumibot.strategies.strategy import Strategy
    from lumibot.traders import Trader

    ALPACA_CONFIG = {
        "API_KEY": "YOUR_ALPACA_API_KEY",
        "API_SECRET": "YOUR_ALPACA_SECRET",
        # Set this to False to use a live account
        "PAPER": True
    }

    class MyStrategy(Strategy):
        parameters = {
            "symbol": "SPY",
            "quantity": 1,
            "side": "buy"
        }

        def initialize(self, symbol=""):
            self.sleeptime = "180M"

        def on_trading_iteration(self):
            symbol = self.parameters["symbol"]
            quantity = self.parameters["quantity"]
            side = self.parameters["side"]
            order = self.create_order(symbol, quantity, side)
            self.submit_order(order)

    trader = Trader()
    broker = Alpaca(ALPACA_CONFIG)
    strategy = MyStrategy(broker=broker, parameters={"symbol": "SPY"})

    backtesting_start = datetime(2020, 1, 1)
    backtesting_end = datetime(2020, 12, 31)
    strategy.run_backtest(
        YahooDataBacktesting,
        backtesting_start,
        backtesting_end,
        parameters={"symbol": "SPY"}
    )

    trader.add_strategy(strategy)
    trader.run_all()

Or you can download the file here: `https://github.com/Lumiwealth/lumibot/blob/dev/lumibot/example_strategies/simple_start_single_file.py <https://github.com/Lumiwealth/lumibot/blob/dev/lumibot/example_strategies/simple_start_single_file.py>`_.

Adding Trading Fees
===================

If you want to add trading fees to your backtesting, you can do so by setting up your backtesting like this:

.. code-block:: python

    from lumibot.backtesting import YahooDataBacktesting
    from lumibot.entities import TradingFee

    # Create trading fees: flat (per order), percent (of order value), or per-contract
    trading_fee_1 = TradingFee(flat_fee=5)  # $5 flat fee per order
    trading_fee_2 = TradingFee(percent_fee=0.01)  # 1% trading fee
    # For options/futures, use per_contract_fee instead:
    # trading_fee = TradingFee(per_contract_fee=0.65)  # $0.65 per contract

    backtesting_start = datetime(2020, 1, 1)
    backtesting_end = datetime(2020, 12, 31)
    strategy.run_backtest(
        YahooDataBacktesting,
        backtesting_start,
        backtesting_end,
        parameters={"symbol": "SPY"},
        buy_trading_fees=[trading_fee_1, trading_fee_2],
        sell_trading_fees=[trading_fee_1, trading_fee_2],
    )

Profiling to Improve Performance
================================

Sometimes you may want to profile your code to see where it is spending the most time and improve performance.

We recommend using the `yappi` library to profile your code. You can install it with the following command in your terminal:

.. code-block:: bash

    pip install yappi

Once installed, you can use `yappi` to profile your code like this:

.. code-block:: python

    import yappi

    # Start the profiler
    yappi.start()

    #######
    # Run your code here, eg. a backtest
    #######
    MachineLearningLongShort.run_backtest(
        PandasDataBacktesting,
        backtesting_start,
        backtesting_end,
        pandas_data=pandas_data,
        benchmark_asset="TQQQ",
    )

    # Stop the profiler
    yappi.stop()

    # Save the results to files
    yappi.get_func_stats().print_all()
    yappi.get_thread_stats().print_all()

    # Save the results to a file
    yappi.get_func_stats().save("yappi.prof", type="pstat")

To get the results of the profiling, you can use snakeviz to visualize the results. You can install snakeviz with the following command in your terminal:

.. code-block:: bash

    pip install snakeviz

Once installed, you can use snakeviz to visualize the results like this:

.. code-block:: bash

    snakeviz yappi.prof

This will open a web browser with a visualization of the profiling results.

.. note::

   **Profiling can slow down your code**, so it is recommended to only use it when you need to.

.. note::

   **Profiling can be complex**, so it is recommended to read the `yappi documentation <https://yappi.readthedocs.io/en/latest/>`__.

Frequently Asked Questions
==========================

**What is the fastest way to test a strategy?**

Use Yahoo Finance backtesting -- it's free and requires no API keys. Import ``YahooDataBacktesting``, set a date range, and call ``.backtest()``. See the example at the top of this page.

**Do I need a broker account to get started?**

No. You can backtest strategies using free data from Yahoo Finance without any broker account. You only need a broker when you're ready to paper trade or go live. Alpaca offers free paper trading accounts.

**Can I build an AI-powered trading strategy?**

Yes! LumiBot supports AI trading agents that use LLMs to make decisions on every bar. Create an agent in ``initialize()``, run it from ``on_trading_iteration()``, and it can call external tools, analyze data with DuckDB, and submit orders. The same code works for backtesting and live trading. See :doc:`agents` for the full guide.

**Why must I use ``self.get_datetime()`` instead of ``datetime.now()``?**

During backtesting, ``datetime.now()`` returns the real current time, not the simulated historical time. This will make your strategy think it's in the present when it's actually replaying historical data. Always use ``self.get_datetime()`` -- it works correctly in both backtesting and live trading.

**Where can I find more help?**

Check the :doc:`faq` for 70+ answered questions covering backtesting, brokers, AI agents, options, crypto, and more. Join the `Discord Community <https://discord.gg/v6asVjTCvh>`_ for live help from other LumiBot users.
