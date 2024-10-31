Lumibot: Backtesting and Algorithmic Trading Library
====================================================

**An Easy to Use and Powerful Backtesting and Trading Library for Crypto, Stocks, Options, Futures and FOREX**

.. raw:: html
   :file: _html/main.html

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

Table of Contents
*****************

.. toctree::
   :maxdepth: 3
   
   Home <self>
   GitHub <https://github.com/Lumiwealth/lumibot>
   Discord Community <https://discord.gg/v6asVjTCvh>
   Get Pre-Built Profitable Strategies <https://lumiwealth.com/?utm_source=documentation&utm_medium=referral&utm_campaign=lumibot_sidebar>
   deployment
   getting_started
   lifecycle_methods
   vars
   strategy_methods
   strategy_properties
   entities
   backtesting
   brokers

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`