Lumibot: Backtesting and Algorithmic Trading Library
=======================================================

**An Easy to Use and Powerful Backtesting and Trading Library for Crypto, Stocks, Options, Futures and FOREX**

.. raw:: html
   :file: _html/main.html

After you have installed Lumibot on your computer, you can then create a strategy and backtest it using free data available from Yahoo Finance, or use your own data (see the Backtesting section to get more details on this). Here's some code to get you started:

.. code-block:: python

    from datetime import datetime

    from lumibot.backtesting import YahooDataBacktesting
    from lumibot.strategies import Strategy


    # A simple strategy that buys AAPL on the first day and hold it
    class MyStrategy(Strategy):
        def on_trading_iteration(self):
            if self.first_iteration:
                aapl_price = self.get_last_price("AAPL")
                quantity = self.portfolio_value // aapl_price
                order = self.create_order("AAPL", quantity, "buy")
                self.submit_order(order)


    # Pick the dates that you want to start and end your backtest
    # and the allocated budget
    backtesting_start = datetime(2020, 11, 1)
    backtesting_end = datetime(2020, 12, 31)

    # Run the backtest
    MyStrategy.backtest(
        YahooDataBacktesting,
        backtesting_start,
        backtesting_end,
    )

Once you have backtested your strategy and found it to be profitable on historical data, you can then very easily take your bot live. Notice here how the strategy code is exactly the same, it only takes a few lines of code to switch from backtesting to live trading. Here's an example using Alpaca (you can create a free Paper Trading account here in minutes: https://alpaca.markets/)

.. code-block:: python

   from lumibot.backtesting import YahooDataBacktesting
   from lumibot.brokers import Alpaca
   from lumibot.strategies.strategy import Strategy
   from lumibot.traders import Trader

   ALPACA_CONFIG = {
        # Put your own Alpaca key here:
        "API_KEY": "YOUR_ALPACA_API_KEY",
        # Put your own Alpaca secret here:
        "API_SECRET": "YOUR_ALPACA_SECRET",
        # If you want to go live, you must change this. It is currently set for paper trading
        "ENDPOINT": "https://paper-api.alpaca.markets"
    }


   # A simple strategy that buys AAPL on the first day and hold it
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

And that's it! Easy-peasy.

If you would like to learn how to modify your strategies we suggest that you first learn about Lifecycle Methods, then Strategy Methods and Strategy Properties. You can find the documentation for these in the menu, with the main pages describing what they are, then the sub-pages describing each method and property individually.

We also have some more sample code that you can check out here: https://github.com/Lumiwealth/lumibot/tree/master/getting_started

We wish you good luck with your trading strategies, don't forget us when you're swimming in cash!

Need Extra Help?
--------------------

.. raw:: html
   :file: _html/course_list.html


Table of Contents
--------------------

.. toctree::
   :maxdepth: 3
   
   Home <self>
   Community/Forum <https://lumiwealth.circle.so/c/lumibot/>
   getting_started
   lifecycle_methods
   strategy_methods
   strategy_properties
   backtesting
   brokers
   entities


Indices and tables
====================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
