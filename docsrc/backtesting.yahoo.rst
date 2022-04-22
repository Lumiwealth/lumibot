Yahoo
===================================

Yahoo backtesting is so named because we get data for the backtesting from the Yahoo Finance website. The user is not required to supply data. Any stock information that is available in the Yahoo Finance API should be available for backtesting. The Yahoo backtester is not used for futures, options, or forex. Additionally, you cannot use the Yahoo backtester for intra-day trading, daily trading only. For other securities, use the Pandas backtester.

Using Yahoo backtester, you can also run backtests very easily on your strategies, you do not have to modify anything in your strategies.

The backtesting module must be imported.

.. code-block:: python

    from lumibot.backtesting import YahooDataBacktesting

Simply call the ``backtest()`` function on your strategy class. The parameters you must define are included in the example below.

There is a logging function that will save the details of your backtest (the portfolio value each day, unspent money, etc) put into a CSV file in the location of ``stats_file.``

There is also a returns plot. By default this will show in a browser. You may suppress it using ``show_plot=False``

.. code-block:: python

    from datetime import datetime

    from lumibot.backtesting import YahooDataBacktesting
    from lumibot.strategies import Strategy


    # A simple strategy that buys AAPL on the first day
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