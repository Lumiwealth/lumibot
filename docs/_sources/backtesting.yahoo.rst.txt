Yahoo
************************

Yahoo backtesting is so named because we get data for the backtesting from the Yahoo Finance website. The user is not required to supply data. Any stock information that is available in the Yahoo Finance API should be available for backtesting. The Yahoo backtester is not used for futures, options, or forex. Additionally, you cannot use the Yahoo backtester for intra-day trading, daily trading only. For other securities, use the Pandas backtester.

Using Yahoo backtester, you can also run backtests very easily on your strategies, you do not have to modify anything in your strategies.

The backtesting module must be imported.

.. code-block:: python

    from lumibot.backtesting import YahooDataBacktesting

Simply call the ``backtest()`` function on your strategy class. The parameters you must define are included in the example below.

There is a logging function that will save the details of your backtest (the portfolio value each day, unspent money, etc) put into a CSV file in the location of ``stats_file.``

There is also a returns plot. By default this will show in a browser. You may suppress it using ``show_plot=False``

.. code-block:: python

    from lumibot.backtesting import YahooDataBacktesting
    from my_strategy import MyStrategy

    from datetime import datetime

    # Pick the dates that you want to start and end your backtest
    # and the allocated budget
    backtesting_start = datetime(2020, 1, 1)
    backtesting_end = datetime(2020, 12, 31)
    budget = 100000

    # Run the backtest
    stats_file = "logs/my_strategy_backtest.csv"
    plot_file = f"logs/my_strategy_backtest.jpg"
    MyStrategy.backtest(
        "my_strategy",
        budget,
        YahooDataBacktesting,
        backtesting_start,
        backtesting_end,
        stats_file=stats_file,
        plot_file=plot_file,
        show_plot=True,
        benchmark_asset="SPY",
        
    )

Yahoo Backtesting
"""""""""""""""

.. automodule:: lumibot.backtesting.yahoo_backtesting
   :members:
   :undoc-members:
   :show-inheritance: