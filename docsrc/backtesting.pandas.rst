Pandas (CSV or other data)
===================================

**NOTE: Please ensure you have installed the latest lumibot version using ``pip install lumibot --upgrade`` before proceeding as there have been some major changes to the backtesting module in the latest version.**

**For most situations, you will want to use the Polygon backtester or the Yahoo backtester instead, they are much easier to use and get started with. The Pandas backtester is intended for advanced users who have their own data and want to use it with Lumibot.**

Pandas backtester is named after the python dataframe library because the user must provide a strictly formatted dataframe. You can use any csv, parquet, database data, etc that you wish, but Lumibot will only accept one format of dataframe.

Pandas backtester allows for intra-day and inter-day backtesting. Time frames for raw data are 1 minute and 1 day. 

Additionally, with Pandas backtester, it is possible to backtest stocks, stock-like securities, futures contracts, crypto and FOREX. 

Pandas backtester is the most flexible backtester in Lumibot, but it is also the most difficult to use. It is intended for advanced users who have their own data and want to use it with Lumibot.

Start by importing the Pandas backtester as follows:

.. code-block:: python

    from lumibot.backtesting import PandasDataBacktesting, BacktestingBroker

Next, create your Strategy class as you normally would. You can use any of the built-in indicators or create your own. You can also use any of the built-in order types or create your own.

.. code-block:: python

    from lumibot.strategies import Strategy

    class MyStrategy(Strategy):
        def on_trading_iteration(self):
            # Do something here

Lumibot will start trading at 0000 hrs for the first date and up to 2359 hrs for the last. This is considered to be in the default time zone of Lumibot unless changed. This is America/New York (aka: EST)

Pandas backtester will receive a dataframe in the following format:

.. code-block:: python

    Index: 
    name: datetime
    type: datetime64

    Columns: 
    names: ['open', 'high', 'low', 'close', 'volume']
    types: float

Your dataframe should look like this:

.. csv-table:: Example Dataframe
   :header: "datetime", "open", "high", "low", "close", "volume"

    2020-01-02 09:31:00,	3237.00,	3234.75,	3235.25,	3237.00,	16808
    2020-01-02 09:32:00,	3237.00,	3234.00,	3237.00,	3234.75,	10439
    2020-01-02 09:33:00,	3235.50,	3233.75,	3234.50,	3234.75,	8203
    ...,	...,	...,	...,	...,	...
    2020-04-22 15:56:00,	2800.75,	2796.25,	2800.75,	2796.25,	8272
    2020-04-22 15:57:00,	2796.50,	2794.00,	2796.25,	2794.00,	7440
    2020-04-22 15:58:00,	2794.75,	2793.00,	2794.25,	2793.25,	7569

Other formats for dataframes will not work.

You can download an example CSV using the yfinance library as follows:

.. code-block:: python

    import yfinance as yf

    # Download minute data for the last 5 days for AAPL
    data = yf.download("AAPL", period="5d", interval="1m")

    # Save the data to a CSV file
    data.to_csv("AAPL.csv")

The data objects will be collected in a dictionary called ``pandas_data`` using the asset as key and the data object as value. Subsequent assets + data can be added and then the dictionary can be passed into Lumibot for backtesting.

One of the important differences when using Pandas backtester is that you must use an ``Asset`` object for each data csv file loaded. You may not use a ``symbol`` as you might in Yahoo backtester.

For example, if you have a CSV file for AAPL, you must create an ``Asset`` object for AAPL and then pass that into the ``Data`` object.

.. code-block:: python

    from lumibot.entities import Asset

    asset = Asset(
        symbol="AAPL",
        asset_type=Asset.AssetType.STOCK,
    )

Next step will be to load the dataframe from csv.

.. code-block:: python

    import pandas as pd

    # The names of the columns are important. Also important that all dates in the 
    # dataframe are time aware before going into lumibot. 
    df = pd.read_csv("AAPL.csv")

Third we make a data object for the asset. The data object must have at least the asset object, the dataframe, and the timestep. The timestep can be either ``minute`` or ``day``. If you are using minute data, you must have a ``minute`` timestep. If you are using daily data, you must have a ``day`` timestep.

.. code-block:: python

    from lumibot.entities import Data

    data = Data(
        asset,
        df,
        timestep="minute",
    )

Next, we create or add to the dictionary that will be passed into Lumibot.

.. code-block:: python

    pandas_data = {
        asset: data
    }

Finally, we can pass the ``pandas_data`` dictionary into Lumibot and run the backtest.

.. code-block:: python

    # Run the backtesting
    trader = Trader(backtest=True)
    data_source = PandasDataBacktesting(
        pandas_data=pandas_data,
        datetime_start=backtesting_start,
        datetime_end=backtesting_end,
    )
    broker = BacktestingBroker(data_source)
    strat = MyStrategy(
        broker=broker,
        budget=100000,
    )
    trader.add_strategy(strat)
    trader.run_all()

In Summary
----------

Putting all of this together, and adding in budget and strategy information, the code would look like the following:

Getting the data would look something like this (this is using yfinance to download the data, but you can use any data source you wish):

.. code-block:: python

    import yfinance as yf

    # Download minute data for the last 5 days for AAPL
    data = yf.download("AAPL", period="5d", interval="1m")

    # Save the data to a CSV file
    data.to_csv("AAPL.csv")

Then, the startegy and backtesting code would look something like this:

.. code-block:: python

    import pandas as pd
    from lumibot.backtesting import BacktestingBroker, PandasDataBacktesting
    from lumibot.entities import Asset, Data
    from lumibot.strategies import Strategy
    from lumibot.traders import Trader


    # A simple strategy that buys SPY on the first day
    class MyStrategy(Strategy):
        def on_trading_iteration(self):
            if self.first_iteration:
                order = self.create_order("AAPL", 100, "buy")
                self.submit_order(order)


    # Read the data from the CSV file (in this example you must have a file named "AAPL.csv"
    # in a folder named "data" in the same directory as this script)
    df = pd.read_csv("AAPL.csv")
    asset = Asset(
        symbol="AAPL",
        asset_type=Asset.AssetType.STOCK,
    )
    pandas_data = {}
    pandas_data[asset] = Data(
        asset,
        df,
        timestep="minute",
    )

    # Pick the date range you want to backtest
    backtesting_start = pandas_data[asset].datetime_start
    backtesting_end = pandas_data[asset].datetime_end

    # Run the backtesting
    trader = Trader(backtest=True)
    data_source = PandasDataBacktesting(
        pandas_data=pandas_data,
        datetime_start=backtesting_start,
        datetime_end=backtesting_end,
    )
    broker = BacktestingBroker(data_source)
    strat = MyStrategy(
        broker=broker,
        budget=100000,
    )
    trader.add_strategy(strat)
    trader.run_all()

