Pandas (CSV or other data)
===================================

Pandas backtester is named after the python dataframe library because the user must provide a strictly formatted dataframe. You can have any csv or parquet or database data you wish, but Lumibot will only accept one format of dataframe for the time being.

Pandas backtester allows for intra-day and inter-day backtesting. Time frames for raw data are 1 minute and 1 day. Resampling of time frames is not yet available.

Additionally, with Pandas backtester, it is possible to backtest stocks, stock-like securities, and futures contracts. Options will be coming shortly and Forex is tagged for future development.

Start by importing the Pandas backtester as follows:

.. code-block:: python

    from lumibot.backtesting import PandasDataBacktesting

The start and end dates for the backtest will be set using datetime.datetime as follows:

.. code-block:: python

    backtesting_start = datetime.datetime(2021, 1, 8)
    backtesting_end = datetime.datetime(2021, 3, 10)

Lumibot will start trading at 0000 hrs for the first date and up to 2359 hrs for the last. This is considered to be in the default time zone of Lumibot unless changed. This is America/New York (aka: EST)

Trading hours are set using ``datetime.time``. You can restrict the trading times in the day. If your data includes a reduced workday, say a holiday Friday, Lumibot will shorten the day to match the data. Following is an example of setting the day:

.. code-block:: python

    trading_hours_start = datetime.time(9, 30)
    trading_hours_end = datetime.time(16, 0)

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
   :header: "datetime (the DataFrame.index)", "open", "high", "low", "close", "volume"

    2020-01-02 09:31:00,	3237.00,	3234.75,	3235.25,	3237.00,	16808
    2020-01-02 09:32:00,	3237.00,	3234.00,	3237.00,	3234.75,	10439
    2020-01-02 09:33:00,	3235.50,	3233.75,	3234.50,	3234.75,	8203
    ...,	...,	...,	...,	...,	...
    2020-04-22 15:56:00,	2800.75,	2796.25,	2800.75,	2796.25,	8272
    2020-04-22 15:57:00,	2796.50,	2794.00,	2796.25,	2794.00,	7440
    2020-04-22 15:58:00,	2794.75,	2793.00,	2794.25,	2793.25,	7569

Any other formats for dataframes will not work.

Every asset with data will have its own Data object to keep track of the information needed for backtesting that asset. Each Data has the following parameters:

.. code-block:: python

    """
    Parameters
    ----------
    strategy : str
        Name of the current strategy object.
    asset : Asset Object
        Asset object to which this data is attached.
    df : dataframe
        Pandas dataframe containing OHLCV etc trade data. Loaded by user
        from csv.
        Index is date and must be pandas datetime64.
        Columns are strictly ["open", "high", "low", "close", "volume"]
    date_start : Datetime or None
        Starting date for this data, if not provided then first date in
        the dataframe.
    date_end : Datetime or None
        Ending date for this data, if not provided then last date in
        the dataframe.
    trading_hours_start : datetime.time or None
        If not supplied, then default is 0001 hrs.
    trading_hours_end : datetime.time or None
        If not supplied, then default is 2359 hrs.
    timestep : str
        Either "minute" (default) or "day"
    columns : list of str
        For feeding in desired columns (not yet used)."""

The data objects will be collected in a dictionary called ``pandas_data`` using the asset as key and the data object as value. Subsequent assets + data can be added and then the dictionary can be passed into Lumibot for backtesting.

One of the important differences when using Pandas backtester is that you must use an ``Asset`` object for each data csv file loaded. You may not use a ``symbol`` as you might in Yahoo backtester. For an example, let's assume we have futures data for the ES mini. First step would be to create an asset object:

.. code-block:: python

    asset = Asset(
        symbol="AAPL",
        asset_type="stock",
    )

Next step will be to load the dataframe from csv.

.. code-block:: python

    # The names of the columns are important. Also important that all dates in the 
    # dataframe are time aware before going into lumibot. 
    df = pd.read_csv(f"data/AAPL.csv")
    df = df.set_index("time")
    df.index = pd.to_datetime(df.index)

Third we make a data object.

.. code-block:: python

    data = Data(
        asset,
        df,
        timestep="minute",
    )

When dealing with futures contracts, it is possible to run into some conflicts with the amount of data available and the expiry date of the contract. Should you hold a position with the contract expires, the position will be closed on the last date of trading. If you hold a position and there is no data for pricing, Lumibot will throw an error since it has no data to value the position.

Finally, we create or add to the dictionary that will be passed into Lumibot.

.. code-block:: python

    pandas_data = {
        asset: data
    }

As with Yahoo backtester, data is passed in by using ``.backtest()`` on your strategy class.

There is a logging function that will save the details of your backtest (the portfolio value each day, cash, etc) put into a CSV file in the location of ``stats_file``.

There is also a returns plot. By default this will show in a browser. You may suppress it using ``show_plot=False``

.. code-block:: python

    strategy_class.backtest(
            PandasDataBacktesting,
            backtesting_start,
            backtesting_end,
            pandas_data=pandas_data,
            budget=100000,
        )

Putting all of this together, and adding in budget and strategy information, the code would look like the following:

.. code-block:: python

    import datetime

    import pandas as pd
    from lumibot.backtesting import PandasDataBacktesting
    from lumibot.entities import Asset, Data
    from lumibot.strategies import Strategy


    # A simple strategy that buys SPY on the first day
    class MyStrategy(Strategy):
        def on_trading_iteration(self):
            if self.first_iteration:
                order = self.create_order("AAPL", 100, "buy")
                self.submit_order(order)

    # Read the data from the CSV file (in this example you must have a file named "AAPL.csv"
    # in a folder named "data" in the same directory as this script)
    df = pd.read_csv(f"data/AAPL.csv")
    df = df.set_index("time")
    df.index = pd.to_datetime(df.index)
    asset = Asset(
        symbol="AAPL",
        asset_type="stock",
    )
    pandas_data = {}
    pandas_data[asset] = Data(
        asset,
        df,
        timestep="minute",
    )

    # Pick the date range you want to backtest
    backtesting_start = datetime.datetime(2021, 7, 2)
    backtesting_end = datetime.datetime(2021, 7, 20)

    # Run the backtesting
    MyStrategy.backtest(
        PandasDataBacktesting,
        backtesting_start,
        backtesting_end,
        pandas_data=pandas_data,
    )

Getting Data
----------------

If you would like an easy way to download pricing data from Alpaca then you can use this code: https://github.com/Lumiwealth/lumibot/blob/master/download_price_data_alpaca.py