Pandas (CSV or other data)
************************

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

                        high	low	open    close	volume
datetime					
2020-01-02 09:31:00	3237.00	3234.75	3235.25	3237.00	16808
2020-01-02 09:32:00	3237.00	3234.00	3237.00	3234.75	10439
2020-01-02 09:33:00	3235.50	3233.75	3234.50	3234.75	8203
2020-01-02 09:34:00	3238.00	3234.75	3234.75	3237.50	8664
2020-01-02 09:35:00	3238.25	3236.25	3237.25	3236.25	7889
...	...	...	...	...	...
2020-04-22 15:56:00	2800.75	2796.25	2800.75	2796.25	8272
2020-04-22 15:57:00	2796.50	2794.00	2796.25	2794.00	7440
2020-04-22 15:58:00	2794.75	2793.00	2794.25	2793.25	7569
2020-04-22 15:59:00	2793.50	2790.75	2793.50	2790.75	10601
2020-04-22 16:00:00	2791.00	2787.75	2790.75	2789.00	57342
30397 rows × 5 columns
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
        symbol='ES',
        asset_type="future",
        expiration=datetime.date(2021, 6, 11),
        multiplier=50,
    )

Next step will be to load the dataframe from csv.

.. code-block:: python

    # The names of the columns are important. Also important that all dates in the 
    # dataframe are time aware before going into lumibot. 
    df = pd.read_csv(
                f"es_data.csv",
                parse_dates=True,
                index_col=0,
                header=0,
                names=["datetime", "high", "low", "open", "close", "volume"],
            )
            df = df[["open", "high", "low", "close", "volume"]]
            df.index = df.index.tz_localize("America/New_York")

Third we make a data object.

.. code-block:: python

    data = Data(
        asset,
        df,
        date_start=datetime.datetime(2021, 3, 14), 
        date_end=datetime.datetime(2021, 6, 11), 
        trading_hours_start=datetime.time(9, 30),
        trading_hours_end=datetime.time(16, 0),
        timestep="minute",
    )

When dealing with futures contracts, it is possible to run into some conflicts with the amount of data available and the expiry date of the contract. Should you hold a position with the contract expires, the position will be closed on the last date of trading. If you hold a position and there is no data for pricing, Lumibot will throw an error since it has no data to value the position.

Finally, we create or add to the dictionary that will be passed into Lumibot.

.. code-block:: python

    pandas_data = dict(
        asset = data
    )

Create a path to save your stats to:

.. code-block:: python

    stats_file = f"logs/strategy_{strategy_class.__name__}_{int(time())}.csv"

As with Yahoo backtester, data is passed in by using ``.backtest()`` on your strategy class.

There is a logging function that will save the details of your backtest (the portfolio value each day, unspent money, etc) put into a CSV file in the location of ``stats_file``.

There is also a returns plot. By default this will show in a browser. You may suppress it using ``show_plot=False``

.. code-block:: python

    strategy_class.backtest(
            "strategy_name",
            budget,
            PandasDataBacktesting,
            backtesting_start,
            backtesting_end,
            pandas_data=pandas_data,
            stats_file=stats_file, 
            plot_file=plot_file,
            show_plot=True,
            benchmark_asset="SPY",
        )

Putting all of this together, and adding in budget and strategy information, the code would look like the following:

.. code-block:: python

    from lumibot.backtesting import PandasDataBacktesting

    strategy_name = "Futures"
    strategy_class = Futures
    budget = 50000

    backtesting_start = datetime.datetime(2021, 1, 8)
    backtesting_end = datetime.datetime(2021, 3, 10)

    trading_hours_start = datetime.time(9, 30)
    trading_hours_end = datetime.time(16, 0)
    
    asset = Asset(
        symbol='ES',
        asset_type="future",
        expiration=datetime.date(2021, 6, 11),
        multiplier=50,
    )
    df = pd.read_csv(
        f"es_data.csv",
        parse_dates=True,
        index_col=0,
        header=0,
        names=["datetime", "high", "low", "open", "close", "volume"],
    )
    df = df[["open", "high", "low", "close", "volume"]]
    df.index = df.index.tz_localize("America/New_York")

    data = Data(
        asset,
        df,
        date_start=datetime.datetime(2021, 3, 14),
        date_end=datetime.datetime(2021, 6, 11),
        trading_hours_start=datetime.time(9, 30),
        trading_hours_end=datetime.time(16, 0),
        timestep="minute",
    )
    pandas_data = dict(
        asset=data
    )

    stats_file = f"logs/strategy_{strategy_class.__name__}_{int(time())}.csv"


    strategy_class.backtest(
        strategy_name,
        budget,
        PandasDataBacktesting,
        backtesting_start,
        backtesting_end,
        pandas_data=pandas_data,
        stats_file=stats_file,
    )

Documentation
""""""""""""""""""""""

.. automodule:: lumibot.backtesting.pandas_backtesting
   :members:
   :undoc-members:
   :show-inheritance: