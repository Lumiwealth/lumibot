Backtesting
************************

Lumibot has three modes for backtesting.

#. Yahoo Backtesting: Daily stock backtesting with data from Yahoo.
#. Pandas Backtesting: Intra-day and inter-day testing of stocks and futures using CSV data supplied by you.
#. Polygon Backtesting: Intra-day and inter-day testing of stocks and futures using Polygon data from polygon.io

It is recommended to use the Yahoo Backtesting for daily stock backtesting, or Polygon Backtesting for intra-day and inter-day testing of stocks, options, crypto and FOREX. Pandas Backtesting is an advanced feature that allows you to test any type of data you have in CSV format but requires more work to setup and is not recommended for most users.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   backtesting.all
   backtesting.yahoo
   backtesting.pandas
   backtesting.polygon