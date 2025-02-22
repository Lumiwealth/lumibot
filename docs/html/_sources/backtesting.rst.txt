Backtesting
************************

Lumibot has three modes for backtesting:

1. **Yahoo Backtesting:** Daily stock backtesting with data from Yahoo.
2. **Pandas Backtesting:** Intra-day and inter-day testing of stocks and futures using CSV data supplied by you.
3. **Polygon Backtesting:** Intra-day and inter-day testing of stocks and futures using Polygon data from polygon.io.

It is recommended to use Yahoo Backtesting for daily stock backtesting, or Polygon Backtesting for intra-day and inter-day testing of stocks, options, crypto, and FOREX. Pandas Backtesting is an advanced feature that allows you to test any type of data you have in CSV format but requires more work to setup and is not recommended for most users.

Files Generated from Backtesting
================================

When you run a backtest, several important files are generated, each prefixed by the strategy name and the date. These files provide detailed insights into the performance and behavior of the strategy.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   backtesting.how_to_backtest
   backtesting.backtesting_function
   backtesting.yahoo
   backtesting.pandas
   backtesting.polygon
   backtesting.thetadata
   backtesting.tearsheet_html
   backtesting.trades_files
   backtesting.indicators_files
   backtesting.logs_csv