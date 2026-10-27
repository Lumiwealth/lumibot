Backtesting Data Sources
========================

.. meta::
   :description: You do not need a broker to backtest with LumiBot. Compare Yahoo, ThetaData, Polygon, Databento, Interactive Brokers, Alpaca, Polymarket and your own CSV data.

You do not need a broker account to backtest. LumiBot backtests run on historical data from one of these sources. Pick the one that covers your assets and bar size.

.. include:: _snippets/botspot_mcp_callout.rst

.. list-table:: Backtesting data sources
   :header-rows: 1
   :widths: 26 40 34

   * - Data source
     - Good for
     - Setup
   * - :doc:`Yahoo <backtesting.yahoo>`
     - Daily stocks and ETFs
     - No account or key
   * - :doc:`ThetaData <backtesting.thetadata>`
     - Intraday stocks and options
     - ThetaData subscription
   * - :doc:`Polygon.io <backtesting.polygon>`
     - Stocks, options, forex and crypto
     - Polygon.io API key
   * - :doc:`Databento <backtesting.databento>`
     - Futures
     - Databento API key
   * - :doc:`Interactive Brokers <backtesting.ibkr>`
     - Interactive Brokers history
     - Client Portal access
   * - :doc:`Alpaca <backtesting.alpaca>`
     - Stocks and options with your Alpaca account
     - Alpaca API key
   * - :doc:`Pandas <backtesting.pandas>`
     - Your own CSV or DataFrame data
     - Your files
   * - :doc:`Polymarket <brokers.polymarket>`
     - Prediction contract prices
     - Polymarket market ids
   * - :doc:`CCXT <brokers.ccxt>`
     - Spot crypto candles
     - Exchange id

Choose the source with ``BACKTESTING_DATA_SOURCE`` or pass a data source class in Python. The full guide is :doc:`backtesting`.

Frequently asked questions
--------------------------

Do I need a broker to backtest?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No. Backtests use historical data only. Yahoo daily data needs no account and no key.

Which data source should I start with?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yahoo, for daily stock and ETF strategies. For intraday stocks or options, use ThetaData, Polygon or Alpaca. For futures, use Databento or Interactive Brokers.

How do I pick the data source without changing my code?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Set ``BACKTESTING_DATA_SOURCE`` in your ``.env`` file. The environment setting takes precedence over the data source class in code.

Do backtest results tell me how a strategy will do live?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No. A backtest is a simulation on past data. Fills, fees and market conditions can differ in live trading.

Can I use my own CSV data?
~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes. Load it into a pandas DataFrame and use the Pandas backtester. See :doc:`backtesting.pandas`.
