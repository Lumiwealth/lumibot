How To Backtest
===================================

Backtesting is a vital step in validating your trading strategies using historical data. With LumiBot, you can backtest strategies across various data sources such as **Yahoo Finance**, **Polygon.io**, **ThetaData**, or even your own custom **CSV** files. This guide will walk you through each step of backtesting, explain the data sources, and introduce the files that LumiBot generates during backtesting.

.. note::

   **Why Backtest?**
   
   Backtesting allows you to see how your strategies would have performed in the past, helping you identify weaknesses or strengths before deploying them in live markets.

Installing LumiBot
-----------------------------------

Before you begin, make sure LumiBot is installed on your machine. You can install LumiBot using the following command:

.. code-block:: bash

    pip install lumibot

To upgrade to the latest version of LumiBot, run:

.. code-block:: bash

    pip install lumibot --upgrade

Once installed, you can use an IDE like **Visual Studio Code (VS Code)** or **PyCharm** to write and test your code.

.. tip::

   **Quick Setup for VS Code**
   
   1. Download and install **Visual Studio Code** from the official website: https://code.visualstudio.com/.
   2. Open VS Code and install the Python extension by going to **Extensions** and searching for **Python**.
   3. Create a new project folder for LumiBot.
   4. Open a terminal in VS Code and install LumiBot using `pip install lumibot`.
   5. You're ready to start backtesting with LumiBot!

Choosing a Data Source
-----------------------------------

LumiBot supports several data sources for backtesting, each suited for different asset types and backtesting needs. Here's an overview of the available sources:

**1. Yahoo Finance**

- Free stock and ETF data for daily trading backtests.
- Suitable for longer-term strategies but not ideal for intraday backtesting.

For more details, see the :ref:`Yahoo Backtesting <backtesting.yahoo>` section.

**2. Polygon.io**

- Offers intraday and end-of-day data for stocks, options, forex, and cryptocurrency.
- Provides up to two years of free data; paid plans offer more advanced features and faster data retrieval.

.. important::

   **Get Your API Key from Polygon.io**
   
   You can get an API key at `Polygon.io <https://polygon.io/?utm_source=affiliate&utm_campaign=lumi10>`_. **Please use the coupon code 'LUMI10' for 10% off!**

For more details, see the :ref:`Polygon.io Backtesting <backtesting.polygon>` section.

**3. ThetaData**

- Designed for users looking to backtest stock and options trading strategies.
- Provides options pricing and other securities.

.. important::

   **Get Your ThetaData Account**
   
   You can get a username and password at `thetadata.net <https://www.thetadata.net/>`_. **Please use the coupon code 'LUMI' for 10% off!**

For more details, see the :ref:`ThetaData Backtesting <backtesting.thetadata>` section.

**4. Pandas (CSV or Other Custom Data)**

- Allows for full flexibility by using your own custom datasets (e.g., CSV, database exports).
- Ideal for advanced users but requires more manual configuration.

For more details, see the :ref:`Pandas Backtesting <backtesting.pandas>` section.

Running a Backtest with Polygon.io
-----------------------------------

Once you've selected your data source and built your strategy, you can run your backtest using the `run_backtest` function. This function requires:

- **Data source**: (e.g., Yahoo, Polygon.io, ThetaData, or Pandas)
- **Start and end dates**: The period you want to test.
- **Additional parameters**: Strategy-specific parameters.

Here's an example of a backtest using **Polygon.io**:

.. important::

   **You Must Use Your Polygon.io API Key**

   Make sure to replace `"YOUR_POLYGON_API_KEY"` with your actual API key from Polygon.io for the backtest to work.

.. code-block:: python

    from datetime import datetime
    from lumibot.backtesting import PolygonDataBacktesting
    from lumibot.strategies import Strategy

    class MyStrategy(Strategy):
        parameters = {
            "symbol": "AAPL",
        }

        def initialize(self):
            self.sleeptime = "1D"  # Sleep for 1 day between iterations

        def on_trading_iteration(self):
            if self.first_iteration:
                symbol = self.parameters["symbol"]
                price = self.get_last_price(symbol)
                qty = self.portfolio_value / price
                order = self.create_order(symbol, quantity=qty, side="buy")
                self.submit_order(order)

    if __name__ == "__main__":
        polygon_api_key = "YOUR_POLYGON_API_KEY"  # Replace with your actual Polygon.io API key
        backtesting_start = datetime(2023, 1, 1)
        backtesting_end = datetime(2023, 5, 1)
        result = MyStrategy.run_backtest(
            PolygonDataBacktesting,
            backtesting_start,
            backtesting_end,
            benchmark_asset="SPY",
            polygon_api_key=polygon_api_key  # Pass the Polygon.io API key here
        )

For more information about running backtests, refer to the :ref:`Backtesting Function <backtesting.backtesting_function>` section.

Files Generated from Backtesting
===================================

When you run a backtest in LumiBot, several important files are generated. These files provide detailed insights into the performance and behavior of the strategy. Each file is crucial for understanding different aspects of your strategy's execution.

Tearsheet HTML
-----------------------------------

LumiBot generates a detailed **Tearsheet HTML** file that provides visual and statistical analysis of your strategy’s performance. The tear sheet includes:

- Equity curve
- Performance metrics (e.g., Sharpe ratio, drawdown)
- Benchmark comparisons

For more information, see the :ref:`Tearsheet HTML <backtesting.tearsheet_html>` section.

Trades Files
-----------------------------------

The **Trades File** logs every trade executed during the backtest, including:

- The asset traded
- The quantity of the trade
- The price at which the trade was executed
- The timestamp of the trade

This file is essential for reviewing your strategy's trading behavior and identifying any potential issues or optimizations.

For more information on how to interpret the **Trades Files**, see the :ref:`Trades Files <backtesting.trades_files>` section.

Indicators Files
-----------------------------------

The **Indicators File** logs any technical indicators used in your strategy, such as moving averages, RSI, or custom indicators. This file helps you understand how your strategy responded to market conditions based on specific indicators.

For more information, see the :ref:`Indicators Files <backtesting.indicators_files>` section.

Conclusion
-----------------------------------

LumiBot’s backtesting feature provides a powerful framework for validating your strategies across multiple data sources. By following this guide, you can quickly set up your environment, choose a data source, and begin backtesting with confidence.

For further details on each data source and the files generated during backtesting, refer to the individual sections listed above.
