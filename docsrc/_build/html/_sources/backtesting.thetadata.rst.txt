ThetaData Backtesting
===================================

.. important::
   
   **You can get a username and password at** `thetadata.net <https://www.thetadata.net/>`_. **Please use the full link to give us credit for the sale (https://www.thetadata.net), it helps support this project. You can use the coupon code 'LUMI' for 10% off.**

ThetaData backtester allows for flexible and robust backtesting. It uses the thetadata API to fetch pricing data for stocks, options, forex, and cryptocurrencies. This backtester simplifies the process of getting pricing data; simply use the thetadata DataSource and it will automatically fetch pricing data when you call `get_last_price()` or `get_historical_prices()`.

As of this writing, ThetaData provides historical data for free. If you pay for an API you can get many years of data and the backtesting will download data much faster because it won't be rate limited.

This backtesting method caches the data on your computer making it faster for subsequent backtests. So even if it takes a bit of time the first time, the following backtests will be much faster.

To use this feature, you need to obtain an API key from thetadata, which is free and you can get in the Dashboard after you have created an account. You must then replace `username` and `password` with your own.

Start by importing the ThetaDataBacktesting, BacktestingBroker and other necessary classes:

.. code-block:: python

    from datetime import datetime

    from lumibot.backtesting import BacktestingBroker, ThetaDataBacktesting
    from lumibot.strategies import Strategy
    from lumibot.traders import Trader

Next, create a strategy class that inherits from the Strategy class. This class will be used to define the strategy that will be backtested. In this example, we will create a simple strategy that buys a stock on the first iteration and holds it until the end of the backtest. The strategy will be initialized with a symbol parameter that will be used to determine which stock to buy. The initialize method will be used to set the sleeptime to 1 day. The on_trading_iteration method will be used to buy the stock on the first iteration. The strategy will be run from 2025-01-01 to 2025-01-31.

.. code-block:: python
    
    class MyStrategy(Strategy):
        parameters = {
            "symbol": "AAPL",
        }

        def initialize(self):
            self.sleeptime = "1D"

        def on_trading_iteration(self):
            if self.first_iteration:
                symbol = self.parameters["symbol"]
                price = self.get_last_price(symbol)
                qty = self.portfolio_value / price
                order = self.create_order(symbol, quantity=qty, side="buy")
                self.submit_order(order)

Set the start and end dates for the backtest:

.. code-block:: python

    backtesting_start = datetime(2025, 1, 1)
    backtesting_end = datetime(2025, 1, 31)

Finally, run the backtest:

.. code-block:: python

    result = MyStrategy.run_backtest(
        ThetaDataBacktesting,
        backtesting_start,
        backtesting_end,
        benchmark_asset="SPY")

Alternatively, if you want to use polygon for stock data, and thetadata for option data, 
you can pass a dictionary with both data sources as the first argument to `run_backtest`. 
In this example, we are using thetadata for both stock and option data.

.. code-block:: python

    result = MyStrategy.run_backtest(
        {"STOCK":PolygonDataBacktesting,"OPTION":ThetaDataBacktesting},
        backtesting_start,
        backtesting_end,
        benchmark_asset="SPY")

Here's the full code (with explicit dates):
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Make sure to replace the `THETADATA_USERNAME` and `THETADATA_PASSWORD` (it's free)**

.. code-block:: python

    from datetime import datetime

    from lumibot.backtesting import BacktestingBroker, ThetaDataBacktesting
    from lumibot.strategies import Strategy
    from lumibot.traders import Trader

    class MyStrategy(Strategy):
        parameters = {
            "symbol": "AAPL",
        }

        def initialize(self):
            self.sleeptime = "1D"

        def on_trading_iteration(self):
            if self.first_iteration:
                symbol = self.parameters["symbol"]
                price = self.get_last_price(symbol)
                qty = self.portfolio_value / price
                order = self.create_order(symbol, quantity=qty, side="buy")
                self.submit_order(order)

    if __name__ == "__main__":
        backtesting_start = datetime(2025, 1, 1)
        backtesting_end = datetime(2025, 1, 31)
        result = MyStrategy.run_backtest(
            ThetaDataBacktesting,
            backtesting_start,
            backtesting_end,
            benchmark_asset="SPY"
        )

.. important::
   
   **You can get a username and password at** `thetadata.net <https://www.thetadata.net/>`_. **Please use the full link to give us credit for the sale (https://www.thetadata.net), it helps support this project. You can use the coupon code 'LUMI' for 10% off.**

Optional: Environment Variables
-------------------------------
Instead of specifying `backtesting_start` and `backtesting_end` in code, you can set these environment variables (along with `IS_BACKTESTING`). LumiBot will detect them automatically:

.. list-table::
   :header-rows: 1
   :widths: 20 60 20

   * - **Variable**
     - **Description**
     - **Example**
   * - IS_BACKTESTING
     - (Optional) **"True"** to enable backtesting, **"False"** for live trading.
     - False
   * - BACKTESTING_START
     - (Optional) Start date (YYYY-MM-DD).
     - 2025-01-01
   * - BACKTESTING_END
     - (Optional) End date (YYYY-MM-DD).
     - 2025-01-31

Below is **the full code** that relies *entirely on environment variables*:

.. code-block:: python

    from lumibot.backtesting import BacktestingBroker, ThetaDataBacktesting
    from lumibot.strategies import Strategy
    from lumibot.traders import Trader

    class MyStrategy(Strategy):
        parameters = {
            "symbol": "AAPL",
        }

        def initialize(self):
            self.sleeptime = "1D"

        def on_trading_iteration(self):
            if self.first_iteration:
                symbol = self.parameters["symbol"]
                price = self.get_last_price(symbol)
                qty = self.portfolio_value / price
                order = self.create_order(symbol, quantity=qty, side="buy")
                self.submit_order(order)

    if __name__ == "__main__":
        # No start/end in code. We rely on environment variables for backtesting dates.
        result = MyStrategy.run_backtest(
            ThetaDataBacktesting,
            benchmark_asset="SPY"
        )

In summary, the ThetaData backtester is a powerful tool for fetching pricing data for backtesting various strategies. With its capability to cache data for faster subsequent backtesting and its easy integration with thetadata API, it is a versatile choice for any backtesting needs.