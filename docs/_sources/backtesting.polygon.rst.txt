Polygon.io Backtesting
===================================

.. important::
   
   **You can get an API key at** `Polygon.io <https://polygon.io/?utm_source=affiliate&utm_campaign=lumi10>`_. **Please use the full link to give us credit for the sale (https://polygon.io/?utm_source=affiliate&utm_campaign=lumi10), it helps support this project. You can use the coupon code 'LUMI10' for 10% off.**

Polygon.io backtester allows for flexible and robust backtesting. It uses the polygon.io API to fetch pricing data for stocks, options, forex, and cryptocurrencies. This backtester simplifies the process of getting pricing data; simply use the PolygonDataSource and it will automatically fetch pricing data when you call `get_last_price()` or `get_historical_prices()`.

As of this writing, polygon provides up to 2 years of historical data for free. If you pay for an API you can get many years of data and the backtesting will download data much faster because it won't be rate limited.

This backtesting method caches the data on your computer making it faster for subsequent backtests. So even if it takes a bit of time the first time, the following backtests will be much faster.

To use this feature, you need to obtain an API key from polygon.io, which is free and you can get in the Dashboard after you have created an account. You must then replace `YOUR_POLYGON_API_KEY` with your own key in the code.

Start by importing the PolygonDataBacktesting, BacktestingBroker and other necessary classes:

.. code-block:: python

    from datetime import datetime

    from lumibot.backtesting import BacktestingBroker, PolygonDataBacktesting
    from lumibot.strategies import Strategy
    from lumibot.traders import Trader

Next, create a strategy class that inherits from the Strategy class. This class will be used to define the strategy that will be backtested. In this example, we will create a simple strategy that buys a stock on the first iteration and holds it until the end of the backtest. The strategy will be initialized with a symbol parameter that will be used to determine which stock to buy. The initialize method will be used to set the sleeptime to 1 day. The on_trading_iteration method will be used to buy the stock on the first iteration. The strategy will be run from 2023-01-01 to 2023-05-01.

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

    backtesting_start = datetime.datetime(2023, 1, 1)
    backtesting_end = datetime.datetime(2023, 5, 1)

Finally, run the backtest:

.. code-block:: python

    result = MyStrategy.run_backtest(
        PolygonDataBacktesting,
        backtesting_start,
        backtesting_end,
        benchmark_asset="SPY")

Here's the full code:

**Make sure to replace YOUR_POLYGON_API_KEY with your own API key form polygon.io (it's free)**

.. code-block:: python

    from datetime import datetime

    from lumibot.backtesting import BacktestingBroker, PolygonDataBacktesting
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
        backtesting_start = datetime(2023, 1, 1)
        backtesting_end = datetime(2023, 5, 1)

        result = MyStrategy.run_backtest(
            PolygonDataBacktesting,
            backtesting_start,
            backtesting_end,
            benchmark_asset="SPY")

.. important::
   
   **You can get an API key at** `Polygon.io <https://polygon.io/?utm_source=affiliate&utm_campaign=lumi10>`_. **Please use the full link to give us credit for the sale (https://polygon.io/?utm_source=affiliate&utm_campaign=lumi10), it helps support this project. You can use the coupon code 'LUMI10' for 10% off.**

In summary, the polygon.io backtester is a powerful tool for fetching pricing data for backtesting various strategies. With its capability to cache data for faster subsequent backtesting and its easy integration with polygon.io API, it is a versatile choice for any backtesting needs.
