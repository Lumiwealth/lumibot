Polygon.io Backtesting
===================================

Polygon.io backtester allows for flexible and robust backtesting. It uses the polygon.io API to fetch pricing data for stocks, options, forex, and cryptocurrencies. This backtester simplifies the process of getting pricing data; simply use the PolygonDataSource and it will automatically fetch pricing data when you call `get_last_price()` or `get_historical_prices()`.

As of this writing, polygon provides up to 2 years of historical data for free. If you pay for an API you can get many years of data and the backtesting will download data much faster because it won't be rate limited.

This backtesting method caches the data on your computer making it faster for subsequent backtests. So even if it takes a bit of time the first time, the following backtests will be much faster.

To use this feature, you need to obtain an API key from polygon.io, which is free and you can get in the Dashboard after you have created an account. You must then replace `YOUR_POLYGON_API_KEY` with your own key in the code.

Start by importing the PolygonDataBacktesting as follows:

.. code-block:: python

    from backtesting import PolygonDataBacktesting

Set the start and end dates for the backtest:

.. code-block:: python

    backtesting_start = datetime.datetime(2023, 1, 1)
    backtesting_end = datetime.datetime(2023, 5, 1)


Optional: Set the quote asset (usually only required for crypto, default is USD) and the trading fee.

.. code-block:: python

    quote_asset = Asset(symbol="USDT", asset_type="crypto")
    trading_fee = TradingFee(percent_fee=0.001)

Finally, run the backtest:

.. code-block:: python

    CryptoEMACross.backtest(
        PolygonDataBacktesting,
        backtesting_start,
        backtesting_end,
        benchmark_asset=Asset(symbol="BTC", asset_type="crypto")
        quote_asset=quote_asset,
        buy_trading_fees=[trading_fee],
        sell_trading_fees=[trading_fee],
        polygon_api_key="YOUR_POLYGON_API_KEY",
        polygon_has_paid_subscription=False,
    )

Here's another example but for for stocks:

.. code-block:: python

    from datetime import datetime
    from lumibot.backtesting import PolygonDataBacktesting
    from lumibot.strategies import Strategy

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

        MyStrategy.backtest(
            PolygonDataBacktesting,
            backtesting_start,
            backtesting_end,
            benchmark_asset=Asset(symbol="SPY", asset_type="stock")
            polygon_api_key="YOUR_POLYGON_API_KEY",
            polygon_has_paid_subscription=False,
        )


In summary, the polygon.io backtester is a powerful tool for fetching pricing data for backtesting various strategies. With its capability to cache data for faster subsequent backtesting and its easy integration with polygon.io API, it is a versatile choice for any backtesting needs.

