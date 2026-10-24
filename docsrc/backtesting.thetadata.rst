.. _backtesting.thetadata:

ThetaData Options and Stock Backtesting with LumiBot
====================================================

.. meta::
   :description: Backtest options, stocks, indexes, forex, and crypto with ThetaData and LumiBot using minute, hourly, or daily historical data.

.. include:: _snippets/botspot_mcp_callout.rst

.. important::
   
   **Sign up at** `ThetaData <https://www.thetadata.net/>`_. **Use the promo code ``BotSpot10`` at checkout for 10% off the first order—ThetaData tracks the code so they can credit BotSpot for the referral.**

ThetaData backtester allows for flexible and robust backtesting. It uses the thetadata API to fetch pricing data for stocks, options, forex, and cryptocurrencies. This backtester simplifies the process of getting pricing data; simply use the thetadata DataSource and it will automatically fetch pricing data when you call `get_last_price()` or `get_historical_prices()`.

Supported timesteps:

- ``"minute"`` and ``"day"`` are supported directly.
- ``"hour"`` (and multi-hour like ``"4h"``) is supported when minute bars are available; LumiBot aggregates minute bars into hourly bars for you.

Use this data source with the :doc:`AI iron condor <agents_example_iron_condor_ai_trading_bot>`
options tutorial or the :doc:`AI opening range breakout
<agents_example_opening_range_breakout_ai_trading_bot>` intraday stock tutorial.

As of this writing, ThetaData provides historical data for free. If you pay for an API you can get many years of data and the backtesting will download data much faster because it won't be rate limited.

This backtesting method caches the data on your computer making it faster for subsequent backtests. So even if it takes a bit of time the first time, the following backtests will be much faster.

Session close coverage (index/stock minute bars)
------------------------------------------------

For some assets, minute-bar feeds are **regular-session (RTH) bounded** (for example, U.S. indexes such as SPX typically have bars from ~09:30 to ~16:00 ET).

In those cases, LumiBot treats **coverage through the last trading session close at or before the requested end** as “complete” for backtest cache reuse (holiday/weekend/early-close safe). This prevents pathological behavior where a backtest end date represented as midnight (or UTC-midnight) would otherwise imply a requirement for bars through ``23:59`` even though the provider does not publish them.

Options pricing and mark-to-market (important)
----------------------------------------------

Options can be illiquid: they may not trade every day, so the **last traded price** can be stale. In live brokers, mark-to-market and many fill models rely on **NBBO quotes** (bid/ask) when available.

LumiBot follows that model:

- ``get_last_price()`` is trade-derived (last trade / bar close) and can be stale for options.
- ``get_quote()`` exposes bid/ask and allows LumiBot to derive a **mark** (mid) for option valuation and quote-based fills.

In some historical option series, ThetaData can have gaps in **EOD/day** option history even when **intraday quote history** exists. In daily-cadence backtests, LumiBot may fall back to an intraday quote snapshot to compute an option mark when day/EOD pricing is unavailable. This avoids “unpriceable” option positions and keeps daily option backtests broker-like.

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
     - (Optional) Read only by startup code that checks it. It does not change a ``backtest()`` call into a broker run; see :doc:`strategy_run_modes`.
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

Frequently asked questions
--------------------------

Is there a ThetaData promo code?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes. Use the promo code ``BotSpot10`` at checkout on `thetadata.net <https://www.thetadata.net/>`__ for 10% off the first order. ThetaData tracks the code so it can credit BotSpot for the referral.

Does ThetaData have a free plan?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Check the `ThetaData pricing page <https://www.thetadata.net/pricing>`__ for current plans. At the time of writing it lists the Options Value, Options Standard and Options Pro monthly plans.

Paid plans give you more years of history, and backtests download faster because requests are not rate limited.

What do I need to run a ThetaData backtest in LumiBot?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You need a ThetaData account and its login, set as ``THETADATA_USERNAME`` and ``THETADATA_PASSWORD``. You also need Java 21 or higher, because ThetaData's `Theta Terminal <https://thetadata.net/docs/Articles/Getting-Started/Getting-Started.html>`__ runs as a local server on your computer.

LumiBot starts and manages the local Theta Terminal for you. If you set ``DATADOWNLOADER_BASE_URL``, LumiBot uses that Data Downloader instead and does not start a local terminal.

Which timeframes does ThetaData support in LumiBot?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``"minute"`` and ``"day"`` bars are supported directly. ``"hour"`` and multi hour bars such as ``"4h"`` work when minute bars are available, because LumiBot builds them from minute bars.

Which assets can I backtest with ThetaData?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ThetaData backtester covers stocks, options, indexes such as SPX, forex and crypto, as described at the top of this page.

How do I select ThetaData without changing my code?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Set ``BACKTESTING_DATA_SOURCE=thetadata``. This setting wins over the data source class in your code. ThetaData is also LumiBot's default when you pass no class and set no environment variable.

Can I use ThetaData for options and another source for stocks?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes. Pass a dictionary such as ``{"STOCK": PolygonDataBacktesting, "OPTION": ThetaDataBacktesting}`` as the first argument to ``run_backtest()``, as shown above. You can also route by asset type with a JSON map in ``BACKTESTING_DATA_SOURCE`` (see :doc:`backtesting.ibkr`).

Why does my option price look stale?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Options can go a whole day without a trade, so ``get_last_price()`` may return an old trade price. Use ``get_quote()`` to get the bid and ask. LumiBot uses the quote midpoint to value options, like most live brokers do.

Where does LumiBot cache ThetaData downloads?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In the ``thetadata`` folder inside ``LUMIBOT_CACHE_FOLDER``. When that variable is not set, LumiBot uses your user cache folder. The first backtest can take a while, and later runs over the same data are much faster.
