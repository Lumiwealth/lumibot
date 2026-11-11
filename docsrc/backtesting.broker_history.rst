Your Broker's History (Schwab, Tradier)
=======================================

.. meta::
   :description: Backtest stocks and ETFs on the daily price history of your own Schwab or Tradier connection.

You can backtest on the price history your own broker keeps. Today this works for **Schwab** and **Tradier**, for **stocks and ETFs**, on **daily bars**.

How to use it
-------------

Set the data source by name, the same way as Alpaca:

.. code-block:: bash

   # Tradier
   BACKTESTING_DATA_SOURCE=tradier
   TRADIER_ACCESS_TOKEN=...
   TRADIER_ACCOUNT_NUMBER=...
   TRADIER_IS_PAPER=true

   # or Schwab
   BACKTESTING_DATA_SOURCE=schwab
   SCHWAB_APP_KEY=...
   SCHWAB_APP_SECRET=...
   SCHWAB_ACCOUNT_NUMBER=...

Or pass the class in code:

.. code-block:: python

   from lumibot.backtesting import TradierHistoryBacktesting

   MyStrategy.backtest(TradierHistoryBacktesting, backtesting_start, backtesting_end)

Use a daily ``sleeptime`` (for example ``"1D"``) and daily bars (``self.get_historical_prices(asset, 20, "day")``).

What to expect
--------------

- Each stock's daily bars are downloaded once for the backtest, then reused. A repeat backtest on the same connection reuses them from the cache.
- A daily bar becomes visible after that day's close, as with the Yahoo source, so there is no lookahead.
- Up to 120 requests per minute per broker. If the broker says you are over its limit, the backtest waits and retries, then stops with a clear message.

Limits
------

The backtest stops with a clear message, naming the limit, when you ask for something your broker's history does not have. It never switches to another data source and never fills in missing days.

- **Stocks and ETFs only.** Options, futures, crypto and indexes are not available here.
- **Daily bars only for now.** Brokers keep minute bars for a short time only (Tradier: 1-minute bars for 20 days; Schwab: about 48 days).
- **Schwab daily history starts in 1985.** Schwab refresh tokens expire after 7 days, so reconnect before a Schwab backtest if yours is older.

Dividends
---------

Schwab and Tradier prices are adjusted for stock splits, not for cash dividends, and **cash dividends are not added to your cash** in these backtests. For dividend-paying stocks the total return is lower than a total-return series would show.
