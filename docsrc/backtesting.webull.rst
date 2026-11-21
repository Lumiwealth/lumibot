Backtest with Webull historical data
====================================

The Webull development candidate provides ``WebullHistoryBacktesting`` for
daily prices on US stocks, ETFs, exact listed option contracts and spot
crypto. It uses the existing
LumiBot backtesting broker and Webull historical endpoints. Adjusted equity
bars become available at the exchange close, including early closes. Spot
crypto daily bars become available when the Eastern trading date ends. Use
``set_market("24/7")`` and pass the base asset and quote currency to the same
strategy methods for crypto. The source keeps each loaded window in memory
for the run. Historical bid and ask and actual venue fills are separate
from daily-bar simulation. Webull PaperTrade can return crypto bars without
volume. Strategies that need volume should verify history coverage first.

.. code-block:: python

   from datetime import datetime
   from zoneinfo import ZoneInfo

   from lumibot.backtesting import WebullHistoryBacktesting
   from lumibot.strategies import Strategy

   class Hold(Strategy):
       def initialize(self):
           self.sleeptime = "1D"

       def on_trading_iteration(self):
           if self.get_position("AAPL") is None:
               self.submit_order(self.create_order("AAPL", 1, "buy"))

   ny = ZoneInfo("America/New_York")
   Hold.run_backtest(
       WebullHistoryBacktesting,
       datetime(2026, 11, 9, tzinfo=ny),
       datetime(2026, 11, 13, tzinfo=ny),
       benchmark_asset=None,
       risk_free_rate=0,
   )

Provide ``WEBULL_APP_KEY`` and ``WEBULL_APP_SECRET`` through your configured
secret store. The historical source makes only market-data requests.
Option replay requires an explicit contract. Historical point-in-time option
chains and bid/ask series are unavailable on this path. The daily option
check uses OHLCV and the shared simulator. Multi-leg outcomes, expiry
settlement, intraday, dated futures and event replay require separate
qualification.
See :doc:`brokers.webull` and :doc:`backtesting.broker_history`.
