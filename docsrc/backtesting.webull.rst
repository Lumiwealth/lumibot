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
check uses OHLCV and the shared simulator. A two-leg call vertical opened
and closed on actual daily contract history. Both legs filled on the same bar
at entry and again at exit. Expiry, assignment and event settlement need
separate qualification.

Minute replay from actual Webull bars
-------------------------------------

Use the ordinary pandas backtesting source to replay an explicit window.
A data-only client loads each instrument before the simulation. Supply
timezone-aware start and end times and an exact dated futures or option
contract. The range includes only completed bars. Missing volume is preserved.
In PaperTrade, the crypto service can ignore historical time arguments and
return recent bars. A request outside its returned window can have no data.

.. code-block:: python

   from os import environ
   from datetime import datetime
   from zoneinfo import ZoneInfo
   from lumibot.backtesting import PandasDataBacktesting
   from lumibot.data_sources import WebullData
   from lumibot.entities import Asset, Data

   ny = ZoneInfo("America/New_York")
   start = datetime(2026, 11, 20, 10, 0, tzinfo=ny)
   end = datetime(2026, 11, 20, 11, 0, tzinfo=ny)
   source = WebullData({"APP_KEY": environ["WEBULL_APP_KEY"],
                        "APP_SECRET": environ["WEBULL_APP_SECRET"], "PAPER": True})
   asset, usd = Asset("SPY"), Asset("USD", "forex")
   bars = source.get_minute_range(asset, start, end, quote=usd)
   if bars is None or bars.empty:
       raise ValueError("The selected Webull interval has no completed bars")
   data = {(asset, usd): Data(asset, bars, timestep="minute", quote=usd)}
   # MyStrategy is your ordinary Strategy subclass with a minute trading interval.
   # MyStrategy.run_backtest(PandasDataBacktesting, start, end, pandas_data=data,
   #                         benchmark_asset=None, risk_free_rate=0)

Stock, ETF, listed call, dated micro future and recent BTC/USD minute windows
each produced two simulated fills and an identical repeated journal and cash
balance with HTTP disabled after loading. OPRA historical bid/ask and
point-in-time chain membership are separate from traded minute OHLCV.
See :doc:`brokers.webull` and :doc:`backtesting.broker_history`.
