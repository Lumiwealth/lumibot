Migrating from Backtrader to LumiBot
====================================

Move a strategy one behavior at a time: data timing, indicators, position sizing,
orders, and execution. Both libraries support strategy lifecycles and broker
abstractions. Choosing LumiBot does not make two backtests numerically equivalent.

The API mapping below was checked against the linked Backtrader source and the
current LumiBot source on September 8, 2026. It makes no claim that Backtrader is
abandoned or that every broker, asset, and Python version has identical support.

Map the lifecycle
-----------------

.. list-table:: Core concepts
   :header-rows: 1
   :widths: 30 35 35

   * - Backtrader
     - LumiBot
     - Migration check
   * - ``bt.Strategy.__init__``
     - ``Strategy.initialize``
     - Configure the decision cadence explicitly.
   * - ``next()``
     - ``on_trading_iteration()``
     - Check which completed bars are visible at that timestamp.
   * - ``self.buy()`` / ``self.sell()``
     - ``create_order`` then ``submit_order``
     - Preserve quantity, side, order type, and pending-order handling.
   * - ``self.data.close[0]``
     - ``get_last_price`` / ``get_historical_prices``
     - Check adjustment, bar interval, timezone, and missing data.
   * - ``self.broker.getcash()`` / ``getvalue()``
     - ``self.cash`` / ``self.portfolio_value``
     - Use matching capital, fees, and valuation conventions.
   * - ``Cerebro.adddata`` and ``Cerebro.run``
     - Data source and ``MyStrategy.backtest``
     - ``Trader`` is the broker-run orchestrator, not a replacement historical data feed.

A small allocation example
--------------------------

The examples express the same target: hold ten shares while the ten-day average
is above the thirty-day average, otherwise hold none. This is an allocation rule,
not an identical crossover implementation or a proven parity test. Use a single
symbol while checking the port; do not liquidate unrelated positions.

Backtrader strategy
~~~~~~~~~~~~~~~~~~~

.. code-block:: python

   import backtrader as bt

   class SmaAllocation(bt.Strategy):
       def __init__(self):
           self.fast = bt.ind.SMA(self.data.close, period=10)
           self.slow = bt.ind.SMA(self.data.close, period=30)
           self.pending = None

       def notify_order(self, order):
           if not order.alive():
               self.pending = None

       def next(self):
           if self.pending:
               return
           target = 10 if self.fast[0] > self.slow[0] else 0
           if self.position.size != target:
               self.pending = self.order_target_size(target=target)

Add this class to your existing ``Cerebro`` runner and existing data feed. Keep
that feed's dates and settings as the baseline. Backtrader's own
`SMA example <https://github.com/mementum/backtrader/blob/master/samples/sigsmacross/sigsmacross.py>`_
and `Cerebro source <https://github.com/mementum/backtrader/blob/master/backtrader/cerebro.py>`_
document those interfaces.

Complete LumiBot backtest
~~~~~~~~~~~~~~~~~~~~~~~~~

Install ``lumibot`` in a Python 3.10+ virtual environment. Save the following as
``sma_allocation.py`` and run ``python sma_allocation.py``. This daily Yahoo-data
example makes no LLM calls and requires no broker keys. It is a porting example,
not a claim of matching the data from your existing Backtrader run.

.. code-block:: python

   from datetime import datetime
   from lumibot.backtesting import YahooDataBacktesting
   from lumibot.strategies import Strategy

   class SmaAllocation(Strategy):
       def initialize(self):
           self.sleeptime = "1D"
           self.vars.pending = None

       def on_trading_iteration(self):
           pending = self.vars.pending
           if pending is not None and pending.is_active():
               return
           bars = self.get_historical_prices("AAPL", 30, timestep="day")
           if bars is None or len(bars.df) < 30:
               return
           closes = bars.df["close"]
           target = 10 if closes.tail(10).mean() > closes.tail(30).mean() else 0
           position = self.get_position("AAPL")
           current = position.quantity if position is not None else 0
           difference = target - current
           if difference:
               order = self.create_order(
                   "AAPL", abs(difference), "buy" if difference > 0 else "sell"
               )
               self.vars.pending = order
               self.submit_order(order)

   if __name__ == "__main__":
       SmaAllocation.backtest(
           YahooDataBacktesting,
           datetime(2025, 1, 6),
           datetime(2025, 4, 1),
           budget=100_000,
           benchmark_asset="SPY",
       )

Compare timestamps and orders first
-----------------------------------

Before comparing returns, reconcile the input bars, indicator warm-up, completed
bar boundary, order timing, quantities, fills, fees, and corporate-action
adjustments. A chart that looks similar is not sufficient evidence of parity.
Inspect pending orders and partial fills before adding more symbols or leverage.

Broker-connected execution uses a different runner
--------------------------------------------------

Keep the strategy class, configure a supported broker, instantiate the strategy
with that broker, add it to ``Trader``, and run the trader. Do not pass a broker
class in place of ``YahooDataBacktesting`` to ``backtest``. Broker authentication,
account permissions, supported order types, and data access still need setup.
See :doc:`deployment` and the relevant broker documentation.

Add AI only after the port is understood
----------------------------------------

Use :doc:`agents_quickstart` to add a research agent, then :doc:`agents_examples`
for stock, macro, and options workflows. Keep the deterministic port as a
baseline. Record model cost and the limits of historical LLM knowledge alongside
any performance comparison.
