Smart Limit Orders
==================

.. meta::
   :description: SMART_LIMIT orders are midpoint-chasing limit orders that walk the bid/ask spread using a timed ladder (Option Alpha “SmartPricing” parity).

SMART_LIMIT orders are midpoint-chasing limit orders that walk the bid/ask spread
using a timed ladder (Option Alpha “SmartPricing” parity). They are meant to model
realistic execution without forcing market orders through wide option spreads.

Overview
--------

- Uses bid/ask to compute a midpoint and final price bound.
- Walks from mid toward the bid/ask using preset step timing.
- Cancels after the final hold window if unfilled.
- If bid/ask is missing, SMART_LIMIT downgrades to a market order and emits a warning.

Presets (Option Alpha parity)
-----------------------------

- **FAST**: 3 price levels, 5 seconds per step
- **NORMAL**: 4 price levels, 10 seconds per step
- **PATIENT**: 5 price levels, 20 seconds per step
- Final hold: 120 seconds

Backtesting behavior
--------------------

SMART_LIMIT fills at **mid + slippage** for buys and **mid - slippage** for sells.
This matches the standard midpoint fill approximation used by platforms like
Option Alpha when only bar data is available.

If the SMART_LIMIT config does **not** specify slippage, backtests will fall back
to strategy-level defaults (``buy_trading_slippages`` / ``sell_trading_slippages``),
which accept ``TradingSlippage`` objects and default to zero when unset.

If bid/ask quotes are missing in a backtest, SMART_LIMIT downgrades to a market
order fill (next-bar open).

Trade logs for backtests include a ``trade_slippage`` column (CSV) and show slippage
in trade marker tooltips (HTML).

Usage
-----

.. code-block:: python

   from lumibot.entities import SmartLimitConfig, SmartLimitPreset

   config = SmartLimitConfig(
       preset=SmartLimitPreset.NORMAL,
       slippage=0.05,  # $0.05 from mid
   )

   order = self.create_order("SPY", 100, "buy", smart_limit=config)
   self.submit_order(order)

Multi-leg orders
----------------

SMART_LIMIT supports multi-leg orders as a package (net bid/ask/mid). In backtests,
the package fills atomically at the net midpoint plus slippage. For multi-leg SMART_LIMIT
orders, build a parent Order with ``order_class=Order.OrderClass.MULTILEG`` and provide
the child leg orders on ``child_orders``.

Scheduled and run-once execution
-------------------------------

``Trader.run_all(run_once=True)`` runs one trading iteration, then continues
advancing active SmartLimit orders until they fill or their configured final
hold ends and cancellation is confirmed by the broker. A short or zero
``LUMIBOT_SCHEDULED_POST_ITERATION_SECONDS`` does not cut off that work.

The completion drain also waits for in-flight broker submissions, pending
cancellation/replacement transitions, and queued order callbacks. Work started
by ``on_strategy_end`` finishes before the final cloud snapshot, variable backup,
and broker disconnect. Ordinary resting limit/GTC, stop and bracket orders do
not keep a run alive waiting for a fill.

The runner allows at least 300 seconds for unresolved work and extends that
budget for the longest observed SmartLimit ladder and final hold plus 300 seconds
of broker-response grace. This is a bounded total drain, not an unlimited chain
of callback-created orders. Timeout or explicit stop fails the run with visible
``drain_failed`` or ``drain_interrupted`` timing instead of claiming completion.
Arbitrary application-created background tasks are not tracked automatically,
and a library cannot prevent a host from forcibly terminating its process.
