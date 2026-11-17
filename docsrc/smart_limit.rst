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

Multi-leg packages without quotes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Some option data has trades but no historical bid and ask. A multi-leg SMART_LIMIT
package on such data fills market-style, the same way a multi-leg market order does:

- Every leg is priced before any leg fills. The package fills whole on one bar, or
  not at all on that bar.
- A leg that has a bar fills at that bar's open.
- An option leg whose contract did not trade on the bar is priced from a quote or
  trade on the current bar when one exists, otherwise from the model estimate. That
  fill is marked ``estimated`` in the trade log and counted in the run's price
  sources. An estimate is never shown as a real price.
- If a leg cannot be priced at all, nothing fills and the order keeps waiting until
  the SMART_LIMIT timeout cancels it.

Example: a call debit spread submitted as one SMART_LIMIT package (see "Multi-leg
orders" below for how to build it) on trade-only option data, checked every 15
minutes. If the short call did not trade in that minute, the long call fills at its
bar open and the short call at its marked estimate, both on the same bar.

Limit: with no quotes there is no spread to walk, so the fill is a market-style
fill, not a mid-price fill.

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

Live repricing and cancellation
--------------------------------

SmartLimit first uses the broker's native price-modification operation. If a
cancel-and-replace is needed, requesting cancellation does not authorize another
purchase. LumiBot waits for a confirmed outcome and preserves any fills that
arrive while the price change or cancellation is in flight.

A completed fill prevents replacement. After a confirmed partial cancellation,
only the unfilled remainder may become a new order, with a fresh identity and no
copied fill history. A package also waits until its remaining leg quantities
match the remaining package quantity. Broker-reported replacement links are
followed before considering a new submission.

If stream updates are delayed, pending SmartLimit mutations are refreshed through
the broker's normal order reads. Missing observations and uncertain submission
responses are not treated as successful cancellation or permission to submit
again. Unresolved work remains visible and can fail the bounded scheduled drain;
neither acceptance nor a price update guarantees a fill.

Scheduled and run-once execution
---------------------------------

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
