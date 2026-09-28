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
