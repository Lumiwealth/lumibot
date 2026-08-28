def on_canceled_order
===================================

The lifecycle method called when an order has been successfully canceled by the
broker. Use this lifecycle event to reconcile terminal cancellation state.

This callback does **not** initiate cancellation or act as a timer. Call
``self.cancel_order(order)`` from the strategy's deadline logic. The broker call
may return before this queued callback runs, so do not assume
``on_canceled_order`` executed synchronously inside ``cancel_order``.

If callbacks, cancel exceptions, and later exact-order reconciliation can all
observe the same order, route them through one idempotent state transition keyed
by ``order.identifier``. Clear only the conflicting order or causal exposure
group. Independent symbols may continue when the strategy's capital and risk
policy permits.

Parameters:

order (Order): The corresponding order object that has been canceled

.. code-block:: python

    class MyStrategy(Strategy):
        def on_canceled_order(self, order):
            order_id = order.identifier
            group = self.order_groups.get(order_id)
            if group is None or group["state"] == "TERMINAL":
                return

            group["state"] = "TERMINAL"
            group["terminal_status"] = "CANCELED"
            self.log_message(f"Order {order_id} was canceled by the broker")

For a deadline-driven pattern, process local pending events while waiting and
use a bounded exact-order read only after a missed callback, restart/reconnect,
or ambiguous cancel result. Repeated broker polling is not required for this
callback to work.

Reference
----------

.. autofunction:: lumibot.strategies.strategy.Strategy.on_canceled_order
