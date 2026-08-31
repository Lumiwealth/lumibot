def on_filled_order
===================================

The lifecycle callback method is called after LumiBot observes that an order
has been fully filled by the broker. Use it as the fast path for fill-dependent
work.

The callback already supplies the filled ``order``. A required hedge should not
wait for another broker-backed ``self.get_order(order.identifier)`` call. Route
the callback order directly to an idempotent hedge helper keyed by the entry
order identifier. The same helper may be called by cancel-exception or
restart/reconciliation paths without submitting a duplicate hedge.

If hedge sizing depends on partial fills, use
``on_partially_filled_order`` to process only the newly filled quantity and
share the same cumulative idempotency state with this full-fill callback.

Parameters:

position (Position): The updated position object related to the order symbol. If the strategy already holds 200 shares of SPY and 300 has just been filled, then position.quantity will be 500 shares otherwise if it is a new position, a new position object will be created and passed to this method.
order (Order): The corresponding order object that has been filled
price (float): The filled price
quantity (int): The filled quantity
multiplier (int): Options multiplier

``quantity`` is the fill quantity applied by this callback. If no partial-fill
callback preceded it, that is normally the full order quantity. If partial
callbacks already applied quantity, this callback carries the remaining delta.
It is not a new cumulative total. Live reconnect/reconciliation paths can repeat
observations, so external side effects such as hedges must be idempotent.

.. code-block:: python

    class MyStrategy(Strategy):
        def on_filled_order(self, position, order, price, quantity, multiplier):
            if order.identifier in self.processed_entry_order_ids:
                return
            self.processed_entry_order_ids.add(order.identifier)

            if order.side == "sell":
                self.log_message(f"{quantity} shares of {order.symbol} has been sold at {price}$")
            elif order.side == "buy":
                self.log_message(f"{quantity} shares of {order.symbol} has been bought at {price}$")

            self.log_message(f"Currently holding {position.quantity} of {position.symbol}")

Reference
----------

.. autofunction:: lumibot.strategies.strategy.Strategy.on_filled_order
