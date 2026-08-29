def on_partially_filled_order
===================================

The lifecycle callback method called after LumiBot observes a partial broker
fill. Use it for quantity-sensitive work such as incremental hedging.

Parameters:

position (Position): The updated position after applying this fill
order (Order): The order object that is being processed by the broker
price (float): The filled price
quantity (int): The newly observed fill quantity for this callback, not the cumulative filled quantity
multiplier (int): Options multiplier

.. code-block:: python

    class MyStrategy(Strategy):
        def on_partially_filled_order(self, position, order, price, quantity, multiplier):
            order_id = order.identifier
            previously_processed = self.processed_fill_quantity.get(order_id, 0)
            self.processed_fill_quantity[order_id] = previously_processed + quantity
            missing = order.quantity - self.processed_fill_quantity[order_id]
            self.log_message(f"{quantity} has been filled")
            self.log_message(f"{quantity} waiting for the remaining {missing}")

The later ``on_filled_order`` callback receives the remaining delta and should
reuse the same idempotency state. Live callbacks may be delayed or reconstructed
after reconciliation; keep the callback short and do not require a fresh broker
read before acting on the supplied order and quantity.

Reference
----------

.. autofunction:: lumibot.strategies.strategy.Strategy.on_partially_filled_order
