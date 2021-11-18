def on_partially_filled_order
===================================

The lifecycle method called when an order has been partially filled by the broker. Use this lifecycle event to execute code when an order has been partially filled by the broker.

Parameters:

order (Order): The order object that is being processed by the broker
price (float): The filled price
quantity (int): The filled quantity
multiplier (int): Options multiplier

.. code-block:: python

    class MyStrategy(Strategy):
        def on_partially_filled_order(self, order, price, quantity, multiplier):
            missing = order.quantity - quantity
            self.log_message(f"{quantity} has been filled")
            self.log_message(f"{quantity} waiting for the remaining {missing}")

Reference
----------

.. autofunction:: strategies.strategy.Strategy.on_partially_filled_order