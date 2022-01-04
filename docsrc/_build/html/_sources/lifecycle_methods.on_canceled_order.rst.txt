def on_canceled_order
===================================

The lifecycle method called when an order has been successfully canceled by the broker. Use this lifecycle event to execute code when an order has been canceled by the broker

Parameters:

order (Order): The corresponding order object that has been canceled

.. code-block:: python

    class MyStrategy(Strategy):
        def on_canceled_order(self, order):
            self.log_message(f"{order} has been canceled by the broker")

Reference
----------

.. autofunction:: strategies.strategy.Strategy.on_canceled_order