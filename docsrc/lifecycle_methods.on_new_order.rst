def on_new_order
===================================
    
This lifecycle method runs when a new order has been successfully submitted to the broker. Use this lifecycle event to execute code when the broker processes a new order.

Parameters:

order (Order): The corresponding order object processed

.. code-block:: python

    class MyStrategy(Strategy):
        def on_new_order(self, order):
            self.log_message("%r is currently being processed by the broker" % order)

Reference
----------

.. autofunction:: strategies.strategy.Strategy.on_new_order