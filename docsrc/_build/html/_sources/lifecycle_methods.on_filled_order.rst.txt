def on_filled_order
===================================

The lifecycle method is called when an order has been successfully filled by the broker. Use this lifecycle event to execute code when an order has been filled by the broker

Parameters:

position (Position): The updated position object related to the order symbol. If the strategy already holds 200 shares of SPY and 300 has just been filled, then position.quantity will be 500 shares otherwise if it is a new position, a new position object will be created and passed to this method.
order (Order): The corresponding order object that has been filled
price (float): The filled price
quantity (int): The filled quantity
multiplier (int): Options multiplier

.. code-block:: python

    class MyStrategy(Strategy):
        def on_filled_order(self, position, order, price, quantity, multiplier):
            if order.side == "sell":
                self.log_message(f"{quantity} shares of {order.symbol} has been sold at {price}$")
            elif order.side == "buy":
                self.log_message(f"{quantity} shares of {order.symbol} has been bought at {price}$")

            self.log_message(f"Currently holding {position.quantity} of {position.symbol}")

Reference
----------

.. autofunction:: strategies.strategy.Strategy.on_filled_order