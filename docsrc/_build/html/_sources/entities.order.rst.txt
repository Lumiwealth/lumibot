Order
-----------------------------

This object represents an order. Each order belongs to a specific strategy.

A simple market order can be constructed as follows:

.. code-block:: python

   symbol = "SPY"
   quantity = 50
   side = "buy"
   order = self.create_order(symbol, quantity, side)

With:

* symbol (str): the string representation of the asset e.g. "GOOG" for Google
* quantity (int): the number of shares to buy/sell
* side (str): must be either "buy" for buying order or "sell" for selling order

Order objects have the following helper methods

* ``to_position()``: convert an order to a position belonging to the same strategy with ``order.quantity`` amount of shares.
* ``get_increment()``: for selling orders returns - ``order.quantity``, for buying orders returns ``order.quantity``
* ``wait_to_be_registered``: wait for the order to be registered by the broker
* ``wait_to_be_closed``: wait for the order to be closed by the broker (Order either filled or closed)

Advanced Order Types
""""""""""""""""""""""""""

**limit order**

A limit order is an order to buy or sell at a specified price or better.

To create a limit order object, add the keyword parameter limit_price

.. code-block:: python

   my_limit_price = 500
   order = self.create_order(symbol, quantity, side, limit_price=my_limit_price)
   self.submit_order(order)


**stop order**

A stop (market) order is an order to buy or sell a security when its price moves past a particular point, ensuring a higher probability of achieving a predetermined entry or exit price.

To create a stop order object, add the keyword parameter stop_price.

.. code-block:: python

   my_stop_price = 400
   order = self.create_order(symbol, quantity, side, stop_price=my_stop_price)
   self.submit_order(order)


**stop limit order**

A stop_limit order is a stop order with a limit price (combining stop orders and limit orders)

To create a stop_limit order object, add the keyword parameters stop_price and limit_price.

.. code-block:: python

   my_limit_price = 405
   my_stop_price = 400
   order = self.create_order(symbol, quantity, side, stop_price=my_stop_price, limit_price=my_limit_price)
   self.submit_order(order)


**trailing stop order**

Trailing stop orders allow you to continuously and automatically keep updating the stop price threshold based on the stock price movement.

To create trailing_stop orders, add either a trail_price or a trail_percent keyword parameter.

.. code-block:: python

   my_trail_price = 20
   order_1 = self.create_order(symbol, quantity, side, trail_price=my_trail_price)
   self.submit_order(order_1)

   my_trail_percent = 2.0 # 2.0 % 
   order_2 = self.create_order(symbol, quantity, side, trail_percent=my_trail_percent)
   self.submit_order(order_2)

Order With Legs
"""""""""""""""""""""

**bracket order**

A bracket order is a chain of three orders that can be used to manage your position entry and exit.

The first order is used to enter a new long or short position, and once it is completely filled, two conditional exit orders will be activated. One of the two closing orders is called a take-profit order, which is a limit order, and the other closing order is a stop-loss order, which is either a stop or stop-limit order. Importantly, only one of the two exit orders can be executed. Once one of the exit orders fills, the other order cancels. Please note, however, that in extremely volatile and fast market conditions, both orders may fill before the cancellation occurs.

To create a bracket order object, add the keyword parameters ``take_profit_price`` and ``stop_loss_price``. A ``stop_loss_limit_price`` can also be specified to make the stop loss order a stop-limit order.

.. code-block:: python

   my_take_profit_price = 420
   my_stop_loss_price = 400
   order = self.create_order(
      symbol, 
      quantity, 
      side, 
      take_profit_price=my_take_profit_price,
      stop_loss_price=my_stop_loss_price
   )
   self.submit_order(order)

Interactive Brokers requires the main or parent order to be a limit order. Add
``limit_price=my_limit_price``.


**OTO (One-Triggers-Other) order**

OTO (One-Triggers-Other) is a variant of bracket order. It takes one of the take-profit or stop-loss order in addition to the entry order.

To create an OTO order object, add either a ``take_profit_price`` or a ``stop_loss_price`` keyword parameter. A ``stop_loss_limit_price`` can also be specified in case of stop loss exit.

Interactive Brokers requires the main or parent order to be a limit order. Add ``limit_price=my_limit_price``.


**OCO (One-Cancels-Other) order**

OCO orders are a set of two orders with the same side (buy/buy or sell/sell). In other words, this is the second part of the bracket orders where the entry order is already filled, and you can submit the take-profit and stop-loss in one order submission.

To create an OCO order object, add the keyword parameters ``take_profit_price`` and ``stop_loss_price`` and set position_filled to ``True``. A ``stop_loss_limit_price`` can also be specified to make the stop loss order a stop-limit order.

.. code-block:: python

   my_take_profit_price = 420
   my_stop_loss_price = 400
   order = self.create_order(
      symbol, 
      quantity, 
      side, 
      take_profit_price=my_take_profit_price,
      stop_loss_price=my_stop_loss_price,
      position_filled=True
   )
   self.submit_order(order)

Interactive Brokers requires the main or parent order to be a limit order. Add limit_price=my_limit_price.

Documentation
"""""""""""""""""""

.. automodule:: entities.order
   :members:
   :undoc-members:
   :show-inheritance: