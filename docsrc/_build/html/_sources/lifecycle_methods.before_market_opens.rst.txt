def before_market_opens
===================================

This lifecycle method is executed each day before market opens. If the strategy is first run when the market is already open, this method will be skipped the first day. Use this lifecycle methods to execute business logic before starting trading like canceling all open orders.

.. code-block:: python

    class MyStrategy(Strategy):
        def before_market_opens(self):
            self.cancel_open_orders()

.. code-block:: python

    def before_starting_trading()

This lifecycle method is similar to before_market_opens. However, unlike before_market_opens, this method will always be executed before starting trading even if the market is already open when the strategy was first launched. After the first execution, both methods will be executed in the following order


Reference
----------

.. autofunction:: strategies.strategy.Strategy.before_market_opens