def before_market_closes
===================================

This lifecycle method is executed self.minutes_before_closing minutes before the market closes. Use this lifecycle method to execute business logic like selling shares and closing open orders.

.. code-block:: python

    class MyStrategy(Strategy):
        def before_market_closes(self):
            self.sell_all()


Reference
----------

.. autofunction:: strategies.strategy.Strategy.before_market_closes