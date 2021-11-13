def after_market_closes
===================================

This lifecycle method is executed right after the market closes.

.. code-block:: python

    class MyStrategy(Strategy):
        def after_market_closes(self):
            pass

Reference
----------

.. autofunction:: strategies.strategy.Strategy.after_market_closes