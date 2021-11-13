def on_abrupt_closing
===================================

This lifecycle method runs when the strategy execution gets interrupted. Use this lifecycle method to execute code to stop trading gracefully like selling all assets

.. code-block:: python

    class MyStrategy(Strategy):
        def on_abrupt_closing(self):
            self.sell_all()

Reference
----------

.. autofunction:: strategies.strategy.Strategy.on_abrupt_closing