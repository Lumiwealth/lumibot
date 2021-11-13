def on_bot_crash
===================================

This lifecycle method runs when the strategy crashes. By default, if not overloaded, it calls on_abrupt_closing.

.. code-block:: python

    class MyStrategy(Strategy):
        def on_bot_crash(self, error):
            self.on_abrupt_closing()

Reference
----------

.. autofunction:: strategies.strategy.Strategy.on_bot_crash