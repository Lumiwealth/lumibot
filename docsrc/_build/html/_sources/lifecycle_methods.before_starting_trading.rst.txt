def before_starting_trading
===================================

Use this lifecycle method to reinitialize variables for day trading like resetting the list of blacklisted shares.

.. code-block:: python

    class MyStrategy(Strategy):
        def before_starting_trading(self):
            self.blacklist = []


Reference
----------

.. autofunction:: strategies.strategy.Strategy.before_starting_trading