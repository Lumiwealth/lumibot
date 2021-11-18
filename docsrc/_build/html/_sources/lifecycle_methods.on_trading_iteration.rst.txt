def on_trading_iteration
===================================

This lifecycle method contains the main trading logic. When the market opens, it will be executed in a loop. After each iteration, the strategy will sleep for self.sleeptime minutes. If no crash or interuption, the loop will be stopped self.minutes_before_closing minutes before market closes and will restart on the next day when market opens again.

.. code-block:: python

    class MyStrategy(Strategy):
        def on_trading_iteration(self):
            # pull data
            # check if should buy an asset based on data
            # if condition, buy/sell asset
            pass


Reference
----------

.. autofunction:: strategies.strategy.Strategy.on_trading_iteration