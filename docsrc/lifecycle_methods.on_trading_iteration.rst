def on_trading_iteration
===================================

This lifecycle method contains the main trading logic. When the market opens, it will be executed in a loop. After each iteration, the strategy will sleep for self.sleeptime minutes. If no crash or interuption, the loop will be stopped self.minutes_before_closing minutes before market closes and will restart on the next day when market opens again.

This is also the most common place to run an AI trading agent with ``self.agents["name"].run(...)``. The strategy still controls when the agent runs, how often it runs, and what tools the agent is allowed to use. See :doc:`agents`.

.. code-block:: python

    class MyStrategy(Strategy):
        def on_trading_iteration(self):
            # pull data
            # check if should buy an asset based on data
            # if condition, buy/sell asset
            pass


Reference
----------

.. autofunction:: lumibot.strategies.strategy.Strategy.on_trading_iteration
