def before_market_closes
===================================

.. meta::
   :description: This lifecycle method is executed self.minutes_before_closing minutes before the market closes.

This lifecycle method is executed self.minutes_before_closing minutes before the market closes. Use this lifecycle method to execute business logic like selling shares and closing open orders.

It runs every trading day in backtests too, whatever the sleeptime: an hourly strategy whose last iteration is at 15:30 still gets ``before_market_closes`` at 15:59 (with the default ``minutes_before_closing = 1``). Set ``minutes_before_closing`` to choose how early it runs; keep it at 1 or more, because the hook needs the market to still be open.

.. code-block:: python

    class MyStrategy(Strategy):
        def before_market_closes(self):
            self.sell_all()


Reference
----------

.. autofunction:: lumibot.strategies.strategy.Strategy.before_market_closes