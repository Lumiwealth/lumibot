def trace_stats
"""""""""""""""""""

Lifecycle method that will be executed after on_trading_iteration. context is a dictionary containing the result of locals() of on_trading_iteration() at the end of its execution.

locals() returns a dictionary of the variables defined in the scope where it is called.

Use this method to dump stats

.. code-block:: python

    import random
    class MyStrategy(Strategy):
    def on_trading_iteration(self):
        google_symbol = "GOOG"
    
    def trace_stats(self, context, snapshot_before):
        print(context)
        # printing
        # { "google_symbol":"GOOG"}
        random_number = random.randint(0, 100)
        row = {"my_custom_stat": random_number}

        return row

Reference
----------

.. autofunction:: strategies.strategy.Strategy.trace_stats