def on_parameters_updated
===================================

This lifecycle method gets called when the strategy's parameters were updated using the `self.update_parameters()` function

.. code-block:: python

    class MyStrategy(Strategy):
        def on_parameters_updated(self, parameters):
            # Do this when the parameters are updated
            self.log_message("Parameters updated")
            self.log_message(parameters)


Reference
----------

.. autofunction:: strategies.strategy.Strategy.on_parameters_updated