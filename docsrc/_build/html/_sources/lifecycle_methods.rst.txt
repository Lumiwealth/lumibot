Lifecycle Methods
************************

The abstract class Strategy defines a design pattern that needs to be followed by user-defined strategies. The design pattern was greatly influenced by React.js components and their lifecycle methods.

When building strategies, lifecycle methods needs to be overloaded. Trading logics should be implemented in these methods.

.. image:: lifecycle_methods.png

.. currentmodule:: strategies.strategy.Strategy

.. toctree::
   :maxdepth: 2
   :caption: Contents:
   
   lifecycle_methods.summary
   lifecycle_methods.initialize
   lifecycle_methods.on_trading_iteration
   lifecycle_methods.before_market_opens
   lifecycle_methods.before_starting_trading
   lifecycle_methods.before_market_closes
   lifecycle_methods.after_market_closes
   lifecycle_methods.on_abrupt_closing
   lifecycle_methods.on_bot_crash
   lifecycle_methods.trace_stats
   lifecycle_methods.on_new_order
   lifecycle_methods.on_partially_filled_order
   lifecycle_methods.on_filled_order
   lifecycle_methods.on_canceled_order
   lifecycle_methods.on_parameters_updated
