Strategies
==========================

All user defined strategies should inherit from the Strategy class.

from strategies import Strategy

.. code-block:: python

   class MyStrategy(Strategy):
      pass

The abstract class Strategy has global parameters with default values, and some properties that can be used as helpers to build trading logic.

The methods of this class can be split into several categories:

**Lifecycle Methods** These are executed at different times during the execution of the bot. These represent the main flow of a strategy, some are mandatory.

**Strategy Methods** These are strategy helper methods.

**Broker Methods** How to interact with the broker (buy, sell, get positions, etc)

**Data Methods** How to get price data easily

All the methods in each of these categories are described below.

Documentation
"""""""""""""""""""

.. automodule:: lumibot.strategies.strategy
   :members:
   :undoc-members:
   :show-inheritance:
