Summary
**************************

Lifecycle methods are methods that are called by the trading engine at specific times. They are used to implement trading logics and to perform other tasks. They are the core of the trading engine and are the most important part of the framework, as they are the ones that actually perform the trading operations.

From a user's perspective, lifecycle methods are the only methods that need to be implemented. The rest of the framework is already implemented and ready to use. The user only needs to implement the lifecycle methods and the trading engine will take care of the rest.

Technically speaking, lifecycle methods are overloaded functions that are called by the trading engine at specific times. The user can implement as many lifecycle methods as he wants, but he must implement at least the ``on_trading_iteration`` method. 

The abstract class Strategy defines a design pattern that needs to be followed by user-defined strategies. The design pattern was greatly influenced by React.js components and their lifecycle methods.

Here is an illustration of the lifecycle methods and their order of execution:

.. image:: lifecycle_methods.png
    :alt: Lifecycle methods
    :align: center

Here is an example of a strategy that implements the ``on_trading_iteration`` method:

.. code-block:: python

    from lumibot.strategies import Strategy

    class MyStrategy(Strategy):
        def on_trading_iteration(self):
            # Implement your trading logic here
            pass

The ``on_trading_iteration`` method is called by the trading engine at each trading iteration. It is the most important method of the framework, as it is the one that actually performs the trading operations. It is the only method that must be implemented by the user. The rest of the methods are optional.

Here is an example of a strategy that implements the initialize method, which is called by the trading engine only once, and before any other method, including ``on_trading_iteration``:

.. code-block:: python

    from lumibot.strategies import Strategy

    class MyStrategy(Strategy):
        def initialize(self):
            # Initialize your strategy here
            pass
        
        def on_trading_iteration(self):
            # Implement your trading logic here
            pass