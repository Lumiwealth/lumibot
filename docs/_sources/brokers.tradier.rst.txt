Tradier
===================================

This is a guide for using Tradier with the Lumibot library.

Getting Started
---------------

To get started, you will need to create a Tradier account and get your Account Number and API Secret. You can do this by visiting the [Tradier website](https://www.tradier.com/).

Once you have an account and you've logged in you can find your Account Number and API Secret by visiting the [API Access page](https://dash.tradier.com/settings/api).

Configuration
-------------

Here is an example dictionary of configuration options for Tradier:

.. code-block:: python

    TRADIER_CONFIG = {
        # Put your own Tradier key here:
        "ACCESS_TOKEN": "qTRz3zUrd9244AHUw2AoyAPgvYra",
        # Put your own Tradier account number here:
        "ACCOUNT_NUMBER": "VA22904793",
        # If you want to use real money you must change this to False
        "PAPER": True,
    }

or more generally:

.. code-block:: python

    TRADIER_CONFIG = {
        "ACCESS_TOKEN": "your_access_token",
        "ACCOUNT_NUMBER": "your_account_number",
        "PAPER": True,
    }

Running Your Strategy
---------------------

To run your strategy, you'll first need to instantiate your chosen broker with the correct configuration:

.. code-block:: python

    from lumibot.brokers import Tradier

    broker = Tradier(config=TRADIER_CONFIG)

Then you can run your strategy as you normally would:

.. code-block:: python

    from lumibot.trader import Trader

    strategy = MyStrategy(broker=broker) # Your normal strategy class, with on_trading_iteration, etc
    trader = Trader()
    trader.add_strategy(strategy)
    strategy_executors = trader.run_all()

That's it! You should now be able to run your strategy using Tradier as your broker.

Full Example Strategy
---------------------

Here is an example of a simple strategy that uses Tradier as the broker:

.. code-block:: python

    from lumibot.brokers import Tradier
    from lumibot.trader import Trader
    from lumibot.strategies import Strategy

    TRADIER_CONFIG = {
        "ACCESS_TOKEN": "your_access_token",
        "ACCOUNT_NUMBER": "your_account_number",
        "PAPER": True,
    }

    class MyStrategy(Strategy):
        def on_trading_iteration(self):
            # Buy 1 share of AAPL if the price is less than $100
            price = self.get_last_price("AAPL")
            self.log_message(f"AAPL price: {price}")

    broker = Tradier(config=TRADIER_CONFIG)
    strategy = MyStrategy(broker=broker)
    trader = Trader()
    trader.add_strategy(strategy)
    strategy_executors = trader.run_all()