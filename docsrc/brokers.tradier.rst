Tradier
===================================

This is a guide for using Tradier with the Lumibot library.

Getting Started
---------------

To get started, you will need to create a Tradier account and get your Account Number and API Secret. You can do this by visiting the [Tradier website](https://www.tradier.com/).

Once you have an account and you've logged in you can find your Account Number and API Secret by visiting the [API Access page](https://dash.tradier.com/settings/api).

.. note::
   **Easy Setup with .env File**
   
   LumiBot automatically loads your API credentials from a `.env` file! Simply create a `.env` file in the same folder as your trading strategy and add your Tradier credentials. LumiBot will automatically detect and use these credentials - no additional configuration required.
   
   **Example .env file:**
   
   .. code-block:: bash
   
      # Tradier Configuration
      TRADIER_ACCESS_TOKEN=your_access_token_here
      TRADIER_ACCOUNT_NUMBER=your_account_number_here
      TRADIER_IS_PAPER=true
   
   That's it! LumiBot handles the rest automatically.

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

Cash Events
-----------

Lumibot can emit normalized live ``cash_events`` in the cloud payload for Tradier strategies. These events are pulled
from Tradier account history and are separate from LumiBot's order lifecycle pipeline.

Supported history categories include:

* ACH and wire activity
* dividends
* interest
* fees and taxes
* journals
* checks, transfers, and adjustments

Normalized cash-event shape
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Each emitted event includes:

* ``event_id`` (stable deterministic ID for downstream idempotency)
* ``broker_event_id`` (when Tradier provides one)
* ``broker_name``
* ``event_type``
* ``raw_type``
* ``raw_subtype``
* ``amount``
* ``currency``
* ``occurred_at``
* ``description``
* ``direction``
* ``is_external_cash_flow``

Tradier-specific limitations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* Tradier account history is a broker history surface, not a real-time stream.
* Tradier's official API only exposes account history for live accounts.
* Tradier sandbox/paper accounts do not provide account history, so the cash-event read path cannot be fully smoke
  tested against paper credentials.
* Tradier history is updated on a delayed/nightly basis, so new cash events are not expected to appear intraday.

See also: :doc:`cash_accounting`
