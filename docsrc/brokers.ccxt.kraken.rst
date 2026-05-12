Kraken Crypto Broker
====================

Kraken is one of Lumibot's auto-detected CCXT credential paths. It is useful for
spot crypto strategies that need a long-running exchange account and simple
API-key based setup.

Status
------

* **Live trading path:** auto-detected through the shared CCXT broker
* **Credential style:** API key and API secret
* **Backtesting:** documented CCXT backtesting exchange id: ``kraken``
* **Asset class:** crypto

Credentials
-----------

Create a Kraken API key in the Kraken account security/API section. Give it the
minimum permissions needed for the strategy.

.. code-block:: bash

   KRAKEN_API_KEY=your_api_key
   KRAKEN_API_SECRET=your_api_secret

Strategy Notes
--------------

.. code-block:: python

   def initialize(self):
       self.set_market("24/7")
       self.sleeptime = "1M"

Backtesting Example
-------------------

Use the CCXT backtesting provider with Kraken when you want historical crypto
bars:

.. code-block:: bash

   export BACKTESTING_DATA_SOURCE=kraken

Validate symbols, quote assets, fees, order behavior, and cache coverage before
using the same strategy with live funds.
