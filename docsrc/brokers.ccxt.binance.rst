Binance CCXT Configuration
==========================

Binance has selected Lumibot/CCXT handling, but it is not currently one of the
global auto-detected credential paths. Use an explicit ``Ccxt`` broker config.

Status
------

* **Live trading path:** explicit ``Ccxt`` config
* **Credential style:** API key and secret
* **Backtesting:** documented CCXT backtesting exchange id: ``binance``
* **Asset class:** crypto

Manual Config
-------------

.. code-block:: python

   from lumibot.brokers import Ccxt

   BINANCE_CONFIG = {
       "exchange_id": "binance",
       "apiKey": "your_api_key",
       "secret": "your_api_secret",
       "margin": False,
       "sandbox": False,
   }

   broker = Ccxt(BINANCE_CONFIG)

Backtesting
-----------

.. code-block:: bash

   export BACKTESTING_DATA_SOURCE=binance

Binance availability and product access vary by jurisdiction. Validate account
permissions, symbols, quote assets, order types, and fee assumptions before
using live funds.
