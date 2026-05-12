KuCoin CCXT Configuration
=========================

KuCoin has selected Lumibot/CCXT handling, but it is not currently one of the
global auto-detected credential paths. Use an explicit ``Ccxt`` broker config
and validate behavior with small quantities.

Status
------

* **Live trading path:** explicit ``Ccxt`` config
* **Credential style:** API key, secret, and passphrase
* **Backtesting:** documented CCXT backtesting exchange id: ``kucoin``
* **Asset class:** crypto

Manual Config
-------------

.. code-block:: python

   from lumibot.brokers import Ccxt

   KUCOIN_CONFIG = {
       "exchange_id": "kucoin",
       "apiKey": "your_api_key",
       "secret": "your_api_secret",
       "password": "your_passphrase",
       "margin": False,
       "sandbox": False,
   }

   broker = Ccxt(KUCOIN_CONFIG)

Backtesting
-----------

.. code-block:: bash

   export BACKTESTING_DATA_SOURCE=kucoin

Always validate balances, order submission, cancellation, fills, precision, and
fees before increasing position size.
