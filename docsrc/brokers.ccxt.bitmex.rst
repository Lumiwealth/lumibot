BitMEX CCXT Configuration
=========================

BitMEX has documented Lumibot CCXT examples, but it is not currently one of the
global auto-detected credential paths. Use an explicit ``Ccxt`` config and
validate exchange-specific behavior carefully.

Status
------

* **Live trading path:** explicit ``Ccxt`` config
* **Credential style:** API key and secret
* **Backtesting:** documented CCXT backtesting exchange id: ``bitmex``
* **Asset class:** crypto derivatives exchange; verify product support before live use

Manual Config
-------------

.. code-block:: python

   from lumibot.brokers import Ccxt

   BITMEX_CONFIG = {
       "exchange_id": "bitmex",
       "apiKey": "your_api_key",
       "secret": "your_api_secret",
       "margin": False,
       "sandbox": False,
   }

   broker = Ccxt(BITMEX_CONFIG)

Backtesting
-----------

.. code-block:: bash

   export BACKTESTING_DATA_SOURCE=bitmex

BitMEX products can involve leverage, contract multipliers, and derivative
semantics that differ from spot crypto. Validate the exact asset, quantity,
margin, and PnL behavior before live trading.
