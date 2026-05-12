WEEX Crypto Broker
==================

WEEX is one of Lumibot's auto-detected CCXT credential paths. Treat it as an
advanced crypto path because jurisdiction, sandbox, and product support differ
from more common spot exchanges.

Status
------

* **Live trading path:** auto-detected through the shared CCXT broker
* **Credential style:** API key, API secret, and passphrase
* **Sandbox:** WEEX does not provide a normal API sandbox
* **Asset class:** spot-oriented crypto path in Lumibot's shared CCXT broker

Credentials
-----------

Create the API key in WEEX API Management and set:

.. code-block:: bash

   WEEX_API_KEY=your_api_key
   WEEX_API_SECRET=your_api_secret
   WEEX_API_PASSPHRASE=your_passphrase

Important Caveats
-----------------

WEEX's Terms of Use exclude residents of the United States, Canada, and several
other jurisdictions. Verify that you are allowed to use the exchange before
connecting an account.

WEEX's primary business is USDT-margined perpetual swaps. Lumibot's documented
WEEX path is spot-oriented through the shared CCXT broker. Swap-specific
position, leverage, liquidation, and funding-rate behavior should not be assumed
to work without additional validation.
