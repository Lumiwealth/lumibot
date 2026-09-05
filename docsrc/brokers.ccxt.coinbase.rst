Coinbase Crypto Broker
======================

Coinbase is one of Lumibot's auto-detected CCXT credential paths. It is a good
starting point for users who want regulated spot crypto access and relatively
simple account setup.

Status
------

* **Live trading path:** auto-detected through the shared CCXT broker
* **Credential style:** Coinbase CDP key name plus private key
* **Backtesting:** use CCXT backtesting with an exchange id when appropriate
* **Asset class:** crypto

Credentials
-----------

Coinbase's current Cloud Developer Platform keys use a key ``name`` and a
multi-line private key. Create the key in the Coinbase CDP portal:

https://portal.cdp.coinbase.com/

Then set:

.. code-block:: bash

   COINBASE_API_KEY_NAME=organizations/<org-id>/apiKeys/<key-id>
   COINBASE_PRIVATE_KEY="-----BEGIN EC PRIVATE KEY-----\n...\n-----END EC PRIVATE KEY-----\n"

``COINBASE_API_PASSPHRASE`` is only for legacy HMAC keys and should usually be
omitted for new CDP keys.

Strategy Notes
--------------

Crypto markets trade continuously, so set the market in ``initialize()``:

.. code-block:: python

   def initialize(self):
       self.set_market("24/7")
       self.sleeptime = "1M"

Start with tiny paper or live test quantities, verify balances, open orders,
fills, and cancellation behavior, and only then increase size.

Live Historical Bars
--------------------

Live Coinbase history uses CCXT timestamp pagination. LumiBot advances the
``since`` cursor to one full timeframe after the last returned candle, which
prevents exchanges with inclusive cursors from returning the same boundary
candle indefinitely. A request either returns the requested number of bars or
raises a short-history error that includes the returned and requested counts.
