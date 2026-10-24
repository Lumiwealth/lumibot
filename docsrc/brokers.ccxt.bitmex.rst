:seo_title: BitMEX Trading Bot in Python: AI Agent and Algorithmic Trading

BitMEX Trading Bot in Python
============================

.. meta::
   :description: Build a BitMEX trading bot in Python with LumiBot. Connect your BitMEX API key, use the testnet, and run an algorithmic or AI agent strategy.

Build a BitMEX trading bot in Python with LumiBot, for algorithmic trading or an
AI agent. LumiBot talks to BitMEX through CCXT, the open source library behind
its crypto broker. BitMEX is not one of the auto-detected credential paths, so
use an explicit ``Ccxt`` config and validate exchange-specific behavior
carefully.

.. include:: _snippets/botspot_mcp_callout.rst

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

   export BACKTESTING_DATA_SOURCE=ccxt

Then pass ``exchange_id="bitmex"`` to ``CcxtBacktesting``, as in
``lumibot/example_strategies/ccxt_backtesting_example.py``. ``bitmex`` itself is
not a valid ``BACKTESTING_DATA_SOURCE`` value.

BitMEX products can involve leverage, contract multipliers, and derivative
semantics that differ from spot crypto. Validate the exact asset, quantity,
margin, and PnL behavior before live trading.

Frequently asked questions
--------------------------

Can I trade on BitMEX with LumiBot?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes, through LumiBot's shared ``Ccxt`` broker with the exchange ID ``bitmex``. BitMEX is a crypto derivatives exchange, so check that the exact product you want behaves as you expect before you trade with real money.

How do I connect BitMEX with environment variables?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Set ``TRADING_BROKER=bitmex``, ``BITMEX_API_KEY`` and ``BITMEX_API_SECRET``. BitMEX only needs an API key and secret. LumiBot does not pick BitMEX on its own from the keys alone, so ``TRADING_BROKER`` is required. You can also pass an explicit ``Ccxt`` config, as shown in `Manual Config`_ above.

Where do I create a BitMEX API key?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create it in your BitMEX account settings. The `BitMEX API overview <https://www.bitmex.com/app/apiOverview>`__ explains how the API and its keys work.

Can I test on the BitMEX testnet first?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes. Set ``BITMEX_SANDBOX=true``, or use ``"sandbox": True`` in an explicit config, and LumiBot connects to the `BitMEX testnet <https://testnet.bitmex.com/>`__. Testnet keys are separate from live keys, so create them on the testnet site.

How do I backtest a strategy with BitMEX data?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Run your backtest with ``CcxtBacktesting`` and pass ``exchange_id="bitmex"``. The ``ccxt_backtesting_example.py`` strategy in ``lumibot/example_strategies`` shows how. Backtesting perpetual contracts with margin, funding and liquidation is not finished yet, so treat derivative backtests with care.

How do I trade a BitMEX perpetual contract?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use the full CCXT symbol, which includes the settlement currency after the colon (the ``BASE/QUOTE:SETTLE`` form). Look up the exact symbol in CCXT's BitMEX market list before you trade. Contract quantities are CCXT contract units, and the contract size comes from the exchange. See the perpetual contracts section of :doc:`brokers.ccxt` for leverage, margin mode and long and short legs.

Is the BitMEX path fully tested in LumiBot?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No. BitMEX is available through CCXT and has documented examples, but it has not been exchange tested for live trading by LumiBot. Start with very small orders and check balances, fills, positions and cancels yourself.
