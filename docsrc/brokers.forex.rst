Forex Brokers for LumiBot
=========================

.. meta::
   :description: Python forex trading bots with LumiBot on Interactive Brokers. Trade currency pairs from a LumiBot strategy or AI agent.

Trade currency pairs from a LumiBot strategy or AI agent through Interactive Brokers. LumiBot treats a currency pair as a ``forex`` asset.

.. include:: _snippets/botspot_mcp_callout.rst

.. list-table:: Forex brokers
   :header-rows: 1
   :widths: 40 30 30

   * - Broker
     - Also trades
     - Paper trading
   * - :doc:`Interactive Brokers <brokers.interactive_brokers>`
     - Stocks, options, futures
     - Yes, IBKR paper account
   * - :doc:`Interactive Brokers (legacy TWS) <brokers.interactive_brokers_legacy>`
     - Stocks, options, futures
     - Yes, IBKR paper account

.. toctree::
   :maxdepth: 1
   :hidden:

   Interactive Brokers <brokers.interactive_brokers>
   Interactive Brokers (legacy TWS) <brokers.interactive_brokers_legacy>

Frequently asked questions
--------------------------

Which brokers support forex in LumiBot?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Interactive Brokers, through either the REST connection or the legacy TWS connection.

How do I create a forex asset in LumiBot?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create the base currency as ``Asset(symbol="EUR", asset_type=Asset.AssetType.FOREX)`` and pass the quote currency to the order, for example ``self.create_order(eur, 10000, "buy", quote=Asset("USD", asset_type=Asset.AssetType.FOREX))``.

Can I paper trade forex?
~~~~~~~~~~~~~~~~~~~~~~~~

Yes. Use an Interactive Brokers paper account and the same strategy code.

Can an AI agent trade forex with LumiBot?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes. A LumiBot AI agent strategy can place forex orders on Interactive Brokers. See :doc:`agents`.

Do Alpaca, Schwab or Tradier support forex in LumiBot?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No. In LumiBot today, forex trading goes through Interactive Brokers only.
