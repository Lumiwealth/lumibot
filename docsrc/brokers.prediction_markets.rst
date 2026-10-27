Prediction Market Brokers for LumiBot
=====================================

.. meta::
   :description: Build a Polymarket trading bot in Python with LumiBot. Trade and backtest prediction contracts with the same strategy code.

Trade and backtest prediction contracts from a LumiBot strategy or AI agent. A prediction contract is priced between 0 and 1 and pays out based on how an event resolves.

.. include:: _snippets/botspot_mcp_callout.rst

.. list-table:: Prediction market brokers
   :header-rows: 1
   :widths: 25 35 40

   * - Market
     - What LumiBot does there
     - Login
   * - :doc:`Polymarket <brokers.polymarket>`
     - Live trading, order books and historical price backtests
     - Wallet private key, set as ``POLYMARKET_PRIVATE_KEY``

.. toctree::
   :maxdepth: 1
   :hidden:

   Polymarket <brokers.polymarket>

Frequently asked questions
--------------------------

Which prediction markets does LumiBot support?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Polymarket. LumiBot has a ``Polymarket`` broker for live trading, a ``PolymarketData`` data source and ``PolymarketBacktesting`` for historical prices.

Can I backtest a prediction market strategy?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes, with Polymarket price history. See the :doc:`Polymarket guide <brokers.polymarket>`.

How is a prediction contract priced?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Each outcome token is a ``prediction_contract`` asset priced between 0 and 1 in USD collateral.

Can an AI agent trade on Polymarket?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes. AI agents use the same LumiBot order path as Python strategies. See :doc:`agents`.

Does LumiBot support Kalshi?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Not today. LumiBot has no Kalshi broker. Polymarket is the supported prediction market.
