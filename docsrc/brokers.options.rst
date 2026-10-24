Options Brokers for LumiBot
===========================

.. meta::
   :description: Python options trading bots with LumiBot on Alpaca, Charles Schwab, Interactive Brokers and Tradier. Compare paper trading, multi-leg support and logins.

Trade stock and ETF options from a LumiBot strategy or AI agent with one of these brokers. The strategy code stays the same; only the broker credentials change.

.. include:: _snippets/botspot_mcp_callout.rst

.. list-table:: Options brokers
   :header-rows: 1
   :widths: 30 35 35

   * - Broker
     - Also trades
     - Paper trading
   * - :doc:`Alpaca <brokers.alpaca>`
     - Stocks, ETFs, crypto
     - Yes, free paper account
   * - :doc:`Charles Schwab <brokers.schwab>`
     - Stocks, ETFs
     - No separate paper account
   * - :doc:`Interactive Brokers <brokers.interactive_brokers>`
     - Stocks, futures, forex
     - Yes, IBKR paper account
   * - :doc:`Interactive Brokers (legacy TWS) <brokers.interactive_brokers_legacy>`
     - Stocks, futures, forex
     - Yes, IBKR paper account
   * - :doc:`Tradier <brokers.tradier>`
     - Stocks, ETFs
     - Yes, sandbox account

.. toctree::
   :maxdepth: 1
   :hidden:

   Alpaca <brokers.alpaca>
   Charles Schwab <brokers.schwab>
   Interactive Brokers <brokers.interactive_brokers>
   Interactive Brokers (legacy TWS) <brokers.interactive_brokers_legacy>
   Tradier <brokers.tradier>

To backtest an options strategy, see :doc:`brokers.data_sources`.

Frequently asked questions
--------------------------

Which brokers can a LumiBot strategy trade options with?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Alpaca, Charles Schwab, Interactive Brokers (REST and legacy TWS) and Tradier. Each guide shows the keys or login the broker needs.

Can I paper trade options before using real money?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes on Alpaca, Tradier and Interactive Brokers, which all offer paper or sandbox accounts. Schwab has no separate paper account.

Do I need to change my strategy code to switch options brokers?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No. The strategy uses the same LumiBot order and asset methods on every broker. You change the broker settings, not the strategy.

Can an AI agent trade options with LumiBot?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes. A LumiBot AI agent strategy can research and place options orders on any of these brokers. See :doc:`agents` for how AI agent strategies work.

How do I backtest an options strategy?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use a backtesting data source with options history, listed on :doc:`brokers.data_sources`. You do not need a broker account to backtest.
