Stock and Options Brokers for LumiBot
=====================================

.. meta::
   :description: Python algorithmic trading for stocks, ETFs and options with LumiBot on Alpaca, Charles Schwab, Interactive Brokers and Tradier. Compare paper trading, keys and assets.

Trade US stocks, ETFs and options from a LumiBot strategy or AI agent with one of these brokers. The strategy code stays the same; only the broker credentials change.

.. include:: _snippets/botspot_mcp_callout.rst

.. list-table:: Stock and options brokers
   :header-rows: 1
   :widths: 24 30 22 24

   * - Broker
     - Assets in LumiBot
     - Paper trading
     - Login
   * - :doc:`Alpaca <brokers.alpaca>`
     - Stocks, ETFs, options, crypto
     - Yes, free paper account
     - API key and secret, or OAuth
   * - :doc:`Charles Schwab <brokers.schwab>`
     - Stocks, ETFs, single-leg options
     - No paper switch in LumiBot
     - Schwab developer app and OAuth
   * - :doc:`Interactive Brokers (Client Portal REST) <brokers.interactive_brokers>`
     - Stocks, options, futures and more, by account
     - Yes, separate paper login
     - Username and password with 2FA
   * - :doc:`Interactive Brokers (legacy TWS) <brokers.interactive_brokers_legacy>`
     - Markets your TWS account can trade
     - Yes, paper port 7497
     - TWS or IB Gateway
   * - :doc:`Tradier <brokers.tradier>`
     - Stocks, ETFs, options
     - Yes, sandbox account
     - Access token and account number

.. toctree::
   :maxdepth: 1
   :hidden:

   Alpaca <brokers.alpaca>
   Charles Schwab <brokers.schwab>
   Interactive Brokers <brokers.interactive_brokers>
   Interactive Brokers (legacy TWS) <brokers.interactive_brokers_legacy>
   Tradier <brokers.tradier>

Frequently asked questions
--------------------------

Which stock broker is easiest to start with?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Alpaca. You can open a free paper account, create an API key in the dashboard, set ``ALPACA_API_KEY`` and ``ALPACA_API_SECRET``, and run. Paper mode is the default.

Which brokers can trade options with LumiBot?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Alpaca, Charles Schwab (single-leg options), Interactive Brokers and Tradier.

Can I backtest a stock or options strategy before connecting a broker?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes. Use Yahoo for free daily stock data, or Alpaca, Polygon, ThetaData or Interactive Brokers data for intraday and options history. See :doc:`backtesting`.

Can an AI agent trade stocks and options through these brokers?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes. An AI agent submits orders through the same LumiBot order pipeline, so it can use any of these brokers. See :doc:`agents`.

Can I switch brokers without rewriting my strategy?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes. The strategy class stays the same. You change the broker settings in your ``.env`` file, as long as the new broker can trade the assets your strategy uses.

How do I switch Alpaca from paper trading to live trading?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Alpaca uses paper trading unless you set ``ALPACA_IS_PAPER=false`` and use your live account keys. See the :doc:`Alpaca guide <brokers.alpaca>`.
