Futures Brokers for LumiBot
===========================

.. meta::
   :description: Python futures trading bots with LumiBot on Tradovate, TopstepX (ProjectX) and Interactive Brokers. Compare demo accounts, logins and market data.

Trade CME futures from a LumiBot strategy or AI agent with one of these brokers. The strategy code stays the same; only the broker settings change.

.. include:: _snippets/botspot_mcp_callout.rst

.. list-table:: Futures brokers
   :header-rows: 1
   :widths: 26 30 22 22

   * - Broker
     - What LumiBot trades there
     - Demo or paper
     - Market data
   * - :doc:`Tradovate <brokers.tradovate>`
     - CME futures
     - Yes, ``TRADOVATE_IS_PAPER=true`` (the default)
     - Not included; use a separate data source
   * - :doc:`TopstepX through ProjectX <brokers.projectx>`
     - Futures only
     - Your TopstepX account
     - Through ProjectX
   * - :doc:`Interactive Brokers <brokers.interactive_brokers>`
     - Futures, plus stocks, options and forex
     - Yes, separate paper login
     - Your IBKR market data subscriptions

.. toctree::
   :maxdepth: 1
   :hidden:

   Tradovate <brokers.tradovate>
   TopstepX (ProjectX) <brokers.projectx>

Interactive Brokers sits under :doc:`brokers.stocks_options` in the sidebar because it also trades stocks and options.

To backtest a futures strategy before you connect a broker, see :doc:`Databento <backtesting.databento>`, :doc:`Interactive Brokers history <backtesting.ibkr>` and :doc:`futures`.

Frequently asked questions
--------------------------

Which brokers can trade futures with LumiBot?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Tradovate, TopstepX through ProjectX, and Interactive Brokers.

Does Tradovate give LumiBot market data?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No. Tradovate places your orders, but you need a separate data source for prices. The :doc:`Tradovate guide <brokers.tradovate>` shows how to set one up.

Can I test a futures bot without real money?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes. Tradovate uses its demo environment unless you set ``TRADOVATE_IS_PAPER=false``, and Interactive Brokers has paper accounts. You can also backtest first with Databento or Interactive Brokers history.

Can I backtest futures with LumiBot?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes. See :doc:`backtesting.databento` and :doc:`backtesting.ibkr`. Backtests are simulations, and live results can differ.

Can an AI agent trade futures?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes. An AI agent sends orders through the same LumiBot order path as a Python strategy, so it can use any broker on this page. See :doc:`agents`.
