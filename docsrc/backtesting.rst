Backtesting Trading Strategies in Python with LumiBot
*****************************************************

.. meta::
   :description: Backtest Python trading strategies with LumiBot using Yahoo, ThetaData, Polygon, Databento, Interactive Brokers, Polymarket, Kalshi, or your own data.

.. include:: _snippets/botspot_mcp_callout.rst

Choose a backtesting source from the strategy's asset class and required bar
interval:

.. list-table:: LumiBot backtesting data choices
   :header-rows: 1
   :widths: 22 22 24 32

   * - Need
     - Start with
     - Typical granularity
     - Setup
   * - Daily stocks and ETFs
     - :doc:`Yahoo <backtesting.yahoo>`
     - Daily
     - No data-provider credentials
   * - Intraday stocks and options
     - :doc:`ThetaData <backtesting.thetadata>`
     - Minute, hour, and daily
     - Data Downloader and provider access
   * - Stocks, options, forex, or crypto
     - :doc:`Polygon.io <backtesting.polygon>`
     - Intraday and daily
     - Polygon.io API key
   * - Futures and market-data schemas
     - :doc:`Databento <backtesting.databento>`
     - Tick through daily, by dataset
     - Databento API key and dataset access
   * - Your own stock or futures data
     - :doc:`Pandas <backtesting.pandas>`
     - Whatever the supplied file contains
     - Local data prepared in LumiBot's format
   * - Interactive Brokers history
     - :doc:`IBKR REST <backtesting.ibkr>`
     - Provider-supported intervals
     - Client Portal and Data Downloader access
   * - Options with your own Alpaca account
     - :doc:`Alpaca <backtesting.alpaca>`
     - Minute and daily
     - Alpaca API key (free accounts include option history from about February 2024)
   * - Prediction contracts
     - Polymarket
     - Market price history
     - Polymarket market identifiers
   * - Selected YES/NO prediction contracts
     - Kalshi
     - Completed minute, hour, or daily candles
     - Market ticker plus selected outcome; public history needs no trading key

Use Yahoo for the simplest free daily-stock example. Use ThetaData when a stock
or option strategy needs intraday history, and use Pandas when you already own
the data and can prepare it in LumiBot's input format.

Select the provider with ``BACKTESTING_DATA_SOURCE`` or pass a data-source class
in Python. **An environment setting takes precedence over the class in code.**
See :ref:`Choose your backtest data <backtest-data-source-selection>` for examples,
defaults, and help with an unexpected provider.

Managed Backtesting on BotSpot
==============================

Backtesting is better on `BotSpot <https://botspot.trade/sales?showLogin=1&utm_source=documentation&utm_medium=backtesting&utm_campaign=lumibot&utm_content=managed_backtesting_text&sample=lumibot_deploy_sample>`_ when you want to move faster than a local setup. BotSpot already has the workflow around Lumibot: hosted data setup, parallel backtest workers, generated artifacts, charts, logs, and the path from a passing backtest into paper or live trading.

- **Backtesting data included.** Use supported hosted stock, futures, options, macro, filings, and other data sources without sourcing every vendor, API key, downloader, and local file yourself. Some data is included; premium datasets can be much cheaper than buying direct subscriptions.
- **Parallel experiments.** Launch multiple strategy variants on BotSpot servers and compare results instead of waiting for one local run at a time.
- **Better artifacts.** Inspect charts, trades, logs, files, decisions, and audit history from one place instead of stitching together local output folders.
- **Lumibot-tuned iteration.** BotSpot's AI workflows and MCP tools understand Lumibot strategy structure, so Codex, Claude Code, Cursor, and other agents can run backtests and inspect results instead of only editing Python.
- **Ready for deployment.** A strategy that survives backtesting can move into paper or live trading with supported broker connections, monitoring, alerts, and kill-switch controls already available.

.. image:: ../docs/assets/readme/cta_deploy_on_botspot.png
   :alt: Try backtesting a sample Lumibot strategy on BotSpot
   :align: center
   :width: 520px
   :target: https://botspot.trade/sales?showLogin=1&utm_source=documentation&utm_medium=backtesting&utm_campaign=lumibot&utm_content=managed_backtesting_button&sample=lumibot_deploy_sample

Agentic Backtesting
===================

Lumibot also supports **agentic backtesting**. A strategy can create one or more AI agents, run them from normal lifecycle methods, analyze point-in-time data with DuckDB, and replay identical agent runs from cache on the next backtest instead of paying for another model call.

This matters if you want:

- an **AI trading agent** that makes decisions inside ``on_trading_iteration()``
- an **LLM trading bot** that can also be tested historically
- external **MCP tools** attached to a strategy
- backtest/live parity for agent-driven strategies

See :doc:`agents` for the full agent runtime guide and usage examples.

Files Generated from Backtesting
================================

When you run a backtest, several important files are generated, each prefixed by the strategy name and the date. These files provide detailed insights into the performance and behavior of the strategy.

.. toctree::
   :maxdepth: 2
   :caption: Contents:

   backtesting.how_to_backtest
   backtesting.backtesting_function
   backtesting.performance
   backtesting.yahoo
   backtesting.pandas
   backtesting.polygon
   backtesting.databento
   backtesting.thetadata
   backtesting.ibkr
   backtesting.alpaca
   backtesting.tearsheet_html
   backtesting.trades_files
   backtesting.indicators_files
   backtesting.logs_csv

Learn to build and backtest with Rob
------------------------------------

Learn with Rob Grzesik, creator of LumiBot. Join the FREE AI challenge.

.. image:: ../docs/assets/ai-trading/rob-challenge-backtest.png
   :alt: Rob Grzesik, creator of LumiBot. Join the FREE AI challenge.
   :width: 640px
   :align: center
   :class: lumibot-learning-image
   :target: https://botspot.trade/challenges?utm_source=documentation&utm_medium=docs&utm_campaign=lumibot_ai_trading&utm_content=backtesting_challenge_image

Frequently asked questions
==========================

Which data sources can LumiBot backtest with?
---------------------------------------------

LumiBot can backtest with Yahoo, ThetaData, Polygon.io, Databento, Interactive Brokers (REST), Alpaca, Polymarket, Kalshi, CCXT crypto exchanges, and your own data through the Pandas backtester. The table at the top of this page shows which one fits each need.

Each source has its own page with setup steps, for example :doc:`backtesting.yahoo`, :doc:`backtesting.thetadata` and :doc:`backtesting.pandas`.

Which asset classes can I backtest in LumiBot?
----------------------------------------------

You can backtest stocks and ETFs, options, futures (including continuous futures), forex, crypto and prediction contracts. What you can test depends on the data source you pick. For example, Yahoo covers daily stocks and ETFs only, while Databento and Interactive Brokers cover futures.

Can I backtest for free?
------------------------

Yes. :doc:`Yahoo <backtesting.yahoo>` daily stock backtests need no data provider credentials. The :doc:`Pandas <backtesting.pandas>` backtester uses data you already have. A free :doc:`Alpaca <backtesting.alpaca>` account also includes option history from about February 2024.

Other providers may need a paid plan for more history or faster downloads. Check each provider's own site for current plans.

How do I pick the data source for a backtest?
---------------------------------------------

Pass a data source class in Python, such as ``YahooDataBacktesting``, or set ``BACKTESTING_DATA_SOURCE``. Accepted names include ``yahoo``, ``thetadata``, ``polygon``, ``databento``, ``ibkr``, ``alpaca``, ``ccxt`` and ``polymarket``.

When ``BACKTESTING_DATA_SOURCE`` is set, it wins over the class in your code. If you set neither, LumiBot uses ThetaData. See :ref:`Choose your backtest data <backtest-data-source-selection>` for more examples.

Can I use different data sources for different assets in one backtest?
----------------------------------------------------------------------

Yes. Set ``BACKTESTING_DATA_SOURCE`` to a JSON map that names a source for each asset type, for example ThetaData for stocks and options and Interactive Brokers for futures. The :doc:`IBKR page <backtesting.ibkr>` shows a full example.

How do I add commissions and slippage to a backtest?
----------------------------------------------------

Pass ``buy_trading_fees`` and ``sell_trading_fees`` (lists of ``TradingFee`` objects) to ``run_backtest()``. A fee can be flat, a percent of the order, or a per contract amount.

For slippage, pass ``buy_trading_slippages`` and ``sell_trading_slippages`` with ``TradingSlippage`` objects. See :doc:`entities.trading_fee` and :doc:`entities.trading_slippage`.

Can I backtest crypto?
----------------------

Yes. Set ``BACKTESTING_DATA_SOURCE=ccxt`` to use the CCXT crypto backtester, or use Polygon.io, ThetaData or Interactive Brokers when they carry the coins you need. You can also load your own crypto bars with the Pandas backtester.

Can an AI agent run inside a backtest?
--------------------------------------

Yes. A strategy can create AI agents, call them from normal lifecycle methods such as ``on_trading_iteration()``, and attach MCP tools. Identical agent runs are replayed from cache on the next backtest, so you do not pay for the same model call twice. See :doc:`agents`.

What files does a backtest create?
----------------------------------

A backtest writes files named after the strategy and the date. They include an HTML tearsheet, trade files, indicator files and a CSV log. See :doc:`backtesting.tearsheet_html`, :doc:`backtesting.trades_files`, :doc:`backtesting.indicators_files` and :doc:`backtesting.logs_csv`.

Do backtest results tell me how a strategy will do live?
--------------------------------------------------------

No. A backtest replays past data with simulated fills. Live results can be different because of real fills, costs, missing data and changing markets. Use a backtest to find problems and compare ideas, not as a forecast.
