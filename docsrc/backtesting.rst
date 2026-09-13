Backtesting Trading Strategies in Python with LumiBot
*****************************************************

.. meta::
   :description: Backtest Python trading strategies with LumiBot using Yahoo, ThetaData, Polygon, Databento, Interactive Brokers, Polymarket, or your own data.

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
   * - Prediction contracts
     - Polymarket
     - Market price history
     - Polymarket market identifiers

Use Yahoo for the simplest free daily-stock example. Use ThetaData when a stock
or option strategy needs intraday history, and use Pandas when you already own
the data and can prepare it in LumiBot's input format.

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
   backtesting.tearsheet_html
   backtesting.trades_files
   backtesting.indicators_files
   backtesting.logs_csv
