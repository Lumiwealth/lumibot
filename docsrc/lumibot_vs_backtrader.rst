LumiBot vs Backtrader
=====================

.. meta::
   :description: Compare LumiBot and Backtrader, two open-source Python backtesting and trading frameworks: assets, brokers, live trading, AI agents, maintenance, license and when to choose each.

Backtrader is one of the best-known Python backtesting libraries. LumiBot is a
Python framework for backtesting and live trading that also runs AI agents inside
the strategy. Both are open source and both use a strategy class with a
``next``-style loop, so moving between them is natural.

At a glance
***********

.. list-table::
   :header-rows: 1
   :widths: 22 39 39

   * -
     - Backtrader
     - LumiBot
   * - What it is
     - Python backtesting library with live trading support
     - Python framework for trading strategies and AI trading agents
   * - License
     - GPL-3.0
     - GPL-3.0
   * - Backtesting
     - Yes, event-driven, 122 built-in indicators, TA-Lib support, resampling and replay, many order types
     - Yes, with charts, trade files, logs and tearsheets
   * - Live trading
     - Interactive Brokers (needs IbPy, not on PyPI), Oanda and Visual Chart
     - Alpaca, Interactive Brokers, Tradier, Schwab, Tradovate, ProjectX, Bitunix, Polymarket and many crypto exchanges through CCXT
   * - Assets
     - Not listed by asset class; examples cover stocks, futures and forex
     - Stocks, ETFs, options, futures, forex, crypto and prediction markets
   * - AI agents
     - None built in
     - Built-in LLM agents that research, call tools and place orders. See :doc:`agents`.
   * - Recent activity
     - Last commit and PyPI release on April 19, 2023 (version 1.9.78.123)
     - Released regularly; see the GitHub releases page
   * - Hosted option
     - Self-hosted
     - Self-hosted, or BotSpot for hosted backtests and deployment

When to choose Backtrader
*************************

Choose Backtrader if you want a stable, self-contained backtesting library with
very few dependencies and a large set of indicators, and you are happy with its
current feature set. Many tutorials and books use it.

When to choose LumiBot
**********************

Choose LumiBot if you want active development, options and broker support beyond
Interactive Brokers and Oanda, or AI agents that make decisions inside a
backtest you can inspect. The same strategy class runs in a backtest, in paper
trading and live.

Moving from Backtrader to LumiBot
*********************************

There is a step-by-step guide: :doc:`MIGRATING_FROM_BACKTRADER`. In short,
Backtrader's ``next()`` becomes LumiBot's ``on_trading_iteration()``, and data
feeds become a backtesting data source or a broker.

Try LumiBot with your AI assistant
**********************************

The fastest way to try LumiBot is to let your AI coding assistant do the setup.
Install the free
`LumiBot trading skills <https://github.com/Lumiwealth/lumibot-trading-skills>`_
in Claude Code, Codex or Cursor with one command, then connect the free BotSpot
MCP so your assistant can run backtests in the cloud. See
:doc:`claude_code_codex_cursor` for the exact commands.

.. include:: _snippets/botspot_mcp_callout.rst

Sources
*******

Facts on this page were checked on October 24, 2026, against each project's own
repository and package page. Backtests are simulations and do not promise future
returns.

- `Backtrader source repository and README <https://github.com/mementum/backtrader>`_
- `Backtrader on PyPI <https://pypi.org/project/backtrader/>`_
- `Backtrader documentation <https://www.backtrader.com/>`_
- `LumiBot source repository <https://github.com/Lumiwealth/lumibot>`_
