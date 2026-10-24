LumiBot vs freqtrade
====================

.. meta::
   :description: Compare LumiBot and freqtrade, two open-source Python trading bot frameworks: assets, brokers and exchanges, backtesting, AI agents, live trading, license and when to choose each.

freqtrade and LumiBot are both free, open-source Python trading frameworks with
backtesting and live trading. The biggest difference is scope. freqtrade is
built for crypto exchanges. LumiBot covers stocks, options, futures, forex,
crypto and prediction markets, and lets AI agents make decisions inside the
strategy.

Both are good projects. Pick the one that fits the markets you trade and how you
want to build.

At a glance
***********

.. list-table::
   :header-rows: 1
   :widths: 22 39 39

   * -
     - freqtrade
     - LumiBot
   * - What it is
     - Open-source crypto trading bot, controlled through Telegram or a web UI
     - Open-source Python framework for trading strategies and AI trading agents
   * - Language
     - Python 3.12+
     - Python 3.10+
   * - License
     - GPL-3.0
     - GPL-3.0
   * - Assets
     - Crypto: spot, plus futures and leverage on supported exchanges
     - Stocks, ETFs, options, futures, forex, crypto and prediction markets
   * - Live trading
     - Yes. Spot on Binance, BingX, Bitget, Bybit, Gate, HTX, Hyperliquid, Kraken and OKX; futures on Binance, Bitget, Bybit, Gate, Hyperliquid, Kraken and OKX. Other exchanges may work through CCXT.
     - Yes. Alpaca, Interactive Brokers, Tradier, Schwab, Tradovate, ProjectX, Bitunix, Polymarket and many crypto exchanges through CCXT. See :doc:`brokers`.
   * - Backtesting
     - Yes, plus hyperopt parameter search, lookahead analysis and recursive analysis
     - Yes, with charts, trade files, logs and tearsheets. See :doc:`backtesting`.
   * - AI and machine learning
     - FreqAI, adaptive machine learning models that retrain on market data
     - Built-in LLM agents that research, call tools and place orders inside the strategy. See :doc:`agents`.
   * - Dry run / paper
     - Dry-run mode
     - Paper trading through supported brokers
   * - Hosted option
     - Self-hosted
     - Self-hosted, or BotSpot for hosted backtests and deployment

When to choose freqtrade
************************

Choose freqtrade if you only trade crypto and want a mature bot you can run and
control from Telegram or a web UI. Its hyperopt tools and FreqAI machine
learning module are strong for tuning crypto strategies. It has a very large
community.

When to choose LumiBot
**********************

Choose LumiBot if you trade more than crypto, for example stocks, options or
futures, or want one strategy style across several brokers. Choose it if you
want large language model agents to research news, filings or prices and decide
trades inside a backtest you can inspect. Strategies are normal Python classes,
and hard rules such as position limits and order sizing can stay in Python.

Moving from freqtrade to LumiBot
********************************

A freqtrade strategy works on a dataframe of candles and sets entry and exit
signals. In LumiBot you write a ``Strategy`` class and place orders in
``on_trading_iteration``. You can still use pandas and indicator libraries.
Start with :doc:`getting_started` and the crypto broker list in
:doc:`brokers.crypto`.

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
repository. Neither project guarantees profitable trading; backtests are
simulations.

- `freqtrade source repository and README <https://github.com/freqtrade/freqtrade>`_
- `freqtrade documentation <https://www.freqtrade.io/>`_
- `LumiBot source repository <https://github.com/Lumiwealth/lumibot>`_
