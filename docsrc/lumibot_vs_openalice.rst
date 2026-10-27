LumiBot vs OpenAlice
====================

.. meta::
   :description: Compare LumiBot and OpenAlice for Python trading strategies, AI agents, agentic backtesting, broker integrations, and inspectable execution.

OpenAlice has a strong "one-person Wall Street" concept: an AI agent that
researches, sizes, manages, and exits trades across markets.

Lumibot is different because it starts from the Python strategy and broker
framework. You can build a simple deterministic strategy, a single AI agent, or
a multi-agent trading team, then backtest and inspect that strategy before
moving toward paper or live trading. You can also keep the parts that should
never be left to a model, such as hard risk limits and order sizing rules, in
normal Python code.

At a glance
***********

.. list-table::
   :header-rows: 1
   :widths: 22 39 39

   * -
     - OpenAlice
     - LumiBot
   * - What it is
     - "The AI orchestrator for trading": a local workspace where AI agents research and prepare trades
     - Python framework for trading strategies and AI trading agents
   * - Language and license
     - TypeScript, AGPL-3.0. Desktop app for macOS and Windows, plus a CLI
     - Python, GPL-3.0. ``pip install lumibot``
   * - AI agents
     - Orchestrates agents you already use, such as Claude Code, Codex and OpenCode
     - Built-in agents that call the model you choose per agent, inside the strategy
   * - Live trading
     - Beta. Broker packs for CCXT, Alpaca, IBKR, LeverUp and Longbridge; you approve staged orders
     - Alpaca, Interactive Brokers, Tradier, Schwab, Tradovate, ProjectX, Bitunix, Polymarket and many crypto exchanges through CCXT
   * - Backtesting
     - Includes a simulate tool that tests one entry and one exit rule over dated bars; check its docs for current backtesting features
     - Full strategy backtests with orders, fills, charts, logs and tearsheets

Where OpenAlice Fits
********************

OpenAlice is useful when you want an agent-product concept around a full
trading lifecycle: research, entry, management, risk, and exit.

Where Lumibot Fits
******************

Use Lumibot when you want developer control over the strategy code and the
ability to test the same logic in a backtest and broker context.

Lumibot supports:

- **Copy-paste Python strategy examples** that can be modified like normal
  code.
- **Deterministic gates plus AI reasoning** so a model can advise, debate, or
  choose, while Python enforces the universe, sizing, risk, cash, and order
  rules.
- **Multi-agent teams** such as researcher, bull, bear, reviewer, risk, and
  trader agents.
- **Backtest observability** with charts, orders, logs, traces, replay cache,
  memory, and tearsheets.
- **Supported broker integrations** for paper and live workflows.
- **BotSpot managed runtime** when you want hosted backtests, deployment,
  monitoring, alerts, MCP tools, broker connections, and kill-switch controls.

Why Backtesting Matters
***********************

An AI trader that can research, enter, manage, and exit trades sounds powerful,
but the important question is how it behaves across many market days. Lumibot
lets you test the agent inside a historical trading loop, inspect each decision,
and improve the strategy before connecting it to a broker workflow.

Short Version
*************

OpenAlice is a strong agent-product concept. Lumibot is the Python trading
framework for developers who want code control, backtests, artifacts,
guardrails, and broker paths behind an AI trading agent.

Continue with the :doc:`complete AI-agent quick start <agents_quickstart>` or
compare the :doc:`broker integrations supported by LumiBot <brokers>`.

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

Facts on this page were checked on October 24, 2026 (OpenAlice v0.94.1).

- `OpenAlice source repository and README <https://github.com/TraderAlice/OpenAlice>`_
- `OpenAlice broker packs <https://github.com/TraderAlice/OpenAlice/blob/master/docs/broker-packs.md>`_
- `LumiBot source repository <https://github.com/Lumiwealth/lumibot>`_

Frequently asked questions
**************************

What is OpenAlice?
~~~~~~~~~~~~~~~~~~

OpenAlice is an open source AI trading project that calls itself "the AI
orchestrator for trading" and "your one-person Wall Street." It brings your AI
agents, market tools, research and trading accounts into one local workspace.
See the `OpenAlice GitHub repository <https://github.com/TraderAlice/OpenAlice>`_.

Its README says it comes as a desktop app for macOS and Windows, with a command
line installer for terminals and servers.

Is OpenAlice open source?
~~~~~~~~~~~~~~~~~~~~~~~~~

Yes. The OpenAlice repository on GitHub uses the AGPL 3.0 license and is
written mostly in TypeScript. LumiBot is also open source. It uses the GPL 3.0
license and is written in Python. See the
`LumiBot GitHub repository <https://github.com/Lumiwealth/lumibot>`_.

What is the main difference between LumiBot and OpenAlice?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

OpenAlice is built around an AI agent workspace for research, tracking ideas
and preparing trades. LumiBot starts from the Python strategy and broker
framework. You write a strategy class, and it can be a simple rules strategy,
one AI agent, or a team of agents.

With LumiBot you can backtest that strategy, inspect every decision, and keep
hard rules such as risk limits and order sizing in normal Python code.

Can I backtest an AI trading agent with LumiBot?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes. LumiBot runs the agent inside a historical trading loop. You can then look
at the orders, logs, traces, charts and tear sheets to see what the agent did on
each day. See :doc:`agents_quickstart` for a complete example.

Can OpenAlice place real trades?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The OpenAlice README says you can connect supported brokers through its
Unified Trading Account. With AI trading turned on, agents can stage proposed
orders, and you review and approve them before they run. The README also says
trading execution is in beta and suggests starting with simulator, paper, demo
or testnet accounts.

Do I need a broker account to try LumiBot or OpenAlice?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

No for both. The OpenAlice README says you do not need a broker account to
start researching. In LumiBot you can run a backtest on historical data with no
broker account, then connect a broker later. See :doc:`getting_started`.

Which AI models or agents can I use?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

OpenAlice works with agents you already use, such as Claude Code, Codex and
OpenCode, according to its README. LumiBot agents call a model you pick per
agent, including OpenAI, Anthropic Claude, Google Gemini and xAI Grok models.
See :doc:`agents` for the list.

Where do LumiBot and OpenAlice run?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

OpenAlice runs on your own machine by default. LumiBot runs wherever you run
Python. If you want a hosted setup, BotSpot adds hosted backtests, deployment,
monitoring and broker connections around LumiBot. See :doc:`botspot_mcp`.
