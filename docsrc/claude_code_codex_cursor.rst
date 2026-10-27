Use LumiBot with Claude Code, Codex, Cursor and VS Code
=======================================================

.. meta::
   :description: Install the free LumiBot trading skills in Claude Code, Codex or Cursor with one command, then connect the free BotSpot MCP so your AI can run backtests in the cloud.

Claude Code, Codex, Cursor, VS Code and other coding assistants can help you write a LumiBot ``Strategy``, test it on
historical data and inspect its trades. The free **LumiBot trading skills** teach
it the framework's methods, strategy lifecycle and backtesting checks.

Install the skills for local development, then connect the **free BotSpot MCP**
if you want your assistant to use hosted strategy-building and backtesting tools.
Connecting is free; hosted features and usage depend on your BotSpot plan.

Step 1: install the trading skills (one command)
------------------------------------------------

The skills live in the public
`lumibot-trading-skills repository <https://github.com/Lumiwealth/lumibot-trading-skills>`_.
They cover stocks, ETFs, options, crypto, futures, indicators and AI agents that
trade.

**Claude Code.** Type these inside Claude Code:

.. code-block:: text

   /plugin marketplace add Lumiwealth/lumibot-trading-skills
   /plugin install lumibot-trading-skills@lumiwealth

Or run the same thing from your shell:

.. code-block:: bash

   claude plugin marketplace add Lumiwealth/lumibot-trading-skills
   claude plugin install lumibot-trading-skills@lumiwealth

**Codex, Cursor, Claude Code and other agents.** Use the
`skills <https://skills.sh>`_ command line tool. It needs Node 22 or newer:

.. code-block:: bash

   npx skills add Lumiwealth/lumibot-trading-skills

Add ``-g`` to install for every project, or ``-a codex``, ``-a cursor`` or
``-a claude-code`` to pick one assistant.

**Codex by hand.** Codex reads personal skills from ``~/.agents/skills``:

.. code-block:: bash

   git clone https://github.com/Lumiwealth/lumibot-trading-skills.git
   mkdir -p ~/.agents/skills
   cp -R lumibot-trading-skills/skills/* ~/.agents/skills/

Restart your assistant if the skills do not show up right away.

What the skills teach
~~~~~~~~~~~~~~~~~~~~~

* ``lumibot-strategy-building``: start here. Write, fix and backtest any LumiBot strategy.
* ``lumibot-api-reference``: exact method names, lifecycle methods and working examples.
* ``stock-etf-strategies`` and ``options-strategies``: moving averages, rotation, covered calls, the wheel, spreads and iron condors.
* ``lumibot-indicators``: SMA, RSI, MACD, Bollinger Bands, and plotting them on the backtest chart.
* ``lumibot-ai-agents``: strategies where AI agents research and one AI agent trades.
* ``fast-lumibot-strategies``: faster strategies and big scanners.
* ``using-botspot-mcp``: building, backtesting and running strategies in the cloud.

See the `repository <https://github.com/Lumiwealth/lumibot-trading-skills>`_
for the current list.

Step 2: connect the free BotSpot MCP
------------------------------------

.. include:: _snippets/botspot_mcp_callout.rst

Step 3: ask for your first strategy
-----------------------------------

Try one of these:

* "Build me a moving average strategy for SPY and backtest it."
* "Write a covered call strategy on QQQ, backtest the last two years and show me the trades."
* "Build an AI trading bot where one agent researches the news and another agent decides whether to buy SPY."

With only the skills installed, your assistant writes the strategy file and you
run it on your machine with ``pip install lumibot``. With the BotSpot MCP
connected, it can also run the backtest in the cloud and show you the results.

Placing real trades with your own broker always needs your approval on
botspot.trade first. Backtests are simulations and do not promise future returns.
These skills teach an AI to write code. They are not investment advice.

Running everything on your own machine
--------------------------------------

You do not need BotSpot to use LumiBot. Everything in LumiBot runs locally. See
:doc:`getting_started` for a Python backtest, :doc:`agents_quickstart` for an AI
backtest, and :doc:`agent_start_here` for the exact starting points a coding
assistant should read. :doc:`botspot_mcp` explains everything the hosted
connection adds.
