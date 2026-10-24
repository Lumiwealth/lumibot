Use LumiBot with Claude Code, Codex and Cursor
==============================================

.. meta::
   :description: Install the free LumiBot trading skills in Claude Code, Codex or Cursor with one command, then connect the free BotSpot MCP so your AI can run backtests in the cloud.

.. include:: _snippets/botspot_mcp_install_buttons.rst

Ask your AI coding assistant to build a trading bot and it will write code. Give
it the free **LumiBot trading skills** and it writes code that actually runs:
correct LumiBot method names, real backtests, and the checks that catch a
backtest that lies.

Then connect the free **BotSpot MCP** and your assistant can run that backtest
in the cloud, read the charts and trades, and paper trade it. No Python setup and
no data subscription on your machine.

.. include:: _snippets/botspot_mcp_callout.rst

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

`BotSpot <https://botspot.trade/agents?utm_source=documentation&utm_medium=assistant_page&utm_campaign=lumibot&utm_content=step2>`__
is the hosted service around LumiBot. Connecting is free and creates a free
BotSpot account. Your assistant can then build strategies, run backtests on
BotSpot's servers with the data already set up, read the charts, trades and logs,
and paper trade.

The MCP address is ``https://mcp.botspot.trade/mcp``. Sign in through the normal
BotSpot sign-in when your assistant asks. Never paste a password or key into a
chat.

**Claude Code:**

.. code-block:: bash

   claude mcp add --transport http botspot https://mcp.botspot.trade/mcp

Then run ``/mcp`` inside Claude Code to finish signing in.

**Codex:**

.. code-block:: bash

   codex mcp add botspot --url https://mcp.botspot.trade/mcp
   codex mcp login botspot

**Cursor.** Add this to your MCP configuration, then use Cursor's sign-in control:

.. code-block:: json

   {
     "mcpServers": {
       "botspot": {
         "url": "https://mcp.botspot.trade/mcp"
       }
     }
   }

**Claude and ChatGPT apps.** Add a custom connector with the same address. The
`BotSpot MCP setup guide <https://github.com/Lumiwealth/botspot-mcp#connect>`_
has the current steps for each app.

`Connect the BotSpot MCP free <https://botspot.trade/agents?utm_source=documentation&utm_medium=assistant_page&utm_campaign=lumibot&utm_content=connect_button>`__

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
