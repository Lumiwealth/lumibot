Agent Built-In Tools
====================

LumiBot agents are useful because they can inspect the same strategy state that
your Python code can inspect. Built-in tools are added automatically when an
agent is created, so a strategy author does not need to manually wire common
market, account, research, memory, or notification tools.

The important design rule is simple:

- Research agents can inspect evidence.
- Trading agents can inspect evidence and mutate orders.
- Backtests expose only data available at the simulated strategy datetime.

Trading Permissions
-------------------

Use ``allow_trading=False`` for any agent that should research, summarize, or
review without changing broker state.

.. code-block:: python

   self.agents.create(
       name="researcher",
       model="openai/gpt-5.4-mini",
       allow_trading=False,
       system_prompt="Gather market data, indicators, news, filings, fundamentals, and macro context.",
   )

With ``allow_trading=False``, LumiBot removes only tools that mutate orders:

- submit order
- modify order
- cancel order

Read-only tools remain available. A research agent can still inspect cash,
positions, open orders, historical prices, indicators, news, SEC filings, FRED
macro data, memory, and notifications.

.. image:: ../docs/assets/ai_committee/docs_tool_permissions.png
   :alt: Lumibot agent tool permissions

Use ``allow_trading=True`` only for the final agent that is allowed to place or
change orders. In an investment committee workflow, that is usually the
portfolio manager or trader agent.

Market And Account State
------------------------

These tools let agents understand what the strategy already knows:

- current datetime
- cash and portfolio value
- positions
- open orders
- historical bars and market data
- account and broker context available to the strategy

These tools are read-only. They remain available even when
``allow_trading=False``.

Technical Indicators
--------------------

Indicator tools expose LumiBot's indicator system to agents:

- ``list_indicators``
- ``get_indicator``
- ``get_indicators``

In backtests, indicators are evaluated against the visible historical data and
return the value at or before the current strategy datetime. This prevents the
agent from seeing a future indicator value.

SEC Fundamentals And Filings
----------------------------

SEC tools use public SEC EDGAR data directly and cache responses locally. They
do not require an API key.

Common tools include:

- ``get_income_statement``
- ``get_balance_sheet``
- ``get_cash_flow``
- ``get_company_facts``
- ``get_filings``
- ``search_filing``
- ``get_filing_document``

Backtests gate filings by filed date or acceptance timestamp, so an agent cannot
read a filing before it existed. Use ``search_filing`` before
``get_filing_document`` when the filing is large and the agent only needs a
specific section.

FRED Macro Data
---------------

FRED tools expose macroeconomic series to agents:

- ``list_fred_series``
- ``get_fred_series``
- ``get_fred_latest``
- ``get_fred_snapshot``

For strict point-in-time backtests, set ``FRED_API_KEY`` so LumiBot can request
FRED/ALFRED vintage observations using realtime parameters. Without a key,
curated public CSV mode can be used in live runs, but it may contain revised
values and is not exposed by default in point-in-time backtests.

News
----

If Alpaca credentials are configured, LumiBot can expose Alpaca/Benzinga news
tools to agents. In backtests, news tools should use the strategy datetime as
the cutoff so the agent cannot read future headlines.

DuckDB And Documentation Search
-------------------------------

Agents can use DuckDB for structured analysis instead of asking the model to
reason over raw tables inside the prompt. Documentation search tools let the
agent inspect LumiBot usage patterns when it needs framework guidance.

Memory
------

Memory tools write local JSONL files so agent decisions remain inspectable:

- ``remember``
- ``search_memory``
- ``remember_decision``
- ``remember_lesson``
- ``open_thesis``
- ``update_thesis``
- ``close_thesis``

Memory works in both backtests and live runs. In a backtest, memory is part of
the run artifact trail. In live trading, it can preserve context across
iterations and restarts when the same memory directory is reused.

Notifications
-------------

``notify_user`` sends through configured notification providers. Telegram is
the first built-in provider. Backtests keep notifications disabled by default,
but you can explicitly opt in when testing notification behavior.

Point-In-Time Safety
--------------------

The built-in research tools are designed around backtest/live parity:

- indicators return current-bar values only
- SEC filings are gated by filed or accepted datetime
- FRED backtests use vintage observations when ``FRED_API_KEY`` is available
- news tools use the strategy datetime as the cutoff

.. image:: ../docs/assets/readme/lumibot_point_in_time_tools.png
   :alt: Lumibot point-in-time research tools

This lets agents research during a backtest without accidentally looking into
the future.
