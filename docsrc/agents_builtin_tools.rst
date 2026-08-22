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

With ``allow_trading=False``, LumiBot removes tools that mutate orders and the
actual-decision memory write:

- submit order
- submit multi-leg order
- modify order
- cancel order
- remember decision

Read-only tools remain available. A research agent can still inspect cash,
positions, open orders, historical prices, indicators, news, SEC filings, FRED
macro data, memory, and notifications.

.. image:: ../docs/assets/ai_committee/docs_tool_permissions.png
   :alt: Lumibot agent tool permissions

Use ``allow_trading=True`` only for the final agent that is allowed to place or
change orders. In an AI trading team workflow, that is usually the portfolio
manager or trader agent.

Order Readiness
---------------

Agent order tools are intentionally broker-like: LumiBot either submits the
exact order requested or rejects it. It does not silently resize, clip, or
normalize a requested order into a different order.

Before ``orders_submit_order`` or ``orders_submit_multileg`` can submit an
order, the agent must inspect the required account and price context in the
same agent run:

- ``account_portfolio`` for cash and portfolio value
- ``account_positions`` for current holdings
- ``market_last_price`` for the ordered symbol, or ``market_last_prices``
  including that symbol

If those checks are missing, the order tool returns an
``ORDER_READINESS_REQUIRED`` error to the agent instead of submitting the order.
This is not a universal margin model. LumiBot does not try to enforce one
broker/country/asset-class leverage rule across stocks, ETFs, options, futures,
forex, and crypto. The readiness gate only prevents blind trading; sizing
judgment remains with the strategy and agent.

Market-price tools:

- ``market_last_price`` accepts one tradable symbol per call.
- ``market_last_prices`` accepts a JSON-friendly symbol list (``symbols`` or
  ``symbols_json``, cap 150) and returns last prices at the current runtime
  datetime plus available/missing symbol lists. Prefer this when scanning a
  provided universe.
- ``market_load_history_table`` still loads one symbol per call; load finalists
  after the batch scan.

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

Options And Multi-Leg Orders
----------------------------

LumiBot exposes generic options capabilities to every agent:

- ``options_get_chain``
- ``options_get_strikes``
- ``options_get_greeks``
- ``options_find_strike_for_delta``
- ``options_find_expiration``
- ``options_evaluate_market``
- ``options_calculate_multileg_price``
- ``options_check_spread_profit``

The tools retrieve data through the configured LumiBot broker or backtest data
source. They do not select a named options strategy or choose its legs. The
agent must select an available expiration, exact listed strikes, quantities,
and actions from the returned evidence.

``options_find_expiration`` finds a listed expiration on or after a target date
using ``min_days`` and/or ``target_date``. ``options_check_spread_profit``
estimates multi-leg P&L percentage from exact legs and the opening cash cost.

``orders_submit_multileg`` submits two or more exact option legs as one atomic
multi-leg order. Opening actions are ``buy_to_open`` and ``sell_to_open``.
Closing actions are ``buy_to_close`` and ``sell_to_close``. Signed net prices
are positive for debits and negative for credits.

If the active broker does not support atomic package submission, LumiBot
rejects the request before submitting any child leg. A multi-leg request never
falls back to independent orders.

After submission, agents can call ``orders_get_status`` or
``orders_wait_for_terminal`` to verify identifiers. Never treat a submitted
status as a fill unless ``is_filled`` is true.

``account_positions`` includes the exact contract, signed quantity, average
fill, current value, and P&L fields when the active broker or backtest provides
them. Agents can use those generic fields to reconstruct and manage existing
option positions.

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

Set ``FRED_API_KEY`` so LumiBot can request FRED/ALFRED vintage observations
using realtime parameters. LumiBot's built-in FRED tools do not use public CSV
fallbacks; macro tool output should either come from the official API or fail
clearly.

News
----

If the active broker is Alpaca, LumiBot can use that broker connection for
Alpaca/Benzinga news. If the active broker is not Alpaca, configure the
news-only ``ALPACA_NEWS_API_KEY`` / ``ALPACA_NEWS_API_SECRET`` env vars instead.
Generic Alpaca broker env vars are intentionally not used for news-only access,
so news credentials do not confuse broker selection for Tradier, IBKR, or other
brokers. In backtests, news tools should use the strategy datetime as the cutoff
so the agent cannot read future headlines.

DuckDB And Documentation Search
-------------------------------

Agents can use DuckDB for structured analysis instead of asking the model to
reason over raw tables inside the prompt. Documentation search tools let the
agent inspect LumiBot usage patterns when it needs framework guidance.

Memory
------

Memory tools write local SQLite and Parquet artifacts so agent decisions remain
inspectable:

- ``remember``
- ``search_memory``
- ``remember_proposal``
- ``remember_risk_note``
- ``remember_decision`` (trading-capable agents only)
- ``remember_lesson``
- ``open_thesis``
- ``update_thesis``
- ``close_thesis``

Memory works in both backtests and live runs. In a backtest, memory is part of
the run artifact trail. In live trading, it can preserve context across
iterations and restarts when the same memory directory is reused.

Use ``remember_proposal`` for research ideas and ``remember_risk_note`` for
bear-case notes. Use ``remember_decision`` only for the final trading decision.
When ``orders_submit_order`` submits an order, Lumibot automatically records an
``order.submitted`` memory event. Memory events and retrievals include
``agent_name`` and ``model_call_id`` when they came from an agent tool call.

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
