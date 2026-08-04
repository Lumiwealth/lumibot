Agent Memory
============

Lumibot includes native local memory for agentic strategies. Memory lets an
agent record why it made a decision, search prior lessons, keep an open thesis,
and leave artifacts that a human can inspect after a backtest or live run.

Memory is SQLite-backed while the strategy runs. SQLite gives Lumibot a real
local database for append-only events, projections, retrieval provenance, and
future backend portability without requiring Postgres or a vector database for
local use.

.. image:: ../docs/assets/ai_committee/docs_memory_lifecycle.png
   :alt: Lumibot agent memory lifecycle

Why Memory Exists
-----------------

Trading agents need more than a one-shot prompt. Useful memory answers
questions such as:

* Why did the agent buy, hold, reduce, or skip?
* What evidence mattered at the time?
* Which thesis is still open?
* What lesson was validated after an outcome?
* Did the same risk appear in a prior iteration?
* Did the agent retrieve the open thesis before changing a held position?

This makes AI strategies easier to audit. It also gives future agent calls a
small, searchable context instead of forcing every run to rediscover the same
facts.

Backtest And Live Parity
------------------------

Memory is available in both backtests and live trading. In a backtest, entries
use the simulated strategy datetime when Lumibot can provide it. In live
trading, entries use real current time.

That matters because Lumibot's core design goal is parity: the same strategy
code should behave the same way whether it is replaying history or connected to
a broker. Memory should be part of that same strategy lifecycle, not a
live-only side channel.

Storage
-------

By default, memory is stored under:

.. code-block:: text

   .lumibot/memory/<strategy_name>/
      memory.sqlite
      agent_memory_memory_events.parquet
      agent_memory_memory_retrievals.parquet
      agent_memory_memory_state.parquet

Override the root directory with:

.. code-block:: bash

   export LUMIBOT_MEMORY_DIR=/path/to/lumibot-memory

Set ``LUMIBOT_MEMORY_EXPORT_PARQUET=0`` to disable best-effort Parquet exports.

Data Model
----------

The SQLite database has three core tables:

``memory_events``
   Append-only ledger of memory writes, proposals, risk notes, decisions,
   lessons, thesis changes, submitted orders, warnings, outcome observations,
   and other memory events.

``memory_index``
   Current searchable projection of memories, decisions, lessons, and theses.
   This is the fast path for ``search_memory`` and compact state injection.

``memory_retrievals``
   Retrieval provenance for every ``search_memory`` call: query, filters,
   candidate IDs, selected IDs, and the rendered text made available to the
   agent.

Lumibot writes to SQLite during execution. Parquet files are derived artifacts
for review, DuckDB queries, BotSpot uploads, and post-run analysis.

When a memory event or retrieval comes from an agent tool call, Lumibot records
``agent_name`` and ``model_call_id``. The ``model_call_id`` is the stable
replay-cache key for that agent request, so memory artifacts can be joined back
to ``agent_detail.parquet``.

Agent Tools
-----------

Agents get these memory tools by default:

``remember``
   Store a general note with optional tags.

``search_memory``
   Search local memories, decisions, lessons, and theses. It supports
   ``kind``, ``symbol``, and ``status`` filters. Every call is recorded in
   ``memory_retrievals``.

``remember_proposal``
   Record a non-final research proposal or trade idea. This is available to
   read-only agents.

``remember_risk_note``
   Record a compact bear-case or risk note. This is available to read-only
   agents.

``remember_decision``
   Record an actual trading decision with optional symbol, action, evidence,
   and tags. This is reserved for trading-capable agents, because a read-only
   researcher should not write memory that looks like an executed decision.

``remember_lesson``
   Record a compact lesson. Lessons are ``proposed`` by default and should only
   become ``validated`` when outcome metadata confirms they held up.

``open_thesis``
   Open an investment thesis, usually before or at trade entry.

``update_thesis``
   Replace the current searchable thesis text while keeping the full event
   history in ``memory_events``.

``close_thesis``
   Close a thesis with optional outcome metadata.

Compact State Injection
-----------------------

Every agent call receives a small ``Lumibot Memory State JSON`` section. It
contains:

* open theses
* current position rationales
* validated lessons
* the retrieval policy

This injected state is intentionally compact. It gives the model the current
state of memory without flooding the prompt. Deeper history should be retrieved
with ``search_memory``.

The separate ``_agent_runtime_state`` entry in ``self.vars`` keeps bounded
prompt notes and run metadata for lifecycle continuity. It does not duplicate
full model summaries. Full history belongs in SQLite memory and agent
observability artifacts.

Lumibot also records execution-side memory. When an agent submits an order with
``orders_submit_order``, Lumibot writes an append-only ``order.submitted``
event after the order is submitted. Open theses receive a best-effort daily
``thesis.outcome_observed`` event while the symbol is held, including current
quantity, last price when available, and market value when available.

Thesis Retrieval Warnings
-------------------------

When an agent already holds a position and uses an order tool to add, reduce,
or sell that symbol, it should first call ``search_memory`` for the open
thesis.

Lumibot does not block the order if the agent skips retrieval. It records a
non-blocking observability warning and a memory event. This keeps the trading
path unblocked while making the behavior visible in ``agent_detail.parquet`` and
``*_memory_events.parquet``.

Common Patterns
---------------

**Decision journal**
   Ask the final decision agent to call ``remember_decision`` whenever it buys,
   sells, reduces, or explicitly skips a high-conviction setup. Keep the entry
   compact: action, symbol, main evidence, risk, and expected invalidation.

**Thesis tracking**
   Use ``open_thesis`` when the agent enters a position, ``update_thesis`` when
   evidence changes, and ``close_thesis`` when the position exits or the thesis
   is invalidated.

**Validated lessons**
   Proposed lessons are cheap to write. Validated lessons should require
   outcome data. This prevents the model from treating every reflection as a
   proven rule.

**Search before acting**
   Before changing a held position, ask the agent to call ``search_memory`` for
   the symbol and open thesis. This helps the agent compare the original reason
   for the trade against current evidence.

Example Prompt
--------------

.. code-block:: python

   self.agents.create(
       name="portfolio_manager",
       model="openai/gpt-5.4-mini",
       allow_trading=True,
       system_prompt=(
           "Review evidence and risk before trading. "
           "Before changing a held position, search memory for the open thesis. "
           "After the decision, record a compact decision memory with the "
           "symbol, action, main evidence, and invalidation condition."
       ),
   )

What Memory Is Not
------------------

Memory is not a risk-management substitute. Hard constraints such as maximum
position size, no-shorting rules, cash reserves, drawdown stops, and symbol
allowlists should still live in Python code or broker/account configuration.

Memory is also not a guarantee that an LLM will improve over time. It is a
structured, inspectable context source. The strategy still controls when memory
is searched, what tools are available, and whether orders can be submitted.

Traceability
------------

Memory complements Lumibot's normal backtest artifacts. Agent traces show the
prompt, tool calls, tool results, and summaries. Memory artifacts show what the
agent preserved, what it retrieved, and what current state was available.

.. image:: ../docs/assets/ai_committee/docs_backtest_artifacts.png
   :alt: Lumibot backtest artifacts and agent memory

Review memory after a run when you want to answer:

* Why did the agent trade?
* Which evidence did it preserve?
* What thesis was open at the time?
* What lesson did it write after the outcome?
* Did the agent search prior memories before acting?
* Did the agent retrieve memory before changing a held position?
