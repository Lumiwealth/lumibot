Agent Memory
============

LumiBot includes a native local memory store for agentic strategies. Memory is
available in backtesting and live trading so behavior stays as similar as
possible across modes.

.. image:: ../docs/assets/ai_committee/docs_memory_lifecycle.png
   :alt: Lumibot memory and notifications flow

Storage
-------

By default, memory is stored under ``.lumibot/memory/<strategy_name>/`` in JSONL
files:

- ``memories.jsonl``
- ``decisions.jsonl``
- ``lessons.jsonl``
- ``theses.jsonl``

Override the root with ``LUMIBOT_MEMORY_DIR``.

Agent Tools
-----------

Agents can call:

- ``remember``
- ``search_memory``
- ``remember_decision``
- ``remember_lesson``
- ``open_thesis``
- ``update_thesis``
- ``close_thesis``

The investment committee example uses these tools to record decisions, thesis
updates, and compact lessons.

Traceability
------------

Memory complements LumiBot's normal backtest artifacts. The agent can search
past decisions and lessons during future iterations, while the developer can
inspect the JSONL files after the run.

.. image:: ../docs/assets/ai_committee/docs_backtest_artifacts.png
   :alt: Lumibot backtest artifacts and agent memory
