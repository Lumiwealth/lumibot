AI Agent Observability
======================

.. meta::
   :description: LumiBot's AI agent runtime is only useful if every run is fully inspectable. The observability system records everything the agent did so you can audit reasoning.

LumiBot's AI agent runtime is only useful if every run is fully inspectable. The observability system records everything the agent did so you can audit reasoning, validate data integrity, and debug surprising behavior.

The file to open
----------------

Raising ``LUMIBOT_LOG_LEVEL`` does not record an agent's decisions. That setting only changes how much the process prints. A custom LiteLLM logger is the same kind of workaround. The prompt, the tool calls, and the model's answer are already saved.

Every agent call is written to ``*_agent_detail.parquet``.

- **Backtest.** The file sits next to the tear sheet and the log, using the same run name. Example: ``logs/my_run_agent_detail.parquet``.
- **Live and paper.** The file is in the LumiBot cache. On macOS that folder is ``~/Library/Caches/lumibot/1.0/agent_runtime/``. Set ``LUMIBOT_CACHE_FOLDER`` before you import lumibot if you want it somewhere else. Per-call JSON traces live under ``agent_runtime/traces/<agent_name>/``. The result also carries ``(result.payload or {}).get("trace_path")``.

Query it with DuckDB:

.. code-block:: sql

    SELECT agent_name, tool_call_count, summary
    FROM 'logs/my_run_agent_detail.parquet'
    WHERE is_call_summary;

    SELECT agent_name, tool_name, event_detail
    FROM 'logs/my_run_agent_detail.parquet'
    WHERE event_kind = 'tool_call';

The full prompt is on the ``call_summary`` row, in ``effective_system_prompt``, ``task_prompt``, and ``user_system_prompt``. Tool arguments and results are in ``event_payload_json`` on the ``tool_call`` and ``tool_result`` rows.

A live run can make hundreds of tool calls while a short backtest makes a dozen. Compare the ``tool_name`` rows in the two parquet files. That count is the diagnosis.

Default Logs
------------

Every agent run emits a compact summary log line that includes:

- Agent name
- Mode (backtest or live)
- Model used
- Cache hit or miss
- Tool call count
- Warning count
- The agent's summary conclusion
- Path to the trace file

The runtime also emits detailed lines for:

- Individual tool calls and their arguments
- Tool results
- Model text output
- Observability warnings

Trace Files
-----------

Each run also writes a structured JSON trace for that one call. The parquet file above is the file to query after the run. The JSON trace records:

- The full composed prompt surface (base prompt + system prompt + context)
- Every tool call with arguments
- Every tool result
- The agent's summary and reasoning
- Observability warnings
- Cache hit/miss metadata
- DuckDB query metrics
- Current datetime and timezone at the time of the run

From strategy code, the trace path is available on the result object:

.. code-block:: python

    trace_path = (result.payload or {}).get("trace_path")

Replay Cache
------------

In backtests, LumiBot caches every agent run. When a subsequent run hits the same combination of prompt, context, model, tool surface, and simulated timestamp, the cached result is returned without making any LLM or MCP calls.

Warm reruns show:

- ``cache_hit=True`` in the result
- Zero model API calls
- Zero external MCP calls
- Identical outputs to the original run

Replay caching makes agentic backtests:

- **Deterministic** -- same inputs always produce same outputs
- **Fast** -- warm reruns complete in seconds instead of minutes
- **Cost-effective** -- no duplicate LLM or MCP API charges

Per-Run Summary Artifacts
-------------------------

LumiBot writes machine-readable artifacts for agent runs:

- ``agent_run_summaries.jsonl`` -- one JSON line per agent run with summary, warnings, and metadata
- ``agent_traces.zip`` -- packaged trace files for the full backtest

These artifacts support downstream tooling, dashboards, and run history display without reparsing raw logs.

Observability Warnings
----------------------

Warnings are diagnostics, not hard enforcement rules. They flag suspicious conditions so you can investigate:

- **No tools called** -- the agent made a decision without consulting any tools
- **Tool error** -- a tool returned an error
- **Future-dated data** -- a tool result references data published after the simulated backtest time
- **Unsupported order** -- an order was submitted without visible supporting evidence in the trace

Warnings appear in:

- The summary log line
- The structured JSON trace
- ``result.warning_messages`` on the result object

Warnings do not automatically invalidate a run, but they are a strong signal that the run should be reviewed.

Recommended Debugging Workflow
------------------------------

When a run looks wrong:

1. **Read the summary line** in the logs. Check the agent name, cache status, tool count, and warning count.
2. **Inspect tool calls and results** in the logs. Did the agent call the right tools? Did the tools return useful data?
3. **Open** ``*_agent_detail.parquet``. The ``call_summary`` row has ``effective_system_prompt``. The event rows are everything the agent saw and did. The JSON file at ``trace_path`` is the same call, one file at a time.
4. **Check cache status.** Was this a fresh run or a replay? If it was a replay, the issue is in the original run, not this one.
5. **Review warnings.** Are there future-dated data warnings? Missing tool usage? Unsupported orders?
6. **Compare the summary to the outcome.** Does the agent's stated reasoning match the actual trade or no-trade decision?

Frequently Asked Questions
--------------------------

**How do I see what the agent is doing?**

Start with the summary log line: agent name, model, cache hit or miss, tool call count, warning count, and the summary. Then open ``*_agent_detail.parquet``. Do not raise ``LUMIBOT_LOG_LEVEL`` to get this record. The ``call_summary`` row has ``effective_system_prompt``. The ``tool_call`` rows have the arguments.

**What are agent traces?**

Two records exist. ``*_agent_detail.parquet`` is the table you query for the whole run: one ``call_summary`` row per AI call, plus rows for thinking, text, tool calls, tool results, and usage. A JSON trace is also written for each call. Both include the full prompt, every tool call and result, the summary, warnings, cache status, and the simulated datetime.

**Where are trace files stored?**

On macOS, live and paper files are under ``~/Library/Caches/lumibot/1.0/agent_runtime/``. Set ``LUMIBOT_CACHE_FOLDER`` before importing lumibot to move that folder. A backtest writes ``*_agent_detail.parquet`` next to the tear sheet instead. The per-call JSON path is ``(result.payload or {}).get("trace_path")``. Summaries are also appended to ``agent_run_summaries.jsonl``.

**How do I debug a bad trade?**

Open ``*_agent_detail.parquet`` for that run. Check: (1) what tools the agent called and what they returned, (2) the summary and ``effective_system_prompt``, (3) warnings such as future-dated data, no tools called, or an unsupported order, and (4) whether the summary matches the trade. Follow the six-step workflow above.

**What are observability warnings?**

Warnings are diagnostics that flag suspicious conditions. They include: no tools called (the agent decided without consulting any tools), tool error (a tool returned an error), future-dated data (a tool result references data after the simulated backtest time), and unsupported order (an order was submitted without visible supporting evidence). Warnings do not automatically invalidate a run but are a strong signal to investigate.

**Why is my agent not trading?**

Check the agent's summary in the logs -- it may have decided to hold because conviction was low. The default base prompt includes an investor policy that encourages conviction over activity and discourages overtrading. Inspect the trace to see what data the agent received and what reasoning it applied. If you want more frequent trading, adjust your system prompt.

**Why is my agent only buying SHV?**

SHV is the defensive parking asset in many demo strategies. If the agent always buys SHV, it means it cannot find enough evidence to take risk. Verify that your custom tools are returning meaningful data (check the trace for tool results). Confirm that the system prompt clearly explains when to be risk-on. Also check that your API keys are valid -- empty or errored tool responses often cause the agent to default to the safe asset.

**What does cache_hit=True mean?**

It means the agent run result was replayed from the replay cache instead of making fresh LLM and tool calls. The inputs (prompt, context, model, tools, simulated timestamp) matched a previously cached run. The output is identical to the original run. If you suspect the cached result is wrong, clear the cache directory and rerun to get a fresh result.

**How do I clear the cache for a fresh run?**

Delete the replay cache at ``~/Library/Caches/lumibot/1.0/agent_runtime/replay/`` on macOS, or the ``agent_runtime/replay/`` folder under ``LUMIBOT_CACHE_FOLDER`` if you set one. After clearing, the next backtest makes fresh model calls.

**What is agent_run_summaries.jsonl?**

It is a machine-readable artifact that records one JSON line per agent run. Each line contains the agent name, model, cache status, tool count, warning count, summary, trace path, and metadata. It supports downstream tooling, dashboards, and run history display without needing to reparse raw logs or individual trace files.

**How do I compare two backtest runs?**

Run the first backtest, note the trace directory. Clear the cache (or change a parameter to get a different cache key), run the second backtest, and compare the trace files and tearsheets. The ``agent_run_summaries.jsonl`` file also lets you compare summaries, tool counts, and warnings across runs programmatically.

**Can I disable the replay cache?**

The replay cache is only active during backtests. In live trading mode, every run makes fresh LLM and tool calls. There is no explicit flag to disable the cache during backtests, but you can clear the cache directory before each run to force fresh calls.

**What DuckDB query metrics are recorded in traces?**

Traces record which DuckDB queries the agent executed, their SQL statements, and timing information. This helps you understand how the agent analyzed historical price data and whether the SQL queries were efficient and correct.

**How do I know if a warning is serious?**

Future-dated data warnings are the most serious in backtesting because they indicate potential look-ahead bias. No-tools-called warnings suggest the agent may be making decisions without evidence. Tool-error warnings mean the agent had incomplete information. Review each warning in context -- a single no-tools-called warning on a quiet day may be fine, but a pattern of future-dated data warnings requires immediate investigation.

Related Pages
-------------

- :doc:`agents` -- main guide and architecture
- :doc:`agents_quickstart` -- code patterns and API reference
- :doc:`agents_canonical_demos` -- the four reference demo strategies

Order evidence after a decision
-------------------------------

Inspect the exact order identifier returned by the submission tool.
``orders_get_status`` reports observed state; ``orders_wait_for_terminal``
performs bounded observation and can advance simulated time in backtests.
A timeout is not a rejection. Reconcile pending orders before a retry, inspect
filled and remaining quantities after partial fills, and refresh positions.
Never call a submitted order a fill based only on an agent's summary.

For examples, retain source/model/date/data metadata beside the result. A
scripted-model integration test proves engine wiring; a real-model eval tests
reasoning and tool use. Neither alone proves real broker behavior.
