AI Agent Observability
======================

LumiBot's AI agent runtime is only useful if every run is fully inspectable. The observability system records everything the agent did so you can audit reasoning, validate data integrity, and debug surprising behavior.

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

Each run writes a structured JSON trace that is the source of truth for debugging. The trace records:

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
3. **Open the trace JSON.** This is the full record of everything the agent saw and did.
4. **Check cache status.** Was this a fresh run or a replay? If it was a replay, the issue is in the original run, not this one.
5. **Review warnings.** Are there future-dated data warnings? Missing tool usage? Unsupported orders?
6. **Compare the summary to the outcome.** Does the agent's stated reasoning match the actual trade or no-trade decision?

Frequently Asked Questions
--------------------------

**How do I see what the agent is doing?**

Every agent run emits a compact summary log line with the agent name, model, cache hit/miss status, tool call count, warning count, and the agent's summary conclusion. This log line is the first place to look. For deeper inspection, open the structured JSON trace file referenced in the log output.

**What are agent traces?**

Traces are structured JSON files that record everything the agent did during a single run. They include the full composed prompt surface (base prompt + system prompt + context), every tool call with arguments, every tool result, the agent's reasoning and summary, all observability warnings, cache hit/miss metadata, DuckDB query metrics, and the simulated datetime. They are the source of truth for debugging.

**Where are trace files stored?**

Trace files are stored in the LumiBot cache directory under ``agent_runtime/``. The default cache location on macOS is ``~/Library/Caches/lumibot/``. You can override it with the ``LUMIBOT_CACHE_FOLDER`` environment variable. The trace path for a specific run is available on the result object via ``(result.payload or {}).get("trace_path")``.

**How do I debug a bad trade?**

Open the trace JSON for the run where the bad trade happened. Check: (1) what tools did the agent call and what data did they return, (2) what reasoning did the agent state, (3) are there any observability warnings (future-dated data, no tools called, unsupported orders), and (4) does the agent's summary match the actual trade. Follow the six-step debugging workflow described in the Recommended Debugging Workflow section above.

**What are observability warnings?**

Warnings are diagnostics that flag suspicious conditions. They include: no tools called (the agent decided without consulting any tools), tool error (a tool returned an error), future-dated data (a tool result references data after the simulated backtest time), and unsupported order (an order was submitted without visible supporting evidence). Warnings do not automatically invalidate a run but are a strong signal to investigate.

**Why is my agent not trading?**

Check the agent's summary in the logs -- it may have decided to hold because conviction was low. The default base prompt includes an investor policy that encourages conviction over activity and discourages overtrading. Inspect the trace to see what data the agent received and what reasoning it applied. If you want more frequent trading, adjust your system prompt.

**Why is my agent only buying SHV?**

SHV is the defensive parking asset in many demo strategies. If the agent always buys SHV, it means it cannot find enough evidence to take risk. Verify that your custom tools are returning meaningful data (check the trace for tool results). Confirm that the system prompt clearly explains when to be risk-on. Also check that your API keys are valid -- empty or errored tool responses often cause the agent to default to the safe asset.

**What does cache_hit=True mean?**

It means the agent run result was replayed from the replay cache instead of making fresh LLM and tool calls. The inputs (prompt, context, model, tools, simulated timestamp) matched a previously cached run. The output is identical to the original run. If you suspect the cached result is wrong, clear the cache directory and rerun to get a fresh result.

**How do I clear the cache for a fresh run?**

Delete the replay cache directory at ``~/Library/Caches/lumibot/agent_runtime/replay/`` (or wherever ``LUMIBOT_CACHE_FOLDER`` points). After clearing, the next backtest run will make fresh LLM and external API calls for every bar, producing new cache entries.

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
