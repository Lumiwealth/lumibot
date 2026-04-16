# AI TRADING AGENT OBSERVABILITY

This page records the observability model for LumiBot's AI runtime.

## Required surfaces

Every meaningful agent run should leave behind:

- a compact summary line
- visible tool call/result logs when enabled
- a structured JSON trace
- machine-readable run summaries
- replay-cache state
- warnings when a run looks suspicious

## Why this matters

AI strategy behavior is hard to trust without evidence. Observability is the mechanism that makes:

- point-in-time audits possible
- cache behavior inspectable
- tool misuse obvious
- surprising trades debuggable

## Main artifacts

- `agent_run_summaries.jsonl`
- per-run trace JSON
- `agent_traces.zip` for packaged backtests

## Warnings

Observability warnings are diagnostics, not automatic bans. They exist to surface suspicious behavior such as:

- tool errors
- no-tool runs
- future-looking timestamps
- orders without visible supporting data

## Debug workflow

When a run looks wrong:

1. inspect the summary line
2. inspect tool calls/results
3. open the trace
4. check cache hit/miss
5. review warnings
6. compare the final summary to the actual trade outcome
