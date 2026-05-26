# AI Agent Memory

Lumibot native memory is local, SQLite-backed, and available in both backtests and live trading.

Default storage:

```text
.lumibot/memory/<strategy_name>/
  memory.sqlite
  agent_memory_memory_events.parquet
  agent_memory_memory_retrievals.parquet
  agent_memory_memory_state.parquet
```

Override the root with:

```bash
export LUMIBOT_MEMORY_DIR=/path/to/memory
```

Set `LUMIBOT_MEMORY_EXPORT_PARQUET=0` to disable best-effort Parquet exports.

## Data Model

The SQLite database has three core tables:

- `memory_events`: append-only ledger of every memory write, proposal, risk note, decision, thesis update, submitted order, warning, outcome observation, and other memory event.
- `memory_index`: current searchable projection of memories, decisions, lessons, and theses.
- `memory_retrievals`: every `search_memory` call, including query, filters, candidate IDs, selected IDs, and rendered text.

Lumibot writes to SQLite while the strategy runs. Parquet files are derived artifacts for inspection, DuckDB queries, BotSpot uploads, and post-run analysis.

Memory events and retrievals include `agent_name` and `model_call_id` when they happen inside an agent tool call. `model_call_id` is the stable replay-cache key for that agent request, so memory artifacts can be joined back to `agent_detail.parquet`.

## Strategy API

```python
self.memory.remember("...")
self.memory.remember_decision("...", symbol="AAPL", action="buy")
self.memory.remember_proposal("...", symbol="AAPL", action="buy")
self.memory.remember_risk_note("...", symbol="AAPL")
self.memory.remember_lesson("...", symbol="AAPL")
self.memory.open_thesis("...", symbol="AAPL")
self.memory.search("AAPL margin", symbol="AAPL", status="open")
self.memory.export_artifacts("/path/to/logs", prefix="my_strategy_agent_memory")
```

`remember_lesson` stores proposed lessons by default. Pass outcome metadata with `validated=True` when a lesson has been validated by later outcome data.

## Agent Behavior

Agents expose the same memory behavior through built-in tools:

- `remember`
- `search_memory`
- `remember_proposal`
- `remember_risk_note`
- `remember_decision`
- `remember_lesson`
- `open_thesis`
- `update_thesis`
- `close_thesis`

Every agent call receives a compact `Lumibot Memory State JSON` section with open theses, current position rationales, and validated lessons. That injected state is intentionally small; deeper history should be retrieved with `search_memory`.

Research agents with `allow_trading=False` can write proposals and risk notes, but they cannot call `remember_decision`. In Lumibot, `remember_decision` means an actual trading decision and is reserved for trading-capable agents. When an agent submits an order through `orders_submit_order`, Lumibot also records an `order.submitted` memory event automatically so the memory ledger contains the executed action, not only the model's prose.

Open theses also receive a best-effort daily `thesis.outcome_observed` event while the symbol is held. This records current quantity, last price when available, and market value when available. The observation is append-only and does not replace the thesis text.

If an agent already holds a position and uses an order tool to add, reduce, or sell that symbol without first calling `search_memory`, Lumibot records a non-blocking observability warning. The order is not blocked. The warning exists so the backtest artifacts show when the agent ignored its own open thesis.

## Artifacts

Agent detail and memory are separate artifacts:

- `agent_detail.parquet`: prompts, tool calls, tool results, usage, warnings, and run-level observability.
- `*_memory_events.parquet`: append-only memory ledger.
- `*_memory_retrievals.parquet`: retrieval provenance for `search_memory`.
- `*_memory_state.parquet`: current memory projection.

Use DuckDB, pandas, or BotSpot artifact search to inspect these files after a run. During live deployment, the same files can be uploaded as live artifacts by the deployment layer.
