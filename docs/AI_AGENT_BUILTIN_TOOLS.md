# AI Agent Built-In Tools

Lumibot agents include built-ins for:

- account and portfolio state
- market prices and history tables
- DuckDB queries
- Lumibot docs search
- Alpaca news
- indicators
- SEC fundamentals and filings
- FRED macro data
- memory
- notifications
- orders

Alpaca news uses an active Alpaca broker when available. Outside Alpaca broker runs, it checks `ALPACA_NEWS_API_KEY` / `ALPACA_NEWS_API_SECRET`. If neither path is available, `alpaca_news` is not exposed to agents.

Trading permission:

```python
self.agents.create(name="researcher", allow_trading=False)
```

`allow_trading=False` removes mutating order tools and actual-decision writes:

- `orders_submit_order`
- `orders_cancel_order`
- `orders_modify_order`
- `remember_decision`

It keeps read-only tools, including `orders_open_orders`, positions, portfolio, market data, indicators, SEC filings, FRED macro data, memory, and notifications.

Indicator tools:

- `list_indicators`
- `get_indicator`
- `get_indicators`

FRED macro tools:

- `list_fred_series`
- `get_fred_series`
- `get_fred_latest`
- `get_fred_snapshot`

Built-in FRED tools require `FRED_API_KEY` so Lumibot can request official FRED/ALFRED observations with `realtime_start` and `realtime_end`. Lumibot does not use public CSV fallbacks for macro data, because those endpoints can contain revised values and are not a safe default for historical simulations.

Memory tools:

- `remember`
- `search_memory` (supports `kind`, `symbol`, and `status` filters)
- `remember_proposal`
- `remember_risk_note`
- `remember_decision` (trading-capable agents only)
- `remember_lesson`
- `open_thesis`
- `update_thesis`
- `close_thesis`

Agent memory is SQLite-backed while the strategy runs. Lumibot also exports memory Parquet artifacts for review:

- `*_memory_events.parquet`
- `*_memory_retrievals.parquet`
- `*_memory_state.parquet`

Use `remember_proposal` for research ideas and `remember_risk_note` for bear-case notes. Use `remember_decision` only for the final trading decision. `orders_submit_order` automatically records an `order.submitted` memory event after Lumibot submits the order, and memory events/retrievals carry `agent_name` and `model_call_id` when they came from an agent tool call.

When an agent is holding a position and plans to add, reduce, or sell that symbol, it should call `search_memory` for the open thesis first. Lumibot records a non-blocking warning when an order tool touches a held symbol without that retrieval.

Notification tool:

- `notify_user`
