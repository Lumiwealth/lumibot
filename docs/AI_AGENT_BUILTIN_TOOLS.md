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

Order readiness:

Before an agent can submit an order with `orders_submit_order`, it must inspect account and price context in the same agent run:

- `account_portfolio` for cash and portfolio value
- `account_positions` for current holdings
- `market_last_price` for the ordered symbol

If any of those checks are missing, the order tool returns a structured `ORDER_READINESS_REQUIRED` error instead of submitting the order. Lumibot does not silently resize orders and does not apply universal margin rules across asset classes; the agent must use the checked cash, portfolio value, positions, and price to size the explicit order it submits.

Market-price tools accept one tradable symbol per call. Do not pass a comma-separated universe to `market_last_price` or `market_load_history_table`; call the tool once per symbol or load each symbol into DuckDB separately before querying.

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

FXMacroData tools:

- `list_fxmacrodata_indicators`
- `get_fxmacrodata_series`
- `get_fxmacrodata_latest`
- `get_fxmacrodata_snapshot`

FXMacroData tools fetch FX-focused macro announcement rows from FXMacroData. USD announcement data is public. Set `FXMD_API_KEY` or `FXMACRODATA_API_KEY` for non-USD and paid endpoint access. Lumibot sends the key as an `X-API-Key` header rather than adding it to request URLs.

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
