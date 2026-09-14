# AI Agent Built-In Tools

Lumibot agents include built-ins for:

- account and portfolio state
- market prices and history tables
- option chains, strikes, Greeks, and executable quote evaluation
- DuckDB queries
- Lumibot docs search
- Alpaca news
- indicators
- SEC fundamentals and filings
- FRED macro data
- memory
- notifications
- orders

Generic option tools:

- `options_get_chain`
- `options_get_strikes`
- `options_get_greeks`
- `options_find_strike_for_delta`
- `options_find_expiration`
- `options_evaluate_market`
- `options_calculate_multileg_price`
- `options_check_spread_profit`

These tools expose LumiBot's configured broker or backtest data source. They do not choose an options strategy or its legs. The agent must retrieve a listed expiration and strikes, inspect exact contract data, and decide what to trade.

`options_find_expiration` wraps `OptionsHelper.get_expiration_on_or_after_date` with JSON-friendly `min_days` and/or `target_date` arguments. `options_check_spread_profit` estimates multi-leg P&L percentage from exact legs plus the opening cash cost.

`orders_submit_multileg` accepts exact option legs selected by the agent and submits them as one atomic multi-leg order. Each leg declares `symbol`, `expiration`, `strike`, `right`, `quantity`, and `side`. Opening actions are `buy_to_open` and `sell_to_open`; closing actions are `buy_to_close` and `sell_to_close`. Signed net prices are positive for debits and negative for credits.

Atomic means fail closed. If the active broker does not implement package
submission, LumiBot rejects the request before submitting any leg. It never
falls back to separate child orders for a multi-leg request.

Order status tools:

- `orders_open_orders`
- `orders_get_status`
- `orders_wait_for_terminal`

Use `orders_get_status` after a submit to verify identifiers. Never claim a fill unless `is_filled` is true. `orders_wait_for_terminal` polls with a bounded timeout (max 120 seconds) and uses `strategy.sleep` so pending fills can process.

Alpaca news uses an active Alpaca broker when available. Outside Alpaca broker runs, it checks `ALPACA_NEWS_API_KEY` / `ALPACA_NEWS_API_SECRET`. If neither path is available, `alpaca_news` is not exposed to agents.

Trading permission:

```python
self.agents.create(name="researcher", allow_trading=False)
```

`allow_trading=False` removes mutating order tools and actual-decision writes:

- `orders_submit_order`
- `orders_submit_multileg`
- `orders_cancel_order`
- `orders_modify_order`
- `remember_decision`

It keeps read-only tools, including `orders_open_orders`, `orders_get_status`, `orders_wait_for_terminal`, positions, portfolio, market data, indicators, SEC filings, FRED macro data, memory, and notifications.

Order readiness:

Before an agent can submit an order with `orders_submit_order` or `orders_submit_multileg`, it must inspect account and price context in the same agent run:

- `account_portfolio` for cash and portfolio value
- `account_positions` for current holdings
- `market_last_price` for the ordered symbol, or `market_last_prices` with that symbol included in the batch

If any of those checks are missing, the order tool returns a structured `ORDER_READINESS_REQUIRED` error instead of submitting the order. Lumibot does not silently resize orders and does not apply universal margin rules across asset classes; the agent must use the checked cash, portfolio value, positions, and price to size the explicit order it submits.

For options, `account_positions` includes exact contract fields, signed quantity, average fill price, current price, market value, and P&L fields when available. This gives the agent the generic information needed to reconstruct and manage open multi-leg positions.

Market-price tools:

- `market_last_price` accepts one tradable symbol per call.
- `market_last_prices` accepts a JSON-friendly list (`symbols` or `symbols_json`, cap 150) and returns last prices available at the current runtime datetime, plus `symbols_available` / `symbols_missing`. Prefer this when scanning a provided universe (for example multi-ticker ORB). Do not invent prices for missing symbols.
- `market_load_history_table` still loads one symbol per call; load finalists into DuckDB after the batch scan.

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
