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

`orders_submit_orders(orders_json=..., reason=...)` wraps the existing
`Strategy.submit_orders` method for an explicit independent basket. Each JSON
object has the same fields as `orders_submit_order`; a row's optional `reason`
overrides the common batch explanation. All rows are validated before the first
submission, and the private journal retains per-order explanations. The tool
does not compute target weights, quantities or funding automatically.

The native batch is not atomic and acknowledgement is not a fill. Submit funding
sells separately, verify their proceeds, then submit dependent buys. Related
option legs still require `orders_submit_multileg`. Inspect every requested
identifier after an exception or short return; never blindly retry a partially
executed basket. Completion requires reconciling the full target list against
positions, pending orders and cash, retaining already-correct holdings without
duplicate buys and explaining specific unavailable constituents.

Alpaca news uses an active Alpaca broker when available. Outside Alpaca broker runs, it checks `ALPACA_NEWS_API_KEY` / `ALPACA_NEWS_API_SECRET`. If neither path is available, `alpaca_news` is not exposed to agents.

Historical news validates each returned publication/update timestamp against
the effective simulated UTC clock. Future, undated, ambiguous and subsequently
revised rows are excluded in full. The `historical_integrity` response records
exclusions, pagination limits and the lack of original-version reconstruction;
it never presents filtered current articles as a complete historical archive.
Live results and eligible full article bodies retain their existing behavior.

`read_document(url, available_at=..., expected_sha256=...)` accepts version proof
from a publisher or discovery index. During a backtest, a timezone-aware
`available_at` after the simulated UTC clock produces a visible tool error before
HTTP. A malformed instant or SHA-256 also fails before HTTP. When fetched bytes
do not match `expected_sha256`, the tool returns an integrity error without
exposing document text or registering tables. These optional arguments preserve
existing callers; they validate supplied proof and cannot establish an unknown
publication time. Agents must compute eligibility for every index table before
following linked documents and must not fetch excluded versions.

Order tools require a private decision reason. Their optional `public_reason`
contains a short qualitative thesis and risk, without private identity, account
balances, account-relative sizing percentages, sizing formulas or credentials.
The private audit retains the decision context. An unsafe public summary is
omitted without changing the order or its private reason.

Trading permission:

```python
self.agents.create(name="researcher", allow_trading=False)
```

`allow_trading=False` removes mutating order tools and actual-decision writes:

- `orders_submit_order`
- `orders_submit_orders`
- `orders_submit_multileg`
- `orders_cancel_order`
- `orders_modify_order`
- `remember_decision`

It keeps read-only tools, including `orders_open_orders`, `orders_get_status`, `orders_wait_for_terminal`, positions, portfolio, market data, indicators, SEC filings, FRED macro data, memory, and notifications.

Network permission (default-deny, since 4.5.92):

```python
self.agents.create(name="page_researcher", allow_trading=False, allow_network=True)
```

`http_request`, `rss_fetch`, and every `browser_*` tool are outbound network tools (`NETWORK_TOOL_NAMES` in `lumibot/components/agents/manager.py`). They are not in the default toolset. `allow_network=True` adds all of them; listing one in `tools=[...]` adds only that tool; `allow_network=False` removes them even when listed. Why: a fetched page is untrusted input and a network tool is the channel it could use to send agent context out, so only the agent that must fetch pages should hold one, never the trader. It also matters for model quality: when the 13 tools joined every default agent, the `options_iron_condor_atomic_open` release eval fell from 3/3 to 1/3 (declined a supported trade, or ordered before the account checks). `tests/test_agent_tool_permissions.py` guards the default, the opt-ins, and the shipped examples.

Unknown tool names: when the model calls a tool that does not exist, the runtime returns `{tool_error: true, unknown_tool: true}` with the real tool names instead of letting ADK end the run. The terminal status ignores these calls, since nothing ran.

Order readiness:

Before an agent can submit an order with `orders_submit_order`, `orders_submit_orders` or `orders_submit_multileg`, it must inspect account and price context in the same agent run:

- `account_portfolio` for cash and portfolio value
- `account_positions` for current holdings
- `market_quote` for the exact asset, or a usable `market_quotes` row for it; current `market_last_price` / `market_last_prices` remain supported fallbacks
- for an order that opens an option position: `options_get_chain` for the underlying (closing a held contract is exempt). Release eval `options_single_leg_chain_and_quote` caught an agent opening a call found only through `options_find_expiration` and `options_find_strike_for_delta`.

If any of those checks are missing, the order tool returns a structured `ORDER_READINESS_REQUIRED` error instead of submitting the order. Lumibot does not silently resize orders and does not apply universal margin rules across asset classes; the agent must use the checked cash, portfolio value, positions, and price to size the explicit order it submits.

For options, `account_positions` includes exact contract fields, signed quantity, average fill price, current price, market value, and P&L fields when available. This gives the agent the generic information needed to reconstruct and manage open multi-leg positions.

Market-price tools:

- `market_quote` wraps `Strategy.get_quote`; `market_quotes` wraps `Strategy.get_quotes`, preserving native provider batching. Results include exact asset identity, last, bid, ask, midpoint, sizes and available timestamps. Missing fields remain null. The strategy datetime and quote-object construction timestamp do not establish provider freshness.
- For a portfolio, pass `symbols` / `symbols_json` to `market_quotes`. For exact options, use `assets_json` entries with `symbol`, `asset_type`, `expiration`, `strike`, `right`, and optional `quote_symbol` / `exchange`. Rows remain separate when contracts share an underlying. Provider group failures stay visible while independent groups remain available.
- Size immediate buys from the current ask, sells from the bid, and limit orders from their intended price. If quote sides are unavailable, use the quote's usable current price or the current last-price tool. Options require the exact contract and multiplier. Historical bars support research and dated valuation, not current execution sizing.
- `market_last_price` accepts one tradable symbol per call.
- `market_last_prices` accepts a JSON-friendly list (`symbols` or `symbols_json`, cap 150) and returns last prices available at the current runtime datetime, plus `symbols_available` / `symbols_missing`. Prefer this when scanning a provided universe (for example multi-ticker ORB). Do not invent prices for missing symbols.
- `market_load_history_table` still loads one symbol per call; load finalists into DuckDB after the batch scan.
- `market_historical_prices(..., table_name=...)` stores bars in the bars' own market wall-clock time (the clock the raw bars show; `datetime_timezone` names it), never the strategy clock. The eval fixture's strategy clock is UTC while its bars are New York time; converting to the strategy clock moved the 09:30 ET open to 13:30 and made an ORB agent find no breakout.
- `market_load_history_table` serves stored source rows only when they are the requested bar size (a `5minute` request against 1-minute data goes through `get_historical_prices`, which aggregates), and for minute and hour bars it excludes the bar that starts at the current time, which has not finished.

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

## Recoverable research and historical news

Agent history, indicator and quote reads report incomplete candidate data as a
visible tool limitation. Batch quote reads retain available symbols and report
unavailable symbols individually. This does not clear an earlier required-data
failure or relax execution and portfolio-valuation requirements.

When a news feed or article fails, agents can continue through another available
feed, publisher archive or web source and read the relevant article body. A
headline or link is not proof of an article read. Historical `read_document`
results distinguish a publication date from verification of the retrieved version;
without retained-original evidence, agents disclose that text may contain later
edits while continuing with supported approximate research. Known future versions
remain excluded. Release-gated real-model cases cover fallback research and
conservative macro vintages, with semantic judges and labeled calibration controls.
