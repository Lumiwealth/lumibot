# Agent builtin vs Strategy API coverage (2026-08-06)

## Context

Rob asked whether Lumibot's trading AI agent could fetch historical prices for
many symbols in one call (not a per-symbol loop), the same way
`market_last_prices` already batches last prices.

## Finding

- Batch last prices already existed: `market_last_prices` -> `get_last_prices`.
- Batch historical bars were missing from agent builtins.
- Strategy API already had `get_historical_prices_for_assets`.

## Change shipped on `version/4.5.84`

- Added agent builtin `market_historical_prices`.
- Wraps `Strategy.get_historical_prices_for_assets` for stock/index universes.
- Returns JSON `bars_by_symbol` with up to 150 symbols per call.
- Falls back to per-symbol `get_historical_prices` when the batch API is absent.
- ORB prompt now prefers `market_historical_prices` for minute opening ranges.

## Remaining high-value Strategy APIs not yet represented as agent tools

These are public Strategy capabilities agents often need, but they are not
exposed as first-class builtins today:

| Strategy API | Agent gap |
|---|---|
| `get_quote` / `get_tick` | No generic quote tool for stocks/ETFs/futures (options use `options_evaluate_market`) |
| `get_historical_prices` (single) as JSON | Only DuckDB load via `market_load_history_table`; single-symbol JSON history is still indirect |
| `get_orders` with status filters / broker refresh | Partial via `orders_open_orders` and `orders_get_status` |
| `sell_all` / `close_position` / `close_positions` | No dedicated close helpers; agent must build sells manually |
| `cancel_open_orders` / `cancel_orders` (bulk) | Only single-id `orders_cancel_order` |
| `await_market_to_open` / `await_market_to_close` / `set_market` | No market-clock wait tools |
| `get_datetime` / `get_timestamp` / timezone helpers | Available only as side fields on other tool results |
| `add_marker` / `add_line` / `add_ohlc` chart helpers | No chart annotation tools |
| `get_chain_full_info` | Options chain tools are thinner than full Strategy helper surface |
| `adjust_cash` / deposit / withdraw | Intentionally absent for agent safety |
| `sleep` / cron callbacks | Runtime-owned; not agent tools |

## Recommended next agent coverage

1. `market_quotes` / `market_quote` for bid/ask/mid on equities and futures.
2. `orders_close_position` and `orders_cancel_open_orders`.
3. Optional `market_load_history_tables` that batch-loads many symbols into DuckDB
   using `get_historical_prices_for_assets` for SQL-heavy multi-ticker analysis.
4. Market-clock helpers if agents need explicit open/close waits in live mode.
