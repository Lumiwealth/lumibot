# BACKTEST_TELEMETRY — The per-run telemetry file and how to find why a backtest is slow

> Every backtest writes `<run>_telemetry.json` beside its other outputs: where the wall time went, how many provider requests and cache reads it made. Read it first; profile only if it does not explain the time.

**Last Updated:** 2026-11-11
**Status:** Active
**Audience:** Developers, AI agents, and tools that read backtest outputs

---

## Overview

`lumibot/tools/backtest_telemetry.py` collects counters during the run. `Strategy.run_backtest` writes them at the end to `logs/<base_filename>_telemetry.json`, in the same folder as `*_settings.json`.

- **Always on.** No setting turns it on or off.
- **Cheap.** Measured on 2026-11-11: 0.4 µs per trading iteration and about 0.5 µs per provider or cache event. A 12-month one-minute run (98,141 iterations, a few thousand cache events) spends about 40 ms on telemetry.
- **Public-safe.** It holds counts, seconds, byte totals, source names (`alpaca`, `ibkr`, `thetadata`, `downloader`...), cache layer names and asset classes. It never holds hostnames, bucket names, keys, credentials, symbols or account data.
- **Stable.** `schema` is `lumibot.backtest_telemetry/1`. Fields are only ever added, never renamed or removed. `tests/test_backtest_telemetry.py` pins the names.

## The file

```json
{
 "schema": "lumibot.backtest_telemetry/1",
 "lumibot_version": "4.6.52",
 "wall_seconds": 38.018,
 "phases": {"data_setup_seconds": 0.616, "simulation_seconds": 33.932, "reports_seconds": 3.468, "uploads_seconds": 0.0},
 "iterations": {"count": 97140, "total_seconds": 22.129, "slowest_seconds": 2.411, "slowest_at": "2025-10-06T10:00:00-04:00"},
 "providers": [
  {"source": "alpaca", "asset_class": "option", "requests": 48, "provider_seconds": 5.605, "rows": 0, "retries": 0,
   "rate_limit_waits": 0, "rate_limit_wait_seconds": 0.0, "backoff_waits": 0, "backoff_wait_seconds": 0.0}
 ],
 "caches": [
  {"layer": "shared_bars", "source": "alpaca", "asset_class": "option", "reads": 2984, "hits": 2561, "misses": 423,
   "read_bytes": 31399344, "read_seconds": 0.0, "uploads": 0, "upload_bytes": 0, "upload_seconds": 0.0, "upload_errors": 0}
 ]
}
```

(A real 12-month Alpaca put-spread repeat run, cut down to one row per list.)

### Fields

| Field | Meaning |
|---|---|
| `wall_seconds` | From the start of `run_backtest` until the file is written. |
| `phases.data_setup_seconds` | Start until the first trading iteration: data source, broker, `initialize()`. |
| `phases.simulation_seconds` | First trading iteration until the simulation loop ends. Includes provider waits. |
| `phases.reports_seconds` | `backtest_analysis`: tearsheet, trades and indicator files, plots. |
| `phases.uploads_seconds` | Waiting at the end for background shared-cache uploads to S3. |
| `iterations.count`, `.total_seconds` | Trading iterations and the time inside `on_trading_iteration`. |
| `iterations.slowest_seconds`, `.slowest_at` | The slowest iteration and its simulated time. That is usually a first chain or contract load. |
| `providers[]` | One row per `source` and `asset_class`. |
| `providers[].requests` | Requests sent to the provider, including failed and retried ones. |
| `providers[].provider_seconds` | Seconds waiting for those answers, including downloader queue time. |
| `providers[].rows` | Rows the provider returned, where the layer can count them. |
| `providers[].retries`, `rate_limit_*`, `backoff_*` | Retries, and deliberate waits with count and seconds. `rate_limit` covers provider 429s and the client-side throttle. `backoff` covers downloader queue_full and transient errors. |
| `caches[]` | One row per `layer`, `source` and `asset_class`. `reads = hits + misses`. |

### Cache layers

| `layer` | What it is |
|---|---|
| `shared_bars` | The shared bar cache (`shared_bar_cache.py`, used by Alpaca and the broker data sources). A hit is a partition found in memory, on local disk or in S3. |
| `shared_bars_s3` | The S3 side of the same cache: GETs that found (hit) or did not find (miss) an object, and uploads. Cold runs also read S3 before each upload, to merge with other writers. |
| `backtest_cache` | `BacktestCacheManager` (ThetaData and IBKR parquet files in S3). Counted only when the S3 backend is on. |
| `alpaca_files` | Alpaca's per-window CSV files. Option keys always miss for picks: partial frames are never written. |
| `alpaca_option_chains` | Alpaca's per-day option chain files. |

### Where the counters come from

| Counter | Recorded in |
|---|---|
| Downloader requests (ThetaData, IBKR) | `data_downloader_queue_client.queue_request`. Backoff waits come from `QueueClient._backoff_sleep`. The source and asset class come from the path. IBKR sets its asset class through `ibkr_helper.get_price_data`. |
| Alpaca requests and waits | `AlpacaBacktesting._alpaca_request` and `_throttle_alpaca_requests`. Rows come from `alpaca_history.fetch_alpaca_bars`. |
| Shared cache | `shared_bar_cache` `_Store.load` (partitions), `_remote_get` and `_remote_put` (S3). |
| Phases and iterations | `StrategyExecutor._on_trading_iteration` and `Trader.run`. |

To attribute new work, wrap a fetch in `backtest_telemetry.provider_scope(source, asset_class)`. Generic transports inside the scope then record under it. Worker threads keep the scope: the shared cache submits work with `contextvars.copy_context()`.

## How to find why a backtest is slow

1. **Open `<run>_telemetry.json`.** Compare the four phases with `wall_seconds`.
   - Large `data_setup_seconds`: data source construction or `initialize()`.
   - Large `reports_seconds`: report files. trades.html on long minute runs, and the tearsheet.
   - Large `uploads_seconds`: many shared-cache partitions written on a cold run.
2. **Inside simulation, compare `providers[].provider_seconds` with `simulation_seconds`.** If the provider explains most of it, the run is waiting on data.
   - Many `requests` with few `rows`: the provider is being asked for nothing. Contracts that never traded are asked again on every run, because an empty answer is never cached.
   - Rate-limit or backoff seconds: the provider or the shared downloader lane is the limit, not LumiBot.
3. **Check the cache rows.** A repeat run should show `misses` near 0 and provider `requests` near 0. Many misses on a repeat mean the cache is not reused. Look for a different credential scope, a cache folder that is not kept, or the S3 backend switched off.
4. **If provider and cache time do not explain `simulation_seconds`, the time is LumiBot's own work.** `iterations.slowest_at` tells you where to look. Profile it:

   ```bash
   BACKTESTING_PROFILE=yappi python my_strategy.py
   ```

   This writes `<run>_profile_yappi.csv`. Sort it by total time. See `docs/BACKTESTING_SPEED_PLAYBOOK.md` for the full procedure.

### Patterns found in November 2026

- **One request per trading day for IBKR stock minute bars.** That is the floor: the downloader's REST route returns at most 1,000 points, and extended hours are about 960 bars. A walk ending at midnight used to split the last session over two pages; fixed in 4.6.52. More sessions per request need the downloader to serve multi-day TWS history.
- **Lookback at the first session open.** The routed source prefetched from the backtest start, so a thinly traded stock had too few bars at 09:31 on day one. It now keeps the IBKR source's five-day buffer.
- **Counters that missed a cache.** `[REMOTE_CACHE][SUMMARY]` showed 0 for Alpaca because the shared bar cache talks to S3 itself. It now has `[SHARED_BARS][SUMMARY]`, and both feed this file.
- **trades.html on long minute runs.** plotly copied the time axis once per line. It is now encoded once: about 4.5 s down to 1.3 s on a 12-month run.
- **Option picks.** A delta pick reads only from the pick month, batched up to 100 contracts per request. Later lookups extend those contracts once. Chain listings are fetched once per run.

## Regression guards (counts, never time)

`tests/test_backtest_request_count_guards.py` runs real LumiBot code against local stand-ins and asserts operation counts from the telemetry. Elapsed time is never asserted.

- A one-week IBKR stock minute backtest makes at most 16 IBKR requests cold (about one page per session) and none warm.
- A month of daily Alpaca option picks fetches chain listings at most twice in total. Each pick makes at most one batched request ending at its pick day and starting in the pick month. Warm runs make no Alpaca requests and have no shared-cache misses.

`tests/test_backtest_request_count_guards_sensitivity.py` proves those guards fail when the bad behavior comes back: picks read from the backtest start, or one-hour IBKR pages.
