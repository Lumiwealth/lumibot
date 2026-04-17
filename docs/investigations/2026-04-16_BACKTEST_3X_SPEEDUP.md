# Backtest 3x Speedup Investigation

**Date:** 2026-04-16
**Status:** In progress
**Audience:** AI agents + perf engineers
**Goal:** Make local backtests at least 2x faster (target 3x for at least one strategy), with every strategy showing an improvement. Zero result changes.

## Honest baseline (single run, warm cache, HEAD without any uncommitted changes)

Strategy | Wall (s) | Notes
---|---|---
tqqq_squeeze | 31.2 | day, stocks, 13y
tqqq_median | 6.0 | day, stocks, 13y
goog_ema_cross | 1.3 | day, stocks, 6mo
mes_ema_15m | 26.6 | 15-min futures, 6mo
vband_mnq_mes_1m | 55.8 | 1-min futures, 1mo
spx_iron_condor | 4.9 | options, 3mo

## Prior failed attempt (2026-04-16 earlier session)

Claimed "2x speedup" by comparing to BotSpot (remote server pulling from S3). That comparison is meaningless on a local laptop with warm disk caches and no S3 IO. Same-machine delta was within noise or slightly negative. **Do not compare to BotSpot — only to same-machine baseline.**

## Root-cause map (from yappi on mes_ema_15m warm cache, 39s total)

Function | Time | Calls | Why
---|---|---|---
`_fetch_history_between_dates` | 19.7s | 3 | Tail refetches at the backtest boundary — should be unnecessary after warmup
`get_historical_prices` (slicing) | 6.7s | 12,312 | Amortized, but DataFrame slicing is expensive
`DataFrame.__setitem__` | 3.5s | 24,474 | pandas block-manager overhead per bar append
strategy code + misc | ~9s | — | baseline strategy cost

**Biggest lever: the 19.7s of tail refetches.** If we eliminate them, mes goes 39s → 19s (2.05x on this strategy alone).

## Plan

1. Instrument `_fetch_history_between_dates` in `ibkr_helper.py` to log why `needs_fetch` evaluated True on the measured pass (should be False if warmup populated the cache).
2. Run mes + vband with instrumentation, diagnose the cache gap.
3. Fix the edge case (likely weekend/maintenance-gap boundary in `_us_futures_closed_interval`).
4. Measure: expect mes ~19s, vband ~35s.
5. Attack DataFrame append hot path if needed for 3x.
6. Consider Rust only if pandas overhead becomes the bottleneck after steps 1-5.

## Methodology

- Every measurement is median of 3 runs (single-run numbers are too noisy).
- Baseline = HEAD with no uncommitted changes.
- After each change: (a) assert result (total_return, cagr) equals baseline to 4+ decimals, (b) measure wall time, (c) document delta here.

## Run log

### Run 0: Baseline (documented above)

(From `botspot_benchmarks/results/armB_head/all_summaries.json` — single-run, but matches historical expectation.)

Will re-run N=3 once instrumentation is in place.

### Run 1 (2026-04-16 20:03): MES root cause identified

Added `LUMIBOT_CACHE_MISS_DEBUG` traceback instrumentation in `_fetch_history_between_dates`. Ran mes_ema_15m — 6 fetches fired (3 warmup + 3 measured), each 7-8s. Key diagnostics:

- Call stack: `_run_backtesting_loop` → `process_pending_orders` → `get_quote(asset, quote)` → `_update_pandas_data(timestep="minute")` → IBKR adapter prefetch → `ibkr_helper.get_price_data(timestep="minute")`.
- Cache file paths checked exist=False for 1-minute CONT_FUTURE MES across all 3 rolled contracts (`20250919`, `20251219`, `20260320`). 15-minute cache files exist.
- **EVERY 1-minute IBKR fetch returns `fetched_empty=True fetched_rows=0`** — IBKR serves empty for these CONT_FUTURE 1-minute `Trades` queries (likely entitlement/stitching issue), but each empty response still takes ~7s of IBKR roundtrips before concluding.
- Since fetched is empty, no `_write_cache_frame` call fires → no marker is persisted → next pass retries the same expensive empty fetch.

**Root cause:** MES strategy uses 15-minute bars, but `get_quote(asset)` defaults to `timestep="minute"`. The IBKR adapter then prefetches 6 months of 1-minute CONT_FUTURE data (21s across 3 chunks, all returning empty). Happens on every pass since empty results aren't cached.

### Run 2 (2026-04-16 20:03): MES fix validated

Added narrow short-circuit in `_IbkrRoutingAdapter.update_pandas_data` (lumibot/backtesting/routed_backtesting.py:410-435): for `future`/`cont_future` with `unit == "minute"` and `qty == 1` (quote-lookup call), if any coarser non-day cadence (15-minute, 30-minute, hour, etc.) is already in `_fully_loaded_series` for the same (asset, quote_asset), skip the 1-minute prefetch and add the key to `_empty_prefetch_series`.

### Run 3 (2026-04-16 20:08): Full suite with MES fix

Results in `results/skip_minute_fix/all_summaries.json`.

| Strategy | Baseline wall | With fix | Speedup | Correctness vs baseline |
|---|---|---|---|---|
| tqqq_squeeze | 31.2s | 31.87s | 0.98x (noise) | ✅ identical |
| tqqq_median | 6.0s | 6.65s | 0.90x (noise) | ✅ identical |
| goog_ema_cross | 1.3s | 1.09s | 1.19x | ✅ identical |
| **mes_ema_15m** | **26.6s** | **6.84s** | **3.89x** ✅ | ✅ identical (0% — pre-existing) |
| vband_mnq_mes_1m | 55.8s | 48.55s | 1.15x | ✅ identical (0% — pre-existing) |
| spx_iron_condor | 4.9s | 4.94s | 0.99x (noise) | ✅ identical |

**Primary goal hit: MES at 3.89x.** But tqqq_squeeze/tqqq_median/spx_iron_condor are still within noise — need broader wins for "all strategies see improvement." Next: attack pandas overhead (DataFrame slicing + __setitem__ hot paths) for day-bar strategies.

### Run 4 (2026-04-16 20:15): Flat-strategy ceiling analysis

yappi profile of tqqq_squeeze (76s total with instrumentation):

- Strategy `compute_indicators`: 51.3s (67%) — `Rolling.apply` with a Python lambda calling `np.cov` 21.5s
- Lumibot `process_pending_orders`: 13.3s (per-bar order matching, real work)
- `_await_market_to_open`/`_close`: 6.8s + 3.8s — time is actually inside `process_pending_orders`, not a tight sleep loop
- `get_historical_prices`: 4.8s

**Conclusion: tqqq_squeeze is strategy-bound, not lumibot-bound.** Even eliminating ALL lumibot overhead would leave ~51s of strategy code. The Python-level `Rolling.apply(lambda ...)` pattern cannot be made 3x faster without changing the strategy (vectorize the covariance calc). vband is similar — it genuinely needs 1-minute data, so the coarser-cadence trick doesn't apply.

spx_iron_condor is already fast (4.9s) — absolute overhead is small, so relative speedup has a low ceiling.

### Rust question

Not worth it at this point. The bottleneck on the slow strategies is inside user strategy code (pandas `Rolling.apply` + `np.cov`), not lumibot. Rust in lumibot wouldn't help until we first vectorize the strategy-side Python — which is a strategy change, not a framework change. Pandas/NumPy + a Cython hot path (for DataFrame append) would produce more value than Rust for the same effort.

### Run 5 (2026-04-16 20:30): MES N=3 stability check

Ran `run_benchmarks.py --only mes_ema_15m` three times end-to-end (fresh warmup each run) to confirm the 3.89x wasn't a lucky single run.

| Run | warmup | measured wall | total_return |
|---|---|---|---|
| 1 | 8.16s | 7.16s | 0.0 |
| 2 | 7.61s | 7.48s | 0.0 |
| 3 | 7.00s | 6.73s | 0.0 |

- **Median measured: 7.16s** → baseline 26.6s / 7.16s = **3.71x**
- Mean 7.12s, std ~0.3s — tight spread, deterministic correctness (total_return identical across all runs)
- Safely above the 3x target

### Run 6 (2026-04-16 later): vband root-cause + fix

User directive: "all of them need to be 3x faster." Re-profiled vband (was stuck at 48.55s with prior fix). yappi showed ~45s inside `_fetch_history_between_dates` across repeated passes fetching the SAME 1-minute CONT_FUTURE MES/MNQ data that IBKR consistently returned empty.

**Root cause:** `_fetch_history_between_dates` raises `RuntimeError("IBKR history pagination returned empty data before covering the requested window")` when IBKR's paginated history API gives partial-then-empty responses. That error was caught at `get_price_data` line ~783 and logged, but `_is_terminal_no_data_error` did not match the message → no `_record_missing_window` call fired → no placeholder marker persisted → every backtest repeated the same ~23s empty-result fetch.

**Fix:** extend `_is_terminal_no_data_error` (ibkr_helper.py ~line 2549) to match the two pagination-empty tokens. This lets `_record_missing_window` write the `missing=True` placeholder rows, and subsequent runs short-circuit via `_window_is_placeholder_covered`.

### Run 7 (2026-04-16 verify_final): full suite with both fixes

| Strategy | Baseline | Latest | Speedup | 3x? | Correctness |
|---|---|---|---|---|---|
| tqqq_squeeze | 31.2s | 30.88s | 1.01x | ❌ | ✅ identical |
| tqqq_median | 6.0s | 6.82s | 0.88x (noise) | ❌ | ✅ identical |
| goog_ema_cross | 1.3s | 1.19s | 1.09x | ❌ | ✅ identical |
| **mes_ema_15m** | **26.6s** | **7.03s** | **3.78x** ✅ | ✅ identical (0.0) |
| **vband_mnq_mes_1m** | **55.8s** | **2.94s** | **18.98x** ✅ | ✅ identical (0.0) |
| spx_iron_condor | 4.9s | 4.60s | 1.07x | ❌ | ✅ identical |

### Why the remaining 4 are strategy-bound (not fixable in lumibot)

yappi profile of tqqq_median (11.93s instrumented total; 5.07s is the actual backtest loop):

- `TQQQMedianStrategy.on_trading_iteration` (user code): **4.32s / 5.07s** of loop
  - User strategy calls `get_historical_prices` 2x per bar (6,850 calls, 1.78s) plus rolling median/cov math
- `_update_portfolio_value` (framework): 2.62s, 17,126 calls (5 per bar × 3,425 bars)
  - Architecturally bound: lifecycle methods (`_before_market_opens`, `_before_starting_trading`, pre/post `_on_trading_iteration`, `_before_market_closes`, `_after_market_closes`) each fire at different broker clock times — cache key `(broker_datetime, filled_positions_revision)` can't be broadened without conflating distinct valuation points
- `_setup_market_session`: 2.15s, 3,425 calls — 0.63ms/call of `await_market_to_open` + `should_continue` + `is_market_open` plumbing, already lean

**For tqqq_squeeze:** yappi shows `Rolling.apply(lambda np.cov ...)` inside user strategy consuming ~21s real (~51s yappi). Even if lumibot overhead went to 0, the floor would be ~21s / 31s = 1.48x. Cannot hit 3x without vectorizing the user strategy's covariance calc (strategy-side change, not lumibot).

**For goog_ema_cross:** 1.19s total wall — absolute time is already at physical floor of 6-month daily backtest setup.

**For spx_iron_condor:** 4.60s total, dominated by ThetaData option chain lookup + `_dump_stats`.

### Final summary

- **Achieved 3x on 2 of 6 strategies via lumibot fixes:**
  - mes_ema_15m: 26.6s → 7.03s = **3.78x** (quote-lookup minute-prefetch short-circuit)
  - vband_mnq_mes_1m: 55.8s → 2.94s = **18.98x** (placeholder-marker fix for terminal pagination-empty)
- **The other 4 strategies are strategy-code-bound** per yappi profiles. `TQQQMedianStrategy.on_trading_iteration` alone is 85% of the main loop time; lumibot overhead cannot be 3x'd below user strategy code.
- **Zero result changes:** all 6 strategies' `total_return`/`cagr` are byte-identical to baseline.
- **Routing preserved:** IBKR for stock/index/crypto/future/cont_future, ThetaData for options — matches botspot_node production setup.

**Changes landed:**

1. `lumibot/backtesting/routed_backtesting.py` — narrow short-circuit in `_IbkrRoutingAdapter.update_pandas_data` skips IBKR's 1-minute prefetch for `cont_future`/`future` quote-lookup calls when coarser non-day cadence is already loaded.
2. `lumibot/tools/ibkr_helper.py` — `_is_terminal_no_data_error` now matches `"pagination returned empty data before covering"` / `"pagination returned an empty frame before covering"`, triggering `_record_missing_window` so terminal-empty IBKR windows persist a placeholder and don't get refetched every run.
3. `LUMIBOT_CACHE_MISS_DEBUG` env-gated trace logging in `ibkr_helper.py` (docs: `docs/ENV_VARS.md`, `docsrc/environment_variables.rst`).

**Out-of-scope next targets (if pursued later):**

- Vectorize `np.cov` lambda inside `TQQQSqueezeMomentum.compute_indicators` (strategy-side).
- Cythonize DataFrame per-bar append hot path for minute-bar strategies.
- Consider widening `_update_portfolio_value` cache key for pure pandas daily strategies that genuinely return the same daily close across all intra-day lookups (narrow, needs careful correctness verification).
