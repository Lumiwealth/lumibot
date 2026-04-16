# Backtest Parallelism Assessment

One-line description: Assessment of where LumiBot backtests are inherently serial, where parallelism already exists, and which speedup ideas are realistic.

**Last Updated:** 2026-03-28
**Status:** Active
**Audience:** Developers, AI Agents

## Overview

This note records a focused assessment of the hypothesis that LumiBot backtesting is slow because it cannot run in parallel.

The short answer is:

- **Not crazy:** there is real speed headroom.
- **But the core issue is not simply “Python cannot run in parallel.”**
- A single LumiBot backtest is a **path-dependent discrete-event simulation**, so the core execution loop is intentionally serial for correctness.
- The best large gains usually come from **request fanout control, warm-cache reuse, batching, event-driven skipping, and multi-run orchestration**, not from parallelizing `on_trading_iteration()` directly.

## Executive Summary

### What is serial by design

1. A single backtest currently runs **one strategy at a time**.
2. The main backtest loop is:
   - `on_trading_iteration()`
   - `process_pending_orders()`
   - advance simulated clock
3. That order matters because:
   - cash depends on previous fills,
   - dependent orders (OCO, bracket, multileg) depend on earlier state transitions,
   - mark-to-market depends on the current simulated timestamp,
   - lifecycle hooks are tied to session boundaries.

Because of that, generic “parallelize the loop” work has a high risk of changing fills, timings, or PnL.

### What is already parallel today

Parallelism already exists in the **data layer**:

1. ThetaData chunk downloads can use a `ThreadPoolExecutor`.
2. Generic multi-asset `get_bars()` fanout uses a reusable thread pool.
3. Some backtesting data sources prefetch the full backtest window and reuse in-memory slices.
4. Prefetch helpers can schedule async/background work ahead of future iterations.

This means “LumiBot cannot run anything in parallel” is not accurate. The current limitation is specifically the **single-backtest strategy execution model**, not the entire system.

### What 10x means in practice

There are two very different meanings of “10x faster”:

1. **10x aggregate throughput across many independent backtests**
   - This is realistic.
   - The right approach is process/container-level concurrency for independent runs, with strict downloader/cache concurrency limits.

2. **10x wall-clock reduction for one backtest**
   - This is possible in some pathological cases, but usually because we eliminated:
     - repeated cache misses,
     - chain/quote request fanout,
     - repeated tiny-window fetches,
     - or heavy artifact generation.
   - It is **not** the most likely outcome from directly parallelizing the core strategy loop.

## Code Anchors

### Single-strategy backtest constraint

- `lumibot/traders/trader.py`
- `Trader.run_all()` rejects multiple strategies in backtest mode with “You can only backtest one at a time.”

### Serial backtest loop

- `lumibot/strategies/strategy_executor.py`
- `_run_backtesting_loop()` runs:
  - dividends update,
  - `self._on_trading_iteration()`,
  - `self.broker.process_pending_orders(...)`,
  - `_strategy_sleep()`,
  - then recalculates `time_to_close`.

### Path-dependent order processing

- `lumibot/backtesting/backtesting_broker.py`
- `process_pending_orders()` handles:
  - pending order collection,
  - async/background prefetch hooks,
  - OCO/bracket/multileg semantics,
  - fill execution,
  - and state transitions that must remain deterministic.

### Per-iteration portfolio valuation cost

- `lumibot/strategies/_strategy.py`
- `_update_portfolio_value()` iterates positions and fetches prices one asset at a time.
- This is a legitimate compute hotspot for warm-cache runs with many positions/contracts.

### Existing data parallelism

- `lumibot/tools/thetadata_helper.py`
- `lumibot/data_sources/data_source.py`
- `lumibot/backtesting/interactive_brokers_rest_backtesting.py`

These files already contain thread-pool or prefetch logic for reducing repeated downloader/API work.

## Local Benchmark Notes

I ran a small synthetic benchmark on local in-memory minute data to isolate engine cost from downloader/cache effects.

Setup:

- 20 trading days
- 1-minute cadence
- in-memory `PandasDataBacktesting`
- no plots / tearsheets / indicators

Observed results:

1. **Empty loop**
   - 7,800 iterations in about **6.0s**
   - about **1,298 iterations/sec**

2. **One `get_last_price()` per bar**
   - 7,800 iterations in about **10.4s**
   - about **752 iterations/sec**

Interpretation:

- The serial engine overhead is real.
- A 100k-iteration minute backtest can spend meaningful time in pure compute even with local in-memory data.
- But these numbers still do **not** support the idea that generic loop parallelization is the first or safest 10x lever.

## What Is Most Likely To Help

### 1. Process-level parallelism for independent runs

Best use cases:

- parameter sweeps,
- walk-forward windows,
- benchmark comparisons,
- strategy-vs-strategy experiments,
- many independent customer backtests.

Why:

- these jobs do not share state,
- correctness is preserved naturally,
- and scaling is operationally straightforward.

Guardrails:

- cap downloader concurrency,
- share warm caches,
- avoid fanout storms against Theta/IBKR/downloaders,
- keep outputs isolated per run.

### 2. Batch more work inside a single backtest

High-value candidates:

- batch portfolio valuation price lookups,
- batch multi-asset history loads,
- reuse already-loaded snapshots/quotes across MTM and order fill code paths,
- reduce per-asset repeated normalization work.

This is likely safer and more valuable than parallelizing arbitrary lifecycle logic.

### 3. Event-driven skipping

If a strategy does not need every intermediate timestamp, the engine should avoid iterating them.

Promising directions:

- event-driven clock advancement,
- sparse timestamp schedules,
- better reuse of provider-prefetched full windows,
- avoiding repeated “tiny slice” requests that reconstruct the same in-memory state.

This is one of the better paths to large single-run speedups.

### 4. Move artifacts off the critical path

Backtests can spend significant time after simulation on:

- tearsheets,
- charts,
- indicators,
- CSV/parquet generation,
- uploads.

If the user only needs core simulation results first, defer or parallelize post-processing after the trade stream is finalized.

### 5. Optimize provider-specific hydration

For option-heavy strategies especially:

- request fanout,
- chain breadth,
- quote/EOD fallback behavior,
- cache chunking,
- and redundant refresh logic

usually dominate before core Python loop parallelism does.

## What Is Unlikely To Pay Off

### “Turn LumiBot into a matrix engine”

This is not a good generic fit for LumiBot because strategies are arbitrary Python with:

- user-defined branching,
- mutable state,
- order callbacks,
- lifecycle hooks,
- broker-like fill semantics.

For a narrow subset of strategies, a separate vectorized engine could make sense, but that would effectively be a **different execution engine**, not a simple optimization of the current one.

### “Parallelize `process_pending_orders()` without a determinism model”

This is dangerous.

Even when two orders appear independent, their fills can interact through:

- available cash,
- order dependency chains,
- position exposure,
- fill ordering,
- per-bar lifecycle assumptions.

If this area is explored, it must be done with explicit determinism rules and trade-by-trade parity validation.

## Recommended Work Order

If future work resumes on backtest speed, the order should be:

1. Measure a real slow strategy with cold vs warm runs and profiler artifacts.
2. Decide whether the run is hydration-bound, compute-bound, or artifact-bound.
3. If many runs are needed, implement **process-level parallel backtest orchestration** first.
4. If one run is slow and warm-cache, pursue:
   - batching,
   - snapshot reuse,
   - event-driven skipping,
   - artifact deferral.
5. Only explore intra-loop parallelism after parity harnesses are in place.

## Documentation Gaps Noted

The internal docs are stronger than the public docs.

Public docs currently under-explain:

1. that a single backtest is serial by design,
2. that some data work is already parallelized,
3. that “many backtests at once” should be done outside one `Trader`,
4. and that realistic speedups depend on whether the run is cold-cache, warm-cache, compute-bound, or artifact-bound.

Recommended public-doc additions:

- a short “execution model” section,
- a “single-backtest vs multi-backtest parallelism” section,
- and a “realistic speedup expectations” note in the performance docs.

## Related Documents

- `docs/BACKTESTING_ARCHITECTURE.md`
- `docs/BACKTESTING_PERFORMANCE.md`
- `docs/BACKTESTING_SECOND_LEVEL_ROADMAP.md`
- `docs/BACKTESTING_SPEED_PLAYBOOK.md`
