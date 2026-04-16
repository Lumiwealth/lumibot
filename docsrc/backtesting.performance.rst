.. _backtesting_performance:

Backtesting Performance (Speed + Parity)
========================================

This page explains how to make backtests faster **without changing strategy correctness**. Performance issues are usually dominated by one of:

- **Startup** (python import time, environment loading, first progress update)
- **Data hydration** (first run downloads data; warm runs should reuse cache)
- **Compute** (strategy logic, pandas transforms, option pricing)
- **Artifacts** (tearsheets, plots, indicators)

If you are new to backtesting, start with :doc:`backtesting.how_to_backtest`.

Warm vs cold runs
-----------------

Backtest speed depends heavily on caching:

- **Cold run:** the cache is empty, so the backtest must fetch historical data.
- **Warm run:** the cache already contains required data, so the backtest should be dramatically faster.

If a “warm” run is still slow, the most common causes are:

- your cache backend is not configured (or not writable)
- your cache namespace changed between runs
- a request type is not being cached (so it keeps downloading)

For deeper cache semantics (engineering notes), see ``docs/remote_cache.md`` in the repository.

Quick diagnosis checklist
-------------------------

1. **Is the backtest downloading a lot of data?**

   - Look for many “Submitted to queue” log lines (ThetaData) or repeated API calls (Polygon).
   - If yes, you are hydration-bound: fix request fanout or cache coverage first.

2. **Is the backtest slow even with near-zero downloads?**

   - If yes, you are compute/IO/artifact-bound: use profiling to attribute time.

3. **Does the backtest look stuck in the UI?**

   - If data is downloading, progress may not advance unless a heartbeat is enabled.

Execution model and realistic parallelism
-----------------------------------------

One LumiBot backtest runs a single strategy through a single simulated broker clock. That means the core execution path is intentionally serial:

- strategy logic runs,
- pending orders are processed,
- then the simulated clock advances.

This is important for correctness because fills, cash, positions, bracket/OCO behavior, and mark-to-market all depend on prior state.

What this means in practice:

- **Do not expect arbitrary strategies to become 10x faster just by “making the loop parallel.”**
- Generic vectorized or matrix-style execution is not a good fit for the current LumiBot execution model.

Where parallelism does help:

- provider-side data downloads and prefetch,
- running many **independent** backtests at once (for example parameter sweeps),
- and deferring expensive report generation until after the simulation is complete.

Practical guidance:

- If one backtest is slow, first determine whether it is hydration-bound, compute-bound, or artifact-bound.
- If you need many backtests, run multiple backtests externally with bounded concurrency rather than expecting one backtest to fan out internally.

Profiling (YAPPI)
-----------------

To attribute where time is spent (S3 IO vs compute vs artifacts), enable profiling:

- Set ``BACKTESTING_PROFILE=yappi``
- Run the backtest
- Inspect the produced ``*_profile_yappi.csv`` artifact

Common hotspots to look for:

- S3 IO (many small objects can be slow even on “warm” runs)
- pandas transforms (merge/concat/tz conversions)
- artifact generation (tearsheet, indicators, plots)

Environment variables
---------------------

Many backtesting behaviors are configurable via environment variables. See:

- :doc:`environment_variables` (public docs)
- ``docs/ENV_VARS.md`` (engineering notes; may include contributor-specific details)

Common performance-related flags:

- ``LUMIBOT_DISABLE_DOTENV``: disables recursive ``.env`` discovery (reduces startup latency and avoids accidental config overrides)
- ``SHOW_TEARSHEET`` / ``SHOW_PLOT`` / ``SHOW_INDICATORS``: disables heavy artifact generation when you only need core results
- ``BACKTESTING_PROFILE``: enable profiling (yappi)

ThetaData options: common performance pitfalls
----------------------------------------------

Options backtests can be slower than stock backtests because they may need:

- option chains (expirations/strikes)
- quote history (bid/ask) for realistic pricing
- additional mark-to-market logic for illiquid contracts

The fastest options backtests are those that:

- build **only the chain data they need** (one expiry and a narrow strike neighborhood)
- avoid probing hundreds/thousands of strikes when searching for a delta/ATM contract
- reuse cached quote history instead of requesting tiny windows repeatedly

In practice, the easiest way to get this right is to use :doc:`options_helper` for strike/expiry selection (for example ``OptionsHelper.find_strike_for_delta(...)``) instead of manually scanning chains and calling ``get_greeks()`` per strike.

For ThetaData details, see :doc:`backtesting.thetadata`.
