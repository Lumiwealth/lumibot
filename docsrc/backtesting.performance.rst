.. _backtesting_performance:

Backtesting Performance (Speed + Parity)
========================================

.. meta::
   :description: This page explains how to make backtests faster without changing strategy correctness. Performance issues are usually dominated by one of:.

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

Wake hints: skip iterations your strategy ignores
-------------------------------------------------

Many strategies run every minute but only act at a few moments: an entry time, a stop or
target level, an exit time. ``self.wake_when(...)`` tells a backtest about that. Until a
condition is met, the backtest skips calling ``on_trading_iteration``. Orders are still
processed and filled every iteration, the portfolio is still valued and the stats file still
gets a row each iteration, so an honest hint gives the same trades and stats, only faster.

.. code-block:: python

    def on_trading_iteration(self):
        spy = Asset("SPY")
        position = self.get_position(spy)
        if position is None:
            # ... entry logic at 10:00 ...
            return
        price = self.get_last_price(spy)
        if price <= self.vars.stop or price >= self.vars.target:
            self.sell_all()
            return
        # Nothing to do until the stop, the target or 15:50. Wake a cent early on each side.
        self.wake_when(
            assets=spy,
            price_below=self.vars.stop + 0.01,
            price_above=self.vars.target - 0.01,
            at=self.get_datetime().replace(hour=15, minute=50),
        )

What you can wait for:

- ``at``: a time; ``every``: an interval (same formats as ``sleeptime``).
- ``price_above`` / ``price_below``: the last price of the ``assets`` you name.
- ``value_above`` / ``value_below``: the value of your positions in those assets
  (quantity x last price x multiplier; short positions count as negative). Useful for a
  multi-leg position such as an option spread.

The backtest also wakes your strategy, whatever the hint says, when one of its orders is
created, filled, partly filled, canceled or rejected, when its cash or positions change for
any other reason (expiry, settlement, dividends, splits), when a lifecycle method runs (so the
first iteration of each session always runs) and when a watched price cannot be read. A hint
lasts until the next time ``on_trading_iteration`` runs; set it again there to keep sleeping.

Rules for an honest hint:

- Only skip time your strategy really does nothing with. If it plots a line every minute or
  checks a condition you did not put in the hint, the results will differ.
- Waking early is always safe; waking late is not. Set levels a little inside your thresholds.
- Live trading ignores hints and runs every iteration, so the same code works in both.

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

On Alpaca data, ``find_strike_for_delta`` asks for all of its candidate strikes in one batched request (up to 100 contracts per request) and loads each contract's trade prints only up to the current backtest day. The rest of a contract's window is loaded once, the first time the strategy reads that contract on a later day. Prices and the chosen strike are the same as loading each contract on its own; the backtest makes far fewer requests. Data sources can offer the same through a ``prefetch_option_bars(assets, until=None)`` method.

For ThetaData details, see :doc:`backtesting.thetadata`.

IBKR history and reuse
----------------------

IBKR equity warmup requests count bars across exchange sessions, including
weekends and holidays. A larger indicator lookback can extend the prefetched
window; repeated requests for the same completed window reuse it.

Routed IBKR daily bars retain their native session-close timestamps. Futures
positions use intraday marks rather than the prior daily candle. This may require
an initial minute-history fetch even for a daily futures strategy; later marks
reuse that series. Missing required history or an unknown valuation gap makes
the result incomplete rather than a verified zero-trade result.
