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

Faster scanners: request history for many symbols in one call
--------------------------------------------------------------

A scanner that loops over its universe and calls ``get_historical_prices`` once per symbol
makes one data request per symbol the first time each symbol is seen. Ask for the whole
universe at once with ``get_historical_prices_for_assets`` instead:

.. code-block:: python

    from lumibot.strategies import Strategy

    class MomentumScanner(Strategy):
        def initialize(self):
            self.sleeptime = "1D"
            self.universe = ["AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "TSLA", "JPM"]

        def on_trading_iteration(self):
            # One grouped call for every symbol (returns {asset: Bars or None}).
            history = self.get_historical_prices_for_assets(self.universe, 60, "day")
            scores = {}
            for asset, bars in history.items():
                if bars is None or len(bars.df) < 60:
                    continue  # no data for this symbol: skip it, never guess
                close = bars.df["close"]
                scores[asset] = close.iloc[-1] / close.iloc[0] - 1
            best = sorted(scores, key=scores.get, reverse=True)[:3]
            self.log_message(f"Top 3: {[a.symbol for a in best]}")

What the grouped call does in a backtest:

- **Alpaca data** (``AlpacaBacktesting``): stock bars that are not cached yet are downloaded
  up to 100 symbols per request (Alpaca's bars endpoint accepts many symbols). 250 stocks
  need 3 requests instead of 250; at Alpaca's 200 requests per minute limit the one-by-one
  path has to wait for the next minute. Each symbol is stored in the same shared cache file
  as a single download, so the bars are identical and later runs read the cache.
- **Every source**: once bars are cached, a scan is in-memory lookups. Measured on synthetic
  cached daily bars, 500 stocks: about 35 ms per scan for 50 bars each.

Limits:

- A symbol with no bars comes back as ``None`` in the result; check for it.
- Options are not part of the grouped stock download (option chains have their own grouped
  download when a strategy picks strikes). Crypto symbols are still requested one by one.
- Minute bars for many symbols are large however they are requested: Alpaca returns at most
  10,000 bars per page, so grouping saves little there. Prefer daily bars for the scan and
  minute bars only for the few symbols you trade.

Want a scanner built for you? Describe it in plain English at `BotSpot <https://botspot.trade>`_
and backtest it there, or connect the free BotSpot MCP to your AI assistant.

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
