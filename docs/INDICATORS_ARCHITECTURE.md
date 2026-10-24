# Indicators Architecture

**Last Updated:** 2026-04-16
**Status:** Active
**Audience:** Contributors / AI sessions maintaining `lumibot/indicators/indicators.py`

## Overview

`self.indicators` is a per-strategy technical-indicator accessor layered on top
of the existing data-store infrastructure. It exists because strategies in
production were eating 10–50x wall-time on `df.rolling(...).apply(...)`
recomputes inside `on_trading_iteration` — the same window computed from
scratch every bar for the full lookback window.

The subsystem collapses that pattern to **one full-series compute** plus one
**O(log N) positional lookup** per iteration, keyed on
`(asset, timestep, indicator_name, kwargs)`. Customer-facing docs live at
`docsrc/indicators.rst`; this file documents the internals.

## File layout

- `lumibot/indicators/__init__.py` — re-exports `Indicators`, `IndicatorRow`.
- `lumibot/indicators/indicators.py` — single module. `Indicators` is the
  accessor, `IndicatorRow` is the read-only view over a single-bar row of a
  multi-column indicator result.
- `lumibot/strategies/_strategy.py` — binds `self.indicators` to a fresh
  `Indicators(self)` in the strategy constructor.
- `tests/test_indicators_unit.py` — hand-rolled `_make_strategy` harness; 13
  unit tests cover compute-once semantics, argument-keyed memo, custom
  dispatch, IndicatorRow access, empty input, unknown-indicator raises,
  time-before-first-bar, invalidate.

## Core flow

```
Strategy.on_trading_iteration
    self.indicators.sma(asset, length=200)
         │  __getattr__("sma") -> callable
         ▼
    _dispatch(asset, "day", "sma", {"length": 200}, custom_fn=None)
         │
         ├── _cache_key(asset, timestep, name, kwargs) -> hashable tuple
         ├── _full_history(asset, timestep) -> DataFrame or None
         │     ├── Try _read_store_df(data_source, asset, timestep)
         │     ├── If empty: get_historical_prices(length=_fallback_length)
         │     │             (purely to trigger routed/live prefetch)
         │     └── Re-read store, fall back to bars.df
         │
         ├── data_tag = (len(df), df.index[-1])   # staleness fingerprint
         │
         ├── If not cached OR data_tag mismatch:
         │     result = _compute(df, name, kwargs, custom_fn)
         │     self._cache[key] = (data_tag, result)
         │
         └── _at_current_bar(result)
               └── _position_at(index, now) via Index.searchsorted  # O(log N)
                   └── iloc[pos]   # O(1) row access
```

## Design decisions

### 1. Attribute-style passthrough instead of an explicit registry

The API is `self.indicators.rsi(asset, length=14)`, not
`self.indicators.register("rsi", ...)` + `self.indicators.get("rsi")`. This
was an explicit product decision — the registry pattern doubles the call-site
boilerplate for every indicator and makes the common case (just give me the
value now) noisy. The tradeoff: `__getattr__` dispatches to
`pandas-ta-classic` lazily, so a typo like `self.indicators.smaa(...)` fails
at call-time rather than at import-time.

### 2. Memo key carries a data-tag, not a timestamp

`data_tag = (len(df), df.index[-1])`. When the full-history frame grows —
common in routed/IBKR backtests where the adapter prefetches more data on
demand — the memo transparently recomputes. We do **not** key on
`id(df)` because routed adapters sometimes replace the underlying frame
in-place; we do **not** key on a sampling hash because that's more
expensive than the compute we're trying to skip.

The staleness check costs two cheap reads (`len` and index tail) per call,
which is negligible compared to the indicator compute.

### 3. Per-bar lookup uses `searchsorted`, not `.loc[:now].iloc[-1]`

Benchmarked on `tqqq_median` (13-yr daily, ~3,400 bars):
- `df.loc[:now].iloc[-1]` per iteration: **54.71s** end-to-end.
- `Index.searchsorted(now, side="right") - 1` + `iloc[pos]`: **5.59s**.

The label-slicing path allocated a fresh view every iteration **and** did a
timezone-aware comparison on every index value. `searchsorted` on a sorted
`DatetimeIndex` is O(log N) and allocation-free. This is the single biggest
perf win in the subsystem and the reason the compute-once pattern actually
pays off.

### 4. Fallback length is 10,000, not 100,000

`_fallback_length = 10_000`. `get_historical_prices(length=100_000, timestep="day")`
computes `start_datetime = now - 100_000 * 1day`, which is approximately
year 1573 — **outside pandas ns-timestamp range** (min is 1677-09-21). The
computation would silently produce a garbage `Timestamp` or raise
`OutOfBoundsDatetime`. 10,000 trading days (~40 years) is more than any
realistic backtest horizon while staying safely inside the pandas epoch.

### 5. `_full_history` always tries the store first, then triggers prefetch

Routed/IBKR backtests populate `_data_store` lazily — the store is empty
until the strategy calls `get_historical_prices` or `get_last_price`. On the
first `self.indicators.sma(...)` call, the store will be empty. We call
`get_historical_prices(length=_fallback_length)` **purely for its
side-effect** of triggering the routed adapter's prefetch, then re-read the
now-populated store. This is why `_full_history` reads the store
*twice* — before and after the fallback call.

The returned `bars.df` is used as a last-resort if the store is *still* empty
(e.g. PANDAS mode where the store isn't populated by that path).

### 6. `_find_in_store` is defensive

Different data sources have different `_data_store` key conventions:
- `PandasData._data_store` keys on `(asset, timestep)`.
- `RoutedBacktestingPandas._data_store` keys on a canonical `(asset, quote)` tuple.
- Some adapters expose `find_asset_in_data_store(asset, timestep=...)`, some
  only `find_asset_in_data_store(asset)`.

`_find_in_store` tries the typed helper with `timestep`, falls back to
without, and finally scans the store linearly for an asset-equal key. This
is intentionally defensive — the indicator subsystem should not break when a
new data source adds a store-key convention.

## What we deliberately don't do

- **No streaming / incremental indicator updates.** The full-history compute
  runs on first call or when the frame grows; it does not maintain
  partial-sum state. The indicators we expose (`sma`, `rsi`, `bbands`, custom
  rolling ops) are all cheap enough that recompute on frame growth is fine.
  Streaming is a 10x code-complexity delta for a 1.1x perf gain on the
  benchmarks we measured.

- **No disk cache.** The memo is per-strategy-instance and dies with it. A
  disk cache would need to invariant on the exact indicator implementation,
  all kwargs, **and** the exact underlying bar series (including any
  adjustments, splits, dividends) — a surface area we don't want to own. If
  a user needs cross-run persistence, they should serialize the result
  themselves.

- **No prev-bar or n-bars-back access.** The public API returns the
  **current** bar only. Strategies that need the prior bar (e.g. EMA-cross
  detection) currently shuttle that via `self.vars.prev_val = current_val`.
  Extending the API to support `self.indicators.sma(asset, length=20, offset=-1)`
  is plausible but has not been a blocker for the strategies we've ported.

## Known gaps / follow-ups

- **1-day MTM cosmetic diff** on the final backtest day when porting a
  strategy from the hand-roll pattern to `self.indicators`. Root cause: the
  old hand-roll pattern called `get_historical_prices(length=300)` on every
  iteration, which forced the routed/IBKR adapter to refresh the frame up to
  the last simulated day. The new subsystem only triggers prefetch when the
  store is empty, so the final day's bar may be one iteration stale. Trades
  are byte-identical; only the portfolio-value series at ``t == end_date``
  differs. Flagged cosmetic; not worth fixing by forcing a prefetch on every
  indicator call (that would undo the perf win).

- **`compute_indicators` rewrite in `botspot_agent` prompts** is still
  pending (task #32). The goal is that `refine_strategy` rewrites legacy
  `compute_indicators(df)` → `self.indicators.custom("label", fn, asset, timestep="day")`
  automatically when it touches an indicator-heavy strategy. This is
  tracked separately and touches agent prompt files, not the core lumibot
  library.

## Performance receipts

Benchmarked on `/Users/robertgrzesik/Documents/Development/Strategy Library/Demos/botspot_benchmarks/`
with `compare_artifacts.py` verifying byte-identical trades.

| Strategy               | Baseline  | With `self.indicators` | Speedup |
|------------------------|-----------|------------------------|---------|
| `tqqq_squeeze_momentum`| 33.19s    | 10.08s                 | **3.29x** |
| `tqqq_median`          | 6.42s     | 5.59s                  | 1.15x  |

Both ports produce byte-identical trades.csv; stats.csv differs only at the
final day's MTM (cosmetic, see "Known gaps").

## Change log

- **2026-04-16** — O(log N) per-bar lookup via `searchsorted`; data-tag
  staleness detection; prefetch-aware full-history fallback;
  `_fallback_length` reduced from 100k to 10k. Commit `fa12dee5`.
- **2026-04-15** — Initial subsystem landed as `12a77a9a` alongside 4.5.0
  version bump.
