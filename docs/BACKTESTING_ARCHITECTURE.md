# BACKTESTING_ARCHITECTURE.md - LumiBot Backtesting Architecture

## Overview

LumiBot is a trading and backtesting framework. This document focuses on the **backtesting architecture**, specifically how data flows from external sources (Yahoo, ThetaData, IBKR Client Portal REST, Polygon) into the backtesting engine.

**CORE PRINCIPLE: Backtesting must mimic live broker behavior.**

**Accuracy (gold standard):** if we can replay a period that was traded live and reproduce the broker’s realized behavior (fills + PnL) within defined tolerances (tick size, fees model).

### Accuracy validation ladder (Tier 3 is the real gold standard)

- **Tier 1 (regression):** vendor parity / stored artifact baselines (e.g., DataBento-era runs) to detect drift.
- **Tier 2 (audit):** manual reviews around known hard edges (session gaps, holidays/early closes, rolls, rounding).
- **Tier 3 (gold):** **live replay baseline** — replay an interval that was traded live and reproduce broker fills + realized PnL within tolerances.

**Speed:** warm-cache runs are queue-free and complete in bounded wall time, with evidence (request counts, cache hit rate, iterations/sec, and wall-time split: data wait vs compute vs artifacts).

**Resilience:** simulation completion must not be masked by post-processing failures (stats/tearsheets/plots). When post-processing fails, the run should still produce as many artifacts as possible and classify the failure (simulation vs postprocess vs upload), so operators can trust the trade stream even when reporting breaks.

If the backtest execution model (data semantics, fill model, order handling, fees, pricing) diverges meaningfully from how real brokers behave, the backtest is not trustworthy.

We optimize for:
1) **Accuracy / realism first** (broker-like behavior; no hidden optimism or lookahead leaks)
2) **Speed second** (make it fast *without changing semantics*)

## Related Docs

- Handoffs: `docs/handoffs/`
- Investigations: `docs/investigations/`
- Performance + parity + startup: `docs/BACKTESTING_PERFORMANCE.md`
- Latest session handoff (IBKR speed + resilience): `docs/handoffs/2026-01-26_IBKR_SPEED_RESILIENCE_MASTER_HANDOFF.md`

## Directory Structure

```
lumibot/
├── backtesting/           # Backtesting data source implementations
│   ├── backtesting_broker.py        # Core BacktestingBroker class
│   ├── yahoo_backtesting.py         # Yahoo Finance adapter
│   ├── thetadata_backtesting_pandas.py  # ThetaData adapter
│   ├── interactive_brokers_rest_backtesting.py # IBKR (Client Portal REST) adapter
│   ├── routed_backtesting.py        # Multi-provider router (Theta + IBKR)
│   ├── polygon_backtesting.py       # Polygon.io adapter
│   └── pandas_backtesting.py        # Base class for pandas-based sources
│
├── data_sources/          # Base data source classes
│   ├── data_source.py               # Abstract DataSource base
│   ├── data_source_backtesting.py   # DataSourceBacktesting base
│   ├── yahoo_data.py                # Yahoo data fetching
│   ├── pandas_data.py               # Pandas data handling
│   └── polars_data.py               # Polars data handling
│
├── tools/                 # Helper modules for data fetching
│   ├── thetadata_helper.py          # ThetaData API & caching (IMPORTANT)
│   ├── ibkr_helper.py               # IBKR API (via downloader) & caching
│   ├── yahoo_helper.py              # Yahoo Finance API
│   ├── polygon_helper.py            # Polygon.io API & caching
│   └── backtest_cache.py            # S3/local cache management
│
├── strategies/            # Strategy execution
│   ├── strategy.py                  # Main Strategy class
│   └── _strategy.py                 # Internal strategy logic
│
└── entities/              # Data structures
    ├── asset.py                     # Asset class
    ├── bars.py                      # OHLCV bars
    ├── data.py                      # Pandas-based Data class (ThetaData, Yahoo, Polygon)
    ├── data_polars.py               # Polars-based DataPolars class (Databento ONLY)
    └── order.py                     # Order handling
```

## Data Flow for Backtesting

```
┌─────────────────────────────────────────────────────────────────────────┐
│                           Strategy.backtest()                            │
│                    (lumibot/strategies/_strategy.py)                     │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    Data Source Selection (line ~1466)                    │
│                                                                          │
│  BACKTESTING_DATA_SOURCE env var OVERRIDES explicit datasource_class    │
│                                                                          │
│  Options: yahoo, thetadata, ibkr, router, polygon, alpaca, ccxt, databento │
│  Set to "none" to use explicit class from code                          │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                    ┌───────────────┼───────────────┐
                    ▼               ▼               ▼
           ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
           │    Yahoo     │ │  ThetaData   │ │    IBKR      │ │   Polygon    │
           │  Backtesting │ │  Backtesting │ │  Backtesting │ │  Backtesting │
           └──────────────┘ └──────────────┘ └──────────────┘ └──────────────┘
                    │               │               │               │
                    ▼               ▼               ▼               ▼
           ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
           │ YahooHelper  │ │ thetadata_   │ │  ibkr_helper │ │  polygon_    │
           │              │ │   helper     │ │              │ │   helper     │
           └──────────────┘ └──────────────┘ └──────────────┘ └──────────────┘
                    │               │               │
                    ▼               ▼               ▼
           ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
           │  yfinance    │ │    Data      │ │ Polygon API  │
           │   library    │ │  Downloader  │ │              │
           └──────────────┘ └──────────────┘ └──────────────┘
                                    │
                                    ▼
                           ┌──────────────┐
                           │  S3 Cache    │
                           │  (optional)  │
                           └──────────────┘
```

## Execution Model and Concurrency Boundaries

Single-backtest execution is intentionally **serial**.

- A backtest currently runs **one strategy per `Trader`**. Backtesting multiple strategies in one shared run is rejected rather than silently interleaving them.
- The core execution model is a path-dependent loop:
  1. execute `on_trading_iteration()`
  2. process pending orders / fills
  3. advance the simulated clock
- This ordering is correctness-critical because cash, positions, OCO/bracket/multileg dependencies, option lifecycle tasks, and mark-to-market all depend on the prior state.

This has an important performance implication:

- **Do not assume that “more threads” inside one backtest will preserve behavior.**
- Generic matrix/vector-style acceleration is not a natural fit for arbitrary LumiBot strategies because strategies are free-form Python with mutable state and broker-like callbacks.

Where parallelism **does** fit today:

- provider/data hydration (parallel chunk downloads, async prefetch, multi-asset bar fanout),
- independent backtest runs (parameter sweeps, window sweeps, strategy comparisons),
- and some bounded batching opportunities inside one run (for example grouped price lookups).

Practical guidance:

- If you need “10 backtests at once,” prefer **process/container-level concurrency for independent runs**.
- If one backtest is slow, first determine whether the dominant cost is:
  - data hydration,
  - compute inside the serial loop,
  - or artifact generation.

See also:

- `docs/BACKTESTING_PERFORMANCE.md`
- `docs/investigations/2026-03-28_BACKTEST_PARALLELISM_ASSESSMENT.md`

## Key Components

### 1. BacktestingBroker (`backtesting/backtesting_broker.py`)

The core broker for simulating trades during backtests:
- Manages simulated positions, orders, and cash
- Tracks market sessions and trading calendars
- Handles futures margin requirements
- Requires a `DataSourceBacktesting` instance

### 2. Data Source Hierarchy

```
DataSource (ABC)
    └── DataSourceBacktesting (ABC)
            ├── PandasData                        # Uses entities/data.py (Data class)
            │   ├── PolygonDataBacktesting
            │   └── ThetaDataBacktestingPandas
            ├── YahooData
            │   └── YahooDataBacktesting
            └── PolarsData                        # Uses entities/data_polars.py (DataPolars class)
                └── DatabentoBacktestingPolars
```

### Entity Classes: Pandas vs Polars

**IMPORTANT:** The `Data` class (pandas-based) and `DataPolars` class (polars-based) are NOT interchangeable.

| Entity Class | File | Used By | Description |
|--------------|------|---------|-------------|
| `Data` | `entities/data.py` | ThetaData, Yahoo, Polygon, Alpaca, CCXT | Pandas-based OHLCV storage with bid/ask support |
| `DataPolars` | `entities/data_polars.py` | Databento ONLY | Polars-based OHLCV storage (optimized for Databento's format) |

**Why the distinction:**
- Databento provides data in a format optimized for polars
- Most other sources (ThetaData, Yahoo, Polygon) use pandas DataFrames
- The two entity classes have similar interfaces but different internal implementations
- **DO NOT** modify `data_polars.py` when fixing ThetaData issues

**Key Methods Both Provide:**
- `get_last_price(dt)` - Get the last *trade-based* price at datetime (close/open from bars; never bid/ask)
- `get_price_snapshot(dt)` - Get OHLC + bid/ask snapshot (used for mark/MTM and quote-based fills)
- `get_iter_count(dt)` - Get iteration index for datetime

### 3. Yahoo Finance (`yahoo_backtesting.py` → `yahoo_data.py` → `yahoo_helper.py`)

**Flow:**
1. `YahooDataBacktesting` inherits from `YahooData`
2. `YahooData` uses `YahooHelper` to fetch data via `yfinance` library
3. Data is **already split-adjusted** by Yahoo
4. No additional split processing needed

**Key Function:** `YahooHelper.get_historical_prices()`

### 4. ThetaData (`thetadata_backtesting_pandas.py` → `thetadata_helper.py`)

**Flow:**
1. `ThetaDataBacktestingPandas` inherits from `PandasData`
2. Calls `thetadata_helper.get_price_data()` to fetch data
3. Data comes from either:
   - a **local ThetaTerminal** (default / public), or
   - the internal **Data Downloader** service (when `DATADOWNLOADER_BASE_URL` is set).
4. Uses S3 cache for performance (when enabled)

**Key Functions:**
- `get_price_data()` - Main entry point (line 1248)
- `_apply_corporate_actions_to_frame()` - Handles splits (line 1018)

### ThetaData Data Downloader (remote service)

This is an **internal/proprietary** service that can proxy ThetaData requests and provide queuing/concurrency controls.

- Selection rule:
  - If `DATADOWNLOADER_BASE_URL` is set, LumiBot routes ThetaData through the downloader queue and **must not** manage
    any local ThetaTerminal process (single-session constraint).
  - Otherwise, LumiBot auto-manages a local ThetaTerminal.

- Base URL (internal): `http://localhost:8080` (local) or `https://<your-downloader-host>:8080` (remote)
- Avoid hard-coded downloader IPs (they can change on redeploy)
- Local downloader code checkout: `Documents/Development/botspot_data_downloader`

Infrastructure notes (read-only):
- DNS is typically controlled via AWS Route53; when investigating, use AWS CLI **read-only** commands to inspect record sets (do not mutate).

## Pricing Semantics (CRITICAL)

LumiBot intentionally separates *trade-based* pricing from *quote/mark* pricing:

- **`get_last_price()` = last traded price only**
  - Backtests: bar-derived last trade (usually `close`, or `open` before bar completion for intraday).
  - Never uses `bid`, `ask`, or `mid` as a fallback.
  - Options can be stale for long periods (no prints); that is realistic.

- **`get_quote()` / snapshots = bid/ask/mark**
  - Quotes can exist even when there are no trades (especially for options).
  - Quote-derived mark pricing (mid) is the correct input for:
    - mark-to-market portfolio valuation, and
    - quote-based fills in illiquid markets (ThetaData backtests only).

This is essential to ensure ThetaData backtests behave like live brokers: brokers return stale last trades, and only quote endpoints provide NBBO/mark.

### SMART_LIMIT Backtest Fills

- SMART_LIMIT fills use **mid ± slippage** when bid/ask is available (mid + slippage for buys, mid - slippage for sells).
- If bid/ask is missing, SMART_LIMIT **downgrades to market** (next-bar open) and logs a warning.
- Inside-spread fills are allowed because they occur regularly in live markets.

## Backtesting Portfolio Valuation (Mark-to-Market)

During backtests, portfolio value is recalculated in strategy code (not fetched from a broker):

- Primary location: `lumibot/strategies/_strategy.py`
  - `_update_portfolio_value()` iterates tracked positions and calls `_get_price_from_source()` per asset.
  - `_get_price_from_source()` prefers a **snapshot** when the data source supports it (faster and richer than `get_last_price()`).

For ThetaData option backtests specifically:
- The MTM path prefers **quote-derived mark** (mid) when bid/ask are available (broker-like option MTM).
- If bid/ask are unavailable, it falls back to **last trade** (trade-only).
- If no current price is available, the backtester may **forward-fill the last known price** for that asset to avoid valuing an illiquid option at 0.
  - This forward-fill behavior can create a “boxy” equity curve (flat stretches then jumps) if the option cannot be priced on many days.
  - To diagnose, run with `BACKTESTING_QUIET_LOGS=false` and look for forward-fill warnings, and confirm option day EOD frames contain actionable bid/ask.

## ThetaData Option MTM “Sawtooth” Failure Mode (FIXED - Dec 2025)

**Symptom:** the backtest equity curve “sawtooths” (sharp down/up flips day-to-day), typically when holding options.

This is almost always **mark-to-market pricing instability**, where the same option position is sometimes priced correctly and sometimes effectively priced at/near 0 (or forced into a bad fallback path). The result looks like the portfolio is repeatedly losing and regaining a large portion of value even though the underlying didn’t move that much.

### Primary root cause (ThetaData day cadence)

For ThetaData daily option pricing, we rely heavily on **EOD NBBO bid/ask** columns (quotes can exist even when there are no prints).

One major failure mode is in the data normalization/repair path:
- `Data.repair_times_and_fill()` (in `lumibot/entities/data.py`) historically treated quote columns like OHLC and could incorrectly clear or mis-fill `bid`/`ask` across session gaps.
- Once `bid`/`ask` are missing for some bars, option MTM becomes intermittently “unpriceable”.

### Secondary root cause: ThetaData option EOD gaps (quotes exist, EOD missing)

ThetaData can return “no data” / placeholder responses for some option **EOD/day history** requests even when the same contract has
actionable **intraday quote history** (NBBO bid/ask).

In daily-cadence strategies, if MTM pricing relies exclusively on the EOD/day history path, the strategy can become unable to
value or exit an option position and may log:

- “Skipping valuation … because no price was available …”

This can produce flat or misleading equity curves and tearsheets, even if the strategy logic is correct.

**Fix direction (implemented):**
- For ThetaData option backtests, daily cadence now falls back to an **intraday snapshot quote mark** (`snapshot_only=True`) when the
  day/EOD quote path has no actionable bid/ask mark.

Investigation write-up:
- `docs/investigations/2026-01-06_THETADATA_OPTION_EOD_GAPS_DAILY_MTM.md`

### Fixes that prevent the sawtooth

These fixes keep MTM stable without changing strategy logic:

1. **Preserve daily option quote columns across session gaps**
   - File: `lumibot/entities/data.py`
   - Behavior: daily quote columns (`bid`, `ask`, etc.) survive the repair/fill process instead of being cleared.
   - Regression test: `tests/test_data_repair_times_and_fill_daily_quotes.py`

2. **Option MTM prefers quote-derived mark and avoids “bad zeros”**
   - File: `lumibot/strategies/_strategy.py`
   - Behavior (ThetaData options): prefer mid from bid/ask when actionable; ignore bid/ask zeros; if still unpriceable, return `None` so the backtester forward-fills rather than flipping to 0; do not fall back to a stale last-trade in a way that creates discontinuities.
   - Regression test: `tests/test_thetadata_option_mtm_prefers_quote_mark.py`

### How to confirm it’s fixed (quick analysis)

From the backtest `*_stats.csv`:
- Slice one row per trading day (typically the `16:00:00` America/New_York row).
- Compute daily returns.
- The sawtooth shows up as **many** days with very large absolute moves (e.g., ≥20%), often alternating sign on adjacent days.

## Validation Backtests (Acceptance Suite)

These are **manual acceptance backtests** run from the Strategy Library (do not edit the demo strategies). They validate the full data → pricing → order simulation pipeline, not just unit tests.

Artifacts are written to:
- `/Users/robertgrzesik/Documents/Development/Strategy Library/logs/`

These same demo scripts are also executed in GitHub CI as normal `pytest` tests (copies live under
`tests/backtest/acceptance_strategies/` and are run by `tests/backtest/test_acceptance_backtests_ci.py`).

CI uses the same env-var model as local/prod backtests (ThetaData via the remote Data Downloader + S3 cache enabled),
but enforces one additional invariant: the dev S3 cache is expected to already be warm for the canonical windows.
So any downloader/queue usage during the CI acceptance suite is treated as a cache regression and fails the test.

### 1) Deep Dip Calls (GOOG; file name says AAPL)

- Demo file: `Strategy Library/Demos/AAPL Deep Dip Calls (Copy 4).py`
- Required window: `2020-01-01 → 2025-12-01`
- Checks:
  - At least **3** option-entry buys across the 2020 / 2022 / early-2025 dip windows.
  - No catastrophic portfolio-value “split cliff” around the GOOG split (mid-July 2022).
  - Trades/indicators/tearsheet artifacts exist.

### 2) Alpha Picks LEAPS (Call Debit Spreads)

- Demo file: `Strategy Library/Demos/Leaps Buy Hold (Alpha Picks).py`
- Required short window: `2025-10-01 → 2025-10-15`
  - Checks: UBER/CLS/MFC each opens a spread with **both legs filled**.
- Optional 1-year window (debugging + confidence): `2025-01-01 → 2025-12-01`
  - Checks: STRL/APP may skip for strategy-logic reasons (DTE constraint / budget cap / no valid long-dated expiration), but should not fail due to missing-data regressions.

### 3) TQQQ SMA200 (ThetaData vs Yahoo)

- Demo file: `Strategy Library/Demos/TQQQ 200-Day MA.py`
- Window: `2013-01-01 → 2025-12-01`
- Checks:
  - ThetaData results should not be obviously inflated vs Yahoo.
  - Goal is “close-ish” parity (ThetaData can be slightly better/worse).

### 4) Backdoor Butterfly 0DTE (Index/Index Options Coverage)

- Demo file: `Strategy Library/Demos/Backdoor Butterfly 0 DTE (Copy).py`
- Window: `2025-01-01 → 2025-12-01`
- Checks:
  - Backtest completes without `[THETA][COVERAGE][TAIL_PLACEHOLDER]` aborts for SPX index data.
  - Artifacts exist.

### 5) MELI Deep Drawdown Calls (Legacy strategy; MTM + tearsheet sanity)

- Demo file: `Strategy Library/Demos/Meli Deep Drawdown Calls.py`
- Window: `2013-01-01 → 2025-12-18` (or through Dec 2025)
- Checks:
  - No option MTM sawtooth pattern during 2024 (see “Sawtooth” section above).
  - Tearsheets render and the strategy’s trade cadence looks plausible for the drawdown logic.

## Daily Bars: Timestamp Alignment (CRITICAL)

ThetaData’s EOD day data is keyed by trading date, but returned timestamps may not be aligned to the actual market session close.

**Failure mode (lookahead bias):**
- If “day” bars are timestamped at `00:00 UTC`, the bar becomes observable in New York time **before** the session, effectively leaking the full day OHLC.

**Fix direction (implemented for ThetaData day bars):**
- Align all ThetaData “day” frames to the **market close timestamp** (`16:00 America/New_York`, converted to UTC).
- Ensure the transform is idempotent and applies consistently on:
  - cache load,
  - cache hit return,
  - fresh EOD fetch results,
  - placeholder rows.

Primary location: `lumibot/tools/thetadata_helper.py` (day-index alignment helpers).

## Intraday Bars: Session-Close Coverage (CRITICAL)

ThetaData index/stock intraday (minute/hour) feeds are often **regular-session (RTH) bounded**.
For example, SPX index minute OHLC typically yields ~391 bars/day and ends at the trading session close (or early close).

**Failure mode (performance + correctness):**
- If the backtest “required end coverage” timestamp is interpreted literally as `23:59` (or `18:59` ET due to UTC-midnight transport),
  the cache can never be considered “complete” for an RTH-bounded feed.
- This can trigger an endless loop of:
  - `[THETA][CACHE][STALE] prefetch_complete but coverage insufficient` and
  - `Submitted to queue ... v3/index/history/ohlc ...`
  even on “warm” runs.

**Fix direction (implemented for ThetaData index intraday):**
- Define “coverage complete” for index intraday by the **last trading session close at or before** the end requirement
  (holiday/weekend/early-close safe), rather than requiring bars through an arbitrary end datetime.

See:
- `docs/investigations/2026-01-13_SPX_INTRADAY_STALE_LOOP_FIX.md`

## ThetaData Coverage Gap: NDX Underlying (CRITICAL)

ThetaData provides **NDX options** history, but does **not** provide the **NDX index underlying** (price/OHLC) history.
In practice, `v3/index/history/*` requests for `NDX` can return placeholder all-zero OHLC rows or `NO_DATA`.

**Failure mode:**
- Strategies that trade NDX options still require an underlying price series for:
  - signals / indicators,
  - moneyness checks,
  - strike selection heuristics,
  - portfolio valuation / cash settlement.
- When NDX underlying history is empty/placeholder-only, the backtest can repeatedly refetch and never progress.

**Platform fix (ThetaDataBacktesting only):**
- LumiBot proxies `Asset("NDX", asset_type=INDEX)` underlying bars/quotes via `QQQ` and scales into NDX “points” units.
- This keeps **NDX options** as the traded root while supplying a fast, usable underlying proxy.
- The proxy is **explicit**: logs include a `[THETA][INDEX_PROXY]` warning (once per run).
- **Invariant:** `Asset("NDX")` defaults to `stock` by design and is **not** treated as an index. Only explicit `asset_type=INDEX` triggers the proxy.

**Limitations / drift:**
- The scaling factor is a stable heuristic (ETF fees/dividend timing can cause slow drift over long horizons).
- If you need higher-fidelity calibration, add a daily factor calibration path derived from NDX options EOD (still Theta-only).

## ThetaData v3 Payload Variants (Downloader Mode)

When routed through the BotSpot Data Downloader, Theta v3 responses are not fully stable across terminal versions:
- **v2-style envelope:** `{"header":{"format":[...]}, "response":[[...], ...]}`
- **row-style:** `{"response":[{"timestamp": "...", ...}, ...]}` (no `header`)
- **nested option history:** `{"response":[{"contract": {...}, "data":[{...}, ...]}]}`

LumiBot normalizes these shapes in `lumibot/tools/thetadata_helper.py` so downstream history parsing:
- builds `DataFrame`s with explicit columns,
- produces a `datetime` index consistently, and
- avoids “NO_DATA”/472 loops caused by mis-parsed quote payloads (especially for NDX options backtests).

**Split Handling (FIXED - Nov 28, 2025)**

✅ **ThetaData split handling is now working correctly.**

The ThetaData Data Downloader returns **UNADJUSTED** prices (NOT split-adjusted like Yahoo).
The `_apply_corporate_actions_to_frame()` function applies split adjustments with idempotency protection.

**Root Cause (Fixed):**
- The function was being called 26+ times per backtest without any idempotency check
- Each call re-applied split adjustments, causing over-correction (81% CAGR vs expected 56%)

**Fix Applied:**
1. Added `_split_adjusted` column marker to track if data has been adjusted
2. Function now skips adjustment if marker is already present
3. Cache version bumped to v7 to invalidate stale data

**Test Results (After Split Fix):**
| Condition | CAGR | Worst Day | Status |
|-----------|------|-----------|--------|
| No adjustment | 7.5% | -64% | WRONG - unadjusted |
| Multiple adjustments (broken) | 81% | -95% | WRONG - over-adjusted |
| With idempotency fix | 55.07% | -18.69% | ✅ CORRECT |
| Yahoo baseline | 56% | -27% | ✅ CORRECT |

**Option Splits (ThetaData)**

ThetaData option history requires special handling around splits:

- Option chains are queried using strikes normalized to strategy inputs.
- Option OHLC and NBBO are normalized in the ThetaData data pipeline so that option series
  remain continuous across splits (matching split-adjusted underlier prices).
- **Backtesting must not apply option split events a second time** (no quantity/cost-basis adjustments
  in the broker layer when using ThetaData-normalized option series).

**Dividend Handling (ThetaData)**

LumiBot treats dividends as **cash events** in backtests.

- ThetaData returns **UNADJUSTED dividend amounts** (pre-split).
- Dividend amounts are **split-adjusted** so the per-share dividend matches the split-adjusted
  price series used in backtests (Yahoo-style share units).
- **ThetaData OHLC is NOT dividend-adjusted**. Dividend-adjusting prices *and* crediting cash dividends
  double-counts return and inflates CAGR.

**Issues Found & Fixed:**

1. **Multiple dividend application** - `_update_cash_with_dividends()` was called 3 times per day
   - Fix: Added `_dividends_applied_tracker` set in `_strategy.py` to track (date, symbol) combinations
   - Dividends now only applied once per day per asset

2. **Dividends not split-adjusted** - Raw ThetaData dividend amounts were used directly
   - Fix: `get_yesterday_dividends()` in `thetadata_backtesting_pandas.py` now fetches splits and divides dividend amounts by cumulative split factor
   - Example: $1.22 dividend from 2015 ÷ 6 (split factor) = $0.20 adjusted

**Test Results (After Dividend Fix):**
| Condition | CAGR | Best Day | Status |
|-----------|------|----------|--------|
| Dividends not adjusted | 51.71% | +24.4% | Inflated by raw dividends |
| With dividend split-adjustment | 47.92% | +18.43% | Baseline for cash-dividend model |
| Yahoo baseline | ~56% | ~30% | (Varies by window/settings) |

**REMAINING ISSUE: ThetaData Phantom Dividends**

ThetaData returns dividends on dates where Yahoo shows NONE:
- 2014-09-18: $0.41 (Yahoo: no dividend)
- 2015-07-02: $1.22 (Yahoo: no dividend)

Even after split adjustment, these phantom dividends affect results. Consider disabling ThetaData dividends entirely or cross-validating with Yahoo.

**Zero-Price Data Filtering (FIXED - Nov 28, 2025)**

ThetaData sometimes returns rows with all-zero OHLC values (e.g., Saturday 2019-06-08 for MELI). This caused `ZeroDivisionError` when strategies tried to calculate position sizes.

**Fix Applied:**
1. Zero-price filtering when loading from cache (`thetadata_helper.py` lines ~2501-2513)
2. Zero-price filtering when receiving new data (`thetadata_helper.py` lines ~2817-2829)
3. Cache is self-healing - bad data automatically filtered on load

**Filtering Logic:**
```python
# Filter rows where ALL OHLC values are zero
all_zero = (df["open"] == 0) & (df["high"] == 0) & (df["low"] == 0) & (df["close"] == 0)
df = df[~all_zero]
```

**Note:** Weekend filtering was intentionally NOT added because markets may trade on weekends in the future (crypto, futures). The issue is zero prices, not weekend dates.

### 5. Polygon (`polygon_backtesting.py` → `polygon_helper.py`)

**Flow:**
1. `PolygonDataBacktesting` inherits from `PandasData`
2. Calls `polygon_helper.get_price_data_from_polygon()` to fetch data
3. Uses local cache in `LUMIBOT_CACHE_FOLDER/polygon`
4. Handles split adjustments via `validate_cache()`

**Key Function:** `get_price_data_from_polygon()` (line 80)

## Progress Logging and Download Status Tracking

### Progress CSV Output

During backtests, LumiBot writes real-time progress to `logs/progress.csv` for frontend display.

**CSV Columns:**
| Column | Description |
|--------|-------------|
| `timestamp` | Wall-clock time of update |
| `percent` | Backtest completion percentage (0-100) |
| `elapsed` | Time elapsed since start |
| `eta` | Estimated time remaining |
| `portfolio_value` | Current portfolio value |
| `simulation_date` | Current datetime in the simulation (YYYY-MM-DD HH:MM:SS) |
| `cash` | Current cash balance |
| `total_return_pct` | Running total return percentage |
| `positions_json` | JSON array of minimal position dicts |
| `orders_json` | JSON array of minimal order dicts |
| `download_status` | JSON object tracking data download progress |

### Minimal Serialization Methods

Entity classes provide `to_minimal_dict()` methods for lightweight progress logging:

**Asset.to_minimal_dict()**
```python
# Stock:
{"symbol": "AAPL", "type": "stock"}

# Option:
{"symbol": "AAPL", "type": "option", "strike": 150.0, "exp": "2024-12-20", "right": "CALL", "mult": 100}

# Future:
{"symbol": "ES", "type": "future", "exp": "2024-12-20", "mult": 50}
```

**Position.to_minimal_dict()**
```python
{"asset": {...}, "qty": 100, "val": 15000.00, "pnl": 500.00}
```

**Order.to_minimal_dict()**
```python
{"asset": {...}, "side": "buy", "qty": 100, "type": "market", "status": "filled"}
# Limit orders add: "limit": 150.0
# Stop orders add: "stop": 140.0
```

### Download Status Tracking (ThetaData)

ThetaData downloads can occur at any point during a backtest when data is needed. The download status tracking system provides visibility into these downloads.

**Location:** `lumibot/tools/thetadata_helper.py`

**Functions:**
- `get_download_status()` - Get current download state
- `set_download_status(asset, quote_asset, data_type, timespan, current, total)` - Update status
- `finalize_download_status()` / `clear_download_status()` - Mark inactive (finalize keeps the last `current/total` visible for UI polling)

**Download Status Format:**
```python
{
    "active": True,           # Whether download is in progress
    "asset": {...},           # Minimal asset dict being downloaded
    "quote": "USD",           # Quote asset symbol
    "data_type": "ohlc",      # Data type (ohlc, trades, quotes)
    "timespan": "minute",     # Timespan (minute, day, etc.)
    "progress": 50,           # Progress percentage (0-100)
    "current": 5,             # Completed request "pieces" for THIS asset operation
    "total": 10               # Total request "pieces" for THIS asset operation
}
```

**Semantics (important):**
- `current/total` are **not** “percent of the whole backtest downloaded”.
- They represent progress for the **single asset currently being hydrated** (e.g., one stock, or one option contract identified by symbol + strike + expiration + right).
- A “piece” is whatever deterministic request plan the data source uses for that asset (e.g., per-trading-day requests for intraday history, or per-date-window requests for EOD history).

**Extending to Other Data Sources:**

To add download status tracking to other data sources (Yahoo, Polygon, etc.):

1. Import the tracking functions:
   ```python
   from lumibot.tools.thetadata_helper import (
       get_download_status, set_download_status, clear_download_status
   )
   ```

## Backtest Performance Notes (Prod)

Minute-cadence option strategies can execute **~100k+ iterations** per backtest window. In production
(`Bot Manager` + CloudWatch/stdout logging), log volume is often the dominant performance bottleneck.

### Per-iteration heartbeat logs

The StrategyExecutor “heartbeat” logs (start/end of each iteration) are extremely noisy for backtests and are now
opt-in:

- `BACKTESTING_LOG_ITERATION_HEARTBEAT=true` enables per-iteration start/end logs
- Default during backtesting: disabled

2. Call `set_download_status()` during fetch operations with current progress

3. Call `clear_download_status()` when fetch completes (success or failure)

4. The status will automatically be included in the progress CSV

**Note:** The download status functions are thread-safe (use a lock internally), so they can be called from parallel download threads.

## Caching System

### S3 Cache (`tools/backtest_cache.py`)

Used primarily by ThetaData:
- Bucket: Configured via `LUMIBOT_CACHE_S3_BUCKET`
- Version: `LUMIBOT_CACHE_S3_VERSION` (bump to invalidate)
- Mode: `LUMIBOT_CACHE_MODE` (read, write, readwrite)

**Important:** If cache has corrupted data (e.g., from before a bug fix), bump the version number.

### Local Cache

Each data source has its own local cache:
- ThetaData: Parquet files in `~/Library/Caches/lumibot/`
- Polygon: Feather files in `LUMIBOT_CACHE_FOLDER/polygon/`
- IBKR: Parquet files in `LUMIBOT_CACHE_FOLDER/ibkr/`

## Environment Variables

### Data Source Selection
```bash
BACKTESTING_DATA_SOURCE=thetadata  # Options: yahoo, thetadata, ibkr, router, polygon, etc.
                                    # Set to "none" to use code-specified class
```

### IBKR Backtesting (Client Portal REST)

IBKR backtesting uses the shared Data Downloader and is cached locally (and optionally mirrored to S3) just like ThetaData.

- Single-provider: `BACKTESTING_DATA_SOURCE=ibkr`
- Multi-provider routing (Theta for stock/option/index; IBKR for futures/crypto):
  ```bash
  export BACKTESTING_DATA_SOURCE='{"default":"thetadata","stock":"thetadata","option":"thetadata","index":"thetadata","future":"ibkr","crypto":"ibkr"}'
  ```
  - You can also route crypto to CCXT by using either:
    - `{"crypto":"ccxt"}` (auto-select exchange from existing env/credentials), or
    - a CCXT exchange id directly, e.g. `{"crypto":"coinbase"}` or `{"crypto":"kraken"}`.

#### Crypto daily bars (important semantics)

IBKR's `bar=1d` history for crypto is not a clean midnight-to-midnight 24/7 day series, and its timestamps can lag the
simulation clock used by daily-cadence strategies. To keep daily backtests stable (no “stale end of data” refresh loops),
LumiBot derives **crypto daily bars** from intraday history and aligns them to midnight day buckets in `LUMIBOT_DEFAULT_PYTZ`
(default: `America/New_York`).

**Note:** IBKR crypto history is often effectively **24/5** (weekends can be missing). For daily backtests, LumiBot
forward-fills short gaps (≤ 3 days) from the prior close so the daily clock can advance without “missing BTC day” churn.

### Backtest output artifacts (HTML/CSV)
```bash
SHOW_PLOT=True        # trades.html + trades.csv
SHOW_INDICATORS=True  # indicators.html + indicators.csv
SHOW_TEARSHEET=True   # tearsheet.html + tearsheet.csv + tearsheet_metrics.json
BACKTESTING_QUIET_LOGS=false  # useful when debugging (otherwise logs may be empty)
```

### ThetaData Configuration
```bash
THETADATA_USERNAME=xxx
THETADATA_PASSWORD=xxx
DATADOWNLOADER_BASE_URL=http://localhost:8080  # Data Downloader URL (set to your environment)
DATADOWNLOADER_API_KEY=xxx
DATADOWNLOADER_API_KEY_HEADER=X-Downloader-Key  # default header name used by downloader
DATADOWNLOADER_SKIP_LOCAL_START=true  # Don't start local ThetaTerminal
```

### S3 Cache Configuration
```bash
LUMIBOT_CACHE_BACKEND=s3
LUMIBOT_CACHE_S3_BUCKET=lumibot-cache-dev
LUMIBOT_CACHE_S3_VERSION=v5  # Bump to invalidate cache
LUMIBOT_CACHE_MODE=readwrite
```

## Important Rules

### ThetaData Rules (from AGENTS.md)

1. **NEVER run ThetaTerminal locally** - Only use the Data Downloader
2. **Use the downloader endpoint from your environment** - Set `DATADOWNLOADER_BASE_URL`
3. **Respect queue/backoff** - Handle `{"error":"queue_full"}` responses
4. **Long commands need safe-timeout** - Use `safe-timeout` wrapper

### Split Adjustment Rules

- **Yahoo**: Already split-adjusted, no action needed ✅
- **ThetaData Data Downloader**: Returns UNADJUSTED data - adjustment code applies splits ✅
  - Fixed Nov 28, 2025: Added idempotency check to prevent multiple adjustments
  - Results now match Yahoo within ~1-2%
- **Polygon**: Handles splits in `validate_cache()`

## Troubleshooting

### Backtest Results Don't Match Between Data Sources

1. Check `BACKTESTING_DATA_SOURCE` env var - it overrides code
2. Verify cache version is consistent across .env files
3. Look for impossible daily returns (e.g., -50%, +100%) indicating split issues
4. Compare raw price data for specific dates (especially around split dates)

### TQQQ Split Dates for Testing

| Date       | Ratio | Type          |
|------------|-------|---------------|
| 2017-01-12 | 2:1   | Forward split |
| 2018-05-24 | 3:1   | Forward split |
| 2021-01-21 | 2:1   | Forward split |
| 2022-01-13 | 1:2   | REVERSE split |
| 2025-11-20 | 2:1   | Forward split |

### Cache Issues

If seeing wrong prices:
1. Bump `LUMIBOT_CACHE_S3_VERSION`
2. Clear local cache: `rm -rf ~/Library/Caches/lumibot/`
3. Re-run backtest to fetch fresh data

## File Locations Summary

| Component | Location |
|-----------|----------|
| LumiBot library | `/Users/robertgrzesik/Documents/Development/lumivest_bot_server/strategies/lumibot/` |
| Strategy Library | `/Users/robertgrzesik/Documents/Development/Strategy Library/` |
| Demo strategies | `/Users/robertgrzesik/Documents/Development/Strategy Library/Demos/` |
| Log output | `/Users/robertgrzesik/Documents/Development/Strategy Library/logs/` |
| Local cache | `~/Library/Caches/lumibot/` |

## See Also

- `AGENTS.md` - Critical rules for ThetaData usage
- `CLAUDE.md` - AI assistant instructions
- `CHANGELOG.md` - Version history

## Tooling Notes (merge/debug workflows)

### GitHub CLI (`gh`)
Useful for reviewing PR conflicts/checks without opening the browser:
```bash
gh pr view 914
gh pr diff 914
gh pr checks 914
```
Avoid `gh pr checkout` because it invokes `git checkout` under the hood (banned in this workspace).

### AWS CLI (read-only)
For diagnosing downloader DNS issues (do not modify records):
```bash
aws route53 list-hosted-zones
aws route53 list-resource-record-sets --hosted-zone-id <ZONEID>
```

## Documentation Layout

- `docs/` = human/AI-authored markdown (architecture, investigations, handoffs, ops notes)
- `docsrc/` = Sphinx source for the public documentation site
- `generated-docs/` = local build output from `docsrc/` (gitignored)
- GitHub Pages should be built + deployed by GitHub Actions on pushes to `dev`
