# IBKR Futures Backtesting (REST) — US Futures (CME/CBOT/COMEX/NYMEX)

This document describes LumiBot’s Interactive Brokers **Client Portal (REST)** backtesting path for **US futures** using IBKR REST historical bars (examples: ES/MES/NQ/MNQ, GC/MGC, CL/MCL).

## Scope (Phase 3)

- **Supported:** US futures across major venues (CME/CBOT/COMEX/NYMEX) via IBKR REST historical bars (1-minute+).
- **Not in scope (current):** tick/second history guarantees and historical data outside IBKR's retention limits.
- **Backtesting + live:** exchange routing and conid resolution logic is shared between:
  - backtesting (`lumibot/tools/ibkr_helper.py`)
  - live IBKR REST (`lumibot/data_sources/interactive_brokers_rest_data.py`)
  so backtests behave as close to live as practical.

### Seconds-level roadmap (futures-first)

This doc describes the **current** IBKR REST futures backtesting path (minute+).

Seconds-level futures backtesting is an active roadmap item, but it is a separate set of problems:
- seconds for fills only (bar magnifier), vs
- true seconds strategy loops (strategy runs on seconds).

When seconds-mode work starts (or if you’re reading this while implementing it), use:
- `docs/BACKTESTING_SECOND_LEVEL_ROADMAP.md`
- `docs/investigations/bot_manager.md` (current implementation plan; futures-first; router-mode and warm-cache invariants)

## Data Path (where the bars come from)

1. LumiBot backtesting requests historical bars through `lumibot/tools/ibkr_helper.py`.
2. `ibkr_helper` calls the **Data Downloader** proxy route:
   - `GET {DATADOWNLOADER_BASE_URL}/ibkr/iserver/marketdata/history`
3. The Data Downloader forwards the request to the **Client Portal Gateway** sidecar (IBeam).

The entire stack is cache-backed:
- Local Parquet cache under `LUMIBOT_CACHE_FOLDER/ibkr/...`
- Optional S3 mirroring through the standard `LUMIBOT_CACHE_*` settings (see `docs/ENV_VARS.md`).

## Exchange Routing (automatic + per-call override)

Goal: strategy authors should not have to guess which exchange to use (CME vs CBOT vs COMEX vs NYMEX).

### Where `exchange=` lives

`exchange=` is **not** a property of the `Asset`. It is a **data retrieval / routing** choice and is supported as an optional parameter in:
- `Strategy.get_historical_prices(..., exchange=...)`
- `Strategy.get_last_price(..., exchange=...)`
- `Strategy.get_quote(..., exchange=...)`

### Automatic futures exchange resolution

When `asset_type in {"future","cont_future"}` and `exchange` is omitted, LumiBot resolves the exchange by calling IBKR’s secdef search (via the Data Downloader):
- `GET /ibkr/iserver/secdef/search?symbol=<root>&secType=FUT`

Tie-break rules (highest priority first):
1. Prefer currency `USD` (when present)
2. Prefer US venues `{CME, CBOT, COMEX, NYMEX}`
3. Otherwise use the first FUT match

If multiple distinct exchanges tie for “best”, LumiBot raises an error and requires an explicit `exchange=...`.

### Caching the resolved exchange

Resolved root→exchange mappings are cached:
- In-memory for the process
- Persisted at `LUMIBOT_CACHE_FOLDER/ibkr/futures_exchanges.json` (and mirrored to S3 if enabled)

### Compatibility fallback

`IBKR_FUTURES_EXCHANGE` is still supported as a **fallback** default when auto-resolution fails, but normal operation should not require it.

## Contract Requirements (expiration + metadata)

### Futures contracts

IBKR futures bar history requires a specific contract identifier (conid). LumiBot resolves conids and caches them under:
- `LUMIBOT_CACHE_FOLDER/ibkr/conids.json`

For deterministic acceptance and correct lookup behavior, **explicit futures** should be used:
- `Asset("MES", asset_type="future", expiration=date(2025, 12, 19))`

### Auto-expiry futures (rolling wrapper; recommended when you don't want to hardcode an expiration)

If you want “front month” behavior without hardcoding an expiration date, use an auto-expiry futures asset:
- `Asset("MES", asset_type="future", auto_expiry=Asset.AutoExpiry.FRONT_MONTH)`

Semantics:
- This is a **rolling wrapper** resolved by the data source/broker as time advances (similar to `cont_future` stitching).
- `Asset.expiration` remains `None` unless you explicitly provide `expiration=...`.
- This keeps backtests and live behavior aligned and avoids `date.today()` skew when backtesting historical periods.

### Crypto futures roots (IBKR / CME)

IBKR’s futures root symbols can differ from spot tickers, especially for CME crypto products. Examples:
- Bitcoin: `MBT` (Micro Bitcoin futures), `BRR` (Bitcoin Reference Rate futures root)
- Ether: `MET` (Micro Ether futures), `ETHUSDRR` (Ether Reference Rate futures root)

### Expired contracts (critical)

IBKR Client Portal cannot reliably discover conids for **expired** futures. LumiBot first uses the mirrored
registry. If REST does not list an expired month, it requests that root, venue, USD currency and month through
the downloader's read-only `GET /ibkr/tws/secdef/contracts` endpoint, using its existing TWS session.
The response must identify one unambiguous FUT contract with the requested identity. Wrong or ambiguous
identities fail visibly; discovery does not imply the provider still retains the requested bars.

- `ibkr/conids.json` (S3-mirrored)

See: `docs/investigations/2026-01-18_IBKR_EXPIRED_FUTURES_CONID_BACKFILL.md`.

Registry publication uses an ETag-conditioned merge. Only identities resolved by the current lookup may
replace remote entries; namespace seeds fill absent entries. Unrelated stale local values cannot replace
corrected shared IDs. Conditional conflicts reload and merge again.

A missing roll contract records `partial` in `ibkr_history_health` even when later contracts return bars.
A nonempty tail is not proof of a complete continuous lookback. Friday 17:00 through Sunday 18:00 New York
time is treated as closed, including daylight-saving transitions. Daily holidays still rely on provider/calendar
evidence. Strategy warmup must count completed bars at its simulated decision time.

Queue execution has a total deadline covering the local concurrency slot, submission, polling and retry
backoff. Normal calls allow three configured timeout windows; a finite attempt cap sets that many windows.
Resubmitting a request does not reset that deadline. Valid partial bars remain cached for subsequent retries.

Operational note:
- If `ibkr/conids.json` in the active S3 cache namespace is only a few hundred bytes (or missing keys like
  `future|GC|USD|COMEX|20250226`), `cont_future` backtests will fail for “past year” windows once contracts expire.
  Seed/promote the registry in S3 using the runbook in the investigation doc above.

### Multiplier + minTick (mandatory for correct PnL and tick rounding)

For realistic futures accounting:
- **PnL uses multiplier** (e.g., MES multiplier = 5)
- **SMART_LIMIT tick rounding** must respect the exchange tick size (e.g., MES minTick = 0.25)

LumiBot populates these via the IBKR contract info endpoint (cached):
- `GET /ibkr/iserver/contract/{conid}/info`
- cached at `LUMIBOT_CACHE_FOLDER/ibkr/future/contracts/CONID_<conid>.json`

## Intraday bars (minute/hour)

Futures backtesting uses **Trades OHLC** bars as the candle series.

By default, futures fills are intended to be **OHLC/TRADES-based**.

Optional quote-based behavior (SMART_LIMIT / buy-at-ask sell-at-bid) can be enabled by deriving bid/ask from
`Bid_Ask` + `Midpoint` history sources, but this is intentionally **disabled by default** because it multiplies request
volume and reintroduces Client Portal history flakiness:

- Enable explicitly: `LUMIBOT_IBKR_ENABLE_FUTURES_BID_ASK=1`

## “Last price” semantics (no lookahead)

For futures/continuous futures in backtests, LumiBot treats `get_last_price(dt)` as **the last completed bar’s close**
(not the current minute’s open). This avoids implicit lookahead at bar boundaries and makes parity comparisons more
stable (especially around the daily maintenance gap and weekend reopen).

Implementation detail:
- `InteractiveBrokersRESTBacktesting.get_last_price()` evaluates the series at `dt - 1µs` for `future`/`cont_future`.

### End-of-window tolerance (avoids repeated downloader retries)

IBKR history can omit the final bar(s) near a requested window boundary (commonly 1–3 bars). To keep deterministic
acceptance runs stable (and avoid hammering the downloader), LumiBot treats cache coverage within a small tolerance
as “good enough” and does not attempt to fetch beyond it.

### Closed-session gaps (weekends/holidays)

Futures windows can begin/end inside long closed intervals (weekends/holidays). LumiBot treats *fully closed* intervals
at the start/end of the requested window as “already satisfied” by the cache and does not attempt to fetch those bars.

## Daily bars (session-aligned, not midnight)

Futures “daily” in LumiBot must align to the **`us_futures`** market session, not midnight:
- many futures strategies use `self.set_market("us_futures")`
- the backtesting clock advances based on that calendar

LumiBot derives futures daily bars by aggregating intraday bars per `pandas_market_calendars.get_calendar("us_futures")`, indexing each daily bar at the **session close** timestamp.

## Continuous futures (synthetic roll schedule)

For `Asset(asset_type="cont_future")`, IBKR REST backtesting follows LumiBot’s **synthetic** roll schedule:
- resolves a sequence of explicit contracts over the requested window using `lumibot/tools/futures_roll.py`
- fetches each contract’s Trades OHLC bars and stitches them into a continuous series

This matches LumiBot’s synthetic roll semantics used across futures brokers/backtest providers.

### Conid registry growth over time (REST auto-populates)

When LumiBot calls `GET /ibkr/trsrv/futures` for a `(root, exchange)`, the response includes a list of contract months.
LumiBot bulk-ingests the entire list into `ibkr/conids.json` (expiration → conid), so the registry stays current as new
contracts list over time.

Important caveat:
- IBKR Client Portal still cannot reliably discover *very old expired* conids. Those historical gaps require a one-time
  backfill (see `docs/investigations/2026-01-18_IBKR_EXPIRED_FUTURES_CONID_BACKFILL.md`).

### S3 source-of-truth + merge-before-upload (internal)

When `LUMIBOT_CACHE_*` is configured for S3 mirroring in **READWRITE** mode, multiple concurrent backtests can update the
registry. To reduce lost updates from last-write-wins, LumiBot uploads `ibkr/conids.json` using a merge-before-upload
retry (download remote → union keys → upload → verify keys present).

### Roll-boundary stitching (one-minute overlap)

Because futures backtests use “last completed bar” semantics for `get_last_price()`, the minute immediately preceding a
roll boundary must exist in the stitched series for the **new** contract (so the previous-close lookup at the roll
timestamp is well-defined).

IBKR cont-futures stitching therefore widens each post-first segment by **1 minute on the left** and relies on stable
de-duping so the newer contract overrides overlaps deterministically.

## Verification (manual)

Quick smoke checklist (no `exchange=` specified):
1. Run a short backtest for `cont_future` roots `GC`, `MGC`, `CL`, `MCL` and confirm bars are non-empty and trades occur.
2. Confirm `LUMIBOT_CACHE_FOLDER/ibkr/futures_exchanges.json` contains entries for those roots.
3. Re-run the same window and confirm it is faster (local Parquet hits) and does not spam downloader requests.

## Deterministic Acceptance Backtests (how we keep this stable)

Acceptance backtests are deterministic and enforce a **warm S3 cache invariant**:
- runs must not submit downloader queue requests
- headline tearsheet metrics are asserted strictly

Acceptance harness:
- `tests/backtest/test_acceptance_backtests_ci.py`

IBKR extension notes:
- IBKR REST backtesting also routes through `queue_request()`, and the telemetry is recorded under
  `thetadata_queue_telemetry` in `*_settings.json`.
- For IBKR acceptance, we therefore assert:
  - `thetadata_queue_telemetry.submit_requests == 0`

See also:
- `docs/ACCEPTANCE_BACKTESTS.md`
- `docsrc/backtesting.ibkr.rst`

## Live broker alignment (Tradovate / ProjectX)

The acceptance and backtesting design target is: **match live trading behavior**.

Operationally:
- **Tradovate** is the primary live futures broker.
- **ProjectX** is expected to behave the same at the strategy semantics level.
- IBKR live trading may require explicit contracts (root + expiration); acceptance uses explicit contracts for determinism.
