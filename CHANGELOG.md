# Changelog

## 4.4.42 - Unreleased

### Added
- Charting: `Strategy.add_ohlc()` and `Strategy.get_ohlc_df()` for exporting OHLC (candlestick) indicator series.
- Indicators: `plot_indicators()` now supports OHLC series in `*_indicators.html` and exports `type=ohlc` rows in `*_indicators.csv`.
- Docs: add seconds-level backtesting guidance and expand seconds-mode notes.

### Changed
- Charting: `Strategy.add_line()` now returns the appended dict (consistent with other chart helpers).
- Docs: recommend `add_ohlc()` for plotting price bars and `add_line()` for single-value indicators.

### Fixed
- Backtest executor safe-sleep overload now applies only in backtests and uses real sleep outside backtesting.

## 4.4.41 - 2026-01-28

### Added
- Tests: add regression coverage for futures calendar spreads (same root symbol, different expirations) to prevent margin/PnL ledger collisions.
- Docs: add investigation notes for ThetaData stale-loop behavior and futures “ghost PnL” equity spikes.

### Changed
- Backtesting helpers: cache trading calendar schedules by year and slice to the requested window to reduce repeated schedule computations.
- ThetaData: avoid eager debug string building in hot paths unless debug logging is enabled.

### Fixed
- ThetaData backtesting: normalize legacy/externally-warmed `prefetch_complete` metadata before cache validation to prevent per-bar STALE/REFRESH thrash.
- ThetaData backtesting (day): treat `tail_missing_permanent=True` as satisfying end-coverage validation to prevent per-bar STALE→REFRESH loops on warm caches.
- Backtesting futures: include expiration in futures margin/PnL ledger keys so calendar spreads (same root, different expiries) don't incorrectly net margin/realized PnL, preventing “ghost PnL” equity spikes.

## 4.4.40 - 2026-01-27

### Added
- ThetaData backtesting: coverage-based `prefetch_complete` computation + tests to prevent per-bar STALE/REFRESH thrash when cached datasets are incomplete.

### Changed
- Yahoo helper: when S3 backtest cache is enabled, hydrate cached pickles before falling back to live Yahoo fetches; upload pickles to the cache on write.

### Fixed
- ThetaData EOD: enforce the provider's 365-day window limit per request and keep progress tracking consistent with chunked downloads.

## 4.4.39 - 2026-01-27

### Added

### Changed

### Fixed
- Backtesting router (IBKR futures/cont_future/crypto): prefetch full backtest window once per series and slice from memory to avoid per-iteration history fetches (major warm-cache speedup).
- Indicators: prevent `plot_indicators()` hovertext generation from crashing when `detail_text` is missing/NaN/NA (e.g., mixed indicator points with and without `detail_text`).

## 4.4.38 - 2026-01-26

### Added
- IBKR futures: automatic exchange resolution for futures and continuous futures (via downloader secdef search) with persisted root→exchange cache.
- IBKR futures: regression/unit coverage for exchange routing, per-call exchange overrides, and conid registry bulk updates.

### Changed
- Backtesting router: accept `futures`/`cont_futures` route-key aliases for convenience (maps to `future`/`cont_future`).

### Fixed
- IBKR futures: honor call-time `exchange=` overrides consistently for `get_historical_prices`, `get_last_price`, and `get_quote` (live + backtesting), and include exchange in cache keys to avoid cross-exchange contamination.
- IBKR futures conid registry: bulk-ingest `trsrv/futures` responses and harden S3 persistence with merge-before-upload retry to avoid lost updates under concurrent backtests.
- Continuous futures: add roll rules for COMEX micro gold (`MGC`) and NYMEX crude oil (`CL`/`MCL`) and fix monthly roll selection to avoid hanging on already-rolled contract months.

## 4.4.37 - 2026-01-24

Deploy marker: `174875a8` ("chore: start 4.4.37")

### Added

### Changed

### Fixed
- Backtesting: support `timestep="hour"` in pandas-backed history requests (`Data.get_bars()`), used by routed backtesting (e.g., IBKR futures/crypto).
- ThetaData backtesting: proxy missing NDX underlying/index bars/quotes via scaled `QQQ` so NDX options strategies have a usable underlying series.
- ThetaData (downloader): normalize v3 row-style and nested option-history payloads so option quotes/chains parse correctly and caches hydrate instead of looping.
- ThetaData: stop incorrectly scaling legitimate high strikes (e.g., NDX ~ 18,000) during chain-building; only de-scale clearly thousandths-encoded payloads.
- Backtesting progress: fix progress-bar throttling keying for non-terminal sinks (prevents intermittent missing output under test runners/log capture).
- Backtesting stats: fix `cagr()`/`volatility()` crash during end-of-run stats generation when returns index uses non-nanosecond datetime dtypes (e.g., `datetime64[us]`/`datetime64[s]`).

## 4.4.36 - 2026-01-24

### Changed
- IBKR futures backtesting: accelerate intraday resampling paths to avoid repeated per-iteration recomputation for timesteps like 5minute/15minute/30minute.

### Fixed
- ThetaData EOD: treat all-zero OHLC rows as missing placeholders to prevent one-day portfolio valuation cliffs.

## 4.4.35 - 2026-01-19

### Changed
- IBKR futures backtesting: cut downloader roundtrips by caching history windows across iterations and preferring native bar sizes (e.g., 15-min) when available.

## 4.4.34 - 2026-01-19

### Added
- IBKR futures: add acceptance backtest strategy covering market/limit/stop/stop-limit/trailing/smart-limit and OCO/OTO/bracket semantics.
- IBKR futures: add parity/apitest helpers + scripts to compare IBKR runs against stored DataBento artifact baselines.
- Docs: add IBKR futures backtesting notes and DataBento parity guidance.

### Changed
- IBKR futures backtesting: interpret `get_last_price(dt)` as the last completed bar close (avoid lookahead bias).
- Continuous futures (IBKR): stitch rolled segments with a 1-minute overlap and deterministic de-duplication.
- US futures gap handling (IBKR): replace flaky calendar logic with a simple rule-based “closed interval” detector to reduce repeated downloader fetches.

## 4.4.33 - 2026-01-12

### Fixed
- SMART_LIMIT (live): avoid scanning full tracked order history in the background loop by using the broker’s active-order fast path, preventing high RSS growth in accounts with large historical order lists.
- Backtesting (router): make dataset lookup timestep-aware so minute requests don’t accidentally resolve to daily Data objects, and routed crypto assets passed as `(base, quote)` work reliably.
- Backtesting (router): refactor multi-provider routing to a provider registry + adapters (no hard-coded branching), add `alpaca`/`ccxt` support, and allow CCXT exchange-id aliases like `coinbase`/`kraken` (case/sep-insensitive).
- IBKR (crypto): normalize daily timestep handling (`day`/`1d`/`1day`) so crypto daily bars consistently use the derived-daily path.
- ThetaData: prevent acceptance backtests from hitting the downloader queue by enforcing CI-only warm-cache guardrails consistently (local runs behave like GitHub CI).
- ThetaData: treat **session close** as “complete coverage” for index minute OHLC to avoid perpetual STALE→REFRESH loops when backtest end dates are represented as midnight.
- Backtest cache (S3): speed up warm-cache hydration by streaming small objects via `get_object` instead of `download_file` transfer manager overhead.

## 4.4.32 - 2026-01-10

### Added
- Runtime telemetry: lightweight memory/health JSON lines (`LUMIBOT_TELEMETRY ...`) for diagnosing OOMs in long-running live workers.
- Broker API smoke apitests: basic Alpaca and Tradier connectivity + order lifecycle checks (paper/live as available).

### Fixed
- Live (Tradier): treat `submitted/open/new` as equivalent to reduce repeated NEW events under polling; bound live trade-event history to avoid unbounded memory growth in long-running workers.
- Live (Tradier): avoid heavy DataFrame copy chains when cleaning orders; skip ingesting large historical *closed* order lists on the first poll to prevent startup memory spikes in accounts with long histories.

## 4.4.31 - 2026-01-09

Deploy marker: `d5c6b730` ("deploy 4.4.31")

### Added
- SMART_LIMIT: live matrix apitests + runner scripts; expanded unit coverage for edge cases.
- Investigations/docs: production endpoint breakdown notes and an expanded backtesting performance playbook.
- ThetaData: per-asset download progress reporting for option-chain strike scans (exposed via `download_status`).

### Changed
- Acceptance backtests now run in CI (no longer marked `apitest`); baselines were refreshed for LEAPS + MELI; CI caps were raised for long full-year strategies due to runner variability.
- CI policy: use pytest markers (not env vars) for opt-in/slow tests; some slow ThetaData backtest tests were made opt-in, then re-enabled once bounded.
- Backtests under pytest no longer auto-open HTML artifacts (plots/indicators/tearsheets) in a browser.
- Strategy collaboration workflow: clarified “shared version branch” conventions.

### Fixed
- ThetaData: reduced option-chain fanout and improved warm-cache parity (reuse chain cache under constraints; prefetch strikes only for head+tail expirations when unconstrained; bounded intraday chain defaults).
- ThetaData: improved intraday cache coverage and corrected daily option MTM behavior.
- Polygon: reduced split-cache rate limit thrash.
- SMART_LIMIT: hardened behavior for quote/stream failures.
- Backtesting progress: improved per-asset `download_status` for clearer “what is downloading” diagnostics.

### Removed
- ⚠️ Removed ThetaData chain default-horizon env vars (`THETADATA_CHAIN_DEFAULT_MAX_DAYS_OUT*`). Chain default horizons are now fixed and covered by tests.
- Removed the short-lived `LUMIBOT_DISABLE_UI` env var (use `SHOW_PLOT/SHOW_INDICATORS/SHOW_TEARSHEET` + pytest non-interactive behavior instead).

## 4.4.30 - 2026-01-06

Version bump marker: `76b31467` ("Docs/tests: normalize artifacts + bump version")

### Added
- Backtesting performance playbook and production/local parity notes.
- `LUMIBOT_DISABLE_DOTENV` to disable recursive `.env` scanning in prod-like runs.

### Fixed
- ThetaData: filtered intraday parquet loads to reduce memory footprint; daily option MTM fixes.

## 4.4.29 - 2026-01-06

Deploy marker: `b8c6a839` ("deploy 4.4.29")

### Fixed
- Prevent production backtests from OOM-like hard exits (`ERROR_CODE_CRASH`) when refreshing multi-year intraday ThetaData caches by avoiding deep copies during cache load/write and trimming non-option intraday frames in-memory.

## 4.4.28 - 2026-01-05

### Added
- Production backtest runner script (`scripts/run_backtest_prod.py`) plus investigation docs for NVDA/SPX accuracy, parity, and startup latency.

### Fixed
- ThetaData missing-day detection for intraday caches across UTC midnights (prevents “every other trading day missing” forward-fill storms).
- Backtesting: improved intraday fills and cache end handling; deterministic drift ordering for rebalances.

## 4.4.27 - 2026-01-05

### Fixed
- Reduced peak memory usage for ThetaData backtests and tear sheet generation to avoid OOM crashes in production.

## 4.4.26 - 2026-01-05

### Changed
- ThetaData: cache snapshot quotes per session and fetch full-session option quote snapshots to reduce downloader fanout.

### Fixed
- Clamp future backtest end dates instead of failing.

## 4.4.25 - 2026-01-04

Deploy marker: `b7f83088` ("Deploy 4.4.25")

### Added
- Public documentation page for environment variables (`docsrc/environment_variables.rst`) plus engineering notes (`docs/ENV_VARS.md`).
- Backtest audit telemetry can be preserved in a separate `*_trade_events.csv` artifact (see `LUMIBOT_BACKTEST_AUDIT`).
- Investigation docs for ThetaData corporate actions and performance.

### Changed
- ThetaData option chain defaults are now bounded to reduce cold-cache request fanout (configurable via `THETADATA_CHAIN_DEFAULT_MAX_DAYS_OUT*`).

### Fixed
- OptionsHelper delta-to-strike selection fast path to prevent per-strike quote storms (SPX Copy2/Copy3 slowness).
- Prevent backtest tear sheet generation from crashing on degenerate/flat returns (NVDA end-of-run failures).
- Reduce ThetaData corporate action request thrash via memoization/negative caching.
- Normalize ThetaData intraday bars for corporate actions in backtests so option strikes and underlying prices stay in the same split-adjusted space (NVDA split issues).
- Improve ThetaData snapshot quote selection near the session open to avoid missing NBBO due to end-of-minute timestamps.

## 4.3.6 - 2024-11-16

- Fixed ThetaData EOD corrections by fetching a real 09:30–09:31 minute window for each trading day, preventing zero-length requests and the resulting terminal hangs.
- Logged the active downloader base URL whenever remote mode is enabled to make it obvious in backtest logs which data path is being used.
- Added regression tests covering the custom session window override plus the fallback path when Theta rejects an invalid minute range.
