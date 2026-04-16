# Changelog

## 4.4.62 - 2026-04-15

### Fixed
- **CRITICAL: Flat-price fabrication bug in backtesting fills.** `BacktestingBroker._pandas_fill_path` previously picked `df_original.iloc[-1:]` when the "bars at or after self.datetime" filter came back empty, OR `df_original.iloc[0]` when the filter caught rows far in the future. Both paths could silently price every fill at a single bar's OPEN that had nothing to do with the simulation time, producing deterministic constant prices across multi-year backtests. Observed incident: BotSpot "Alpha Picks" backtest where 72 of 84 stocks froze at the OPEN of 2026-04-10 for every fill across 2022-2024. The fix rejects future bars outside a narrow window (2 days for daily, 2 hours for hourly, 5 minutes for minute), prefers the most recent bar at or before the simulation time, and refuses to fabricate a fill when no bar exists within a reasonable window of `self.datetime`. A second-layer `max_fill_distance` sanity check (7 days for daily, 1 day for intraday) hard-rejects any fill price derived from a bar too far from the simulation clock, cancelling the offending iteration rather than producing a garbage fill. Provider-agnostic; applies to every data source that flows through `PANDAS` / `InteractiveBrokersREST` fill paths (ThetaData, IBKR, Polygon, etc.).
- IBKR history loading now fails open by returning available real bars instead of synthesizing an empty dataset when the cache refresh leaves the requested window underfilled (commit `5de1362a`, April 14).
- Interactive Brokers REST backtesting data source now prefers an already-loaded daily stock/index series for `get_last_price`/`get_quote` before triggering a separate intraday minute fetch, which avoids unnecessary VIX/USD midpoint history requests during daily-cadence backtests (commit `5de1362a`, April 14).
- `Order.to_dict()` now emits `identifier` and `avg_fill_price` consistently so downstream consumers that rely on these fields on serialized orders no longer see missing keys after backtest-time fills (commit `e3cb48fe`, April 15).
- Release the IBKR downloader fail-closed hotfix from the corrected commit so CI and PyPI ship the same behavior validated locally (merged from dev).

### Changed
- README restructured for visitor conversion: Quick Start code block, competitor comparison table (LumiBot is the only open-source Python trading library with options support), supported brokers & data sources matrix, migration guide from Backtrader (merged from dev).
- PyPI metadata overhaul: added 20 search keywords and 12 classifiers (Financial, Investment, AI, Python 3.10-3.12) after being previously empty. Updated package description for search discoverability (merged from dev).
- Author email updated to rob@botspot.trade; added project_urls for documentation, Discord, and BotSpot platform (merged from dev).

### Added
- `docs/MIGRATING_FROM_BACKTRADER.md`: concept mapping and side-by-side code examples for users switching from the now-unmaintained Backtrader library (merged from dev).
- Release readiness script now verifies that every referenced artifact file actually exists on disk (not just that `artifact_path` is non-empty), preventing stale pointer references from passing the deployment gate (commit `323b6488`, April 12).
- LumiBot matrix runner emits `lumibot_runner_state.json` checkpoint files during long runs so in-flight case state is inspectable during multi-hour acceptance runs (commit `6944e22c`, April 12).

## 4.4.61 - 2026-04-01

Release bookkeeping only. No functional code changes from 4.4.60.

## 4.4.60 - 2026-04-01

### Fixed
- Data Downloader queue client now uses a dedicated configurable connect-timeout budget instead of a hardcoded `5s`, which prevents IBKR/VIX history refreshes from failing closed on slow downloader connections.
- IBKR history loading now fails closed when a refresh leaves the requested window underfilled, so stale cached slices are no longer returned as if they were complete history.

## 4.4.58 - 2026-04-01

### Added
- `@agent_tool` decorator now auto-includes function source code in tool descriptions, giving AI agents full visibility into parameters, defaults, and implementation details without manual documentation.
- `AgentHandle` now always merges built-in tools with custom user tools (previously custom tools replaced built-ins).
- Four new canonical agent demo strategies: M2 Liquidity (FRED data), Macro Risk (Alpaca bars), Momentum Allocator (Alpaca bars + news), and News Sentiment (Alpaca news). These replace the previous stress-test examples with production-quality patterns.
- Version logged at startup (`LumiBot v{version} starting`) via `logger.info` for CloudWatch/backtest/live log visibility.
- Version included in backtest `settings.json` artifacts (`lumibot_version` field) for post-deploy verification.
- Auto-create next version branch job in release workflow to prevent team-blocking delays after a release.
- Post-deployment verification steps documented in `DEPLOYMENT.md`.

### Changed
- Improved lookahead-bias guardrails in agent system prompts: agents must now explicitly set end-date bounds on ALL temporal tool parameters, not just known ones.
- Major documentation refresh: agents quickstart, canonical demos, observability, FAQ, getting started page with agent framework introduction.

### Fixed
- `BACKTESTING_QUIET_LOGS` env var parsing was broken (comparing string to `None`); now correctly parses boolean-like strings (`true`, `1`, `yes`, `on`).
- Removed contradictory `set_console_log_level("ERROR")` call when `quiet_logs=False` in `trader.py`.
- IBKR pagination test assertions updated to match current behavior.
- `.gitignore` fix for deployment reliability.

## 4.4.57 - 2026-03-30

### Changed
- Bump `quantstats-lumi` dependency to `>=1.1.3,<1.2.0` so tearsheet consumers require the renamed `Worst 1-Month Return` row and the latest machine-readable contract.

### Fixed
- Backtest console print settings no longer get silently overwritten when `lumibot_logger` re-applies log levels during a backtest run. (PR #981 — @davidlatte)
- Tearsheet summary artifact compatibility with `quantstats-lumi` machine-readable metric contract (typed scalar values, no `%` string leakage in JSON scalar values).
- Removed the duplicate `cash_financing_rates()` strategy hook so cash financing now uses a single public interface centered on `set_cash_financing_rates(...)`.
- Backtest stats, plots, and tearsheet inputs now subtract external cashflows from returns, so deposits and withdrawals no longer distort `total_return`, CAGR, or other performance metrics.
- Backtest runners now honor caller-provided `plot_file_html` and `trades_file` paths instead of silently writing trade artifacts to the default `logs/` directory.

### Added
- Tradier stock shorting support: `sell_short` and `buy_to_cover` order sides now map correctly so short-selling equities works on Tradier. (PR #976 — @brettelliot)
- AI trading agent framework: `self.agents.create(...)` inside strategies with DuckDB query tools, agentic backtesting with replay cache, and external MCP server mounting. New modules under `lumibot/components/agents/`.
- End-to-end tearsheet custom-metrics proof coverage for real backtest runs that generate both `tearsheet.html` and `tearsheet_metrics.json`.
- Backtest cash-accounting coverage for `adjust_cash`, `deposit_cash`, `withdraw_cash`, and strategy-managed financing-rate updates.
- Normalized `cash_events` live payload support in LumiBot for Alpaca and Tradier, including stable event IDs, retry-safe pending emission, and bounded payload serialization.
- Period-delta cash columns in `stats.csv` (`cash_*_period`) for manual inspection of deposits, withdrawals, financing accruals, and cashflow-adjusted return math.
- Cash-event rows in `trades.csv` / parquet and cash-event markers in `trades.html`, including deposits, withdrawals, and financing credits/debits.

### Docs
- Expanded public documentation for `tearsheet_custom_metrics(...)`, including parameter structure, full examples, literal-scalar unit behavior, and release-order guidance for QuantStats/LumiBot metric changes.
- Added public documentation for strategy cash accounting, financing lifecycle usage, broker cash-event normalization, and broker-specific limitations for Alpaca and Tradier.

## 4.4.55 - 2026-03-15

### Added
- `BACKTESTING_PARAMETERS` environment variable support for parameter injection in backtest runs.
- Machine-readable `*_tearsheet_metrics.json` artifacts (summary-first) with placeholder output on insufficient/degenerate returns.
- New strategy lifecycle hook `tearsheet_custom_metrics(...)` for appending custom metrics to tearsheet HTML and JSON artifacts.
- Regression coverage for multi-timeframe day-timestep stock lookup and tearsheet metrics/custom-hook passthrough.

### Changed
- Backtest analysis and trader APIs now accept `tearsheet_metrics_file`; default output filename is `*_tearsheet_metrics.json`.
- QuantStats `metrics_json` generation now runs in `summary_only` mode and forwards custom metrics to both HTML and JSON outputs.
- Documentation updates for tearsheet metrics/lifecycle hooks and TradingFee guidance (`per_contract_fee` usage).

### Fixed
- Day-timestep asset lookup regression for multi-timeframe stock/index backtests (including minute->day fallback paths where appropriate).
- IBKR stale no-data cache reuse now forces refresh when requested windows extend beyond cached coverage.
- ProjectX order processing race-condition and tracking hardening merged from `dev`.

Deploy marker: `15e8e268` ("deploy 4.4.55")

## 4.4.54 - 2026-03-08

### Added
- `TradingFee` now supports `per_contract_fee` for broker-style option commissions charged per contract.
- Regression tests for `per_contract_fee` initialization and trade-cost calculations in backtesting.

### Changed
- `TradingFee` fee fields now coerce through `Decimal(str(...))` for stable decimal handling across float inputs.

### Fixed
- Backtesting trade-cost calculations now apply `per_contract_fee * quantity` for taker and maker fee paths (`market`, `stop`, `limit`, `stop_limit`, `smart_limit`).

## 4.4.53 - 2026-03-06

### Added
- Regression tests for daily-cadence datasource seeding in `StrategyExecutor`, routed `1D` timestep normalization, put-delta normalization/model-path strike selection, and IBKR equity corporate-action cache reuse.
- Regression tests for IBKR paged-history retention when later pages are empty, plus option valuation fallback coverage for off-session stale mark scenarios.

### Changed
- Daily-cadence backtests now seed datasource cadence to `day` during strategy initialization to avoid first-lookup minute prefetch blowups.
- `Strategy.get_last_price()` now consistently prefers daily bars for stock/index assets in daily backtest cadence, including routed IBKR stock/index paths.
- Routed backtesting now treats day-like timestep aliases (`1D`, `1day`, etc.) as daily cadence for non-Theta last-price/quote reads.
- ThetaData daily option fetches now prefetch forward in bounded chunks (capped by expiration/end) to reduce repeated downloader round-trips during long runs.
- Option helper strike selection now normalizes absolute delta inputs by option side and uses a fast model-based strike pick for Theta daily option backtests.
- IBKR equity corporate-action enrichment now uses Yahoo history with coverage hints (`last_needed_datetime`) and date-bucket cache keys for stable reuse.
- Backtest artifact export now always writes CSV/parquet outputs for trades/stats/indicators/trade-events regardless of `show_plot` mode.

### Fixed
- Guarded option MTM valuation against off-session stale marks that could cause transient portfolio-value drops in backtests.
- Fixed IBKR history pagination to preserve already-fetched chunks when a subsequent page returns empty.
- Refreshed acceptance baseline metrics for `aapl_deep_dip_calls` and `leaps_alpha_picks_short` to match current deterministic CI outputs.
- Updated `test_classic_60_40` drift-rebalancer expectations to the corrected daily-cadence fill quantities.

## 4.4.52 - 2026-03-03

### Added
- Regression tests for Yahoo corporate-actions helpers (`get_symbol_actions`, `get_symbols_actions`) and IBKR daily equity action enrichment.
- Regression test for routed IBKR daily stock prefetch to guarantee full lookback warmup coverage.

### Changed
- Production-readiness harness (`scripts/ibkr_theta_prod_readiness.py`) now defaults SPX stress windows to 3 months (`2025-01-01` through `2025-03-31`) with a longer timeout.
- Prod-like runner (`scripts/run_backtest_prodlike.py`) now supports `--perf-mode` for cleaner runtime benchmarking without plot/indicator/progress noise.
- Routed IBKR daily stock/index prefetch now uses the computed bar lookback window (`start_datetime`) instead of a short calendar cap from backtest start.
- Acceptance performance history records were refreshed for ongoing regression tracking.
- Deployment runbook now documents local-timeout fallback and explicit review of local-only commit ranges before release.

### Fixed
- Yahoo helper typo in corporate-actions paths (`get_symbol_actions` / `get_symbols_actions`) that prevented IBKR equity split/dividend enrichment from loading actions.
- Acceptance gate hardening: apply a bounded, case-scoped tolerance override for `ibkr_crypto_acceptance_btc_usd` metric jitter (CI/provider-data drift) to reduce false negatives.
- Router benchmark stats now prefer routed datasource bars for stock benchmarks and only fall back to Yahoo on router fetch failure (removes flaky Yahoo-first behavior in CI).

## 4.4.51 - 2026-02-26

### Added
- Option lifecycle event support in backtesting for option expiration outcomes: `assigned`, `exercised`, and `expired` (in addition to `cash_settled`).
- Regression coverage for equity/ETF physical settlement and index cash settlement paths at expiration.
- Opt-in early-assignment heuristic model for short ITM, physically-settled options (`strategy.parameters`: `option_early_assignment_enabled`, `option_early_assignment_max_dte_days`, `option_early_assignment_max_extrinsic`).

### Changed
- Options expiration behavior now follows broker-style settlement defaults:
  - Equity/ETF options settle physically at expiration (short ITM -> assignment, long ITM -> exercise when account constraints allow).
  - Index options settle to cash at intrinsic value.
- Trade artifacts now preserve option-expiration lifecycle statuses in `trades.csv` / `trades.parquet` and `trade_events` exports so downstream consumers can render assignment/exercise/cash-settlement explicitly.

### Fixed
- ThetaData daily options MTM: prefer snapshot quote marks over stale day marks, and allow forward-fill when snapshot data is unavailable.
- ThetaData backtesting: keep intraday index minute/hour fetch bounds aligned to the simulation timestamp instead of forcing full-window end coverage.
- Long ITM equity option expirations now avoid unrealistic forced delivery when account constraints are not met; these contracts expire unexercised in backtests.
- Acceptance baselines: refresh `aapl_deep_dip_calls` and `leaps_alpha_picks_short` metrics to match current option settlement behavior.

## 4.4.50 - 2026-02-19

### Changed
- Indicators HTML: improve subplot scaling so indicator panels render with sane proportions across mixed plots.
- Indicators export: make HTML export non-fatal so backtests still complete if HTML rendering fails.

### Fixed
- Acceptance baselines: refresh 0DTE backdoor baseline metrics and timing metadata to match current provider data revisions.
- Acceptance CI: allow a bounded queue-fill threshold for `spx_short_straddle_repro` while keeping strict queue-free checks for other ThetaData acceptance cases.

## 4.4.49 - 2026-02-10
### Added
- Backtesting artifacts: add `LUMIBOT_BACKTEST_PARQUET_MODE` with `required` contract mode (fail-fast on parquet export failures) and structured parquet export logs (rows/cols/bytes/duration, coerced columns).

### Changed
- Indicators: always emit `*_indicators.csv` + `*_indicators.parquet`, even when a strategy produced no markers/lines/OHLC (empty indicators = valid artifact).
- Trade events: always emit `*_trade_events.csv` + `*_trade_events.parquet` (empty events = valid artifact).

### Fixed
- Stats: stop embedding raw `Asset` objects in the `positions` stats snapshot; sanitize object-ish stats columns before parquet export to prevent `Conversion failed for column positions with type object`.

## 4.4.48 - 2026-02-10

### Added
- Backtesting artifacts: emit Parquet siblings for `*_indicators.csv`, `*_trades.csv`, `*_stats.csv`, and `*_trade_events.csv` (zstd + PyArrow). CSV remains the compatibility layer.

### Changed
- Tradier: support OAuth payload + access token refresh; add runtime notes for the refresh flow.
- Tests: mark DataBento backtest coverage as `apitest` so the default CI suite stays deterministic without vendor credentials.
- Docs: clarify auto-expiry futures behavior and IBKR crypto roots.

### Fixed
- Data: handle `timeshift=None` in Data bars.
- Futures (auto-expiry): make selection roll-aware and harden IBKR conid negative cache behavior.

## 4.4.47 - 2026-02-07
### Added
- Backtesting: support `BACKTESTING_BUDGET` environment override for strategy backtest cash/budget.

### Changed
- Downloader: rename the downloader queue client module from `thetadata_queue_client` to `data_downloader_queue_client` (provider-agnostic naming).

### Fixed
- IBKR: parse seconds-style timesteps (e.g. `20S`) for history requests where supported.
- IBKR crypto futures: harden continuous futures expiration selection in backtesting.
- Logging: avoid stale env-driven logger levels by re-applying Lumibot logging configuration on each `get_logger()` call (reduces test flakiness when env vars toggle).

## 4.4.46 - 2026-02-04

### Fixed
- Backtesting routing: when `futures`/`future` is configured, default `cont_future` to the same provider so `AssetType.CONT_FUTURE` does not fall back to `default`.
- Backtesting performance: default per-asset fetch throttling (`sleep_time`) to 0 for backtesting data sources (keeps live default throttling unchanged).
- Backtesting performance: bound `get_trading_days()` calendar initialization to the backtest date window to avoid building decades of unused schedules.

## 4.4.45 - 2026-01-30

### Fixed
- Release: include `lumibot/resources/ThetaTerminal.jar` in the PyPI wheel/sdist (required by BotManager and ThetaData setup).
- Backtesting: `BacktestingBroker.process_pending_orders()` now accepts both iterable order buckets and legacy buckets that expose `get_list()`.

## 4.4.44 - 2026-01-30

### Added
- Charting: `Strategy.add_ohlc()` and `Strategy.get_ohlc_df()` for exporting OHLC (candlestick) indicator series.
- Indicators: `plot_indicators()` now supports OHLC series in `*_indicators.html` and exports `type=ohlc` rows in `*_indicators.csv`.
- Docs: add seconds-level backtesting guidance and expand seconds-mode notes.

### Changed
- Charting: `Strategy.add_line()` now returns the appended dict (consistent with other chart helpers).
- Docs: recommend `add_ohlc()` for plotting price bars and `add_line()` for single-value indicators.

### Fixed
- Release: correct PyPI packaging so `lumibot==4.4.44` includes `Strategy.add_ohlc()` (the published `4.4.43` wheel was missing it).

## 4.4.43 - 2026-01-30

**NOTE:** The PyPI `lumibot==4.4.43` artifact was published from an older commit and does **not** include the changes
listed below. Upgrade to `lumibot==4.4.44`.

### Added
- Charting: `Strategy.add_ohlc()` and `Strategy.get_ohlc_df()` for exporting OHLC (candlestick) indicator series.
- Indicators: `plot_indicators()` now supports OHLC series in `*_indicators.html` and exports `type=ohlc` rows in `*_indicators.csv`.
- Docs: add seconds-level backtesting guidance and expand seconds-mode notes.

### Changed
- Charting: `Strategy.add_line()` now returns the appended dict (consistent with other chart helpers).
- Docs: recommend `add_ohlc()` for plotting price bars and `add_line()` for single-value indicators.

### Fixed
- Backtest executor safe-sleep overload now applies only in backtests and uses real sleep outside backtesting.

## 4.4.42 - 2026-01-30

**NOTE:** The PyPI `lumibot==4.4.42` artifact was published from an older commit and does **not** include the changes
listed below. Upgrade to `lumibot==4.4.43`.

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
