# Environment Variables (Engineering Notes)

This page documents environment variables used by LumiBot, with an emphasis on **backtesting** and **ThetaData / downloader / caching** behavior.

**Public docs (source of truth):** the Sphinx page at `docsrc/environment_variables.rst` must be updated whenever env var behavior changes.

## Rules

- **Never commit secrets.** Document variable *names*, accepted values, and semantics—never real API keys, tokens, passwords, or AWS secrets.
- **Env var changes require docs changes.** If you add/change an env var, update:
  - `docsrc/environment_variables.rst` (public docs), and
  - this file (engineering notes) when it helps contributors.

## Backtesting selection + dates

### `LUMIBOT_DISABLE_DOTENV`
- Purpose: Disable recursive `.env` discovery (`os.walk`) at startup.
- Values: truthy enables (`1`, `true`, `yes`); unset/`0` disables.
- Default: disabled.
- Why it matters:
  - Recursive `.env` scanning can add startup latency and can accidentally load the wrong `.env` when running in a directory with nested repos.
  - In production/BotManager backtests we rely on injected environment variables, so `.env` discovery should be off.
- Where: `lumibot/credentials.py`

### `IS_BACKTESTING`
- Purpose: Signals backtesting mode for certain code paths.
- Values: `True` / `False` (string).

### `BACKTESTING_START` / `BACKTESTING_END`
- Purpose: Default date range used by `Strategy.run_backtest()` / `Strategy.backtest()` when dates are not passed in code.
- Format: `YYYY-MM-DD`

### `BACKTESTING_BUDGET`
- Purpose: Override the starting cash used for backtests (initial portfolio cash).
- Format: Positive number. Accepted examples: `500`, `5000`, `5k`, `1_000_000`, `$10,000`.
- Notes:
  - When set, this value is preferred over any `budget=` passed in strategy code, so it can be controlled per-run via injected environment variables.
  - Default (when unset and no code budget is provided): `100000`.

### `BACKTESTING_DATA_SOURCE`
- Purpose: Selects the backtesting datasource **even if code passes an explicit `datasource_class`**.
- Values (case-insensitive):
  - `thetadata`, `yahoo`, `polygon`, `alpaca`, `ccxt`, `databento`
  - `ibkr` / `interactivebrokersrest` / `interactive_brokers_rest` (IBKR Client Portal REST via Data Downloader)
  - `router` (multi-provider routing; defaults to Theta for stock/option/index and IBKR for futures/crypto)
  - JSON mapping (multi-provider routing by asset type), e.g.:
    - `{"default":"thetadata","stock":"thetadata","option":"thetadata","index":"thetadata","future":"ibkr","crypto":"ibkr"}`
    - Provider values are case/whitespace/_/- insensitive. Supported values include:
      - `thetadata`, `ibkr`, `polygon`, `alpaca`
      - `ccxt` (auto-select exchange from existing env/credentials)
      - any CCXT exchange id (e.g., `coinbase`, `kraken`, `binance`, `kucoin`) to route crypto to that exchange
  - `none` to disable env override and rely on code.
- Where: `lumibot/strategies/_strategy.py` datasource selection logic.

## Testing / CI guardrails (engineering-only)

### `LUMIBOT_ACCEPTANCE_TRIPWIRE`
- Purpose: **Acceptance backtests only** — when truthy, a Python startup hook (`tests/backtest/acceptance_tripwire/usercustomize.py`)
  aborts the subprocess the moment it attempts to call the remote Data Downloader.
- Values: truthy enables (`1`, `true`, `yes`); unset/`0` disables.
- Default: disabled outside the acceptance harness.
- Notes:
  - This is intentionally test-only behavior and must not change production LumiBot semantics.
  - CI uses this to enforce the “warm S3 cache invariant” for canonical acceptance windows.
  - Exit behavior: tripwire prints `[ACCEPTANCE][TRIPWIRE] …` and hard-exits the subprocess with code `86`.

## Live scheduled execution (BotSpot/BotManager)

- `LUMIBOT_SCHEDULED_EXECUTION`: internal BotManager flag. Truthy values (`1`, `true`, `yes`, `y`, `on`) make `Strategy.run_live()` run one live iteration and exit.
- `LUMIBOT_SCHEDULED_STATE_BACKEND`: external state backend prepared by BotManager: `s3`, `dynamodb`, or `none`. `none` disables scheduled `self.vars` file load/save.
- `LUMIBOT_SCHEDULED_STATE_FILE`: local JSON file managed by BotManager/bootstrap code to restore and persist `self.vars` for one scheduled live run. State is restored before scheduled lifecycle hooks.

## Backtest output + UX flags

### `SHOW_PLOT`, `SHOW_INDICATORS`, `SHOW_TEARSHEET`
- Purpose: Enables/disables artifact generation.
- Values: `True` / `False` (string).

### `LUMIBOT_BACKTEST_PARQUET_MODE`
- Purpose: Controls parquet export semantics for backtest artifacts (indicators/trades/stats/trade events).
- Values:
  - `best_effort` (default): parquet failures log warnings; CSV remains the compatibility layer.
  - `required`: parquet export failures raise and should fail the backtest (artifact contract mode).
- Notes:
  - This is intended for BotManager/BotSpot backtests where downstream tooling depends on Parquet for performance.
  - BotManager should set `LUMIBOT_BACKTEST_PARQUET_MODE=required` for production backtests.
- Where:
  - Mode parsing + sanitizers: `lumibot/tools/parquet_utils.py`
  - Stats parquet: `lumibot/strategies/_strategy.py`
  - Indicators/trades parquet: `lumibot/tools/indicators.py`
  - Trade events parquet: `lumibot/brokers/broker.py`

### `BACKTESTING_QUIET_LOGS`
- Purpose: Reduce log noise during backtests.
- Values: `true` / `false` (string).

### `BACKTESTING_SHOW_PROGRESS_BAR`
- Purpose: Enable progress bar updates.
- Values: `true` / `false` (string).

## Backtest progress file (BotSpot/BotManager UI)

### `LOG_BACKTEST_PROGRESS_TO_FILE`
- Purpose: When truthy, write `logs/progress.csv` during backtests so BotManager/BotSpot can show live progress.
- Values: truthy enables (`1`, `true`, `yes`); unset/`0` disables.
- Notes:
  - On startup, LumiBot writes an initial `progress.csv` row immediately to reduce “time-to-first-progress” latency for short backtests.
  - In BotManager, a background thread watches `/app/logs/*progress.csv` and uploads the most recent row to DynamoDB.
- Where: `lumibot/data_sources/data_source_backtesting.py`

### `BACKTESTING_PROGRESS_HEARTBEAT`
- Purpose: Enable periodic `progress.csv` updates while a ThetaData download is active (prevents the UI appearing stuck when simulation datetime is not advancing).
- Values: `true` / `false` (string).
- Default: enabled (`true`).
- Where: `lumibot/data_sources/data_source_backtesting.py`

### `BACKTESTING_PROGRESS_HEARTBEAT_SECONDS`
- Purpose: Heartbeat interval (seconds) for writing `progress.csv` while downloading.
- Values: float seconds (string).
- Default: `2.0`
- Where: `lumibot/data_sources/data_source_backtesting.py`

## Trade audit telemetry (NVDA/SPX accuracy audits)

### `LUMIBOT_BACKTEST_AUDIT`
- Purpose: Emit **per-fill audit telemetry** into the trades/event CSV as `audit.*` columns.
- Values: `1` enables (anything truthy); unset/`0` disables.
- Output:
  - `*_trade_events.csv` (full trade-event export) contains additional `audit.*` columns.
  - Includes quote/bid/ask snapshots (asset + underlying for options), bar OHLC, SMART_LIMIT inputs, and multileg linkage.
- Where:
  - Audit collection: `lumibot/backtesting/backtesting_broker.py`
  - Audit column emission: `lumibot/brokers/broker.py`
  - Trade-event file routing: `lumibot/strategies/_strategy.py` (exports `*_trade_events.csv`; the plotter writes a simplified `*_trades.csv` for UI/quick review).

## Profiling (parity + performance investigations)

### `BACKTESTING_PROFILE`
- Purpose: Enable profiling during backtests to attribute time (S3 IO vs compute vs artifacts).
- Values:
  - `yappi` (supported)
- Output: produces a `*_profile_yappi.csv` artifact alongside other backtest artifacts.
- Related tooling: `scripts/analyze_yappi_csv.py`

### `LUMIBOT_CACHE_MISS_DEBUG`
- Purpose: Opt-in diagnostic logging for IBKR history cache misses and every real network fetch. Emits `[CACHE_MISS]` (why the cache miss fired) and `[FETCH]` (with a short Python traceback of the caller) at WARNING level.
- Values: truthy enables (`1`, `true`); unset/`0` disables.
- Default: disabled (zero runtime cost when unset — gated before any work).
- When to use: diagnosing unexpected IBKR roundtrips in warm-cache backtests. Drove the root-cause find that produced the 2026-04-16 MES 3.89x speedup (IBKR returning empty 1-minute CONT_FUTURE history at 7s/chunk on every pass).
- Where: `lumibot/tools/ibkr_helper.py`

## Remote downloader (ThetaData via shared service) — internal/proprietary

This section describes the internal **Data Downloader** service used by LumiWealth/BotSpot deployments.
Open-source users typically should not set these variables.

Selection rule (ThetaData):
- If `DATADOWNLOADER_BASE_URL` is set, LumiBot routes ThetaData through the downloader queue and **must not** manage any
  local ThetaTerminal process (single-session constraint).
- Otherwise, LumiBot auto-manages a local ThetaTerminal and talks to it directly on `THETADATA_BASE_URL` / `127.0.0.1:25503`.

### `DATADOWNLOADER_BASE_URL`
- Purpose: Points LumiBot at the remote downloader service.
- Example (local): `http://localhost:8080`
- Example (remote): `https://<your-downloader-host>:8080`

### `DATADOWNLOADER_API_KEY` / `DATADOWNLOADER_API_KEY_HEADER`
- Purpose: Authentication for the downloader service.
- Values: **do not document actual values**; they must be supplied by the runtime environment.

### `DATADOWNLOADER_SKIP_LOCAL_START`
- Purpose: Prevents any local downloader/ThetaTerminal bootstrap logic from running (backtests must use the remote downloader in production workflows).

## ThetaData option chain building (performance)

These env vars are used by the ThetaData chain cache/builder in `lumibot/tools/thetadata_helper.py`.

### `THETADATA_CHAIN_DEFAULT_MAX_DAYS_OUT`
- Purpose: Bounds the default option-chain expiration window for equity underlyings to reduce strike-list fanout in cold caches/backtests.
- Values: integer days.
- Default: `730` (2 years).
- Notes: set to `0` to disable the default bound (fetch all expirations).

### `THETADATA_CHAIN_DEFAULT_MAX_DAYS_OUT_INDEX`
- Purpose: Same as `THETADATA_CHAIN_DEFAULT_MAX_DAYS_OUT`, but for index-like underlyings (SPX/NDX/VIX/etc) with dense expiration schedules.
- Values: integer days.
- Default: `180`.
- Notes: set to `0` to disable the default bound.

### `THETADATA_CHAIN_RECENT_FILE_TOLERANCE_DAYS`
- Purpose: Local chain cache file reuse window (equities) when no chain hints are in effect.
- Values: integer days.
- Default: `7`.

### `THETADATA_CHAIN_STRIKES_TIMEOUT`
- Purpose: Downloader wait timeout per strike-list request when building chains.
- Values: seconds (float).
- Default: `300`.

### `THETADATA_CHAIN_STRIKES_BATCH_SIZE`
- Purpose: Number of in-flight strike-list requests when building chains.
- Values: integer.
- Default: `0` (use queue client concurrency).

## ThetaData corporate action normalization (accuracy)

### `THETADATA_APPLY_CORPORATE_ACTIONS_INTRADAY`
- Purpose: Apply split/dividend adjustments to **intraday** frames (minute/second/hour) in backtests so:
  - intraday stock OHLC/quotes match **daily** split-adjusted prices, and
  - option-chain strike normalization (which uses split-adjusted daily reference prices) stays consistent.
- Values: truthy enables (`1`, `true`, `yes`); falsy disables (`0`, `false`).
- Default:
  - enabled when `IS_BACKTESTING` is truthy
  - disabled otherwise
- Pitfall: disabling can break options strike selection around splits (example: NVDA 2024-06-10 10:1 split).
- Where: `lumibot/tools/thetadata_helper.py` (`get_price_data`)

## Remote cache (S3)

### `LUMIBOT_CACHE_BACKEND` / `LUMIBOT_CACHE_MODE`
- Purpose: Enable remote cache mirroring.
- Common values:
  - `LUMIBOT_CACHE_BACKEND=s3`
  - `LUMIBOT_CACHE_MODE=readwrite` (or `readonly`)

### `LUMIBOT_CACHE_FOLDER`
- Purpose: Override the local cache folder (useful to simulate a fresh ECS task).
- Notes: This is read at import/startup time; changing it mid-run will not relocate already-created paths.

### `LUMIBOT_CACHE_S3_BUCKET`, `LUMIBOT_CACHE_S3_PREFIX`, `LUMIBOT_CACHE_S3_REGION`
- Purpose: S3 target configuration.

### `LUMIBOT_CACHE_S3_VERSION`
- Purpose: Namespace/version the remote cache without deleting anything.
- Practical use: set a unique version to simulate a “cold S3” run safely.

### `LUMIBOT_CACHE_S3_ACCESS_KEY_ID`, `LUMIBOT_CACHE_S3_SECRET_ACCESS_KEY`, `LUMIBOT_CACHE_S3_SESSION_TOKEN`
- Purpose: Credentials for S3 access when not using an instance/task role.
- Values: **never commit**.

For cache key layout and validation workflow, see `docs/remote_cache.md`.

## Runtime telemetry (memory/health)

LumiBot can emit lightweight, vendor-neutral telemetry lines to stdout so you can debug OOMs in any environment
(Render, ECS, local Docker, etc.). Telemetry is **best-effort**: failures are swallowed and must never crash trading.

Each emission is a single JSON line prefixed with `LUMIBOT_TELEMETRY`.

### `LUMIBOT_TELEMETRY`
- Purpose: Enable/disable runtime telemetry emission.
- Values: truthy enables (`1`, `true`, `yes`); falsy disables (`0`, `false`).
- Default: enabled for live runs; disabled for backtests and pytest.

### `LUMIBOT_TELEMETRY_INTERVAL_SECONDS`
- Purpose: Base telemetry cadence.
- Values: seconds (float).
- Default: `300`.

### `LUMIBOT_TELEMETRY_DEEP`
- Purpose: Enable deep snapshot mode for diagnosing unknown memory sources.
- Values: truthy enables (`1`, `true`, `yes`); falsy disables.
- Default: disabled.
- Notes: Deep mode uses `tracemalloc` and only emits snapshots when memory is near OOM.

Notes:
- Burst mode (more frequent logs) turns on automatically above ~80% of container memory.
- Deep snapshots trigger above ~90% with a ~1 hour cooldown (these thresholds are fixed defaults today).
