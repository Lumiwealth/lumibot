# Environment Variables (Engineering Notes)

This page documents environment variables used by LumiBot, with an emphasis on **backtesting** and **ThetaData / downloader / caching** behavior.

**Public docs (source of truth):** the Sphinx page at `docsrc/environment_variables.rst` must be updated whenever env var behavior changes.

## Rules

- **Never commit secrets.** Document variable *names*, accepted values, and semantics—never real API keys, tokens, passwords, or AWS secrets.
- **Env var changes require docs changes.** If you add/change an env var, update:
  - `docsrc/environment_variables.rst` (public docs), and
  - this file (engineering notes) when it helps contributors.

## Backtesting selection + dates

### `ALPACA_NEWS_API_KEY` / `ALPACA_NEWS_API_SECRET`
- Purpose: Optional credentials for the built-in `alpaca_news` agent tool when there is no active Alpaca broker.
- Fallback: If these are unset and there is no active Alpaca broker, `alpaca_news` is not exposed to agents.
- Notes:
  - The tool date-gates requests to the strategy datetime during backtests.
  - Do not commit real key values.

### Polymarket CLOB broker variables
- Selectors:
  - `TRADING_BROKER=polymarket` or `TRADING_BROKER=polymarket_clob`
  - `DATA_SOURCE=polymarket` or `DATA_SOURCE=polymarket_clob` for public market data only.
- Local prototype secret storage: `.env.local` is loaded after `.env` unless `LUMIBOT_DISABLE_DOTENV_LOCAL=1`.
- Credential fields:
  - `POLYMARKET_PRIVATE_KEY`: wallet private key/session signer for CLOB signing (**secret**).
  - `POLYMARKET_OWNER_ADDRESS`: optional owner/signer address for Magic/proxy accounts. Used as a fallback for Data API position/value reads.
  - `POLYMARKET_WALLET_ADDRESS`: proxy wallet, deposit wallet, Safe, or other funder address passed to the CLOB client.
  - `POLYMARKET_SIGNATURE_TYPE`: CLOB signature type. `0` = EOA, `1` = existing Polymarket proxy/Magic wallet, `2` = Gnosis Safe, `3` = deposit wallet / `POLY_1271`. The broker defaults to `1` when owner and wallet differ, otherwise `3`; set it explicitly for live tests.
  - `POLYMARKET_CLOB_API_KEY`, `POLYMARKET_CLOB_API_SECRET`, `POLYMARKET_CLOB_API_PASSPHRASE`: optional CLOB L2 credentials (**secret**).
  - `POLYMARKET_API_CREDENTIALS_JSON`: optional JSON wrapper for CLOB credentials (**secret**).
  - `POLYMARKET_RELAYER_API_KEY`, `POLYMARKET_RELAYER_API_KEY_ADDRESS`: relayer credentials for wallet deployment/approval flows when required (**secret**).
  - `POLYMARKET_BUILDER_CODE`: optional attribution code.
  - `POLYMARKET_AUTO_APPROVE`: truthy to attempt CLOB collateral approval setup before live trading.
  - `POLYMARKET_MAX_MARKET_ORDER_NOTIONAL`: hard cap for market BUY dollar amount; defaults to `5`.
- Test gates:
  - `POLYMARKET_TEST_TOKEN_ID`: explicit token id for public/live smoke tests.
  - `POLYMARKET_LIVE_TRADING_ENABLED=true`: required before any live submit/cancel smoke test can run.
  - `POLYMARKET_TEST_MAX_NOTIONAL`: additional live-test cap; default path never exceeds `5`.
- Notes:
  - Do not auto-detect Polymarket from a private key. Use explicit `TRADING_BROKER=polymarket`.
  - Relayer keys are not the same as CLOB trading credentials.
  - CLOB collateral balances are returned in 6-decimal raw units by `get_balance_allowance`; LumiBot scales those into dollars.
  - Current local proof on 2026-07-01 can read balances, positions, open orders, trades, public data, and public/private WebSockets. Live submit is blocked for Rob's current Magic/proxy account by Polymarket's deposit-wallet/API-key binding error: `maker address not allowed, please use the deposit wallet flow`.
  - Direct smoke helper: `scripts/polymarket_smoke.py`. It loads `.env.local`, redacts output, writes artifacts under gitignored `logs/`, and only submits live orders when `POLYMARKET_LIVE_TRADING_ENABLED=true` plus notional caps are present.
  - Never commit or log raw Polymarket private keys or CLOB credentials.

### `LUMIBOT_DISABLE_DOTENV`
- Purpose: Disable automatic `.env` discovery and loading at startup.
- Values: truthy enables (`1`, `true`, `yes`); unset/`0` disables.
- Default: disabled.
- Why it matters:
  - LumiBot normally searches upward from the running script directory for the nearest `.env`, then from the current working directory if needed.
  - LumiBot no longer recursively scans every nested directory under the start path. This reduces startup latency and lowers the chance of loading an unrelated nested repo's `.env`.
  - In production/BotManager backtests we rely on injected environment variables, so `.env` discovery should be off with `LUMIBOT_DISABLE_DOTENV=1`.
- Where: `lumibot/credentials.py`

### `LUMIBOT_DISABLE_DOTENV_LOCAL`
- Purpose: Disable sibling `.env.local` loading after a discovered `.env`.
- Values: truthy enables (`1`, `true`, `yes`); unset/`0` disables.
- Default: disabled.
- Why it matters:
  - When dotenv loading is enabled and a `.env` file is found, LumiBot loads a sibling `.env.local` afterward with override behavior.
  - Local overrides are convenient for developer machines but should usually be disabled in production/runtime-secret contexts.
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
    - `{"default":"thetadata","stock":"thetadata","option":"thetadata","index":"thetadata","future":"ibkr","cont_future":"ibkr","crypto":"ibkr","crypto_future":"ibkr"}`
    - Provider values are case/whitespace/_/- insensitive. Supported values include:
      - `thetadata`, `ibkr`, `polygon`, `alpaca`
      - `ccxt` (auto-select exchange from existing env/credentials; defaults to Coinbase when no exchange credential/config is available)
      - supported CCXT backtesting exchange ids such as `coinbase`, `kraken`, `binance`, `kucoin`, `bitmex`, `bybit`, and `okx`
  - `none` to disable env override and rely on code.
- Crypto futures/perpetuals: `Asset.AssetType.CRYPTO_FUTURE` routes through `crypto_future` when present, otherwise `crypto`, then `default`. Spot proxy pricing preserves the requested quote asset exactly: `BTCUSDT`, `ETHUSDT`, and `SOLUSDT` request `BTC/USDT`, `ETH/USDT`, and `SOL/USDT`. If that exact pair has no provider data, the result is missing data; LumiBot must not silently substitute a USD proxy.
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
- `LUMIBOT_SCHEDULED_TARGET_RUN_AT`: UTC ISO-8601 target time for exact scheduled runs. When present, LumiBot initializes the strategy/broker first, waits locally until this timestamp immediately before `on_trading_iteration()`, and skips the iteration if the drift budget is exceeded.
- `LUMIBOT_SCHEDULED_PRE_START_AT`: UTC ISO-8601 pre-start time used by BotManager telemetry to compare scheduler launch timing with the requested target.
- `LUMIBOT_SCHEDULED_MAX_TARGET_DRIFT_MS`: maximum allowed late drift in milliseconds for exact scheduled runs. Defaults to `1000`.
- `LUMIBOT_SCHEDULED_POST_ITERATION_SECONDS`: drain window after the one live iteration. During this window LumiBot continues processing broker/order queue events before exiting.
- `LUMIBOT_SCHEDULED_TIMING_FILE`: local JSON timing file written by LumiBot for BotManager bootstrap telemetry.
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

### `THETADATA_QUEUE_QUOTE_TIMEOUT`
- Purpose: Data Downloader wait timeout for `/history/quote` requests used in point-in-time option pricing.
- Values: seconds (float); set to `0` only if you intentionally want unbounded waits.
- Default: `300`.
- Notes: this is intentionally shorter than `THETADATA_QUEUE_HISTORY_TIMEOUT` so one stuck option quote fails visibly instead of making a backtest appear frozen for a full OHLC-history timeout window.

### `THETADATA_QUEUE_OPTION_OHLC_TIMEOUT`
- Purpose: Data Downloader wait timeout for `/option/history/ohlc` requests used by sparse option contract probes.
- Values: seconds (float); set to `0` only if you intentionally want unbounded waits.
- Default: `300`.
- Notes: stock/index history continues to use `THETADATA_QUEUE_HISTORY_TIMEOUT`; this only bounds option OHLC requests that can otherwise stall a backtest on one illiquid contract/day.

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

## AI agent fundamentals, memory, and notifications

### `LUMIBOT_SEC_USER_AGENT`
- Purpose: Contact-style SEC EDGAR user agent header.
- Values: Human-readable app/contact string.
- Default: LumiBot support contact.

### `LUMIBOT_SEC_CACHE_DIR`
- Purpose: Override the local SEC fundamentals and filing cache.
- Default: `~/.lumibot/cache/sec`.

### `FRED_API_KEY`
- Purpose: Required official FRED/ALFRED API key for macro data tools.
- Default: unset.
- Notes: Required for FRED macro data tools. Lumibot requests vintage observations with `realtime_start` and `realtime_end` for point-in-time backtests. Built-in FRED agent tools are not exposed during backtests without this key because Lumibot does not use revised public CSV fallbacks for macro data.

### `LUMIBOT_FRED_CACHE_DIR`
- Purpose: Override the local FRED macro data cache.
- Default: `~/.lumibot/cache/fred`.

### `LUMIBOT_MEMORY_DIR`
- Purpose: Override the local SQLite agent memory root.
- Default: `.lumibot/memory` under the current working directory.

### `LUMIBOT_AGENT_MEMORY_NOTE_MAX_CHARS`
- Purpose: Limit how much prior agent-run memory is injected back into the next agent prompt.
- Default: `2000`.
- Notes: Full run traces and artifacts are still written separately. This only compacts the lightweight runtime notes so repeated backtest iterations do not blow up the model context window.

### `LUMIBOT_AGENT_MAX_MODEL_CALLS`
- Purpose: Hard cap uncached agent model calls in a single strategy run.
- Default: unset.
- Notes: When set, Lumibot raises before making the next provider call once the cap is reached. This is intended for expensive AI backtests and smoke runs where accidental spend matters.

### `LUMIBOT_AGENT_MAX_RUN_ATTEMPTS`
- Purpose: Override the retry budget for a single agent model call.
- Default: `2` in backtests, `10` in live trading.
- Notes: Backtests default to a lower retry budget so a bad provider window does not multiply model spend across many simulated iterations.

### `TELEGRAM_BOT_TOKEN`
- Purpose: Telegram Bot API token for `self.notifications.configure_telegram()`.
- Values: Bot token from BotFather.

### `TELEGRAM_CHAT_ID`
- Purpose: Telegram chat/channel/user id for strategy notifications.
- Values: Telegram chat id.
