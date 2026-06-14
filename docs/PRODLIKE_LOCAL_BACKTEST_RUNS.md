# Prod-Like Local Backtests (NVDA / SPX / Repros)

This doc standardizes how we run **production-faithful** backtests locally without:
- accidentally loading unrelated `.env` or `.env.local` files,
- mixing artifacts across runs, or
- running long, unbounded commands.

## Rules

- **Always use a timeout guard**: wrap runs with `/Users/robertgrzesik/bin/safe-timeout …`.
- **Never run from `Strategy Library/` directly** (it often contains nested `.env` files).
- For production-like BotSpot/BotManager runs, set `LUMIBOT_DISABLE_DOTENV=1` so injected runtime environment variables are the source of truth.
- If you intentionally allow a local `.env`, set `LUMIBOT_DISABLE_DOTENV_LOCAL=1` when `.env.local` overrides would make the run non-repeatable.
- Prefer **short windows** (days/weeks/months) for diagnosis; only run full windows once request volume looks sane.
- Do not delete shared caches. Use `LUMIBOT_CACHE_S3_VERSION=...` to isolate “cold namespace” simulations.
- Use `--cache-mode readonly` when you want a fresh local cache folder to hydrate
  from the configured S3 namespace without uploading or mutating shared cache
  objects.
- For repeated runs, prevent browser/UI spam (while still writing artifacts) by setting:
  - `LUMIBOT_DISABLE_UI=1`

## Canonical runner

Use `scripts/run_backtest_prodlike.py`.

Key behavior:
- Runs the strategy in a **clean per-run workdir** under `~/Documents/Development/backtest_runs/…`.
- Writes artifacts to `workdir/logs/` (`*_tearsheet.html`, `*_trades.csv`, `*_logs.csv`, `*_settings.json`).
- Prints a small “scoreboard” and writes `workdir/metrics.json` (wall time + queue submits + Theta STALE count + top endpoint families) so runs are comparable.
- Loads downloader + S3 config from `botspot_node/.env-local` **without printing secrets**.
- Forwards `THETADATA_USERNAME`, `THETADATA_PASSWORD`, and
  `THETADATA_API_KEY` from the dotenv when present, matching BotSpot Node's
  provider environment injection.
- Optionally copies artifacts into another folder (e.g., `Strategy Library/logs`) via `--copy-artifacts-to`.
- For investigations, you can enable:
  - `--audit` → sets `LUMIBOT_BACKTEST_AUDIT=1` (adds `audit.*` columns to trade logs)
  - `--profile yappi` → sets `BACKTESTING_PROFILE=yappi` (writes a yappi CSV artifact)

### Current workspace overrides

In this checkout family, do not rely on the runner defaults. Some examples and
defaults still point at the older `~/Documents/Development/...` tree. For runs
from the current repo, pass these explicitly:

- `--lumibot-root /Users/robertgrzesik/Development/lumibot`
- `--dotenv /Users/robertgrzesik/Development/botspot_node/.env-local` for dev-cache runs
- a separate approved production-like dotenv for prod-cache runs

`botspot_node/.env-local` on this Mac is not production parity for the TQQQ
IBKR investigation: it points at the dev cache namespace. Exact production-cache
replay needs a dotenv/env source whose non-secret cache identifiers match the
production backtest settings being compared, for example:

- `LUMIBOT_CACHE_S3_BUCKET=lumibot-cache-prod`
- `LUMIBOT_CACHE_S3_PREFIX=prod/cache`
- `LUMIBOT_CACHE_S3_VERSION=v1` for historical runs that recorded `v1`

Do not edit repo `.env` files or paste secrets into docs. Use an approved
local/runtime secret source and verify the runner's printed cache bucket,
prefix, and version before trusting any result.

### Provider override proof

LumiBot's `BACKTESTING_DATA_SOURCE` env var overrides a strategy's explicit
`datasource_class` in `Strategy.backtest()`. This is why saved BotSpot strategy
files that pass `YahooDataBacktesting` can still run on ThetaData, IBKR, or the
BotSpot Auto router when the managed runtime injects the provider env.

For production-like local runs, `LUMIBOT_DISABLE_DOTENV=1` is mandatory. Without
it, LumiBot scans for `.env`/`.env.local`; this machine's
`/Users/robertgrzesik/Development/lumibot/.env.local` currently sets
`BACKTESTING_DATA_SOURCE=ibkr`, which can silently overwrite a runner-injected
ThetaData or router setting.

Before a comparison run, verify the import path from outside the repo:

```bash
/Users/robertgrzesik/bin/safe-timeout 30s \
  env PYTHONPATH=/Users/robertgrzesik/Development/lumibot \
  python3 -c "import lumibot, sys; print(lumibot.__file__); print(getattr(lumibot, '__version__', None)); print(sys.path[:3])"
```

The expected import path starts with:

```text
/Users/robertgrzesik/Development/lumibot/lumibot/__init__.py
```

Without the `PYTHONPATH` override, this Mac may import the pip-installed
package instead.

## TQQQ SMC provider matrix replay

Use this path when comparing saved BotSpot revisions such as TQQQ SMC v15/v19
across ThetaData and BotSpot Auto while running local LumiBot code.

Source-of-truth env chain in production:

1. BotSpot Node selects a provider slug (`theta_data`, `botspot_auto`, etc.).
2. `DataAccessService.getProviderEnvironment()` turns that slug into
   `BACKTESTING_DATA_SOURCE` plus downloader/cache env.
3. BotSpot Node flattens the env block into `bot_config`.
4. Bot Manager adds backtest flags and starts the Python strategy container.
5. LumiBot reads `BACKTESTING_DATA_SOURCE` at runtime and overrides the
   strategy file's explicit datasource class.

Provider values to pass locally:

```bash
BOTSPOT_AUTO_ROUTER='{"default":"ibkr","stock":"ibkr","index":"ibkr","option":"thetadata","crypto":"ibkr","crypto_future":"ibkr","future":"ibkr","cont_future":"ibkr"}'
THETA_DATA_SOURCE='thetadata'
```

Run command template:

```bash
cd /Users/robertgrzesik/Development/lumibot

RUNID="$(date +%Y%m%d_%H%M%S)"
BASE="/Users/robertgrzesik/Development/lumibot/logs/tqqq_provider_matrix_${RUNID}"
PRODLIKE_DOTENV="/path/to/approved/uncommitted-prodlike-backtest-env.env"
MAIN="/Users/robertgrzesik/Development/lumibot/logs/tqqq_provider_diff_20260613/code/v15_main.py"

mkdir -p "$BASE/cache/v15_theta" "$BASE/runs/v15_theta"

/Users/robertgrzesik/bin/safe-timeout 7200s \
  python3 scripts/run_backtest_prodlike.py \
    --label tqqq_v15_theta_full \
    --main "$MAIN" \
    --start 2016-01-21 \
    --end 2026-04-16 \
    --data-source "$THETA_DATA_SOURCE" \
    --dotenv "$PRODLIKE_DOTENV" \
    --lumibot-root /Users/robertgrzesik/Development/lumibot \
    --workdir "$BASE/runs/v15_theta" \
    --cache-folder "$BASE/cache/v15_theta" \
    --cache-mode readwrite \
    --cache-bucket lumibot-cache-prod \
    --cache-prefix prod/cache \
    --cache-version v1 \
    --subprocess-log "$BASE/runs/v15_theta/subprocess.log"
```

For the BotSpot Auto scenario, change only the label, main path if testing a
different revision, run/cache folders, and data source:

```bash
--data-source "$BOTSPOT_AUTO_ROUTER"
```

For non-mutating diagnostic reads against the same S3 namespace, use
`--cache-mode readonly` and clearly label the run as readonly. For exact
production mutation semantics, use the production mode recorded in the
reference settings file, usually `readwrite`.

Minimum proof before trusting a result:

- runner output prints local `--lumibot-root`, intended cache bucket/prefix/version,
  and intended data source;
- child stdout does not contain `.env file loaded from:` or `.env.local file loaded from:`;
- `*_settings.json` records the intended `backtesting_data_sources`;
- `*_settings.json` records the expected local LumiBot version, and the separate
  import-path proof above shows the child can import the local checkout;
- `metrics.json` and subprocess log are saved under the durable repo-local run
  folder, not `/tmp`;
- warm/cold comparisons use the same cache mode on both sides.

## NVDA example (short-window diagnostic)

```bash
RUNID="$(date +%Y%m%d_%H%M%S)"
CACHE_DIR="/Users/robertgrzesik/Documents/Development/tmp/lumibot_cache_nvda_${RUNID}"
mkdir -p "$CACHE_DIR"

/Users/robertgrzesik/bin/safe-timeout 600s \
  python3 scripts/run_backtest_prodlike.py \
    --label nvda_diag \
    --audit \
    --cache-folder "$CACHE_DIR" \
    --main "/Users/robertgrzesik/Documents/Development/Strategy Library/tmp/backtest_code/334e2c98-7134-4f38-860c-b6b11879a51b/main.py" \
    --start 2024-01-02 \
    --end 2024-03-30 \
    --copy-artifacts-to "/Users/robertgrzesik/Documents/Development/Strategy Library/logs"
```

## SPX example (cold namespace inspection)

```bash
RUNID="$(date +%Y%m%d_%H%M%S)"
CACHE_DIR="/Users/robertgrzesik/Documents/Development/tmp/lumibot_cache_spx_${RUNID}"
mkdir -p "$CACHE_DIR"

/Users/robertgrzesik/bin/safe-timeout 900s \
  python3 scripts/run_backtest_prodlike.py \
    --label spx_copy2_cold_inspect \
    --audit \
    --cache-folder "$CACHE_DIR" \
    --cache-version "spx_cold_${RUNID}" \
    --main "/Users/robertgrzesik/Documents/Development/Strategy Library/tmp/backtest_code/c7c6bbd9-41f7-48c9-8754-3231e354f83b/main.py" \
    --start 2025-01-07 \
    --end 2025-02-07 \
    --copy-artifacts-to "/Users/robertgrzesik/Documents/Development/Strategy Library/logs"
```

Warm proof:
- keep the same `--cache-version`
- change only `--cache-folder` to a new empty folder
- expect near-zero “Submitted to queue” lines

## Client benchmark: `SPX0DTEHybridStrangle` (SPX Short Straddle Intraday Copy 4)

This is the current “must-be-fast” benchmark strategy (client-facing).

Strategy file:
- `/Users/robertgrzesik/Documents/Development/Strategy Library/Demos/SPX Short Straddle Intraday (Copy 4).py`

Related investigation (why “ETA days” happened in prod for SPX strategies):
- `docs/investigations/2026-01-13_SPX_INTRADAY_STALE_LOOP_FIX.md`

### Cold run (new S3 namespace)

```bash
RUNID="$(date +%Y%m%d_%H%M%S)"
CACHE_DIR="/Users/robertgrzesik/Documents/Development/tmp/lumibot_cache_spx_${RUNID}"
mkdir -p "$CACHE_DIR"

/Users/robertgrzesik/bin/safe-timeout 900s \
  python3 scripts/run_backtest_prodlike.py \
    --label spx0dtehybridstrangle_cold \
    --cache-folder "$CACHE_DIR" \
    --cache-version "spx_cold_${RUNID}" \
    --main "/Users/robertgrzesik/Documents/Development/Strategy Library/Demos/SPX Short Straddle Intraday (Copy 4).py" \
    --start 2025-02-03 \
    --end 2025-02-07
```

### Warm run (same S3 namespace; yappi enabled)

Warm definition: `queue_submits == 0` (same `--cache-version`, new empty local `--cache-folder`).

```bash
RUNID="$(date +%Y%m%d_%H%M%S)"
CACHE_DIR="/Users/robertgrzesik/Documents/Development/tmp/lumibot_cache_spx_warm_${RUNID}"
mkdir -p "$CACHE_DIR"

/Users/robertgrzesik/bin/safe-timeout 900s \
  python3 scripts/run_backtest_prodlike.py \
    --label spx0dtehybridstrangle_warm \
    --profile yappi \
    --cache-folder "$CACHE_DIR" \
    --cache-version "<THE_SAME_CACHE_VERSION_USED_FOR_COLD>" \
    --main "/Users/robertgrzesik/Documents/Development/Strategy Library/Demos/SPX Short Straddle Intraday (Copy 4).py" \
    --start 2025-02-03 \
    --end 2025-02-07
```

### Where the outputs go (always)

For each run, the runner creates a clean workdir under:
- `~/Documents/Development/backtest_runs/<run_id>/`

Important artifacts:
- `~/Documents/Development/backtest_runs/<run_id>/metrics.json`
  - wall time (seconds)
  - `queue_submits`
  - `thetadata_cache_stale`
  - top endpoint families (by `path=...`)
- `~/Documents/Development/backtest_runs/<run_id>/logs/*_profile_yappi.csv` (when `--profile yappi` is used)

Practical advice:
- Don’t jump to “S3 is slow” conclusions until you’ve run the warm run and checked `metrics.json`.
