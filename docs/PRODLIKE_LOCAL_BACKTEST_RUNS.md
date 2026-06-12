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
- Optionally copies artifacts into another folder (e.g., `Strategy Library/logs`) via `--copy-artifacts-to`.
- For investigations, you can enable:
  - `--audit` → sets `LUMIBOT_BACKTEST_AUDIT=1` (adds `audit.*` columns to trade logs)
  - `--profile yappi` → sets `BACKTESTING_PROFILE=yappi` (writes a yappi CSV artifact)

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
